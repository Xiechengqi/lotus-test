package storageadapter

// this file implements storagemarket.StorageProviderNode

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/storage"
	"github.com/go-redis/redis/v8"
	"strings"
	"time"

	market8 "github.com/filecoin-project/go-state-types/builtin/v8/market"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors/builtin/market"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/chain/events"
	"github.com/filecoin-project/lotus/chain/events/state"
	"github.com/filecoin-project/lotus/chain/types"
	sealing "github.com/filecoin-project/lotus/extern/storage-sealing"
	"github.com/filecoin-project/lotus/lib/sigs"
	"github.com/filecoin-project/lotus/markets/utils"
	"github.com/filecoin-project/lotus/node/config"
	"github.com/filecoin-project/lotus/node/modules/helpers"
	"github.com/filecoin-project/lotus/storage/sectorblocks"
)

var addPieceRetryWait = 5 * time.Minute
var addPieceRetryTimeout = 6 * time.Hour
var defaultMaxProviderCollateralMultiplier = uint64(2)
var log = logging.Logger("storageadapter")

type ProviderNodeAdapter struct {
	v1api.FullNode

	secb *sectorblocks.SectorBlocks
	ev   *events.Events

	dealPublisher *DealPublisher
	as            *storage.AddressSelector

	addBalanceSpec              *api.MessageSendSpec
	maxDealCollateralMultiplier uint64
	dsMatcher                   *dealStateMatcher
	scMgr                       *SectorCommittedManager

	redisClient *redis.ClusterClient
}

func NewProviderNodeAdapter(fc *config.MinerFeeConfig, dc *config.DealmakingConfig) func(mctx helpers.MetricsCtx, lc fx.Lifecycle, secb *sectorblocks.SectorBlocks, full v1api.FullNode, dealPublisher *DealPublisher, redisClient *redis.ClusterClient, as *storage.AddressSelector) (storagemarket.StorageProviderNode, error) {
	return func(mctx helpers.MetricsCtx, lc fx.Lifecycle, secb *sectorblocks.SectorBlocks, full v1api.FullNode, dealPublisher *DealPublisher, redisClient *redis.ClusterClient, as *storage.AddressSelector) (storagemarket.StorageProviderNode, error) {
		ctx := helpers.LifecycleCtx(mctx, lc)

		ev, err := events.NewEvents(ctx, full)
		if err != nil {
			return nil, err
		}
		na := &ProviderNodeAdapter{
			FullNode: full,

			secb:          secb,
			ev:            ev,
			dealPublisher: dealPublisher,
			dsMatcher:     newDealStateMatcher(state.NewStatePredicates(state.WrapFastAPI(full))),
		}
		if fc != nil {
			na.addBalanceSpec = &api.MessageSendSpec{MaxFee: abi.TokenAmount(fc.MaxMarketBalanceAddFee)}
		}
		na.maxDealCollateralMultiplier = defaultMaxProviderCollateralMultiplier
		if dc != nil {
			na.maxDealCollateralMultiplier = dc.MaxProviderCollateralMultiplier
		}
		na.scMgr = NewSectorCommittedManager(ev, na, &apiWrapper{api: full})

		//redisConn, _ := os.LookupEnv("REDIS_CONN")
		//redisPassword, _ := os.LookupEnv("REDIS_PASSWORD")

		//log.Infof("octopus: init redis in market provider: %v ******", redisConn)
		//na.redisClient = redis.NewClusterClient(&redis.ClusterOptions{
		//	Addrs:    strings.Split(redisConn, ","),
		//	Password: redisPassword,
		//	PoolSize: 1,
		//})
		na.redisClient = redisClient
		na.as = as

		return na, nil
	}
}

func (n *ProviderNodeAdapter) PublishDeals(ctx context.Context, deal storagemarket.MinerDeal) (cid.Cid, error) {
	return n.dealPublisher.Publish(ctx, deal.ClientDealProposal)
}

func (n *ProviderNodeAdapter) OnDealComplete(ctx context.Context, deal storagemarket.MinerDeal, pieceSize abi.UnpaddedPieceSize, pieceData shared.ReadSeekStarter, path string) (*storagemarket.PackingResult, error) {
	if deal.PublishCid == nil {
		return nil, xerrors.Errorf("deal.PublishCid can't be nil")
	}

	sdInfo := api.PieceDealInfo{
		DealID:       deal.DealID,
		DealProposal: &deal.Proposal,
		PublishCid:   deal.PublishCid,
		DealSchedule: api.DealSchedule{
			StartEpoch: deal.ClientDealProposal.Proposal.StartEpoch,
			EndEpoch:   deal.ClientDealProposal.Proposal.EndEpoch,
		},
		KeepUnsealed: deal.FastRetrieval,
	}

	var p abi.SectorNumber
	var offset abi.PaddedPieceSize
	var err error
	isRetry := false

	// if redis error, process as a new deal.
	key := fmt.Sprintf("%v-ap-%d", deal.Proposal.Provider, deal.DealID)
	exists, redisErr := n.redisClient.Exists(ctx, key).Result()
	if redisErr != nil {
		log.Errorf("octopus: exists key %v error: %v", key, redisErr)
		// error as not exist
		exists = 0
	}
	if exists == 1 {
		log.Infof("octopus: key %s exists in redis", key)
		// check redis
		ticker := time.NewTicker(1 * time.Minute)
	completeLoop:
		for {
			select {
			case <-ticker.C:
				va, redisErr := n.redisClient.Get(ctx, key).Result()
				if redisErr == nil {
					apInfo := &sealing.APInfo{}
					json.Unmarshal([]byte(va), apInfo)
					log.Infof("octopus: key %s status %s", key, apInfo.Status)

					if apInfo.Status == "Error" {
						if strings.Contains(apInfo.Reason, "retry add piece") {
							log.Infof("octopus: deal %d ap error: %v, retry.", deal.DealID, apInfo.Reason)
							isRetry = true
							break completeLoop
						} else {
							return nil, xerrors.Errorf("AddPiece failed: %s", apInfo.Reason)
						}
					} else if apInfo.Status == "Complete" {
						p = apInfo.P
						offset = apInfo.Offset
						break completeLoop
					} else {
						// wait next interval check
						// should not reach here.
						log.Debug("octopus: redis: ap status %s.", apInfo.Status)
					}
				} else {
					// wait next interval check
					log.Warn("octopus: redis: get key %s error", key)
				}
			}
		}
	}
	if exists == 0 || isRetry == true {
		log.Infof("octopus: key %s doesn't exists, or should retry.", key)
		// Attempt to add the piece to the sector
		p, offset, err = n.secb.AddPiece(ctx, pieceSize, pieceData, sdInfo, path)
		curTime := build.Clock.Now()
	keepLoop:
		for build.Clock.Since(curTime) < addPieceRetryTimeout {
			if err != nil {
				if strings.Contains(err.Error(), "retry add piece") {
					log.Infof("octopus: error %v, try to add piece again.", err)
					select {
					case <-build.Clock.After(addPieceRetryWait):
						// Reset the reader to the start
						err = pieceData.SeekStart()
						if err != nil {
							return nil, xerrors.Errorf("failed to reset piece reader to start before retrying AddPiece for deal %d: %w", deal.DealID, err)
						}

						// Attempt to add the piece again
						p, offset, err = n.secb.AddPiece(ctx, pieceSize, pieceData, sdInfo, path)
					case <-ctx.Done():
						return nil, xerrors.New("context expired while waiting to retry AddPiece")
					}
				} else if strings.Contains(err.Error(), "websocket connection closed") {
					log.Infof("websocket connection closed, check redis")
					// check redis
					ticker := time.NewTicker(1 * time.Minute)
					for {
						select {
						case <-ticker.C:
							va, redisErr := n.redisClient.Get(ctx, key).Result()
							if redisErr == nil {
								apInfo := &sealing.APInfo{}
								json.Unmarshal([]byte(va), apInfo)
								if apInfo.Status == "Error" {
									if strings.Contains(apInfo.Reason, "retry add piece") {
										log.Infof("octopus: deal %d ap error: %v, retry.", deal.DealID, apInfo.Reason)
										err = xerrors.Errorf(apInfo.Reason)
										break
									} else {
										log.Debugf("octopus: deal %d status error: %s", deal.DealID, apInfo.Reason)
										return nil, xerrors.Errorf("AddPiece failed: %s", apInfo.Reason)
									}
								} else if apInfo.Status == "Complete" {
									p = apInfo.P
									offset = apInfo.Offset
									ticker.Stop()
									log.Debugf("octopus: deal complete: p: %v offset: %v", p, offset)
									break keepLoop
								} else {
									// wait next interval check
									log.Warnf("octopus: apInfo.status=%s wait next interval", apInfo.Status)
								}
							} else {
								// wait next interval check
								log.Warnf("octopus: deal %d: get key error %v", deal.DealID, redisErr)
							}
						}
					}
				} else {
					return nil, xerrors.Errorf("AddPiece failed: %s", err)
				}
			} else {
				break
			}
		}
	}

	//if err != nil {
	//	return nil, xerrors.Errorf("AddPiece failed: %s", err)
	//}
	log.Warnf("New Deal: deal %d", deal.DealID)

	return &storagemarket.PackingResult{
		SectorNumber: p,
		Offset:       offset,
		Size:         pieceSize.Padded(),
	}, nil
}

func (n *ProviderNodeAdapter) VerifySignature(ctx context.Context, sig crypto.Signature, addr address.Address, input []byte, encodedTs shared.TipSetToken) (bool, error) {
	addr, err := n.StateAccountKey(ctx, addr, types.EmptyTSK)
	if err != nil {
		return false, err
	}

	err = sigs.Verify(&sig, addr, input)
	return err == nil, err
}

func (n *ProviderNodeAdapter) GetMinerWorkerAddress(ctx context.Context, maddr address.Address, tok shared.TipSetToken) (address.Address, error) {
	tsk, err := types.TipSetKeyFromBytes(tok)
	if err != nil {
		return address.Undef, err
	}

	mi, err := n.StateMinerInfo(ctx, maddr, tsk)
	if err != nil {
		return address.Address{}, err
	}
	return mi.Worker, nil
}

func (n *ProviderNodeAdapter) GetAddBalanceAddress(ctx context.Context, maddr address.Address, tok shared.TipSetToken) (address.Address, error) {
	tsk, err := types.TipSetKeyFromBytes(tok)
	if err != nil {
		return address.Undef, err
	}

	mi, err := n.StateMinerInfo(ctx, maddr, tsk)
	if err != nil {
		return address.Address{}, err
	}

	addr, _, err := n.as.AddressFor(ctx, n, mi, api.DealPublishAddr, big.Zero(), big.Zero())
	if err != nil {
		return mi.Worker, nil
		//return address.Undef, xerrors.Errorf("selecting address for add balance: %w", err)
	}
	return addr, nil
}

func (n *ProviderNodeAdapter) GetProofType(ctx context.Context, maddr address.Address, tok shared.TipSetToken) (abi.RegisteredSealProof, error) {
	tsk, err := types.TipSetKeyFromBytes(tok)
	if err != nil {
		return 0, err
	}

	mi, err := n.StateMinerInfo(ctx, maddr, tsk)
	if err != nil {
		return 0, err
	}

	nver, err := n.StateNetworkVersion(ctx, tsk)
	if err != nil {
		return 0, err
	}

	return miner.PreferredSealProofTypeFromWindowPoStType(nver, mi.WindowPoStProofType)
}

func (n *ProviderNodeAdapter) SignBytes(ctx context.Context, signer address.Address, b []byte) (*crypto.Signature, error) {
	signer, err := n.StateAccountKey(ctx, signer, types.EmptyTSK)
	if err != nil {
		return nil, err
	}

	localSignature, err := n.WalletSign(ctx, signer, b)
	if err != nil {
		return nil, err
	}
	return localSignature, nil
}

func (n *ProviderNodeAdapter) ReserveFunds(ctx context.Context, wallet, addr address.Address, amt abi.TokenAmount) (cid.Cid, error) {
	return n.MarketReserveFunds(ctx, wallet, addr, amt)
}

func (n *ProviderNodeAdapter) ReleaseFunds(ctx context.Context, addr address.Address, amt abi.TokenAmount) error {
	return n.MarketReleaseFunds(ctx, addr, amt)
}

// Adds funds with the StorageMinerActor for a storage participant.  Used by both providers and clients.
func (n *ProviderNodeAdapter) AddFunds(ctx context.Context, addr address.Address, amount abi.TokenAmount) (cid.Cid, error) {
	// (Provider Node API)
	smsg, err := n.MpoolPushMessage(ctx, &types.Message{
		To:     market.Address,
		From:   addr,
		Value:  amount,
		Method: market.Methods.AddBalance,
	}, n.addBalanceSpec)
	if err != nil {
		return cid.Undef, err
	}

	return smsg.Cid(), nil
}

func (n *ProviderNodeAdapter) GetBalance(ctx context.Context, addr address.Address, encodedTs shared.TipSetToken) (storagemarket.Balance, error) {
	tsk, err := types.TipSetKeyFromBytes(encodedTs)
	if err != nil {
		return storagemarket.Balance{}, err
	}

	bal, err := n.StateMarketBalance(ctx, addr, tsk)
	if err != nil {
		return storagemarket.Balance{}, err
	}

	return utils.ToSharedBalance(bal), nil
}

// TODO: why doesnt this method take in a sector ID?
func (n *ProviderNodeAdapter) LocatePieceForDealWithinSector(ctx context.Context, dealID abi.DealID, encodedTs shared.TipSetToken) (sectorID abi.SectorNumber, offset abi.PaddedPieceSize, length abi.PaddedPieceSize, err error) {
	refs, err := n.secb.GetRefs(ctx, dealID)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(refs) == 0 {
		return 0, 0, 0, xerrors.New("no sector information for deal ID")
	}

	// TODO: better strategy (e.g. look for already unsealed)
	var best api.SealedRef
	var bestSi api.SectorInfo
	for _, r := range refs {
		si, err := n.secb.SectorBuilder.SectorsStatus(ctx, r.SectorID, false)
		if err != nil {
			return 0, 0, 0, xerrors.Errorf("getting sector info: %w", err)
		}
		if si.State == api.SectorState(sealing.Proving) {
			best = r
			bestSi = si
			break
		}
	}
	if bestSi.State == api.SectorState(sealing.UndefinedSectorState) {
		return 0, 0, 0, xerrors.New("no sealed sector found")
	}
	return best.SectorID, best.Offset, best.Size.Padded(), nil
}

func (n *ProviderNodeAdapter) DealProviderCollateralBounds(ctx context.Context, size abi.PaddedPieceSize, isVerified bool) (abi.TokenAmount, abi.TokenAmount, error) {
	bounds, err := n.StateDealProviderCollateralBounds(ctx, size, isVerified, types.EmptyTSK)
	if err != nil {
		return abi.TokenAmount{}, abi.TokenAmount{}, err
	}

	// The maximum amount of collateral that the provider will put into escrow
	// for a deal is calculated as a multiple of the minimum bounded amount
	max := types.BigMul(bounds.Min, types.NewInt(n.maxDealCollateralMultiplier))

	return bounds.Min, max, nil
}

// TODO: Remove dealID parameter, change publishCid to be cid.Cid (instead of pointer)
func (n *ProviderNodeAdapter) OnDealSectorPreCommitted(ctx context.Context, provider address.Address, dealID abi.DealID, proposal market8.DealProposal, publishCid *cid.Cid, publishEpoch abi.ChainEpoch, cb storagemarket.DealSectorPreCommittedCallback) error {
	return n.scMgr.OnDealSectorPreCommitted(ctx, provider, proposal, *publishCid, publishEpoch, cb)
}

// TODO: Remove dealID parameter, change publishCid to be cid.Cid (instead of pointer)
func (n *ProviderNodeAdapter) OnDealSectorCommitted(ctx context.Context, provider address.Address, dealID abi.DealID, sectorNumber abi.SectorNumber, proposal market8.DealProposal, publishCid *cid.Cid, publishEpoch abi.ChainEpoch, cb storagemarket.DealSectorCommittedCallback) error {
	return n.scMgr.OnDealSectorCommitted(ctx, provider, sectorNumber, proposal, *publishCid, publishEpoch, cb)
}

func (n *ProviderNodeAdapter) GetChainHead(ctx context.Context) (shared.TipSetToken, abi.ChainEpoch, error) {
	head, err := n.ChainHead(ctx)
	if err != nil {
		return nil, 0, err
	}

	return head.Key().Bytes(), head.Height(), nil
}

func (n *ProviderNodeAdapter) WaitForMessage(ctx context.Context, mcid cid.Cid, cb func(code exitcode.ExitCode, bytes []byte, finalCid cid.Cid, err error) error) error {
	receipt, err := n.StateWaitMsg(ctx, mcid, 2*build.MessageConfidence, api.LookbackNoLimit, true)
	if err != nil {
		return cb(0, nil, cid.Undef, err)
	}
	return cb(receipt.Receipt.ExitCode, receipt.Receipt.Return, receipt.Message, nil)
}

func (n *ProviderNodeAdapter) WaitForPublishDeals(ctx context.Context, publishCid cid.Cid, proposal market8.DealProposal) (*storagemarket.PublishDealsWaitResult, error) {
	// Wait for deal to be published (plus additional time for confidence)
	receipt, err := n.StateWaitMsg(ctx, publishCid, 2*build.MessageConfidence, api.LookbackNoLimit, true)
	if err != nil {
		return nil, xerrors.Errorf("WaitForPublishDeals errored: %w", err)
	}
	if receipt.Receipt.ExitCode != exitcode.Ok {
		return nil, xerrors.Errorf("WaitForPublishDeals exit code: %s", receipt.Receipt.ExitCode)
	}

	// The deal ID may have changed since publish if there was a reorg, so
	// get the current deal ID
	head, err := n.ChainHead(ctx)
	if err != nil {
		return nil, xerrors.Errorf("WaitForPublishDeals failed to get chain head: %w", err)
	}

	res, err := n.scMgr.dealInfo.GetCurrentDealInfo(ctx, head.Key().Bytes(), &proposal, publishCid)
	if err != nil {
		return nil, xerrors.Errorf("WaitForPublishDeals getting deal info errored: %w", err)
	}

	return &storagemarket.PublishDealsWaitResult{DealID: res.DealID, FinalCid: receipt.Message, Height: receipt.Height}, nil
}

func (n *ProviderNodeAdapter) GetDataCap(ctx context.Context, addr address.Address, encodedTs shared.TipSetToken) (*abi.StoragePower, error) {
	tsk, err := types.TipSetKeyFromBytes(encodedTs)
	if err != nil {
		return nil, err
	}

	sp, err := n.StateVerifiedClientStatus(ctx, addr, tsk)
	return sp, err
}

func (n *ProviderNodeAdapter) OnDealExpiredOrSlashed(ctx context.Context, dealID abi.DealID, onDealExpired storagemarket.DealExpiredCallback, onDealSlashed storagemarket.DealSlashedCallback) error {
	head, err := n.ChainHead(ctx)
	if err != nil {
		return xerrors.Errorf("client: failed to get chain head: %w", err)
	}

	sd, err := n.StateMarketStorageDeal(ctx, dealID, head.Key())
	if err != nil {
		return xerrors.Errorf("client: failed to look up deal %d on chain: %w", dealID, err)
	}

	// Called immediately to check if the deal has already expired or been slashed
	checkFunc := func(ctx context.Context, ts *types.TipSet) (done bool, more bool, err error) {
		if ts == nil {
			// keep listening for events
			return false, true, nil
		}

		// Check if the deal has already expired
		if sd.Proposal.EndEpoch <= ts.Height() {
			onDealExpired(nil)
			return true, false, nil
		}

		// If there is no deal assume it's already been slashed
		if sd.State.SectorStartEpoch < 0 {
			onDealSlashed(ts.Height(), nil)
			return true, false, nil
		}

		// No events have occurred yet, so return
		// done: false, more: true (keep listening for events)
		return false, true, nil
	}

	// Called when there was a match against the state change we're looking for
	// and the chain has advanced to the confidence height
	stateChanged := func(ts *types.TipSet, ts2 *types.TipSet, states events.StateChange, h abi.ChainEpoch) (more bool, err error) {
		// Check if the deal has already expired
		if ts2 == nil || sd.Proposal.EndEpoch <= ts2.Height() {
			onDealExpired(nil)
			return false, nil
		}

		// Timeout waiting for state change
		if states == nil {
			log.Error("timed out waiting for deal expiry")
			return false, nil
		}

		changedDeals, ok := states.(state.ChangedDeals)
		if !ok {
			panic("Expected state.ChangedDeals")
		}

		deal, ok := changedDeals[dealID]
		if !ok {
			// No change to deal
			return true, nil
		}

		// Deal was slashed
		if deal.To == nil {
			onDealSlashed(ts2.Height(), nil)
			return false, nil
		}

		return true, nil
	}

	// Called when there was a chain reorg and the state change was reverted
	revert := func(ctx context.Context, ts *types.TipSet) error {
		// TODO: Is it ok to just ignore this?
		log.Warn("deal state reverted; TODO: actually handle this!")
		return nil
	}

	// Watch for state changes to the deal
	match := n.dsMatcher.matcher(ctx, dealID)

	// Wait until after the end epoch for the deal and then timeout
	timeout := (sd.Proposal.EndEpoch - head.Height()) + 1
	if err := n.ev.StateChanged(checkFunc, stateChanged, revert, int(build.MessageConfidence)+1, timeout, match); err != nil {
		return xerrors.Errorf("failed to set up state changed handler: %w", err)
	}

	return nil
}

var _ storagemarket.StorageProviderNode = &ProviderNodeAdapter{}
