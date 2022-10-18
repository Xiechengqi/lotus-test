package sealing

import (
	"bytes"
	"context"
	"github.com/filecoin-project/go-statemachine"
	"os"
	"strconv"
	"strings"

	"github.com/filecoin-project/go-state-types/proof"

	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-commp-utils/zerocomm"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/go-state-types/network"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/go-state-types/builtin/v8/miner"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/actors/policy"
)

var RecoveryingSectorPriority = 1025
var DealSectorPriority = 1024
var MaxTicketAge = policy.MaxPreCommitRandomnessLookback

func (m *Sealing) handleZeroPacking(ctx statemachine.Context, sector SectorInfo) error {
	m.inputLk.Lock()
	// make sure we not accepting deals into this sector
	for _, c := range m.assignedPieces[m.minerSectorID(sector.SectorNumber)] {
		pp := m.pendingPieces[c]
		delete(m.pendingPieces, c)
		if pp == nil {
			log.Errorf("nil assigned pending piece %s", c)
			continue
		}

		// todo: return to the sealing queue (this is extremely unlikely to happen)
		pp.accepted(sector.SectorNumber, 0, xerrors.Errorf("sector %d entered packing state early", sector.SectorNumber))
	}

	delete(m.openSectors, m.minerSectorID(sector.SectorNumber))
	delete(m.assignedPieces, m.minerSectorID(sector.SectorNumber))
	m.inputLk.Unlock()

	log.Infow("octopus: pack zero sector...", "sector", sector.SectorNumber)

	ssize, err := sector.SectorType.SectorSize()
	if err != nil {
		return err
	}

	existing := make([]abi.UnpaddedPieceSize, 0)
	fillerPieces := make([]abi.PieceInfo, 1)
	unpaddedSize := abi.PaddedPieceSize(ssize).Unpadded()
	ppi, err := m.sealer.AddPiece(
		sector.sealingCtx(ctx.Context()),
		m.minerSector(sector.SectorType, sector.SectorNumber),
		existing,
		unpaddedSize,
		NewNullReader(unpaddedSize),
	)
	if err != nil {
		return err
	}
	fillerPieces[0] = ppi

	if sector.Recovering {
		// if in recovering phase, go to p1 directly.
		log.Infof("octopus: recovering: SectorPackedForRecovery event")
		return ctx.Send(SectorPackedForRecovery{FillerPieces: fillerPieces})
	} else {
		return ctx.Send(SectorPacked{FillerPieces: fillerPieces})
	}
}

func (m *Sealing) handleRecover(ctx statemachine.Context, sector SectorInfo) error {
	if sector.hasDeals() {
		// TODO: consider how to support recover deal
		log.Error("octopus: recovering: unsupport recovery deal sector.")
		return nil
	} else {
		return ctx.Send(SectorCCRecovery{})
	}
}

func (m *Sealing) handlePacking(ctx statemachine.Context, sector SectorInfo) error {
	m.inputLk.Lock()
	// make sure we are not accepting deals into this sector
	for _, c := range m.assignedPieces[m.minerSectorID(sector.SectorNumber)] {
		pp := m.pendingPieces[c]
		delete(m.pendingPieces, c)
		if pp == nil {
			log.Errorf("nil assigned pending piece %s", c)
			continue
		}

		// todo: return to the sealing queue (this is extremely unlikely to happen)
		pp.accepted(sector.SectorNumber, 0, xerrors.Errorf("sector %d entered packing state early", sector.SectorNumber))
	}

	delete(m.openSectors, m.minerSectorID(sector.SectorNumber))
	delete(m.assignedPieces, m.minerSectorID(sector.SectorNumber))
	m.inputLk.Unlock()

	log.Infow("performing filling up rest of the sector...", "sector", sector.SectorNumber)

	var allocated abi.UnpaddedPieceSize
	for _, piece := range sector.Pieces {
		allocated += piece.Piece.Size.Unpadded()
	}

	ssize, err := sector.SectorType.SectorSize()
	if err != nil {
		return err
	}

	ubytes := abi.PaddedPieceSize(ssize).Unpadded()

	if allocated > ubytes {
		return xerrors.Errorf("too much data in sector: %d > %d", allocated, ubytes)
	}

	fillerSizes, err := fillersFromRem(ubytes - allocated)
	if err != nil {
		return err
	}

	if len(fillerSizes) > 0 {
		log.Warnf("Creating %d filler pieces for sector %d", len(fillerSizes), sector.SectorNumber)
	}

	fillerPieces, err := m.padSector(sector.sealingCtx(ctx.Context()), m.minerSector(sector.SectorType, sector.SectorNumber), sector.existingPieceSizes(), fillerSizes...)
	if err != nil {
		return xerrors.Errorf("filling up the sector (%v): %w", fillerSizes, err)
	}

	return ctx.Send(SectorPacked{FillerPieces: fillerPieces})
}

func (m *Sealing) padSector(ctx context.Context, sectorID storage.SectorRef, existingPieceSizes []abi.UnpaddedPieceSize, sizes ...abi.UnpaddedPieceSize) ([]abi.PieceInfo, error) {
	if len(sizes) == 0 {
		return nil, nil
	}

	log.Infof("Pledge %d, contains %+v", sectorID, existingPieceSizes)

	out := make([]abi.PieceInfo, len(sizes))
	for i, size := range sizes {
		expectCid := zerocomm.ZeroPieceCommitment(size)

		ppi, err := m.sealer.AddPiece(context.WithValue(ctx, "isCC", false), sectorID, existingPieceSizes, size, NewNullReader(size))
		if err != nil {
			return nil, xerrors.Errorf("add piece: %w", err)
		}
		if !expectCid.Equals(ppi.PieceCID) {
			return nil, xerrors.Errorf("got unexpected padding piece CID: expected:%s, got:%s", expectCid, ppi.PieceCID)
		}

		existingPieceSizes = append(existingPieceSizes, size)

		out[i] = ppi
	}

	return out, nil
}

func checkTicketExpired(ticket, head abi.ChainEpoch) bool {
	sealDuration := abi.ChainEpoch(0)
	sealDurationString := os.Getenv("P1P2_DURATION_EPOCHS")
	if sealDurationString != "" {
		sd, err := strconv.Atoi(sealDurationString)
		if err == nil {
			sealDuration = abi.ChainEpoch(sd)
		}
	}
	return head+sealDuration-ticket > MaxTicketAge // TODO: allow configuring expected seal durations
}

func checkProveCommitExpired(preCommitEpoch, msd abi.ChainEpoch, currEpoch abi.ChainEpoch) bool {
	return currEpoch > preCommitEpoch+msd
}

func (m *Sealing) getTicket(ctx statemachine.Context, sector SectorInfo) (abi.SealRandomness, abi.ChainEpoch, bool, error) {
	tok, epoch, err := m.Api.ChainHead(ctx.Context())
	if err != nil {
		log.Errorf("getTicket: api error, not proceeding: %+v", err)
		return nil, 0, false, nil
	}

	// the reason why the StateMinerSectorAllocated function is placed here, if it is outside,
	//	if the MarshalCBOR function and StateSectorPreCommitInfo function return err, it will be executed
	allocated, aerr := m.Api.StateMinerSectorAllocated(ctx.Context(), m.maddr, sector.SectorNumber, nil)
	if aerr != nil {
		log.Errorf("getTicket: api error, checking if sector is allocated: %+v", aerr)
		return nil, 0, false, nil
	}

	ticketEpoch := epoch - policy.SealRandomnessLookback
	buf := new(bytes.Buffer)
	if err := m.maddr.MarshalCBOR(buf); err != nil {
		return nil, 0, allocated, err
	}

	pci, err := m.Api.StateSectorPreCommitInfo(ctx.Context(), m.maddr, sector.SectorNumber, tok)
	if err != nil {
		return nil, 0, allocated, xerrors.Errorf("getting precommit info: %w", err)
	}

	if pci != nil {
		ticketEpoch = pci.Info.SealRandEpoch

		nv, err := m.Api.StateNetworkVersion(ctx.Context(), tok)
		if err != nil {
			return nil, 0, allocated, xerrors.Errorf("getTicket: StateNetworkVersion: api error, not proceeding: %+v", err)
		}

		av, err := actors.VersionForNetwork(nv)
		if err != nil {
			return nil, 0, allocated, xerrors.Errorf("getTicket: actor version for network error, not proceeding: %w", err)
		}
		msd, err := policy.GetMaxProveCommitDuration(av, sector.SectorType)
		if err != nil {
			return nil, 0, allocated, xerrors.Errorf("getTicket: max prove commit duration policy error, not proceeding: %w", err)
		}

		if checkProveCommitExpired(pci.PreCommitEpoch, msd, epoch) {
			return nil, 0, allocated, xerrors.Errorf("ticket expired for precommitted sector")
		}
	}

	if pci == nil && allocated { // allocated is true, sector precommitted but expired, will SectorCommitFailed or SectorRemove
		return nil, 0, allocated, xerrors.Errorf("sector %s precommitted but expired", sector.SectorNumber)
	}

	rand, err := m.Api.StateGetRandomnessFromTickets(ctx.Context(), crypto.DomainSeparationTag_SealRandomness, ticketEpoch, buf.Bytes(), tok)
	if err != nil {
		return nil, 0, allocated, err
	}

	return abi.SealRandomness(rand), ticketEpoch, allocated, nil
}

func (m *Sealing) handleGetTicket(ctx statemachine.Context, sector SectorInfo) error {
	ticketValue, ticketEpoch, allocated, err := m.getTicket(ctx, sector)
	if err != nil {
		if allocated {
			if sector.CommitMessage != nil {
				// Some recovery paths with unfortunate timing lead here
				return ctx.Send(SectorCommitFailed{xerrors.Errorf("sector %s is committed but got into the GetTicket state", sector.SectorNumber)})
			}

			log.Errorf("Sector %s precommitted but expired", sector.SectorNumber)
			return ctx.Send(SectorRemove{})
		}

		return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("getting ticket failed: %w", err)})
	}

	return ctx.Send(SectorTicket{
		TicketValue: ticketValue,
		TicketEpoch: ticketEpoch,
	})
}

func (m *Sealing) handlePreCommit1(ctx statemachine.Context, sector SectorInfo) error {
	if err := checkPieces(ctx.Context(), m.maddr, sector, m.Api, false); err != nil { // Sanity check state
		switch err.(type) {
		case *ErrApi:
			log.Errorf("handlePreCommit1: api error, not proceeding: %+v", err)
			return nil
		case *ErrInvalidDeals:
			log.Warnf("invalid deals in sector %d: %v", sector.SectorNumber, err)
			return ctx.Send(SectorInvalidDealIDs{Return: RetPreCommit1})
		case *ErrExpiredDeals: // Probably not much we can do here, maybe re-pack the secto10r?
			return ctx.Send(SectorDealsExpired{xerrors.Errorf("expired dealIDs in sector: %w", err)})
		default:
			return xerrors.Errorf("checkPieces sanity check error: %w", err)
		}
	}

	tok, height, err := m.Api.ChainHead(ctx.Context())
	if err != nil {
		log.Errorf("handlePreCommit1: api error, not proceeding: %+v", err)
		return nil
	}

	//if sector.SectorNumber == abi.SectorNumber(253) || sector.SectorNumber == abi.SectorNumber(254) || sector.SectorNumber == abi.SectorNumber(255) || sector.SectorNumber == abi.SectorNumber(256) || sector.SectorNumber == abi.SectorNumber(257) || sector.SectorNumber == abi.SectorNumber(258) || sector.SectorNumber == abi.SectorNumber(259) {
	//	log.Infof("sector %v ignore ticket check", sector.SectorNumber)
	//	pc1o, err := m.sealer.SealPreCommit1(sector.sealingCtx(ctx.Context()), m.minerSector(sector.SectorType, sector.SectorNumber), sector.TicketValue, sector.pieceInfos())
	//	if err != nil {
	//		return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("seal pre commit(1) failed: %w", err)})
	//	}
	//
	//	return ctx.Send(SectorPreCommit1{
	//		PreCommit1Out: pc1o,
	//	})
	//}
	if checkTicketExpired(sector.TicketEpoch, height) {
		if !sector.Recovering {
			pci, err := m.Api.StateSectorPreCommitInfo(ctx.Context(), m.maddr, sector.SectorNumber, tok)
			if err != nil {
				log.Errorf("handlePreCommit1: StateSectorPreCommitInfo: api error, not proceeding: %+v", err)
				return nil
			}

			if pci == nil {
				return ctx.Send(SectorOldTicket{}) // go get new ticket
			}

			nv, err := m.Api.StateNetworkVersion(ctx.Context(), tok)
			if err != nil {
				log.Errorf("handlePreCommit1: StateNetworkVersion: api error, not proceeding: %+v", err)
				return nil
			}

			av, err := actors.VersionForNetwork(nv)
			if err != nil {
				log.Errorf("handlePreCommit1: VersionForNetwork error, not proceeding: %w", err)
				return nil
			}
			msd, err := policy.GetMaxProveCommitDuration(av, sector.SectorType)
			if err != nil {
				log.Errorf("handlePreCommit1: GetMaxProveCommitDuration error, not proceeding: %w", err)
				return nil
			}

			// if height >  PreCommitEpoch + msd, there is no need to recalculate
			if checkProveCommitExpired(pci.PreCommitEpoch, msd, height) {
				return ctx.Send(SectorOldTicket{}) // will be removed
			}
		} else {
			// 恢复时忽略expire过期。
			log.Infof("octopus: recovering: sector %v is expired. continue to p1", sector.SectorNumber)
		}
	}

	pc1o, err := m.sealer.SealPreCommit1(sector.sealingCtx(ctx.Context()), m.minerSector(sector.SectorType, sector.SectorNumber), sector.TicketValue, sector.pieceInfos())
	if err != nil {
		return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("seal pre commit(1) failed: %w", err)})
	}

	return ctx.Send(SectorPreCommit1{
		PreCommit1Out: pc1o,
	})
}

func (m *Sealing) handlePreCommit2(ctx statemachine.Context, sector SectorInfo) error {
	cids, err := m.sealer.SealPreCommit2(sector.sealingCtx(ctx.Context()), m.minerSector(sector.SectorType, sector.SectorNumber), sector.PreCommit1Out)
	if err != nil {
		if strings.Contains(err.Error(), "check rounds") {
			log.Errorf("octopus: sector %v: verify PreCommit2 result error: %v", sector.SectorNumber, err)
			return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("seal pre commit(2) verify failed: %w", err)})
		}
		return ctx.Send(SectorSealPreCommit2Failed{xerrors.Errorf("seal pre commit(2) failed: %w", err)})
	}

	if cids.Unsealed == cid.Undef {
		return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("seal pre commit(2) returned undefined CommD")})
	}

	// log.Infof("sector %v p2 unsealedCid=%v sealedCid=%v", sector.SectorNumber, cids.Unsealed, cids.Sealed)
	// err = m.verifyPreCommit2Result(ctx, sector, cids)
	// if err != nil {
	// 	log.Errorf("octopus: sector %v: verify PreCommit2 result error: %v", sector.SectorNumber, err)
	// 	return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("seal pre commit(2) verify failed: %w", err)})
	// }

	//if sector.SectorNumber == abi.SectorNumber(253) || sector.SectorNumber == abi.SectorNumber(254) || sector.SectorNumber == abi.SectorNumber(255) || sector.SectorNumber == abi.SectorNumber(256) || sector.SectorNumber == abi.SectorNumber(257) || sector.SectorNumber == abi.SectorNumber(258) || sector.SectorNumber == abi.SectorNumber(259) {
	//	log.Infof("sector %v p2 finish. wait", sector.SectorNumber)
	//	return nil
	//}

	// 检查恢复是否和链上一致。
	if sector.Recovering {
		sealedCID, err := m.getOnChainSealedCid(ctx.Context(), sector.SectorNumber)
		if err != nil {
			return ctx.Send(SectorSealPreCommit2Failed{xerrors.Errorf("seal pre commit(2) recover failed, can not get onchain info, %+v", err)})
		}
		if cids.Sealed != sealedCID {
			return ctx.Send(SectorSealPreCommit2Failed{xerrors.Errorf("seal pre commit(2) recover failed, cids are not the same, require %v, got %v", sealedCID, cids.Sealed)})
		} else {
			return ctx.Send(SectorRecovered{})
		}
	}

	return ctx.Send(SectorPreCommit2{
		Unsealed: cids.Unsealed,
		Sealed:   cids.Sealed,
	})
}

// TODO: 仅支持Proving后的扇
func (m *Sealing) getOnChainSealedCid(ctx context.Context, sectorId abi.SectorNumber) (cid.Cid, error) {
	tok, _, err := m.Api.ChainHead(ctx)
	if err != nil {
		return cid.Undef, err
	}

	onChainInfo, err := m.Api.StateSectorGetInfo(ctx, m.maddr, sectorId, tok)
	if err != nil {
		return cid.Undef, err
	}
	return onChainInfo.SealedCID, nil
}
func (m *Sealing) preCommitParams(ctx statemachine.Context, sector SectorInfo) (*miner.SectorPreCommitInfo, big.Int, TipSetToken, error) {
	tok, height, err := m.Api.ChainHead(ctx.Context())
	if err != nil {
		log.Errorf("handlePreCommitting: api error, not proceeding: %+v", err)
		return nil, big.Zero(), nil, nil
	}

	if err := checkPrecommit(ctx.Context(), m.Address(), sector, tok, height, m.Api); err != nil {
		switch err := err.(type) {
		case *ErrApi:
			log.Errorf("handlePreCommitting: api error, not proceeding: %+v", err)
			return nil, big.Zero(), nil, nil
		case *ErrBadCommD: // TODO: Should this just back to packing? (not really needed since handlePreCommit1 will do that too)
			return nil, big.Zero(), nil, ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("bad CommD error: %w", err)})
		case *ErrExpiredTicket:
			return nil, big.Zero(), nil, ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("ticket expired: %w", err)})
		case *ErrBadTicket:
			return nil, big.Zero(), nil, ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("bad ticket: %w", err)})
		case *ErrInvalidDeals:
			log.Warnf("invalid deals in sector %d: %v", sector.SectorNumber, err)
			return nil, big.Zero(), nil, ctx.Send(SectorInvalidDealIDs{Return: RetPreCommitting})
		case *ErrExpiredDeals:
			return nil, big.Zero(), nil, ctx.Send(SectorDealsExpired{xerrors.Errorf("sector deals expired: %w", err)})
		case *ErrPrecommitOnChain:
			return nil, big.Zero(), nil, ctx.Send(SectorPreCommitLanded{TipSet: tok}) // we re-did precommit
		case *ErrSectorNumberAllocated:
			log.Errorf("handlePreCommitFailed: sector number already allocated, not proceeding: %+v", err)
			// TODO: check if the sector is committed (not sure how we'd end up here)
			return nil, big.Zero(), nil, nil
		default:
			return nil, big.Zero(), nil, xerrors.Errorf("checkPrecommit sanity check error: %w", err)
		}
	}

	expiration, err := m.pcp.Expiration(ctx.Context(), sector.Pieces...)
	if err != nil {
		return nil, big.Zero(), nil, ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("handlePreCommitting: failed to compute pre-commit expiry: %w", err)})
	}

	nv, err := m.Api.StateNetworkVersion(ctx.Context(), tok)
	if err != nil {
		return nil, big.Zero(), nil, ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("failed to get network version: %w", err)})
	}

	av, err := actors.VersionForNetwork(nv)
	if err != nil {
		return nil, big.Zero(), nil, ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("failed to get actors version: %w", err)})
	}
	msd, err := policy.GetMaxProveCommitDuration(av, sector.SectorType)
	if err != nil {
		return nil, big.Zero(), nil, ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("failed to get max prove commit duration: %w", err)})
	}

	if minExpiration := sector.TicketEpoch + policy.MaxPreCommitRandomnessLookback + msd + miner.MinSectorExpiration; expiration < minExpiration {
		expiration = minExpiration
	}

	// Assume: both precommit msg & commit msg land on chain as early as possible
	maxExpiration := height + policy.GetPreCommitChallengeDelay() + policy.GetMaxSectorExpirationExtension()
	if expiration > maxExpiration {
		expiration = maxExpiration
	}

	params := &miner.SectorPreCommitInfo{
		Expiration:   expiration,
		SectorNumber: sector.SectorNumber,
		SealProof:    sector.SectorType,

		SealedCID:     *sector.CommR,
		SealRandEpoch: sector.TicketEpoch,
		DealIDs:       sector.dealIDs(),
	}

	collateral, err := m.Api.StateMinerPreCommitDepositForPower(ctx.Context(), m.maddr, *params, tok)
	if err != nil {
		return nil, big.Zero(), nil, xerrors.Errorf("getting initial pledge collateral: %w", err)
	}

	return params, collateral, tok, nil
}

func (m *Sealing) handlePreCommitting(ctx statemachine.Context, sector SectorInfo) error {
	cfg, err := m.getConfig()
	if err != nil {
		return xerrors.Errorf("getting config: %w", err)
	}

	if cfg.BatchPreCommits {
		nv, err := m.Api.StateNetworkVersion(ctx.Context(), nil)
		if err != nil {
			return xerrors.Errorf("getting network version: %w", err)
		}

		if nv >= network.Version13 {
			return ctx.Send(SectorPreCommitBatch{})
		}
	}

	params, pcd, tok, err := m.preCommitParams(ctx, sector)
	if err != nil {
		return ctx.Send(SectorChainPreCommitFailed{xerrors.Errorf("preCommitParams: %w", err)})
	}
	if params == nil {
		return nil // event was sent in preCommitParams
	}

	deposit, err := collateralSendAmount(ctx.Context(), m.Api, m.maddr, cfg, pcd)
	if err != nil {
		return err
	}

	enc := new(bytes.Buffer)
	if err := params.MarshalCBOR(enc); err != nil {
		return ctx.Send(SectorChainPreCommitFailed{xerrors.Errorf("could not serialize pre-commit sector parameters: %w", err)})
	}

	mi, err := m.Api.StateMinerInfo(ctx.Context(), m.maddr, tok)
	if err != nil {
		log.Errorf("handlePreCommitting: api error, not proceeding: %+v", err)
		return nil
	}

	goodFunds := big.Add(deposit, big.Int(m.feeCfg.MaxPreCommitGasFee))

	from, _, err := m.addrSel(ctx.Context(), mi, api.PreCommitAddr, goodFunds, deposit)
	if err != nil {
		return ctx.Send(SectorChainPreCommitFailed{xerrors.Errorf("no good address to send precommit message from: %w", err)})
	}

	log.Infof("submitting precommit for sector %d (deposit: %s): ", sector.SectorNumber, deposit)
	mcid, err := m.Api.SendMsg(ctx.Context(), from, m.maddr, builtin.MethodsMiner.PreCommitSector, deposit, big.Int(m.feeCfg.MaxPreCommitGasFee), enc.Bytes())
	if err != nil {
		return ctx.Send(SectorChainPreCommitFailed{xerrors.Errorf("pushing message to mpool: %w", err)})
	}

	return ctx.Send(SectorPreCommitted{Message: mcid, PreCommitDeposit: pcd, PreCommitInfo: *params})
}

func (m *Sealing) handleSubmitPreCommitBatch(ctx statemachine.Context, sector SectorInfo) error {
	if sector.CommD == nil || sector.CommR == nil {
		return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("sector had nil commR or commD")})
	}

	params, deposit, _, err := m.preCommitParams(ctx, sector)
	if err != nil {
		return ctx.Send(SectorChainPreCommitFailed{xerrors.Errorf("preCommitParams: %w", err)})
	}
	if params == nil {
		return nil // event was sent in preCommitParams
	}

	res, err := m.precommiter.AddPreCommit(ctx.Context(), sector, deposit, params)
	if err != nil {
		return ctx.Send(SectorChainPreCommitFailed{xerrors.Errorf("queuing precommit batch failed: %w", err)})
	}

	if res.Error != "" {
		return ctx.Send(SectorChainPreCommitFailed{xerrors.Errorf("precommit batch error: %s", res.Error)})
	}

	if res.Msg == nil {
		return ctx.Send(SectorChainPreCommitFailed{xerrors.Errorf("batch message was nil")})
	}

	return ctx.Send(SectorPreCommitBatchSent{*res.Msg})
}

func (m *Sealing) handlePreCommitWait(ctx statemachine.Context, sector SectorInfo) error {
	if sector.PreCommitMessage == nil {
		return ctx.Send(SectorChainPreCommitFailed{xerrors.Errorf("precommit message was nil")})
	}

	// would be ideal to just use the events.Called handler, but it wouldn't be able to handle individual message timeouts
	log.Info("Sector precommitted: ", sector.SectorNumber)
	mw, err := m.Api.StateWaitMsg(ctx.Context(), *sector.PreCommitMessage)
	if err != nil {
		return ctx.Send(SectorChainPreCommitFailed{err})
	}

	switch mw.Receipt.ExitCode {
	case exitcode.Ok:
		// this is what we expect
	case exitcode.SysErrInsufficientFunds:
		fallthrough
	case exitcode.SysErrOutOfGas:
		// gas estimator guessed a wrong number / out of funds:
		return ctx.Send(SectorRetryPreCommit{})
	default:
		log.Error("sector precommit failed: ", mw.Receipt.ExitCode)
		err := xerrors.Errorf("sector precommit failed: %d", mw.Receipt.ExitCode)
		return ctx.Send(SectorChainPreCommitFailed{err})
	}

	log.Info("precommit message landed on chain: ", sector.SectorNumber)

	return ctx.Send(SectorPreCommitLanded{TipSet: mw.TipSetTok})
}

func (m *Sealing) handleWaitSeed(ctx statemachine.Context, sector SectorInfo) error {
	tok, _, err := m.Api.ChainHead(ctx.Context())
	if err != nil {
		log.Errorf("handleWaitSeed: api error, not proceeding: %+v", err)
		return nil
	}

	pci, err := m.Api.StateSectorPreCommitInfo(ctx.Context(), m.maddr, sector.SectorNumber, tok)
	if err != nil {
		log.Errorf("octopus: getting precommit info: %w", err)
		return xerrors.Errorf("getting precommit info: %w", err)
	}
	if pci == nil {
		log.Error("octopus: precommit info not found on chain")
		return ctx.Send(SectorChainPreCommitFailed{error: xerrors.Errorf("precommit info not found on chain")})
	}

	randHeight := pci.PreCommitEpoch + policy.GetPreCommitChallengeDelay()

	err = m.events.ChainAt(func(ectx context.Context, _ TipSetToken, curH abi.ChainEpoch) error {
		log.Infof("octopus: %v: sector no %v: ChainAt Handle", sector.SectorNumber, curH)
		// in case of null blocks the randomness can land after the tipset we
		// get from the events API
		tok, _, err := m.Api.ChainHead(ctx.Context())
		if err != nil {
			log.Errorf("handleWaitSeed: api error, not proceeding: %+v", err)
			log.Errorf("handleWaitSeed: sector no %v: api error, not proceeding: %+v", sector.SectorNumber, err)
			return nil
		}

		buf := new(bytes.Buffer)
		if err := m.maddr.MarshalCBOR(buf); err != nil {
			return err
		}
		rand, err := m.Api.StateGetRandomnessFromBeacon(ectx, crypto.DomainSeparationTag_InteractiveSealChallengeSeed, randHeight, buf.Bytes(), tok)
		if err != nil {
			err = xerrors.Errorf("failed to get randomness for computing seal proof (ch %d; rh %d; tsk %x): %w", curH, randHeight, tok, err)
			log.Errorf("octopus: sector no %v: failed to get randomness for computing seal proof (ch %d; rh %d; tsk %x): %w", sector.SectorNumber, curH, randHeight, tok, err)

			_ = ctx.Send(SectorChainPreCommitFailed{error: err})
			return err
		}

		log.Debugf("octopus: sector no %v: send SectorSeedReady", sector.SectorNumber)
		_ = ctx.Send(SectorSeedReady{SeedValue: abi.InteractiveSealRandomness(rand), SeedEpoch: randHeight})

		return nil
	}, func(ctx context.Context, ts TipSetToken) error {
		log.Warn("revert in interactive commit sector step")
		log.Warnf("octopus: sector no %v: revert in interactive commit sector step", sector.SectorNumber)
		// TODO: need to cancel running process and restart...
		return nil
	}, InteractivePoRepConfidence, randHeight)
	if err != nil {
		log.Warn("waitForPreCommitMessage ChainAt errored: ", err)
		log.Warnf("octopus: sector no %v: waitForPreCommitMessage ChainAt errored: ", sector.SectorNumber, err)
	}

	return nil
}

func (m *Sealing) handleCommitting(ctx statemachine.Context, sector SectorInfo) error {
	if sector.CommitMessage != nil {
		log.Warnf("sector %d entered committing state with a commit message cid", sector.SectorNumber)

		ml, err := m.Api.StateSearchMsg(ctx.Context(), *sector.CommitMessage)
		if err != nil {
			log.Warnf("sector %d searching existing commit message %s: %+v", sector.SectorNumber, *sector.CommitMessage, err)
		}

		if ml != nil {
			// some weird retry paths can lead here
			return ctx.Send(SectorRetryCommitWait{})
		}
	}

	cfg, err := m.getConfig()
	if err != nil {
		return xerrors.Errorf("getting config: %w", err)
	}

	log.Info("scheduling seal proof computation...")

	log.Infof("KOMIT %d %x(%d); %x(%d); %v; r:%s; d:%s", sector.SectorNumber, sector.TicketValue, sector.TicketEpoch, sector.SeedValue, sector.SeedEpoch, sector.pieceInfos(), sector.CommR, sector.CommD)

	if sector.CommD == nil || sector.CommR == nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("sector had nil commR or commD")})
	}

	cids := storage.SectorCids{
		Unsealed: *sector.CommD,
		Sealed:   *sector.CommR,
	}
	c2in, err := m.sealer.SealCommit1(sector.sealingCtx(ctx.Context()), m.minerSector(sector.SectorType, sector.SectorNumber), sector.TicketValue, sector.SeedValue, sector.pieceInfos(), cids)
	if err != nil {
		//return ctx.Send(SectorComputeProofFailed{xerrors.Errorf("computing seal proof failed(1): %w", err)})
		return ctx.Send(SectorCommit1Failed{xerrors.Errorf("computing seal proof failed(1): %w", err)})
	}

	proof, err := m.sealer.SealCommit2(sector.sealingCtx(ctx.Context()), m.minerSector(sector.SectorType, sector.SectorNumber), c2in)
	if err != nil {
		return ctx.Send(SectorComputeProofFailed{xerrors.Errorf("computing seal proof failed(2): %w", err)})
	}

	{
		tok, _, err := m.Api.ChainHead(ctx.Context())
		if err != nil {
			log.Errorf("handleCommitting: api error, not proceeding: %+v", err)
			return nil
		}

		if err := m.checkCommit(ctx.Context(), sector, proof, tok); err != nil {
			switch err.(type) {
			case *ErrUnmatchCommR:
				log.Errorf("octopus: checkCommit failed: unmatch commR, remove sector %v.", sector.SectorNumber)
				return ctx.Send(SectorRemove{})
			default:
				return ctx.Send(SectorCommitFailed{xerrors.Errorf("commit check error: %w", err)})
			}
		}
	}

	if cfg.FinalizeEarly {
		return ctx.Send(SectorProofReady{
			Proof: proof,
		})
	}

	return ctx.Send(SectorCommitted{
		Proof: proof,
	})
}

func (m *Sealing) handleSubmitCommit(ctx statemachine.Context, sector SectorInfo) error {
	cfg, err := m.getConfig()
	if err != nil {
		return xerrors.Errorf("getting config: %w", err)
	}

	if cfg.AggregateCommits {
		nv, err := m.Api.StateNetworkVersion(ctx.Context(), nil)
		if err != nil {
			return xerrors.Errorf("getting network version: %w", err)
		}

		if nv >= network.Version13 {
			return ctx.Send(SectorSubmitCommitAggregate{})
		}
	}

	tok, _, err := m.Api.ChainHead(ctx.Context())
	if err != nil {
		log.Errorf("handleSubmitCommit: api error, not proceeding: %+v", err)
		return nil
	}

	if err := m.checkCommit(ctx.Context(), sector, sector.Proof, tok); err != nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("commit check error: %w", err)})
	}

	enc := new(bytes.Buffer)
	params := &miner.ProveCommitSectorParams{
		SectorNumber: sector.SectorNumber,
		Proof:        sector.Proof,
	}

	if err := params.MarshalCBOR(enc); err != nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("could not serialize commit sector parameters: %w", err)})
	}

	mi, err := m.Api.StateMinerInfo(ctx.Context(), m.maddr, tok)
	if err != nil {
		log.Errorf("handleCommitting: api error, not proceeding: %+v", err)
		return nil
	}

	pci, err := m.Api.StateSectorPreCommitInfo(ctx.Context(), m.maddr, sector.SectorNumber, tok)
	if err != nil {
		return xerrors.Errorf("getting precommit info: %w", err)
	}
	if pci == nil {
		return ctx.Send(SectorCommitFailed{error: xerrors.Errorf("precommit info not found on chain")})
	}

	collateral, err := m.Api.StateMinerInitialPledgeCollateral(ctx.Context(), m.maddr, pci.Info, tok)
	if err != nil {
		return xerrors.Errorf("getting initial pledge collateral: %w", err)
	}

	collateral = big.Sub(collateral, pci.PreCommitDeposit)
	if collateral.LessThan(big.Zero()) {
		collateral = big.Zero()
	}

	collateral, err = collateralSendAmount(ctx.Context(), m.Api, m.maddr, cfg, collateral)
	if err != nil {
		return err
	}

	goodFunds := big.Add(collateral, big.Int(m.feeCfg.MaxCommitGasFee))

	from, _, err := m.addrSel(ctx.Context(), mi, api.CommitAddr, goodFunds, collateral)
	if err != nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("no good address to send commit message from: %w", err)})
	}

	// TODO: check seed / ticket / deals are up to date
	mcid, err := m.Api.SendMsg(ctx.Context(), from, m.maddr, builtin.MethodsMiner.ProveCommitSector, collateral, big.Int(m.feeCfg.MaxCommitGasFee), enc.Bytes())
	if err != nil {
		if strings.Contains(err.Error(), "failed to submit proof for bulk verification") {
			log.Errorf("octopus: may be caused by 'attempting to prove commit over 200 sectors in epoch', retry sumbit commit")
			return ctx.Send(SectorCommitSubmitFailed{err})
		}
		if strings.Contains(err.Error(), "too many pending prove message") {
			log.Errorf("octopus: may be caused by 'too many pending prove message', retry sumbit commit")
			return ctx.Send(SectorCommitSubmitFailed{err})
		}
		if strings.Contains(err.Error(), "websocket connection closed") {
			return ctx.Send(SectorCommitSubmitFailed{err})
		}
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("pushing message to mpool: %w", err)})
	}

	return ctx.Send(SectorCommitSubmitted{
		Message: mcid,
	})
}

func (m *Sealing) handleSubmitCommitAggregate(ctx statemachine.Context, sector SectorInfo) error {
	if sector.CommD == nil || sector.CommR == nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("sector had nil commR or commD")})
	}

	res, err := m.commiter.AddCommit(ctx.Context(), sector, AggregateInput{
		Info: proof.AggregateSealVerifyInfo{
			Number:                sector.SectorNumber,
			Randomness:            sector.TicketValue,
			InteractiveRandomness: sector.SeedValue,
			SealedCID:             *sector.CommR,
			UnsealedCID:           *sector.CommD,
		},
		Proof: sector.Proof, // todo: this correct??
		Spt:   sector.SectorType,
	})

	if err != nil || res.Error != "" {
		tok, _, err := m.Api.ChainHead(ctx.Context())
		if err != nil {
			log.Errorf("handleSubmitCommit: api error, not proceeding: %+v", err)
			return nil
		}

		if err := m.checkCommit(ctx.Context(), sector, sector.Proof, tok); err != nil {
			return ctx.Send(SectorCommitFailed{xerrors.Errorf("commit check error: %w", err)})
		}

		return ctx.Send(SectorRetrySubmitCommit{})
	}

	if e, found := res.FailedSectors[sector.SectorNumber]; found {
		if strings.Contains(e, "failed to submit proof for bulk verification") {
			log.Errorf("octopus: may be caused by 'attempting to prove commit over 200 sectors in epoch', retry sumbit commit")
			return ctx.Send(SectorCommitSubmitFailed{xerrors.Errorf("sector failed in aggregate processing: %s", e)})
		}
		if strings.Contains(e, "too many pending prove message") {
			log.Errorf("octopus: may be caused by 'too many pending prove message', retry sumbit commit")
			return ctx.Send(SectorCommitSubmitFailed{xerrors.Errorf("sector failed in aggregate processing: %s", e)})
		}
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("sector failed in aggregate processing: %s", e)})
	}

	if res.Msg == nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("aggregate message was nil")})
	}

	return ctx.Send(SectorCommitAggregateSent{*res.Msg})
}

func (m *Sealing) handleCommitWait(ctx statemachine.Context, sector SectorInfo) error {
	if sector.CommitMessage == nil {
		log.Errorf("sector %d entered commit wait state without a message cid", sector.SectorNumber)
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("entered commit wait with no commit cid")})
	}

	mw, err := m.Api.StateWaitMsg(ctx.Context(), *sector.CommitMessage)
	if err != nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("failed to wait for porep inclusion: %w", err)})
	}

	switch mw.Receipt.ExitCode {
	case exitcode.Ok:
		// this is what we expect
	case exitcode.SysErrInsufficientFunds:
		fallthrough
	case exitcode.FirstActorSpecificExitCode:
		fallthrough
	case exitcode.SysErrOutOfGas:
		// gas estimator guessed a wrong number / out of funds
		return ctx.Send(SectorRetrySubmitCommit{})
	default:
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("submitting sector proof failed (exit=%d, msg=%s) (t:%x; s:%x(%d); p:%x)", mw.Receipt.ExitCode, sector.CommitMessage, sector.TicketValue, sector.SeedValue, sector.SeedEpoch, sector.Proof)})
	}

	si, err := m.Api.StateSectorGetInfo(ctx.Context(), m.maddr, sector.SectorNumber, mw.TipSetTok)
	if err != nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("proof validation failed, calling StateSectorGetInfo: %w", err)})
	}
	if si == nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("proof validation failed, sector not found in sector set after cron")})
	}

	return ctx.Send(SectorProving{})
}

func (m *Sealing) handleFinalizeSector(ctx statemachine.Context, sector SectorInfo) error {
	// TODO: Maybe wait for some finality

	cfg, err := m.getConfig()
	if err != nil {
		return xerrors.Errorf("getting sealing config: %w", err)
	}

	if sector.FinalizedTimes == 0 {
		if err := m.sealer.FinalizeSector(sector.sealingCtx(ctx.Context()), m.minerSector(sector.SectorType, sector.SectorNumber), sector.keepUnsealedRanges(sector.Pieces, false, cfg.AlwaysKeepUnsealedCopy)); err != nil {
			return ctx.Send(SectorFinalizeFailed{xerrors.Errorf("finalize sector: %w", err)})
		}
	} else {
		log.Infof("octopus: sector %v has been finalized %d times, skip.", sector.SectorNumber, sector.FinalizedTimes)
	}
	if cfg.MakeCCSectorsAvailable && !sector.hasDeals() {
		return ctx.Send(SectorFinalizedAvailable{})
	}

	return ctx.Send(SectorFinalized{})
}
