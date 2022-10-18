package octopus

import (
	"bufio"
	"context"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v8/miner"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/dline"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	apitypes "github.com/filecoin-project/lotus/api/types"
	"github.com/filecoin-project/lotus/api/v1api"
	lminer "github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/chain/types"
	cliutil "github.com/filecoin-project/lotus/cli/util"
	"github.com/filecoin-project/lotus/node/repo"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// best practise:
// for sake of performance, if enable automatic switch, do not remove/set current in the same time.
// for sake of performance, if enable automatic switch, list may delay if auto switch happens in the same time.
// for automatic switch, smaller for winning(<30), larger for sealing(<5min), medium for post(<2min)?

// fullnodes file is used to load/store fullnodes state.
// file format:
// <auth api info>
// <auth api info> X
const fullnodesFile = "fullnodes"

var log = logging.Logger("octopus")

type FullNodePool struct {
	fnapiinfo []string                        // api info string is used to connect daemon directly.
	fnnodes   map[string]v1api.FullNode       // key is apiinfo
	fnclosers map[string]jsonrpc.ClientCloser // key is apiinfo
	current   int                             // current daemon index in fnapiinfo in use
	lk        sync.RWMutex
	stop      chan struct{}

	repo string // repo path is loaded during initialization when cli context is available.
}

func NewFullNodePool(
	clictx *cli.Context,
	ctx context.Context,
	apiinfo string,
	api v1api.FullNode,
	closer jsonrpc.ClientCloser,
) *FullNodePool {
	fnp := FullNodePool{
		fnapiinfo: []string{apiinfo},
		current:   0,
		stop:      make(chan struct{}),
	}
	fnp.fnnodes = make(map[string]v1api.FullNode)
	fnp.fnnodes[apiinfo] = api

	fnp.fnclosers = make(map[string]jsonrpc.ClientCloser)
	fnp.fnclosers[apiinfo] = closer

	fnp.initRepoPath(clictx)

	fnp.load(ctx)

	// TODO: is this context appropriate.
	go fnp.Start(ctx)

	return &fnp
}

func (fnp *FullNodePool) initRepoPath(clictx *cli.Context) {
	repoFlags := cliutil.FlagsForRepo(repo.StorageMiner)
	for _, f := range repoFlags {
		path := clictx.String(f)
		if path == "" {
			continue
		}

		p, err := homedir.Expand(path)
		if err != nil {
			log.Warnf("load fullnodes error: could not expand home dir (%s): %v", f, err)
			continue
		}

		fnp.repo = p
		break
	}
	// in any case repo is empty, load/store from fullnodes file will be skipped.
}

// skip store if there is any error. no lock required since it's lock in specific actions-remove/add/current.
// TODO: consider
// 1. if store() is better to be called asynchronizely.
// 2. if store() is better to be moved out of lock.
func (fnp *FullNodePool) store() {
	if fnp.repo != "" {
		fnf := filepath.Join(fnp.repo, fullnodesFile)
		var file *os.File
		var err error
		if file, err = os.Create(fnf); err != nil {
			log.Errorf("Create/Open %s error: %v", fnf, err)
			return
		}
		defer file.Close()

		for i, ai := range fnp.fnapiinfo {
			file.WriteString(ai)
			if i == fnp.current {
				file.WriteString(" X")
			}
			file.WriteString("\n")
		}
	}
}

func (fnp *FullNodePool) load(ctx context.Context) {
	if fnp.repo != "" {
		fnf := filepath.Join(fnp.repo, fullnodesFile)
		file, err := os.Open(fnf)
		if os.IsNotExist(err) {
			log.Warnf("load fullnodes failed: %s not exists", fnf)
			return
		} else if err != nil {
			log.Errorf("load fullnodes error: %v", err)
			return
		}
		defer file.Close() //nolint: errcheck // Read only op

		currentAi := ""
		br := bufio.NewReader(file)
		for {
			line, _, err := br.ReadLine()
			if err == io.EOF {
				break
			}

			log.Debugf("load fullnode line: ", string(line))
			ais := strings.Split(string(line), " ")
			if len(ais) > 0 {
				if err := fnp.add(ctx, ais[0]); err != nil {
					log.Errorf("add fullnode %s error: %v", ais[0], err)
					continue
				}
			}
			if len(ais) > 1 {
				if ais[1] == "X" {
					currentAi = ais[0]
					log.Debugf("last current fullnode is %s, reset.", currentAi)
				}
			}
		}
		if currentAi != "" {
			for i, ai := range fnp.fnapiinfo {
				if ai == currentAi {
					fnp.current = i
					break
				}
			}
		}
	}
}

func getFullNode(ctx context.Context, apiInfoStr string, checkVersion bool) (api.FullNode, jsonrpc.ClientCloser, error) {
	aInfo := cliutil.ParseApiInfo(apiInfoStr)
	version := "v1"
	addr, err := aInfo.DialArgs(version)
	if err != nil {
		return nil, nil, xerrors.Errorf("could not get DialArgs: %w", err)
	}
	v1API, closer, err := client.NewFullNodeRPCV1(ctx, addr, aInfo.AuthHeader())
	if err != nil {
		return nil, nil, err
	}

	if checkVersion {
		v, err := v1API.Version(ctx)
		if err != nil {
			return nil, nil, err
		}

		if !v.APIVersion.EqMajorMinor(api.FullAPIVersion1) {
			return nil, nil, xerrors.Errorf("Remote API version didn't match (expected %s, remote %s)", api.FullAPIVersion1, v.APIVersion)
		}
	}
	return v1API, closer, nil
}

func (fnp *FullNodePool) Start(ctx context.Context) {
	log.Infow("start fullnode switching loop")
	var err error
	for {
		// smaller for winning(<30), larger for sealing(<5min), medium for post(<2min)?
		interval := 25
		fullnodeSwitchIntervalStr := os.Getenv("FULLNODE_SWITCH_INTERVAL")
		if fullnodeSwitchIntervalStr != "" {
			interval, err = strconv.Atoi(fullnodeSwitchIntervalStr)
			if err != nil {
				log.Errorf("invalid env FULLNODE_SWTICH_INTERVAL")
			}
		}

		select {
		case <-time.After(time.Duration(interval) * time.Second):
			if len(fnp.fnapiinfo) > 1 && os.Getenv("ENABLE_FULLNODE_SWITCH") == "true" {
				log.Debugf("try switch fullnode")

				// get current daemon best epoch. if current daemon is down, 0 as its best.
				curEpoch := abi.ChainEpoch(0)
				curTs, err := fnp.fnnodes[fnp.fnapiinfo[fnp.current]].ChainHead(ctx)
				if err == nil {
					curEpoch = curTs.Height()
				}

				bestEpoch := abi.ChainEpoch(0)
				bestIndex := int(-1)
				for i, id := range fnp.fnapiinfo {
					if i == fnp.current {
						continue
					} else {
						bestTs, err := fnp.fnnodes[id].ChainHead(ctx)
						if err == nil {
							if bestTs.Height() > bestEpoch {
								bestEpoch = bestTs.Height()
								bestIndex = i
							}
						}
					}
				}
				if bestEpoch-curEpoch > 0 {
					log.Warnf("current fullnode is not the best. best=%d cur=%d", bestEpoch, curEpoch)
				}

				defaultMaxLag := 5
				maxLagEnv := os.Getenv("FULLNODE_MAXLAG")
				if maxLagEnv != "" {
					defaultMaxLag, err = strconv.Atoi(maxLagEnv)
					if err != nil {
						log.Errorf("invalid env FULLNODE_MAXLAG")
					}
				}
				if int(bestEpoch-curEpoch) > defaultMaxLag {
					fnp.SetCurrent(ctx, bestIndex)
				}
			}
		case <-fnp.stop:
			return
		}
	}
}

func (fnp *FullNodePool) Remove(ctx context.Context, indexToRemove int) error {
	if indexToRemove < 0 {
		return xerrors.Errorf("index to remove %d < 0")
	}
	if indexToRemove >= len(fnp.fnapiinfo) {
		return xerrors.Errorf("index to remove %d >= max size %d", indexToRemove, len(fnp.fnapiinfo))
	}

	if indexToRemove == fnp.current {
		return xerrors.Errorf("can not remove current full node.")
	}

	fnp.lk.Lock()
	defer fnp.lk.Unlock()

	// disconnect
	fnp.fnclosers[fnp.fnapiinfo[indexToRemove]]()

	// clear data. clear closers and nodes first.
	delete(fnp.fnclosers, fnp.fnapiinfo[indexToRemove])
	delete(fnp.fnnodes, fnp.fnapiinfo[indexToRemove])
	fnp.fnapiinfo = append(fnp.fnapiinfo[:indexToRemove], fnp.fnapiinfo[indexToRemove+1:]...)

	// update current if neccessary
	if fnp.current > indexToRemove {
		fnp.current = fnp.current - 1
	}

	fnp.store()

	return nil
}

func (fnp *FullNodePool) Add(ctx context.Context, apiInfoStr string) error {
	fnp.lk.Lock()
	defer fnp.lk.Unlock()
	if err := fnp.add(ctx, apiInfoStr); err != nil {
		return err
	}
	fnp.store()
	return nil
}

func (fnp *FullNodePool) add(ctx context.Context, apiInfoStr string) error {
	if _, exist := fnp.fnnodes[apiInfoStr]; exist {
		return xerrors.Errorf("fullnode %s already exists.", apiInfoStr)
	}

	node, closer, err := getFullNode(ctx, apiInfoStr, true)
	if err != nil {
		return err
	}

	fnp.fnapiinfo = append(fnp.fnapiinfo, apiInfoStr)
	fnp.fnclosers[apiInfoStr] = closer
	fnp.fnnodes[apiInfoStr] = node

	return nil
}

func (fnp *FullNodePool) SetCurrent(ctx context.Context, indexToSet int) error {
	if indexToSet < 0 {
		return xerrors.Errorf("index to set current %d < 0")
	}
	if indexToSet >= len(fnp.fnapiinfo) {
		return xerrors.Errorf("index to remove %d >= max size %d", indexToSet, len(fnp.fnapiinfo))
	}
	if indexToSet == fnp.current {
		return xerrors.Errorf("current is %d, no need to set.", indexToSet)
	}

	fnp.lk.Lock()
	defer fnp.lk.Unlock()

	oldIndex := fnp.current
	// in a new goroutine to eliminate lock scope. ChainNotify will recover a little later after current change.
	go func() {
		oldApiInfo := fnp.fnapiinfo[oldIndex]
		if _, err := fnp.fnnodes[oldApiInfo].Version(ctx); err == nil {
			// in order to close ChainNotify channel, reconnect old node.
			fnp.fnclosers[oldApiInfo]()
			node, closer, err := getFullNode(ctx, oldApiInfo, false)
			if err == nil {
				fnp.fnclosers[oldApiInfo] = closer
				fnp.fnnodes[oldApiInfo] = node
			} else {
				// TODO: old node may be in a bad state if reconnect failed.
				log.Warnf("!!!%s is in a very bad state!!!", oldApiInfo)
			}
		} else {
			// if old one is disconnect, no need to close and reconnect. ChainNotify will automatically switch to the new one.
		}
	}()

	// TODO: if new node is good?
	fnp.current = indexToSet
	log.Infof("fullnode current is switch to %d", indexToSet)

	fnp.store()
	return nil
}

func (fnp FullNodePool) List(ctx context.Context) ([]api.FnpItem, error) {
	// eliminate lock scope, delay ChainHead out of lock
	fnp.lk.Lock()
	cur := fnp.current
	var aiArray []string
	var nodeArray []api.FullNode
	for _, ai := range fnp.fnapiinfo {
		aiArray = append(aiArray, ai)
		nodeArray = append(nodeArray, fnp.fnnodes[ai])
	}
	defer fnp.lk.Unlock()

	var list []api.FnpItem
	for i, id := range aiArray {
		// if node is disconnect/removed, ChainHead reports error.
		ts, err := nodeArray[i].ChainHead(ctx)
		if err != nil {
			list = append(list, api.FnpItem{
				ID:      id,
				Err:     err.Error(),
				Current: i == cur,
			})
		} else {
			list = append(list, api.FnpItem{
				ID:      id,
				Latest:  ts.Height(),
				Current: i == cur,
			})
		}
	}
	return list, nil
}

func (fnp FullNodePool) Close() {
	fnp.lk.Lock()
	defer fnp.lk.Unlock()

	for apiInfo, closer := range fnp.fnclosers {
		log.Info("closing fullnode ", apiInfo)
		closer()
		log.Info("fullnode ", apiInfo, "is closed")
	}

	log.Info("stopping fullnode auto-switching")
	close(fnp.stop)
	log.Info("full node pool is down.")
}

func (fnp FullNodePool) currentFullNodeApi() v1api.FullNode {
	fnp.lk.Lock()
	defer fnp.lk.Unlock()

	return fnp.fnnodes[fnp.fnapiinfo[fnp.current]]
}

func (fnp FullNodePool) WalletSign(ctx context.Context, address address.Address, msg []byte) (*crypto.Signature, error) {
	log.Debugf("FullNodePool(%d %s): WalletSign", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().WalletSign(ctx, address, msg)
}

func (fnp FullNodePool) ChainHead(ctx context.Context) (*types.TipSet, error) {
	log.Debugf("FullNodePool(%d %s): ChainHead", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().ChainHead(ctx)
}

func (fnp FullNodePool) StateNetworkVersion(ctx context.Context, tsk types.TipSetKey) (apitypes.NetworkVersion, error) {
	log.Debugf("FullNodePool(%d %s): StateNetworkVersion", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateNetworkVersion(ctx, tsk)
}

func (fnp FullNodePool) ChainTipSetWeight(ctx context.Context, tsk types.TipSetKey) (types.BigInt, error) {
	log.Debugf("FullNodePool(%d %s): ChainTipSetWeight", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().ChainTipSetWeight(ctx, tsk)
}

func (fnp FullNodePool) StateSectorGetInfo(ctx context.Context, adr address.Address, sn abi.SectorNumber, tsk types.TipSetKey) (*miner.SectorOnChainInfo, error) {
	log.Debugf("FullNodePool(%d %s): StateSectorGetInfo", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateSectorGetInfo(ctx, adr, sn, tsk)
}

func (fnp FullNodePool) StateMinerPartitions(ctx context.Context, m address.Address, dlIdx uint64, tsk types.TipSetKey) ([]api.Partition, error) {
	log.Debugf("FullNodePool(%d %s): StateMinerPartitions", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateMinerPartitions(ctx, m, dlIdx, tsk)
}

func (fnp FullNodePool) StateMinerDeadlines(ctx context.Context, adr address.Address, tsk types.TipSetKey) ([]api.Deadline, error) {
	log.Debugf("FullNodePool(%d %s): StateMinerDeadlines", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateMinerDeadlines(ctx, adr, tsk)
}

func (fnp FullNodePool) MinerCreateBlock(ctx context.Context, t *api.BlockTemplate) (*types.BlockMsg, error) {
	log.Debugf("FullNodePool(%d %s): MinerCreateBlock", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().MinerCreateBlock(ctx, t)
}

func (fnp FullNodePool) MinerGetBaseInfo(ctx context.Context, adr address.Address, epoch abi.ChainEpoch, tsk types.TipSetKey) (*api.MiningBaseInfo, error) {
	log.Debugf("FullNodePool(%d %s): MinerGetBaseInfo", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().MinerGetBaseInfo(ctx, adr, epoch, tsk)
}

func (fnp FullNodePool) StateGetBeaconEntry(ctx context.Context, epoch abi.ChainEpoch) (*types.BeaconEntry, error) {
	log.Debugf("FullNodePool(%d %s): BeaconGetEntry", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateGetBeaconEntry(ctx, epoch)
}

func (fnp FullNodePool) SyncSubmitBlock(ctx context.Context, blk *types.BlockMsg) error {
	log.Debugf("FullNodePool(%d %s): SyncSubmitBlock", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().SyncSubmitBlock(ctx, blk)
}

func (fnp FullNodePool) StateGetRandomnessFromBeacon(ctx context.Context, personalization crypto.DomainSeparationTag, randEpoch abi.ChainEpoch, entropy []byte, tsk types.TipSetKey) (abi.Randomness, error) {
	log.Debugf("FullNodePool(%d %s): StateGetRandomnessFromBeacon", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateGetRandomnessFromBeacon(ctx, personalization, randEpoch, entropy, tsk)
}

func (fnp FullNodePool) StateMinerPower(ctx context.Context, adr address.Address, tsk types.TipSetKey) (*api.MinerPower, error) {
	log.Debugf("FullNodePool(%d %s): StateMinerPower", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateMinerPower(ctx, adr, tsk)
}
func (fnp FullNodePool) MpoolSelect(ctx context.Context, tsk types.TipSetKey, f float64) ([]*types.SignedMessage, error) {
	log.Debugf("FullNodePool(%d %s): MpoolSelect", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().MpoolSelect(ctx, tsk, f)
}

func (fnp FullNodePool) StateCall(ctx context.Context, msg *types.Message, tsk types.TipSetKey) (*api.InvocResult, error) {
	log.Debugf("FullNodePool(%d %s): StateCall", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateCall(ctx, msg, tsk)
}
func (fnp FullNodePool) StateMinerSectors(ctx context.Context, addr address.Address, bf *bitfield.BitField, tsk types.TipSetKey) ([]*miner.SectorOnChainInfo, error) {
	log.Debugf("FullNodePool(%d %s): StateMinerSectors", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateMinerSectors(ctx, addr, bf, tsk)
}
func (fnp FullNodePool) StateSectorPreCommitInfo(ctx context.Context, addr address.Address, sn abi.SectorNumber, tsk types.TipSetKey) (miner.SectorPreCommitOnChainInfo, error) {
	log.Debugf("FullNodePool(%d %s): StateSectorPreCommitInfo", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateSectorPreCommitInfo(ctx, addr, sn, tsk)
}
func (fnp FullNodePool) StateSectorPartition(ctx context.Context, maddr address.Address, sectorNumber abi.SectorNumber, tok types.TipSetKey) (*lminer.SectorLocation, error) {
	log.Debugf("FullNodePool(%d %s): StateSectorPartition", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateSectorPartition(ctx, maddr, sectorNumber, tok)
}
func (fnp FullNodePool) StateMinerInfo(ctx context.Context, maddr address.Address, tok types.TipSetKey) (api.MinerInfo, error) {
	log.Debugf("FullNodePool(%d %s): StateMinerInfo", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateMinerInfo(ctx, maddr, tok)
}
func (fnp FullNodePool) StateMinerAvailableBalance(ctx context.Context, maddr address.Address, tok types.TipSetKey) (types.BigInt, error) {
	log.Debugf("FullNodePool(%d %s): StateMinerAvailableBalance", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateMinerAvailableBalance(ctx, maddr, tok)
}
func (fnp FullNodePool) StateMinerActiveSectors(ctx context.Context, maddr address.Address, tok types.TipSetKey) ([]*miner.SectorOnChainInfo, error) {
	log.Debugf("FullNodePool(%d %s): StateMinerActiveSectors", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateMinerActiveSectors(ctx, maddr, tok)
}
func (fnp FullNodePool) StateMinerProvingDeadline(ctx context.Context, maddr address.Address, tok types.TipSetKey) (*dline.Info, error) {
	log.Debugf("FullNodePool(%d %s): StateMinerProvingDeadline", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateMinerProvingDeadline(ctx, maddr, tok)
}
func (fnp FullNodePool) StateMinerPreCommitDepositForPower(ctx context.Context, maddr address.Address, ci miner.SectorPreCommitInfo, tok types.TipSetKey) (types.BigInt, error) {
	log.Debugf("FullNodePool(%d %s): StateMinerPreCommitDepositForPower", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateMinerPreCommitDepositForPower(ctx, maddr, ci, tok)
}
func (fnp FullNodePool) StateMinerInitialPledgeCollateral(ctx context.Context, maddr address.Address, ci miner.SectorPreCommitInfo, tok types.TipSetKey) (types.BigInt, error) {
	log.Debugf("FullNodePool(%d %s): StateMinerInitialPledgeCollateral", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateMinerInitialPledgeCollateral(ctx, maddr, ci, tok)
}
func (fnp FullNodePool) StateMinerSectorAllocated(ctx context.Context, maddr address.Address, sn abi.SectorNumber, tok types.TipSetKey) (bool, error) {
	log.Debugf("FullNodePool(%d %s): StateMinerSectorAllocated", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateMinerSectorAllocated(ctx, maddr, sn, tok)
}
func (fnp FullNodePool) StateSearchMsg(ctx context.Context, from types.TipSetKey, msg cid.Cid, limit abi.ChainEpoch, allowReplaced bool) (*api.MsgLookup, error) {
	log.Debugf("FullNodePool(%d %s): StateSearchMsg", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateSearchMsg(ctx, from, msg, limit, allowReplaced)
}
func (fnp FullNodePool) StateWaitMsg(ctx context.Context, cid cid.Cid, confidence uint64, limit abi.ChainEpoch, allowReplaced bool) (*api.MsgLookup, error) {
	log.Debugf("FullNodePool(%d %s): StateWaitMsg", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateWaitMsg(ctx, cid, confidence, limit, allowReplaced)
}
func (fnp FullNodePool) StateGetActor(ctx context.Context, actor address.Address, ts types.TipSetKey) (*types.Actor, error) {
	log.Debugf("FullNodePool(%d %s): StateGetActor", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateGetActor(ctx, actor, ts)
}
func (fnp FullNodePool) StateMarketStorageDeal(ctx context.Context, dealId abi.DealID, tok types.TipSetKey) (*api.MarketDeal, error) {
	log.Debugf("FullNodePool(%d %s): StateMarketStorageDeal", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateMarketStorageDeal(ctx, dealId, tok)
}
func (fnp FullNodePool) StateMinerFaults(ctx context.Context, maddr address.Address, tok types.TipSetKey) (bitfield.BitField, error) {
	log.Debugf("FullNodePool(%d %s): StateMinerFaults", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateMinerFaults(ctx, maddr, tok)
}
func (fnp FullNodePool) StateMinerRecoveries(ctx context.Context, maddr address.Address, tok types.TipSetKey) (bitfield.BitField, error) {
	log.Debugf("FullNodePool(%d %s): StateMinerRecoveries", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateMinerRecoveries(ctx, maddr, tok)
}
func (fnp FullNodePool) StateAccountKey(ctx context.Context, maddr address.Address, tok types.TipSetKey) (address.Address, error) {
	log.Debugf("FullNodePool(%d %s): StateAccountKey", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateAccountKey(ctx, maddr, tok)
}
func (fnp FullNodePool) StateLookupID(ctx context.Context, maddr address.Address, tok types.TipSetKey) (address.Address, error) {
	log.Debugf("FullNodePool(%d %s): StateLookupID", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateLookupID(ctx, maddr, tok)
}
func (fnp FullNodePool) MpoolPushMessage(ctx context.Context, msg *types.Message, tok *api.MessageSendSpec) (*types.SignedMessage, error) {
	log.Debugf("FullNodePool(%d %s): MpoolPushMessage", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().MpoolPushMessage(ctx, msg, tok)
}
func (fnp FullNodePool) GasEstimateMessageGas(ctx context.Context, msg *types.Message, spec *api.MessageSendSpec, tok types.TipSetKey) (*types.Message, error) {
	log.Debugf("FullNodePool(%d %s): GasEstimateMessageGas", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().GasEstimateMessageGas(ctx, msg, spec, tok)
}
func (fnp FullNodePool) GasEstimateFeeCap(ctx context.Context, msg *types.Message, i int64, tok types.TipSetKey) (types.BigInt, error) {
	log.Debugf("FullNodePool(%d %s): GasEstimateFeeCap", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().GasEstimateFeeCap(ctx, msg, i, tok)
}
func (fnp FullNodePool) GasEstimateGasPremium(ctx context.Context, nblocksincl uint64, sender address.Address, gaslimit int64, tsk types.TipSetKey) (types.BigInt, error) {
	log.Debugf("FullNodePool(%d %s): GasEstimateGasPremium", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().GasEstimateGasPremium(ctx, nblocksincl, sender, gaslimit, tsk)
}
func (fnp FullNodePool) ChainNotify(ctx context.Context) (<-chan []*api.HeadChange, error) {
	log.Debugf("FullNodePool(%d %s): ChainNotify", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().ChainNotify(ctx)
}
func (fnp FullNodePool) StateGetRandomnessFromTickets(ctx context.Context, personalization crypto.DomainSeparationTag, randEpoch abi.ChainEpoch, entropy []byte, tsk types.TipSetKey) (abi.Randomness, error) {
	log.Debugf("FullNodePool(%d %s): StateGetRandomnessFromTickets", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateGetRandomnessFromTickets(ctx, personalization, randEpoch, entropy, tsk)
}
func (fnp FullNodePool) ChainGetTipSetByHeight(ctx context.Context, epoch abi.ChainEpoch, tsk types.TipSetKey) (*types.TipSet, error) {
	log.Debugf("FullNodePool(%d %s): ChainGetTipSetByHeight", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().ChainGetTipSetByHeight(ctx, epoch, tsk)
}
func (fnp FullNodePool) ChainGetTipSetAfterHeight(ctx context.Context, epoch abi.ChainEpoch, tsk types.TipSetKey) (*types.TipSet, error) {
	log.Debugf("FullNodePool(%d %s): ChainGetTipSetAfterHeight", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().ChainGetTipSetAfterHeight(ctx, epoch, tsk)
}
func (fnp FullNodePool) ChainGetBlockMessages(ctx context.Context, cid cid.Cid) (*api.BlockMessages, error) {
	log.Debugf("FullNodePool(%d %s): ChainGetBlockMessages", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().ChainGetBlockMessages(ctx, cid)
}
func (fnp FullNodePool) ChainGetMessage(ctx context.Context, mc cid.Cid) (*types.Message, error) {
	log.Debugf("FullNodePool(%d %s): ChainGetMessage", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().ChainGetMessage(ctx, mc)
}
func (fnp FullNodePool) ChainGetPath(ctx context.Context, from, to types.TipSetKey) ([]*api.HeadChange, error) {
	log.Debugf("FullNodePool(%d %s): ChainGetPath", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().ChainGetPath(ctx, from, to)
}
func (fnp FullNodePool) ChainReadObj(ctx context.Context, cid cid.Cid) ([]byte, error) {
	log.Debugf("FullNodePool(%d %s): ChainReadObj", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().ChainReadObj(ctx, cid)
}
func (fnp FullNodePool) ChainHasObj(ctx context.Context, cid cid.Cid) (bool, error) {
	log.Debugf("FullNodePool(%d %s): ChainHasObj", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().ChainHasObj(ctx, cid)
}
func (fnp FullNodePool) ChainPutObj(ctx context.Context, blk blocks.Block) error {
	log.Debugf("FullNodePool(%d %s): ChainPutObj", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().ChainPutObj(ctx, blk)
}
func (fnp FullNodePool) ChainGetTipSet(ctx context.Context, key types.TipSetKey) (*types.TipSet, error) {
	log.Debugf("FullNodePool(%d %s): ChainGetTipSet", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().ChainGetTipSet(ctx, key)
}
func (fnp FullNodePool) WalletBalance(ctx context.Context, maddr address.Address) (types.BigInt, error) {
	log.Debugf("FullNodePool(%d %s): WalletBalance", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().WalletBalance(ctx, maddr)
}
func (fnp FullNodePool) WalletHas(ctx context.Context, maddr address.Address) (bool, error) {
	log.Debugf("FullNodePool(%d %s): WalletHas", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().WalletHas(ctx, maddr)
}
func (fnp FullNodePool) StateSectorExpiration(ctx context.Context, maddr address.Address, sn abi.SectorNumber, tok types.TipSetKey) (*lminer.SectorExpiration, error) {
	log.Debugf("FullNodePool(%d %s): StateSectorExpiration", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateSectorExpiration(ctx, maddr, sn, tok)
}
func (fnp FullNodePool) StateMarketDeals(ctx context.Context, tok types.TipSetKey) (map[string]*api.MarketDeal, error) {
	log.Debugf("FullNodePool(%d %s): StateMarketDeals", fnp.current, fnp.fnapiinfo[fnp.current])
	return fnp.currentFullNodeApi().StateMarketDeals(ctx, tok)
}
