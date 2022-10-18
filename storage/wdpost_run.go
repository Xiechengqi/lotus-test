package storage

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/filecoin-project/go-state-types/proof"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/dline"
	"github.com/filecoin-project/go-state-types/network"
	"github.com/ipfs/go-cid"
	"go.opencensus.io/trace"
	"golang.org/x/xerrors"

	proof7 "github.com/filecoin-project/specs-actors/v7/actors/runtime/proof"

	"github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/go-state-types/builtin/v8/miner"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/actors/policy"
	"github.com/filecoin-project/lotus/chain/messagepool"
	"github.com/filecoin-project/lotus/chain/types"
)

// recordPoStFailure records a failure in the journal.
func (s *WindowPoStScheduler) recordPoStFailure(err error, ts *types.TipSet, deadline *dline.Info) {
	s.journal.RecordEvent(s.evtTypes[evtTypeWdPoStScheduler], func() interface{} {
		c := evtCommon{Error: err}
		if ts != nil {
			c.Deadline = deadline
			c.Height = ts.Height()
			c.TipSet = ts.Cids()
		}
		return WdPoStSchedulerEvt{
			evtCommon: c,
			State:     SchedulerStateFaulted,
		}
	})
}

// recordProofsEvent records a successful proofs_processed event in the
// journal, even if it was a noop (no partitions).
func (s *WindowPoStScheduler) recordProofsEvent(partitions []miner.PoStPartition, mcid cid.Cid) {
	s.journal.RecordEvent(s.evtTypes[evtTypeWdPoStProofs], func() interface{} {
		return &WdPoStProofsProcessedEvt{
			evtCommon:  s.getEvtCommon(nil),
			Partitions: partitions,
			MessageCID: mcid,
		}
	})
}

// startGeneratePoST kicks off the process of generating a PoST
func (s *WindowPoStScheduler) startGeneratePoST(
	ctx context.Context,
	ts *types.TipSet,
	deadline *dline.Info,
	completeGeneratePoST CompleteGeneratePoSTCb,
) context.CancelFunc {
	ctx, abort := context.WithCancel(ctx)
	go func() {
		defer abort()

		s.journal.RecordEvent(s.evtTypes[evtTypeWdPoStScheduler], func() interface{} {
			return WdPoStSchedulerEvt{
				evtCommon: s.getEvtCommon(nil),
				State:     SchedulerStateStarted,
			}
		})

		posts, err := s.runGeneratePoST(ctx, ts, deadline)
		completeGeneratePoST(posts, err)
	}()

	return abort
}

// runGeneratePoST generates the PoST
func (s *WindowPoStScheduler) runGeneratePoST(
	ctx context.Context,
	ts *types.TipSet,
	deadline *dline.Info,
) ([]miner.SubmitWindowedPoStParams, error) {
	ctx, span := trace.StartSpan(ctx, "WindowPoStScheduler.generatePoST")
	defer span.End()

	posts, err := s.runPoStCycle(ctx, false, *deadline, ts)
	if err != nil {
		log.Errorf("runPoStCycle failed: %+v", err)
		return nil, err
	}

	if len(posts) == 0 {
		s.recordProofsEvent(nil, cid.Undef)
	}

	return posts, nil
}

// startSubmitPoST kicks of the process of submitting PoST
func (s *WindowPoStScheduler) startSubmitPoST(
	ctx context.Context,
	ts *types.TipSet,
	deadline *dline.Info,
	posts []miner.SubmitWindowedPoStParams,
	completeSubmitPoST CompleteSubmitPoSTCb,
) context.CancelFunc {

	ctx, abort := context.WithCancel(ctx)
	go func() {
		defer abort()

		err := s.runSubmitPoST(ctx, ts, deadline, posts)
		if err == nil {
			s.journal.RecordEvent(s.evtTypes[evtTypeWdPoStScheduler], func() interface{} {
				return WdPoStSchedulerEvt{
					evtCommon: s.getEvtCommon(nil),
					State:     SchedulerStateSucceeded,
				}
			})
		}
		completeSubmitPoST(err)
	}()

	return abort
}

// runSubmitPoST submits PoST
func (s *WindowPoStScheduler) runSubmitPoST(
	ctx context.Context,
	ts *types.TipSet,
	deadline *dline.Info,
	posts []miner.SubmitWindowedPoStParams,
) error {
	if len(posts) == 0 {
		return nil
	}

	ctx, span := trace.StartSpan(ctx, "WindowPoStScheduler.submitPoST")
	defer span.End()

	// Get randomness from tickets
	// use the challenge epoch if we've upgraded to network version 4
	// (actors version 2). We want to go back as far as possible to be safe.
	commEpoch := deadline.Open
	if ver, err := s.api.StateNetworkVersion(ctx, types.EmptyTSK); err != nil {
		log.Errorw("failed to get network version to determine PoSt epoch randomness lookback", "error", err)
	} else if ver >= network.Version4 {
		commEpoch = deadline.Challenge
	}

	commRand, err := s.api.StateGetRandomnessFromTickets(ctx, crypto.DomainSeparationTag_PoStChainCommit, commEpoch, nil, ts.Key())
	if err != nil {
		err = xerrors.Errorf("failed to get chain randomness from tickets for windowPost (ts=%d; deadline=%d): %w", ts.Height(), commEpoch, err)
		log.Errorf("submitPoStMessage failed: %+v", err)

		return err
	}

	var submitErr error
	for i := range posts {
		// Add randomness to PoST
		post := &posts[i]
		post.ChainCommitEpoch = commEpoch
		post.ChainCommitRand = commRand

		// Submit PoST
		sm, err := s.submitPoStMessage(ctx, post)
		if err != nil {
			log.Errorf("submit window post failed: %+v", err)
			submitErr = err
		} else {
			s.recordProofsEvent(post.Partitions, sm.Cid())
		}
	}

	return submitErr
}

func (s *WindowPoStScheduler) checkSectors(ctx context.Context, check bitfield.BitField, tsk types.TipSetKey) (bitfield.BitField, map[abi.SectorID]storiface.SectorPaths, error) {
	mid, err := address.IDFromAddress(s.actor)
	if err != nil {
		return bitfield.BitField{}, nil, err
	}

	sectorInfos, err := s.api.StateMinerSectors(ctx, s.actor, &check, tsk)
	if err != nil {
		return bitfield.BitField{}, nil, err
	}

	//type checkSector struct {
	//sealed cid.Cid
	//update bool
	//}
	//sectors := make(map[abi.SectorNumber]checkSector)

	sectors := make(map[abi.SectorNumber]struct{})
	var tocheck []storage.SectorRef
	var update []bool
	for _, info := range sectorInfos {
		//sectors[info.SectorNumber] = checkSector{
		//sealed: info.SealedCID,
		//update: info.SectorKeyCID != nil,
		//}
		sectors[info.SectorNumber] = struct{}{}
		tocheck = append(tocheck, storage.SectorRef{
			ProofType: info.SealProof,
			ID: abi.SectorID{
				Miner:  abi.ActorID(mid),
				Number: info.SectorNumber,
			},
		})
		update = append(update, info.SectorKeyCID != nil)
	}

	bad, goodPaths, err := s.faultTracker.CheckProvable(ctx, s.proofType, tocheck, update, nil)
	//bad, err := s.faultTracker.CheckProvable(ctx, s.proofType, tocheck, func(ctx context.Context, id abi.SectorID) (cid.Cid, bool, error) {
	//	s, ok := sectors[id.Number]
	//	if !ok {
	//		return cid.Undef, false, xerrors.Errorf("sealed CID not found")
	//	}
	//	return s.sealed, s.update, nil
	//})
	if err != nil {
		return bitfield.BitField{}, nil, xerrors.Errorf("checking provable sectors: %w", err)
	}
	for id := range bad {
		delete(sectors, id.Number)
	}

	log.Warnw("Checked sectors", "checked", len(tocheck), "good", len(sectors))

	sbf := bitfield.New()
	for s := range sectors {
		sbf.Set(uint64(s))
	}

	return sbf, goodPaths, nil
}

// declareRecoveries identifies sectors that were previously marked as faulty
// for our miner, but are now recovered (i.e. are now provable again) and
// still not reported as such.
//
// It then reports the recovery on chain via a `DeclareFaultsRecovered`
// message to our miner actor.
//
// This is always invoked ahead of time, before the deadline for the evaluated
// sectors arrives. That way, recoveries are declared in preparation for those
// sectors to be proven.
//
// If a declaration is made, it awaits for build.MessageConfidence confirmations
// on chain before returning.
//
// TODO: the waiting should happen in the background. Right now this
//  is blocking/delaying the actual generation and submission of WindowPoSts in
//  this deadline!
func (s *WindowPoStScheduler) declareRecoveries(ctx context.Context, dlIdx uint64, partitions []api.Partition, tsk types.TipSetKey) ([][]miner.RecoveryDeclaration, []*types.SignedMessage, error) {
	ctx, span := trace.StartSpan(ctx, "storage.declareRecoveries")
	defer span.End()

	faulty := uint64(0)

	var batchedRecoveryDecls [][]miner.RecoveryDeclaration
	batchedRecoveryDecls = append(batchedRecoveryDecls, []miner.RecoveryDeclaration{})
	totalRecoveries := 0

	for partIdx, partition := range partitions {
		unrecovered, err := bitfield.SubtractBitField(partition.FaultySectors, partition.RecoveringSectors)
		if err != nil {
			return nil, nil, xerrors.Errorf("subtracting recovered set from fault set: %w", err)
		}

		uc, err := unrecovered.Count()
		if err != nil {
			return nil, nil, xerrors.Errorf("counting unrecovered sectors: %w", err)
		}

		if uc == 0 {
			continue
		}

		faulty += uc

		recovered, _, err := s.checkSectors(ctx, unrecovered, tsk)
		if err != nil {
			return nil, nil, xerrors.Errorf("checking unrecovered sectors: %w", err)
		}

		// if all sectors failed to recover, don't declare recoveries
		recoveredCount, err := recovered.Count()
		if err != nil {
			return nil, nil, xerrors.Errorf("counting recovered sectors: %w", err)
		}

		if recoveredCount == 0 {
			continue
		}

		// respect user config if set
		if s.maxPartitionsPerRecoveryMessage > 0 &&
			len(batchedRecoveryDecls[len(batchedRecoveryDecls)-1]) >= s.maxPartitionsPerRecoveryMessage {
			batchedRecoveryDecls = append(batchedRecoveryDecls, []miner.RecoveryDeclaration{})
		}

		batchedRecoveryDecls[len(batchedRecoveryDecls)-1] = append(batchedRecoveryDecls[len(batchedRecoveryDecls)-1], miner.RecoveryDeclaration{
			Deadline:  dlIdx,
			Partition: uint64(partIdx),
			Sectors:   recovered,
		})

		totalRecoveries++
	}

	if totalRecoveries == 0 {
		if faulty != 0 {
			log.Warnw("No recoveries to declare", "deadline", dlIdx, "faulty", faulty)
		}

		return nil, nil, nil
	}

	var msgs []*types.SignedMessage
	for _, recovery := range batchedRecoveryDecls {
		params := &miner.DeclareFaultsRecoveredParams{
			Recoveries: recovery,
		}

		enc, aerr := actors.SerializeParams(params)
		if aerr != nil {
			return nil, nil, xerrors.Errorf("could not serialize declare recoveries parameters: %w", aerr)
		}

		msg := &types.Message{
			To:     s.actor,
			Method: builtin.MethodsMiner.DeclareFaultsRecovered,
			Params: enc,
			Value:  types.NewInt(0),
		}
		spec := &api.MessageSendSpec{MaxFee: abi.TokenAmount(s.feeCfg.MaxWindowPoStGasFee)}
		if err := s.prepareMessage(ctx, msg, spec); err != nil {
			return nil, nil, err
		}

		sm, err := s.api.MpoolPushMessage(ctx, msg, &api.MessageSendSpec{MaxFee: abi.TokenAmount(s.feeCfg.MaxWindowPoStGasFee)})
		if err != nil {
			return nil, nil, xerrors.Errorf("pushing message to mpool: %w", err)
		}

		log.Warnw("declare faults recovered Message CID", "cid", sm.Cid())
		msgs = append(msgs, sm)
	}

	for _, msg := range msgs {
		rec, err := s.api.StateWaitMsg(context.TODO(), msg.Cid(), build.MessageConfidence, api.LookbackNoLimit, true)
		if err != nil {
			return batchedRecoveryDecls, msgs, xerrors.Errorf("declare faults recovered wait error: %w", err)
		}

		if rec.Receipt.ExitCode != 0 {
			return batchedRecoveryDecls, msgs, xerrors.Errorf("declare faults recovered wait non-0 exit code: %d", rec.Receipt.ExitCode)
		}
	}

	return batchedRecoveryDecls, msgs, nil
}

// declareFaults identifies the sectors on the specified proving deadline that
// are faulty, and reports the faults on chain via the `DeclareFaults` message
// to our miner actor.
//
// This is always invoked ahead of time, before the deadline for the evaluated
// sectors arrives. That way, faults are declared before a penalty is accrued.
//
// If a declaration is made, it awaits for build.MessageConfidence confirmations
// on chain before returning.
//
// TODO: the waiting should happen in the background. Right now this
//  is blocking/delaying the actual generation and submission of WindowPoSts in
//  this deadline!
func (s *WindowPoStScheduler) declareFaults(ctx context.Context, dlIdx uint64, partitions []api.Partition, tsk types.TipSetKey) ([]miner.FaultDeclaration, *types.SignedMessage, error) {
	ctx, span := trace.StartSpan(ctx, "storage.declareFaults")
	defer span.End()

	bad := uint64(0)
	params := &miner.DeclareFaultsParams{
		Faults: []miner.FaultDeclaration{},
	}

	for partIdx, partition := range partitions {
		nonFaulty, err := bitfield.SubtractBitField(partition.LiveSectors, partition.FaultySectors)
		if err != nil {
			return nil, nil, xerrors.Errorf("determining non faulty sectors: %w", err)
		}

		good, _, err := s.checkSectors(ctx, nonFaulty, tsk)
		if err != nil {
			return nil, nil, xerrors.Errorf("checking sectors: %w", err)
		}

		newFaulty, err := bitfield.SubtractBitField(nonFaulty, good)
		if err != nil {
			return nil, nil, xerrors.Errorf("calculating faulty sector set: %w", err)
		}

		c, err := newFaulty.Count()
		if err != nil {
			return nil, nil, xerrors.Errorf("counting faulty sectors: %w", err)
		}

		if c == 0 {
			continue
		}

		bad += c

		params.Faults = append(params.Faults, miner.FaultDeclaration{
			Deadline:  dlIdx,
			Partition: uint64(partIdx),
			Sectors:   newFaulty,
		})
	}

	faults := params.Faults
	if len(faults) == 0 {
		return faults, nil, nil
	}

	log.Errorw("DETECTED FAULTY SECTORS, declaring faults", "count", bad)

	enc, aerr := actors.SerializeParams(params)
	if aerr != nil {
		return faults, nil, xerrors.Errorf("could not serialize declare faults parameters: %w", aerr)
	}

	msg := &types.Message{
		To:     s.actor,
		Method: builtin.MethodsMiner.DeclareFaults,
		Params: enc,
		Value:  types.NewInt(0), // TODO: Is there a fee?
	}
	spec := &api.MessageSendSpec{MaxFee: abi.TokenAmount(s.feeCfg.MaxWindowPoStGasFee)}
	if err := s.prepareMessage(ctx, msg, spec); err != nil {
		return faults, nil, err
	}

	sm, err := s.api.MpoolPushMessage(ctx, msg, spec)
	if err != nil {
		return faults, sm, xerrors.Errorf("pushing message to mpool: %w", err)
	}

	log.Warnw("declare faults Message CID", "cid", sm.Cid())

	rec, err := s.api.StateWaitMsg(context.TODO(), sm.Cid(), build.MessageConfidence, api.LookbackNoLimit, true)
	if err != nil {
		return faults, sm, xerrors.Errorf("declare faults wait error: %w", err)
	}

	if rec.Receipt.ExitCode != 0 {
		return faults, sm, xerrors.Errorf("declare faults wait non-0 exit code: %d", rec.Receipt.ExitCode)
	}

	return faults, sm, nil
}

func (s *WindowPoStScheduler) asyncFaultRecover(di dline.Info, ts *types.TipSet) {
	enableNextDeadlineCheck := os.Getenv("ENABLE_NEXT_DEADLINE_CHECK")
	log.Debugf("octopus: runPoStCycle dl: %v", di.Index)
	log.Infof("octopus: wd: ENABLE_NEXT_DEADLINE_CHECK=%v ", enableNextDeadlineCheck)

	if enableNextDeadlineCheck == "" || enableNextDeadlineCheck == "true" {
		go func() {
			log.Debugf("octopus: wd: check faults / recoveries for the *next* deadline")
			// check faults / recoveries for the *next* deadline. It's already too
			// late to declare them for this deadline
			declDeadline := (di.Index + 2) % di.WPoStPeriodDeadlines

			partitions, err := s.api.StateMinerPartitions(context.TODO(), s.actor, declDeadline, ts.Key())
			if err != nil {
				log.Errorf("getting partitions: %v", err)
				return
			}

			var (
				sigmsgs    []*types.SignedMessage
				recoveries [][]miner.RecoveryDeclaration

				// optionalCid returns the CID of the message, or cid.Undef is the
				// message is nil. We don't need the argument (could capture the
				// pointer), but it's clearer and purer like that.
				optionalCid = func(sigmsg *types.SignedMessage) cid.Cid {
					if sigmsg == nil {
						return cid.Undef
					}
					return sigmsg.Cid()
				}
			)

			if recoveries, sigmsgs, err = s.declareRecoveries(context.TODO(), declDeadline, partitions, ts.Key()); err != nil {
				// TODO: This is potentially quite bad, but not even trying to post when this fails is objectively worse
				log.Errorf("checking sector recoveries: %v", err)
			}

			// should always be true, skip journaling if not for some reason
			if len(recoveries) == len(sigmsgs) {
				for i, recovery := range recoveries {
					// clone for function literal
					recovery := recovery
					msgCID := optionalCid(sigmsgs[i])
					s.journal.RecordEvent(s.evtTypes[evtTypeWdPoStRecoveries], func() interface{} {
						j := WdPoStRecoveriesProcessedEvt{
							evtCommon:    s.getEvtCommon(err),
							Declarations: recovery,
							MessageCID:   msgCID,
						}
						j.Error = err
						return j
					})
				}
			}
		}()
	}
}

// runPoStCycle runs a full cycle of the PoSt process:
//
//  1. performs recovery declarations for the next deadline.
//  2. performs fault declarations for the next deadline.
//  3. computes and submits proofs, batching partitions and making sure they
//     don't exceed message capacity.
//
// When `manual` is set, no messages (fault/recover) will be automatically sent
func (s *WindowPoStScheduler) runPoStCycle(ctx context.Context, manual bool, di dline.Info, ts *types.TipSet) ([]miner.SubmitWindowedPoStParams, error) {
	ctx, span := trace.StartSpan(ctx, "storage.runPoStCycle")
	defer span.End()

	if !manual {
		// TODO: extract from runPoStCycle, run on fault cutoff boundaries
		s.asyncFaultRecover(di, ts)
	}

	buf := new(bytes.Buffer)
	if err := s.actor.MarshalCBOR(buf); err != nil {
		return nil, xerrors.Errorf("failed to marshal address to cbor: %w", err)
	}

	headTs, err := s.api.ChainHead(ctx)
	if err != nil {
		return nil, xerrors.Errorf("getting current head: %w", err)
	}

	rand, err := s.api.StateGetRandomnessFromBeacon(ctx, crypto.DomainSeparationTag_WindowedPoStChallengeSeed, di.Challenge, buf.Bytes(), headTs.Key())
	if err != nil {
		return nil, xerrors.Errorf("failed to get chain randomness from beacon for window post (ts=%d; deadline=%d): %w", ts.Height(), di, err)
	}

	// Get the partitions for the given deadline
	partitions, err := s.api.StateMinerPartitions(ctx, s.actor, di.Index, ts.Key())
	if err != nil {
		return nil, xerrors.Errorf("getting partitions: %w", err)
	}

	nv, err := s.api.StateNetworkVersion(ctx, ts.Key())
	if err != nil {
		return nil, xerrors.Errorf("getting network version: %w", err)
	}

	// Split partitions into batches, so as not to exceed the number of sectors
	// allowed in a single message
	//partitionBatches, err := s.batchPartitions(partitions, nv)
	start, partitionBatches, err := s.assignPartitions(partitions, nv)
	if err != nil {
		return nil, err
	}

	defer func() {
		if r := recover(); r != nil {
			log.Errorf("recover: %s", r)
		}
	}()

	// Generate proofs in batches
	posts := make([]miner.SubmitWindowedPoStParams, 0, len(partitionBatches))
	for batchIdx, batch := range partitionBatches {
		batchPartitionStartIdx := 0
		for _, batch := range partitionBatches[:batchIdx] {
			batchPartitionStartIdx += len(batch)
		}

		params := miner.SubmitWindowedPoStParams{
			Deadline:   di.Index,
			Partitions: make([]miner.PoStPartition, 0, len(batch)),
			Proofs:     nil,
		}

		postSkipped := bitfield.New()
		somethingToProve := false

		maxRetries := 2 // default retry 1 time.
		retriesStr := os.Getenv("POST_RETRIES")
		if retriesStr != "" {
			maxRetries, _ = strconv.Atoi(retriesStr)
		}
		// Retry until we run out of sectors to prove.
		log.Debugf("octopus: max retries=%d", maxRetries)
		for retries := 0; retries < maxRetries; retries++ {
			skipCount := uint64(0)
			var partitions []miner.PoStPartition
			var xsinfos []proof7.ExtendedSectorInfo
			var goodPathsMap = make(map[abi.SectorID]storiface.SectorPaths)
			for partIdx, partition := range batch {
				// TODO: Can do this in parallel
				toProve, err := bitfield.SubtractBitField(partition.LiveSectors, partition.FaultySectors)
				if err != nil {
					return nil, xerrors.Errorf("removing faults from set of sectors to prove: %w", err)
				}
				toProve, err = bitfield.MergeBitFields(toProve, partition.RecoveringSectors)
				if err != nil {
					return nil, xerrors.Errorf("adding recoveries to set of sectors to prove: %w", err)
				}

				good, goodPaths, err := s.checkSectors(ctx, toProve, ts.Key())
				if err != nil {
					return nil, xerrors.Errorf("checking sectors to skip: %w", err)
				}
				for i, p := range goodPaths {
					goodPathsMap[i] = p
				}

				good, err = bitfield.SubtractBitField(good, postSkipped)
				if err != nil {
					return nil, xerrors.Errorf("toProve - postSkipped: %w", err)
				}

				skipped, err := bitfield.SubtractBitField(toProve, good)
				if err != nil {
					return nil, xerrors.Errorf("toProve - good: %w", err)
				}

				sc, err := skipped.Count()
				if err != nil {
					return nil, xerrors.Errorf("getting skipped sector count: %w", err)
				}

				skipCount += sc

				ssi, err := s.sectorsForProof(ctx, good, partition.AllSectors, ts)
				if err != nil {
					return nil, xerrors.Errorf("getting sorted sector info: %w", err)
				}

				if len(ssi) == 0 {
					continue
				}

				xsinfos = append(xsinfos, ssi...)
				partitions = append(partitions, miner.PoStPartition{
					Index:   uint64(start + batchPartitionStartIdx + partIdx),
					Skipped: skipped,
				})
				log.Infof("octopus: wd: di: %v, partition index: %v", di.Index, uint64(start+batchPartitionStartIdx+partIdx))
			}

			if len(xsinfos) == 0 {
				// nothing to prove for this batch
				break
			}

			// Generate proof
			log.Infow("running window post",
				"chain-random", rand,
				"deadline", di,
				"height", ts.Height(),
				"skipped", skipCount)

			tsStart := build.Clock.Now()

			mid, err := address.IDFromAddress(s.actor)
			if err != nil {
				return nil, err
			}

			//defer func() {
			//	if r := recover(); r != nil {
			//		log.Errorf("recover: %s", r)
			//	}
			//}()
			//log.Debug("octopus: good paths:")
			//for g, p := range goodPathsMap {
			//	log.Debugf("octopus: sector %d -> paths: %s %s", g, p.Cache, p.Sealed)
			//}
			ctxWithPaths := context.WithValue(ctx, "goodPaths", goodPathsMap)
			postOut, ps, err := s.prover.GenerateWindowPoSt(ctxWithPaths, abi.ActorID(mid), xsinfos, append(abi.PoStRandomness{}, rand...))
			elapsed := time.Since(tsStart)
			log.Infow("computing window post", "batch", batchIdx, "elapsed", elapsed, "skip", len(ps), "err", err)
			if err != nil {
				log.Errorf("error generating window post: %s", err)
			}
			if err == nil {

				// If we proved nothing, something is very wrong.
				if len(postOut) == 0 {
					log.Errorf("len(postOut) == 0")
					return nil, xerrors.Errorf("received no proofs back from generate window post")
				}

				headTs, err := s.api.ChainHead(ctx)
				if err != nil {
					return nil, xerrors.Errorf("getting current head: %w", err)
				}

				checkRand, err := s.api.StateGetRandomnessFromBeacon(ctx, crypto.DomainSeparationTag_WindowedPoStChallengeSeed, di.Challenge, buf.Bytes(), headTs.Key())
				if err != nil {
					return nil, xerrors.Errorf("failed to get chain randomness from beacon for window post (ts=%d; deadline=%d): %w", ts.Height(), di, err)
				}

				if !bytes.Equal(checkRand, rand) {
					log.Warnw("windowpost randomness changed", "old", rand, "new", checkRand, "ts-height", ts.Height(), "challenge-height", di.Challenge, "tsk", ts.Key())
					rand = checkRand
					continue
				}

				// If we generated an incorrect proof, try again.
				sinfos := make([]proof7.SectorInfo, len(xsinfos))
				for i, xsi := range xsinfos {
					sinfos[i] = proof7.SectorInfo{
						SealProof:    xsi.SealProof,
						SectorNumber: xsi.SectorNumber,
						SealedCID:    xsi.SealedCID,
					}
				}
				if correct, err := s.verifier.VerifyWindowPoSt(ctx, proof.WindowPoStVerifyInfo{
					Randomness:        abi.PoStRandomness(checkRand),
					Proofs:            postOut,
					ChallengedSectors: sinfos,
					Prover:            abi.ActorID(mid),
				}); err != nil {
					log.Errorw("window post verification failed", "post", postOut, "error", err)
					time.Sleep(5 * time.Second)
					continue
				} else if !correct {
					log.Errorw("generated incorrect window post proof", "post", postOut, "error", err)
					continue
				}

				// Proof generation successful, stop retrying
				somethingToProve = true
				params.Partitions = partitions
				params.Proofs = postOut
				break
			}

			// Proof generation failed, so retry
			log.Debugf("Proof generation failed, retry")
			if len(ps) == 0 {
				// If we didn't skip any new sectors, we failed
				// for some other reason and we need to abort.
				return nil, xerrors.Errorf("running window post failed: %w", err)
			}
			// TODO: maybe mark these as faulty somewhere?

			log.Warnw("generate window post skipped sectors", "sectors", ps, "error", err, "try", retries)

			// Explicitly make sure we haven't aborted this PoSt
			// (GenerateWindowPoSt may or may not check this).
			// Otherwise, we could try to continue proving a
			// deadline after the deadline has ended.
			if ctx.Err() != nil {
				log.Warnw("aborting PoSt due to context cancellation", "error", ctx.Err(), "deadline", di.Index)
				return nil, ctx.Err()
			}

			for _, sector := range ps {
				postSkipped.Set(uint64(sector.Number))
			}
		}

		// Nothing to prove for this batch, try the next batch
		if !somethingToProve {
			continue
		}
		posts = append(posts, params)
	}
	return posts, nil
}

func (s *WindowPoStScheduler) assignPartitions(partitions []api.Partition, nv network.Version) (int, [][]api.Partition, error) {
	emptyBatches := make([][]api.Partition, 0, 0)
	if len(partitions) <= 0 {
		return 0, emptyBatches, nil
	}

	minerIndexString := os.Getenv("POST_MINER_INDEX")
	log.Infof("octopus: wd: POST_MINER_INDEX=%s", minerIndexString)
	if minerIndexString == "" {
		// if POST_MINER_INDEX is not set, follow official logics
		p, e := s.batchPartitions(partitions, nv)
		return 0, p, e
	}

	minerIndex, err := strconv.Atoi(minerIndexString)
	if err != nil {
		log.Errorf("octopus: wd: invalid POST_MINER_INDEX: %s %v", minerIndexString, err)
		return 0, emptyBatches, nil
	}

	maxPartitions, err := strconv.Atoi(os.Getenv("POST_MAX_PARTITIONS"))
	if err != nil {
		log.Errorf("octopus: wd: invalid POST_MAX_PARTITIONS: %s %v", os.Getenv("POST_MAX_PARTITIONS"), err)
		return 0, emptyBatches, nil
	} else {
		if maxPartitions > 10 {
			maxPartitions = 10
			log.Warnf("octopus: wd: POST_MAX_PARTITIONS=%d, reset to 10", maxPartitions)
		} else {
			log.Infof("octopus: wd: POST_MAX_PARTITIONS=%d", maxPartitions)
		}
	}

	var partitionsPerMiner int
	var start int
	var end int

	totalPostMinersString := os.Getenv("POST_TOTAL_MINERS")
	log.Infof("octopus: POST_TOTAL_MINERS=%s", totalPostMinersString)
	if totalPostMinersString == "" {
		start = minerIndex * maxPartitions
		end = start + maxPartitions
	} else {
		totalPostMiners, err := strconv.Atoi(totalPostMinersString)
		if err != nil {
			log.Errorf("invalid POST_TOTAL_MINERS: %s %w", os.Getenv("POST_TOTAL_MINERS"), err)
			return 0, emptyBatches, nil
		}

		partitionsPerMiner = len(partitions) / totalPostMiners
		if partitionsPerMiner >= maxPartitions {
			start = minerIndex * maxPartitions
			end = start + maxPartitions
		} else {
			remainder := len(partitions) % totalPostMiners
			if remainder == 0 {
				start = minerIndex * partitionsPerMiner
				end = start + partitionsPerMiner
			} else {
				if minerIndex < remainder {
					start = minerIndex * (partitionsPerMiner + 1)
					end = start + (partitionsPerMiner + 1)
				} else {
					start = remainder*(partitionsPerMiner+1) + (minerIndex-remainder)*partitionsPerMiner
					end = start + partitionsPerMiner
				}
			}
		}
	}

	if start >= len(partitions) {
		log.Errorf("octopus: wd: invalid POST_MINER_INDEX: %s: %d exceeds partition length %d", minerIndexString, start, len(partitions))
		return 0, emptyBatches, nil
	}
	if end > len(partitions) {
		end = len(partitions)
	}
	log.Infof("octopus: wd: post miner %d:  partitions range %d -> %d", minerIndex, start, end)
	if os.Getenv("WDPOST_INDIVIDUAL_BATCH") == "true" {
		batches := make([][]api.Partition, 0, end-start)
		for i := start; i < end; i++ {
			batches = append(batches, partitions[i:i+1])
		}
		return start, batches, nil
	} else {
		batches := make([][]api.Partition, 0, 1)
		batches = append(batches, partitions[start:end])
		return start, batches, nil
	}
}

func (s *WindowPoStScheduler) batchPartitions(partitions []api.Partition, nv network.Version) ([][]api.Partition, error) {
	// We don't want to exceed the number of sectors allowed in a message.
	// So given the number of sectors in a partition, work out the number of
	// partitions that can be in a message without exceeding sectors per
	// message:
	// floor(number of sectors allowed in a message / sectors per partition)
	// eg:
	// max sectors per message  7:  ooooooo
	// sectors per partition    3:  ooo
	// partitions per message   2:  oooOOO
	//                              <1><2> (3rd doesn't fit)
	partitionsPerMsg, err := policy.GetMaxPoStPartitions(nv, s.proofType)
	if err != nil {
		return nil, xerrors.Errorf("getting sectors per partition: %w", err)
	}

	// Also respect the AddressedPartitionsMax (which is the same as DeclarationsMax (which is all really just MaxPartitionsPerDeadline))
	declMax, err := policy.GetDeclarationsMax(nv)
	if err != nil {
		return nil, xerrors.Errorf("getting max declarations: %w", err)
	}
	if partitionsPerMsg > declMax {
		partitionsPerMsg = declMax
	}

	// respect user config if set
	if s.maxPartitionsPerPostMessage > 0 {
		if partitionsPerMsg > s.maxPartitionsPerPostMessage {
			partitionsPerMsg = s.maxPartitionsPerPostMessage
		}
	}

	// The number of messages will be:
	// ceiling(number of partitions / partitions per message)
	batchCount := len(partitions) / partitionsPerMsg
	if len(partitions)%partitionsPerMsg != 0 {
		batchCount++
	}

	// Split the partitions into batches
	batches := make([][]api.Partition, 0, batchCount)
	for i := 0; i < len(partitions); i += partitionsPerMsg {
		end := i + partitionsPerMsg
		if end > len(partitions) {
			end = len(partitions)
		}
		batches = append(batches, partitions[i:end])
	}

	return batches, nil
}

func (s *WindowPoStScheduler) sectorsForProof(ctx context.Context, goodSectors, allSectors bitfield.BitField, ts *types.TipSet) ([]proof7.ExtendedSectorInfo, error) {
	sset, err := s.api.StateMinerSectors(ctx, s.actor, &goodSectors, ts.Key())
	if err != nil {
		return nil, err
	}

	if len(sset) == 0 {
		return nil, nil
	}

	substitute := proof7.ExtendedSectorInfo{
		SectorNumber: sset[0].SectorNumber,
		SealedCID:    sset[0].SealedCID,
		SealProof:    sset[0].SealProof,
		SectorKey:    sset[0].SectorKeyCID,
	}

	sectorByID := make(map[uint64]proof7.ExtendedSectorInfo, len(sset))
	for _, sector := range sset {
		sectorByID[uint64(sector.SectorNumber)] = proof7.ExtendedSectorInfo{
			SectorNumber: sector.SectorNumber,
			SealedCID:    sector.SealedCID,
			SealProof:    sector.SealProof,
			SectorKey:    sector.SectorKeyCID,
		}
	}

	proofSectors := make([]proof7.ExtendedSectorInfo, 0, len(sset))
	if err := allSectors.ForEach(func(sectorNo uint64) error {
		if info, found := sectorByID[sectorNo]; found {
			proofSectors = append(proofSectors, info)
		} else {
			proofSectors = append(proofSectors, substitute)
		}
		return nil
	}); err != nil {
		return nil, xerrors.Errorf("iterating partition sector bitmap: %w", err)
	}

	return proofSectors, nil
}

// submitPoStMessage builds a SubmitWindowedPoSt message and submits it to
// the mpool. It doesn't synchronously block on confirmations, but it does
// monitor in the background simply for the purposes of logging.
func (s *WindowPoStScheduler) submitPoStMessage(ctx context.Context, proof *miner.SubmitWindowedPoStParams) (*types.SignedMessage, error) {
	ctx, span := trace.StartSpan(ctx, "storage.commitPost")
	defer span.End()

	var sm *types.SignedMessage

	enc, aerr := actors.SerializeParams(proof)
	if aerr != nil {
		return nil, xerrors.Errorf("could not serialize submit window post parameters: %w", aerr)
	}

	msg := &types.Message{
		To:     s.actor,
		Method: builtin.MethodsMiner.SubmitWindowedPoSt,
		Params: enc,
		Value:  types.NewInt(0),
	}
	spec := &api.MessageSendSpec{MaxFee: abi.TokenAmount(s.feeCfg.MaxWindowPoStGasFee)}
	if err := s.prepareMessage(ctx, msg, spec); err != nil {
		return nil, err
	}

	var parLog = ""
	for _, p := range proof.Partitions {
		parLog = fmt.Sprintf("%s,%d", parLog, p.Index)
	}
	log.Infof("Try to Submitted window post: (deadline %d partitions %s)", proof.Deadline, parLog)
	sm, err := s.api.MpoolPushMessage(ctx, msg, spec)
	if err != nil {
		return nil, xerrors.Errorf("pushing message to mpool: %w", err)
	}

	log.Infof("Submitted window post: %s (deadline %d)", sm.Cid(), proof.Deadline)

	go func() {
		rec, err := s.api.StateWaitMsg(context.TODO(), sm.Cid(), build.MessageConfidence, api.LookbackNoLimit, true)
		if err != nil {
			log.Error(err)
			return
		}

		if rec.Receipt.ExitCode == 0 {
			log.Infow("Window post submission successful", "cid", sm.Cid(), "deadline", proof.Deadline, "epoch", rec.Height, "ts", rec.TipSet.Cids())
			return
		}

		log.Errorf("Submitting window post %s failed: exit %d", sm.Cid(), rec.Receipt.ExitCode)
	}()

	return sm, nil
}

// prepareMessage prepares a message before sending it, setting:
//
// * the sender (from the AddressSelector, falling back to the worker address if none set)
// * the right gas parameters
func (s *WindowPoStScheduler) prepareMessage(ctx context.Context, msg *types.Message, spec *api.MessageSendSpec) error {
	mi, err := s.api.StateMinerInfo(ctx, s.actor, types.EmptyTSK)
	if err != nil {
		return xerrors.Errorf("error getting miner info: %w", err)
	}
	// set the worker as a fallback
	msg.From = mi.Worker

	// (optimal) initial estimation with some overestimation that guarantees
	// block inclusion within the next 20 tipsets.
	gm, err := s.api.GasEstimateMessageGas(ctx, msg, spec, types.EmptyTSK)
	if err != nil {
		log.Errorw("estimating gas", "error", err)
		return nil
	}
	log.Infof("*gm.GasFeeCap %v", gm.GasFeeCap)
	*msg = *gm

	// calculate a more frugal estimation; premium is estimated to guarantee
	// inclusion within 5 tipsets, and fee cap is estimated for inclusion
	// within 4 tipsets.
	minGasFeeMsg := *msg

	minGasFeeMsg.GasPremium, err = s.api.GasEstimateGasPremium(ctx, 5, msg.From, msg.GasLimit, types.EmptyTSK)
	if err != nil {
		log.Errorf("failed to estimate minimum gas premium: %+v", err)
		minGasFeeMsg.GasPremium = msg.GasPremium
	}

	minGasFeeMsg.GasFeeCap, err = s.api.GasEstimateFeeCap(ctx, &minGasFeeMsg, 4, types.EmptyTSK)
	if err != nil {
		log.Errorf("failed to estimate minimum gas fee cap: %+v", err)
		minGasFeeMsg.GasFeeCap = msg.GasFeeCap
	}

	// goodFunds = funds needed for optimal inclusion probability.
	// minFunds  = funds needed for more speculative inclusion probability.
	goodFunds := big.Add(msg.RequiredFunds(), msg.Value)
	minFunds := big.Min(big.Add(minGasFeeMsg.RequiredFunds(), minGasFeeMsg.Value), goodFunds)

	pa, avail, err := s.addrSel.AddressFor(ctx, s.api, mi, api.PoStAddr, goodFunds, minFunds)
	if err != nil {
		log.Errorw("error selecting address for window post", "error", err)
		return nil
	}

	msg.From = pa
	bestReq := big.Add(msg.RequiredFunds(), msg.Value)
	if avail.LessThan(bestReq) {
		mff := func() (abi.TokenAmount, error) {
			return msg.RequiredFunds(), nil
		}

		messagepool.CapGasFee(mff, msg, &api.MessageSendSpec{MaxFee: big.Min(big.Sub(avail, msg.Value), msg.RequiredFunds())})
	}
	log.Infof("msg.GasFeeCap %v", msg.GasFeeCap)
	return nil
}

func (s *WindowPoStScheduler) ComputePoSt(ctx context.Context, dlIdx uint64, ts *types.TipSet) ([]miner.SubmitWindowedPoStParams, error) {
	dl, err := s.api.StateMinerProvingDeadline(ctx, s.actor, ts.Key())
	if err != nil {
		return nil, xerrors.Errorf("getting deadline: %w", err)
	}
	curIdx := dl.Index
	dl.Index = dlIdx
	dlDiff := dl.Index - curIdx
	if dl.Index > curIdx {
		dlDiff -= dl.WPoStPeriodDeadlines
		dl.PeriodStart -= dl.WPoStProvingPeriod
	}

	epochDiff := (dl.WPoStProvingPeriod / abi.ChainEpoch(dl.WPoStPeriodDeadlines)) * abi.ChainEpoch(dlDiff)

	// runPoStCycle only needs dl.Index and dl.Challenge
	dl.Challenge += epochDiff

	return s.runPoStCycle(ctx, true, *dl, ts)
}
