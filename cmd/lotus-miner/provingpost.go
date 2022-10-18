package main

import (
	"bytes"
	"context"
	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/network"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors/policy"
	"github.com/filecoin-project/go-state-types/builtin/v8/miner"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper/basicfs"
	"github.com/filecoin-project/specs-actors/v3/actors/runtime/proof"
	proof7 "github.com/filecoin-project/specs-actors/v7/actors/runtime/proof"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"strconv"
	"strings"
	"time"
)

var _nodeAPI api.StorageMiner
var _api v0api.FullNode
var _windowPoStProofType abi.RegisteredPoStProof
var _maddr address.Address

var provingPostCmd = &cli.Command{
	Name:      "post",
	Usage:     "generate wdpost by deadlineId",
	ArgsUsage: "<deadlineId>",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "roots",
			Usage: "roots split by space",
			Value: "/data/storage /data/storage1",
		},
		&cli.IntFlag{
			Name:  "epoch",
			Usage: "",
			Value: -1,
		},
	},
	Action: func(cctx *cli.Context) error {

		if cctx.Args().Len() != 1 {
			return xerrors.Errorf("must pass deadline index")
		}

		index, err := strconv.ParseUint(cctx.Args().Get(0), 10, 64)
		if err != nil {
			return xerrors.Errorf("could not parse deadline index: %w", err)
		}

		nodeAPI, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		_nodeAPI = nodeAPI

		_maddr, err = nodeAPI.ActorAddress(cctx.Context)
		if err != nil {
			return err
		}

		api, closer, err := lcli.GetFullNodeAPI(cctx)
		_api = api
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)

		head, err := api.ChainHead(ctx)
		if err != nil {
			return err
		}

		var tsk types.TipSetKey
		var ts *types.TipSet
		epoch := cctx.Int64("epoch")
		if epoch > 0 {
			ts, err = api.ChainGetTipSetByHeight(ctx, abi.ChainEpoch(epoch), types.EmptyTSK)
			if err != nil {
				return err
			}
		} else {
			ts = head
		}
		tsk = ts.Key()

		di, err := api.StateMinerProvingDeadline(ctx, _maddr, tsk)

		mInfo, err := api.StateMinerInfo(ctx, _maddr, types.EmptyTSK)
		_windowPoStProofType = mInfo.WindowPoStProofType
		if err != nil {
			return err
		}

		buf := new(bytes.Buffer)
		if err := _maddr.MarshalCBOR(buf); err != nil {
			return xerrors.Errorf("failed to marshal address to cbor: %w", err)
		}

		rand, err := api.StateGetRandomnessFromBeacon(ctx, crypto.DomainSeparationTag_WindowedPoStChallengeSeed, di.Challenge, buf.Bytes(), tsk)
		if err != nil {
			return xerrors.Errorf("failed to get chain randomness from beacon for window post (ts=%d; deadline=%d): %w", ts, di)
		}

		// Get the partitions for the given deadline
		partitions, err := api.StateMinerPartitions(ctx, _maddr, index, tsk)
		if err != nil {
			return xerrors.Errorf("getting partitions: %w", err)
		}

		nv, err := api.StateNetworkVersion(ctx, tsk)
		if err != nil {
			return xerrors.Errorf("getting network version: %w", err)
		}

		// Split partitions into batches, so as not to exceed the number of sectors
		// allowed in a single message
		partitionBatches, err := batchPartitions(partitions, nv)
		if err != nil {
			return err
		}

		// Generate proofs in batches
		posts := make([]miner.SubmitWindowedPoStParams, 0, len(partitionBatches))
		for batchIdx, batch := range partitionBatches {
			batchPartitionStartIdx := 0
			for _, batch := range partitionBatches[:batchIdx] {
				batchPartitionStartIdx += len(batch)
			}

			params := miner.SubmitWindowedPoStParams{
				Deadline:   index,
				Partitions: make([]miner.PoStPartition, 0, len(batch)),
				Proofs:     nil,
			}

			postSkipped := bitfield.New()
			somethingToProve := false

			// Retry until we run out of sectors to prove.
			for retries := 0; ; retries++ {
				skipCount := uint64(0)
				var partitions []miner.PoStPartition
				var xsinfos []proof7.ExtendedSectorInfo
				for partIdx, partition := range batch {
					// TODO: Can do this in parallel
					toProve, err := bitfield.SubtractBitField(partition.LiveSectors, partition.FaultySectors)
					if err != nil {
						return xerrors.Errorf("removing faults from set of sectors to prove: %w", err)
					}
					toProve, err = bitfield.MergeBitFields(toProve, partition.RecoveringSectors)
					if err != nil {
						return xerrors.Errorf("adding recoveries to set of sectors to prove: %w", err)
					}

					good, err := checkSectors(ctx, toProve, tsk)
					if err != nil {
						return xerrors.Errorf("checking sectors to skip: %w", err)
					}

					good, err = bitfield.SubtractBitField(good, postSkipped)
					if err != nil {
						return xerrors.Errorf("toProve - postSkipped: %w", err)
					}

					skipped, err := bitfield.SubtractBitField(toProve, good)
					if err != nil {
						return xerrors.Errorf("toProve - good: %w", err)
					}

					sc, err := skipped.Count()
					if err != nil {
						return xerrors.Errorf("getting skipped sector count: %w", err)
					}

					skipCount += sc

					ssi, err := sectorsForProof(ctx, good, partition.AllSectors, ts)
					if err != nil {
						return xerrors.Errorf("getting sorted sector info: %w", err)
					}

					if len(ssi) == 0 {
						continue
					}

					xsinfos = append(xsinfos, ssi...)
					partitions = append(partitions, miner.PoStPartition{
						Index:   uint64(batchPartitionStartIdx + partIdx),
						Skipped: skipped,
					})
				}

				if len(xsinfos) == 0 {
					// nothing to prove for this batch
					break
				}

				// Generate proof
				log.Infow("running window post",
					"chain-random", rand,
					"deadline", index,
					"height", ts.Height(),
					"skipped", skipCount)

				tsStart := build.Clock.Now()

				mid, err := address.IDFromAddress(_maddr)
				if err != nil {
					return err
				}

				root := cctx.String("roots")
				roots := strings.Split(root, " ")
				sbfs := &basicfs.RootsProvider{
					Roots: roots,
				}
				sb, err := ffiwrapper.New(sbfs)
				if err != nil {
					return err
				}

				postOut, ps, err := sb.GenerateWindowPoSt(ctx, abi.ActorID(mid), xsinfos, append(abi.PoStRandomness{}, rand...))
				elapsed := time.Since(tsStart)
				log.Infow("computing window post", "batch", batchIdx, "elapsed", elapsed)

				if err == nil {
					// If we proved nothing, something is very wrong.
					if len(postOut) == 0 {
						return xerrors.Errorf("received no proofs back from generate window post")
					}

					//headTs, err := api.ChainHead(ctx)
					//if err != nil {
					//	return xerrors.Errorf("getting current head: %w", err)
					//}

					checkRand, err := api.StateGetRandomnessFromBeacon(ctx, crypto.DomainSeparationTag_WindowedPoStChallengeSeed, di.Challenge, buf.Bytes(), tsk)
					if err != nil {
						return xerrors.Errorf("failed to get chain randomness from beacon for window post (ts=%d; deadline=%d): %w", ts.Height(), index, err)
					}

					if !bytes.Equal(checkRand, rand) {
						log.Warnw("windowpost randomness changed", "old", rand, "new", checkRand, "ts-height", ts.Height(), "challenge-height", index, "tsk", ts.Key())
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

					info := proof.WindowPoStVerifyInfo{
						Randomness:        abi.PoStRandomness(checkRand),
						Proofs:            postOut,
						ChallengedSectors: sinfos,
						Prover:            abi.ActorID(mid),
					}

					info.Randomness[31] &= 0x3f

					if correct, err := ffi.VerifyWindowPoSt(info); err != nil {
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

				if len(ps) == 0 {
					// If we didn't skip any new sectors, we failed
					// for some other reason and we need to abort.
					return xerrors.Errorf("running window post failed: %w", err)
				}
				// TODO: maybe mark these as faulty somewhere?

				log.Warnw("generate window post skipped sectors", "sectors", ps, "error", err, "try", retries)

				// Explicitly make sure we haven't aborted this PoSt
				// (GenerateWindowPoSt may or may not check this).
				// Otherwise, we could try to continue proving a
				// deadline after the deadline has ended.
				if ctx.Err() != nil {
					log.Warnw("aborting PoSt due to context cancellation", "error", ctx.Err(), "deadline", index)
					return ctx.Err()
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
		log.Infow("result-----------:", "posts", posts)

		return nil
	},
}

func batchPartitions(partitions []api.Partition, nv network.Version) ([][]api.Partition, error) {
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
	partitionsPerMsg, err := policy.GetMaxPoStPartitions(nv, _windowPoStProofType)
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

func checkSectors(ctx context.Context, check bitfield.BitField, tsk types.TipSetKey) (bitfield.BitField, error) {
	mid, err := address.IDFromAddress(_maddr)
	if err != nil {
		return bitfield.BitField{}, err
	}

	sectorInfos, err := _api.StateMinerSectors(ctx, _maddr, &check, tsk)
	if err != nil {
		return bitfield.BitField{}, err
	}

	sectors := make(map[abi.SectorNumber]struct{})
	var tocheck []storage.SectorRef
	var update []bool
	for _, info := range sectorInfos {
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

	bad, err := _nodeAPI.CheckProvable(ctx, _windowPoStProofType, tocheck, update, false)
	if err != nil {
		return bitfield.BitField{}, xerrors.Errorf("checking provable sectors: %w", err)
	}
	for id := range bad {
		delete(sectors, id)
	}

	log.Warnw("Checked sectors", "checked", len(tocheck), "good", len(sectors))

	sbf := bitfield.New()
	for s := range sectors {
		sbf.Set(uint64(s))
	}

	return sbf, nil
}

func sectorsForProof(ctx context.Context, goodSectors, allSectors bitfield.BitField, ts *types.TipSet) ([]proof7.ExtendedSectorInfo, error) {
	sset, err := _api.StateMinerSectors(ctx, _maddr, &goodSectors, ts.Key())
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
