//go:build cgo
// +build cgo

package ffiwrapper

import (
	"context"
	"path/filepath"

	"go.opencensus.io/trace"
	"golang.org/x/xerrors"

	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-actors/v7/actors/runtime/proof"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

func (sb *Sealer) GenerateWinningPoSt(ctx context.Context, minerID abi.ActorID, sectorInfo []proof.ExtendedSectorInfo, randomness abi.PoStRandomness) ([]proof.PoStProof, error) {
	randomness[31] &= 0x3f
	privsectors, skipped, done, err := sb.pubExtendedSectorToPriv(ctx, minerID, sectorInfo, nil, abi.RegisteredSealProof.RegisteredWinningPoStProof, nil) // TODO: FAULTS?
	if err != nil {
		return nil, err
	}
	defer done()
	if len(skipped) > 0 {
		return nil, xerrors.Errorf("pubSectorToPriv skipped sectors: %+v", skipped)
	}

	return ffi.GenerateWinningPoSt(minerID, privsectors, randomness)
}

func (sb *Sealer) GenerateWindowPoSt(ctx context.Context, minerID abi.ActorID, sectorInfo []proof.ExtendedSectorInfo, randomness abi.PoStRandomness) ([]proof.PoStProof, []abi.SectorID, error) {
	randomness[31] &= 0x3f
	var goodPaths map[abi.SectorID]storiface.SectorPaths
	if ctx.Value("goodPaths") != nil {
		goodPaths = ctx.Value("goodPaths").(map[abi.SectorID]storiface.SectorPaths)
		log.Debug("octopus: GenerateWindowPoSt: good paths:")
		for g, p := range goodPaths {
			if uint64(g.Number) == 177 || uint64(g.Number) == 178 || uint64(g.Number) == 187 || uint64(g.Number) == 188 || uint64(g.Number) == 189 {
				log.Infof("octopus: good path: sector %d -> paths: %s %s", g, p.Cache, p.Sealed)
			} else {
				log.Debugf("octopus: good path: sector %d -> paths: %s %s", g, p.Cache, p.Sealed)
			}
		}
	}
	privsectors, skipped, done, err := sb.pubExtendedSectorToPriv(ctx, minerID, sectorInfo, nil, abi.RegisteredSealProof.RegisteredWindowPoStProof, goodPaths)
	if err != nil {
		return nil, nil, xerrors.Errorf("gathering sector info: %w", err)
	}

	defer done()

	if len(skipped) > 0 {
		return nil, skipped, xerrors.Errorf("pubSectorToPriv skipped some sectors")
	}

	proof, faulty, err := ffi.GenerateWindowPoSt(minerID, privsectors, randomness)

	var faultyIDs []abi.SectorID
	for _, f := range faulty {
		faultyIDs = append(faultyIDs, abi.SectorID{
			Miner:  minerID,
			Number: f,
		})
	}
	return proof, faultyIDs, err
}

func (sb *Sealer) buildSector(ctx context.Context, postProofType abi.RegisteredPoStProof, sid storage.SectorRef, sectorInfo proof.SectorInfo) ffi.PrivateSectorInfo {
	var privateSectorInfo = ffi.PrivateSectorInfo{
		PoStProofType: postProofType,
		SectorInfo:    sectorInfo,
	}
	if IsWinningPost(postProofType) && QiniuFeatureEnabled(QiniuFeatureSearchMultiSectorPath) {
		//post?????????Winning???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
		//??????path??????????????????sealed???????????????????????????
		paths := QiniuMultipleSectorPath()
		if foundIdx := FindSealedInPaths(paths, sid.ID); foundIdx >= 0 {
			path := paths[foundIdx]
			privateSectorInfo.SealedSectorPath = filepath.Join(path, storiface.FTSealed.String(), storiface.SectorName(sid.ID))
			log.Infof("QINIU winning sector sealed %s-%d use path %s", sid.ID.Miner.String(), sid.ID.Number, path)
		}
		// ????????????????????????????????????
		if privateSectorInfo.SealedSectorPath == "" {
			log.Warnf("QINIU winning does not find sector sealed %s-%d", sid.ID.Miner.String(), sid.ID.Number)
			privateSectorInfo.SealedSectorPath = filepath.Join(paths[0], storiface.FTSealed.String(), storiface.SectorName(sid.ID))
		}
		//??????path??????????????????cache???????????????????????????
		if foundIdx := FindCachePauxInPaths(paths, sid.ID); foundIdx >= 0 {
			path := paths[foundIdx]
			privateSectorInfo.CacheDirPath = filepath.Join(path, storiface.FTCache.String(), storiface.SectorName(sid.ID))
			log.Infof("QINIU winning sector cache %s-%d use path %s", sid.ID.Miner.String(), sid.ID.Number, path)
		}
		// ????????????????????????????????????
		if privateSectorInfo.CacheDirPath == "" {
			log.Warnf("QINIU winning does not find sector cache %s-%d", sid.ID.Miner.String(), sid.ID.Number)
			privateSectorInfo.CacheDirPath = filepath.Join(paths[0], storiface.FTCache.String(), storiface.SectorName(sid.ID))
		}
	} else if IsWinningPost(postProofType) && QiniuFeatureEnabled(QiniuFeatureSingleSectorPath) {
		//post?????????WINNING?????????????????????????????????????????????????????????????????????????????????????????????
		privateSectorInfo.CacheDirPath = filepath.Join(QiniuStorePath(), storiface.FTCache.String(), storiface.SectorName(sid.ID))
		privateSectorInfo.SealedSectorPath = filepath.Join(QiniuStorePath(), storiface.FTSealed.String(), storiface.SectorName(sid.ID))
	} else if IsWindowPost(postProofType) && QiniuFeatureEnabled(QiniuFeatureSearchMultiSectorPath) {
		//post?????????WINDOW??????????????????????????????????????????????????????????????????
		//?????????????????????????????????????????????????????????????????????????????????????????????
		//QINIU=/root/cfg.toml QINIU_AUTOCONFIG_SECTOR_PATH=true ./lotus-miner run
		//???????????????????????????
		//????????????????????????????????? ???????????????
		//1. ??????????????????checkProvable????????????????????????WindowPostSectorRootCache??????????????????
		//2. lotus-bench??????????????????QINIU_AUTOCONFIG_SECTOR_PATH???true???
		sealedRoot := WindowPostSectorSealedRoot(sid.ID)
		cacheRoot := WindowPostSectorCacheRoot(sid.ID)

		privateSectorInfo.CacheDirPath = filepath.Join(cacheRoot, storiface.FTCache.String(), storiface.SectorName(sid.ID))
		privateSectorInfo.SealedSectorPath = filepath.Join(sealedRoot, storiface.FTSealed.String(), storiface.SectorName(sid.ID))
		//?????????????????????????????????
	} else if IsWindowPost(postProofType) && QiniuFeatureEnabled(QiniuFeatureSingleSectorPath) {
		//post?????????WINDOW???????????????????????????????????????????????????????????????????????????????????????
		//??????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????/root/.lotusminer?????????
		//QINIU=/root/cfg.toml QINIU_STORE_PATH=/root/.lotusminer ./lotus-miner run
		privateSectorInfo.CacheDirPath = filepath.Join(QiniuStorePath(), storiface.FTCache.String(), storiface.SectorName(sid.ID))
		privateSectorInfo.SealedSectorPath = filepath.Join(QiniuStorePath(), storiface.FTSealed.String(), storiface.SectorName(sid.ID))
	} else {
		log.Warnf("QINIU branch does not find sector  %s-%d", sid.ID.Miner.String(), sid.ID.Number)
	}
	return privateSectorInfo
}

func (sb *Sealer) pubExtendedSectorToPriv(ctx context.Context, mid abi.ActorID, sectorInfo []proof.ExtendedSectorInfo, faults []abi.SectorNumber, rpt func(abi.RegisteredSealProof) (abi.RegisteredPoStProof, error), goodPaths map[abi.SectorID]storiface.SectorPaths) (ffi.SortedPrivateSectorInfo, []abi.SectorID, func(), error) {
	fmap := map[abi.SectorNumber]struct{}{}
	for _, fault := range faults {
		fmap[fault] = struct{}{}
	}

	var doneFuncs []func()
	done := func() {
		for _, df := range doneFuncs {
			df()
		}
	}

	var skipped []abi.SectorID
	var out []ffi.PrivateSectorInfo
	for _, s := range sectorInfo {
		if _, faulty := fmap[s.SectorNumber]; faulty {
			continue
		}

		postProofType, err := rpt(s.SealProof)
		if err != nil {
			done()
			return ffi.SortedPrivateSectorInfo{}, nil, nil, xerrors.Errorf("acquiring registered PoSt proof from sector info %+v: %w", s, err)
		}

		sid := storage.SectorRef{
			ID:        abi.SectorID{Miner: mid, Number: s.SectorNumber},
			ProofType: s.SealProof,
		}

		ffiInfo := proof.SectorInfo{
			SealProof:    s.SealProof,
			SectorNumber: s.SectorNumber,
			SealedCID:    s.SealedCID,
		}

		if QiniuFeatureEnabled(QiniuFeatureSingleSectorPath) {
			sec := sb.buildSector(ctx, postProofType, sid, ffiInfo)
			out = append(out, sec)
		} else {
			var cache string
			var sealed string

			//post?????????WINDOW/WINNING???????????????????????????????????????????????????????????????????????????????????????
			var paths storiface.SectorPaths
			if goodPaths != nil {
				paths = goodPaths[sid.ID]
				log.Debugf("octopus: pubSectorToPriv: goodPaths[%v]=%v", sid.ID, paths)

				cache = paths.Cache
				sealed = paths.Sealed
			}
			if cache == "" || sealed == "" {
				// back to official logics
				proveUpdate := s.SectorKey != nil
				if proveUpdate {
					log.Infof("Posting over updated sector for sector id: %d", s.SectorNumber)
					paths, d, err := sb.sectors.AcquireSector(ctx, sid, storiface.FTUpdateCache|storiface.FTUpdate, 0, storiface.PathStorage)
					if err != nil {
						log.Warnw("failed to acquire FTUpdateCache and FTUpdate of sector, skipping", "sector", sid.ID, "error", err)
						skipped = append(skipped, sid.ID)
						continue
					}
					doneFuncs = append(doneFuncs, d)
					cache = paths.UpdateCache
					sealed = paths.Update
				} else {
					log.Infof("Posting over sector key sector for sector id: %d", s.SectorNumber)
					paths, d, err := sb.sectors.AcquireSector(ctx, sid, storiface.FTCache|storiface.FTSealed, 0, storiface.PathStorage)
					if err != nil {
						log.Warnw("failed to acquire FTCache and FTSealed of sector, skipping", "sector", sid.ID, "error", err)
						skipped = append(skipped, sid.ID)
						continue
					}
					doneFuncs = append(doneFuncs, d)
					cache = paths.Cache
					sealed = paths.Sealed
				}

				if cache == "" || sealed == "" {
					log.Warnw("failed to acquire sector, skipping", "sector", sid.ID)
					skipped = append(skipped, sid.ID)
					continue
				}

			}

			if uint64(ffiInfo.SectorNumber) == 177 || uint64(ffiInfo.SectorNumber) == 178 || uint64(ffiInfo.SectorNumber) == 187 || uint64(ffiInfo.SectorNumber) == 188 ||
				uint64(ffiInfo.SectorNumber) == 189 {
				log.Infof("octopus: %v cache=%s sealed=%s sealedCid=%v", ffiInfo.SectorNumber, cache, sealed, ffiInfo.SealedCID.String())
			}
			out = append(out, ffi.PrivateSectorInfo{
				CacheDirPath:     cache,
				PoStProofType:    postProofType,
				SealedSectorPath: sealed,
				SectorInfo:       ffiInfo,
			})
		}
	}

	return ffi.NewSortedPrivateSectorInfo(out...), skipped, done, nil
}

var _ Verifier = ProofVerifier

type proofVerifier struct{}

var ProofVerifier = proofVerifier{}

func (proofVerifier) VerifySeal(info proof.SealVerifyInfo) (bool, error) {
	return ffi.VerifySeal(info)
}

func (proofVerifier) VerifyAggregateSeals(aggregate proof.AggregateSealVerifyProofAndInfos) (bool, error) {
	return ffi.VerifyAggregateSeals(aggregate)
}

func (proofVerifier) VerifyReplicaUpdate(update proof.ReplicaUpdateInfo) (bool, error) {
	return ffi.SectorUpdate.VerifyUpdateProof(update)
}

func (proofVerifier) VerifyWinningPoSt(ctx context.Context, info proof.WinningPoStVerifyInfo) (bool, error) {
	info.Randomness[31] &= 0x3f
	_, span := trace.StartSpan(ctx, "VerifyWinningPoSt")
	defer span.End()

	return ffi.VerifyWinningPoSt(info)
}

func (proofVerifier) VerifyWindowPoSt(ctx context.Context, info proof.WindowPoStVerifyInfo) (bool, error) {
	info.Randomness[31] &= 0x3f
	_, span := trace.StartSpan(ctx, "VerifyWindowPoSt")
	defer span.End()

	return ffi.VerifyWindowPoSt(info)
}

func (proofVerifier) GenerateWinningPoStSectorChallenge(ctx context.Context, proofType abi.RegisteredPoStProof, minerID abi.ActorID, randomness abi.PoStRandomness, eligibleSectorCount uint64) ([]uint64, error) {
	randomness[31] &= 0x3f
	return ffi.GenerateWinningPoStSectorChallenge(proofType, minerID, randomness, eligibleSectorCount)
}
