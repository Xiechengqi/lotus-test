package sectorstorage

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/filecoin-project/lotus/build"
	"go.uber.org/atomic"
	"golang.org/x/xerrors"

	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-actors/actors/runtime/proof"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

// FaultTracker TODO: Track things more actively
type FaultTracker interface {
	CheckProvable(ctx context.Context, pp abi.RegisteredPoStProof, sectors []storage.SectorRef, update []bool, rg storiface.RGetter) (map[abi.SectorID]string, map[abi.SectorID]storiface.SectorPaths, error)
}

// CheckProvable returns unprovable sectors
func (m *Manager) CheckProvable(ctx context.Context, pp abi.RegisteredPoStProof, sectors []storage.SectorRef, update []bool, rg storiface.RGetter) (map[abi.SectorID]string, map[abi.SectorID]storiface.SectorPaths, error) {
	var bad = make(map[abi.SectorID]string)
	var badLock sync.Mutex

	var goodPaths = make(map[abi.SectorID]storiface.SectorPaths)
	var goodLock sync.Mutex

	ssize, err := pp.SectorSize()
	if err != nil {
		return nil, nil, err
	}

	checkList := make(map[string]SectorFile, len(sectors)*fileCount(ssize)*2)
	var checkListLock sync.Mutex

	// TODO: More better checks
	wg := sync.WaitGroup{}
	wgErr := atomic.Error{}
	tsStart := build.Clock.Now()
	tsBatchStart := build.Clock.Now()
	for i, sectorOuter := range sectors {
		log.Debugf("octopus: prove: %d check sector %v...", i, sectorOuter.ID.Number)
		wg.Add(1)

		sector := sectorOuter
		ii := i
		go func() {
			err := func() error {
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()

				var fReplica string
				var fCache string

				if ffiwrapper.QiniuFeatureEnabled(ffiwrapper.QiniuFeatureSingleSectorPath) {
					//此处是使用固定路径逻辑，使用此项之前，要求对象存储上所有的文件均处在同一路径，我们以/root/.lotusminer为例：
					//QINIU=/root/cfg.toml QINIU_STORE_PATH=/root/.lotusminer ./lotus-miner run
					// TODO: update/update_cache
					fCache = filepath.Join(ffiwrapper.QiniuStorePath(), storiface.FTCache.String(), storiface.SectorName(sector.ID))
					fReplica = filepath.Join(ffiwrapper.QiniuStorePath(), storiface.FTSealed.String(), storiface.SectorName(sector.ID))

					if fReplica == "" || fCache == "" {
						log.Warnw("CheckProvable Sector FAULT: cache and/or sealed paths not found", "sector", sector, "sealed", fReplica, "cache", fCache)
						updateBad(sector.ID, bad, &badLock, fmt.Sprintf("cache and/or sealed paths not found, cache %q, sealed %q", fCache, fReplica))
						return nil
					}

					if rg == nil {
						checkListLock.Lock()
						addCheckList(fReplica, fCache, sector.ID, ssize, checkList)
						checkListLock.Unlock()
					}
				} else {
					if update[ii] {
						lockedUpdate, err := m.index.StorageTryLock(ctx, sector.ID, storiface.FTUpdate|storiface.FTUpdateCache, storiface.FTNone)
						if err != nil {
							return xerrors.Errorf("acquiring sector lock: %w", err)
						}
						if !lockedUpdate {
							log.Warnw("CheckProvable Sector FAULT: can't acquire read lock on update replica", "sector", sector)
							updateBad(sector.ID, bad, &badLock, fmt.Sprint("can't acquire read lock"))
							return nil
						}
						lp, _, err := m.localStore.AcquireSector(ctx, sector, storiface.FTUpdate|storiface.FTUpdateCache, storiface.FTNone, storiface.PathStorage, storiface.AcquireMove)
						if err != nil {
							log.Warnw("CheckProvable Sector FAULT: acquire sector update replica in checkProvable", "sector", sector, "error", err)
							updateBad(sector.ID, bad, &badLock, fmt.Sprintf("acquire sector failed: %s", err))
							return nil
						}
						fReplica = lp.Update
						fCache = lp.UpdateCache
					} else {
						locked, err := m.index.StorageTryLock(ctx, sector.ID, storiface.FTSealed|storiface.FTCache, storiface.FTNone)
						if err != nil {
							return xerrors.Errorf("acquiring sector lock: %w", err)
						}

						if !locked {
							log.Warnw("CheckProvable Sector FAULT: can't acquire read lock", "sector", sector)
							updateBad(sector.ID, bad, &badLock, fmt.Sprint("can't acquire read lock"))
							return nil
						}
						lp, _, err := m.localStore.AcquireSector(ctx, sector, storiface.FTSealed|storiface.FTCache, storiface.FTNone, storiface.PathStorage, storiface.AcquireMove)
						if err != nil {
							log.Warnw("CheckProvable Sector FAULT: acquire sector in checkProvable", "sector", sector, "error", err)
							updateBad(sector.ID, bad, &badLock, fmt.Sprintf("acquire sector failed: %s", err))
							return nil
						}
						fReplica = lp.Sealed
						fCache = lp.Cache
					}

					if fReplica == "" || fCache == "" {
						//bad[sector.ID] = fmt.Sprintf("cache and/or sealed paths not found, cache %q, sealed %q", lp.Cache, lp.Sealed)
						log.Warnw("octopus: acquire sector empty, try from local stores.")

						log.Debugf("octopus: wdpost: AcquireSector %v error, try to traverse store storages...", sector.ID.Number)
						// octopus: try from storage local path.
						// post miner must configure store storage
						storagePaths, localErr := m.localStore.LocalPaths(ctx)
						if localErr != nil {
							log.Warnw("CheckProvable Sector FAULT: acquire sector in checkProvable", "sector", sector, "error", err)
							updateBad(sector.ID, bad, &badLock, fmt.Sprintf("acquire sector failed: %s / %s", err, localErr))
							return nil
						}

						isBad := true
						var badReason string
						for _, storagePath := range storagePaths {
							if storagePath.CanStore {
								if update[ii] {
									fCache = filepath.Join(storagePath.LocalPath, storiface.FTUpdateCache.String(), storiface.SectorName(sector.ID))
									fReplica = filepath.Join(storagePath.LocalPath, storiface.FTUpdate.String(), storiface.SectorName(sector.ID))
								} else {
									fCache = filepath.Join(storagePath.LocalPath, storiface.FTCache.String(), storiface.SectorName(sector.ID))
									fReplica = filepath.Join(storagePath.LocalPath, storiface.FTSealed.String(), storiface.SectorName(sector.ID))
								}
								isBad, badReason = checkIsBad(fReplica, fCache, sector, ssize)
								log.Debugf("octopus: wdpost: check sector %v from storage %v %s: cache=%s sealed=%s %v", sector.ID.Number, storagePath.ID, storagePath.LocalPath, fCache, fReplica, isBad)
								if !isBad {
									updateGood(sector.ID, goodPaths, &goodLock, fReplica, fCache)
									break
								}
							}
						}
						if isBad {
							// last storage's bad reason
							updateBad(sector.ID, bad, &badLock, badReason)
							return nil
						}
					} else {
						if !updateResult(fReplica, fCache, sector, ssize, &badLock, &goodLock, bad, goodPaths) {
							return nil
						}
					}
				}

				if rg != nil {
					check := true
					if ffiwrapper.UseQiniu() {
						checkListOneSector := make(map[string]SectorFile, fileCount(ssize)*2)
						checkListLock.Lock()
						addCheckList(fReplica, fCache, sector.ID, ssize, checkListOneSector)
						checkListLock.Unlock()
						l_before := len(bad)
						checkBad(bad, checkListOneSector)
						l_after := len(bad)

						check = l_after == l_before
					}

					if check {
						wpp, err := sector.ProofType.RegisteredWindowPoStProof()
						if err != nil {
							return err
						}

						var pr abi.PoStRandomness = make([]byte, abi.RandomnessLength)
						_, _ = rand.Read(pr)
						pr[31] &= 0x3f

						ch, err := ffi.GeneratePoStFallbackSectorChallenges(wpp, sector.ID.Miner, pr, []abi.SectorNumber{
							sector.ID.Number,
						})
						if err != nil {
							log.Warnw("CheckProvable Sector FAULT: generating challenges", "sector", sector, "sealed", fReplica, "cache", fReplica, "err", err)
							updateBad(sector.ID, bad, &badLock, fmt.Sprintf("generating fallback challenges: %s", err))
							return nil
						}

						commr, _, err := rg(ctx, sector.ID)
						if err != nil {
							log.Warnw("CheckProvable Sector FAULT: getting commR", "sector", sector, "sealed", fReplica, "cache", fCache, "err", err)
							updateBad(sector.ID, bad, &badLock, fmt.Sprintf("getting commR: %s", err))
							return nil
						}

						_, err = ffi.GenerateSingleVanillaProof(ffi.PrivateSectorInfo{
							SectorInfo: proof.SectorInfo{
								SealProof:    sector.ProofType,
								SectorNumber: sector.ID.Number,
								SealedCID:    commr,
							},
							CacheDirPath:     fCache,
							PoStProofType:    wpp,
							SealedSectorPath: fReplica,
						}, ch.Challenges[sector.ID.Number])
						if err != nil {
							log.Warnw("CheckProvable Sector FAULT: generating vanilla proof", "sector", sector, "sealed", fReplica, "cache", fCache, "err", err)
							updateBad(sector.ID, bad, &badLock, fmt.Sprintf("generating vanilla proof: %s", err))
							return nil
						}
					}
				}

				return nil
			}()
			if err != nil {
				log.Errorf("octopus: %v", err)
				wgErr.Store(err)
			}
			wg.Done()
		}()
		if (i+1)%10 == 0 {
			wg.Wait()

			log.Debugf("octopus: %d: check sectors stat costs: %v", i, time.Since(tsBatchStart))
			if wgErr.Load() != nil {
				return nil, nil, wgErr.Load()
			}
			tsBatchStart = build.Clock.Now()
		}
	}
	wg.Wait()

	log.Debugf("octopus: final: check sectors stat elapsed: %v", time.Since(tsBatchStart))
	if wgErr.Load() != nil {
		return nil, nil, wgErr.Load()
	}
	log.Infof("octopus: check sectors total elapsed: %v", time.Since(tsStart))

	if rg == nil && ffiwrapper.UseQiniu() {
		checkBad(bad, checkList)
	}
	return bad, goodPaths, nil
}

func updateBad(sid abi.SectorID, bad map[abi.SectorID]string, badLock *sync.Mutex, err string) {
	badLock.Lock()
	bad[sid] = err
	badLock.Unlock()
}

func updateGood(sid abi.SectorID, goodPaths map[abi.SectorID]storiface.SectorPaths, goodLock *sync.Mutex, fReplica string, fCache string) {
	var lp storiface.SectorPaths
	lp.Sealed = fReplica
	lp.Cache = fCache
	goodLock.Lock()
	goodPaths[sid] = lp
	goodLock.Unlock()
}

func updateResult(fReplica string, fCache string, sector storage.SectorRef, ssize abi.SectorSize,
	badLock *sync.Mutex, goodLock *sync.Mutex,
	bad map[abi.SectorID]string, goodPaths map[abi.SectorID]storiface.SectorPaths) bool {
	isBad, badReason := checkIsBad(fReplica, fCache, sector, ssize)
	if isBad {
		updateBad(sector.ID, bad, badLock, badReason)
		return false
	} else {
		updateGood(sector.ID, goodPaths, goodLock, fReplica, fCache)
		return true
	}

}

func checkIsBad(fReplica string, fCache string, sector storage.SectorRef, ssize abi.SectorSize) (bool, string) {
	toCheck := map[string]int64{
		fReplica:                       1,
		filepath.Join(fCache, "p_aux"): 0,
	}

	addCachePathsForSectorSize(toCheck, fCache, ssize)

	tsStart := build.Clock.Now()
	for p, sz := range toCheck {
		st, err := os.Stat(p)

		if err != nil {
			log.Warnw("CheckProvable Sector FAULT: sector file stat error", "sector", sector, "sealed", fReplica, "cache", fCache, "file", p, "err", err)
			return true, fmt.Sprintf("%s", err)
		}

		if sz != 0 {
			if st.Size() != int64(ssize)*sz {
				log.Warnw("CheckProvable Sector FAULT: sector file is wrong size", "sector", sector, "sealed", fReplica, "cache", fCache, "file", p, "size", st.Size(), "expectSize", int64(ssize)*sz)
				return true, fmt.Sprintf("%s is wrong size (got %d, expect %d)", p, st.Size(), int64(ssize)*sz)
			}
		}
	}
	elapsed := time.Since(tsStart)
	log.Debugf("octopus: wdpost: sector %v stat elapse %v", sector.ID, elapsed)
	return false, ""
}

func addCachePathsForSectorSize(chk map[string]int64, cacheDir string, ssize abi.SectorSize) {
	switch ssize {
	case 2 << 10:
		fallthrough
	case 8 << 20:
		fallthrough
	case 512 << 20:
		chk[filepath.Join(cacheDir, "sc-02-data-tree-r-last.dat")] = 0
	case 32 << 30:
		for i := 0; i < 8; i++ {
			chk[filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))] = 0
		}
	case 64 << 30:
		for i := 0; i < 16; i++ {
			chk[filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))] = 0
		}
	default:
		log.Warnf("not checking cache files of %s sectors for faults", ssize)
	}
}

var _ FaultTracker = &Manager{}
