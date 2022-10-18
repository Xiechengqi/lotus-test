package stores

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/bits"
	"math/rand"
	"os"
	pathx "path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/go-state-types/proof"

	"golang.org/x/xerrors"

	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/extern/sector-storage/fsutil"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

// LocalStorageMeta [path]/sectorstore.json
type LocalStorageMeta struct {
	ID storiface.ID

	// A high weight means data is more likely to be stored in this path
	Weight uint64 // 0 = readonly

	// Intermediate data for the sealing process will be stored here
	CanSeal bool

	// Finalized sectors that will be proved over time will be stored here
	CanStore bool

	// MaxStorage specifies the maximum number of bytes to use for sector storage
	// (0 = unlimited)
	MaxStorage uint64

	// List of storage groups this path belongs to
	Groups []string

	// List of storage groups to which data from this path can be moved. If none
	// are specified, allow to all
	AllowTo []string
}

// StorageConfig .lotusstorage/storage.json
type StorageConfig struct {
	StoragePaths []LocalPath
}

type LocalPath struct {
	Path string
}

type LocalStorage interface {
	GetStorage() (StorageConfig, error)
	SetStorage(func(*StorageConfig)) error

	Stat(path string) (fsutil.FsStat, error)

	// returns real disk usage for a file/directory
	// os.ErrNotExit when file doesn't exist
	DiskUsage(path string) (int64, error)
}

const MetaFile = "sectorstore.json"

const MaxDiskUsage = int64(23 * 1024 * 1024 * 1024 * 1024)
const MinDiskUsage = int64(10 * 1024 * 1024 * 1024 * 1024)

type Local struct {
	localStorage LocalStorage
	index        SectorIndex
	urls         []string

	paths map[storiface.ID]*path

	localLk    sync.RWMutex
	finalizeLk sync.RWMutex
}

type path struct {
	local      string // absolute local path
	maxStorage uint64
	canStore   bool

	reserved     int64
	reservations map[abi.SectorID]storiface.SectorFileType
}

func (p *path) stat(ls LocalStorage) (fsutil.FsStat, error) {
	tsStart := build.Clock.Now()
	stat, err := ls.Stat(p.local)
	elapsed := time.Since(tsStart)
	log.Debugf("octopus: statfs %s elapse: %v", p.local, elapsed)

	if err != nil {
		return fsutil.FsStat{}, xerrors.Errorf("stat %s: %w", p.local, err)
	}

	stat.Reserved = p.reserved

	for id, ft := range p.reservations {
		for _, fileType := range storiface.PathTypes {
			if fileType&ft == 0 {
				continue
			}

			sp := p.sectorPath(id, fileType)

			used, err := ls.DiskUsage(sp)
			if err == os.ErrNotExist {
				p, ferr := tempFetchDest(sp, false)
				if ferr != nil {
					return fsutil.FsStat{}, ferr
				}

				used, err = ls.DiskUsage(p)
			}
			if err != nil {
				// we don't care about 'not exist' errors, as storage can be
				// reserved before any files are written, so this error is not
				// unexpected
				if !os.IsNotExist(err) {
					log.Warnf("getting disk usage of '%s': %+v", p.sectorPath(id, fileType), err)
				}
				continue
			}

			stat.Reserved -= used
		}
	}

	if stat.Reserved < 0 {
		log.Warnf("negative reserved storage: p.reserved=%d, reserved: %d", p.reserved, stat.Reserved)
		stat.Reserved = 0
	}

	stat.Available -= stat.Reserved
	if stat.Available < 0 {
		stat.Available = 0
	}

	if p.maxStorage > 0 {
		used, err := ls.DiskUsage(p.local)
		if err != nil {
			return fsutil.FsStat{}, err
		}

		stat.Max = int64(p.maxStorage)
		stat.Used = used

		avail := int64(p.maxStorage) - used
		if uint64(used) > p.maxStorage {
			avail = 0
		}

		if avail < stat.Available {
			stat.Available = avail
		}
	}

	return stat, err
}

func (p *path) sectorPath(sid abi.SectorID, fileType storiface.SectorFileType) string {
	return filepath.Join(p.local, fileType.String(), storiface.SectorName(sid))
}

type URLs []string

func NewLocal(ctx context.Context, ls LocalStorage, index SectorIndex, urls []string) (*Local, error) {
	l := &Local{
		localStorage: ls,
		index:        index,
		urls:         urls,

		paths: map[storiface.ID]*path{},
	}
	return l, l.open(ctx)
}

func (st *Local) OpenPath(ctx context.Context, p string) error {
	st.localLk.Lock()
	defer st.localLk.Unlock()

	mb, err := ioutil.ReadFile(filepath.Join(p, MetaFile))
	if err != nil {
		return xerrors.Errorf("reading storage metadata for %s: %w", p, err)
	}

	var meta LocalStorageMeta
	if err := json.Unmarshal(mb, &meta); err != nil {
		return xerrors.Errorf("unmarshalling storage metadata for %s: %w", p, err)
	}

	// TODO: Check existing / dedupe

	out := &path{
		local: p,

		maxStorage:   meta.MaxStorage,
		canStore:     meta.CanStore,
		reserved:     0,
		reservations: map[abi.SectorID]storiface.SectorFileType{},
	}

	fst, err := out.stat(st.localStorage)
	if err != nil {
		return err
	}

	err = st.index.StorageAttach(ctx, storiface.StorageInfo{
		ID:         meta.ID,
		URLs:       st.urls,
		Weight:     meta.Weight,
		MaxStorage: meta.MaxStorage,
		CanSeal:    meta.CanSeal,
		CanStore:   meta.CanStore,
		Groups:     meta.Groups,
		AllowTo:    meta.AllowTo,
	}, fst)
	if err != nil {
		return xerrors.Errorf("declaring storage in index: %w", err)
	}

	ifDeclareSectors := os.Getenv("DECLARE_SECTORS")
	log.Infof("ENV DECLARE_SECTORS: %s", ifDeclareSectors)
	if ifDeclareSectors == "true" {
		if err := st.declareSectors(ctx, p, meta.ID, meta.CanStore); err != nil {
			return err
		}
	}

	st.paths[meta.ID] = out

	return nil
}

func (st *Local) open(ctx context.Context) error {
	cfg, err := st.localStorage.GetStorage()
	if err != nil {
		return xerrors.Errorf("getting local storage config: %w", err)
	}

	for _, path := range cfg.StoragePaths {
		err := st.OpenPath(ctx, path.Path)
		if err != nil {
			return xerrors.Errorf("opening path %s: %w", path.Path, err)
		}
	}

	go st.reportHealth(ctx)

	return nil
}

func (st *Local) Redeclare(ctx context.Context) error {
	st.localLk.Lock()
	defer st.localLk.Unlock()

	for id, p := range st.paths {
		mb, err := ioutil.ReadFile(filepath.Join(p.local, MetaFile))
		if err != nil {
			return xerrors.Errorf("reading storage metadata for %s: %w", p.local, err)
		}

		var meta LocalStorageMeta
		if err := json.Unmarshal(mb, &meta); err != nil {
			return xerrors.Errorf("unmarshalling storage metadata for %s: %w", p.local, err)
		}

		fst, err := p.stat(st.localStorage)
		if err != nil {
			return err
		}

		if id != meta.ID {
			log.Errorf("storage path ID changed: %s; %s -> %s", p.local, id, meta.ID)
			continue
		}

		err = st.index.StorageAttach(ctx, storiface.StorageInfo{
			ID:         id,
			URLs:       st.urls,
			Weight:     meta.Weight,
			MaxStorage: meta.MaxStorage,
			CanSeal:    meta.CanSeal,
			CanStore:   meta.CanStore,
			Groups:     meta.Groups,
			AllowTo:    meta.AllowTo,
		}, fst)
		if err != nil {
			return xerrors.Errorf("redeclaring storage in index: %w", err)
		}

		if err := st.declareSectors(ctx, p.local, meta.ID, meta.CanStore); err != nil {
			return xerrors.Errorf("redeclaring sectors: %w", err)
		}
	}

	return nil
}

func (st *Local) loadQiniuSectors(ctx context.Context, p string, id storiface.ID, primary bool) error {
	//加载扇区逻辑由默认加载改为显示打开开关才能加载，避免lotus-worker也去加载扇区，用法如下：
	//QINIU=/root/cfg.toml QINIU_LOAD_SECTORS=true ./lotus-miner run
	sectors := ffiwrapper.QiniuLoadSectors(p)
	for _, v := range sectors {
		if ffiwrapper.CheckFetching(p, v) {
			log.Debugf("QINIU skip fetching directory  %d(t:%d) -> %s", v, id)
			continue
		}
		//加载sealed文件
		if strings.Contains(v, storiface.FTSealed.String()) {
			sector, err := storiface.ParseSectorID(pathx.Base(v))
			if err != nil {
				continue
			}
			log.Debugf("QINIU load sector %s %s", sector, v)
			//declare sealed
			if err := st.index.StorageDeclareSector(ctx, id, sector, storiface.FTSealed, primary); err != nil {
				return xerrors.Errorf("QINIU declare sector %s(t:%d) -> %s: %w", v, storiface.FTSealed, id, err)
			}

			// declare cache
			if err := st.index.StorageDeclareSector(ctx, id, sector, storiface.FTCache, primary); err != nil {
				return xerrors.Errorf("QINIU declare sector cache %s(t:%d) -> %s: %w", v, storiface.FTCache, id, err)
			}
		}
		//加载cache文件
		// if strings.Contains(v, storiface.FTCache.String()) && strings.Contains(v, "p_aux") {
		// 	sector, err := storiface.ParseSectorID(pathx.Base(pathx.Dir(v)))
		// 	if err != nil {
		// 		continue
		// 	}
		// 	log.Debugf("QINIU load sector %s %s", sector, v)
		// 	if err := st.index.StorageDeclareSector(ctx, id, sector, storiface.FTCache, primary); err != nil {
		// 		return xerrors.Errorf("QINIU declare sector %s(t:%d) -> %s: %w", v, storiface.FTCache, id, err)
		// 	}
		// }
		// //加载unsealed文件
		// if strings.Contains(v, storiface.FTUnsealed.String()) {
		// 	sector, err := storiface.ParseSectorID(pathx.Base(v))
		// 	if err != nil {
		// 		continue
		// 	}

		// 	if err := st.index.StorageDeclareSector(ctx, id, sector, storiface.FTUnsealed, primary); err != nil {
		// 		return xerrors.Errorf("declare sector %s(t:%d) -> %s: %w", v, storiface.FTUnsealed, id, err)
		// 	}
		// }
	}
	return nil
}

func (st *Local) declareSectors(ctx context.Context, p string, id storiface.ID, primary bool) error {
	if ffiwrapper.QiniuFeatureEnabled(ffiwrapper.QiniuFeatureLoadSectors) {
		if err := st.loadQiniuSectors(ctx, p, id, primary); err != nil {
			return err
		}
	}
	for _, t := range storiface.PathTypes {
		ents, err := ioutil.ReadDir(filepath.Join(p, t.String()))
		if err != nil {
			if os.IsNotExist(err) {
				if err := os.MkdirAll(filepath.Join(p, t.String()), 0755); err != nil { // nolint
					return xerrors.Errorf("openPath mkdir '%s': %w", filepath.Join(p, t.String()), err)
				}

				continue
			}
			return xerrors.Errorf("listing %s: %w", filepath.Join(p, t.String()), err)
		}

		for _, ent := range ents {
			if ent.Name() == fetchTempSubdir {
				continue
			}
			if strings.HasSuffix(ent.Name(), ".tmp") {
				continue
			}

			sid, err := storiface.ParseSectorID(ent.Name())
			if err != nil {
				return xerrors.Errorf("parse sector id %s: %w", ent.Name(), err)
			}

			if err := st.index.StorageDeclareSector(ctx, id, sid, t, primary); err != nil {
				return xerrors.Errorf("declare sector %d(t:%d) -> %s: %w", sid, t, id, err)
			}
		}
	}

	return nil
}

func (st *Local) DeclareSectors(ctx context.Context, s []abi.SectorID) (map[abi.SectorNumber][]storiface.SectorFileType, error) {
	result := make(map[abi.SectorNumber][]storiface.SectorFileType)
	for id, value := range st.paths { //遍历存储
		stinfo, err := st.index.StorageInfo(ctx, id)
		if err != nil {
			log.Errorw("DeclareSectors--", "storageid:", id, "err:", err)
			continue
		}
		if stinfo.CanStore == false {
			continue
		}

		for _, t := range storiface.PathTypes { //遍历PathTypes
			path := filepath.Join(value.local, t.String())
			for _, v := range s { //需要加载到内存的扇区
				temp := fmt.Sprintf("s-t0%d-%d", v.Miner, v.Number)
				realpath := filepath.Join(path, temp)
				if _, err := os.Stat(realpath); err != nil {
					continue
				} else {
					if err := st.index.StorageDeclareSector(ctx, id, v, t, stinfo.CanStore); err != nil {
						log.Errorw("DeclareSectorsErr---", "realpath:", realpath, "err:", err)
					} else {
						if _, ok := result[v.Number]; !ok {
							result[v.Number] = []storiface.SectorFileType{t}
						} else {
							result[v.Number] = append(result[v.Number], t)
						}
						log.Debugw("DeclareSectorsSuccess---", "realpath:", realpath)
					}
				}
			}
		}

	}
	return result, nil
}

func (st *Local) reportHealth(ctx context.Context) {
	// randomize interval by ~10%
	intervalStr := os.Getenv("STORAGE_HEARTBEAT_INTERVAL")
	log.Infof("octopus: env STORAGE_HEARTBEAT_INTERVAL=%s", intervalStr)
	var hi time.Duration
	if intervalStr == "" {
		hi = HeartbeatInterval
	} else {
		intervalInt, err := strconv.Atoi(intervalStr)
		if err != nil {
			log.Errorf("octopus: invalid STORAGE_HEARTBEAT_INTERVAL %s", intervalStr)
			hi = HeartbeatInterval
		} else {
			hi = time.Duration(intervalInt) * time.Second
		}
	}
	interval := (hi*100_000 + time.Duration(rand.Int63n(10_000))) / 100_000

	for {
		select {
		case <-time.After(interval):
		case <-ctx.Done():
			return
		}

		st.reportStorage(ctx)
	}
}

func (st *Local) reportStorage(ctx context.Context) {
	st.localLk.RLock()

	toReport := map[storiface.ID]storiface.HealthReport{}
	for id, p := range st.paths {
		stat, err := p.stat(st.localStorage)
		r := storiface.HealthReport{Stat: stat}
		if err != nil {
			r.Err = err.Error()
		}

		toReport[id] = r
	}

	st.localLk.RUnlock()

	for id, report := range toReport {
		if err := st.index.StorageReportHealth(ctx, id, report); err != nil {
			log.Warnf("error reporting storage health for %s (%+v): %+v", id, report, err)
		}
	}
}

func (st *Local) Reserve(ctx context.Context, sid storage.SectorRef, ft storiface.SectorFileType, storageIDs storiface.SectorPaths, overheadTab map[storiface.SectorFileType]int) (func(), error) {
	ssize, err := sid.ProofType.SectorSize()
	if err != nil {
		return nil, err
	}

	st.localLk.Lock()

	done := func() {}
	deferredDone := func() { done() }
	defer func() {
		st.localLk.Unlock()
		deferredDone()
	}()

	for _, fileType := range storiface.PathTypes {
		if fileType&ft == 0 {
			continue
		}

		id := storiface.ID(storiface.PathByType(storageIDs, fileType))

		p, ok := st.paths[id]
		if !ok {
			return nil, errPathNotFound
		}

		stat, err := p.stat(st.localStorage)
		if err != nil {
			return nil, xerrors.Errorf("getting local storage stat: %w", err)
		}

		//tsStart := build.Clock.Now()
		//used, err := st.localStorage.DiskUsage(p.local)
		//elapsed := time.Since(tsStart)
		//log.Infof("octopus: Reserve DiskUsage elapsed %v", elapsed)

		//if err != nil {
		//	log.Errorf("failed to get disk usage %s, %+v", p.local, err)
		//	return nil, storiface.Err(storiface.ErrTempAllocateSpace, err)
		//} else {
		//maxDiskUsage := MaxDiskUsage
		//maxDiskUsageStr := os.Getenv("MAX_DISK_USAGE")
		//if maxDiskUsageStr != "" {
		//	m, err := strconv.ParseInt(maxDiskUsageStr, 10, 64)
		//	if err == nil {
		//		maxDiskUsage = m
		//	}
		//}
		//if used > maxDiskUsage {
		//	log.Warnf("Current disk usage: %d(bytes)", used)
		//	return nil, storiface.Err(storiface.ErrTempAllocateSpace, xerrors.Errorf(storiface.NotEnoughSpace))
		//} else {
		//	log.Infof("Current disk usage: %d(bytes)", used)
		//}
		//
		//if retErr != nil {
		//	minDiskUsage := MinDiskUsage
		//	minDiskUsageStr := os.Getenv("MIN_DISK_USAGE")
		//	if minDiskUsageStr != "" {
		//		m, err := strconv.ParseInt(minDiskUsageStr, 10, 64)
		//		if err == nil {
		//			minDiskUsage = m
		//		}
		//	}
		//	if used < minDiskUsage {
		//		if os.Getenv("PRESERVED_ABILITY") != "" {
		//			retErr = xerrors.Errorf(storiface.LowUsedSpace)
		//		}
		//	}
		//}
		//}

		overhead := int64(overheadTab[fileType]) * int64(ssize) / storiface.FSOverheadDen

		if stat.Available < overhead {
			return nil, storiface.Err(storiface.ErrTempAllocateSpace, xerrors.Errorf("can't reserve %d bytes in '%s' (id:%s), only %d available", overhead, p.local, id, stat.Available))
		}

		p.reserved += overhead
		p.reservations[sid.ID] |= fileType

		prevDone := done
		saveFileType := fileType
		done = func() {
			prevDone()

			st.localLk.Lock()
			defer st.localLk.Unlock()

			p.reserved -= overhead
			p.reservations[sid.ID] ^= saveFileType
			if p.reservations[sid.ID] == storiface.FTNone {
				delete(p.reservations, sid.ID)
			}
		}
	}

	deferredDone = func() {}
	return done, nil
}

func (st *Local) AcquireSector(ctx context.Context, sid storage.SectorRef, existing storiface.SectorFileType, allocate storiface.SectorFileType, pathType storiface.PathType, op storiface.AcquireMode) (storiface.SectorPaths, storiface.SectorPaths, error) {
	if existing|allocate != existing^allocate {
		return storiface.SectorPaths{}, storiface.SectorPaths{}, xerrors.New("can't both find and allocate a sector")
	}

	ssize, err := sid.ProofType.SectorSize()
	if err != nil {
		return storiface.SectorPaths{}, storiface.SectorPaths{}, err
	}

	st.localLk.RLock()
	defer st.localLk.RUnlock()

	var out storiface.SectorPaths
	var storageIDs storiface.SectorPaths

	ept := ctx.Value("pathTypeForExisting")
	pathTypeForExisting := false
	if p, ok := ept.(bool); ok {
		if p {
			log.Debugf("octopus: context existingPathType=%v", ept)
			pathTypeForExisting = true
		}
	}

	for _, fileType := range storiface.PathTypes {
		if fileType&existing == 0 {
			continue
		}

		log.Debugf("octopus: StorageFindSector file type %v: sector %v", fileType, sid.ID.Number)
		si, err := st.index.StorageFindSector(ctx, sid.ID, fileType, ssize, false)
		if err != nil {
			log.Warnf("finding existing sector %d(t:%d) failed: %+v", sid, fileType, err)
			continue
		}

		for _, info := range si {
			log.Debugf("octopus: StorageFindSector file type %v: sector %v storage id %v", fileType, sid.ID.Number, info.ID)
			p, ok := st.paths[info.ID]
			log.Debugf("octopus: StorageFindSector file type %v: sector %v path %v", fileType, sid.ID.Number, p)
			if !ok {
				continue
			}

			if p.local == "" { // TODO: can that even be the case?
				log.Debugf("octopus: p.local is empty")
				continue
			}

			if pathTypeForExisting {
				if (pathType == storiface.PathSealing) && !info.CanSeal {
					log.Debugf("octopus: existing path type %v, %v can't seal", pathType, info.ID)
					continue
				}

				if (pathType == storiface.PathStorage) && !info.CanStore {
					log.Debugf("octopus: existing path type %v, %v can't store", pathType, info.ID)
					continue
				}
			}

			spath := p.sectorPath(sid.ID, fileType)
			storiface.SetPathByType(&out, fileType, spath)
			storiface.SetPathByType(&storageIDs, fileType, string(info.ID))

			existing ^= fileType
			break
		}
	}

	for _, fileType := range storiface.PathTypes {
		if fileType&allocate == 0 {
			continue
		}

		sis, err := st.index.StorageBestAlloc(ctx, fileType, ssize, pathType)
		log.Debugf("octopus: StorageBestAlloc pathType=%v fileType=%v len(sis)=%d", pathType, fileType, len(sis))
		if err != nil {
			return storiface.SectorPaths{}, storiface.SectorPaths{}, xerrors.Errorf("finding best storage for allocating : %w", err)
		}

		var best string
		var bestID storiface.ID

		for _, si := range sis {
			p, ok := st.paths[si.ID]
			log.Debugf("octopus: check sis: %v", si.ID)
			if !ok {
				log.Debugf("octopus: sis: %v not in paths", si.ID)
				continue
			}

			if p.local == "" { // TODO: can that even be the case?
				log.Debugf("octopus: %v local is empty", si.ID)
				continue
			}

			if (pathType == storiface.PathSealing) && !si.CanSeal {
				log.Debugf("octopus: %v can't seal", si.ID)
				continue
			}

			if (pathType == storiface.PathStorage) && !si.CanStore {
				log.Debugf("octopus: %v can't store", si.ID)
				continue
			}

			// TODO: Check free space

			best = p.sectorPath(sid.ID, fileType)
			bestID = si.ID
			break
		}

		if best == "" {
			return storiface.SectorPaths{}, storiface.SectorPaths{}, xerrors.Errorf("couldn't find a suitable path for a sector")
		}

		storiface.SetPathByType(&out, fileType, best)
		storiface.SetPathByType(&storageIDs, fileType, string(bestID))
		allocate ^= fileType
	}

	return out, storageIDs, nil
}

func (st *Local) Local(ctx context.Context) ([]storiface.StoragePath, error) {
	st.localLk.RLock()
	defer st.localLk.RUnlock()

	var out []storiface.StoragePath
	for id, p := range st.paths {
		if p.local == "" {
			continue
		}

		si, err := st.index.StorageInfo(ctx, id)
		if err != nil {
			return nil, xerrors.Errorf("get storage info for %s: %w", id, err)
		}

		out = append(out, storiface.StoragePath{
			ID:        id,
			Weight:    si.Weight,
			LocalPath: p.local,
			CanSeal:   si.CanSeal,
			CanStore:  si.CanStore,
		})
	}

	return out, nil
}

func (st *Local) LocalPaths(ctx context.Context) ([]storiface.StoragePath, error) {
	st.localLk.RLock()
	defer st.localLk.RUnlock()

	var out []storiface.StoragePath
	for id, p := range st.paths {
		if p.local == "" {
			continue
		}

		out = append(out, storiface.StoragePath{
			ID:        id,
			LocalPath: p.local,
			CanStore:  p.canStore,
		})
	}
	return out, nil
}

func (st *Local) Remove(ctx context.Context, sid abi.SectorID, typ storiface.SectorFileType, force bool, keepIn []storiface.ID) error {
	if bits.OnesCount(uint(typ)) != 1 {
		return xerrors.New("delete expects one file type")
	}

	si, err := st.index.StorageFindSector(ctx, sid, typ, 0, false)
	if err != nil {
		return xerrors.Errorf("finding existing sector %d(t:%d) failed: %w", sid, typ, err)
	}

	if len(si) == 0 && !force {
		return xerrors.Errorf("can't delete sector %v(%d), not found", sid, typ)
	}

storeLoop:
	for _, info := range si {
		for _, id := range keepIn {
			if id == info.ID {
				continue storeLoop
			}
		}

		if err := st.removeSector(ctx, sid, typ, info.ID); err != nil {
			return err
		}
	}

	return nil
}

func (st *Local) RemoveCopies(ctx context.Context, sid abi.SectorID, typ storiface.SectorFileType) error {
	if bits.OnesCount(uint(typ)) != 1 {
		return xerrors.New("delete expects one file type")
	}

	si, err := st.index.StorageFindSector(ctx, sid, typ, 0, false)
	if err != nil {
		return xerrors.Errorf("finding existing sector %d(t:%d) failed: %w", sid, typ, err)
	}

	var hasPrimary bool
	for _, info := range si {
		if info.Primary {
			hasPrimary = true
			break
		}
	}

	if !hasPrimary {
		log.Warnf("RemoveCopies: no primary copies of sector %v (%s), not removing anything", sid, typ)
		return nil
	}

	for _, info := range si {
		if info.Primary {
			continue
		}

		if err := st.removeSector(ctx, sid, typ, info.ID); err != nil {
			return err
		}
	}

	return nil
}

func (st *Local) removeSector(ctx context.Context, sid abi.SectorID, typ storiface.SectorFileType, storage storiface.ID) error {
	p, ok := st.paths[storage]
	if !ok {
		return nil
	}

	forceRemove := false
	sp := ctx.Value("forceRemove")
	if p, ok := sp.(bool); ok {
		if p {
			forceRemove = true
		}
	}

	removeCanStore := os.Getenv("REMOVE_CAN_STORE")
	log.Debugf("octopus: env REMOVE_CAN_STORE=%s", removeCanStore)
	if !forceRemove && removeCanStore != "true" && p.canStore {
		log.Infof("octopus: storage id(%s) path(%s) can store, cancel remove.", storage, p.local)
		return nil
	}

	if p.local == "" { // TODO: can that even be the case?
		return nil
	}

	if err := st.index.StorageDropSector(ctx, storage, sid, typ); err != nil {
		return xerrors.Errorf("dropping sector from index: %w", err)
	}

	spath := p.sectorPath(sid, typ)
	log.Infof("remove %s", spath)

	if err := os.RemoveAll(spath); err != nil {
		log.Errorf("removing sector (%v) from %s: %+v", sid, spath, err)
	}

	st.reportStorage(ctx) // report freed space

	return nil
}

func (st *Local) MoveStorage(ctx context.Context, s storage.SectorRef, types storiface.SectorFileType) error {
	log.Debugf("octopus: MoveStorage %v: wait lock", s.ID.Number)
	st.finalizeLk.Lock()
	defer st.finalizeLk.Unlock()

	log.Debugf("octopus: MoveStorage %v: lock acquired", s.ID.Number)

	dest, destIds, err := st.AcquireSector(ctx, s, storiface.FTNone, types, storiface.PathStorage, storiface.AcquireMove)
	if err != nil {
		return xerrors.Errorf("acquire dest storage: %w", err)
	}

	src, srcIds, err := st.AcquireSector(ctx, s, types, storiface.FTNone, storiface.PathStorage, storiface.AcquireMove)
	if err != nil {
		return xerrors.Errorf("acquire src storage: %w", err)
	}

	tsStart := build.Clock.Now()
	log.Infof("octopus: MoveStorage started, sector ID: %v.", s.ID.Number)

	for _, fileType := range storiface.PathTypes {
		if fileType&types == 0 {
			continue
		}

		sst, err := st.index.StorageInfo(ctx, storiface.ID(storiface.PathByType(srcIds, fileType)))
		if err != nil {
			return xerrors.Errorf("failed to get source storage info: %w", err)
		}

		dst, err := st.index.StorageInfo(ctx, storiface.ID(storiface.PathByType(destIds, fileType)))
		if err != nil {
			return xerrors.Errorf("failed to get source storage info: %w", err)
		}

		if sst.ID == dst.ID {
			log.Debugf("not moving %v(%d); src and dest are the same", s, fileType)
			continue
		}

		if sst.CanStore {
			log.Debugf("not moving %v(%d); source supports storage", s, fileType)
			continue
		}

		log.Debugf("moving %v(%d) to storage: %s(se:%t; st:%t) -> %s(se:%t; st:%t)", s, fileType, sst.ID, sst.CanSeal, sst.CanStore, dst.ID, dst.CanSeal, dst.CanStore)

		//if err := move(storiface.PathByType(src, fileType), storiface.PathByType(dest, fileType)); err != nil {
		//	// TODO: attempt some recovery (check if src is still there, re-declare)
		//	return xerrors.Errorf("moving sector %v(%d): %w", s, fileType, err)
		//}
		srcPath := storiface.PathByType(src, fileType)

		srcSizeInfo, err := fsutil.FileStatSize(srcPath)
		if err != nil {
			return xerrors.Errorf("filesize src path %s error: %w", srcPath, err)
		}
		log.Infof("octopus: src %s file size is %d", srcPath, srcSizeInfo)

		destPath := storiface.PathByType(dest, fileType)
		//tmpPath := fmt.Sprintf("%s.%v.%s", destPath, time.Now().UnixNano(), "tmp")

		storageType := os.Getenv("STORAGE_TYPE")
		var tmpPath string
		if storageType == "QINIU" {
			tmpPath = filepath.Join(filepath.Dir(destPath), "fetching", filepath.Base(destPath))
		} else {
			tmpPath = filepath.Join(filepath.Dir(filepath.Dir(destPath)), "tmp", fmt.Sprintf("%s.%v.%s", filepath.Base(destPath), time.Now().UnixNano(), fileType.String()))
		}

		log.Infof("octopus: copying sector %s -> %s ...", srcPath, tmpPath)
		if err := copy2(srcPath, tmpPath); err != nil {
			return xerrors.Errorf("copying sector %s -> %s: %w", srcPath, tmpPath, err)
		}

		time.Sleep(10 * time.Second)
		// check stat
		copySuccess := false
		retries := 5
		for i := 1; i <= retries; i++ {
			var tmpSizeInfo int64 = 0
			fi, err := os.Lstat(tmpPath)
			if err == nil {
				if fi.IsDir() {
					log.Debugf("octopus: %s is a dir", tmpPath)
					fis, err := ioutil.ReadDir(srcPath)
					if err == nil {
						for _, fi := range fis {
							fii, err := os.Lstat(filepath.Join(tmpPath, fi.Name()))
							log.Debugf("octopus: lstat %s", filepath.Join(tmpPath, fi.Name()))
							if err == nil {
								stat, ok := fii.Sys().(*syscall.Stat_t)
								if !ok {
									err = xerrors.New("FileInfo.Sys of wrong type")
									break
								} else {
									log.Debugf("octopus: %s size=%d", filepath.Join(tmpPath, fi.Name()), stat.Size)
									tmpSizeInfo += stat.Size
								}
							} else {
								break
							}
						}
						log.Debugf("octopus: %s dir size=%d", tmpPath, tmpSizeInfo)
					}
				} else {
					stat, ok := fi.Sys().(*syscall.Stat_t)
					if !ok {
						err = xerrors.New("FileInfo.Sys of wrong type")
					} else {
						tmpSizeInfo = stat.Size
					}
					log.Debugf("octopus: %s is not dir, size=%d", tmpPath, tmpSizeInfo)
				}
			}
			//tmpSizeInfo, err := fsutil.FileStatSize(tmpPath)
			if err != nil {
				log.Errorf("octopus: filesize %s err: %v, retry in 10 secs", tmpPath, err)
				time.Sleep(60 * time.Second)
			} else if srcSizeInfo.OnDisk != tmpSizeInfo {
				log.Errorf("octopus: filesize wrong: %s size=%d", tmpPath, tmpSizeInfo)
				time.Sleep(60 * time.Second)
			} else {
				copySuccess = true
				break
			}
		}

		if !copySuccess {
			log.Errorf("octopus: stat fail. removing %s ...", tmpPath)
			if removeErr := os.RemoveAll(tmpPath); removeErr != nil {
				log.Errorf("octopus: stat %s fail: remove %s error: %v", tmpPath, tmpPath, removeErr)
			}
			return xerrors.Errorf("octopus: after stat check, copy sector %s -> %s fail", srcPath, tmpPath)
		}

		log.Infof("octopus: moving %s -> %s", tmpPath, destPath)
		if err := move2(tmpPath, destPath); err != nil {
			return xerrors.Errorf("move sector %s -> %s: %w", tmpPath, destPath, err)
		}

		if err := st.index.StorageDropSector(ctx, storiface.ID(storiface.PathByType(srcIds, fileType)), s.ID, fileType); err != nil {
			return xerrors.Errorf("dropping source sector from index: %w", err)
		}

		if err := st.index.StorageDeclareSector(ctx, storiface.ID(storiface.PathByType(destIds, fileType)), s.ID, fileType, true); err != nil {
			return xerrors.Errorf("declare sector %d(t:%d) -> %s: %w", s, fileType, storiface.ID(storiface.PathByType(destIds, fileType)), err)
		}

		log.Infof("octopus: removing %s ...", srcPath)
		if err := os.RemoveAll(srcPath); err != nil {
			log.Warnf("octopus: remove %s failed: %s", srcPath, err)
		}
	}

	log.Infof("octopus: MoveStorage done, sector ID: %v. elapse %v.", s.ID.Number, time.Since(tsStart))

	st.reportStorage(ctx) // report space use changes

	return nil
}

var errPathNotFound = xerrors.Errorf("fsstat: path not found")

func (st *Local) FsStat(ctx context.Context, id storiface.ID) (fsutil.FsStat, error) {
	st.localLk.RLock()
	defer st.localLk.RUnlock()

	p, ok := st.paths[id]
	if !ok {
		return fsutil.FsStat{}, errPathNotFound
	}

	return p.stat(st.localStorage)
}

func (st *Local) GenerateSingleVanillaProof(ctx context.Context, minerID abi.ActorID, si storiface.PostSectorChallenge, ppt abi.RegisteredPoStProof) ([]byte, error) {
	sr := storage.SectorRef{
		ID: abi.SectorID{
			Miner:  minerID,
			Number: si.SectorNumber,
		},
		ProofType: si.SealProof,
	}

	var cache string
	var sealed string
	if si.Update {
		src, _, err := st.AcquireSector(ctx, sr, storiface.FTUpdate|storiface.FTUpdateCache, storiface.FTNone, storiface.PathStorage, storiface.AcquireMove)
		if err != nil {
			return nil, xerrors.Errorf("acquire sector: %w", err)
		}
		cache, sealed = src.UpdateCache, src.Update
	} else {
		src, _, err := st.AcquireSector(ctx, sr, storiface.FTSealed|storiface.FTCache, storiface.FTNone, storiface.PathStorage, storiface.AcquireMove)
		if err != nil {
			return nil, xerrors.Errorf("acquire sector: %w", err)
		}
		cache, sealed = src.Cache, src.Sealed
	}

	if sealed == "" || cache == "" {
		return nil, errPathNotFound
	}

	psi := ffi.PrivateSectorInfo{
		SectorInfo: proof.SectorInfo{
			SealProof:    si.SealProof,
			SectorNumber: si.SectorNumber,
			SealedCID:    si.SealedCID,
		},
		CacheDirPath:     cache,
		PoStProofType:    ppt,
		SealedSectorPath: sealed,
	}

	return ffi.GenerateSingleVanillaProof(psi, si.Challenge)
}

var _ Store = &Local{}
