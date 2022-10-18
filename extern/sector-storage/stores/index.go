package stores

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	gopath "path"
	"sort"
	"sync"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/lotus/extern/sector-storage/fsutil"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/lotus/metrics"
)

var HeartbeatInterval = 10 * time.Second
var SkippedHeartbeatThresh = HeartbeatInterval * 5

//go:generate go run github.com/golang/mock/mockgen -destination=mocks/index.go -package=mocks . SectorIndex

type SectorIndex interface { // part of storage-miner api
	StorageAttach(context.Context, storiface.StorageInfo, fsutil.FsStat) error
	StorageInfo(context.Context, storiface.ID) (storiface.StorageInfo, error)
	StorageReportHealth(context.Context, storiface.ID, storiface.HealthReport) error

	StorageDeclareSector(ctx context.Context, storageID storiface.ID, s abi.SectorID, ft storiface.SectorFileType, primary bool) error
	StorageDropSector(ctx context.Context, storageID storiface.ID, s abi.SectorID, ft storiface.SectorFileType) error
	StorageFindSector(ctx context.Context, sector abi.SectorID, ft storiface.SectorFileType, ssize abi.SectorSize, allowFetch bool) ([]storiface.SectorStorageInfo, error)

	StorageBestAlloc(ctx context.Context, allocate storiface.SectorFileType, ssize abi.SectorSize, pathType storiface.PathType) ([]storiface.StorageInfo, error)

	// atomically acquire locks on all sector file types. close ctx to unlock
	StorageLock(ctx context.Context, sector abi.SectorID, read storiface.SectorFileType, write storiface.SectorFileType) error
	StorageTryLock(ctx context.Context, sector abi.SectorID, read storiface.SectorFileType, write storiface.SectorFileType) (bool, error)
	StorageGetLocks(ctx context.Context) (storiface.SectorLocks, error)

	StorageList(ctx context.Context) (map[storiface.ID][]storiface.Decl, error)
	StorageListSelected(ctx context.Context, selected map[storiface.ID]string) (map[storiface.ID][]storiface.Decl, error)
}

type Decl struct {
	abi.SectorID
	storiface.SectorFileType
}

type declMeta struct {
	Storage storiface.ID
	Primary bool
}

type storageEntry struct {
	Info *storiface.StorageInfo
	Fsi  fsutil.FsStat

	LastHeartbeat time.Time
	HeartbeatErr  error
}

type Index struct {
	*indexLocks
	lk sync.RWMutex

	sectors map[storiface.Decl][]*declMeta
	stores  map[storiface.ID]*storageEntry
}

func NewIndex() *Index {
	return &Index{
		indexLocks: &indexLocks{
			locks: map[abi.SectorID]*sectorLock{},
		},
		sectors: map[storiface.Decl][]*declMeta{},
		stores:  map[storiface.ID]*storageEntry{},
	}
}

func (i *Index) StorageList(ctx context.Context) (map[storiface.ID][]storiface.Decl, error) {
	i.lk.RLock()
	defer i.lk.RUnlock()

	byID := map[storiface.ID]map[abi.SectorID]storiface.SectorFileType{}

	for id := range i.stores {
		byID[id] = map[abi.SectorID]storiface.SectorFileType{}
	}
	for decl, ids := range i.sectors {
		for _, id := range ids {
			byID[id.Storage][decl.SectorID] |= decl.SectorFileType
		}
	}

	out := map[storiface.ID][]storiface.Decl{}
	for id, m := range byID {
		out[id] = []storiface.Decl{}
		for sectorID, fileType := range m {
			out[id] = append(out[id], storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: fileType,
			})
		}
	}

	return out, nil
}

func (i *Index) StorageAttach(ctx context.Context, si storiface.StorageInfo, st fsutil.FsStat) error {
	i.lk.Lock()
	defer i.lk.Unlock()

	log.Infof("New sector storage: %s", si.ID)

	if _, ok := i.stores[si.ID]; ok {
		for _, u := range si.URLs {
			if _, err := url.Parse(u); err != nil {
				return xerrors.Errorf("failed to parse url %s: %w", si.URLs, err)
			}
		}

	uloop:
		for _, u := range si.URLs {
			for _, l := range i.stores[si.ID].Info.URLs {
				if u == l {
					continue uloop
				}
			}

			i.stores[si.ID].Info.URLs = append(i.stores[si.ID].Info.URLs, u)
		}

		i.stores[si.ID].Info.Weight = si.Weight
		i.stores[si.ID].Info.MaxStorage = si.MaxStorage
		i.stores[si.ID].Info.CanSeal = si.CanSeal
		i.stores[si.ID].Info.CanStore = si.CanStore
		i.stores[si.ID].Info.Groups = si.Groups
		i.stores[si.ID].Info.AllowTo = si.AllowTo

		return nil
	}
	i.stores[si.ID] = &storageEntry{
		Info: &si,
		Fsi:  st,

		LastHeartbeat: time.Now(),
	}
	return nil
}

func (i *Index) StorageReportHealth(ctx context.Context, id storiface.ID, report storiface.HealthReport) error {
	i.lk.Lock()
	defer i.lk.Unlock()

	ent, ok := i.stores[id]
	if !ok {
		return xerrors.Errorf("health report for unknown storage: %s", id)
	}

	ent.Fsi = report.Stat
	if report.Err != "" {
		ent.HeartbeatErr = errors.New(report.Err)
	} else {
		ent.HeartbeatErr = nil
	}
	ent.LastHeartbeat = time.Now()

	if report.Stat.Capacity > 0 {
		ctx, _ = tag.New(ctx, tag.Upsert(metrics.StorageID, string(id)))

		stats.Record(ctx, metrics.StorageFSAvailable.M(float64(report.Stat.FSAvailable)/float64(report.Stat.Capacity)))
		stats.Record(ctx, metrics.StorageAvailable.M(float64(report.Stat.Available)/float64(report.Stat.Capacity)))
		stats.Record(ctx, metrics.StorageReserved.M(float64(report.Stat.Reserved)/float64(report.Stat.Capacity)))

		stats.Record(ctx, metrics.StorageCapacityBytes.M(report.Stat.Capacity))
		stats.Record(ctx, metrics.StorageFSAvailableBytes.M(report.Stat.FSAvailable))
		stats.Record(ctx, metrics.StorageAvailableBytes.M(report.Stat.Available))
		stats.Record(ctx, metrics.StorageReservedBytes.M(report.Stat.Reserved))

		if report.Stat.Max > 0 {
			stats.Record(ctx, metrics.StorageLimitUsed.M(float64(report.Stat.Used)/float64(report.Stat.Max)))
			stats.Record(ctx, metrics.StorageLimitUsedBytes.M(report.Stat.Used))
			stats.Record(ctx, metrics.StorageLimitMaxBytes.M(report.Stat.Max))
		}
	}

	return nil
}

func (i *Index) StorageDeclareSector(ctx context.Context, storageID storiface.ID, s abi.SectorID, ft storiface.SectorFileType, primary bool) error {
	i.lk.Lock()
	defer i.lk.Unlock()

loop:
	for _, fileType := range storiface.PathTypes {
		if fileType&ft == 0 {
			continue
		}

		d := storiface.Decl{SectorID: s, SectorFileType: fileType}

		for _, sid := range i.sectors[d] {
			if sid.Storage == storageID {
				if !sid.Primary && primary {
					sid.Primary = true
				} else {
					log.Warnf("sector %v redeclared in %s", s, storageID)
				}
				continue loop
			}
		}

		i.sectors[d] = append(i.sectors[d], &declMeta{
			Storage: storageID,
			Primary: primary,
		})
	}

	return nil
}

func (i *Index) StorageDropSector(ctx context.Context, storageID storiface.ID, s abi.SectorID, ft storiface.SectorFileType) error {
	i.lk.Lock()
	defer i.lk.Unlock()

	for _, fileType := range storiface.PathTypes {
		if fileType&ft == 0 {
			continue
		}

		d := storiface.Decl{SectorID: s, SectorFileType: fileType}

		if len(i.sectors[d]) == 0 {
			continue
		}

		rewritten := make([]*declMeta, 0, len(i.sectors[d])-1)
		for _, sid := range i.sectors[d] {
			if sid.Storage == storageID {
				continue
			}

			rewritten = append(rewritten, sid)
		}
		if len(rewritten) == 0 {
			delete(i.sectors, d)
			continue
		}

		i.sectors[d] = rewritten
	}

	return nil
}

func (i *Index) StorageFindSector(ctx context.Context, s abi.SectorID, ft storiface.SectorFileType, ssize abi.SectorSize, allowFetch bool) ([]storiface.SectorStorageInfo, error) {
	i.lk.RLock()
	defer i.lk.RUnlock()

	storageIDs := map[storiface.ID]uint64{}
	isprimary := map[storiface.ID]bool{}

	allowTo := map[storiface.Group]struct{}{}

	for _, pathType := range storiface.PathTypes {
		if ft&pathType == 0 {
			continue
		}

		for _, id := range i.sectors[storiface.Decl{SectorID: s, SectorFileType: pathType}] {
			storageIDs[id.Storage]++
			isprimary[id.Storage] = isprimary[id.Storage] || id.Primary
		}
	}

	out := make([]storiface.SectorStorageInfo, 0, len(storageIDs))

	for id, n := range storageIDs {
		st, ok := i.stores[id]
		if !ok {
			log.Warnf("storage %s is not present in sector index (referenced by sector %v)", id, s)
			continue
		}

		urls, burls := make([]string, len(st.Info.URLs)), make([]string, len(st.Info.URLs))
		for k, u := range st.Info.URLs {
			rl, err := url.Parse(u)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse url: %w", err)
			}

			rl.Path = gopath.Join(rl.Path, ft.String(), storiface.SectorName(s))
			urls[k] = rl.String()
			burls[k] = u
		}

		if allowTo != nil && len(st.Info.AllowTo) > 0 {
			for _, group := range st.Info.AllowTo {
				allowTo[group] = struct{}{}
			}
		} else {
			allowTo = nil // allow to any
		}

		out = append(out, storiface.SectorStorageInfo{
			ID:       id,
			URLs:     urls,
			BaseURLs: burls,
			Weight:   st.Info.Weight * n, // storage with more sector types is better

			CanSeal:  st.Info.CanSeal,
			CanStore: st.Info.CanStore,

			Primary: isprimary[id],
		})
	}

	if allowFetch {
		spaceReq, err := ft.SealSpaceUse(ctx, ssize)
		if err != nil {
			return nil, xerrors.Errorf("estimating required space: %w", err)
		}

		for id, st := range i.stores {
			if !st.Info.CanSeal {
				continue
			}

			if spaceReq > uint64(st.Fsi.Available) {
				log.Debugf("not selecting on %s, out of space (available: %d, need: %d)", st.Info.ID, st.Fsi.Available, spaceReq)
				continue
			}

			if time.Since(st.LastHeartbeat) > SkippedHeartbeatThresh {
				log.Debugf("not selecting on %s, didn't receive heartbeats for %s", st.Info.ID, time.Since(st.LastHeartbeat))
				continue
			}

			if st.HeartbeatErr != nil {
				log.Debugf("not selecting on %s, heartbeat error: %s", st.Info.ID, st.HeartbeatErr)
				continue
			}

			if _, ok := storageIDs[id]; ok {
				continue
			}

			if allowTo != nil {
				allow := false
				for _, group := range st.Info.Groups {
					if _, found := allowTo[group]; found {
						log.Debugf("path %s in allowed group %s", st.Info.ID, group)
						allow = true
						break
					}
				}

				if !allow {
					log.Debugf("not selecting on %s, not in allowed group, allow %+v; path has %+v", st.Info.ID, allowTo, st.Info.Groups)
					continue
				}
			}

			urls, burls := make([]string, len(st.Info.URLs)), make([]string, len(st.Info.URLs))
			for k, u := range st.Info.URLs {
				rl, err := url.Parse(u)
				if err != nil {
					return nil, xerrors.Errorf("failed to parse url: %w", err)
				}

				rl.Path = gopath.Join(rl.Path, ft.String(), storiface.SectorName(s))
				urls[k] = rl.String()
				burls[k] = u
			}

			out = append(out, storiface.SectorStorageInfo{
				ID:       id,
				URLs:     urls,
				BaseURLs: burls,
				Weight:   st.Info.Weight * 0, // TODO: something better than just '0'

				CanSeal:  st.Info.CanSeal,
				CanStore: st.Info.CanStore,

				Primary: false,
			})
		}
	}

	return out, nil
}

func (i *Index) StorageInfo(ctx context.Context, id storiface.ID) (storiface.StorageInfo, error) {
	i.lk.RLock()
	defer i.lk.RUnlock()

	si, found := i.stores[id]
	if !found {
		return storiface.StorageInfo{}, xerrors.Errorf("sector store not found")
	}

	return *si.Info, nil
}

func (i *Index) StorageBestAlloc(ctx context.Context, allocate storiface.SectorFileType, ssize abi.SectorSize, pathType storiface.PathType) ([]storiface.StorageInfo, error) {
	i.lk.RLock()
	defer i.lk.RUnlock()

	var candidates []storageEntry

	var err error
	var spaceReq uint64
	switch pathType {
	case storiface.PathSealing:
		spaceReq, err = allocate.SealSpaceUse(ctx, ssize)
	case storiface.PathStorage:
		spaceReq, err = allocate.StoreSpaceUse(ssize)
	default:
		panic(fmt.Sprintf("unexpected pathType: %s", pathType))
	}
	if err != nil {
		return nil, xerrors.Errorf("estimating required space: %w", err)
	}

	for _, p := range i.stores {
		if (pathType == storiface.PathSealing) && !p.Info.CanSeal {
			continue
		}
		if (pathType == storiface.PathStorage) && !p.Info.CanStore {
			continue
		}

		if spaceReq > uint64(p.Fsi.Available) {
			log.Debugf("not allocating on %s, out of space (available: %d, need: %d)", p.Info.ID, p.Fsi.Available, spaceReq)
			continue
		}

		if time.Since(p.LastHeartbeat) > SkippedHeartbeatThresh {
			log.Debugf("not allocating on %s, didn't receive heartbeats for %s", p.Info.ID, time.Since(p.LastHeartbeat))
			continue
		}

		if p.HeartbeatErr != nil {
			log.Debugf("not allocating on %s, heartbeat error: %s", p.Info.ID, p.HeartbeatErr)
			continue
		}

		candidates = append(candidates, *p)
	}

	if len(candidates) == 0 {
		return nil, xerrors.New("no good path found")
	}

	sort.Slice(candidates, func(i, j int) bool {
		iw := big.Mul(big.NewInt(candidates[i].Fsi.Available), big.NewInt(int64(candidates[i].Info.Weight)))
		jw := big.Mul(big.NewInt(candidates[j].Fsi.Available), big.NewInt(int64(candidates[j].Info.Weight)))

		return iw.GreaterThan(jw)
	})

	out := make([]storiface.StorageInfo, len(candidates))
	for i, candidate := range candidates {
		out[i] = *candidate.Info
	}

	return out, nil
}

func (i *Index) FindSector(id abi.SectorID, typ storiface.SectorFileType) ([]storiface.ID, error) {
	i.lk.RLock()
	defer i.lk.RUnlock()

	f, ok := i.sectors[storiface.Decl{
		SectorID:       id,
		SectorFileType: typ,
	}]
	if !ok {
		return nil, nil
	}
	out := make([]storiface.ID, 0, len(f))
	for _, meta := range f {
		out = append(out, meta.Storage)
	}

	return out, nil
}

//var _ SectorIndex = &Index{}
var _ SectorIndex = &RedisIndex{}
