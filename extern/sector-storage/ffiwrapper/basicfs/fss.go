package basicfs

import (
	"context"
	"os"
	"path/filepath"
	"sync"

	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type RootsProvider struct {
	Roots []string

	lk         sync.Mutex
	waitSector map[sectorFile]chan struct{}
}

func (b *RootsProvider) AcquireSector(ctx context.Context, id storage.SectorRef, existing storiface.SectorFileType, allocate storiface.SectorFileType, ptype storiface.PathType) (storiface.SectorPaths, func(), error) {
	for _, v := range b.Roots {
		if err := os.Mkdir(filepath.Join(v, storiface.FTUnsealed.String()), 0755); err != nil && !os.IsExist(err) { // nolint
			return storiface.SectorPaths{}, nil, err
		}
		if err := os.Mkdir(filepath.Join(v, storiface.FTSealed.String()), 0755); err != nil && !os.IsExist(err) { // nolint
			return storiface.SectorPaths{}, nil, err
		}
		if err := os.Mkdir(filepath.Join(v, storiface.FTCache.String()), 0755); err != nil && !os.IsExist(err) { // nolint
			return storiface.SectorPaths{}, nil, err
		}
	}

	done := func() {}

	out := storiface.SectorPaths{
		ID: id.ID,
	}

	for _, fileType := range storiface.PathTypes {
		if !existing.Has(fileType) && !allocate.Has(fileType) {
			continue
		}

		b.lk.Lock()
		if b.waitSector == nil {
			b.waitSector = map[sectorFile]chan struct{}{}
		}
		ch, found := b.waitSector[sectorFile{id.ID, fileType}]
		if !found {
			ch = make(chan struct{}, 1)
			b.waitSector[sectorFile{id.ID, fileType}] = ch
		}
		b.lk.Unlock()

		select {
		case ch <- struct{}{}:
		case <-ctx.Done():
			done()
			return storiface.SectorPaths{}, nil, ctx.Err()
		}

		flag := false

		prevDone := done
		done = func() {
			prevDone()
			<-ch
		}

		for _, v := range b.Roots {
			path := filepath.Join(v, fileType.String(), storiface.SectorName(id.ID))
			if !allocate.Has(fileType) {
				if _, err := os.Stat(path); os.IsNotExist(err) {
					continue
				}
			}
			flag = true
			storiface.SetPathByType(&out, fileType, path)
			break
		}
		if !flag {
			done()
			return storiface.SectorPaths{}, nil, storiface.ErrSectorNotFound
		}
	}
	done()
	return out, func() {}, nil
}
