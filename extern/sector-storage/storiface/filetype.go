package storiface

import (
	"context"
	"fmt"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
)

const (
	FTUnsealed SectorFileType = 1 << iota
	FTSealed
	FTCache
	FTUpdate
	FTUpdateCache

	FileTypes = iota
)

var PathTypes = []SectorFileType{FTUnsealed, FTSealed, FTCache, FTUpdate, FTUpdateCache}

const (
	FTNone SectorFileType = 0
)

const FSOverheadDen = 10

var FSOverheadSeal = map[SectorFileType]int{ // 10x overheads
	FTUnsealed:    FSOverheadDen,
	FTSealed:      FSOverheadDen,
	FTUpdate:      FSOverheadDen,
	FTUpdateCache: FSOverheadDen*2 + 1,
	FTCache:       141, // 11 layers + D(2x ssize) + C + R'
}

var FSOverheadCCSeal = map[SectorFileType]int{ // 10x overheads
	FTUnsealed: 0,
	FTSealed:   0,
	FTCache:    121, // 11 layers + C + R
}

// sector size * disk / fs overhead.  FSOverheadDen is like the unit of sector size

var FsOverheadFinalized = map[SectorFileType]int{
	FTUnsealed:    FSOverheadDen,
	FTSealed:      FSOverheadDen,
	FTUpdate:      FSOverheadDen,
	FTUpdateCache: 1,
	FTCache:       1,
}

type SectorFileType int

func (t SectorFileType) String() string {
	switch t {
	case FTUnsealed:
		return "unsealed"
	case FTSealed:
		return "sealed"
	case FTCache:
		return "cache"
	case FTUpdate:
		return "update"
	case FTUpdateCache:
		return "update-cache"
	default:
		return fmt.Sprintf("<unknown %d>", t)
	}
}

func (t SectorFileType) Has(singleType SectorFileType) bool {
	return t&singleType == singleType
}

func (t SectorFileType) SealSpaceUse(ctx context.Context, ssize abi.SectorSize) (uint64, error) {
	var need uint64

	isCCSector := true
	sp := ctx.Value("isCC")
	if p, ok := sp.(bool); ok {
		if !p {
			isCCSector = false
		}
	}
	var overhead map[SectorFileType]int
	if isCCSector {
		overhead = FSOverheadCCSeal
	} else {
		overhead = FSOverheadSeal
	}

	for _, pathType := range PathTypes {
		if !t.Has(pathType) {
			continue
		}

		oh, ok := overhead[pathType]
		if !ok {
			return 0, xerrors.Errorf("no seal overhead info for %s", pathType)
		}

		need += uint64(oh) * uint64(ssize) / FSOverheadDen
	}

	return need, nil
}

func (t SectorFileType) StoreSpaceUse(ssize abi.SectorSize) (uint64, error) {
	var need uint64
	for _, pathType := range PathTypes {
		if !t.Has(pathType) {
			continue
		}

		oh, ok := FsOverheadFinalized[pathType]
		if !ok {
			return 0, xerrors.Errorf("no finalized overhead info for %s", pathType)
		}

		need += uint64(oh) * uint64(ssize) / FSOverheadDen
	}

	return need, nil
}

func (t SectorFileType) All() [FileTypes]bool {
	var out [FileTypes]bool

	for i := range out {
		out[i] = t&(1<<i) > 0
	}

	return out
}

type SectorPaths struct {
	ID abi.SectorID

	Unsealed    string
	Sealed      string
	Cache       string
	Update      string
	UpdateCache string
}

func ParseSectorID(baseName string) (abi.SectorID, error) {
	var n abi.SectorNumber
	var mid abi.ActorID
	read, err := fmt.Sscanf(baseName, "s-t0%d-%d", &mid, &n)
	if err != nil {
		return abi.SectorID{}, xerrors.Errorf("sscanf sector name ('%s'): %w", baseName, err)
	}

	if read != 2 {
		return abi.SectorID{}, xerrors.Errorf("parseSectorID expected to scan 2 values, got %d", read)
	}

	return abi.SectorID{
		Miner:  mid,
		Number: n,
	}, nil
}

func SectorName(sid abi.SectorID) string {
	return fmt.Sprintf("s-t0%d-%d", sid.Miner, sid.Number)
}

func PathByType(sps SectorPaths, fileType SectorFileType) string {
	switch fileType {
	case FTUnsealed:
		return sps.Unsealed
	case FTSealed:
		return sps.Sealed
	case FTCache:
		return sps.Cache
	case FTUpdate:
		return sps.Update
	case FTUpdateCache:
		return sps.UpdateCache
	}

	panic("requested unknown path type")
}

func SetPathByType(sps *SectorPaths, fileType SectorFileType, p string) {
	switch fileType {
	case FTUnsealed:
		sps.Unsealed = p
	case FTSealed:
		sps.Sealed = p
	case FTCache:
		sps.Cache = p
	case FTUpdate:
		sps.Update = p
	case FTUpdateCache:
		sps.UpdateCache = p
	}
}
