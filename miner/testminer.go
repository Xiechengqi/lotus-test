package miner

import (
	"context"
	"github.com/filecoin-project/lotus/octopus"
	lru "github.com/hashicorp/golang-lru"
	ds "github.com/ipfs/go-datastore"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/gen"
	"github.com/filecoin-project/lotus/chain/gen/slashfilter"
	"github.com/filecoin-project/lotus/journal"
)

type MineReq struct {
	InjectNulls abi.ChainEpoch
	Done        func(bool, abi.ChainEpoch, error)
}

func NewTestMiner(nextCh <-chan MineReq, addr address.Address) func(v1api.FullNode, gen.WinningPoStProver) *Miner {
	return func(api v1api.FullNode, epp gen.WinningPoStProver) *Miner {
		arc, err := lru.NewARC(10000)
		if err != nil {
			panic(err)
		}

		p := octopus.NewFullNodePool(nil, nil, "default", api, func() {})
		m := &Miner{
			apis:              p,
			waitFunc:          chanWaiter(nextCh),
			epp:               epp,
			minedBlockHeights: arc,
			address:           addr,
			sf:                slashfilter.New(ds.NewMapDatastore()),
			journal:           journal.NilJournal(),
		}

		if err := m.Start(context.TODO()); err != nil {
			panic(err)
		}
		return m
	}
}

func chanWaiter(next <-chan MineReq) func(ctx context.Context, _ uint64) (func(bool, abi.ChainEpoch, error), abi.ChainEpoch, error) {
	return func(ctx context.Context, _ uint64) (func(bool, abi.ChainEpoch, error), abi.ChainEpoch, error) {
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case req := <-next:
			return req.Done, req.InjectNulls, nil
		}
	}
}
