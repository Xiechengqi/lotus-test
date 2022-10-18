package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/docker/go-units"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper/basicfs"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"os"
	"time"
)

var recoveryCmd = &cli.Command {
	Name: "recover-sector",
	Usage: "",
	UsageText: `
`,
	Flags: []cli.Flag {
		&cli.StringFlag {
			Name: "miner-id",
			Value: "",
			Usage: "",
		},
		&cli.StringFlag {
			Name: "sector-id",
			Value: "",
			Usage: "",
		},
		&cli.StringFlag{
			Name: "sector-size",
			Value: "",
			Usage: "",
		},
		&cli.StringFlag{
			Name: "dir",
			Value: "",
			Usage: "",
		},
		&cli.StringFlag{
			Name: "ticket",
			Value: "",
			Usage: "",
		},
		&cli.StringFlag {
			Name: "commr",
			Value: "",
			Usage: "",
		},
		&cli.BoolFlag{
			Name: "keep-unsealed",
			Value: false,
			Usage: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		// commr
		if cctx.String("commr") == "" {
			return xerrors.Errorf("must provide commr")
		}

		// sector size
		sectorSizeInt, err := units.RAMInBytes(cctx.String("sector-size"))
		if err != nil {
			return err
		}
		ssize := abi.SectorSize(sectorSizeInt)

		// miner id
		minerId := cctx.String("miner-id")
		maddr, err := address.NewFromString(minerId)
		if err != nil {
			return err
		}
		amid, err := address.IDFromAddress(maddr)
		if err != nil {
			return err
		}
		mid := abi.ActorID(amid)

		// sector id
		sectorId := abi.SectorNumber(cctx.Int64("sector-id"))
		sectorFileName := fmt.Sprintf("s-%s-%d", minerId, sectorId)

		// ticket
		ticketStr := cctx.String("ticket")
		b, err := base64.StdEncoding.DecodeString(ticketStr)
		if err != nil {
			return err
		}
		ticket := abi.SealRandomness(b)
		//if len(ticketStr) != 64 {
		//	return xerrors.Errorf("ticket len is invalid")
		//}
		//ticketHex, err := hex.DecodeString(ticketStr)
		//if err != nil {
		//	return err
		//}
		//ticket := abi.SealRandomness(ticketHex[:])

		// dir
		dir := cctx.String("dir")
		sbfs := &basicfs.Provider {
			Root: dir,
		}
		sb, err := ffiwrapper.New(sbfs)
		if err != nil {
			return err
		}

		// zero packing
		log.Infof("start zero packing...")
		tsStart := build.Clock.Now()
		sectorRef := storage.SectorRef{
			ID: abi.SectorID{
				Miner: mid,
				Number: sectorId,
			},
			ProofType: spt(ssize),
		}
		ppi, err := sb.AddPiece(
			context.TODO(),
			sectorRef,
			nil,
			abi.PaddedPieceSize(ssize).Unpadded(),
			nil,
		)
		if err != nil {
			return err
		}
		log.Infof("complete zero packing. costs %v", time.Since(tsStart))
		pieces := []abi.PieceInfo{ ppi }

		// p1
		log.Infof("start precommit1...")
		tsStart = build.Clock.Now()
		pc1o, err := sb.SealPreCommit1(context.TODO(), sectorRef, ticket, pieces)
		if err != nil {
			return err
		}
		log.Infof("complete precommit1. costs %v", time.Since(tsStart))

		// p2
		log.Infof("start precommit2...")
		tsStart = build.Clock.Now()
		cids, err := sb.SealPreCommit2(context.TODO(), sectorRef, pc1o)
		if err != nil {
			return err
		}
		log.Infof("complete precommit2. costs %v", time.Since(tsStart))
		log.Infof("unsealed cid: %s", cids.Unsealed.String())
		log.Infof("sealed cid: %s", cids.Sealed.String())

		// verify
		if cids.Sealed.String() != cctx.String("commr") {
			return xerrors.Errorf("invalid sealed cid. expected=%s actual=%s", cctx.String("commr"), cids.Sealed.String())
		}

		if !cctx.Bool("keep-unsealed") {
			// removing unsealed
			log.Infof("start removing unsealed...")
			tsStart = build.Clock.Now()
			unsealedFile := dir + "/unsealed" + sectorFileName
			if err := os.RemoveAll(unsealedFile); err != nil {
				return err
			}
			log.Infof("complete removing unsealed. costs %v", time.Since(tsStart))
		}

		// finalize
		log.Infof("start finalizing...")
		tsStart = build.Clock.Now()
		err = sb.FinalizeSector(context.TODO(), sectorRef, nil)
		if err != nil {
			return err
		}
		log.Infof("complete finalizing. costs %v", time.Since(tsStart))

		return nil
	},
}

func spt(ssize abi.SectorSize) abi.RegisteredSealProof {
	spt, err := miner.SealProofTypeFromSectorSize(ssize, build.NewestNetworkVersion)
	if err != nil {
		panic(err)
	}

	return spt
}
