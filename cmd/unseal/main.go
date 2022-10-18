package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"github.com/docker/go-units"
	"github.com/filecoin-project/go-address"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	saproof2 "github.com/filecoin-project/specs-actors/v2/actors/runtime/proof"
	util "github.com/ipfs/go-ipfs-util"
	"github.com/mitchellh/go-homedir"
	"hash"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper/basicfs"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/minio/blake2b-simd"
	"golang.org/x/xerrors"
)

func main() {
	dir := flag.String("dir", "stdir", "storage dir")
	sectorSize := flag.String("sector-size", "2KiB", "sector size")
	flag.Parse()

	var sbdir string

	sdir, err := homedir.Expand(*dir)
	if err != nil {
		log.Println(err)
		return
	}

	err = os.MkdirAll(sdir, 0775) //nolint:gosec
	if err != nil {
		log.Println("creating sectorbuilder dir:", err)
		return
	}

	tsdir, err := ioutil.TempDir(sdir, "lo")
	if err != nil {
		log.Println(err)
		return
	}
	//defer func() {
	//	if err := os.RemoveAll(tsdir); err != nil {
	//		log.Println("remove all: ", err)
	//	}
	//}()

	// TODO: pretty sure this isnt even needed?
	if err := os.MkdirAll(tsdir, 0775); err != nil {
		log.Println(err)
		return
	}

	sbdir = tsdir

	sbfs := &basicfs.Provider{
		Root: sbdir,
	}

	sb, err := ffiwrapper.New(sbfs)
	if err != nil {
		log.Println(err)
		return
	}
	// miner address
	maddr, err := address.NewFromString("t01000")
	if err != nil {
		log.Println(err)
		return
	}
	amid, err := address.IDFromAddress(maddr)
	if err != nil {
		log.Println(err)
		return
	}
	mid := abi.ActorID(amid)

	// sector size
	sectorSizeInt, err := units.RAMInBytes(*sectorSize)
	if err != nil {
		log.Println(err)
		return
	}
	size := abi.SectorSize(sectorSizeInt)

	ticketPreimage := []byte("seal_ticket")
	trand := blake2b.Sum256(ticketPreimage)
	ticket := abi.SealRandomness(trand[:])
	sid := storage.SectorRef{
		ID: abi.SectorID{
			Miner:  mid,
			Number: 0,
		},
		ProofType: spt(size),
	}
	r := rand.New(rand.NewSource(100))
	rc := &countReader{
		count:  0,
		Reader: r,
		h:      md5.New(),
	}
	piece, err := runSeals(context.TODO(), sb, sid, rc, size, ticket)
	if err != nil {
		log.Println(err)
		return
	}
	h0 := hex.EncodeToString(rc.h.Sum(nil))
	log.Println("pieces count", rc.count, "piece hash", h0)

	if os.Getenv("QINIU") != "" {
		submitQ(sbfs, sid.ID)
	}

	log.Println("Unsealing sector...")
	if os.Getenv("QINIU_UNSEAL_STORE") == "" {
		p, done, err := sbfs.AcquireSector(context.TODO(), sid, storiface.FTUnsealed, storiface.FTNone, storiface.PathSealing)
		if err != nil {
			log.Println("acquire unsealed sector for removing:", err)
			return
		}
		done()

		if err := os.Remove(p.Unsealed); err != nil {
			log.Println("removing unsealed sector:", err)
			return
		}
		log.Println("removed unseal ", p.Unsealed, util.FileExists(p.Unsealed))
	}
	log.Println("enter.....")
	err = sb.UnsealPiece(context.TODO(), sid, 0, abi.PaddedPieceSize(size).Unpadded(), ticket, piece.PieceCID)
	if err != nil {
		log.Println(err)
		return
	}
	cw := &countHash{
		count: 0,
		Hash:  md5.New(),
	}

	b, err := sb.ReadPiece(context.TODO(), cw, sid, 0, abi.PaddedPieceSize(size).Unpadded())
	if err != nil {
		log.Println(err, b)
		return
	}
	h1 := hex.EncodeToString(cw.Sum(nil))
	log.Println("unseal hash", h1, h0 == h1, cw.count)
}

type countHash struct {
	count int64
	hash.Hash
}

func (c *countHash) Write(p []byte) (n int, err error) {
	n, err = c.Hash.Write(p)
	c.count += int64(n)
	return
}

type countReader struct {
	count int64
	io.Reader
	h hash.Hash
}

func (c *countReader) Read(p []byte) (n int, err error) {
	n, err = c.Reader.Read(p)
	c.count += int64(n)
	if err != nil {
		log.Println("count reader err", err)
	}
	c.h.Write(p[:n])
	return
}

func runSeals(ctx context.Context, sealer *ffiwrapper.Sealer, sid storage.SectorRef, data storage.Data, sectorSize abi.SectorSize, ticket abi.SealRandomness) (*abi.PieceInfo, error) {
	log.Println("AddPiece...")
	piecePad, err := sealer.AddPiece(ctx, sid, nil, abi.PaddedPieceSize(sectorSize).Unpadded(), data)
	if err != nil {
		return nil, xerrors.Errorf("pad piece: %w", err)
	}

	log.Println("PreCommit1...")
	pieces := []abi.PieceInfo{piecePad}
	pre1, err := sealer.SealPreCommit1(context.TODO(), sid, ticket, pieces)
	if err != nil {
		return nil, xerrors.Errorf("precommit1: %w", err)
	}
	log.Println("PreCommit2...")
	cids, err := sealer.SealPreCommit2(ctx, sid, pre1)
	if err != nil {
		return nil, xerrors.Errorf("precommit2: %w", err)
	}
	log.Println("Commit1...")
	seed := lapi.SealSeed{
		Epoch: 101,
		Value: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 255},
	}
	c1, err := sealer.SealCommit1(ctx, sid, ticket, seed.Value, pieces, cids)
	if err != nil {
		return nil, xerrors.Errorf("commit1: %w", err)
	}
	log.Println("Commit2...")
	c2, err := sealer.SealCommit2(ctx, sid, c1)
	if err != nil {
		return nil, xerrors.Errorf("commit2: %w", err)
	}

	svi := saproof2.SealVerifyInfo{
		SectorID:              sid.ID,
		SealedCID:             cids.Sealed,
		SealProof:             sid.ProofType,
		Proof:                 c2,
		DealIDs:               nil,
		Randomness:            ticket,
		InteractiveRandomness: seed.Value,
		UnsealedCID:           cids.Unsealed,
	}

	ok, err := ffiwrapper.ProofVerifier.VerifySeal(svi)
	if err != nil {
		return nil, xerrors.Errorf("VerifySeal: %w", err)
	}
	if !ok {
		return nil, xerrors.Errorf("porep proof for sector was invalid")
	}

	return &piecePad, nil
}

func spt(ssize abi.SectorSize) abi.RegisteredSealProof {
	spt, err := miner.SealProofTypeFromSectorSize(ssize, build.NewestNetworkVersion)
	if err != nil {
		panic(err)
	}

	return spt
}
