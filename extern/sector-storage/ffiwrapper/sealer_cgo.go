//go:build cgo
// +build cgo

package ffiwrapper

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/filecoin-project/lotus/build"
	util "github.com/ipfs/go-ipfs-util"

	"github.com/filecoin-project/go-state-types/proof"

	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/detailyang/go-fallocate"
	ffi "github.com/filecoin-project/filecoin-ffi"
	rlepluslazy "github.com/filecoin-project/go-bitfield/rle"
	commpffi "github.com/filecoin-project/go-commp-utils/ffiwrapper"
	"github.com/filecoin-project/go-commp-utils/zerocomm"
	commcid "github.com/filecoin-project/go-fil-commcid"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/qiniupd/qiniu-go-sdk/syncdata/operation"

	"github.com/filecoin-project/lotus/extern/sector-storage/fr32"
	"github.com/filecoin-project/lotus/extern/sector-storage/partialfile"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

var _ Storage = &Sealer{}

func New(sectors SectorProvider) (*Sealer, error) {
	sb := &Sealer{
		sectors: sectors,

		stopping: make(chan struct{}),
	}

	return sb, nil
}

func (sb *Sealer) NewSector(ctx context.Context, sector storage.SectorRef) error {
	// TODO: Allocate the sector here instead of in addpiece

	return nil
}

func (sb *Sealer) DataCid(ctx context.Context, pieceSize abi.UnpaddedPieceSize, pieceData storage.Data) (abi.PieceInfo, error) {
	// TODO: allow tuning those:
	chunk := abi.PaddedPieceSize(4 << 20)
	parallel := runtime.NumCPU()

	maxSizeSpt := abi.RegisteredSealProof_StackedDrg64GiBV1_1

	throttle := make(chan []byte, parallel)
	piecePromises := make([]func() (abi.PieceInfo, error), 0)

	buf := make([]byte, chunk.Unpadded())
	for i := 0; i < parallel; i++ {
		if abi.UnpaddedPieceSize(i)*chunk.Unpadded() >= pieceSize {
			break // won't use this many buffers
		}
		throttle <- make([]byte, chunk.Unpadded())
	}

	for {
		var read int
		for rbuf := buf; len(rbuf) > 0; {
			n, err := pieceData.Read(rbuf)
			if err != nil && err != io.EOF {
				return abi.PieceInfo{}, xerrors.Errorf("pr read error: %w", err)
			}

			rbuf = rbuf[n:]
			read += n

			if err == io.EOF {
				break
			}
		}
		if read == 0 {
			break
		}

		done := make(chan struct {
			cid.Cid
			error
		}, 1)
		pbuf := <-throttle
		copy(pbuf, buf[:read])

		go func(read int) {
			defer func() {
				throttle <- pbuf
			}()

			c, err := sb.pieceCid(maxSizeSpt, pbuf[:read])
			done <- struct {
				cid.Cid
				error
			}{c, err}
		}(read)

		piecePromises = append(piecePromises, func() (abi.PieceInfo, error) {
			select {
			case e := <-done:
				if e.error != nil {
					return abi.PieceInfo{}, e.error
				}

				return abi.PieceInfo{
					Size:     abi.UnpaddedPieceSize(read).Padded(),
					PieceCID: e.Cid,
				}, nil
			case <-ctx.Done():
				return abi.PieceInfo{}, ctx.Err()
			}
		})
	}

	if len(piecePromises) == 1 {
		return piecePromises[0]()
	}

	var payloadRoundedBytes abi.PaddedPieceSize
	pieceCids := make([]abi.PieceInfo, len(piecePromises))
	for i, promise := range piecePromises {
		pinfo, err := promise()
		if err != nil {
			return abi.PieceInfo{}, err
		}

		pieceCids[i] = pinfo
		payloadRoundedBytes += pinfo.Size
	}

	pieceCID, err := ffi.GenerateUnsealedCID(maxSizeSpt, pieceCids)
	if err != nil {
		return abi.PieceInfo{}, xerrors.Errorf("generate unsealed CID: %w", err)
	}

	// validate that the pieceCID was properly formed
	if _, err := commcid.CIDToPieceCommitmentV1(pieceCID); err != nil {
		return abi.PieceInfo{}, err
	}

	if payloadRoundedBytes < pieceSize.Padded() {
		paddedCid, err := commpffi.ZeroPadPieceCommitment(pieceCID, payloadRoundedBytes.Unpadded(), pieceSize)
		if err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("failed to pad data: %w", err)
		}

		pieceCID = paddedCid
	}

	return abi.PieceInfo{
		Size:     pieceSize.Padded(),
		PieceCID: pieceCID,
	}, nil
}

func (sb *Sealer) AddPiece(ctx context.Context, sector storage.SectorRef, existingPieceSizes []abi.UnpaddedPieceSize, pieceSize abi.UnpaddedPieceSize, file storage.Data) (abi.PieceInfo, error) {
	isCCSector := true
	sp := ctx.Value("isCC")
	log.Debugf("octopus: p1: context isCC=%v", sp)
	if p, ok := sp.(bool); ok {
		if !p {
			isCCSector = false
		}
	}
	if isCCSector {
		log.Infof("octopus: add zero piece...")
		return addZeroPiece(ctx, sector, sb, pieceSize)
		// I was trying to use original logic under zero piece failure, but there is  cycle import issue.
		//p, err := addZeroPiece(ctx, sector, sb, pieceSize)
		//if err == nil {
		//	return p, nil
		//} else {
		//log.Errorf("octopus: add zero piece error: %v", err)
		//file = sealing.NewNullReader(pieceSize)
		//}
	}

	// TODO: allow tuning those:
	chunk := abi.PaddedPieceSize(4 << 20)
	parallel := runtime.NumCPU()

	var offset abi.UnpaddedPieceSize
	for _, size := range existingPieceSizes {
		offset += size
	}

	ssize, err := sector.ProofType.SectorSize()
	if err != nil {
		return abi.PieceInfo{}, err
	}

	maxPieceSize := abi.PaddedPieceSize(ssize)

	if offset.Padded()+pieceSize.Padded() > maxPieceSize {
		return abi.PieceInfo{}, xerrors.Errorf("can't add %d byte piece to sector %v with %d bytes of existing pieces", pieceSize, sector, offset)
	}

	var done func()
	var stagedFile *partialfile.PartialFile

	defer func() {
		if done != nil {
			done()
		}

		if stagedFile != nil {
			if err := stagedFile.Close(); err != nil {
				log.Errorf("closing staged file: %+v", err)
			}
		}
	}()

	var stagedPath storiface.SectorPaths
	if len(existingPieceSizes) == 0 {
		stagedPath, done, err = sb.sectors.AcquireSector(ctx, sector, 0, storiface.FTUnsealed, storiface.PathSealing)
		if err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("acquire unsealed sector: %w", err)
		}

		stagedFile, err = partialfile.CreatePartialFile(maxPieceSize, stagedPath.Unsealed)
		if err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("creating unsealed sector file: %w", err)
		}
	} else {
		stagedPath, done, err = sb.sectors.AcquireSector(ctx, sector, storiface.FTUnsealed, 0, storiface.PathSealing)
		if err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("acquire unsealed sector: %w", err)
		}

		stagedFile, err = partialfile.OpenPartialFile(maxPieceSize, stagedPath.Unsealed)
		if err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("opening unsealed sector file: %w", err)
		}
	}

	w, err := stagedFile.Writer(storiface.UnpaddedByteIndex(offset).Padded(), pieceSize.Padded())
	if err != nil {
		return abi.PieceInfo{}, xerrors.Errorf("getting partial file writer: %w", err)
	}

	pw := fr32.NewPadWriter(w)

	pr := io.TeeReader(io.LimitReader(file, int64(pieceSize)), pw)

	throttle := make(chan []byte, parallel)
	piecePromises := make([]func() (abi.PieceInfo, error), 0)

	buf := make([]byte, chunk.Unpadded())
	for i := 0; i < parallel; i++ {
		if abi.UnpaddedPieceSize(i)*chunk.Unpadded() >= pieceSize {
			break // won't use this many buffers
		}
		throttle <- make([]byte, chunk.Unpadded())
	}

	for {
		var read int
		for rbuf := buf; len(rbuf) > 0; {
			n, err := pr.Read(rbuf)
			if err != nil && err != io.EOF {
				return abi.PieceInfo{}, xerrors.Errorf("pr read error: %w", err)
			}

			rbuf = rbuf[n:]
			read += n

			if err == io.EOF {
				break
			}
		}
		if read == 0 {
			break
		}

		done := make(chan struct {
			cid.Cid
			error
		}, 1)
		pbuf := <-throttle
		copy(pbuf, buf[:read])

		go func(read int) {
			defer func() {
				throttle <- pbuf
			}()

			c, err := sb.pieceCid(sector.ProofType, pbuf[:read])
			done <- struct {
				cid.Cid
				error
			}{c, err}
		}(read)

		piecePromises = append(piecePromises, func() (abi.PieceInfo, error) {
			select {
			case e := <-done:
				if e.error != nil {
					return abi.PieceInfo{}, e.error
				}

				return abi.PieceInfo{
					Size:     abi.UnpaddedPieceSize(len(buf[:read])).Padded(),
					PieceCID: e.Cid,
				}, nil
			case <-ctx.Done():
				return abi.PieceInfo{}, ctx.Err()
			}
		})
	}

	if err := pw.Close(); err != nil {
		return abi.PieceInfo{}, xerrors.Errorf("closing padded writer: %w", err)
	}

	if err := stagedFile.MarkAllocated(storiface.UnpaddedByteIndex(offset).Padded(), pieceSize.Padded()); err != nil {
		return abi.PieceInfo{}, xerrors.Errorf("marking data range as allocated: %w", err)
	}

	if err := stagedFile.Close(); err != nil {
		return abi.PieceInfo{}, err
	}
	stagedFile = nil

	if len(piecePromises) == 1 {
		return piecePromises[0]()
	}

	var payloadRoundedBytes abi.PaddedPieceSize
	pieceCids := make([]abi.PieceInfo, len(piecePromises))
	for i, promise := range piecePromises {
		pinfo, err := promise()
		if err != nil {
			return abi.PieceInfo{}, err
		}

		pieceCids[i] = pinfo
		payloadRoundedBytes += pinfo.Size
	}

	pieceCID, err := ffi.GenerateUnsealedCID(sector.ProofType, pieceCids)
	if err != nil {
		return abi.PieceInfo{}, xerrors.Errorf("generate unsealed CID: %w", err)
	}

	// validate that the pieceCID was properly formed
	if _, err := commcid.CIDToPieceCommitmentV1(pieceCID); err != nil {
		return abi.PieceInfo{}, err
	}

	if payloadRoundedBytes < pieceSize.Padded() {
		paddedCid, err := commpffi.ZeroPadPieceCommitment(pieceCID, payloadRoundedBytes.Unpadded(), pieceSize)
		if err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("failed to pad data: %w", err)
		}

		pieceCID = paddedCid
	}

	return abi.PieceInfo{
		Size:     pieceSize.Padded(),
		PieceCID: pieceCID,
	}, nil
}

func addZeroPiece(ctx context.Context, sector storage.SectorRef, sb *Sealer, pieceSize abi.UnpaddedPieceSize) (abi.PieceInfo, error) {
	var err error
	var stagedPath storiface.SectorPaths
	var done func()

	defer func() {
		if done != nil {
			done()
		}
	}()

	stagedPath, done, err = sb.sectors.AcquireSector(ctx, sector, 0, storiface.FTUnsealed, storiface.PathSealing)
	if err != nil {
		return abi.PieceInfo{}, xerrors.Errorf("acquire unsealed sector: %w", err)
	}

	var unsealFileSuffix string
	var pieceCID cid.Cid
	if sector.ProofType == abi.RegisteredSealProof_StackedDrg2KiBV1 || sector.ProofType == abi.RegisteredSealProof_StackedDrg2KiBV1_1 {
		unsealFileSuffix = "StackedDrg2KiBV1"
		pieceCID, _ = cid.Parse("baga6ea4seaqpy7usqklokfx2vxuynmupslkeutzexe2uqurdg5vhtebhxqmpqmy")
	} else if sector.ProofType == abi.RegisteredSealProof_StackedDrg32GiBV1 || sector.ProofType == abi.RegisteredSealProof_StackedDrg32GiBV1_1 {
		unsealFileSuffix = "StackedDrg32GiBV1"
		pieceCID, _ = cid.Parse("baga6ea4seaqao7s73y24kcutaosvacpdjgfe5pw76ooefnyqw4ynr3d2y6x2mpq")
	} else if sector.ProofType == abi.RegisteredSealProof_StackedDrg64GiBV1 || sector.ProofType == abi.RegisteredSealProof_StackedDrg64GiBV1_1 {
		unsealFileSuffix = "StackedDrg64GiBV1"
		pieceCID, _ = cid.Parse("baga6ea4seaqomqafu276g53zko4k23xzh4h4uecjwicbmvhsuqi7o4bhthhm4aq")
	} else {
		return abi.PieceInfo{}, xerrors.Errorf("unsupported unseal file for sector proof type %v", sector.ProofType)
	}

	if os.Getenv("StackedDrg_cp") == "true" {
		log.Infof("octopus: cp from: %s to: %s", filepath.Join(os.Getenv("CC_SECTOR_DATA_PATH"), unsealFileSuffix), stagedPath.Unsealed)
		if _, err = copyFile(filepath.Join(os.Getenv("CC_SECTOR_DATA_PATH"), unsealFileSuffix), stagedPath.Unsealed); err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("cp error: %w", err)
		}
	} else {
		log.Infof("octopus: symlink from: %s to: %s", filepath.Join(os.Getenv("CC_SECTOR_DATA_PATH"), unsealFileSuffix), stagedPath.Unsealed)
		if err = os.Symlink(filepath.Join(os.Getenv("CC_SECTOR_DATA_PATH"), unsealFileSuffix), stagedPath.Unsealed); err != nil {
			return abi.PieceInfo{}, xerrors.Errorf("make symlink error: %w", err)
		}
	}

	return abi.PieceInfo{
		Size:     pieceSize.Padded(),
		PieceCID: pieceCID,
	}, nil
}

func (sb *Sealer) pieceCid(spt abi.RegisteredSealProof, in []byte) (cid.Cid, error) {
	prf, werr, err := commpffi.ToReadableFile(bytes.NewReader(in), int64(len(in)))
	if err != nil {
		return cid.Undef, xerrors.Errorf("getting tee reader pipe: %w", err)
	}

	pieceCID, err := ffi.GeneratePieceCIDFromFile(spt, prf, abi.UnpaddedPieceSize(len(in)))
	if err != nil {
		return cid.Undef, xerrors.Errorf("generating piece commitment: %w", err)
	}

	_ = prf.Close()

	return pieceCID, werr()
}

func (sb *Sealer) tryDecodeUpdatedReplica(ctx context.Context, sector storage.SectorRef, commD cid.Cid, unsealedPath string) (bool, error) {
	paths, done, err := sb.sectors.AcquireSector(ctx, sector, storiface.FTUpdate|storiface.FTSealed|storiface.FTCache, storiface.FTNone, storiface.PathStorage)
	if xerrors.Is(err, storiface.ErrSectorNotFound) {
		return false, nil
	} else if err != nil {
		return false, xerrors.Errorf("reading updated replica: %w", err)
	}
	defer done()

	// Sector data stored in replica update
	updateProof, err := sector.ProofType.RegisteredUpdateProof()
	if err != nil {
		return false, err
	}
	return true, ffi.SectorUpdate.DecodeFrom(updateProof, unsealedPath, paths.Update, paths.Sealed, paths.Cache, commD)
}

func (sb *Sealer) UnsealPiece(ctx context.Context, sector storage.SectorRef, offset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize, randomness abi.SealRandomness, commd cid.Cid) error {
	ssize, err := sector.ProofType.SectorSize()
	if err != nil {
		return err
	}
	maxPieceSize := abi.PaddedPieceSize(ssize)

	// try finding existing
	unsealedPath, done, err := sb.sectors.AcquireSector(ctx, sector, storiface.FTUnsealed, storiface.FTNone, storiface.PathStorage)
	var pf *partialfile.PartialFile

	switch {
	case xerrors.Is(err, storiface.ErrSectorNotFound):
		unsealedPath, done, err = sb.sectors.AcquireSector(ctx, sector, storiface.FTNone, storiface.FTUnsealed, storiface.PathSealing)
		if err != nil {
			return xerrors.Errorf("acquire unsealed sector path (allocate): %w", err)
		}
		defer done()

		if os.Getenv("QINIU_UNSEAL_STORE") != "" && IsQiniuFileExists(unsealedPath.Unsealed) {
			log.Info("download unseal from cloud", unsealedPath.Unsealed)
			d := operation.NewDownloaderV2()
			_, err = d.DownloadFile(unsealedPath.Unsealed, unsealedPath.Unsealed)
			if err != nil {
				return xerrors.Errorf("download unsealed file: %w", err)
			}
			pf, err = partialfile.OpenPartialFile(maxPieceSize, unsealedPath.Unsealed)
			if err != nil {
				return xerrors.Errorf("opening download partial file: %w", err)
			}
			return nil
		} else {
			pf, err = partialfile.CreatePartialFile(maxPieceSize, unsealedPath.Unsealed)
			if err != nil {
				return xerrors.Errorf("create unsealed file: %w", err)
			}
		}

	case err == nil:
		defer done()

		pf, err = partialfile.OpenPartialFile(maxPieceSize, unsealedPath.Unsealed)
		if err != nil {
			return xerrors.Errorf("opening partial file: %w", err)
		}
	default:
		return xerrors.Errorf("acquire unsealed sector path (existing): %w", err)
	}
	defer pf.Close() // nolint

	allocated, err := pf.Allocated()
	if err != nil {
		return xerrors.Errorf("getting bitruns of allocated data: %w", err)
	}

	toUnseal, err := computeUnsealRanges(allocated, offset, size)
	if err != nil {
		return xerrors.Errorf("computing unseal ranges: %w", err)
	}

	if !toUnseal.HasNext() {
		return nil
	}

	// If piece data stored in updated replica decode whole sector
	decoded, err := sb.tryDecodeUpdatedReplica(ctx, sector, commd, unsealedPath.Unsealed)
	if err != nil {
		return xerrors.Errorf("decoding sector from replica: %w", err)
	}
	if decoded {
		return pf.MarkAllocated(0, maxPieceSize)
	}

	// Piece data sealed in sector
	srcPaths, srcDone, err := sb.sectors.AcquireSector(ctx, sector, storiface.FTCache|storiface.FTSealed, storiface.FTNone, storiface.PathStorage)
	if err != nil {
		return xerrors.Errorf("acquire sealed sector paths: %w", err)
	}
	defer srcDone()
	var sealed *os.File

	if os.Getenv("QINIU") != "" && !util.FileExists(srcPaths.Sealed) {
		d := operation.NewDownloaderV2()
		name := strings.ReplaceAll(srcPaths.Sealed, "/", "_")
		tempPath := os.Getenv("QINIU_SEALED_TEMP")
		p := path.Join(tempPath, name)
		sealed, err = d.DownloadFile(srcPaths.Sealed, p)
	} else {
		sealed, err = os.OpenFile(srcPaths.Sealed, os.O_RDONLY, 0644) // nolint:gosec
	}

	if err != nil {
		return xerrors.Errorf("opening sealed file: %w", err)
	}
	defer sealed.Close() // nolint

	var at, nextat abi.PaddedPieceSize
	first := true
	for first || toUnseal.HasNext() {
		first = false

		piece, err := toUnseal.NextRun()
		if err != nil {
			return xerrors.Errorf("getting next range to unseal: %w", err)
		}

		at = nextat
		nextat += abi.PaddedPieceSize(piece.Len)

		if !piece.Val {
			continue
		}

		out, err := pf.Writer(offset.Padded(), size.Padded())
		if err != nil {
			return xerrors.Errorf("getting partial file writer: %w", err)
		}

		// <eww>
		opr, opw, err := os.Pipe()
		if err != nil {
			return xerrors.Errorf("creating out pipe: %w", err)
		}

		var perr error
		outWait := make(chan struct{})

		{
			go func() {
				defer close(outWait)
				defer opr.Close() // nolint

				padwriter := fr32.NewPadWriter(out)

				bsize := uint64(size.Padded())
				if bsize > uint64(runtime.NumCPU())*fr32.MTTresh {
					bsize = uint64(runtime.NumCPU()) * fr32.MTTresh
				}

				bw := bufio.NewWriterSize(padwriter, int(abi.PaddedPieceSize(bsize).Unpadded()))

				_, err := io.CopyN(bw, opr, int64(size))
				if err != nil {
					perr = xerrors.Errorf("copying data: %w", err)
					return
				}

				if err := bw.Flush(); err != nil {
					perr = xerrors.Errorf("flushing unpadded data: %w", err)
					return
				}

				if err := padwriter.Close(); err != nil {
					perr = xerrors.Errorf("closing padwriter: %w", err)
					return
				}
			}()
		}
		// </eww>

		// TODO: This may be possible to do in parallel
		err = ffi.UnsealRange(sector.ProofType,
			srcPaths.Cache,
			sealed,
			opw,
			sector.ID.Number,
			sector.ID.Miner,
			randomness,
			commd,
			uint64(at.Unpadded()),
			uint64(abi.PaddedPieceSize(piece.Len).Unpadded()))

		_ = opw.Close()

		if err != nil {
			return xerrors.Errorf("unseal range: %w", err)
		}

		select {
		case <-outWait:
		case <-ctx.Done():
			return ctx.Err()
		}

		if perr != nil {
			return xerrors.Errorf("piping output to unsealed file: %w", perr)
		}

		if err := pf.MarkAllocated(storiface.PaddedByteIndex(at), abi.PaddedPieceSize(piece.Len)); err != nil {
			return xerrors.Errorf("marking unsealed range as allocated: %w", err)
		}

		if !toUnseal.HasNext() {
			break
		}
	}

	return nil
}

func (sb *Sealer) ReadPiece(ctx context.Context, writer io.Writer, sector storage.SectorRef, offset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize) (bool, error) {
	up := os.Getenv("QINIU")
	if up != "" {
		return sb.ReadPieceQiniu(ctx, writer, sector, offset, size)
	}

	path, done, err := sb.sectors.AcquireSector(ctx, sector, storiface.FTUnsealed, storiface.FTNone, storiface.PathStorage)
	if err != nil {
		return false, xerrors.Errorf("acquire unsealed sector path: %w", err)
	}
	defer done()

	ssize, err := sector.ProofType.SectorSize()
	if err != nil {
		return false, err
	}
	maxPieceSize := abi.PaddedPieceSize(ssize)

	pf, err := partialfile.OpenPartialFile(maxPieceSize, path.Unsealed)
	if err != nil {
		if xerrors.Is(err, os.ErrNotExist) {
			return false, nil
		}

		return false, xerrors.Errorf("opening partial file: %w", err)
	}

	ok, err := pf.HasAllocated(offset, size)
	if err != nil {
		_ = pf.Close()
		return false, err
	}

	if !ok {
		_ = pf.Close()
		return false, nil
	}

	f, err := pf.Reader(offset.Padded(), size.Padded())
	if err != nil {
		_ = pf.Close()
		return false, xerrors.Errorf("getting partial file reader: %w", err)
	}

	upr, err := fr32.NewUnpadReader(f, size.Padded())
	if err != nil {
		return false, xerrors.Errorf("creating unpadded reader: %w", err)
	}

	if _, err := io.CopyN(writer, upr, int64(size)); err != nil {
		_ = pf.Close()
		return false, xerrors.Errorf("reading unsealed file: %w", err)
	}

	if err := pf.Close(); err != nil {
		return false, xerrors.Errorf("closing partial file: %w", err)
	}

	return true, nil
}

func (sb *Sealer) SealPreCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, pieces []abi.PieceInfo) (out storage.PreCommit1Out, err error) {
	paths, done, err := sb.sectors.AcquireSector(ctx, sector, storiface.FTUnsealed, storiface.FTSealed|storiface.FTCache, storiface.PathSealing)
	if err != nil {
		return nil, xerrors.Errorf("acquiring sector paths: %w", err)
	}
	defer done()

	e, err := os.OpenFile(paths.Sealed, os.O_RDWR|os.O_CREATE, 0644) // nolint:gosec
	if err != nil {
		return nil, xerrors.Errorf("ensuring sealed file exists: %w", err)
	}
	if err := e.Close(); err != nil {
		return nil, err
	}

	if err := os.Mkdir(paths.Cache, 0755); err != nil { // nolint
		if os.IsExist(err) {
			log.Warnf("existing cache in %s; removing", paths.Cache)

			if err := os.RemoveAll(paths.Cache); err != nil {
				return nil, xerrors.Errorf("remove existing sector cache from %s (sector %d): %w", paths.Cache, sector, err)
			}

			if err := os.Mkdir(paths.Cache, 0755); err != nil { // nolint:gosec
				return nil, xerrors.Errorf("mkdir cache path after cleanup: %w", err)
			}
		} else {
			return nil, err
		}
	}

	var sum abi.UnpaddedPieceSize
	for _, piece := range pieces {
		sum += piece.Size.Unpadded()
	}
	ssize, err := sector.ProofType.SectorSize()
	if err != nil {
		return nil, err
	}
	ussize := abi.PaddedPieceSize(ssize).Unpadded()
	if sum != ussize {
		return nil, xerrors.Errorf("aggregated piece sizes don't match sector size: %d != %d (%d)", sum, ussize, int64(ussize-sum))
	}

	isCCSector := true
	sp := ctx.Value("isCC")
	log.Debugf("octopus: p1: context isCC=%v", sp)
	if p, ok := sp.(bool); ok {
		if !p {
			isCCSector = false
		}
	}

	if isCCSector {
		var prefix string
		if sector.ProofType == abi.RegisteredSealProof_StackedDrg2KiBV1 || sector.ProofType == abi.RegisteredSealProof_StackedDrg2KiBV1_1 {
			prefix = "StackedDrg2KiBV1"
		} else if sector.ProofType == abi.RegisteredSealProof_StackedDrg32GiBV1 || sector.ProofType == abi.RegisteredSealProof_StackedDrg32GiBV1_1 {
			prefix = "StackedDrg32GiBV1"
		} else if sector.ProofType == abi.RegisteredSealProof_StackedDrg64GiBV1 || sector.ProofType == abi.RegisteredSealProof_StackedDrg64GiBV1_1 {
			prefix = "StackedDrg64GiBV1"
		} else {
			return nil, xerrors.Errorf("unsupported unseal file for sector proof type %v", sector.ProofType)
		}

		if os.Getenv("StackedDrg_cp") == "true" {
			log.Infof("octopus: cp from: %s to: %s", filepath.Join(paths.Cache, "sc-02-data-tree-d.dat"), filepath.Join(os.Getenv("CC_SECTOR_DATA_PATH"), prefix+"-data-tree-d.dat"))
			if _, err = copyFile(filepath.Join(os.Getenv("CC_SECTOR_DATA_PATH"), prefix+"-data-tree-d.dat"), filepath.Join(paths.Cache, "sc-02-data-tree-d.dat")); err != nil {
				return nil, xerrors.Errorf("cp error: %w", err)
			}
		} else {
			log.Infof("octopus: symlink from: %s to: %s", filepath.Join(paths.Cache, "sc-02-data-tree-d.dat"), filepath.Join(os.Getenv("CC_SECTOR_DATA_PATH"), prefix+"-data-tree-d.dat"))
			if err = os.Symlink(filepath.Join(os.Getenv("CC_SECTOR_DATA_PATH"), prefix+"-data-tree-d.dat"), filepath.Join(paths.Cache, "sc-02-data-tree-d.dat")); err != nil {
				return nil, xerrors.Errorf("make symlink error: %w", err)
			}
		}
	}

	// TODO: context cancellation respect

	tsStart := build.Clock.Now()
	p1o, err := ffi.SealPreCommitPhase1(
		sector.ProofType,
		paths.Cache,
		paths.Unsealed,
		paths.Sealed,
		sector.ID.Number,
		sector.ID.Miner,
		ticket,
		pieces,
	)
	elapsed := time.Since(tsStart)
	log.Infof("octopus: seal task: sector %v: p1 elapse: %v", sector.ID.Number, elapsed)

	if err != nil {
		return nil, xerrors.Errorf("presealing sector %d (%s): %w", sector.ID.Number, paths.Unsealed, err)
	}

	p1odec := map[string]interface{}{}
	if err := json.Unmarshal(p1o, &p1odec); err != nil {
		return nil, xerrors.Errorf("unmarshaling pc1 output: %w", err)
	}

	p1odec["_lotus_SealRandomness"] = ticket

	return json.Marshal(&p1odec)
}

func copyFile(src, dst string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return 0, err
	}

	defer destination.Close()
	nBytes, err := io.Copy(destination, source)
	return nBytes, err
}

func prepareSealPreCommit2(paths storiface.SectorPaths, ssize abi.SectorSize) error {
	log.Infof("octopus: prepareSealPreCommit2...")
	// !!!only valid for zero packing data.
	fi, err := os.Lstat(paths.Sealed)
	if err != nil {
		return xerrors.Errorf("ensuring sealed file exists: %w", err)
	}

	// symbolic link in canada environment, no need to check sealed file, p2 rust will remove it at the beginning.
	if fi.Mode()&os.ModeSymlink != 0 {
		log.Debugf("octopus: %s is symlink. no need to check.", paths.Sealed)
		return nil
	}

	sealedFile, err := os.OpenFile(paths.Sealed, os.O_RDWR|os.O_CREATE, 0644) // nolint:gosec
	if err != nil {
		return xerrors.Errorf("ensuring sealed file %s exists: %w", paths.Sealed, err)
	}
	defer sealedFile.Close()

	bytesToCheck := 8
	sealBuf := make([]byte, bytesToCheck)
	sealBytesRead, err := sealedFile.Read(sealBuf)
	if err != nil {
		return xerrors.Errorf("octopus: sealed file: %s: first n bytes read err: %w", paths.Sealed, err)
	}

	unsealedFile, err := os.Open(paths.Unsealed)
	if err != nil {
		return xerrors.Errorf("open %s error: %w", paths.Unsealed, err)
	}
	defer unsealedFile.Close()

	if sealBytesRead != bytesToCheck {
		log.Debugf("octopus: %s: bytes read %d expected %d. copy from unsealed directly.", paths.Sealed, sealBytesRead, bytesToCheck)
		// if there is no bytesToCheck bytes read, copy from unsealed directly.
		return prepareSealFile(paths, sealedFile, unsealedFile, ssize)
	} else {
		unsealBuf := make([]byte, bytesToCheck)
		unsealBytesRead, err := unsealedFile.Read(unsealBuf)
		if err != nil {
			return xerrors.Errorf("octopus: unsealed file: %s: first n bytes read err: %w", paths.Unsealed, err)
		}
		if unsealBytesRead != bytesToCheck {
			return xerrors.Errorf("octopus: unsealed file's first n bytes read err: expected %d, actual %d", bytesToCheck, unsealBytesRead)
		}

		isDiff := false
		for i := 0; i < bytesToCheck; i++ {
			if sealBuf[i] != unsealBuf[i] {
				isDiff = true
				break
			}
		}

		if isDiff {
			log.Debugf("octopus: first %d bytes of %s and %s are different. will copy.", bytesToCheck, paths.Unsealed, paths.Sealed)
			return prepareSealFile(paths, sealedFile, unsealedFile, ssize)
		} else {
			log.Debugf("octopus: %s and %s are the same.", paths.Unsealed, paths.Sealed)
		}
	}

	return nil
}

func prepareSealFile(paths storiface.SectorPaths, sealedFile *os.File, unsealedFile *os.File, ssize abi.SectorSize) error {
	tsStart := build.Clock.Now()
	log.Debugf("octopus: copy %s -> %s...", paths.Unsealed, paths.Sealed)
	_, err := io.Copy(sealedFile, unsealedFile)
	if err != nil {
		return xerrors.Errorf("copy %s to %s error: %w", paths.Unsealed, paths.Sealed, err)
	}
	if err = sealedFile.Truncate(int64(ssize)); err != nil {
		return err
	}
	elapsed := time.Since(tsStart)
	log.Debugf("octopus: copy %s -> %s elapse: %v", paths.Unsealed, paths.Sealed, elapsed)
	return nil
}

var PC2CheckRounds = 5

func (sb *Sealer) SealPreCommit2(ctx context.Context, sector storage.SectorRef, phase1Out storage.PreCommit1Out) (storage.SectorCids, error) {
	paths, done, err := sb.sectors.AcquireSector(context.WithValue(ctx, "pathTypeForExisting", true), sector, storiface.FTSealed|storiface.FTCache|storiface.FTUnsealed, 0, storiface.PathSealing)
	if err != nil {
		return storage.SectorCids{}, xerrors.Errorf("acquiring sector paths: %w", err)
	}
	defer done()

	ssize, err := sector.ProofType.SectorSize()
	if err != nil {
		return storage.SectorCids{}, err
	}

	if err = prepareSealPreCommit2(paths, ssize); err != nil {
		return storage.SectorCids{}, xerrors.Errorf("check and prepare sealed file %s: %w", paths.Sealed, err)
	}

	tsStart := build.Clock.Now()
	sealedCID, unsealedCID, err := ffi.SealPreCommitPhase2(phase1Out, paths.Cache, paths.Sealed)
	elapsed := time.Since(tsStart)
	log.Infof("octopus: seal task: sector %v: p2 elapse: %v", sector.ID.Number, elapsed)
	if err != nil {
		return storage.SectorCids{}, xerrors.Errorf("presealing sector %d (%s): %w", sector.ID.Number, paths.Unsealed, err)
	}

	p1odec := map[string]interface{}{}
	if err := json.Unmarshal(phase1Out, &p1odec); err != nil {
		return storage.SectorCids{}, xerrors.Errorf("unmarshaling pc1 output: %w", err)
	}

	var ticket abi.SealRandomness
	ti, found := p1odec["_lotus_SealRandomness"]

	if found {
		ticket, err = base64.StdEncoding.DecodeString(ti.(string))
		if err != nil {
			return storage.SectorCids{}, xerrors.Errorf("decoding ticket: %w", err)
		}

		p2CheckRoundsStr := os.Getenv("P2_CHECK_ROUNDS")
		if p2CheckRoundsStr != "" {
			PC2CheckRounds, _ = strconv.Atoi(p2CheckRoundsStr)
		}
		for i := 0; i < PC2CheckRounds; i++ {
			var sd [32]byte
			_, _ = rand.Read(sd[:])

			_, err := ffi.SealCommitPhase1(
				sector.ProofType,
				sealedCID,
				unsealedCID,
				paths.Cache,
				paths.Sealed,
				sector.ID.Number,
				sector.ID.Miner,
				ticket,
				sd[:],
				[]abi.PieceInfo{{Size: abi.PaddedPieceSize(ssize), PieceCID: unsealedCID}},
			)
			if err != nil {
				log.Warn("checking PreCommit failed: ", err)
				log.Warnf("num:%d tkt:%v seed:%v sealedCID:%v, unsealedCID:%v", sector.ID.Number, ticket, sd[:], sealedCID, unsealedCID)

				return storage.SectorCids{}, xerrors.Errorf("check rounds: checking PreCommit failed: %w", err)
			}
		}
	}

	return storage.SectorCids{
		Unsealed: unsealedCID,
		Sealed:   sealedCID,
	}, nil
}

func (sb *Sealer) SealCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (storage.Commit1Out, error) {
	paths, done, err := sb.sectors.AcquireSector(context.WithValue(ctx, "pathTypeForExisting", true), sector, storiface.FTSealed|storiface.FTCache, 0, storiface.PathSealing)
	if err != nil {
		return nil, xerrors.Errorf("acquire sector paths: %w", err)
	}
	defer done()
	tsStart := build.Clock.Now()
	output, err := ffi.SealCommitPhase1(
		sector.ProofType,
		cids.Sealed,
		cids.Unsealed,
		paths.Cache,
		paths.Sealed,
		sector.ID.Number,
		sector.ID.Miner,
		ticket,
		seed,
		pieces,
	)
	elapsed := time.Since(tsStart)
	log.Infof("octopus: seal task: sector %v: c1 elapse: %v", sector.ID.Number, elapsed)
	if err != nil {
		log.Warn("StandaloneSealCommit error: ", err)
		log.Warnf("num:%d tkt:%v seed:%v, pi:%v sealedCID:%v, unsealedCID:%v", sector.ID.Number, ticket, seed, pieces, cids.Sealed, cids.Unsealed)

		return nil, xerrors.Errorf("StandaloneSealCommit: %w", err)
	}
	return output, nil
}

func (sb *Sealer) SealCommit2(ctx context.Context, sector storage.SectorRef, phase1Out storage.Commit1Out) (storage.Proof, error) {
	tsStart := build.Clock.Now()
	f, err := ffi.SealCommitPhase2(phase1Out, sector.ID.Number, sector.ID.Miner)
	elapsed := time.Since(tsStart)
	log.Infof("octopus: seal task: sector %v: c2 elapse: %v", sector.ID.Number, elapsed)
	return f, err
}

func (sb *Sealer) ReplicaUpdate(ctx context.Context, sector storage.SectorRef, pieces []abi.PieceInfo) (storage.ReplicaUpdateOut, error) {
	empty := storage.ReplicaUpdateOut{}
	paths, done, err := sb.sectors.AcquireSector(ctx, sector, storiface.FTUnsealed|storiface.FTSealed|storiface.FTCache, storiface.FTUpdate|storiface.FTUpdateCache, storiface.PathSealing)
	if err != nil {
		return empty, xerrors.Errorf("failed to acquire sector paths: %w", err)
	}
	defer done()

	updateProofType := abi.SealProofInfos[sector.ProofType].UpdateProof

	s, err := os.Stat(paths.Sealed)
	if err != nil {
		return empty, err
	}
	sealedSize := s.Size()

	u, err := os.OpenFile(paths.Update, os.O_RDWR|os.O_CREATE, 0644) // nolint:gosec
	if err != nil {
		return empty, xerrors.Errorf("ensuring updated replica file exists: %w", err)
	}
	if err := fallocate.Fallocate(u, 0, sealedSize); err != nil {
		return empty, xerrors.Errorf("allocating space for replica update file: %w", err)
	}

	if err := u.Close(); err != nil {
		return empty, err
	}

	if err := os.Mkdir(paths.UpdateCache, 0755); err != nil { // nolint
		if os.IsExist(err) {
			log.Warnf("existing cache in %s; removing", paths.UpdateCache)

			if err := os.RemoveAll(paths.UpdateCache); err != nil {
				return empty, xerrors.Errorf("remove existing sector cache from %s (sector %d): %w", paths.UpdateCache, sector, err)
			}

			if err := os.Mkdir(paths.UpdateCache, 0755); err != nil { // nolint:gosec
				return empty, xerrors.Errorf("mkdir cache path after cleanup: %w", err)
			}
		} else {
			return empty, err
		}
	}
	sealed, unsealed, err := ffi.SectorUpdate.EncodeInto(updateProofType, paths.Update, paths.UpdateCache, paths.Sealed, paths.Cache, paths.Unsealed, pieces)
	if err != nil {
		return empty, xerrors.Errorf("failed to update replica %d with new deal data: %w", sector.ID.Number, err)
	}
	return storage.ReplicaUpdateOut{NewSealed: sealed, NewUnsealed: unsealed}, nil
}

func (sb *Sealer) ProveReplicaUpdate1(ctx context.Context, sector storage.SectorRef, sectorKey, newSealed, newUnsealed cid.Cid) (storage.ReplicaVanillaProofs, error) {
	paths, done, err := sb.sectors.AcquireSector(ctx, sector, storiface.FTSealed|storiface.FTCache|storiface.FTUpdate|storiface.FTUpdateCache, storiface.FTNone, storiface.PathSealing)
	if err != nil {
		return nil, xerrors.Errorf("failed to acquire sector paths: %w", err)
	}
	defer done()

	updateProofType := abi.SealProofInfos[sector.ProofType].UpdateProof

	vanillaProofs, err := ffi.SectorUpdate.GenerateUpdateVanillaProofs(updateProofType, sectorKey, newSealed, newUnsealed, paths.Update, paths.UpdateCache, paths.Sealed, paths.Cache)
	if err != nil {
		return nil, xerrors.Errorf("failed to generate proof of replica update for sector %d: %w", sector.ID.Number, err)
	}
	return vanillaProofs, nil
}

func (sb *Sealer) ProveReplicaUpdate2(ctx context.Context, sector storage.SectorRef, sectorKey, newSealed, newUnsealed cid.Cid, vanillaProofs storage.ReplicaVanillaProofs) (storage.ReplicaUpdateProof, error) {
	updateProofType := abi.SealProofInfos[sector.ProofType].UpdateProof
	return ffi.SectorUpdate.GenerateUpdateProofWithVanilla(updateProofType, sectorKey, newSealed, newUnsealed, vanillaProofs)
}

func (sb *Sealer) GenerateSectorKeyFromData(ctx context.Context, sector storage.SectorRef, commD cid.Cid) error {
	paths, done, err := sb.sectors.AcquireSector(ctx, sector, storiface.FTUnsealed|storiface.FTCache|storiface.FTUpdate|storiface.FTUpdateCache, storiface.FTSealed, storiface.PathSealing)
	defer done()
	if err != nil {
		return xerrors.Errorf("failed to acquire sector paths: %w", err)
	}

	s, err := os.Stat(paths.Update)
	if err != nil {
		return xerrors.Errorf("measuring update file size: %w", err)
	}
	sealedSize := s.Size()
	e, err := os.OpenFile(paths.Sealed, os.O_RDWR|os.O_CREATE, 0644) // nolint:gosec
	if err != nil {
		return xerrors.Errorf("ensuring sector key file exists: %w", err)
	}
	if err := fallocate.Fallocate(e, 0, sealedSize); err != nil {
		return xerrors.Errorf("allocating space for sector key file: %w", err)
	}
	if err := e.Close(); err != nil {
		return err
	}

	updateProofType := abi.SealProofInfos[sector.ProofType].UpdateProof
	return ffi.SectorUpdate.RemoveData(updateProofType, paths.Sealed, paths.Cache, paths.Update, paths.UpdateCache, paths.Unsealed, commD)
}

func (sb *Sealer) ReleaseSealed(ctx context.Context, sector storage.SectorRef) error {
	return xerrors.Errorf("not supported at this layer")
}

func (sb *Sealer) freeUnsealed(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range) error {
	ssize, err := sector.ProofType.SectorSize()
	if err != nil {
		return err
	}
	maxPieceSize := abi.PaddedPieceSize(ssize)

	if len(keepUnsealed) > 0 {

		sr := partialfile.PieceRun(0, maxPieceSize)

		for _, s := range keepUnsealed {
			si := &rlepluslazy.RunSliceIterator{}
			if s.Offset != 0 {
				si.Runs = append(si.Runs, rlepluslazy.Run{Val: false, Len: uint64(s.Offset)})
			}
			si.Runs = append(si.Runs, rlepluslazy.Run{Val: true, Len: uint64(s.Size)})

			var err error
			sr, err = rlepluslazy.Subtract(sr, si)
			if err != nil {
				return err
			}
		}

		paths, done, err := sb.sectors.AcquireSector(ctx, sector, storiface.FTUnsealed, 0, storiface.PathSealing)
		if err != nil {
			return xerrors.Errorf("acquiring sector unsealed path: %w", err)
		}
		defer done()

		pf, err := partialfile.OpenPartialFile(maxPieceSize, paths.Unsealed)
		if err == nil {
			var at uint64
			for sr.HasNext() {
				r, err := sr.NextRun()
				if err != nil {
					_ = pf.Close()
					return err
				}

				offset := at
				at += r.Len
				if !r.Val {
					continue
				}

				err = pf.Free(storiface.PaddedByteIndex(abi.UnpaddedPieceSize(offset).Padded()), abi.UnpaddedPieceSize(r.Len).Padded())
				if err != nil {
					//_ = pf.Close()
					//return xerrors.Errorf("free partial file range: %w", err)
					log.Errorf("free partial file range: %v", err)
					break
				}
			}

			if err := pf.Close(); err != nil {
				return err
			}
		} else {
			if !xerrors.Is(err, os.ErrNotExist) {
				return xerrors.Errorf("opening partial file: %w", err)
			}
		}

	}

	return nil
}

func (sb *Sealer) FinalizeSector(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range) error {
	ssize, err := sector.ProofType.SectorSize()
	if err != nil {
		return err
	}

	if err := sb.freeUnsealed(ctx, sector, keepUnsealed); err != nil {
		return err
	}

	paths, done, err := sb.sectors.AcquireSector(ctx, sector, storiface.FTCache, 0, storiface.PathStorage)
	if err != nil {
		return xerrors.Errorf("acquiring sector cache path: %w", err)
	}
	defer done()

	if QiniuFeatureEnabled(QiniuFeatureFinalizeUpload) && QiniuFeatureEnabled(QiniuFeatureSingleSectorPath) {
		if err = sb.qiniuFinalizeSector(ctx, sector, keepUnsealed, paths); err != nil {
			return err
		}
	}

	if QiniuFeatureEnabled(QiniuFeatureFinalizeUpload) && QiniuFeatureEnabled(QiniuFeatureFinalizeUploadAutoClean) {
		//注意，这里是直接return
		return sb.qiniuClearFile(ctx, sector, keepUnsealed, paths)
	}

	return ffi.ClearCache(uint64(ssize), paths.Cache)
}

func (sb *Sealer) qiniuClearFile(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range, paths storiface.SectorPaths) error {
	uploadUnseal := len(keepUnsealed) > 0
	pathNew := storiface.SectorPaths{
		ID:    sector.ID,
		Cache: paths.Cache,
	}

	sealPath, done1, err := sb.sectors.AcquireSector(ctx, sector, storiface.FTSealed, 0, storiface.PathStorage)
	if err != nil {
		return xerrors.Errorf("acquiring sector seal path: %w", err)
	} else {
		pathNew.Sealed = sealPath.Sealed
	}
	defer done1()

	if uploadUnseal {
		unsealPath, done2, err := sb.sectors.AcquireSector(ctx, sector, storiface.FTUnsealed, 0, storiface.PathStorage)
		if err != nil {
			return xerrors.Errorf("acquiring sector unseal path: %w", err)
		} else {
			pathNew.Unsealed = unsealPath.Unsealed
		}
		defer done2()
	}

	return qiniuCleanSeal(pathNew, uploadUnseal)

}

func (sb *Sealer) qiniuFinalizeSector(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range, paths storiface.SectorPaths) error {
	// 此处用于FinalizeSector阶段上传，必须指定一个绝对路径作为上传路径
	// 如果打开自动开关，则我们修改调度逻辑，在此处清理本地文件，并且后续不再触发MoveStorage操作
	// 例如：QINIU=/root/cfg.toml QINIU_FINALIZE_UPLOAD=true QINIU_STORE_PATH=/ceph ./lotus-worker
	pathNew := storiface.SectorPaths{
		ID:    sector.ID,
		Cache: paths.Cache,
	}
	uploadUnseal := len(keepUnsealed) > 0

	sealPath, done1, err := sb.sectors.AcquireSector(ctx, sector, storiface.FTSealed, 0, storiface.PathStorage)
	if err != nil {
		return xerrors.Errorf("acquiring sector seal path: %w", err)
	} else {
		pathNew.Sealed = sealPath.Sealed
	}
	defer done1()

	if uploadUnseal {
		unsealPath, done2, err := sb.sectors.AcquireSector(ctx, sector, storiface.FTUnsealed, 0, storiface.PathStorage)
		if err != nil {
			return xerrors.Errorf("acquiring sector unseal path: %w", err)
		} else {
			pathNew.Unsealed = unsealPath.Unsealed
		}
		defer done2()
	}

	log.Debugf("QINIU DEBUG path %s", pathNew)

	return SubmitFixedPathSector(pathNew, sector.ID, uploadUnseal)
}

func (sb *Sealer) FinalizeReplicaUpdate(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range) error {
	ssize, err := sector.ProofType.SectorSize()
	if err != nil {
		return err
	}

	if err := sb.freeUnsealed(ctx, sector, keepUnsealed); err != nil {
		return err
	}

	{
		paths, done, err := sb.sectors.AcquireSector(ctx, sector, storiface.FTCache, 0, storiface.PathStorage)
		if err != nil {
			return xerrors.Errorf("acquiring sector cache path: %w", err)
		}
		defer done()

		if err := ffi.ClearCache(uint64(ssize), paths.Cache); err != nil {
			return xerrors.Errorf("clear cache: %w", err)
		}
	}

	{
		paths, done, err := sb.sectors.AcquireSector(ctx, sector, storiface.FTUpdateCache, 0, storiface.PathStorage)
		if err != nil {
			return xerrors.Errorf("acquiring sector cache path: %w", err)
		}
		defer done()

		if err := ffi.ClearCache(uint64(ssize), paths.UpdateCache); err != nil {
			return xerrors.Errorf("clear cache: %w", err)
		}
	}

	return nil
}

func (sb *Sealer) ReleaseUnsealed(ctx context.Context, sector storage.SectorRef, safeToFree []storage.Range) error {
	// This call is meant to mark storage as 'freeable'. Given that unsealing is
	// very expensive, we don't remove data as soon as we can - instead we only
	// do that when we don't have free space for data that really needs it

	// This function should not be called at this layer, everything should be
	// handled in localworker
	return xerrors.Errorf("not supported at this layer")
}

func (sb *Sealer) ReleaseReplicaUpgrade(ctx context.Context, sector storage.SectorRef) error {
	return xerrors.Errorf("not supported at this layer")
}

func (sb *Sealer) ReleaseSectorKey(ctx context.Context, sector storage.SectorRef) error {
	return xerrors.Errorf("not supported at this layer")
}

func (sb *Sealer) Remove(ctx context.Context, sector storage.SectorRef) error {
	return xerrors.Errorf("not supported at this layer") // happens in localworker
}

func GetRequiredPadding(oldLength abi.PaddedPieceSize, newPieceLength abi.PaddedPieceSize) ([]abi.PaddedPieceSize, abi.PaddedPieceSize) {

	padPieces := make([]abi.PaddedPieceSize, 0)

	toFill := uint64(-oldLength % newPieceLength)

	n := bits.OnesCount64(toFill)
	var sum abi.PaddedPieceSize
	for i := 0; i < n; i++ {
		next := bits.TrailingZeros64(toFill)
		psize := uint64(1) << uint(next)
		toFill ^= psize

		padded := abi.PaddedPieceSize(psize)
		padPieces = append(padPieces, padded)
		sum += padded
	}

	return padPieces, sum
}

func GenerateUnsealedCID(proofType abi.RegisteredSealProof, pieces []abi.PieceInfo) (cid.Cid, error) {
	ssize, err := proofType.SectorSize()
	if err != nil {
		return cid.Undef, err
	}

	pssize := abi.PaddedPieceSize(ssize)
	allPieces := make([]abi.PieceInfo, 0, len(pieces))
	if len(pieces) == 0 {
		allPieces = append(allPieces, abi.PieceInfo{
			Size:     pssize,
			PieceCID: zerocomm.ZeroPieceCommitment(pssize.Unpadded()),
		})
	} else {
		var sum abi.PaddedPieceSize

		padTo := func(pads []abi.PaddedPieceSize) {
			for _, p := range pads {
				allPieces = append(allPieces, abi.PieceInfo{
					Size:     p,
					PieceCID: zerocomm.ZeroPieceCommitment(p.Unpadded()),
				})

				sum += p
			}
		}

		for _, p := range pieces {
			ps, _ := GetRequiredPadding(sum, p.Size)
			padTo(ps)

			allPieces = append(allPieces, p)
			sum += p.Size
		}

		ps, _ := GetRequiredPadding(sum, pssize)
		padTo(ps)
	}

	return ffi.GenerateUnsealedCID(proofType, allPieces)
}

func (sb *Sealer) GenerateWinningPoStWithVanilla(ctx context.Context, proofType abi.RegisteredPoStProof, minerID abi.ActorID, randomness abi.PoStRandomness, vanillas [][]byte) ([]proof.PoStProof, error) {
	return ffi.GenerateWinningPoStWithVanilla(proofType, minerID, randomness, vanillas)
}

func (sb *Sealer) GenerateWindowPoStWithVanilla(ctx context.Context, proofType abi.RegisteredPoStProof, minerID abi.ActorID, randomness abi.PoStRandomness, proofs [][]byte, partitionIdx int) (proof.PoStProof, error) {
	pp, err := ffi.GenerateSinglePartitionWindowPoStWithVanilla(proofType, minerID, randomness, proofs, uint(partitionIdx))
	if err != nil {
		return proof.PoStProof{}, err
	}
	if pp == nil {
		// should be impossible, but just in case do not panic
		return proof.PoStProof{}, xerrors.New("postproof was nil")
	}

	return proof.PoStProof{
		PoStProof:  pp.PoStProof,
		ProofBytes: pp.ProofBytes,
	}, nil
}