package stores

import (
	"errors"
	"fmt"
	"github.com/filecoin-project/lotus/extern/sector-storage/fsutil"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"
	"io"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

func (t *storageEntry) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write([]byte{164}); err != nil {
		return err
	}
	scratch := make([]byte, 9)
	// Info
	if err := t.Info.MarshalCBOR(w); err != nil {
		return err
	}
	// fsi
	if err := t.Fsi.MarshalCBOR(w); err != nil {
		return err
	}
	// last heartbeat
	time, err := t.LastHeartbeat.MarshalBinary()
	if err != nil {
		return err
	}
	if len(time) > cbg.ByteArrayMaxLen {
		return xerrors.Errorf("Byte array in field time was too long")
	}
	if err = cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajByteString, uint64(len(time))); err != nil {
		return err
	}
	if _, err = w.Write(time[:]); err != nil {
		return err
	}
	// error
	if t.HeartbeatErr == nil {
		if _, err := w.Write(cbg.CborNull); err != nil {
			return err
		}
	} else {
		if err = cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len(t.HeartbeatErr.Error()))); err != nil {
			return err
		}
		if _, err = io.WriteString(w, t.HeartbeatErr.Error()); err != nil {
			return err
		}
	}
	return nil
}

func (t *storageEntry) UnmarshalCBOR(r io.Reader) error {
	*t = storageEntry{}

	br := cbg.GetPeeker(r)
	scratch := make([]byte, 8)

	maj, extra, err := cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}
	if maj != cbg.MajMap {
		return fmt.Errorf("cbor input should be of type map, but is %v", maj)
	}

	// info
	var info storiface.StorageInfo
	info.UnmarshalCBOR(br)
	t.Info = &info

	// fsi
	var fsi fsutil.FsStat
	fsi.UnmarshalCBOR(br)
	t.Fsi = fsi

	// heartbeat time
	maj, extra, err = cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}
	if extra > cbg.ByteArrayMaxLen {
		return fmt.Errorf("t.Value: byte array too large (%d)", extra)
	}
	if maj != cbg.MajByteString {
		return fmt.Errorf("expected byte array")
	}
	if extra > 0 {
		bytes := make([]uint8, extra)
		if _, err = io.ReadFull(br, bytes[:]); err != nil {
			return err
		}
		if err = t.LastHeartbeat.UnmarshalBinary(bytes); err != nil {
			return err
		}
	}

	// heartbeat err
	b, err := br.ReadByte()
	if err != nil {
		return err
	}
	if b != cbg.CborNull[0] {
		if err := br.UnreadByte(); err != nil {
			return err
		}

		errStr, err := cbg.ReadStringBuf(r, scratch)
		if err != nil {
			return err
		}
		t.HeartbeatErr = errors.New(errStr)
	}

	return nil
}



// declMeta
func (t *declMeta) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write([]byte{162}); err != nil {
		return err
	}
	scratch := make([]byte, 9)
	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len(t.Storage))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string(t.Storage)); err != nil {
		return err
	}

	if err := cbg.WriteBool(w, t.Primary); err != nil {
		return err
	}

	return nil
}

func (t *declMeta) UnmarshalCBOR(r io.Reader) error {
	*t = declMeta{}

	br := cbg.GetPeeker(r)
	scratch := make([]byte, 8)

	maj, extra, err := cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}
	if maj != cbg.MajMap {
		return fmt.Errorf("cbor input should be of type map")
	}

	id, err := cbg.ReadStringBuf(br, scratch)
	t.Storage = storiface.ID(id)

	maj, extra, err = cbg.CborReadHeaderBuf(br, scratch)
	if maj != cbg.MajOther {
		return fmt.Errorf("booleans must be major type 7")
	}
	switch extra {
	case 20:
		t.Primary = false
	case 21:
		t.Primary = true
	default:
		return fmt.Errorf("booleans are either major type 7, value 20 or 21 (got %d)", extra)
	}

	return nil
}
