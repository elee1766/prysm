package haystack

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/spf13/afero"
	"go.etcd.io/bbolt"
)

const indexSizeOffset = 8 + 8 + 32

var haystackIndexBucket = []byte("HaystackBucketIndex")
var haystackMetaBucket = []byte("HaystackBucketMeta")

var haystackMagicHead = int32(0x44beef)
var haystackMagicFoot = int32(0xbeef44)

type Config struct {
	MaxPileSize int
}

type Haystack struct {
	Config Config

	db *bbolt.DB
	fs afero.Fs

	currentPile Pile
}

type Pile struct {
	fs      afero.File
	name    uint64
	dataCur uint64
}

func NewHaystack(db *bbolt.DB, root afero.Fs) *Haystack {
	haystackFS := afero.NewCacheOnReadFs(root, afero.NewMemMapFs(), 60*time.Second)
	return &Haystack{
		fs: haystackFS,
		db: db,
	}
}
func (h *Haystack) flushPile(tx *bbolt.Tx) (err error) {
	err = tx.Bucket(haystackMetaBucket).Put([]byte("current_pile_idx"), binary.LittleEndian.AppendUint64(nil, h.currentPile.name))
	if err != nil {
		return err
	}
	err = tx.Bucket(haystackMetaBucket).Put([]byte("current_pile_cur"), binary.LittleEndian.AppendUint64(nil, h.currentPile.dataCur))
	if err != nil {
		return err
	}
	return nil
}
func (h *Haystack) syncPile(tx *bbolt.Tx) (err error) {
	lastname := h.currentPile.name
	h.currentPile.name = binary.LittleEndian.Uint64(tx.Bucket(haystackMetaBucket).Get([]byte("current_pile_idx")))
	h.currentPile.dataCur = binary.LittleEndian.Uint64(tx.Bucket(haystackMetaBucket).Get([]byte("current_pile_cur")))
	if h.currentPile.fs == nil || (h.currentPile.name > 0 && lastname != h.currentPile.name) {
		h.currentPile.fs, err = h.fs.Open(fmt.Sprintf("blobs/%d.pile", h.currentPile))
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *Haystack) addBlob(tx *bbolt.Tx, key []byte, data []byte) error {
	if err := h.syncPile(tx); err != nil {
		return err
	}
	idxNeedle := &IndexNeedle{}
	res := tx.Bucket(haystackIndexBucket).Get(key)
	if res == nil {
		return nil
	}
	if err := binary.Read(bytes.NewBuffer(res), binary.LittleEndian, idxNeedle); err != nil {
		return err
	}
	// new needle
	blobNeedle := &BlobNeedle{
		MagicHead: haystackMagicHead,
		Flag:      0,
		KeySize:   int32(len(key)),
		Key:       key,
		DataSize:  int32(len(data)),
		Data:      data,
		MagicFoot: haystackMagicFoot,
		Checksum:  0,
	}
	_, err := h.currentPile.fs.Seek(int64(h.currentPile.dataCur), io.SeekStart)
	if err != nil {
		return err
	}
	idxNeedle.Offset = int64(h.currentPile.dataCur)
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, blobNeedle)
	if err != nil {
		return err
	}
	written, err := h.currentPile.fs.Write(binary.LittleEndian.AppendUint64(nil, h.currentPile.dataCur))
	h.currentPile.dataCur = h.currentPile.dataCur + uint64(written)
	if err != nil {
		return err
	}
	err = h.flushPile(tx)
	if err != nil {
		return err
	}
	idxNeedle.Pile = int64(h.currentPile.name)
	idxNeedle.Size = int64(written)
	idxNeedle.Flag = 0

	buf.Reset()
	err = binary.Write(buf, binary.LittleEndian, key)
	if err != nil {
		return err
	}
	err = tx.Bucket(haystackIndexBucket).Put(key, buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (h *Haystack) addIndexNeedle(key []byte, i IndexNeedle) error {
	val := make([]byte, 0, 8*4)
	val = binary.LittleEndian.AppendUint64(val, uint64(i.Pile))
	val = binary.LittleEndian.AppendUint64(val, uint64(i.Offset))
	val = binary.LittleEndian.AppendUint64(val, uint64(i.Size))
	val = binary.LittleEndian.AppendUint64(val, uint64(i.Flag))
	return h.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(haystackIndexBucket).Put(
			key,
			val,
		)
	})
}
