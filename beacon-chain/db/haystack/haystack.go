package haystack

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"

	"github.com/golang/snappy"
	"go.opencensus.io/trace"
	"google.golang.org/protobuf/proto"
)

type PileProvider interface {
	GetStacks() ([]uint64, error)
	GetFd(uint64) (io.ReadSeeker, error)
}

type IndexNeedle struct {
	Pile   int64
	Offset int64
	Size   int64
	Flag   int64
}

type BlobNeedle struct {
	MagicHead int32
	Flag      int64
	KeySize   int32
	Key       []byte
	DataSize  int32
	Data      []byte
	MagicFoot int32
	Checksum  int64
}

func FindNeedle(ctx context.Context, pile io.ReadSeeker, loc *IndexNeedle) (*BlobNeedle, error) {
	n := &BlobNeedle{}
	_, err := pile.Seek(loc.Offset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, loc.Size)
	_, err = io.ReadFull(pile, buf)
	if err != nil {
		return nil, err
	}
	fd := bytes.NewBuffer(buf)
	err = binary.Read(fd, binary.LittleEndian, &n.MagicHead)
	if err != nil {
		return nil, err
	}
	err = binary.Read(fd, binary.LittleEndian, &n.Flag)
	if err != nil {
		return nil, err
	}
	err = binary.Read(fd, binary.LittleEndian, &n.KeySize)
	if err != nil {
		return nil, err
	}
	n.Key = make([]byte, n.KeySize)
	err = binary.Read(fd, binary.LittleEndian, &n.Key)
	if err != nil {
		return nil, err
	}
	err = binary.Read(fd, binary.LittleEndian, &n.DataSize)
	if err != nil {
		return nil, err
	}
	n.Data = make([]byte, n.DataSize)
	_, err = io.ReadFull(fd, n.Data)
	if err != nil {
		return nil, err
	}
	err = binary.Read(fd, binary.LittleEndian, &n.MagicFoot)
	if err != nil {
		return nil, err
	}
	err = binary.Read(fd, binary.LittleEndian, &n.Checksum)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func decode(ctx context.Context, data []byte, dst proto.Message) error {
	ctx, span := trace.StartSpan(ctx, "BeaconDB.decode")
	defer span.End()

	data, err := snappy.Decode(nil, data)
	if err != nil {
		return err
	}
	return proto.Unmarshal(data, dst)
}
