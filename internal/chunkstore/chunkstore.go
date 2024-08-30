package chunkstore

import (
	"crypto/md5"
	"crypto/sha256"
	"io"
)

type ChunkHash = [sha256.Size]byte
type MD5Hash = [md5.Size]byte

type Store interface {
	NewWriter() (ChunkWriter, error)
	NewReader(ChunkHash) (io.ReadSeekCloser, error)
	Delete(ChunkHash) error
}

type ChunkWriter interface {
	io.Writer
	Close() (ChunkHash, MD5Hash, error)
}
