package file

import (
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/cbrewster/gcs-emulator/internal/chunkstore"
)

func chunkPath(storeDir string, chunkHash chunkstore.ChunkHash) string {
	return filepath.Join(
		storeDir, "chunks",
		fmt.Sprintf("%X", chunkHash[:2]),
		fmt.Sprintf("%X", chunkHash),
	)
}

type store struct {
	dir string
}

var _ chunkstore.Store = (*store)(nil)

type chunkWriter struct {
	storeDir     string
	file         *os.File
	md5Hasher    hash.Hash
	sha256Hasher hash.Hash
	closed       atomic.Bool
}

var _ chunkstore.ChunkWriter = (*chunkWriter)(nil)

func New(dir string) (chunkstore.Store, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, fmt.Errorf("make chunk dir: %w", err)
	}

	return &store{dir: dir}, nil
}

// NewReader implements chunkstore.Store.
func (s *store) NewReader(hash chunkstore.ChunkHash) (io.ReadSeekCloser, error) {
	return os.OpenFile(chunkPath(s.dir, hash), os.O_RDONLY, 0)
}

// Delete implements chunkstore.Store.
func (s *store) Delete(hash chunkstore.ChunkHash) error {
	return os.Remove(chunkPath(s.dir, hash))
}

// NewWriter implements chunkstore.Store.
func (s *store) NewWriter() (chunkstore.ChunkWriter, error) {
	uploadsDir := filepath.Join(s.dir, "uploads")
	err := os.MkdirAll(uploadsDir, 0755)
	if err != nil {
		return nil, fmt.Errorf("create uploads dir: %w", err)
	}

	file, err := os.CreateTemp(uploadsDir, "partial-*")
	if err != nil {
		return nil, fmt.Errorf("create file: %w", err)
	}

	// TODO: Consider bufio?
	return &chunkWriter{
		storeDir:     s.dir,
		file:         file,
		md5Hasher:    md5.New(),
		sha256Hasher: sha256.New(),
	}, nil
}

// Write implements chunkstore.ChunkWriter.
func (w *chunkWriter) Write(p []byte) (int, error) {
	n, err := w.file.Write(p)
	if err != nil {
		return n, err
	}

	w.md5Hasher.Write(p[:n])
	w.sha256Hasher.Write(p[:n])

	return n, err
}

// Close implements chunkstore.ChunkWriter.
func (w *chunkWriter) Close() (chunkstore.ChunkHash, chunkstore.MD5Hash, error) {
	if w.closed.Swap(true) {
		return chunkstore.ChunkHash{}, chunkstore.MD5Hash{}, os.ErrClosed
	}

	defer os.Remove(w.file.Name())

	err := w.file.Sync()
	if err != nil {
		return chunkstore.ChunkHash{}, chunkstore.MD5Hash{}, fmt.Errorf("sync file: %w", err)
	}

	err = w.file.Close()
	if err != nil {
		return chunkstore.ChunkHash{}, chunkstore.MD5Hash{}, fmt.Errorf("close file: %w", err)
	}

	md5Hash := chunkstore.MD5Hash(w.md5Hasher.Sum(nil))
	chunkHash := chunkstore.ChunkHash(w.sha256Hasher.Sum(nil))

	dest := chunkPath(w.storeDir, chunkHash)
	err = os.MkdirAll(filepath.Dir(dest), 0755)
	if err != nil {
		return chunkstore.ChunkHash{}, chunkstore.MD5Hash{}, fmt.Errorf("make chunk dir: %w", err)
	}

	err = os.Rename(w.file.Name(), dest)
	if err != nil {
		return chunkstore.ChunkHash{}, chunkstore.MD5Hash{}, fmt.Errorf("rename chunk: %w", err)
	}

	return chunkHash, md5Hash, nil
}
