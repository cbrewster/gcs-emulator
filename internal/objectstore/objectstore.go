package objectstore

import (
	"errors"
	"io"
	"os"
	"sync/atomic"

	"github.com/cbrewster/gcs-emulator/internal/chunkstore"
	"github.com/cbrewster/gcs-emulator/internal/metastore"
)

type Store struct {
	metaStore  metastore.Store
	chunkStore chunkstore.Store
}

func New(metaStore metastore.Store, chunkStore chunkstore.Store) *Store {
	return &Store{metaStore, chunkStore}
}

func (s *Store) Bucket(name string) (*Bucket, error) {
	metaBucket, err := s.metaStore.Bucket(name)
	if err != nil {
		return nil, err
	}

	return &Bucket{
		metaBucket: metaBucket,
		chunkStore: s.chunkStore,
		name:       name,
	}, nil
}

func (s *Store) CreateBucket(name string) (*Bucket, error) {
	metaBucket, err := s.metaStore.CreateBucket(name, metastore.NewBucketOptions{})
	if err != nil {
		return nil, err
	}

	return &Bucket{
		metaBucket: metaBucket,
		chunkStore: s.chunkStore,
		name:       name,
	}, nil
}

type Bucket struct {
	metaBucket metastore.Bucket
	chunkStore chunkstore.Store
	name       string
}

func (b *Bucket) Object(name string) *Object {
	return &Object{
		metaBucket: b.metaBucket,
		chunkStore: b.chunkStore,
		name:       b.name,
	}
}

type Object struct {
	metaBucket metastore.Bucket
	chunkStore chunkstore.Store
	name       string
}

func (o *Object) NewWriter() (*ObjectWriter, error) {
	writer, err := o.chunkStore.NewWriter()
	if err != nil {
		return nil, err
	}

	return &ObjectWriter{
		object: o,
		writer: writer,
	}, nil
}

type ObjectWriter struct {
	object   *Object
	writer   chunkstore.ChunkWriter
	metadata *metastore.Object
}

// Write implements io.WriteCloser.
func (w *ObjectWriter) Write(p []byte) (n int, err error) {
	return w.writer.Write(p)
}

// Close implements io.WriteCloser.
func (w *ObjectWriter) Close() error {
	chunkHash, md5Hash, err := w.writer.Close()
	if err != nil {
		return err
	}

	metadata, err := w.object.metaBucket.PutObject(w.object.name, metastore.PutObjectOptions{
		Chunks: []chunkstore.ChunkHash{chunkHash},
		MD5Sum: md5Hash,
	})
	if err != nil {
		// TODO: Not safe to delete chunk since it may be shared.
		return err
	}

	w.metadata = metadata

	return nil
}

// TODO: We shouldn't leak metastore outside, probably need _another_ set of types?
func (w *ObjectWriter) Metadata() *metastore.Object {
	return w.metadata
}

func (o *Object) NewReader() (*ObjectReader, error) {
	metadata, err := o.metaBucket.Object(o.name)
	if err != nil {
		return nil, err
	}

	return &ObjectReader{
		object:   o,
		metadata: metadata,
		queue:    metadata.Chunks,
	}, nil
}

type ObjectReader struct {
	object   *Object
	metadata *metastore.Object
	queue    []chunkstore.ChunkHash
	current  io.ReadSeekCloser
	closed   atomic.Bool
}

var _ io.ReadCloser = (*ObjectReader)(nil)

func (r *ObjectReader) next() error {
	if len(r.queue) == 0 {
		return io.EOF
	}

	var first chunkstore.ChunkHash
	first, r.queue = r.queue[0], r.queue[1:]

	var err error
	r.current, err = r.object.chunkStore.NewReader(first)
	if err != nil {
		return err
	}

	return nil
}

// Read implements io.ReadCloser.
func (r *ObjectReader) Read(p []byte) (int, error) {
	if r.closed.Load() {
		return 0, os.ErrClosed
	}

	if r.current == nil {
		err := r.next()
		if err != nil {
			return 0, err
		}
	}

	// TODO: Could read in a loop to move to next chunk, but meh this is fine for now.
	n, err := r.current.Read(p)
	if errors.Is(err, io.EOF) {
		err := r.current.Close()
		if err != nil {
			return n, err
		}
		r.current = nil
		return n, nil
	}

	return n, err
}

// Close implements io.ReadCloser.
func (r *ObjectReader) Close() error {
	if r.closed.Swap(true) {
		return os.ErrClosed
	}

	if r.current == nil {
		return nil
	}

	return r.current.Close()
}
