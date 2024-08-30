package objectstore_test

import (
	"crypto/rand"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/shoenig/test/must"

	"github.com/cbrewster/gcs-emulator/internal/chunkstore"
	"github.com/cbrewster/gcs-emulator/internal/chunkstore/file"
	"github.com/cbrewster/gcs-emulator/internal/metastore"
	"github.com/cbrewster/gcs-emulator/internal/metastore/bolt"
	"github.com/cbrewster/gcs-emulator/internal/objectstore"
)

func newFileStore(t *testing.T) chunkstore.Store {
	dir, err := os.MkdirTemp("", "chunkstore-test-*")
	must.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})

	store, err := file.New(dir)
	must.NoError(t, err)

	return store
}

func newBoltStore(t *testing.T) metastore.Store {
	dir, err := os.MkdirTemp("", "metastore-test-*")
	must.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})

	store, err := bolt.New(filepath.Join(dir, "db.bolt"))
	must.NoError(t, err)

	return store
}

var testCases = []struct {
	name       string
	metaStore  func(t *testing.T) metastore.Store
	chunkStore func(t *testing.T) chunkstore.Store
}{{
	name:       "bolt+file",
	metaStore:  newBoltStore,
	chunkStore: newFileStore,
}}

func TestWriteReadObject(t *testing.T) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := objectstore.New(tc.metaStore(t), tc.chunkStore(t))

			bucket, err := store.CreateBucket("my-bucket")
			must.NoError(t, err)

			data, err := io.ReadAll(io.LimitReader(rand.Reader, 1024))
			must.NoError(t, err)

			object := bucket.Object("cool")

			w, err := object.NewWriter()
			must.NoError(t, err)
			defer w.Close()

			_, err = w.Write(data)
			must.NoError(t, err)

			err = w.Close()
			must.NoError(t, err)

			expectedHash := sha256.Sum256(data)
			metadata := w.Metadata()
			must.Eq(t, expectedHash, metadata.Chunks[0])

			r, err := object.NewReader()
			must.NoError(t, err)
			defer r.Close()

			read, err := io.ReadAll(r)
			must.NoError(t, err)
			must.Eq(t, data, read)
		})
	}
}
