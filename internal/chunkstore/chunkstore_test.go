package chunkstore_test

import (
	"io"
	"os"
	"testing"

	"github.com/cbrewster/gcs-emulator/internal/chunkstore"
	"github.com/cbrewster/gcs-emulator/internal/chunkstore/file"
	"github.com/shoenig/test/must"
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

var testCases = []struct {
	name  string
	store func(t *testing.T) chunkstore.Store
}{{
	name:  "file",
	store: newFileStore,
}}

func TestWriteReadDeleteChunk(t *testing.T) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.store(t)

			contents := []byte("hello world")

			w, err := store.NewWriter()
			must.NoError(t, err)
			defer w.Close()

			_, err = w.Write(contents)
			must.NoError(t, err)

			chunkHash, _, err := w.Close()
			must.NoError(t, err)

			r, err := store.NewReader(chunkHash)
			must.NoError(t, err)
			defer r.Close()

			read, err := io.ReadAll(r)
			must.NoError(t, err)
			must.Eq(t, contents, read)

			err = store.Delete(chunkHash)
			must.NoError(t, err)

			_, err = store.NewReader(chunkHash)
			must.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}
