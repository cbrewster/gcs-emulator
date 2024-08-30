package metastore_test

import (
	"crypto/md5"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/shoenig/test/must"

	"github.com/cbrewster/gcs-emulator/internal/chunkstore"
	"github.com/cbrewster/gcs-emulator/internal/metastore"
	"github.com/cbrewster/gcs-emulator/internal/metastore/bolt"
)

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
	name  string
	store func(t *testing.T) metastore.Store
}{{
	name:  "bolt",
	store: newBoltStore,
}}

var ignoreBucketTimestamps = must.Cmp(cmpopts.IgnoreFields(metastore.BucketMetadata{}, "CreatedAt", "UpdatedAt"))

func TestCreateBucket(t *testing.T) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.store(t)

			bucket, err := store.CreateBucket("test-bucket", metastore.NewBucketOptions{})
			must.NoError(t, err)

			metadata, err := bucket.Metadata()
			must.NoError(t, err)
			must.Eq(t, &metastore.BucketMetadata{}, metadata, ignoreBucketTimestamps)

			_, err = store.CreateBucket("test-bucket", metastore.NewBucketOptions{})
			must.ErrorIs(t, err, metastore.ErrAlreadyExists)

			bucket, err = store.CreateBucket("versioned-bucket", metastore.NewBucketOptions{
				Versioning: true,
			})
			must.NoError(t, err)

			metadata, err = bucket.Metadata()
			must.NoError(t, err)
			must.Eq(t, &metastore.BucketMetadata{Versioning: true}, metadata, ignoreBucketTimestamps)
		})
	}
}

func TestCreateObjects(t *testing.T) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.store(t)

			bucket, err := store.CreateBucket("test-bucket", metastore.NewBucketOptions{})
			must.NoError(t, err)

			putRes, err := bucket.PutObject("foo", metastore.PutObjectOptions{
				Chunks: []chunkstore.ChunkHash{sha256.Sum256([]byte("phony"))},
				MD5Sum: md5.Sum([]byte("phony")),
			})
			must.NoError(t, err)
			must.NotEq(t, 0, putRes.Generation)
			must.Eq(t, 1, putRes.Metageneration)

			getRes, err := bucket.Object("foo")
			must.NoError(t, err)
			must.Eq(t, putRes, getRes)

			putRes, err = bucket.PutObject("foo", metastore.PutObjectOptions{
				Chunks: []chunkstore.ChunkHash{sha256.Sum256([]byte("phony"))},
				MD5Sum: md5.Sum([]byte("phony")),
			})
			must.NoError(t, err)
			must.NotEq(t, getRes, putRes)

			otherBucket, err := store.CreateBucket("other-bucket", metastore.NewBucketOptions{})
			must.NoError(t, err)

			_, err = otherBucket.Object("foo")
			must.ErrorIs(t, err, metastore.ErrNotExist)
		})
	}
}
