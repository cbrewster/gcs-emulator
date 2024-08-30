package metastore

import (
	"crypto/md5"
	"errors"
	"time"

	"github.com/cbrewster/gcs-emulator/internal/chunkstore"
)

var (
	ErrNotExist      = errors.New("does not exist")
	ErrAlreadyExists = errors.New("already exists")
)

type Store interface {
	Bucket(name string) (Bucket, error)
	CreateBucket(name string, options NewBucketOptions) (Bucket, error)
	DeleteBucket(name string) error
}

type Bucket interface {
	Metadata() (*BucketMetadata, error)
	Object(name string) (*Object, error)
	PutObject(name string, options PutObjectOptions) (*Object, error)
}

type NewBucketOptions struct {
	Versioning bool
}

type PutObjectOptions struct {
	Chunks []chunkstore.ChunkHash
	MD5Sum [md5.Size]byte
}

type BucketMetadata struct {
	CreatedAt time.Time
	UpdatedAt time.Time

	Versioning bool
}

type Object struct {
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt time.Time

	Chunks         []chunkstore.ChunkHash
	MD5Sum         [md5.Size]byte
	Generation     int64
	Metageneration int64
}
