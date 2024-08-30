package bolt

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cbrewster/gcs-emulator/internal/chunkstore"
	"github.com/cbrewster/gcs-emulator/internal/metastore"
	"go.etcd.io/bbolt"
)

var (
	// rootBucketName is the root bolt bucket where bucket metadata is stored.
	rootBucketName = []byte("buckets")
	// objectsBucketName is the nested bucket which contains the actual objects.
	objectsBucketName = []byte("buckets")
	// bucketMetaKey contains metadata about the bucket configuration.
	bucketMetaKey = []byte("metadata")
)

type bucketMetadata struct {
	UpdatedAt time.Time `json:"updated_at"`
	CreatedAt time.Time `json:"created_at"`

	Generation     int64      `json:"generation"`
	Metageneration int64      `json:"metageneration"`
	Versioning     versioning `json:"versioning,omitempty"`
}

type versioning struct {
	Enabled bool `json:"enabled,omitempty"`
}

type objectMetadata struct {
	Current    *objectVersion  `json:"current,omitempty"`
	NonCurrent []objectVersion `json:"non_current,omitempty"`
}

type objectVersion struct {
	UpdatedAt time.Time `json:"updated_at"`
	CreatedAt time.Time `json:"created_at"`
	DeletedAt time.Time `json:"deleted_at"`

	Chunks         []chunkstore.ChunkHash `json:"chunks"`
	MD5            [md5.Size]byte         `json:"md5,omitempty"`
	Generation     int64                  `json:"generation"`
	Metageneration int64                  `json:"metageneration"`
}

type store struct {
	db *bbolt.DB
}

var _ metastore.Store = (*store)(nil)

type bucket struct {
	db   *bbolt.DB
	name []byte
}

var _ metastore.Bucket = (*bucket)(nil)

func newGeneration() int64 {
	return time.Now().UnixNano()
}

func New(path string) (metastore.Store, error) {
	db, err := bbolt.Open(path, 0755, nil)
	if err != nil {
		return nil, fmt.Errorf("open bolt db: %w", err)
	}

	tx, err := db.Begin(true)
	if err != nil {
		return nil, fmt.Errorf("begin db tx: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.CreateBucketIfNotExists(rootBucketName)
	if err != nil {
		return nil, fmt.Errorf("create root bucket: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("commit root bucket: %w", err)
	}

	return &store{
		db: db,
	}, nil
}

// CreateBucket implements Store.
func (s *store) CreateBucket(
	name string,
	options metastore.NewBucketOptions,
) (metastore.Bucket, error) {
	tx, err := s.db.Begin(true)
	if err != nil {
		return nil, fmt.Errorf("begin db tx: %w", err)
	}
	defer tx.Rollback()

	existing := tx.Bucket(rootBucketName).Bucket([]byte(name))
	if existing != nil {
		return nil, metastore.ErrAlreadyExists
	}

	b, err := tx.Bucket(rootBucketName).CreateBucket([]byte(name))
	if err != nil {
		return nil, fmt.Errorf("create bucket: %w", err)
	}
	_, err = b.CreateBucket(objectsBucketName)
	if err != nil {
		return nil, fmt.Errorf("create objects bucket: %w", err)
	}

	metadata, err := json.Marshal(bucketMetadata{
		UpdatedAt: time.Now(),
		CreatedAt: time.Now(),

		Generation:     newGeneration(),
		Metageneration: 1,
		Versioning: versioning{
			Enabled: options.Versioning,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("marshal bucket metadata: %w", err)
	}

	err = b.Put(bucketMetaKey, metadata)
	if err != nil {
		return nil, fmt.Errorf("write bucket meta: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("commit create bucket: %w", err)
	}

	return &bucket{db: s.db, name: []byte(name)}, nil
}

// DeleteBucket implements metastore.Store.
func (s *store) DeleteBucket(name string) error {
	tx, err := s.db.Begin(true)
	if err != nil {
		return fmt.Errorf("begin db tx: %w", err)
	}
	defer tx.Rollback()

	err = tx.Bucket(rootBucketName).DeleteBucket([]byte(name))
	if err != nil {
		return fmt.Errorf("delete bucket: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit delete bucket: %w", err)
	}

	return nil
}

// Bucket implements Store.
func (s *store) Bucket(name string) (metastore.Bucket, error) {
	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("begin db tx: %w", err)
	}
	defer tx.Rollback()

	b := tx.Bucket(rootBucketName).Bucket([]byte(name))
	if b == nil {
		return nil, metastore.ErrNotExist
	}

	return &bucket{db: s.db, name: []byte(name)}, nil
}

func (b *bucket) objectsBucket(tx *bbolt.Tx) *bbolt.Bucket {
	return tx.Bucket(rootBucketName).Bucket([]byte(b.name)).Bucket(objectsBucketName)
}

func (b *bucket) bucketMetadata(tx *bbolt.Tx) (*bucketMetadata, error) {
	metaBytes := tx.Bucket(rootBucketName).Bucket([]byte(b.name)).Get(bucketMetaKey)
	if metaBytes == nil {
		return nil, errors.New("bucket missing metadata")
	}

	var metadata bucketMetadata
	err := json.Unmarshal(metaBytes, &metadata)
	if err != nil {
		return nil, fmt.Errorf("unmarshal bucket metadata: %w", err)
	}

	return &metadata, nil
}

func (b *bucket) objectMetadata(tx *bbolt.Tx, name []byte) (*objectMetadata, error) {
	metaBytes := b.objectsBucket(tx).Get(name)
	if metaBytes == nil {
		return nil, metastore.ErrNotExist
	}

	var metadata objectMetadata
	err := json.Unmarshal(metaBytes, &metadata)
	if err != nil {
		return nil, fmt.Errorf("unmarshal object metadata: %w", err)
	}

	return &metadata, nil
}

func (b *bucket) putObjectMetadata(tx *bbolt.Tx, name []byte, metadata *objectMetadata) error {
	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("marshal object metadata: %w", err)
	}

	err = b.objectsBucket(tx).Put(name, metaBytes)
	if err != nil {
		return fmt.Errorf("put object metadata: %w", err)
	}

	return nil
}

// Metadata implements metastore.Bucket.
func (b *bucket) Metadata() (*metastore.BucketMetadata, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("begin db tx: %w", err)
	}
	defer tx.Rollback()

	metadata, err := b.bucketMetadata(tx)
	if err != nil {
		return nil, err
	}

	return &metastore.BucketMetadata{
		CreatedAt:  metadata.CreatedAt,
		UpdatedAt:  metadata.UpdatedAt,
		Versioning: metadata.Versioning.Enabled,
	}, nil
}

// Object implements Bucket.
func (b *bucket) Object(name string) (*metastore.Object, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("begin db tx: %w", err)
	}
	defer tx.Rollback()

	metadata, err := b.objectMetadata(tx, []byte(name))
	if err != nil {
		return nil, err
	}
	if metadata.Current == nil {
		return nil, metastore.ErrNotExist
	}
	version := metadata.Current

	return &metastore.Object{
		CreatedAt: version.CreatedAt,
		UpdatedAt: version.UpdatedAt,
		DeletedAt: version.DeletedAt,

		Chunks:         version.Chunks,
		MD5Sum:         version.MD5,
		Generation:     version.Generation,
		Metageneration: version.Metageneration,
	}, nil
}

// PutObject implements Bucket.
func (b *bucket) PutObject(
	name string,
	options metastore.PutObjectOptions,
) (*metastore.Object, error) {
	tx, err := b.db.Begin(true)
	if err != nil {
		return nil, fmt.Errorf("begin db tx: %w", err)
	}
	defer tx.Rollback()

	bucketMetadata, err := b.bucketMetadata(tx)
	if err != nil {
		return nil, err
	}

	oldMetadata, err := b.objectMetadata(tx, []byte(name))
	if err != nil && !errors.Is(err, metastore.ErrNotExist) {
		return nil, err
	}

	newMetadata := objectMetadata{
		Current: &objectVersion{
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),

			Chunks:         options.Chunks,
			MD5:            options.MD5Sum,
			Generation:     newGeneration(),
			Metageneration: 1,
		},
	}
	if oldMetadata != nil && oldMetadata.Current != nil && bucketMetadata.Versioning.Enabled {
		newMetadata.NonCurrent = append(oldMetadata.NonCurrent, *oldMetadata.Current)
	}

	err = b.putObjectMetadata(tx, []byte(name), &newMetadata)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("commit put object: %w", err)
	}

	version := newMetadata.Current
	return &metastore.Object{
		CreatedAt: version.CreatedAt,
		UpdatedAt: version.UpdatedAt,
		DeletedAt: version.DeletedAt,

		Chunks:         version.Chunks,
		MD5Sum:         version.MD5,
		Generation:     version.Generation,
		Metageneration: version.Metageneration,
	}, nil
}
