# TODO

- Implement Seek support on `ObjectReader`
  We'll need to get chunk sizes to support quickly seeking
- Initial HTTP server
- Object metadata
- Preconditions
- Bucket Listing
- Object Listing
  With support for offset & prefix
- Versioning support
  Groundwork is already laid, but need to expose it at higher layers
- Chunk store garbage collection
  Need to either support a GC scan or some sort of reference counting.
  Design is to be determined.
