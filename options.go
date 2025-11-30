package qcow2

// Default cache sizes
const (
	// DefaultL2CacheSize is the default number of L2 table entries to cache.
	// With 64KB clusters, 32 entries = 2MB of L2 tables covering 16GB of virtual space.
	DefaultL2CacheSize = 32

	// DefaultCompressedCacheSize is the default number of decompressed clusters to cache.
	DefaultCompressedCacheSize = 16

	// DefaultRefcountCacheSize is the default number of refcount block entries to cache.
	DefaultRefcountCacheSize = 16
)

// Option configures how an image is opened.
type Option func(*imageOptions)

// imageOptions holds configuration for opening an image.
type imageOptions struct {
	l2CacheSize         int
	compressedCacheSize int
	refcountCacheSize   int
}

// defaultImageOptions returns the default configuration.
func defaultImageOptions() *imageOptions {
	return &imageOptions{
		l2CacheSize:         DefaultL2CacheSize,
		compressedCacheSize: DefaultCompressedCacheSize,
		refcountCacheSize:   DefaultRefcountCacheSize,
	}
}

// WithL2CacheSize sets the number of L2 table entries to cache.
// Each L2 table is one cluster in size (typically 64KB).
// With 64KB clusters and default 32 entries, this caches 2MB of L2 tables
// covering 16GB of virtual address space.
//
// Larger values improve performance for sequential access patterns
// and reduce disk I/O, but consume more memory.
func WithL2CacheSize(size int) Option {
	return func(o *imageOptions) {
		if size > 0 {
			o.l2CacheSize = size
		}
	}
}

// WithCompressedCacheSize sets the number of decompressed clusters to cache.
// Compressed clusters must be fully decompressed before reading any part,
// so caching them avoids repeated decompression overhead.
//
// Set to 0 to disable caching (each read decompresses).
// Increase for workloads that repeatedly access the same compressed clusters.
func WithCompressedCacheSize(size int) Option {
	return func(o *imageOptions) {
		if size >= 0 {
			o.compressedCacheSize = size
		}
	}
}

// WithRefcountCacheSize sets the number of refcount block entries to cache.
// Each refcount block is one cluster in size.
// Refcount lookups occur during allocation and deallocation.
//
// Larger values reduce disk I/O during heavy write workloads.
func WithRefcountCacheSize(size int) Option {
	return func(o *imageOptions) {
		if size > 0 {
			o.refcountCacheSize = size
		}
	}
}
