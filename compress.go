package qcow2

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
)

// parseCompressedL2Entry extracts offset and size from a compressed L2 entry.
//
// Compressed L2 entry format (from QCOW2 spec):
//
//	Bit  62:     Always 1 (compression flag)
//	Bits 0..x-1: Host cluster offset (byte-aligned, not 512-byte aligned like normal entries)
//	Bits x..61:  Compressed size minus one, in 512-byte sectors
//
// The value of x depends on cluster size:
//
//	x = 62 - (cluster_bits - 8)
//	  = 62 - cluster_bits + 8
//	  = 70 - cluster_bits
//
// For default 64KB clusters (cluster_bits=16):
//
//	x = 70 - 16 = 54
//	Offset uses bits 0-53 (54 bits = max 16 PB addressable)
//	Size uses bits 54-61 (8 bits = max 256 sectors = 128KB compressed)
//
// For 4KB clusters (cluster_bits=12):
//
//	x = 70 - 12 = 58
//	Offset uses bits 0-57 (58 bits)
//	Size uses bits 58-61 (4 bits = max 16 sectors = 8KB compressed)
func (img *Image) parseCompressedL2Entry(l2Entry uint64) (offset uint64, compressedSize uint64) {
	// x = 70 - cluster_bits (see formula derivation above)
	x := 70 - img.clusterBits

	// Extract offset (bits 0 to x-1)
	offsetMask := (uint64(1) << x) - 1
	offset = l2Entry & offsetMask

	// Extract compressed size (bits x to 61)
	// Size is stored as (sectors - 1), so add 1 and multiply by 512
	sizeBits := (l2Entry >> x) & ((uint64(1) << (62 - x)) - 1)
	compressedSize = (sizeBits + 1) * 512

	return offset, compressedSize
}

// decompressCluster reads and decompresses a compressed cluster.
func (img *Image) decompressCluster(l2Entry uint64) ([]byte, error) {
	// Check for zstd compression (not supported - requires external library)
	if img.header.CompressionType == CompressionZstd {
		return nil, ErrUnsupportedCompression
	}

	offset, compressedSize := img.parseCompressedL2Entry(l2Entry)

	// Read compressed data
	compressed := make([]byte, compressedSize)
	n, err := img.file.ReadAt(compressed, int64(offset))
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("qcow2: failed to read compressed cluster at 0x%x: %w", offset, err)
	}
	compressed = compressed[:n]

	// Decompress using deflate (zlib without header)
	// This is the default compression for QCOW2 (CompressionZlib)
	reader := flate.NewReader(bytes.NewReader(compressed))
	defer reader.Close()

	decompressed := make([]byte, img.clusterSize)
	totalRead := 0
	for totalRead < int(img.clusterSize) {
		n, err := reader.Read(decompressed[totalRead:])
		totalRead += n
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("qcow2: failed to decompress cluster: %w", err)
		}
	}

	// Pad with zeros if decompressed data is smaller than cluster
	// (shouldn't happen with valid data, but be safe)
	for i := totalRead; i < int(img.clusterSize); i++ {
		decompressed[i] = 0
	}

	return decompressed, nil
}

// compressedClusterCache caches decompressed clusters to avoid repeated decompression.
// This is separate from the L2 cache.
type compressedClusterCache struct {
	cache *l2Cache // Reuse the LRU implementation
}

func newCompressedClusterCache(maxSize int, clusterSize int) *compressedClusterCache {
	return &compressedClusterCache{
		cache: newL2Cache(maxSize, clusterSize),
	}
}

func (c *compressedClusterCache) get(offset uint64) []byte {
	return c.cache.get(offset)
}

func (c *compressedClusterCache) put(offset uint64, data []byte) {
	c.cache.put(offset, data)
}
