package qcow2

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
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
	offset, compressedSize := img.parseCompressedL2Entry(l2Entry)

	// Read compressed data (use dataFile for external data file support)
	compressed := make([]byte, compressedSize)
	n, err := img.dataFile().ReadAt(compressed, int64(offset))
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("qcow2: failed to read compressed cluster at 0x%x: %w", offset, err)
	}
	compressed = compressed[:n]

	// Decompress based on compression type
	switch img.header.CompressionType {
	case CompressionZstd:
		return img.decompressZstd(compressed)
	default:
		// Default is deflate (zlib without header)
		return img.decompressDeflate(compressed)
	}
}

// decompressDeflate decompresses data using deflate (the default QCOW2 compression).
func (img *Image) decompressDeflate(compressed []byte) ([]byte, error) {
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
			return nil, fmt.Errorf("qcow2: failed to decompress deflate cluster: %w", err)
		}
	}

	// Pad with zeros if decompressed data is smaller than cluster
	for i := totalRead; i < int(img.clusterSize); i++ {
		decompressed[i] = 0
	}

	return decompressed, nil
}

// decompressZstd decompresses data using zstd compression.
// Uses a streaming decoder to handle padded data correctly - the decoder
// will stop at the zstd frame boundary and ignore trailing padding bytes.
func (img *Image) decompressZstd(compressed []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("qcow2: failed to create zstd decoder: %w", err)
	}
	defer decoder.Close()

	decompressed := make([]byte, img.clusterSize)
	totalRead := 0
	for totalRead < int(img.clusterSize) {
		n, err := decoder.Read(decompressed[totalRead:])
		totalRead += n
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("qcow2: failed to decompress zstd cluster: %w", err)
		}
	}

	// Zero-pad if decompressed data is smaller than cluster
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

// CompressionLevel controls the compression level for writes.
type CompressionLevel int

const (
	// CompressionDisabled disables compression (default).
	CompressionDisabled CompressionLevel = iota
	// CompressionFast uses fast compression (level 1).
	CompressionFast
	// CompressionDefault uses default compression (level 6).
	CompressionDefault
	// CompressionBest uses best compression (level 9).
	CompressionBest
)

// toFlateLevel converts CompressionLevel to flate compression level.
func (c CompressionLevel) toFlateLevel() int {
	switch c {
	case CompressionFast:
		return flate.BestSpeed
	case CompressionDefault:
		return flate.DefaultCompression
	case CompressionBest:
		return flate.BestCompression
	default:
		return flate.DefaultCompression
	}
}

// compressCluster compresses cluster data using deflate.
// Returns the compressed data or an error. If the compressed data is
// larger than the original, returns ErrCompressionNotBeneficial.
func (img *Image) compressCluster(data []byte) ([]byte, error) {
	if len(data) != int(img.clusterSize) {
		return nil, fmt.Errorf("qcow2: compress requires full cluster (%d bytes), got %d",
			img.clusterSize, len(data))
	}

	var compressed []byte
	var err error

	// Use zstd if explicitly set, otherwise default to deflate
	if img.compressionType == CompressionZstd {
		compressed, err = img.compressZstd(data)
	} else {
		compressed, err = img.compressDeflate(data)
	}
	if err != nil {
		return nil, err
	}

	// Check if compression is beneficial (must save at least one sector)
	// Round up to 512-byte sectors
	compressedSectors := (len(compressed) + 511) / 512
	originalSectors := int(img.clusterSize) / 512
	if compressedSectors >= originalSectors {
		return nil, ErrCompressionNotBeneficial
	}

	return compressed, nil
}

// compressDeflate compresses data using deflate.
func (img *Image) compressDeflate(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, img.compressionLevel.toFlateLevel())
	if err != nil {
		return nil, fmt.Errorf("qcow2: failed to create deflate compressor: %w", err)
	}

	if _, err := w.Write(data); err != nil {
		return nil, fmt.Errorf("qcow2: failed to compress data with deflate: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("qcow2: failed to finish deflate compression: %w", err)
	}

	return buf.Bytes(), nil
}

// compressZstd compresses data using zstd.
func (img *Image) compressZstd(data []byte) ([]byte, error) {
	level := zstd.SpeedDefault
	switch img.compressionLevel {
	case CompressionFast:
		level = zstd.SpeedFastest
	case CompressionBest:
		level = zstd.SpeedBestCompression
	}

	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
	if err != nil {
		return nil, fmt.Errorf("qcow2: failed to create zstd encoder: %w", err)
	}
	defer encoder.Close()

	return encoder.EncodeAll(data, nil), nil
}

// updateHeaderCompressionType updates the compression type in the header and persists it.
// This is needed when writing compressed clusters with a non-default compression type.
func (img *Image) updateHeaderCompressionType(ctype uint8) error {
	// Update in-memory header
	img.header.CompressionType = ctype

	// For v3 headers, compression type is at byte 104
	if img.header.Version >= 3 {
		buf := []byte{ctype}
		if _, err := img.file.WriteAt(buf, 104); err != nil {
			return fmt.Errorf("qcow2: failed to write compression type to header: %w", err)
		}

		// Also need to set the compression type feature bit in incompatible features
		// Bit 3 indicates compression type is present in header
		if ctype != CompressionZlib {
			img.header.IncompatibleFeatures |= IncompatCompression
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, img.header.IncompatibleFeatures)
			if _, err := img.file.WriteAt(buf, 72); err != nil {
				return fmt.Errorf("qcow2: failed to write incompatible features: %w", err)
			}
		}
	}

	return nil
}

// buildCompressedL2Entry creates an L2 entry for a compressed cluster.
func (img *Image) buildCompressedL2Entry(offset uint64, compressedSize int) uint64 {
	// Calculate x: the bit position separating offset from size (70 minus cluster_bits)
	x := 70 - img.clusterBits

	// Size is stored as (sectors - 1)
	sectors := uint64((compressedSize + 511) / 512)
	sizeBits := sectors - 1

	// Build entry: compression flag | (sizeBits << x) | offset
	entry := L2EntryCompressed | (sizeBits << x) | offset

	return entry
}

// allocateCompressedSpace allocates space for compressed data at file end.
// Unlike regular clusters, compressed data is byte-aligned (not cluster-aligned).
// Returns the offset where the compressed data should be written.
func (img *Image) allocateCompressedSpace(size int) (uint64, error) {
	// Use dataFile for external data file support
	dataFile := img.dataFile()
	info, err := dataFile.Stat()
	if err != nil {
		return 0, fmt.Errorf("qcow2: failed to stat file for compressed allocation: %w", err)
	}

	// Compressed clusters are written at file end, no alignment required
	offset := uint64(info.Size())

	// Extend file
	if err := dataFile.Truncate(info.Size() + int64(size)); err != nil {
		return 0, fmt.Errorf("qcow2: failed to extend file for compressed data: %w", err)
	}

	return offset, nil
}

// writeCompressedCluster compresses and writes a full cluster of data.
// The cluster must be complete (partial cluster writes cannot be compressed).
// If compression is not beneficial, falls back to normal uncompressed write.
// Returns the L2 entry and any error.
func (img *Image) writeCompressedCluster(virtOff uint64, data []byte) (uint64, error) {
	// Serialize with write operations to prevent races
	img.writeMu.Lock()
	defer img.writeMu.Unlock()

	if len(data) != int(img.clusterSize) {
		return 0, fmt.Errorf("qcow2: compressed write requires full cluster")
	}

	// Try to compress
	compressed, err := img.compressCluster(data)
	if err == ErrCompressionNotBeneficial {
		// Fall back to normal allocation
		return 0, ErrCompressionNotBeneficial
	}
	if err != nil {
		return 0, err
	}

	// Update header compression type if needed (for zstd support)
	if img.compressionType != img.header.CompressionType {
		if err := img.updateHeaderCompressionType(img.compressionType); err != nil {
			return 0, err
		}
	}

	// Round up to 512-byte sector boundary for L2 entry encoding
	paddedSize := ((len(compressed) + 511) / 512) * 512

	// Allocate space for compressed data
	offset, err := img.allocateCompressedSpace(paddedSize)
	if err != nil {
		return 0, err
	}

	// Write compressed data (padded) - use dataFile for external data file support
	padded := make([]byte, paddedSize)
	copy(padded, compressed)
	if _, err := img.dataFile().WriteAt(padded, int64(offset)); err != nil {
		return 0, fmt.Errorf("qcow2: failed to write compressed cluster: %w", err)
	}

	// Build L2 entry
	l2Entry := img.buildCompressedL2Entry(offset, paddedSize)

	// Update L2 table
	if err := img.updateL2EntryForCompressed(virtOff, l2Entry); err != nil {
		return 0, err
	}

	// Sync if needed
	if err := img.metadataBarrier(); err != nil {
		return 0, fmt.Errorf("qcow2: metadata sync failed: %w", err)
	}

	return l2Entry, nil
}

// WriteAtCompressed writes a full cluster of data with compression.
// The offset must be cluster-aligned and data must be exactly one cluster.
// If compression is not beneficial, falls back to normal uncompressed write.
// Returns the number of bytes written (always clusterSize on success) and any error.
func (img *Image) WriteAtCompressed(data []byte, off int64) (int, error) {
	if img.readOnly {
		return 0, ErrReadOnly
	}

	// Extended L2 images are read-only for now
	if img.extendedL2 {
		return 0, fmt.Errorf("qcow2: writing to extended L2 images (subcluster allocation) is not yet supported")
	}

	if off < 0 {
		return 0, ErrOffsetOutOfRange
	}

	// Must be cluster-aligned
	if uint64(off)&img.offsetMask != 0 {
		return 0, fmt.Errorf("qcow2: compressed write offset must be cluster-aligned")
	}

	// Must be exactly one cluster
	if len(data) != int(img.clusterSize) {
		return 0, fmt.Errorf("qcow2: compressed write must be exactly one cluster (%d bytes)", img.clusterSize)
	}

	// Check bounds
	if off >= img.Size() {
		return 0, ErrOffsetOutOfRange
	}

	// Invalidate any persistent bitmaps on first write
	if !img.bitmapsInvalidated && img.hasBitmaps() {
		if err := img.invalidateBitmaps(); err != nil {
			return 0, fmt.Errorf("qcow2: failed to invalidate bitmaps: %w", err)
		}
		img.bitmapsInvalidated = true
	}

	// Try compressed write
	_, err := img.writeCompressedCluster(uint64(off), data)
	if err == ErrCompressionNotBeneficial {
		// Fall back to normal write
		return img.WriteAt(data, off)
	}
	if err != nil {
		return 0, err
	}

	img.dirty.Store(true)
	return len(data), nil
}

// updateL2EntryForCompressed updates the L2 table entry for a compressed cluster.
func (img *Image) updateL2EntryForCompressed(virtOff uint64, l2Entry uint64) error {
	l2Index := (virtOff >> img.clusterBits) & (img.l2Entries - 1)
	l1Index := virtOff >> (img.clusterBits + img.l2Bits)

	// Get or allocate L2 table
	l2TableOff, err := img.getOrAllocateL2Table(l1Index)
	if err != nil {
		return err
	}

	// Get L2 table
	l2Table, err := img.getL2Table(l2TableOff)
	if err != nil {
		return err
	}

	// Update L2 entry
	binary.BigEndian.PutUint64(l2Table[l2Index*8:], l2Entry)

	// Write updated entry to disk
	if _, err := img.file.WriteAt(l2Table[l2Index*8:l2Index*8+8],
		int64(l2TableOff+l2Index*8)); err != nil {
		return fmt.Errorf("qcow2: failed to write compressed L2 entry: %w", err)
	}

	// Update cache
	img.l2Cache.put(l2TableOff, l2Table)

	return nil
}
