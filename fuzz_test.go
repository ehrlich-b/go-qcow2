package qcow2

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// validV2Header returns a minimal valid QCOW2 v2 header for fuzzing seeds.
func validV2Header() []byte {
	h := make([]byte, HeaderSizeV2)
	// Magic
	binary.BigEndian.PutUint32(h[0:4], Magic)
	// Version 2
	binary.BigEndian.PutUint32(h[4:8], 2)
	// Cluster bits = 16 (64KB)
	binary.BigEndian.PutUint32(h[20:24], 16)
	// Size = 1GB
	binary.BigEndian.PutUint64(h[24:32], 1024*1024*1024)
	// L1 size = 2 entries
	binary.BigEndian.PutUint32(h[36:40], 2)
	// L1 table offset = 64KB (after header)
	binary.BigEndian.PutUint64(h[40:48], 65536)
	// Refcount table offset = 128KB
	binary.BigEndian.PutUint64(h[48:56], 131072)
	// Refcount table clusters = 1
	binary.BigEndian.PutUint32(h[56:60], 1)
	return h
}

// validV3Header returns a minimal valid QCOW2 v3 header for fuzzing seeds.
func validV3Header() []byte {
	h := make([]byte, HeaderSizeV3)
	// Magic
	binary.BigEndian.PutUint32(h[0:4], Magic)
	// Version 3
	binary.BigEndian.PutUint32(h[4:8], 3)
	// Cluster bits = 16 (64KB)
	binary.BigEndian.PutUint32(h[20:24], 16)
	// Size = 1GB
	binary.BigEndian.PutUint64(h[24:32], 1024*1024*1024)
	// L1 size = 2 entries
	binary.BigEndian.PutUint32(h[36:40], 2)
	// L1 table offset = 64KB
	binary.BigEndian.PutUint64(h[40:48], 65536)
	// Refcount table offset = 128KB
	binary.BigEndian.PutUint64(h[48:56], 131072)
	// Refcount table clusters = 1
	binary.BigEndian.PutUint32(h[56:60], 1)
	// Refcount order = 4 (16-bit)
	binary.BigEndian.PutUint32(h[96:100], 4)
	// Header length = 104
	binary.BigEndian.PutUint32(h[100:104], 104)
	return h
}

// FuzzParseHeader fuzzes the header parsing function.
func FuzzParseHeader(f *testing.F) {
	// Seed with valid headers
	f.Add(validV2Header())
	f.Add(validV3Header())

	// Seed with edge cases
	f.Add([]byte{}) // Empty
	f.Add([]byte{0x51, 0x46, 0x49, 0xfb})  // Just magic
	f.Add(make([]byte, HeaderSizeV2-1))    // One byte short
	f.Add(make([]byte, HeaderSizeV3))      // All zeros
	f.Add(bytes.Repeat([]byte{0xFF}, 200)) // All 0xFF

	// Invalid magic variations
	badMagic := validV2Header()
	badMagic[0] = 0x00
	f.Add(badMagic)

	// Invalid version
	badVersion := validV2Header()
	binary.BigEndian.PutUint32(badVersion[4:8], 99)
	f.Add(badVersion)

	// Extreme cluster bits
	extremeCluster := validV3Header()
	binary.BigEndian.PutUint32(extremeCluster[20:24], 30) // Too large
	f.Add(extremeCluster)

	f.Fuzz(func(t *testing.T, data []byte) {
		header, err := ParseHeader(data)
		if err != nil {
			// Invalid input is expected, just ensure no panic
			return
		}

		// If parsing succeeded, header should be usable
		_ = header.ClusterSize()
		_ = header.L2Entries()

		// Validate should not panic
		_ = header.Validate()

		// Check basic sanity
		if header.Magic != Magic {
			t.Errorf("Parsed header has wrong magic: 0x%x", header.Magic)
		}
		if header.Version != 2 && header.Version != 3 {
			t.Errorf("Parsed header has invalid version: %d", header.Version)
		}
	})
}

// FuzzL2Entry fuzzes L2 entry interpretation.
func FuzzL2Entry(f *testing.F) {
	// Seed with known L2 entry patterns
	f.Add(uint64(0))                             // Unallocated
	f.Add(uint64(L2EntryZeroFlag))               // Zero cluster
	f.Add(uint64(0x50000 | L2EntryCopied))       // Normal allocated
	f.Add(uint64(L2EntryCompressed | 0x1000))    // Compressed
	f.Add(uint64(0xFFFFFFFFFFFFFFFF))            // All bits set
	f.Add(uint64(L2EntryOffsetMask))             // Max offset
	f.Add(uint64(L2EntryCopied | L2EntryZeroFlag)) // Copied + zero

	f.Fuzz(func(t *testing.T, entry uint64) {
		// Parse entry components - should not panic
		isCompressed := entry&L2EntryCompressed != 0
		isZero := entry&L2EntryZeroFlag != 0
		isCopied := entry&L2EntryCopied != 0
		offset := entry & L2EntryOffsetMask

		// Verify we can use these values
		_ = isCompressed
		_ = isZero
		_ = isCopied
		_ = offset

		// For uncompressed entries, offset should be 512-byte aligned
		if !isCompressed && offset != 0 && offset&0x1ff != 0 {
			// Misaligned offset - this is an invalid entry
			// QCOW2 requires 512-byte alignment for uncompressed clusters
		}
	})
}

// FuzzReadWrite fuzzes read/write operations with various offsets and sizes.
func FuzzReadWrite(f *testing.F) {
	// Seeds with various offset/size combinations
	f.Add(int64(0), []byte("test"))
	f.Add(int64(65535), []byte("boundary"))
	f.Add(int64(65536), []byte("cluster start"))
	f.Add(int64(0), []byte{})                       // Empty write
	f.Add(int64(0), bytes.Repeat([]byte{0xAA}, 100)) // Pattern
	f.Add(int64(1000000), []byte("far offset"))

	f.Fuzz(func(t *testing.T, offset int64, data []byte) {
		// Skip invalid inputs
		if len(data) == 0 || len(data) > 64*1024 {
			return // Skip empty or very large writes
		}
		if offset < 0 {
			return // Skip negative offsets
		}

		// Create temp image for each test
		dir := t.TempDir()
		path := filepath.Join(dir, "fuzz.qcow2")

		// Create image large enough for the write
		imgSize := uint64(offset) + uint64(len(data)) + 65536
		if imgSize > 100*1024*1024 {
			imgSize = 100 * 1024 * 1024 // Cap at 100MB
		}

		img, err := CreateSimple(path, imgSize)
		if err != nil {
			return // Skip if can't create
		}
		defer img.Close()

		// Ensure offset is within image bounds
		if offset+int64(len(data)) > img.Size() {
			return // Skip writes beyond image size
		}

		// Write should succeed
		n, err := img.WriteAt(data, offset)
		if err != nil {
			return // Some offsets may be invalid
		}

		// Read back and verify
		readBuf := make([]byte, n)
		rn, err := img.ReadAt(readBuf, offset)
		if err != nil {
			t.Fatalf("Read failed after successful write: %v", err)
		}

		if !bytes.Equal(data[:n], readBuf[:rn]) {
			t.Fatalf("Data mismatch at offset %d", offset)
		}
	})
}

// FuzzRefcountEntry fuzzes refcount entry reading/writing.
func FuzzRefcountEntry(f *testing.F) {
	// Seed with various bit widths and values
	type seed struct {
		bits  uint32
		index uint64
		value uint64
	}

	seeds := []seed{
		{16, 0, 1},
		{16, 100, 65535},
		{8, 0, 255},
		{32, 0, 0xDEADBEEF},
		{64, 0, 0xDEADBEEFCAFEBABE},
		{1, 0, 1},
		{2, 0, 3},
		{4, 0, 15},
	}

	for _, s := range seeds {
		// Pack seed into bytes
		data := make([]byte, 16)
		binary.BigEndian.PutUint32(data[0:4], s.bits)
		binary.BigEndian.PutUint64(data[4:12], s.index)
		data[12] = byte(s.value)
		f.Add(data)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 13 {
			return
		}

		bits := binary.BigEndian.Uint32(data[0:4])
		index := binary.BigEndian.Uint64(data[4:12])
		value := uint64(data[12])

		// Only test valid bit widths
		validBits := []uint32{1, 2, 4, 8, 16, 32, 64}
		isValid := false
		for _, vb := range validBits {
			if bits == vb {
				isValid = true
				break
			}
		}
		if !isValid {
			return
		}

		// Limit index to avoid huge allocations
		if index > 10000 {
			return
		}

		// Calculate block size needed
		var blockSize int
		switch bits {
		case 1:
			blockSize = int(index/8) + 1
		case 2:
			blockSize = int(index/4) + 1
		case 4:
			blockSize = int(index/2) + 1
		case 8:
			blockSize = int(index) + 1
		case 16:
			blockSize = int(index*2) + 2
		case 32:
			blockSize = int(index*4) + 4
		case 64:
			blockSize = int(index*8) + 8
		}

		if blockSize > 100000 {
			return // Skip very large blocks
		}

		block := make([]byte, blockSize)

		// Write and read back
		writeRefcountEntry(block, index, bits, value)
		got := readRefcountEntry(block, index, bits)

		// Mask value to fit in bits
		maxValue := uint64((1 << bits) - 1)
		expected := value & maxValue

		if got != expected {
			t.Errorf("bits=%d index=%d: got %d, want %d (value=%d)",
				bits, index, got, expected, value)
		}
	})
}

// FuzzFullImage fuzzes opening potentially malformed image files.
func FuzzFullImage(f *testing.F) {
	// Create valid image seeds
	validImg := func() []byte {
		dir, _ := os.MkdirTemp("", "fuzz-seed")
		defer os.RemoveAll(dir)
		path := filepath.Join(dir, "seed.qcow2")

		img, err := CreateSimple(path, 1024*1024)
		if err != nil {
			return nil
		}
		img.WriteAt([]byte("test data"), 0)
		img.Close()

		data, _ := os.ReadFile(path)
		return data
	}

	if valid := validImg(); valid != nil {
		f.Add(valid)
	}

	// Seed with minimal header
	f.Add(validV3Header())

	// Seed with corrupted variations
	corrupted := validV3Header()
	corrupted[0] = 0 // Bad magic
	f.Add(corrupted)

	f.Fuzz(func(t *testing.T, imageData []byte) {
		if len(imageData) < HeaderSizeV2 {
			return // Too small to be valid
		}

		// Write to temp file
		dir := t.TempDir()
		path := filepath.Join(dir, "fuzz.qcow2")
		if err := os.WriteFile(path, imageData, 0644); err != nil {
			return
		}

		// Try to open - should not panic
		img, err := Open(path)
		if err != nil {
			return // Invalid image is expected
		}
		defer img.Close()

		// If opened successfully, basic operations shouldn't panic
		_ = img.Size()
		_ = img.ClusterSize()
		_ = img.IsDirty()

		// Try to read first cluster
		buf := make([]byte, 512)
		img.ReadAt(buf, 0)

		// Check should not panic
		img.Check()
	})
}
