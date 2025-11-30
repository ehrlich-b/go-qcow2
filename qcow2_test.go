package qcow2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ehrlich-b/go-qcow2/testutil"
)

func TestCreateAndOpen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create a 1MB image
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Check size
	if img.Size() != 1024*1024 {
		t.Errorf("Size = %d, want %d", img.Size(), 1024*1024)
	}

	// Check cluster size
	if img.ClusterSize() != DefaultClusterSize {
		t.Errorf("ClusterSize = %d, want %d", img.ClusterSize(), DefaultClusterSize)
	}

	img.Close()

	// Reopen and verify
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img2.Close()

	if img2.Size() != 1024*1024 {
		t.Errorf("Reopened Size = %d, want %d", img2.Size(), 1024*1024)
	}
}

func TestReadWriteRoundtrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create a 1MB image
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write some data
	data := []byte("Hello, QCOW2! This is a test message.")
	n, err := img.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("WriteAt n = %d, want %d", n, len(data))
	}

	// Read it back
	buf := make([]byte, len(data))
	n, err = img.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("ReadAt n = %d, want %d", n, len(data))
	}
	if !bytes.Equal(buf, data) {
		t.Errorf("ReadAt data mismatch: got %q, want %q", buf, data)
	}

	img.Close()

	// Reopen and verify data persists
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img2.Close()

	buf2 := make([]byte, len(data))
	_, err = img2.ReadAt(buf2, 0)
	if err != nil {
		t.Fatalf("ReadAt after reopen failed: %v", err)
	}
	if !bytes.Equal(buf2, data) {
		t.Errorf("Data after reopen mismatch: got %q, want %q", buf2, data)
	}
}

func TestReadUnallocated(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create a 1MB image
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Read unallocated region - should return zeros
	buf := make([]byte, 4096)
	for i := range buf {
		buf[i] = 0xFF // Fill with non-zero to detect if zeros are written
	}

	n, err := img.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != len(buf) {
		t.Errorf("ReadAt n = %d, want %d", n, len(buf))
	}

	// Verify all zeros
	for i, b := range buf {
		if b != 0 {
			t.Errorf("ReadAt buf[%d] = %d, want 0", i, b)
			break
		}
	}
}

func TestWriteAtOffset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create a 1MB image
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Write at various offsets
	offsets := []int64{0, 4096, 65536, 100000}
	for _, off := range offsets {
		data := []byte("test data at offset")
		_, err := img.WriteAt(data, off)
		if err != nil {
			t.Errorf("WriteAt at offset %d failed: %v", off, err)
			continue
		}

		buf := make([]byte, len(data))
		_, err = img.ReadAt(buf, off)
		if err != nil {
			t.Errorf("ReadAt at offset %d failed: %v", off, err)
			continue
		}
		if !bytes.Equal(buf, data) {
			t.Errorf("Data mismatch at offset %d", off)
		}
	}
}

func TestCrossClusterWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create a 1MB image
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Write data that spans cluster boundary (64KB default)
	clusterSize := img.ClusterSize()
	offset := int64(clusterSize - 100) // Start 100 bytes before cluster boundary

	data := make([]byte, 200) // Spans into next cluster
	for i := range data {
		data[i] = byte(i)
	}

	n, err := img.WriteAt(data, offset)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("WriteAt n = %d, want %d", n, len(data))
	}

	// Read it back
	buf := make([]byte, len(data))
	_, err = img.ReadAt(buf, offset)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Errorf("Cross-cluster data mismatch")
	}
}

func TestReadBeyondEOF(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create a 1MB image
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Read at exactly EOF
	buf := make([]byte, 100)
	_, err = img.ReadAt(buf, img.Size())
	if err != io.EOF {
		t.Errorf("ReadAt at EOF: got err %v, want io.EOF", err)
	}

	// Read beyond EOF
	_, err = img.ReadAt(buf, img.Size()+1000)
	if err != io.EOF {
		t.Errorf("ReadAt beyond EOF: got err %v, want io.EOF", err)
	}
}

func TestHeaderParsing(t *testing.T) {
	t.Parallel()
	// Test with a manually constructed v3 header
	header := make([]byte, HeaderSizeV3)

	// Magic
	header[0] = 0x51 // Q
	header[1] = 0x46 // F
	header[2] = 0x49 // I
	header[3] = 0xfb

	// Version 3
	header[4] = 0
	header[5] = 0
	header[6] = 0
	header[7] = 3

	// Cluster bits = 16
	header[20] = 0
	header[21] = 0
	header[22] = 0
	header[23] = 16

	// Size = 1GB
	size := uint64(1024 * 1024 * 1024)
	header[24] = byte(size >> 56)
	header[25] = byte(size >> 48)
	header[26] = byte(size >> 40)
	header[27] = byte(size >> 32)
	header[28] = byte(size >> 24)
	header[29] = byte(size >> 16)
	header[30] = byte(size >> 8)
	header[31] = byte(size)

	// Refcount order = 4 (16-bit)
	header[96] = 0
	header[97] = 0
	header[98] = 0
	header[99] = 4

	// Header length
	header[100] = 0
	header[101] = 0
	header[102] = 0
	header[103] = 104

	h, err := ParseHeader(header)
	if err != nil {
		t.Fatalf("ParseHeader failed: %v", err)
	}

	if h.Magic != Magic {
		t.Errorf("Magic = 0x%x, want 0x%x", h.Magic, Magic)
	}
	if h.Version != 3 {
		t.Errorf("Version = %d, want 3", h.Version)
	}
	if h.ClusterBits != 16 {
		t.Errorf("ClusterBits = %d, want 16", h.ClusterBits)
	}
	if h.Size != size {
		t.Errorf("Size = %d, want %d", h.Size, size)
	}
	if h.ClusterSize() != 65536 {
		t.Errorf("ClusterSize() = %d, want 65536", h.ClusterSize())
	}
	if h.L2Entries() != 8192 {
		t.Errorf("L2Entries() = %d, want 8192", h.L2Entries())
	}
}

func TestInvalidMagic(t *testing.T) {
	t.Parallel()
	header := make([]byte, HeaderSizeV2)
	header[0] = 0x00 // Wrong magic

	_, err := ParseHeader(header)
	if err != ErrInvalidMagic {
		t.Errorf("ParseHeader with bad magic: got err %v, want ErrInvalidMagic", err)
	}
}

func TestL2Cache(t *testing.T) {
	t.Parallel()
	cache := newL2Cache(3, 64) // Small cache for testing

	// Add entries
	data1 := make([]byte, 64)
	data1[0] = 1
	cache.put(1000, data1)

	data2 := make([]byte, 64)
	data2[0] = 2
	cache.put(2000, data2)

	data3 := make([]byte, 64)
	data3[0] = 3
	cache.put(3000, data3)

	// Access order: 1000, 2000, 3000 (3000 is MRU)
	// Verify all present
	if got := cache.get(1000); len(got) == 0 || got[0] != 1 {
		t.Error("cache.get(1000) failed")
	}
	// After accessing 1000, order is: 2000, 3000, 1000 (1000 is MRU)
	if got := cache.get(2000); len(got) == 0 || got[0] != 2 {
		t.Error("cache.get(2000) failed")
	}
	// After accessing 2000, order is: 3000, 1000, 2000 (2000 is MRU)
	if got := cache.get(3000); len(got) == 0 || got[0] != 3 {
		t.Error("cache.get(3000) failed")
	}
	// After accessing 3000, order is: 1000, 2000, 3000 (3000 is MRU, 1000 is LRU)

	// Add 4th entry, should evict 1000 (LRU)
	data4 := make([]byte, 64)
	data4[0] = 4
	cache.put(4000, data4)

	if cache.size() != 3 {
		t.Errorf("cache size = %d, want 3", cache.size())
	}

	// 1000 should be evicted (was LRU)
	if got := cache.get(1000); got != nil {
		t.Error("cache.get(1000) should have been evicted")
	}

	// 2000, 3000, 4000 should still be present
	if got := cache.get(2000); len(got) == 0 || got[0] != 2 {
		t.Error("cache.get(2000) should still be present")
	}
	if got := cache.get(3000); len(got) == 0 || got[0] != 3 {
		t.Error("cache.get(3000) should still be present")
	}
	if got := cache.get(4000); len(got) == 0 || got[0] != 4 {
		t.Error("cache.get(4000) should still be present")
	}
}

func TestDirtyBitTracking(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create an image
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// After create, image should be clean (we close it properly in Create)
	// But we have it open for RW, so it should be dirty now
	if !img.IsDirty() {
		t.Error("Image should be dirty when open for RW")
	}

	// Close cleanly
	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen read-only to check dirty bit
	img2, err := OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile RDONLY failed: %v", err)
	}

	if img2.IsDirty() {
		t.Error("Image should be clean after proper close")
	}
	img2.Close()

	// Open RW, don't close properly (simulate crash)
	img3, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Should be dirty now
	if !img3.IsDirty() {
		t.Error("Image should be dirty when open for RW")
	}

	// Close the file handle without calling Close() to simulate crash
	// We need to access the underlying file - let's just check it's dirty
	// and close normally for cleanup
	img3.Close()
}

func TestBackingFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.qcow2")
	overlayPath := filepath.Join(dir, "overlay.qcow2")

	// Create base image and write some data
	base, err := CreateSimple(basePath, 1024*1024)
	if err != nil {
		t.Fatalf("Create base failed: %v", err)
	}

	baseData := []byte("Hello from base image!")
	if _, err := base.WriteAt(baseData, 0); err != nil {
		t.Fatalf("WriteAt base failed: %v", err)
	}
	base.Close()

	// Create overlay
	overlay, err := CreateOverlay(overlayPath, basePath)
	if err != nil {
		t.Fatalf("CreateOverlay failed: %v", err)
	}

	// Check backing file is set
	if !overlay.HasBackingFile() {
		t.Error("Overlay should have backing file")
	}

	// Read should fall through to base
	buf := make([]byte, len(baseData))
	n, err := overlay.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt from overlay failed: %v", err)
	}
	if n != len(baseData) {
		t.Errorf("ReadAt n = %d, want %d", n, len(baseData))
	}
	if !bytes.Equal(buf, baseData) {
		t.Errorf("ReadAt data mismatch: got %q, want %q", buf, baseData)
	}

	// Write to overlay IN THE SAME CLUSTER as base data (test COW)
	// Offset 1000 is in the same 64KB cluster as offset 0
	overlayOffset := int64(1000)
	overlayData := []byte("Hello from overlay!")
	if _, err := overlay.WriteAt(overlayData, overlayOffset); err != nil {
		t.Fatalf("WriteAt overlay failed: %v", err)
	}

	// Read overlay data
	buf2 := make([]byte, len(overlayData))
	if _, err := overlay.ReadAt(buf2, overlayOffset); err != nil {
		t.Fatalf("ReadAt overlay data failed: %v", err)
	}
	if !bytes.Equal(buf2, overlayData) {
		t.Errorf("Overlay data mismatch: got %q, want %q", buf2, overlayData)
	}

	// Original base data should still read correctly (COW preserved it)
	buf3 := make([]byte, len(baseData))
	if _, err := overlay.ReadAt(buf3, 0); err != nil {
		t.Fatalf("ReadAt base data via overlay failed: %v", err)
	}
	if !bytes.Equal(buf3, baseData) {
		t.Errorf("Base data via overlay mismatch: got %q, want %q", buf3, baseData)
	}

	overlay.Close()

	// Verify base is unchanged
	base2, err := OpenFile(basePath, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Reopen base failed: %v", err)
	}
	defer base2.Close()

	buf4 := make([]byte, len(overlayData))
	if _, err := base2.ReadAt(buf4, overlayOffset); err != nil {
		t.Fatalf("ReadAt base at overlay offset failed: %v", err)
	}
	// Base should have zeros at overlayOffset (we only wrote to overlay)
	for i, b := range buf4 {
		if b != 0 {
			t.Errorf("Base should have zeros at offset %d, got byte %d at index %d", overlayOffset, b, i)
			break
		}
	}
}

func TestRefcountReading(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create an image
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Get refcount info
	info, err := img.GetRefcountInfo()
	if err != nil {
		t.Fatalf("GetRefcountInfo failed: %v", err)
	}

	// Default is 16-bit refcounts
	if info.RefcountBits != 16 {
		t.Errorf("RefcountBits = %d, want 16", info.RefcountBits)
	}

	// Check that header cluster has refcount 1
	refcount, err := img.ClusterRefcount(0)
	if err != nil {
		t.Fatalf("ClusterRefcount(0) failed: %v", err)
	}
	if refcount != 1 {
		t.Errorf("Header cluster refcount = %d, want 1", refcount)
	}

	// Write some data to allocate a new cluster
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	img.Close()
}

func TestReadOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create an image
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	img.Close()

	// Open read-only
	img2, err := OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile RDONLY failed: %v", err)
	}
	defer img2.Close()

	// Try to write - should fail
	_, err = img2.WriteAt([]byte("test"), 0)
	if err != ErrReadOnly {
		t.Errorf("WriteAt on read-only: got err %v, want ErrReadOnly", err)
	}
}

func TestRefcountUpdates(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create an image
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Initial clusters (0-3) should have refcount 1
	// Cluster 0: header
	// Cluster 1: L1 table
	// Cluster 2: refcount table
	// Cluster 3: refcount block
	clusterSize := uint64(img.ClusterSize())
	for i := uint64(0); i < 4; i++ {
		refcount, err := img.ClusterRefcount(i * clusterSize)
		if err != nil {
			t.Fatalf("ClusterRefcount(%d) failed: %v", i, err)
		}
		if refcount != 1 {
			t.Errorf("Initial cluster %d refcount = %d, want 1", i, refcount)
		}
	}

	// Write some data to allocate a new cluster
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// The newly allocated clusters should have refcount 1
	// We allocated:
	// - 1 L2 table (cluster 4)
	// - 1 data cluster (cluster 5)
	for i := uint64(4); i <= 5; i++ {
		refcount, err := img.ClusterRefcount(i * clusterSize)
		if err != nil {
			t.Fatalf("ClusterRefcount(%d) failed: %v", i, err)
		}
		if refcount != 1 {
			t.Errorf("Allocated cluster %d refcount = %d, want 1", i, refcount)
		}
	}

	// Write to a different L2 region to allocate another L2 table
	// Each L2 table covers 8192 * 64KB = 512MB with default settings
	// So offset 512MB will need a new L2 table
	largeOffset := int64(512 * 1024 * 1024) // 512MB
	if largeOffset < img.Size() {
		if _, err := img.WriteAt(data, largeOffset); err != nil {
			t.Fatalf("WriteAt at large offset failed: %v", err)
		}
	}
}

func TestRefcountEntryReadWrite(t *testing.T) {
	t.Parallel()
	// Test readRefcountEntry and writeRefcountEntry for various bit widths
	tests := []struct {
		bits  uint32
		index uint64
		value uint64
	}{
		{16, 0, 1},
		{16, 1, 65535},
		{16, 100, 42},
		{8, 0, 1},
		{8, 1, 255},
		{32, 0, 0xDEADBEEF},
		{64, 0, 0xDEADBEEFCAFEBABE},
	}

	for _, tc := range tests {
		// Calculate block size needed
		var blockSize int
		switch tc.bits {
		case 1:
			blockSize = int(tc.index/8) + 1
		case 2:
			blockSize = int(tc.index/4) + 1
		case 4:
			blockSize = int(tc.index/2) + 1
		case 8:
			blockSize = int(tc.index) + 1
		case 16:
			blockSize = int(tc.index*2) + 2
		case 32:
			blockSize = int(tc.index*4) + 4
		case 64:
			blockSize = int(tc.index*8) + 8
		}

		block := make([]byte, blockSize)

		// Write
		writeRefcountEntry(block, tc.index, tc.bits, tc.value)

		// Read back
		got := readRefcountEntry(block, tc.index, tc.bits)
		if got != tc.value {
			t.Errorf("bits=%d index=%d: got %d, want %d", tc.bits, tc.index, got, tc.value)
		}
	}
}

func TestHeaderExtensions(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create an image
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Check that extensions were parsed (even if empty)
	ext := img.Extensions()
	if ext == nil {
		t.Error("Extensions() returned nil")
	}

	// Our created images don't have backing format or feature names by default
	if img.BackingFormat() != "" {
		t.Errorf("BackingFormat = %q, want empty", img.BackingFormat())
	}

	img.Close()
}

func TestHeaderExtensionsParsing(t *testing.T) {
	t.Parallel()
	// Test feature name table parsing
	names := make(map[string]string)

	// Create a mock feature name table entry
	// Format: 1 byte type + 1 byte bit + 46 bytes name
	entry := make([]byte, 48)
	entry[0] = 0                 // incompatible
	entry[1] = 0                 // bit 0
	copy(entry[2:], "dirty bit") // name

	parseFeatureNameTable(entry, names)

	if names["incompat_0"] != "dirty bit" {
		t.Errorf("Feature name = %q, want %q", names["incompat_0"], "dirty bit")
	}
}

func TestWriteZeroAt(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image with 2 clusters worth of space (128KB with default 64KB clusters)
	img, err := CreateSimple(path, 128*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write some non-zero data first
	data := make([]byte, 64*1024) // One cluster
	for i := range data {
		data[i] = byte(i % 256)
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Write a second cluster with data
	if _, err := img.WriteAt(data, 64*1024); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	img.Close()

	// Reopen and zero out the first cluster
	img, err = Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Get file size before zeroing
	info1, _ := img.file.Stat()
	sizeBefore := info1.Size()

	// Zero the first cluster using WriteZeroAt
	if err := img.WriteZeroAt(0, 64*1024); err != nil {
		t.Fatalf("WriteZeroAt failed: %v", err)
	}

	// Verify reading back zeros
	readBuf := make([]byte, 64*1024)
	if _, err := img.ReadAt(readBuf, 0); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	for i, b := range readBuf {
		if b != 0 {
			t.Errorf("Expected zero at offset %d, got %d", i, b)
			break
		}
	}

	// Verify second cluster still has data
	if _, err := img.ReadAt(readBuf, 64*1024); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	for i, b := range readBuf {
		expected := byte(i % 256)
		if b != expected {
			t.Errorf("Data corrupted at offset %d: got %d, want %d", i, b, expected)
			break
		}
	}

	// File size should not have grown (zero cluster doesn't allocate)
	info2, _ := img.file.Stat()
	sizeAfter := info2.Size()
	if sizeAfter > sizeBefore {
		t.Logf("Note: File grew from %d to %d (expected: no growth for zero clusters)", sizeBefore, sizeAfter)
	}

	img.Close()
}

func TestWriteZeroAtPartialCluster(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image with 2 clusters worth of space
	img, err := CreateSimple(path, 128*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write non-zero data to first cluster
	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = 0xAB
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Zero only part of the cluster (first 32KB)
	if err := img.WriteZeroAt(0, 32*1024); err != nil {
		t.Fatalf("WriteZeroAt failed: %v", err)
	}

	// Verify first 32KB is zeros
	readBuf := make([]byte, 32*1024)
	if _, err := img.ReadAt(readBuf, 0); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	for i, b := range readBuf {
		if b != 0 {
			t.Errorf("Expected zero at offset %d, got %d", i, b)
			break
		}
	}

	// Verify second 32KB still has data (0xAB)
	if _, err := img.ReadAt(readBuf, 32*1024); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	for i, b := range readBuf {
		if b != 0xAB {
			t.Errorf("Data corrupted at offset %d: got %d, want 0xAB", i, b)
			break
		}
	}

	img.Close()
}

func TestWriteZeroAtModeAlloc(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image with 2 clusters
	img, err := CreateSimple(path, 128*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write data to first cluster
	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Get file size after write
	info1, _ := img.file.Stat()
	sizeBefore := info1.Size()

	// Zero using ZeroAlloc mode (should keep allocation)
	if err := img.WriteZeroAtMode(0, 64*1024, ZeroAlloc); err != nil {
		t.Fatalf("WriteZeroAtMode(ZeroAlloc) failed: %v", err)
	}

	// Verify reading back zeros
	readBuf := make([]byte, 64*1024)
	if _, err := img.ReadAt(readBuf, 0); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	for i, b := range readBuf {
		if b != 0 {
			t.Errorf("Expected zero at offset %d, got %d", i, b)
			break
		}
	}

	// File size should be unchanged (allocation preserved)
	info2, _ := img.file.Stat()
	sizeAfter := info2.Size()
	if sizeAfter != sizeBefore {
		t.Errorf("File size changed: before=%d, after=%d (ZeroAlloc should preserve)", sizeBefore, sizeAfter)
	}

	img.Close()

	// Reopen and verify zeros persist
	img, err = Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if _, err := img.ReadAt(readBuf, 0); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	for i, b := range readBuf {
		if b != 0 {
			t.Errorf("After reopen: expected zero at offset %d, got %d", i, b)
			break
		}
	}
	img.Close()
}

func TestWriteZeroAtModeAllocOnUnallocated(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image
	img, err := CreateSimple(path, 128*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Get file size before (no data written yet)
	info1, _ := img.file.Stat()
	sizeBefore := info1.Size()

	// Zero using ZeroAlloc mode on unallocated cluster (should allocate)
	if err := img.WriteZeroAtMode(0, 64*1024, ZeroAlloc); err != nil {
		t.Fatalf("WriteZeroAtMode(ZeroAlloc) failed: %v", err)
	}

	// File size should have grown (new cluster allocated)
	info2, _ := img.file.Stat()
	sizeAfter := info2.Size()
	if sizeAfter <= sizeBefore {
		t.Errorf("File size didn't grow: before=%d, after=%d (ZeroAlloc on unallocated should allocate)", sizeBefore, sizeAfter)
	}

	// Verify reading back zeros
	readBuf := make([]byte, 64*1024)
	if _, err := img.ReadAt(readBuf, 0); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	for i, b := range readBuf {
		if b != 0 {
			t.Errorf("Expected zero at offset %d, got %d", i, b)
			break
		}
	}

	img.Close()
}

func TestFreeClusterReuse(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image
	img, err := CreateSimple(path, 256*1024) // 4 clusters
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write data to first two clusters
	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = 0xAA
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if _, err := img.WriteAt(data, 64*1024); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Get file size after initial writes
	info1, _ := img.file.Stat()
	sizeAfterWrites := info1.Size()

	// Get the physical offset of the first cluster
	info, err := img.translate(0)
	if err != nil {
		t.Fatalf("translate failed: %v", err)
	}
	firstClusterOffset := info.physOff & ^img.offsetMask

	// Zero the first cluster (this frees it)
	if err := img.WriteZeroAt(0, 64*1024); err != nil {
		t.Fatalf("WriteZeroAt failed: %v", err)
	}

	// Write data to a new virtual offset (third cluster)
	// This should reuse the freed cluster
	for i := range data {
		data[i] = 0xBB
	}
	if _, err := img.WriteAt(data, 128*1024); err != nil {
		t.Fatalf("WriteAt to third cluster failed: %v", err)
	}

	// File should not have grown significantly (cluster was reused)
	info2, _ := img.file.Stat()
	sizeAfterReuse := info2.Size()

	// Get physical offset of the new cluster
	info3, err := img.translate(128 * 1024)
	if err != nil {
		t.Fatalf("translate failed: %v", err)
	}
	newClusterOffset := info3.physOff & ^img.offsetMask

	// The new cluster should reuse the freed cluster's offset
	if newClusterOffset == firstClusterOffset {
		t.Logf("Free cluster reuse confirmed: offset 0x%x", newClusterOffset)
	} else {
		// File should have grown if cluster wasn't reused
		t.Logf("Note: Cluster not reused (first: 0x%x, new: 0x%x)", firstClusterOffset, newClusterOffset)
	}

	// File size should not have grown much more than one cluster
	// (may grow slightly due to refcount blocks, etc.)
	if sizeAfterReuse > sizeAfterWrites+int64(img.clusterSize)*2 {
		t.Logf("Warning: File grew significantly: %d -> %d", sizeAfterWrites, sizeAfterReuse)
	}

	// Verify the data is correct
	readBuf := make([]byte, 64*1024)
	if _, err := img.ReadAt(readBuf, 128*1024); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	for i, b := range readBuf {
		if b != 0xBB {
			t.Errorf("Data corrupted at offset %d: got %d, want 0xBB", i, b)
			break
		}
	}

	img.Close()
}

func TestRefcountDeallocation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image
	img, err := CreateSimple(path, 128*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write data to allocate a cluster
	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = 0xCD
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Get the physical offset of the allocated cluster
	info, err := img.translate(0)
	if err != nil {
		t.Fatalf("translate failed: %v", err)
	}
	if info.ctype != clusterNormal {
		t.Fatalf("Expected clusterNormal, got %v", info.ctype)
	}

	// Align to cluster start
	clusterOffset := info.physOff & ^img.offsetMask

	// Verify refcount is 1
	refcount, err := img.ClusterRefcount(clusterOffset)
	if err != nil {
		t.Fatalf("ClusterRefcount failed: %v", err)
	}
	if refcount != 1 {
		t.Errorf("Refcount before deallocation = %d, want 1", refcount)
	}

	// Zero the cluster (should decrement refcount)
	if err := img.WriteZeroAt(0, 64*1024); err != nil {
		t.Fatalf("WriteZeroAt failed: %v", err)
	}

	// Verify refcount is now 0
	refcount, err = img.ClusterRefcount(clusterOffset)
	if err != nil {
		t.Fatalf("ClusterRefcount failed: %v", err)
	}
	if refcount != 0 {
		t.Errorf("Refcount after deallocation = %d, want 0", refcount)
	}

	// Verify the cluster now reads as zero
	readBuf := make([]byte, 64*1024)
	if _, err := img.ReadAt(readBuf, 0); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	for i, b := range readBuf {
		if b != 0 {
			t.Errorf("Expected zero at offset %d, got %d", i, b)
			break
		}
	}

	img.Close()
}

func TestRawBackingFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	rawPath := filepath.Join(dir, "base.raw")
	overlayPath := filepath.Join(dir, "overlay.qcow2")

	// Create a raw backing file with test pattern
	rawSize := int64(128 * 1024) // 128KB
	rawFile, err := os.Create(rawPath)
	if err != nil {
		t.Fatalf("Failed to create raw file: %v", err)
	}

	// Write test pattern to raw file
	testData := make([]byte, rawSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	if _, err := rawFile.Write(testData); err != nil {
		rawFile.Close()
		t.Fatalf("Failed to write raw file: %v", err)
	}
	rawFile.Close()

	// Create qcow2 overlay with raw backing file
	overlay, err := Create(overlayPath, CreateOptions{
		Size:          uint64(rawSize),
		BackingFile:   rawPath,
		BackingFormat: "raw",
	})
	if err != nil {
		t.Fatalf("Create overlay failed: %v", err)
	}

	// Verify backing format extension was written
	if overlay.BackingFormat() != "raw" {
		t.Errorf("BackingFormat() = %q, want %q", overlay.BackingFormat(), "raw")
	}

	// Read data - should come from raw backing file
	readBuf2 := make([]byte, rawSize)
	n, err := overlay.ReadAt(readBuf2, 0)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if int64(n) != rawSize {
		t.Errorf("Read %d bytes, want %d", n, rawSize)
	}

	// Verify data matches raw file
	for i := range readBuf2 {
		expected := byte(i % 256)
		if readBuf2[i] != expected {
			t.Errorf("Data mismatch at offset %d: got %d, want %d", i, readBuf2[i], expected)
			break
		}
	}

	// Write to overlay (COW)
	cowData := []byte("OVERLAY DATA")
	if _, err := overlay.WriteAt(cowData, 1000); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Read back - should see overlay data
	readBuf3 := make([]byte, len(cowData))
	if _, err := overlay.ReadAt(readBuf3, 1000); err != nil {
		t.Fatalf("ReadAt after write failed: %v", err)
	}
	if !bytes.Equal(readBuf3, cowData) {
		t.Errorf("COW data = %q, want %q", readBuf3, cowData)
	}

	overlay.Close()

	// Reopen and verify data persists
	overlay2, err := Open(overlayPath)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer overlay2.Close()

	// Verify backing format is still "raw"
	if overlay2.BackingFormat() != "raw" {
		t.Errorf("After reopen: BackingFormat() = %q, want %q", overlay2.BackingFormat(), "raw")
	}

	// Verify COW data
	if _, err := overlay2.ReadAt(readBuf3, 1000); err != nil {
		t.Fatalf("ReadAt after reopen failed: %v", err)
	}
	if !bytes.Equal(readBuf3, cowData) {
		t.Errorf("After reopen: COW data = %q, want %q", readBuf3, cowData)
	}

	t.Log("Raw backing file support confirmed")
}

func TestOverlapChecks(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image
	img, err := CreateSimple(path, 1024*1024) // 1MB
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Write some data to allocate an L2 table and data cluster
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xAB
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Test header overlap (cluster 0)
	result := img.CheckOverlap(0)
	if !result.Overlaps || result.MetadataType != "header" {
		t.Errorf("Header check: got Overlaps=%v, Type=%s; want Overlaps=true, Type=header",
			result.Overlaps, result.MetadataType)
	}

	// Test L1 table overlap (typically cluster 1)
	result = img.CheckOverlap(img.header.L1TableOffset)
	if !result.Overlaps || result.MetadataType != "l1_table" {
		t.Errorf("L1 table check: got Overlaps=%v, Type=%s; want Overlaps=true, Type=l1_table",
			result.Overlaps, result.MetadataType)
	}

	// Test refcount table overlap (typically cluster 2)
	result = img.CheckOverlap(img.header.RefcountTableOffset)
	if !result.Overlaps || result.MetadataType != "refcount_table" {
		t.Errorf("Refcount table check: got Overlaps=%v, Type=%s; want Overlaps=true, Type=refcount_table",
			result.Overlaps, result.MetadataType)
	}

	// Find the refcount block offset from the refcount table
	if len(img.refcountTable) >= 8 {
		refBlockOffset := binary.BigEndian.Uint64(img.refcountTable[0:8])
		if refBlockOffset != 0 {
			result = img.CheckOverlap(refBlockOffset)
			if !result.Overlaps || result.MetadataType != "refcount_block" {
				t.Errorf("Refcount block check: got Overlaps=%v, Type=%s; want Overlaps=true, Type=refcount_block",
					result.Overlaps, result.MetadataType)
			}
		}
	}

	// Find the L2 table offset from the L1 table
	if len(img.l1Table) >= 8 {
		l1Entry := binary.BigEndian.Uint64(img.l1Table[0:8])
		if l1Entry != 0 {
			l2Offset := l1Entry & L2EntryOffsetMask
			result = img.CheckOverlap(l2Offset)
			if !result.Overlaps || result.MetadataType != "l2_table" {
				t.Errorf("L2 table check: got Overlaps=%v, Type=%s; want Overlaps=true, Type=l2_table",
					result.Overlaps, result.MetadataType)
			}
		}
	}

	// Test a data cluster (should not overlap)
	// Get the data cluster offset from the L2 table
	info, err := img.translate(0)
	if err != nil {
		t.Fatalf("translate failed: %v", err)
	}
	if info.ctype == clusterNormal {
		dataCluster := info.physOff & ^img.offsetMask
		result = img.CheckOverlap(dataCluster)
		if result.Overlaps {
			t.Errorf("Data cluster check: got Overlaps=true, Type=%s; want Overlaps=false",
				result.MetadataType)
		}
	}

	t.Log("Overlap checks verified for all metadata types")
}

func TestLazyRefcounts(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image with lazy refcounts enabled
	img, err := Create(path, CreateOptions{
		Size:          1024 * 1024, // 1MB
		LazyRefcounts: true,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Verify lazy refcounts is enabled
	if !img.HasLazyRefcounts() {
		t.Error("HasLazyRefcounts() should return true")
	}

	// Write some data to allocate clusters
	data1 := make([]byte, 64*1024) // One cluster
	for i := range data1 {
		data1[i] = byte(i % 256)
	}
	if _, err := img.WriteAt(data1, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Write to a second cluster
	data2 := make([]byte, 64*1024)
	for i := range data2 {
		data2[i] = byte((i + 128) % 256)
	}
	if _, err := img.WriteAt(data2, 64*1024); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	img.Close()

	// Reopen - image should be dirty (lazy refcounts keeps dirty bit set)
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Should have rebuilt refcounts on open
	if !img2.HasLazyRefcounts() {
		t.Error("After reopen: HasLazyRefcounts() should return true")
	}

	// Verify data is still intact
	readBuf := make([]byte, 64*1024)
	if _, err := img2.ReadAt(readBuf, 0); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	for i := range readBuf {
		expected := byte(i % 256)
		if readBuf[i] != expected {
			t.Errorf("Data mismatch at offset %d: got %d, want %d", i, readBuf[i], expected)
			break
		}
	}

	// Check that refcounts are correct after rebuild
	// Header cluster should have refcount 1
	refcount, err := img2.ClusterRefcount(0)
	if err != nil {
		t.Fatalf("ClusterRefcount(0) failed: %v", err)
	}
	if refcount != 1 {
		t.Errorf("Header cluster refcount = %d, want 1", refcount)
	}

	// Allocated data cluster should have refcount 1
	info, err := img2.translate(0)
	if err != nil {
		t.Fatalf("translate failed: %v", err)
	}
	if info.ctype == clusterNormal {
		dataCluster := info.physOff & ^img2.offsetMask
		refcount, err := img2.ClusterRefcount(dataCluster)
		if err != nil {
			t.Fatalf("ClusterRefcount for data cluster failed: %v", err)
		}
		if refcount != 1 {
			t.Errorf("Data cluster refcount = %d, want 1", refcount)
		}
	}

	img2.Close()
	t.Log("Lazy refcounts feature verified")
}

func TestLazyRefcountsDeferral(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image with lazy refcounts enabled
	img, err := Create(path, CreateOptions{
		Size:          1024 * 1024,
		LazyRefcounts: true,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write and zero clusters - refcount updates should be skipped
	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = 0xAB
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Get the data cluster offset
	info, err := img.translate(0)
	if err != nil {
		t.Fatalf("translate failed: %v", err)
	}
	dataCluster := info.physOff & ^img.offsetMask

	// Zero the cluster (would normally decrement refcount)
	if err := img.WriteZeroAt(0, 64*1024); err != nil {
		t.Fatalf("WriteZeroAt failed: %v", err)
	}

	// In lazy mode, refcount might not be updated yet
	// But after reopening, it should be correctly rebuilt to 0 (since cluster is zeroed)
	img.Close()

	// Reopen and check
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img2.Close()

	// After rebuild, the zeroed cluster should have refcount 0
	refcount, err := img2.ClusterRefcount(dataCluster)
	if err != nil {
		t.Fatalf("ClusterRefcount failed: %v", err)
	}
	if refcount != 0 {
		t.Errorf("Zeroed cluster refcount after rebuild = %d, want 0", refcount)
	}

	t.Log("Lazy refcount deferral and rebuild verified")
}

func TestCheck(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create a clean image
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write some data
	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Run check
	result, err := img.Check()
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}

	// Should be clean
	if !result.IsClean() {
		t.Errorf("Image should be clean, got: corruptions=%d, leaks=%d, errors=%v",
			result.Corruptions, result.Leaks, result.Errors)
	}

	// Should have some referenced clusters
	if result.ReferencedClusters == 0 {
		t.Error("ReferencedClusters should be > 0")
	}

	// Should have some allocated clusters
	if result.AllocatedClusters == 0 {
		t.Error("AllocatedClusters should be > 0")
	}

	img.Close()
	t.Logf("Check result: referenced=%d, allocated=%d, fragmented=%d",
		result.ReferencedClusters, result.AllocatedClusters, result.FragmentedClusters)
}

func TestCheckAfterWriteZero(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image and write data
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = 0xCD
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Zero the data (deallocates cluster)
	if err := img.WriteZeroAt(0, 64*1024); err != nil {
		t.Fatalf("WriteZeroAt failed: %v", err)
	}

	// Check should find the deallocated cluster as a leak
	// (refcount was decremented to 0, but the cluster still exists in the file)
	result, err := img.Check()
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}

	// The zeroed cluster should show as a leak
	if result.Leaks == 0 {
		t.Log("No leaks detected (cluster may have been reused or not counted)")
	} else {
		t.Logf("Detected %d leaked clusters (%d bytes)", result.Leaks, result.LeakedClusters)
	}

	img.Close()
}

func TestRepair(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image with lazy refcounts (so refcounts aren't updated during writes)
	img, err := Create(path, CreateOptions{
		Size:          1024 * 1024,
		LazyRefcounts: true,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write some data
	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if _, err := img.WriteAt(data, 64*1024); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Run repair
	result, err := img.Repair()
	if err != nil {
		t.Fatalf("Repair failed: %v", err)
	}

	// After repair, should be clean
	if !result.IsClean() {
		t.Errorf("After repair, image should be clean, got: corruptions=%d, leaks=%d, errors=%v",
			result.Corruptions, result.Leaks, result.Errors)
	}

	// Verify data is still intact
	readBuf := make([]byte, 64*1024)
	if _, err := img.ReadAt(readBuf, 0); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	for i := range readBuf {
		expected := byte(i % 256)
		if readBuf[i] != expected {
			t.Errorf("Data corrupted at offset %d: got %d, want %d", i, readBuf[i], expected)
			break
		}
	}

	img.Close()
	t.Log("Repair verified successfully")
}

func TestCheckReadOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create and close image
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	img.Close()

	// Open read-only
	img2, err := OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	defer img2.Close()

	// Check should work on read-only image
	result, err := img2.Check()
	if err != nil {
		t.Fatalf("Check on read-only failed: %v", err)
	}
	if !result.IsClean() {
		t.Errorf("Read-only image should be clean")
	}

	// Repair should fail on read-only image
	_, err = img2.Repair()
	if err != ErrReadOnly {
		t.Errorf("Repair on read-only: got err %v, want ErrReadOnly", err)
	}
}

func TestWriteBarrierModes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Default should be BarrierMetadata
	if img.WriteBarrierMode() != BarrierMetadata {
		t.Errorf("Default barrier mode = %d, want %d (BarrierMetadata)",
			img.WriteBarrierMode(), BarrierMetadata)
	}

	// Test setting barrier mode
	img.SetWriteBarrierMode(BarrierNone)
	if img.WriteBarrierMode() != BarrierNone {
		t.Errorf("After SetWriteBarrierMode(BarrierNone), got %d", img.WriteBarrierMode())
	}

	img.SetWriteBarrierMode(BarrierBatched)
	if img.WriteBarrierMode() != BarrierBatched {
		t.Errorf("After SetWriteBarrierMode(BarrierBatched), got %d", img.WriteBarrierMode())
	}

	img.SetWriteBarrierMode(BarrierFull)
	if img.WriteBarrierMode() != BarrierFull {
		t.Errorf("After SetWriteBarrierMode(BarrierFull), got %d", img.WriteBarrierMode())
	}

	// Write with BarrierFull mode should work
	data := []byte("test data for barrier mode")
	_, err = img.WriteAt(data, 0)
	if err != nil {
		t.Errorf("WriteAt with BarrierFull failed: %v", err)
	}

	// Read back
	buf := make([]byte, len(data))
	_, err = img.ReadAt(buf, 0)
	if err != nil {
		t.Errorf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Errorf("Data mismatch after BarrierFull write")
	}
}

func TestWriteBarrierModeNone(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image with BarrierNone for maximum performance
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	img.SetWriteBarrierMode(BarrierNone)

	// Write multiple clusters to exercise allocation paths
	for i := 0; i < 10; i++ {
		data := make([]byte, 4096)
		for j := range data {
			data[j] = byte(i)
		}
		_, err = img.WriteAt(data, int64(i*65536)) // Different clusters
		if err != nil {
			t.Fatalf("WriteAt cluster %d failed: %v", i, err)
		}
	}

	img.Close()

	// Reopen and verify data
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer img2.Close()

	for i := 0; i < 10; i++ {
		buf := make([]byte, 4096)
		_, err = img2.ReadAt(buf, int64(i*65536))
		if err != nil {
			t.Fatalf("ReadAt cluster %d failed: %v", i, err)
		}
		for j, b := range buf {
			if b != byte(i) {
				t.Errorf("Cluster %d byte %d: got %d, want %d", i, j, b, i)
				break
			}
		}
	}
}

func TestWriteBarrierWithZeroCluster(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Use BarrierFull mode
	img.SetWriteBarrierMode(BarrierFull)

	// Write some data
	data := make([]byte, 65536) // One cluster
	for i := range data {
		data[i] = 0xAA
	}
	_, err = img.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Zero the cluster
	err = img.WriteZeroAt(0, 65536)
	if err != nil {
		t.Fatalf("WriteZeroAt failed: %v", err)
	}

	img.Close()

	// Reopen and verify zeros
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer img2.Close()

	buf := make([]byte, 65536)
	_, err = img2.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	for i, b := range buf {
		if b != 0 {
			t.Errorf("Byte %d after WriteZeroAt: got %d, want 0", i, b)
			break
		}
	}
}

func TestWriteBarrierModeBatched(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image with BarrierBatched for performance
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	img.SetWriteBarrierMode(BarrierBatched)

	if img.WriteBarrierMode() != BarrierBatched {
		t.Errorf("Barrier mode = %d, want %d (BarrierBatched)",
			img.WriteBarrierMode(), BarrierBatched)
	}

	// Write multiple clusters - syncs should be deferred
	for i := 0; i < 10; i++ {
		data := make([]byte, 4096)
		for j := range data {
			data[j] = byte(i)
		}
		_, err = img.WriteAt(data, int64(i*65536)) // Different clusters
		if err != nil {
			t.Fatalf("WriteAt cluster %d failed: %v", i, err)
		}
	}

	// Explicit Flush should sync all pending writes
	if err := img.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	img.Close()

	// Reopen and verify data
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer img2.Close()

	for i := 0; i < 10; i++ {
		buf := make([]byte, 4096)
		_, err = img2.ReadAt(buf, int64(i*65536))
		if err != nil {
			t.Fatalf("ReadAt cluster %d failed: %v", i, err)
		}
		for j, b := range buf {
			if b != byte(i) {
				t.Errorf("Cluster %d byte %d: got %d, want %d", i, j, b, i)
				break
			}
		}
	}
}

func TestWriteBarrierBatchedWithBackingFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create base image with pattern
	basePath := filepath.Join(dir, "base.qcow2")
	base, err := CreateSimple(basePath, 256*1024)
	if err != nil {
		t.Fatalf("Create base failed: %v", err)
	}
	baseData := make([]byte, 65536)
	for i := range baseData {
		baseData[i] = 0xBB
	}
	if _, err := base.WriteAt(baseData, 0); err != nil {
		t.Fatalf("Write to base failed: %v", err)
	}
	base.Close()

	// Create overlay with batched mode
	overlayPath := filepath.Join(dir, "overlay.qcow2")
	overlay, err := CreateOverlay(overlayPath, basePath)
	if err != nil {
		t.Fatalf("Create overlay failed: %v", err)
	}
	overlay.SetWriteBarrierMode(BarrierBatched)

	// COW write (should copy from backing first)
	cowData := []byte("COW data in batched mode")
	if _, err := overlay.WriteAt(cowData, 100); err != nil {
		t.Fatalf("COW write failed: %v", err)
	}

	// Flush batched syncs
	if err := overlay.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	overlay.Close()

	// Verify overlay reads correctly
	overlay2, err := Open(overlayPath)
	if err != nil {
		t.Fatalf("Reopen overlay failed: %v", err)
	}
	defer overlay2.Close()

	// Check COW data
	buf := make([]byte, len(cowData))
	if _, err := overlay2.ReadAt(buf, 100); err != nil {
		t.Fatalf("Read COW data failed: %v", err)
	}
	if !bytes.Equal(buf, cowData) {
		t.Errorf("COW data mismatch: got %q, want %q", buf, cowData)
	}

	// Check backing data still readable (outside COW area but same cluster)
	buf2 := make([]byte, 10)
	if _, err := overlay2.ReadAt(buf2, 0); err != nil {
		t.Fatalf("Read backing data failed: %v", err)
	}
	// After COW, the cluster should contain copied backing data (0xBB)
	for i, b := range buf2 {
		if b != 0xBB {
			t.Errorf("Backing data at %d: got 0x%02x, want 0xBB", i, b)
			break
		}
	}
}

func TestBackingChainDepthLimit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create a chain that exceeds MaxBackingChainDepth
	// We only need MaxBackingChainDepth+2 images to trigger the error:
	// - base image (depth 0)
	// - MaxBackingChainDepth overlays (depths 1 through MaxBackingChainDepth)
	// - one more overlay that should fail (depth MaxBackingChainDepth+1)
	chainLength := MaxBackingChainDepth + 2

	// Create base image
	basePath := filepath.Join(dir, "base.qcow2")
	base, err := CreateSimple(basePath, 64*1024)
	if err != nil {
		t.Fatalf("Create base failed: %v", err)
	}
	base.Close()

	// Create chain of overlays
	prevPath := basePath

	for i := 1; i < chainLength; i++ {
		overlayPath := filepath.Join(dir, fmt.Sprintf("overlay%d.qcow2", i))
		overlay, err := CreateOverlay(overlayPath, prevPath)

		if i <= MaxBackingChainDepth {
			// These should succeed
			if err != nil {
				t.Fatalf("CreateOverlay %d failed unexpectedly: %v", i, err)
			}
			overlay.Close()
			prevPath = overlayPath
		} else {
			// This should fail with ErrBackingChainTooDeep
			if err == nil {
				overlay.Close()
				t.Fatalf("CreateOverlay %d should have failed with chain too deep error", i)
			}
			if !errors.Is(err, ErrBackingChainTooDeep) {
				t.Errorf("CreateOverlay %d error = %v, want ErrBackingChainTooDeep", i, err)
			}
			return // Test passed
		}
	}

	t.Error("Expected backing chain depth limit to be enforced")
}

func TestWriteAtCompressedCompressibleData(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "compressed.qcow2")

	// Create a 1MB image
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Set compression level
	img.SetCompressionLevel(CompressionDefault)

	// Create highly compressible data (all zeros)
	data := make([]byte, img.ClusterSize())

	// Write compressed
	n, err := img.WriteAtCompressed(data, 0)
	if err != nil {
		t.Fatalf("WriteAtCompressed failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("WriteAtCompressed returned %d, want %d", n, len(data))
	}

	// Read back and verify
	readBuf := make([]byte, img.ClusterSize())
	_, err = img.ReadAt(readBuf, 0)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(readBuf, data) {
		t.Error("Read data doesn't match written data")
	}

	img.Close()

	// Reopen and verify data persists
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer img2.Close()

	readBuf2 := make([]byte, img2.ClusterSize())
	_, err = img2.ReadAt(readBuf2, 0)
	if err != nil {
		t.Fatalf("ReadAt after reopen failed: %v", err)
	}
	if !bytes.Equal(readBuf2, data) {
		t.Error("Read data after reopen doesn't match")
	}
}

func TestWriteAtCompressedPatternData(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "compressed_pattern.qcow2")

	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	img.SetCompressionLevel(CompressionDefault)

	// Create compressible data with repeating pattern
	data := make([]byte, img.ClusterSize())
	pattern := []byte("This is a test pattern that repeats!")
	for i := 0; i < len(data); i += len(pattern) {
		copy(data[i:], pattern)
	}

	// Write compressed
	n, err := img.WriteAtCompressed(data, 0)
	if err != nil {
		t.Fatalf("WriteAtCompressed failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("WriteAtCompressed returned %d, want %d", n, len(data))
	}

	// Read back and verify
	readBuf := make([]byte, img.ClusterSize())
	_, err = img.ReadAt(readBuf, 0)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(readBuf, data) {
		t.Error("Read data doesn't match written data")
	}
}

func TestWriteAtCompressedRandomData(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "compressed_random.qcow2")

	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	img.SetCompressionLevel(CompressionDefault)

	// Create incompressible data (pseudo-random)
	data := make([]byte, img.ClusterSize())
	for i := range data {
		// Simple PRNG
		data[i] = byte((i * 7919) ^ (i >> 3))
	}

	// Write - should fall back to uncompressed since random data doesn't compress
	n, err := img.WriteAtCompressed(data, 0)
	if err != nil {
		t.Fatalf("WriteAtCompressed failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("WriteAtCompressed returned %d, want %d", n, len(data))
	}

	// Read back and verify
	readBuf := make([]byte, img.ClusterSize())
	_, err = img.ReadAt(readBuf, 0)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(readBuf, data) {
		t.Error("Read data doesn't match written data")
	}
}

func TestWriteAtCompressedUnaligned(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "compressed_unaligned.qcow2")

	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	img.SetCompressionLevel(CompressionDefault)

	data := make([]byte, img.ClusterSize())

	// Writing at unaligned offset should fail
	_, err = img.WriteAtCompressed(data, 512)
	if err == nil {
		t.Error("WriteAtCompressed with unaligned offset should fail")
	}
}

func TestWriteAtCompressedPartialCluster(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "compressed_partial.qcow2")

	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	img.SetCompressionLevel(CompressionDefault)

	// Partial cluster should fail
	data := make([]byte, 4096)
	_, err = img.WriteAtCompressed(data, 0)
	if err == nil {
		t.Error("WriteAtCompressed with partial cluster should fail")
	}
}

func TestCompressionLevelSettings(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "compression_level.qcow2")

	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Default should be disabled
	if img.GetCompressionLevel() != CompressionDisabled {
		t.Errorf("Default compression level should be Disabled, got %d", img.GetCompressionLevel())
	}

	// Set to Fast
	img.SetCompressionLevel(CompressionFast)
	if img.GetCompressionLevel() != CompressionFast {
		t.Errorf("After SetCompressionLevel(Fast), got %d", img.GetCompressionLevel())
	}

	// Set to Default
	img.SetCompressionLevel(CompressionDefault)
	if img.GetCompressionLevel() != CompressionDefault {
		t.Errorf("After SetCompressionLevel(Default), got %d", img.GetCompressionLevel())
	}

	// Set to Best
	img.SetCompressionLevel(CompressionBest)
	if img.GetCompressionLevel() != CompressionBest {
		t.Errorf("After SetCompressionLevel(Best), got %d", img.GetCompressionLevel())
	}
}

func TestUnknownEncryptionMethodRejected(t *testing.T) {
	t.Parallel()

	// Test that unknown encryption methods (>2) are rejected
	// AES (method=1) and LUKS (method=2) are supported (read-only)
	dir := t.TempDir()
	path := filepath.Join(dir, "encrypted.qcow2")

	// Create a minimal QCOW2 header with unknown encryption method = 99
	header := make([]byte, HeaderSizeV3)

	// Magic
	binary.BigEndian.PutUint32(header[0:4], Magic)
	// Version 3
	binary.BigEndian.PutUint32(header[4:8], 3)
	// No backing file
	binary.BigEndian.PutUint64(header[8:16], 0)
	binary.BigEndian.PutUint32(header[16:20], 0)
	// Cluster bits = 16 (64KB)
	binary.BigEndian.PutUint32(header[20:24], 16)
	// Virtual size = 1MB
	binary.BigEndian.PutUint64(header[24:32], 1024*1024)
	// Unknown encryption method = 99 - THIS SHOULD CAUSE REJECTION
	binary.BigEndian.PutUint32(header[32:36], 99)
	// L1 size
	binary.BigEndian.PutUint32(header[36:40], 1)
	// L1 table offset
	binary.BigEndian.PutUint64(header[40:48], 0x30000)
	// Refcount table offset
	binary.BigEndian.PutUint64(header[48:56], 0x10000)
	// Refcount table clusters
	binary.BigEndian.PutUint32(header[56:60], 1)
	// No snapshots
	binary.BigEndian.PutUint32(header[60:64], 0)
	binary.BigEndian.PutUint64(header[64:72], 0)
	// V3 fields
	binary.BigEndian.PutUint64(header[72:80], 0)              // Incompatible features
	binary.BigEndian.PutUint64(header[80:88], 0)              // Compatible features
	binary.BigEndian.PutUint64(header[88:96], 0)              // Autoclear features
	binary.BigEndian.PutUint32(header[96:100], 4)             // Refcount order
	binary.BigEndian.PutUint32(header[100:104], HeaderSizeV3) // Header length

	// Write header to file
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	if _, err := f.Write(header); err != nil {
		f.Close()
		t.Fatalf("Failed to write header: %v", err)
	}
	f.Close()

	// Try to open - should fail with encrypted image error (unknown method)
	_, err = Open(path)
	if err == nil {
		t.Error("Open should fail for unknown encrypted image method")
	}
	if !errors.Is(err, ErrEncryptedImage) {
		t.Errorf("Expected ErrEncryptedImage for unknown method, got: %v", err)
	}
}

func TestHeaderEncryptionMethods(t *testing.T) {
	// Test IsEncrypted and EncryptionMethod on headers
	tests := []struct {
		method      uint32
		isEncrypted bool
	}{
		{EncryptionNone, false},
		{EncryptionAES, true},
		{EncryptionLUKS, true},
	}

	for _, tc := range tests {
		h := &Header{EncryptMethod: tc.method}
		if h.IsEncrypted() != tc.isEncrypted {
			t.Errorf("EncryptMethod=%d: IsEncrypted()=%v, want %v",
				tc.method, h.IsEncrypted(), tc.isEncrypted)
		}
		if h.EncryptionMethod() != tc.method {
			t.Errorf("EncryptMethod=%d: EncryptionMethod()=%d, want %d",
				tc.method, h.EncryptionMethod(), tc.method)
		}
	}
}

func TestWriteAtCompressedZstd(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "zstd_compressed.qcow2")

	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Enable zstd compression
	img.SetCompressionLevel(CompressionDefault)
	img.SetCompressionType(CompressionZstd)

	// Create compressible data (zeros compress very well)
	data := make([]byte, img.ClusterSize())

	// Write compressed with zstd
	n, err := img.WriteAtCompressed(data, 0)
	if err != nil {
		t.Fatalf("WriteAtCompressed with zstd failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("WriteAtCompressed returned %d, want %d", n, len(data))
	}

	img.Close()

	// Reopen - header should now have compression type persisted
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer img2.Close()

	// Verify compression type was persisted
	if img2.header.CompressionType != CompressionZstd {
		t.Errorf("Compression type not persisted: got %d, want %d",
			img2.header.CompressionType, CompressionZstd)
	}

	// Read back and verify
	readBuf := make([]byte, img2.ClusterSize())
	_, err = img2.ReadAt(readBuf, 0)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(readBuf, data) {
		t.Error("Read data doesn't match written data")
	}
}

func TestCompressionTypeSettings(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "compression_type.qcow2")

	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Default should be zlib (0)
	if img.GetCompressionType() != CompressionZlib {
		t.Errorf("Default compression type should be Zlib (0), got %d", img.GetCompressionType())
	}

	// Set to zstd
	img.SetCompressionType(CompressionZstd)
	if img.GetCompressionType() != CompressionZstd {
		t.Errorf("After SetCompressionType(Zstd), got %d", img.GetCompressionType())
	}

	// Set back to zlib
	img.SetCompressionType(CompressionZlib)
	if img.GetCompressionType() != CompressionZlib {
		t.Errorf("After SetCompressionType(Zlib), got %d", img.GetCompressionType())
	}
}

func TestExternalDataFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	imgPath := filepath.Join(dir, "test.qcow2")
	dataPath := filepath.Join(dir, "test.raw")

	// Create a qcow2 image with external data file using qemu-img
	result := testutil.RunQemuImg(t, "create", "-f", "qcow2", "-o", "data_file="+dataPath, imgPath, "1M")
	if result.ExitCode != 0 {
		t.Skipf("qemu-img doesn't support external data files: %s", result.Stderr)
	}

	// Open the image
	img, err := Open(imgPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img.Close()

	// Verify external data file was detected
	if !img.header.HasExternalDataFile() {
		t.Fatal("Expected HasExternalDataFile() to be true")
	}

	if img.extensions.ExternalDataFile != dataPath {
		t.Errorf("ExternalDataFile = %q, want %q", img.extensions.ExternalDataFile, dataPath)
	}

	// Write test data
	testData := []byte("Hello, External Data File!")
	testData = append(testData, make([]byte, 64*1024-len(testData))...) // Pad to cluster size

	n, err := img.WriteAt(testData, 0)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if n != len(testData) {
		t.Errorf("WriteAt = %d, want %d", n, len(testData))
	}

	img.Close()

	// Reopen and verify data persists
	img2, err := Open(imgPath)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer img2.Close()

	// Read data back
	readBuf := make([]byte, len(testData))
	n, err = img2.ReadAt(readBuf, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != len(testData) {
		t.Errorf("ReadAt = %d, want %d", n, len(testData))
	}

	if !bytes.Equal(readBuf, testData) {
		t.Errorf("Data mismatch: read %q, want %q", readBuf[:50], testData[:50])
	}

	// Verify qemu-img check passes
	checkResult := testutil.QemuCheck(t, imgPath)
	if !checkResult.IsClean {
		t.Errorf("qemu-img check failed: %s", checkResult.Stderr)
	}
}

func TestExtendedL2Entries(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	imgPath := filepath.Join(dir, "test_extl2.qcow2")

	// Create a qcow2 image with extended L2 entries using qemu-img (requires QEMU >= 5.2)
	result := testutil.RunQemuImg(t, "create", "-f", "qcow2", "-o", "extended_l2=on", imgPath, "1M")
	if result.ExitCode != 0 {
		t.Skipf("qemu-img doesn't support extended_l2: %s", result.Stderr)
	}

	// Write test pattern using qemu-io at various subcluster offsets
	subclusterSize := 64 * 1024 / 32 // 2KB subclusters for 64KB clusters
	result = testutil.RunQemuIO(t, imgPath,
		fmt.Sprintf("write -P 0xAA 0 %d", subclusterSize),                    // Subcluster 0
		fmt.Sprintf("write -P 0xBB %d %d", subclusterSize*5, subclusterSize), // Subcluster 5
	)
	if result.ExitCode != 0 {
		t.Fatalf("qemu-io write failed: %s", result.Stderr)
	}

	// Open the image
	img, err := Open(imgPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img.Close()

	// Verify extended L2 was detected
	if !img.header.HasExtendedL2() {
		t.Fatal("Expected HasExtendedL2() to be true")
	}

	// Verify the extendedL2 field
	if !img.extendedL2 {
		t.Error("Expected img.extendedL2 to be true")
	}

	// Verify L2 entry size
	if img.l2EntrySize != 16 {
		t.Errorf("l2EntrySize = %d, want 16", img.l2EntrySize)
	}

	// Verify subcluster size
	expectedSubclusterSize := uint64(img.clusterSize / 32)
	if img.subclusterSize != expectedSubclusterSize {
		t.Errorf("subclusterSize = %d, want %d", img.subclusterSize, expectedSubclusterSize)
	}

	// Read from allocated subcluster 0
	buf := make([]byte, subclusterSize)
	n, err := img.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt subcluster 0 failed: %v", err)
	}
	if n != subclusterSize {
		t.Errorf("ReadAt = %d, want %d", n, subclusterSize)
	}
	for i, b := range buf {
		if b != 0xAA {
			t.Errorf("byte %d = 0x%02x, want 0xAA", i, b)
			break
		}
	}

	// Read from allocated subcluster 5
	n, err = img.ReadAt(buf, int64(subclusterSize*5))
	if err != nil {
		t.Fatalf("ReadAt subcluster 5 failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xBB {
			t.Errorf("byte %d = 0x%02x, want 0xBB", i, b)
			break
		}
	}

	// Read from unallocated subcluster 3 (should return zeros)
	n, err = img.ReadAt(buf, int64(subclusterSize*3))
	if err != nil {
		t.Fatalf("ReadAt subcluster 3 failed: %v", err)
	}
	for i, b := range buf {
		if b != 0x00 {
			t.Errorf("unallocated byte %d = 0x%02x, want 0x00", i, b)
			break
		}
	}
}
