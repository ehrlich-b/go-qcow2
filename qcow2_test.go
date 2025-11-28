package qcow2

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestCreateAndOpen(t *testing.T) {
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
	n, err = img2.ReadAt(buf2, 0)
	if err != nil {
		t.Fatalf("ReadAt after reopen failed: %v", err)
	}
	if !bytes.Equal(buf2, data) {
		t.Errorf("Data after reopen mismatch: got %q, want %q", buf2, data)
	}
}

func TestReadUnallocated(t *testing.T) {
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
	n, err = img.ReadAt(buf, offset)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Errorf("Cross-cluster data mismatch")
	}
}

func TestReadBeyondEOF(t *testing.T) {
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
	header := make([]byte, HeaderSizeV2)
	header[0] = 0x00 // Wrong magic

	_, err := ParseHeader(header)
	if err != ErrInvalidMagic {
		t.Errorf("ParseHeader with bad magic: got err %v, want ErrInvalidMagic", err)
	}
}

func TestL2Cache(t *testing.T) {
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
	if got := cache.get(1000); got == nil || got[0] != 1 {
		t.Error("cache.get(1000) failed")
	}
	// After accessing 1000, order is: 2000, 3000, 1000 (1000 is MRU)
	if got := cache.get(2000); got == nil || got[0] != 2 {
		t.Error("cache.get(2000) failed")
	}
	// After accessing 2000, order is: 3000, 1000, 2000 (2000 is MRU)
	if got := cache.get(3000); got == nil || got[0] != 3 {
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
	if got := cache.get(2000); got == nil || got[0] != 2 {
		t.Error("cache.get(2000) should still be present")
	}
	if got := cache.get(3000); got == nil || got[0] != 3 {
		t.Error("cache.get(3000) should still be present")
	}
	if got := cache.get(4000); got == nil || got[0] != 4 {
		t.Error("cache.get(4000) should still be present")
	}
}

func TestDirtyBitTracking(t *testing.T) {
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
