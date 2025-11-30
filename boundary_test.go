// boundary_test.go - Phase 2.1 Boundary Condition Tests
// These tests verify correct handling of edge cases at various boundaries.

package qcow2

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"
)

// =============================================================================
// 2.1 Boundary Condition Tests
// =============================================================================

// TestWriteAtExactClusterBoundary tests write that starts exactly at cluster boundary.
func TestWriteAtExactClusterBoundary(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "cluster_boundary.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16, // 64KB clusters
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	clusterSize := int64(img.ClusterSize())

	// Write exactly at cluster boundaries
	testOffsets := []int64{
		0,                 // First cluster start
		clusterSize,       // Second cluster start
		clusterSize * 2,   // Third cluster start
		clusterSize * 10,  // 10th cluster start
	}

	for _, offset := range testOffsets {
		data := make([]byte, 4096)
		for i := range data {
			data[i] = byte(offset / clusterSize)
		}

		n, err := img.WriteAt(data, offset)
		if err != nil {
			t.Errorf("WriteAt offset %d failed: %v", offset, err)
			continue
		}
		if n != len(data) {
			t.Errorf("WriteAt offset %d: wrote %d bytes, expected %d", offset, n, len(data))
		}

		// Verify read back
		buf := make([]byte, 4096)
		if _, err := img.ReadAt(buf, offset); err != nil {
			t.Errorf("ReadAt offset %d failed: %v", offset, err)
			continue
		}
		if !bytes.Equal(buf, data) {
			t.Errorf("Data mismatch at offset %d", offset)
		}
	}
}

// TestWriteEndsAtClusterBoundary tests write that ends exactly at cluster boundary.
func TestWriteEndsAtClusterBoundary(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "write_ends_boundary.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	clusterSize := int64(img.ClusterSize())

	// Write that ends exactly at cluster boundary
	// Start at (clusterSize - 4096) and write 4096 bytes
	offset := clusterSize - 4096
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xAA
	}

	n, err := img.WriteAt(data, offset)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if n != len(data) {
		t.Fatalf("WriteAt: wrote %d bytes, expected %d", n, len(data))
	}

	// Verify
	buf := make([]byte, 4096)
	if _, err := img.ReadAt(buf, offset); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Error("Data mismatch for write ending at boundary")
	}

	// Also test ending at second cluster boundary
	offset2 := clusterSize*2 - 8192
	data2 := make([]byte, 8192)
	for i := range data2 {
		data2[i] = 0xBB
	}

	if _, err := img.WriteAt(data2, offset2); err != nil {
		t.Fatalf("WriteAt second boundary failed: %v", err)
	}

	buf2 := make([]byte, 8192)
	if _, err := img.ReadAt(buf2, offset2); err != nil {
		t.Fatalf("ReadAt second boundary failed: %v", err)
	}
	if !bytes.Equal(buf2, data2) {
		t.Error("Data mismatch for write ending at second boundary")
	}
}

// TestWriteSpansThreeClusters tests a single write across 3 clusters.
func TestWriteSpansThreeClusters(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "spans_three.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	clusterSize := int64(img.ClusterSize())

	// Write starting in middle of first cluster, spanning through 3 clusters
	// Start at clusterSize/2, write 2*clusterSize bytes
	// This spans: cluster 0 (second half), cluster 1 (full), cluster 2 (first half)
	offset := clusterSize / 2
	dataSize := int(clusterSize * 2)
	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	n, err := img.WriteAt(data, offset)
	if err != nil {
		t.Fatalf("WriteAt spanning 3 clusters failed: %v", err)
	}
	if n != dataSize {
		t.Fatalf("WriteAt: wrote %d bytes, expected %d", n, dataSize)
	}

	// Verify entire write
	buf := make([]byte, dataSize)
	if _, err := img.ReadAt(buf, offset); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Error("Data mismatch for write spanning 3 clusters")
	}

	// Verify by reading each cluster portion separately
	// First cluster (second half)
	buf1 := make([]byte, clusterSize/2)
	if _, err := img.ReadAt(buf1, offset); err != nil {
		t.Fatalf("ReadAt first portion failed: %v", err)
	}
	if !bytes.Equal(buf1, data[:clusterSize/2]) {
		t.Error("First portion mismatch")
	}

	// Second cluster (full)
	buf2 := make([]byte, clusterSize)
	if _, err := img.ReadAt(buf2, clusterSize); err != nil {
		t.Fatalf("ReadAt second cluster failed: %v", err)
	}
	if !bytes.Equal(buf2, data[clusterSize/2:clusterSize/2+clusterSize]) {
		t.Error("Second cluster mismatch")
	}

	// Third cluster (first half)
	buf3 := make([]byte, clusterSize/2)
	if _, err := img.ReadAt(buf3, clusterSize*2); err != nil {
		t.Fatalf("ReadAt third portion failed: %v", err)
	}
	if !bytes.Equal(buf3, data[clusterSize+clusterSize/2:]) {
		t.Error("Third portion mismatch")
	}
}

// TestWriteSpansMultipleL2Tables tests write that crosses L2 table boundary.
func TestWriteSpansMultipleL2Tables(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "spans_l2.qcow2")

	// With 64KB clusters, each L2 table covers 8192 * 64KB = 512MB
	// We need a large virtual size to have multiple L2 tables
	img, err := Create(path, CreateOptions{
		Size:        1024 * 1024 * 1024, // 1GB - spans 2 L2 tables
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	clusterSize := int64(img.ClusterSize())
	l2Entries := clusterSize / 8              // 8192 entries per L2 table
	l2Coverage := l2Entries * clusterSize     // 512MB per L2 table

	// Write across L2 table boundary
	// Start 2 clusters before L2 boundary, write 4 clusters
	offset := l2Coverage - 2*clusterSize
	dataSize := int(4 * clusterSize)
	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	n, err := img.WriteAt(data, offset)
	if err != nil {
		t.Fatalf("WriteAt spanning L2 tables failed: %v", err)
	}
	if n != dataSize {
		t.Fatalf("WriteAt: wrote %d bytes, expected %d", n, dataSize)
	}

	// Verify
	buf := make([]byte, dataSize)
	if _, err := img.ReadAt(buf, offset); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Error("Data mismatch for write spanning L2 tables")
	}
}

// TestWriteAtLastByte tests writing to virtual_size - 1.
func TestWriteAtLastByte(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "last_byte.qcow2")

	virtualSize := int64(10 * 1024 * 1024)
	img, err := Create(path, CreateOptions{
		Size:        uint64(virtualSize),
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Write single byte at last position
	lastOffset := virtualSize - 1
	data := []byte{0xFF}

	n, err := img.WriteAt(data, lastOffset)
	if err != nil {
		t.Fatalf("WriteAt last byte failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("WriteAt: wrote %d bytes, expected 1", n)
	}

	// Verify
	buf := make([]byte, 1)
	if _, err := img.ReadAt(buf, lastOffset); err != nil {
		t.Fatalf("ReadAt last byte failed: %v", err)
	}
	if buf[0] != 0xFF {
		t.Errorf("Last byte mismatch: expected 0xFF, got 0x%02X", buf[0])
	}

	// Write larger buffer ending at last byte
	offset := virtualSize - 4096
	data2 := make([]byte, 4096)
	for i := range data2 {
		data2[i] = byte(i % 256)
	}

	if _, err := img.WriteAt(data2, offset); err != nil {
		t.Fatalf("WriteAt ending at last byte failed: %v", err)
	}

	buf2 := make([]byte, 4096)
	if _, err := img.ReadAt(buf2, offset); err != nil {
		t.Fatalf("ReadAt ending at last byte failed: %v", err)
	}
	if !bytes.Equal(buf2, data2) {
		t.Error("Data mismatch for write ending at last byte")
	}
}

// TestWriteBeyondVirtualSize tests that write beyond virtual size fails gracefully.
func TestWriteBeyondVirtualSize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "beyond_size.qcow2")

	virtualSize := int64(10 * 1024 * 1024)
	img, err := Create(path, CreateOptions{
		Size:        uint64(virtualSize),
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Write starting beyond virtual size
	data := []byte("test data")
	_, err = img.WriteAt(data, virtualSize)
	if err == nil {
		t.Error("WriteAt beyond virtual size should fail")
	} else {
		t.Logf("WriteAt beyond size correctly failed: %v", err)
	}

	// Write that would extend beyond virtual size
	// Some implementations truncate to virtual size, others fail
	largeData := make([]byte, 8192)
	n, err := img.WriteAt(largeData, virtualSize-4096)
	if err != nil {
		t.Logf("WriteAt extending beyond correctly failed: %v", err)
	} else if n == 4096 {
		t.Logf("WriteAt extending beyond was truncated to %d bytes (acceptable)", n)
	} else if n == 8192 {
		t.Error("WriteAt extending beyond virtual size should not succeed with full write")
	}

	// Write at very large offset
	_, err = img.WriteAt(data, virtualSize*100)
	if err == nil {
		t.Error("WriteAt at huge offset should fail")
	}
}

// TestReadAtLastByte tests reading the last byte of image.
func TestReadAtLastByte(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "read_last_byte.qcow2")

	virtualSize := int64(10 * 1024 * 1024)
	img, err := Create(path, CreateOptions{
		Size:        uint64(virtualSize),
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Write a known value at the last byte
	lastOffset := virtualSize - 1
	if _, err := img.WriteAt([]byte{0xAB}, lastOffset); err != nil {
		t.Fatalf("WriteAt last byte failed: %v", err)
	}

	// Read single byte at last position
	buf := make([]byte, 1)
	n, err := img.ReadAt(buf, lastOffset)
	if err != nil {
		t.Fatalf("ReadAt last byte failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("ReadAt: read %d bytes, expected 1", n)
	}
	if buf[0] != 0xAB {
		t.Errorf("Last byte mismatch: expected 0xAB, got 0x%02X", buf[0])
	}

	// Read buffer that ends at last byte
	offset := virtualSize - 4096
	// First write known data there
	writeData := make([]byte, 4096)
	for i := range writeData {
		writeData[i] = byte(i % 256)
	}
	if _, err := img.WriteAt(writeData, offset); err != nil {
		t.Fatalf("WriteAt for read test failed: %v", err)
	}

	buf2 := make([]byte, 4096)
	if _, err := img.ReadAt(buf2, offset); err != nil {
		t.Fatalf("ReadAt ending at last byte failed: %v", err)
	}
	if !bytes.Equal(buf2, writeData) {
		t.Error("Data mismatch for read ending at last byte")
	}
}

// TestZeroLengthWrite tests WriteAt with empty buffer.
func TestZeroLengthWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "zero_write.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Zero-length write should succeed with 0 bytes written
	data := []byte{}
	n, err := img.WriteAt(data, 0)
	if err != nil {
		t.Errorf("Zero-length WriteAt failed: %v", err)
	}
	if n != 0 {
		t.Errorf("Zero-length WriteAt: wrote %d bytes, expected 0", n)
	}

	// Zero-length write at various offsets
	offsets := []int64{0, 4096, 65536, 1024 * 1024}
	for _, off := range offsets {
		n, err := img.WriteAt(data, off)
		if err != nil {
			t.Errorf("Zero-length WriteAt at %d failed: %v", off, err)
		}
		if n != 0 {
			t.Errorf("Zero-length WriteAt at %d: wrote %d bytes", off, n)
		}
	}
}

// TestZeroLengthRead tests ReadAt with empty buffer.
func TestZeroLengthRead(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "zero_read.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Zero-length read should succeed with 0 bytes read
	buf := []byte{}
	n, err := img.ReadAt(buf, 0)
	if err != nil {
		t.Errorf("Zero-length ReadAt failed: %v", err)
	}
	if n != 0 {
		t.Errorf("Zero-length ReadAt: read %d bytes, expected 0", n)
	}

	// Zero-length read at various offsets
	offsets := []int64{0, 4096, 65536, 1024 * 1024}
	for _, off := range offsets {
		n, err := img.ReadAt(buf, off)
		if err != nil {
			t.Errorf("Zero-length ReadAt at %d failed: %v", off, err)
		}
		if n != 0 {
			t.Errorf("Zero-length ReadAt at %d: read %d bytes", off, n)
		}
	}
}

// TestNegativeOffset tests handling of negative offset.
func TestNegativeOffset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "negative_offset.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	data := []byte("test")
	buf := make([]byte, 4)

	// Negative offset for write
	_, err = img.WriteAt(data, -1)
	if err == nil {
		t.Error("WriteAt with negative offset should fail")
	} else {
		t.Logf("WriteAt negative offset correctly failed: %v", err)
	}

	// Negative offset for read
	_, err = img.ReadAt(buf, -1)
	if err == nil {
		t.Error("ReadAt with negative offset should fail")
	} else {
		t.Logf("ReadAt negative offset correctly failed: %v", err)
	}

	// Very negative offset
	_, err = img.WriteAt(data, -1000000)
	if err == nil {
		t.Error("WriteAt with very negative offset should fail")
	}
}

// TestMaxInt64Offset tests handling of very large offset.
func TestMaxInt64Offset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "max_offset.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	data := []byte("test")
	buf := make([]byte, 4)

	// Max int64 offset
	maxOffset := int64(1<<63 - 1)

	_, err = img.WriteAt(data, maxOffset)
	if err == nil {
		t.Error("WriteAt with max int64 offset should fail")
	} else {
		t.Logf("WriteAt max offset correctly failed: %v", err)
	}

	_, err = img.ReadAt(buf, maxOffset)
	if err == nil {
		t.Error("ReadAt with max int64 offset should fail")
	} else {
		t.Logf("ReadAt max offset correctly failed: %v", err)
	}

	// Large but not max offset (beyond virtual size)
	largeOffset := int64(1 << 40) // 1TB
	_, err = img.WriteAt(data, largeOffset)
	if err == nil {
		t.Error("WriteAt with 1TB offset should fail for 10MB image")
	}
}

// TestBoundaryReadBeyondEOF tests reading with buffer that extends past EOF.
func TestBoundaryReadBeyondEOF(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "read_beyond_eof.qcow2")

	virtualSize := int64(10 * 1024 * 1024)
	img, err := Create(path, CreateOptions{
		Size:        uint64(virtualSize),
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Write some data near the end
	offset := virtualSize - 4096
	writeData := make([]byte, 4096)
	for i := range writeData {
		writeData[i] = 0xAA
	}
	if _, err := img.WriteAt(writeData, offset); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Try to read buffer larger than remaining space
	buf := make([]byte, 8192)
	n, err := img.ReadAt(buf, offset)

	// Should either:
	// 1. Return io.EOF with partial read
	// 2. Return error
	if err == nil {
		// If no error, should have read only 4096 bytes
		if n != 4096 {
			t.Errorf("ReadAt beyond EOF: read %d bytes, expected 4096", n)
		}
	} else if err == io.EOF {
		// EOF is acceptable, but should still have read 4096 bytes
		if n != 4096 {
			t.Errorf("ReadAt beyond EOF with io.EOF: read %d bytes, expected 4096", n)
		}
		// Verify the data that was read
		if !bytes.Equal(buf[:n], writeData) {
			t.Error("Data mismatch in partial read")
		}
	} else {
		t.Logf("ReadAt beyond EOF failed: %v (n=%d)", err, n)
	}

	// Read starting exactly at virtual size
	_, err = img.ReadAt(buf, virtualSize)
	if err == nil {
		t.Error("ReadAt starting at virtual size should fail or return EOF")
	}
}

// TestAlignedVsUnalignedWrites tests performance and correctness of aligned vs unaligned writes.
func TestAlignedVsUnalignedWrites(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "aligned_test.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	clusterSize := int64(img.ClusterSize())

	// Aligned write (full cluster)
	alignedData := make([]byte, clusterSize)
	for i := range alignedData {
		alignedData[i] = 0xAA
	}
	if _, err := img.WriteAt(alignedData, 0); err != nil {
		t.Fatalf("Aligned write failed: %v", err)
	}

	// Unaligned write (partial cluster, odd offset)
	unalignedData := make([]byte, 1337)
	for i := range unalignedData {
		unalignedData[i] = 0xBB
	}
	oddOffset := int64(12345)
	if _, err := img.WriteAt(unalignedData, oddOffset); err != nil {
		t.Fatalf("Unaligned write failed: %v", err)
	}

	// Verify both
	buf := make([]byte, clusterSize)
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("Read aligned failed: %v", err)
	}
	// First part should be 0xAA, then 0xBB where we wrote, then 0xAA again
	for i := int64(0); i < oddOffset; i++ {
		if buf[i] != 0xAA {
			t.Errorf("Byte %d: expected 0xAA, got 0x%02X", i, buf[i])
			break
		}
	}
	for i := int64(0); i < 1337; i++ {
		if buf[oddOffset+i] != 0xBB {
			t.Errorf("Byte %d: expected 0xBB, got 0x%02X", oddOffset+i, buf[oddOffset+i])
			break
		}
	}

	// Verify unaligned data separately
	buf2 := make([]byte, 1337)
	if _, err := img.ReadAt(buf2, oddOffset); err != nil {
		t.Fatalf("Read unaligned failed: %v", err)
	}
	if !bytes.Equal(buf2, unalignedData) {
		t.Error("Unaligned data mismatch")
	}
}
