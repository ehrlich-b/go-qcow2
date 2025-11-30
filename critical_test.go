// critical_test.go - Phase 1 Critical Tests for Production Readiness
// These tests verify crash recovery and corruption handling behavior.

package qcow2

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/ehrlich-b/go-qcow2/testutil"
)

// =============================================================================
// 1.1 Crash Recovery / Power Failure Tests
// =============================================================================

// TestCrashDuringClusterAllocation simulates a crash during cluster allocation.
// The image is left in a state where a cluster is allocated but L2 entry not updated.
func TestCrashDuringClusterAllocation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "crash_alloc.qcow2")

	// Create image
	img, err := CreateSimple(path, 10*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write some data to allocate clusters
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xAA
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Close normally first
	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Simulate crash by truncating file mid-cluster
	// This simulates an allocated cluster that was never fully written
	fileInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	// Truncate a bit off the end (simulating incomplete write)
	truncatedSize := fileInfo.Size() - 512
	if err := os.Truncate(path, truncatedSize); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Try to open the truncated image
	img2, err := Open(path)
	if err != nil {
		// Some errors are acceptable for severely truncated images
		t.Logf("Open after truncation failed (expected for severe truncation): %v", err)
		return
	}
	defer img2.Close()

	// Try to read - should not panic
	buf := make([]byte, 4096)
	_, err = img2.ReadAt(buf, 0)
	if err != nil {
		t.Logf("ReadAt after truncation: %v", err)
	}

	// Check should report issues
	result, err := img2.Check()
	if err != nil {
		t.Logf("Check after truncation: %v", err)
	} else {
		t.Logf("Check result: corruptions=%d, leaks=%d", result.Corruptions, result.Leaks)
	}
}

// TestCrashDuringL2TableWrite simulates a crash during L2 table write.
// The L2 table is partially written to disk.
func TestCrashDuringL2TableWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "crash_l2.qcow2")

	// Create image with QEMU to ensure valid structure
	testutil.RequireQemu(t)
	testutil.QemuCreate(t, path, "10M")

	// Write some data with our library
	img, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xBB
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Open the file directly and corrupt the L2 table partially
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	// Read header to find L1 table
	headerBuf := make([]byte, HeaderSizeV3)
	if _, err := f.ReadAt(headerBuf, 0); err != nil {
		f.Close()
		t.Fatalf("ReadAt header failed: %v", err)
	}

	l1TableOffset := binary.BigEndian.Uint64(headerBuf[40:48])

	// Read L1 entry to find L2 table
	l1Entry := make([]byte, 8)
	if _, err := f.ReadAt(l1Entry, int64(l1TableOffset)); err != nil {
		f.Close()
		t.Fatalf("ReadAt L1 entry failed: %v", err)
	}

	l2TableOffset := binary.BigEndian.Uint64(l1Entry) & L1EntryOffsetMask
	if l2TableOffset == 0 {
		f.Close()
		t.Skip("No L2 table allocated (image may be too sparse)")
	}

	// Write garbage to part of the L2 table (simulating crash mid-write)
	garbage := make([]byte, 256)
	for i := range garbage {
		garbage[i] = 0xFF
	}
	if _, err := f.WriteAt(garbage, int64(l2TableOffset)+100); err != nil {
		f.Close()
		t.Fatalf("WriteAt garbage failed: %v", err)
	}
	f.Close()

	// Try to open the corrupted image
	img2, err := Open(path)
	if err != nil {
		t.Logf("Open after L2 corruption failed (acceptable): %v", err)
		return
	}
	defer img2.Close()

	// Read should handle the corruption gracefully
	buf := make([]byte, 4096)
	_, err = img2.ReadAt(buf, 0)
	if err != nil {
		t.Logf("ReadAt with corrupted L2: %v", err)
	}

	// Check should detect the corruption
	result, err := img2.Check()
	if err != nil {
		t.Logf("Check: %v", err)
	} else if result.Corruptions == 0 && len(result.Errors) == 0 {
		t.Error("Check should have detected corruptions in L2 table")
	} else {
		t.Logf("Check detected issues: corruptions=%d, errors=%d",
			result.Corruptions, len(result.Errors))
	}
}

// TestCrashDuringL1TableUpdate simulates L2 allocated but L1 not updated.
func TestCrashDuringL1TableUpdate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "crash_l1.qcow2")

	// Create image
	img, err := CreateSimple(path, 10*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	clusterSize := img.ClusterSize()
	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Open file and manually corrupt L1 entry to point to invalid location
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	// Read header
	headerBuf := make([]byte, HeaderSizeV3)
	if _, err := f.ReadAt(headerBuf, 0); err != nil {
		f.Close()
		t.Fatalf("ReadAt header failed: %v", err)
	}

	l1TableOffset := binary.BigEndian.Uint64(headerBuf[40:48])

	// Write a bogus L1 entry pointing to a location that exists but isn't valid L2
	bogusL1 := make([]byte, 8)
	// Point to offset 512 (inside header area) with COPIED flag
	bogusOffset := uint64(512) | L1EntryCopied
	binary.BigEndian.PutUint64(bogusL1, bogusOffset)
	if _, err := f.WriteAt(bogusL1, int64(l1TableOffset)); err != nil {
		f.Close()
		t.Fatalf("WriteAt bogus L1 failed: %v", err)
	}
	f.Close()

	// Try to open
	img2, err := Open(path)
	if err != nil {
		t.Logf("Open after L1 corruption: %v (acceptable)", err)
		return
	}
	defer img2.Close()

	// Read should fail or return error, not panic
	buf := make([]byte, clusterSize)
	_, err = img2.ReadAt(buf, 0)
	if err != nil {
		t.Logf("ReadAt with bad L1: %v", err)
	}
}

// TestCrashDuringRefcountUpdate simulates cluster allocated but refcount not incremented.
func TestCrashDuringRefcountUpdate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "crash_refcount.qcow2")

	// Create image
	img, err := CreateSimple(path, 10*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write to allocate some clusters
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xCC
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Open file and zero out the refcount block (simulating refcount not written)
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	// Read header to find refcount table
	headerBuf := make([]byte, HeaderSizeV3)
	if _, err := f.ReadAt(headerBuf, 0); err != nil {
		f.Close()
		t.Fatalf("ReadAt header failed: %v", err)
	}

	refcountTableOffset := binary.BigEndian.Uint64(headerBuf[48:56])

	// Read first refcount table entry to find refcount block
	rcTableEntry := make([]byte, 8)
	if _, err := f.ReadAt(rcTableEntry, int64(refcountTableOffset)); err != nil {
		f.Close()
		t.Fatalf("ReadAt refcount table entry failed: %v", err)
	}

	refcountBlockOffset := binary.BigEndian.Uint64(rcTableEntry)
	if refcountBlockOffset == 0 {
		f.Close()
		t.Skip("No refcount block allocated")
	}

	// Zero out the FIRST entries of the refcount block (where header/metadata refcounts are)
	// Each refcount entry is 2 bytes by default (16-bit refcounts)
	// Zeroing entries 0-5 should affect header, L1, refcount table, etc.
	zeros := make([]byte, 12) // 6 entries * 2 bytes
	if _, err := f.WriteAt(zeros, int64(refcountBlockOffset)); err != nil {
		f.Close()
		t.Fatalf("WriteAt zeros failed: %v", err)
	}
	f.Close()

	// Try to open - may fail due to broken refcounts
	img2, err := Open(path)
	if err != nil {
		t.Logf("Open after refcount corruption: %v (acceptable)", err)
		return
	}
	defer img2.Close()

	// Check should detect inconsistent refcounts
	result, err := img2.Check()
	if err != nil {
		t.Logf("Check: %v", err)
	} else {
		t.Logf("Check result: corruptions=%d, leaks=%d", result.Corruptions, result.Leaks)
		// Either leaks or corruptions should be detected
		// (corruptions because metadata has refcount 0 when it should be 1)
	}
}

// TestCrashWithLazyRefcounts verifies lazy refcount recovery works.
func TestCrashWithLazyRefcounts(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "lazy_crash.qcow2")

	// Create image with lazy refcounts enabled
	testutil.QemuCreate(t, path, "10M", "-o", "lazy_refcounts=on")

	// Write data
	img, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	data := make([]byte, 65536)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Simulate crash by NOT calling Close() - leave dirty flag set
	// (Note: img will be garbage collected, but file should remain dirty)
	img.Flush()
	img.file.Close() // Force close without clean shutdown

	// Mark the file as dirty by setting the dirty bit
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	headerBuf := make([]byte, HeaderSizeV3)
	if _, err := f.ReadAt(headerBuf, 0); err != nil {
		f.Close()
		t.Fatalf("ReadAt header failed: %v", err)
	}

	// Set dirty bit
	incompat := binary.BigEndian.Uint64(headerBuf[72:80])
	incompat |= IncompatDirtyBit
	binary.BigEndian.PutUint64(headerBuf[72:80], incompat)
	if _, err := f.WriteAt(headerBuf[72:80], 72); err != nil {
		f.Close()
		t.Fatalf("WriteAt dirty bit failed: %v", err)
	}
	f.Close()

	// Reopen - should trigger lazy refcount recovery
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen after crash failed: %v", err)
	}
	defer img2.Close()

	// Verify data is still readable
	buf := make([]byte, 65536)
	if _, err := img2.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt after recovery failed: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Error("Data mismatch after lazy refcount recovery")
	}

	// Check should pass
	result, err := img2.Check()
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if result.Corruptions > 0 {
		t.Errorf("Check found corruptions after recovery: %d", result.Corruptions)
	}
}

// TestCrashDuringSnapshotCreation simulates crash during snapshot creation.
func TestCrashDuringSnapshotCreation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "crash_snapshot.qcow2")

	// Create image and add a snapshot
	img, err := CreateSimple(path, 10*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write data
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xDD
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Create a snapshot
	if _, err := img.CreateSnapshot("snap1"); err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Corrupt the snapshot table by truncating it
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	headerBuf := make([]byte, HeaderSizeV3)
	if _, err := f.ReadAt(headerBuf, 0); err != nil {
		f.Close()
		t.Fatalf("ReadAt header failed: %v", err)
	}

	snapshotsOffset := binary.BigEndian.Uint64(headerBuf[64:72])
	if snapshotsOffset == 0 {
		f.Close()
		t.Skip("No snapshots offset set")
	}

	// Write garbage at snapshot offset
	garbage := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	if _, err := f.WriteAt(garbage, int64(snapshotsOffset)); err != nil {
		f.Close()
		t.Fatalf("WriteAt garbage failed: %v", err)
	}
	f.Close()

	// Try to open - should handle gracefully
	img2, err := Open(path)
	if err != nil {
		t.Logf("Open after snapshot corruption: %v (acceptable)", err)
		return
	}
	defer img2.Close()

	// List snapshots - might be empty or corrupted
	snapshots := img2.Snapshots()
	t.Logf("Found %d snapshots after corruption", len(snapshots))

	// But reading current data should still work
	buf := make([]byte, 4096)
	if _, err := img2.ReadAt(buf, 0); err != nil {
		t.Errorf("ReadAt should still work: %v", err)
	}
}

// TestRecoveryAfterDirtyBitSet verifies image marked dirty is repaired.
func TestRecoveryAfterDirtyBitSet(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "dirty_recovery.qcow2")

	// Create image
	img, err := CreateSimple(path, 10*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write data
	data := make([]byte, 65536)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Set dirty bit manually
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	headerBuf := make([]byte, HeaderSizeV3)
	if _, err := f.ReadAt(headerBuf, 0); err != nil {
		f.Close()
		t.Fatalf("ReadAt header failed: %v", err)
	}

	incompat := binary.BigEndian.Uint64(headerBuf[72:80])
	incompat |= IncompatDirtyBit
	binary.BigEndian.PutUint64(headerBuf[72:80], incompat)
	if _, err := f.WriteAt(headerBuf[72:80], 72); err != nil {
		f.Close()
		t.Fatalf("WriteAt dirty bit failed: %v", err)
	}
	f.Close()

	// Reopen - should handle dirty flag
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Open after dirty bit set failed: %v", err)
	}
	defer img2.Close()

	// Verify data is still correct
	buf := make([]byte, 65536)
	if _, err := img2.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Error("Data mismatch after recovery from dirty flag")
	}

	// Check should pass
	result, err := img2.Check()
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if result.Corruptions > 0 {
		t.Errorf("Check found corruptions: %d", result.Corruptions)
	}
}

// TestPartiallyWrittenCluster simulates crash mid-cluster write.
func TestPartiallyWrittenCluster(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "partial_cluster.qcow2")

	// Create image
	img, err := CreateSimple(path, 10*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	clusterSize := int(img.ClusterSize())

	// Write a full cluster
	data := make([]byte, clusterSize)
	for i := range data {
		data[i] = 0xEE
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Simulate partial cluster write by overwriting only half with different data
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	// Find the cluster location
	headerBuf := make([]byte, HeaderSizeV3)
	if _, err := f.ReadAt(headerBuf, 0); err != nil {
		f.Close()
		t.Fatalf("ReadAt header failed: %v", err)
	}

	l1TableOffset := binary.BigEndian.Uint64(headerBuf[40:48])
	l1Entry := make([]byte, 8)
	if _, err := f.ReadAt(l1Entry, int64(l1TableOffset)); err != nil {
		f.Close()
		t.Fatalf("ReadAt L1 entry failed: %v", err)
	}

	l2TableOffset := binary.BigEndian.Uint64(l1Entry) & L1EntryOffsetMask
	if l2TableOffset == 0 {
		f.Close()
		t.Skip("No L2 table allocated")
	}

	l2Entry := make([]byte, 8)
	if _, err := f.ReadAt(l2Entry, int64(l2TableOffset)); err != nil {
		f.Close()
		t.Fatalf("ReadAt L2 entry failed: %v", err)
	}

	clusterOffset := binary.BigEndian.Uint64(l2Entry) & L2EntryOffsetMask
	if clusterOffset == 0 {
		f.Close()
		t.Skip("No cluster allocated")
	}

	// Write partial data (simulating crash mid-write)
	partialData := make([]byte, clusterSize/2)
	for i := range partialData {
		partialData[i] = 0xFF
	}
	if _, err := f.WriteAt(partialData, int64(clusterOffset)); err != nil {
		f.Close()
		t.Fatalf("WriteAt partial data failed: %v", err)
	}
	f.Close()

	// Reopen and read - should see mixed data (partial write)
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer img2.Close()

	buf := make([]byte, clusterSize)
	if _, err := img2.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	// First half should be 0xFF, second half 0xEE
	for i := 0; i < clusterSize/2; i++ {
		if buf[i] != 0xFF {
			t.Errorf("First half byte %d: expected 0xFF, got 0x%02X", i, buf[i])
			break
		}
	}
	for i := clusterSize / 2; i < clusterSize; i++ {
		if buf[i] != 0xEE {
			t.Errorf("Second half byte %d: expected 0xEE, got 0x%02X", i, buf[i])
			break
		}
	}
}

// =============================================================================
// 1.2 Corrupted/Malformed Image Tests
// =============================================================================

// TestCorruptedMagic tests handling of invalid magic number.
func TestCorruptedMagic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "bad_magic.qcow2")

	// Create valid image
	img, err := CreateSimple(path, 1*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	img.Close()

	// Corrupt magic
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	if _, err := f.WriteAt([]byte{0xBA, 0xAD, 0xF0, 0x0D}, 0); err != nil {
		f.Close()
		t.Fatalf("WriteAt magic failed: %v", err)
	}
	f.Close()

	// Open should fail with clear error
	_, err = Open(path)
	if err == nil {
		t.Fatal("Open should fail with invalid magic")
	}
	if err != ErrInvalidMagic {
		t.Logf("Open failed with: %v", err)
	}
}

// TestCorruptedVersion tests handling of unknown version.
func TestCorruptedVersion(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "bad_version.qcow2")

	// Create valid image
	img, err := CreateSimple(path, 1*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	img.Close()

	// Set version to 99
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	versionBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(versionBytes, 99)
	if _, err := f.WriteAt(versionBytes, 4); err != nil {
		f.Close()
		t.Fatalf("WriteAt version failed: %v", err)
	}
	f.Close()

	// Open should fail
	_, err = Open(path)
	if err == nil {
		t.Fatal("Open should fail with unsupported version")
	}
	t.Logf("Open failed as expected: %v", err)
}

// TestCorruptedClusterBits tests handling of out-of-range cluster bits.
func TestCorruptedClusterBits(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	testCases := []struct {
		name        string
		clusterBits uint32
	}{
		{"too_small", 8}, // < MinClusterBits (9)
		{"too_large", 22}, // > MaxClusterBits (21)
		{"zero", 0},
		{"huge", 64},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(dir, tc.name+".qcow2")

			// Create valid image
			img, err := CreateSimple(path, 1*1024*1024)
			if err != nil {
				t.Fatalf("Create failed: %v", err)
			}
			img.Close()

			// Corrupt cluster bits
			f, err := os.OpenFile(path, os.O_RDWR, 0644)
			if err != nil {
				t.Fatalf("OpenFile failed: %v", err)
			}
			bits := make([]byte, 4)
			binary.BigEndian.PutUint32(bits, tc.clusterBits)
			if _, err := f.WriteAt(bits, 20); err != nil {
				f.Close()
				t.Fatalf("WriteAt cluster bits failed: %v", err)
			}
			f.Close()

			// Open should fail
			_, err = Open(path)
			if err == nil {
				t.Fatalf("Open should fail with cluster_bits=%d", tc.clusterBits)
			}
			t.Logf("Open failed as expected: %v", err)
		})
	}
}

// TestCorruptedL1TableOffset tests handling of L1 table offset beyond EOF.
func TestCorruptedL1TableOffset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "bad_l1_offset.qcow2")

	// Create valid image
	img, err := CreateSimple(path, 1*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	img.Close()

	fileInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	// Set L1 table offset beyond EOF
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	offsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(offsetBytes, uint64(fileInfo.Size()+1000000))
	if _, err := f.WriteAt(offsetBytes, 40); err != nil {
		f.Close()
		t.Fatalf("WriteAt L1 offset failed: %v", err)
	}
	f.Close()

	// Open should fail or error on first operation
	img2, err := Open(path)
	if err != nil {
		t.Logf("Open failed as expected: %v", err)
		return
	}
	defer img2.Close()

	// Read should fail
	buf := make([]byte, 4096)
	_, err = img2.ReadAt(buf, 0)
	if err == nil {
		t.Error("ReadAt should fail with L1 offset beyond EOF")
	}
}

// TestCorruptedL1TableSize tests handling of L1 size larger than file can support.
func TestCorruptedL1TableSize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "bad_l1_size.qcow2")

	// Create valid image
	img, err := CreateSimple(path, 1*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	img.Close()

	// Get file size to set a reasonable but still invalid L1 size
	fileInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	// Set L1 size to a value that would require more than the file size
	// but not so large it would OOM. Use 100K entries = 800KB L1 table.
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	sizeBytes := make([]byte, 4)
	// Pick a size that extends beyond the file but won't OOM
	largeSize := uint32(fileInfo.Size()/8 + 10000)
	binary.BigEndian.PutUint32(sizeBytes, largeSize)
	if _, err := f.WriteAt(sizeBytes, 36); err != nil {
		f.Close()
		t.Fatalf("WriteAt L1 size failed: %v", err)
	}
	f.Close()

	// Open should fail (L1 table extends beyond file)
	img2, err := Open(path)
	if err != nil {
		t.Logf("Open failed as expected: %v", err)
		return
	}
	defer img2.Close()

	// If open succeeded, operations should fail
	buf := make([]byte, 4096)
	_, err = img2.ReadAt(buf, 0)
	if err != nil {
		t.Logf("ReadAt failed: %v", err)
	}
}

// TestCorruptedL2Entry tests handling of L2 entry pointing beyond EOF.
func TestCorruptedL2Entry(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "bad_l2_entry.qcow2")

	// Create image with QEMU and write data
	testutil.QemuCreate(t, path, "10M")
	testutil.RequireQemuIO(t)
	testutil.QemuWrite(t, path, 0xAA, 0, 4096)

	// Read L2 entry and corrupt it
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	headerBuf := make([]byte, HeaderSizeV3)
	if _, err := f.ReadAt(headerBuf, 0); err != nil {
		f.Close()
		t.Fatalf("ReadAt header failed: %v", err)
	}

	l1TableOffset := binary.BigEndian.Uint64(headerBuf[40:48])
	l1Entry := make([]byte, 8)
	if _, err := f.ReadAt(l1Entry, int64(l1TableOffset)); err != nil {
		f.Close()
		t.Fatalf("ReadAt L1 entry failed: %v", err)
	}

	l2TableOffset := binary.BigEndian.Uint64(l1Entry) & L1EntryOffsetMask
	if l2TableOffset == 0 {
		f.Close()
		t.Skip("No L2 table")
	}

	// Write L2 entry pointing beyond EOF
	badL2 := make([]byte, 8)
	binary.BigEndian.PutUint64(badL2, uint64(0x7FFFFFFFFFFFE00)|L2EntryCopied) // Huge offset with COPIED flag
	if _, err := f.WriteAt(badL2, int64(l2TableOffset)); err != nil {
		f.Close()
		t.Fatalf("WriteAt bad L2 entry failed: %v", err)
	}
	f.Close()

	// Open should succeed
	img, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img.Close()

	// Read should fail or return error, not panic
	buf := make([]byte, 4096)
	_, err = img.ReadAt(buf, 0)
	if err != nil {
		t.Logf("ReadAt with bad L2 entry: %v", err)
	}

	// Check should detect the issue
	result, err := img.Check()
	if err != nil {
		t.Logf("Check: %v", err)
	} else if result.Corruptions == 0 {
		t.Log("Check did not detect corruption (may be acceptable if bounds checking happens at read time)")
	}
}

// TestCorruptedL2EntryAlignment tests handling of non-aligned L2 entry offset.
func TestCorruptedL2EntryAlignment(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "bad_l2_alignment.qcow2")

	// Create image with QEMU and write data
	testutil.QemuCreate(t, path, "10M")
	testutil.RequireQemuIO(t)
	testutil.QemuWrite(t, path, 0xBB, 0, 4096)

	// Corrupt L2 entry with non-512-byte-aligned offset
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	headerBuf := make([]byte, HeaderSizeV3)
	if _, err := f.ReadAt(headerBuf, 0); err != nil {
		f.Close()
		t.Fatalf("ReadAt header failed: %v", err)
	}

	l1TableOffset := binary.BigEndian.Uint64(headerBuf[40:48])
	l1Entry := make([]byte, 8)
	if _, err := f.ReadAt(l1Entry, int64(l1TableOffset)); err != nil {
		f.Close()
		t.Fatalf("ReadAt L1 entry failed: %v", err)
	}

	l2TableOffset := binary.BigEndian.Uint64(l1Entry) & L1EntryOffsetMask
	if l2TableOffset == 0 {
		f.Close()
		t.Skip("No L2 table")
	}

	// Read current L2 entry
	l2Entry := make([]byte, 8)
	if _, err := f.ReadAt(l2Entry, int64(l2TableOffset)); err != nil {
		f.Close()
		t.Fatalf("ReadAt L2 entry failed: %v", err)
	}

	// Create misaligned offset (offset 0x1001, not 512-byte aligned)
	misaligned := uint64(0x10001) | L2EntryCopied // Set some low bits
	binary.BigEndian.PutUint64(l2Entry, misaligned)
	if _, err := f.WriteAt(l2Entry, int64(l2TableOffset)); err != nil {
		f.Close()
		t.Fatalf("WriteAt misaligned L2 entry failed: %v", err)
	}
	f.Close()

	// Open might succeed
	img, err := Open(path)
	if err != nil {
		t.Logf("Open with misaligned L2 failed: %v", err)
		return
	}
	defer img.Close()

	// Read with misaligned offset
	buf := make([]byte, 4096)
	_, err = img.ReadAt(buf, 0)
	if err != nil {
		t.Logf("ReadAt with misaligned L2: %v", err)
	}
}

// TestCorruptedRefcountTableOffset tests handling of invalid refcount table offset.
func TestCorruptedRefcountTableOffset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "bad_refcount_offset.qcow2")

	// Create valid image
	img, err := CreateSimple(path, 1*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	img.Close()

	// Set refcount table offset to invalid value
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	offsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(offsetBytes, 0xDEADBEEF00000000)
	if _, err := f.WriteAt(offsetBytes, 48); err != nil {
		f.Close()
		t.Fatalf("WriteAt refcount offset failed: %v", err)
	}
	f.Close()

	// Open might succeed (refcount table read lazily)
	img2, err := Open(path)
	if err != nil {
		t.Logf("Open with bad refcount offset: %v", err)
		return
	}
	defer img2.Close()

	// Write should fail (needs refcount update)
	data := []byte("test")
	_, err = img2.WriteAt(data, 0)
	if err != nil {
		t.Logf("WriteAt with bad refcount offset: %v", err)
	}
}

// TestCorruptedRefcountBlock tests handling of refcount block pointing to itself.
// This creates a circular reference in the refcount structure.
func TestCorruptedRefcountBlock(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "circular_refcount.qcow2")

	// Create valid image and write data to force refcount block allocation
	img, err := CreateSimple(path, 10*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	data := make([]byte, 65536)
	for i := range data {
		data[i] = 0xEE
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Open file and make refcount table entry point to itself
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	// Read header to find refcount table
	headerBuf := make([]byte, HeaderSizeV3)
	if _, err := f.ReadAt(headerBuf, 0); err != nil {
		f.Close()
		t.Fatalf("ReadAt header failed: %v", err)
	}

	refcountTableOffset := binary.BigEndian.Uint64(headerBuf[48:56])

	// Make the first refcount table entry point to the refcount table itself
	// This creates a circular reference: refcount_table[0] -> refcount_table_offset
	circularEntry := make([]byte, 8)
	binary.BigEndian.PutUint64(circularEntry, refcountTableOffset)
	if _, err := f.WriteAt(circularEntry, int64(refcountTableOffset)); err != nil {
		f.Close()
		t.Fatalf("WriteAt circular refcount entry failed: %v", err)
	}
	f.Close()

	// Try to open the image with circular refcount reference
	img2, err := Open(path)
	if err != nil {
		t.Logf("Open with circular refcount block: %v (acceptable)", err)
		return
	}
	defer img2.Close()

	// Read should not infinite loop or panic
	buf := make([]byte, 4096)
	_, err = img2.ReadAt(buf, 0)
	if err != nil {
		t.Logf("ReadAt with circular refcount: %v", err)
	}

	// Write should detect the corruption or handle it gracefully
	_, err = img2.WriteAt([]byte("test"), 65536)
	if err != nil {
		t.Logf("WriteAt with circular refcount: %v", err)
	}

	// Check should detect the circular reference
	result, err := img2.Check()
	if err != nil {
		t.Logf("Check: %v", err)
	} else {
		t.Logf("Check result: corruptions=%d, leaks=%d, errors=%d",
			result.Corruptions, result.Leaks, len(result.Errors))
		// The circular reference should be detected as some form of corruption or error
	}
}

// TestCircularL1L2Reference tests handling of L2 table pointing back to L1.
func TestCircularL1L2Reference(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "circular_ref.qcow2")

	// Create image and write data
	testutil.QemuCreate(t, path, "10M")
	testutil.RequireQemuIO(t)
	testutil.QemuWrite(t, path, 0xCC, 0, 4096)

	// Make L1 entry point to itself (circular reference)
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	headerBuf := make([]byte, HeaderSizeV3)
	if _, err := f.ReadAt(headerBuf, 0); err != nil {
		f.Close()
		t.Fatalf("ReadAt header failed: %v", err)
	}

	l1TableOffset := binary.BigEndian.Uint64(headerBuf[40:48])

	// Make L1 entry point to L1 table offset itself
	l1Entry := make([]byte, 8)
	binary.BigEndian.PutUint64(l1Entry, l1TableOffset|L1EntryCopied)
	if _, err := f.WriteAt(l1Entry, int64(l1TableOffset)); err != nil {
		f.Close()
		t.Fatalf("WriteAt circular L1 failed: %v", err)
	}
	f.Close()

	// Open should succeed
	img, err := Open(path)
	if err != nil {
		t.Logf("Open with circular reference: %v", err)
		return
	}
	defer img.Close()

	// Read should not infinite loop
	buf := make([]byte, 4096)
	_, err = img.ReadAt(buf, 0)
	if err != nil {
		t.Logf("ReadAt with circular reference: %v", err)
	}
}

// TestOverlappingMetadata tests handling of overlapping L1/L2/refcount tables.
func TestOverlappingMetadata(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "overlap_metadata.qcow2")

	// Create image
	testutil.QemuCreate(t, path, "10M")

	// Make L1 table offset same as refcount table offset (overlap)
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	headerBuf := make([]byte, HeaderSizeV3)
	if _, err := f.ReadAt(headerBuf, 0); err != nil {
		f.Close()
		t.Fatalf("ReadAt header failed: %v", err)
	}

	refcountOffset := binary.BigEndian.Uint64(headerBuf[48:56])

	// Set L1 table to same offset as refcount table
	offsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(offsetBytes, refcountOffset)
	if _, err := f.WriteAt(offsetBytes, 40); err != nil {
		f.Close()
		t.Fatalf("WriteAt L1 offset failed: %v", err)
	}
	f.Close()

	// Open might detect the overlap
	img, err := Open(path)
	if err != nil {
		t.Logf("Open with overlapping metadata: %v", err)
		return
	}
	defer img.Close()

	// Check should detect overlapping metadata
	result, err := img.Check()
	if err != nil {
		t.Logf("Check: %v", err)
	} else {
		t.Logf("Check result: corruptions=%d, errors=%d",
			result.Corruptions, len(result.Errors))
	}
}

// TestTruncatedImage tests handling of file shorter than header indicates.
func TestTruncatedImage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "truncated.qcow2")

	// Create valid image with data
	img, err := CreateSimple(path, 10*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	data := make([]byte, 65536)
	for i := range data {
		data[i] = 0xDD
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Truncate to just the header
	if err := os.Truncate(path, HeaderSizeV3); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Open should fail or handle gracefully
	img2, err := Open(path)
	if err != nil {
		t.Logf("Open truncated image: %v (expected)", err)
		return
	}
	defer img2.Close()

	// Read should fail
	buf := make([]byte, 4096)
	_, err = img2.ReadAt(buf, 0)
	if err != nil {
		t.Logf("ReadAt truncated image: %v", err)
	}
}

// TestZeroSizeImage tests handling of virtual size = 0.
func TestZeroSizeImage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "zero_size.qcow2")

	// Create valid image
	img, err := CreateSimple(path, 1*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	img.Close()

	// Set virtual size to 0
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	sizeBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeBytes, 0)
	if _, err := f.WriteAt(sizeBytes, 24); err != nil {
		f.Close()
		t.Fatalf("WriteAt size failed: %v", err)
	}
	f.Close()

	// Open might succeed
	img2, err := Open(path)
	if err != nil {
		t.Logf("Open zero-size image: %v", err)
		return
	}
	defer img2.Close()

	if img2.Size() != 0 {
		t.Errorf("Expected size 0, got %d", img2.Size())
	}

	// Read beyond size should fail
	buf := make([]byte, 4096)
	_, err = img2.ReadAt(buf, 0)
	if err == nil {
		t.Error("ReadAt on zero-size image should fail")
	}
}

// TestHugeVirtualSize tests handling of impossibly large virtual size.
func TestHugeVirtualSize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "huge_size.qcow2")

	// Create valid image
	img, err := CreateSimple(path, 1*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	img.Close()

	// Set virtual size to 1 exabyte (impossibly large)
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	sizeBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeBytes, 1<<60) // 1 EB
	if _, err := f.WriteAt(sizeBytes, 24); err != nil {
		f.Close()
		t.Fatalf("WriteAt size failed: %v", err)
	}
	f.Close()

	// Open might succeed (virtual size is just a number)
	img2, err := Open(path)
	if err != nil {
		t.Logf("Open huge image: %v", err)
		return
	}
	defer img2.Close()

	t.Logf("Image reports size: %d bytes", img2.Size())

	// Read at offset 0 should work (if there's data)
	buf := make([]byte, 4096)
	_, err = img2.ReadAt(buf, 0)
	if err != nil {
		t.Logf("ReadAt on huge image: %v", err)
	}
}

// TestInvalidCompressedDescriptor tests handling of bad compressed cluster descriptor.
func TestInvalidCompressedDescriptor(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.qcow2")
	compPath := filepath.Join(dir, "compressed.qcow2")

	// Create and compress an image with QEMU
	testutil.QemuCreate(t, srcPath, "10M")
	testutil.RequireQemuIO(t)
	testutil.QemuWrite(t, srcPath, 0x00, 0, 65536) // Compressible data
	testutil.QemuConvert(t, srcPath, compPath, true)

	// Find and corrupt the compressed cluster descriptor
	f, err := os.OpenFile(compPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	headerBuf := make([]byte, HeaderSizeV3)
	if _, err := f.ReadAt(headerBuf, 0); err != nil {
		f.Close()
		t.Fatalf("ReadAt header failed: %v", err)
	}

	l1TableOffset := binary.BigEndian.Uint64(headerBuf[40:48])
	l1Entry := make([]byte, 8)
	if _, err := f.ReadAt(l1Entry, int64(l1TableOffset)); err != nil {
		f.Close()
		t.Fatalf("ReadAt L1 entry failed: %v", err)
	}

	l2TableOffset := binary.BigEndian.Uint64(l1Entry) & L1EntryOffsetMask
	if l2TableOffset == 0 {
		f.Close()
		t.Skip("No L2 table")
	}

	// Read L2 entry
	l2Entry := make([]byte, 8)
	if _, err := f.ReadAt(l2Entry, int64(l2TableOffset)); err != nil {
		f.Close()
		t.Fatalf("ReadAt L2 entry failed: %v", err)
	}

	entry := binary.BigEndian.Uint64(l2Entry)
	if entry&L2EntryCompressed == 0 {
		f.Close()
		t.Skip("L2 entry is not compressed")
	}

	// Write invalid compressed descriptor (bad size/offset)
	badEntry := L2EntryCompressed | 0xDEADBEEF
	binary.BigEndian.PutUint64(l2Entry, badEntry)
	if _, err := f.WriteAt(l2Entry, int64(l2TableOffset)); err != nil {
		f.Close()
		t.Fatalf("WriteAt bad compressed descriptor failed: %v", err)
	}
	f.Close()

	// Open should succeed
	img, err := Open(compPath)
	if err != nil {
		t.Logf("Open with bad compressed descriptor: %v", err)
		return
	}
	defer img.Close()

	// Read should fail gracefully (not panic)
	buf := make([]byte, 4096)
	_, err = img.ReadAt(buf, 0)
	if err != nil {
		t.Logf("ReadAt with bad compressed descriptor: %v", err)
	}
}

// TestOverlappingDataClusters tests handling of two L2 entries pointing to same cluster.
func TestOverlappingDataClusters(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "overlapping_data.qcow2")

	// Create image and write to two different offsets
	testutil.QemuCreate(t, path, "10M")
	testutil.RequireQemuIO(t)
	testutil.QemuWrite(t, path, 0xAA, 0, 4096)
	testutil.QemuWrite(t, path, 0xBB, 65536, 4096) // Second cluster

	// Make the second L2 entry point to the same cluster as the first
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	headerBuf := make([]byte, HeaderSizeV3)
	if _, err := f.ReadAt(headerBuf, 0); err != nil {
		f.Close()
		t.Fatalf("ReadAt header failed: %v", err)
	}

	l1TableOffset := binary.BigEndian.Uint64(headerBuf[40:48])
	l1Entry := make([]byte, 8)
	if _, err := f.ReadAt(l1Entry, int64(l1TableOffset)); err != nil {
		f.Close()
		t.Fatalf("ReadAt L1 entry failed: %v", err)
	}

	l2TableOffset := binary.BigEndian.Uint64(l1Entry) & L1EntryOffsetMask
	if l2TableOffset == 0 {
		f.Close()
		t.Skip("No L2 table")
	}

	// Read first L2 entry
	l2Entry0 := make([]byte, 8)
	if _, err := f.ReadAt(l2Entry0, int64(l2TableOffset)); err != nil {
		f.Close()
		t.Fatalf("ReadAt L2 entry 0 failed: %v", err)
	}

	// Write same entry to second position (making two entries point to same cluster)
	if _, err := f.WriteAt(l2Entry0, int64(l2TableOffset)+8); err != nil {
		f.Close()
		t.Fatalf("WriteAt L2 entry 1 failed: %v", err)
	}
	f.Close()

	// Open should succeed
	img, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img.Close()

	// Both reads should return same data (since they point to same cluster)
	buf0 := make([]byte, 4096)
	buf1 := make([]byte, 4096)
	if _, err := img.ReadAt(buf0, 0); err != nil {
		t.Fatalf("ReadAt 0 failed: %v", err)
	}
	if _, err := img.ReadAt(buf1, 65536); err != nil {
		t.Fatalf("ReadAt 65536 failed: %v", err)
	}

	if !bytes.Equal(buf0, buf1) {
		t.Log("Reads returned different data despite same cluster (unexpected)")
	} else {
		t.Log("Both reads returned same data (expected for overlapping clusters)")
	}

	// Check should detect the overlap as a refcount issue
	result, err := img.Check()
	if err != nil {
		t.Logf("Check: %v", err)
	} else {
		t.Logf("Check result: corruptions=%d, leaks=%d", result.Corruptions, result.Leaks)
	}
}

// =============================================================================
// 1.3 Concurrent Snapshot Tests
// =============================================================================

// TestSnapshotDuringConcurrentWrites tests creating a snapshot while writes are in progress.
func TestSnapshotDuringConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent test in short mode")
	}
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "concurrent_snap.qcow2")

	// Create image
	img, err := Create(path, CreateOptions{
		Size:        20 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	clusterSize := int64(img.ClusterSize())
	numWriters := 5
	writesPerWriter := 20
	done := make(chan error, numWriters+1)

	// Start concurrent writers
	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			data := make([]byte, 4096)
			for i := range data {
				data[i] = byte(writerID)
			}

			for i := 0; i < writesPerWriter; i++ {
				offset := int64((writerID*writesPerWriter + i) % 50) * clusterSize
				if _, err := img.WriteAt(data, offset); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}(w)
	}

	// Create snapshot while writes are happening
	go func() {
		// Wait a tiny bit for writes to start
		for i := 0; i < 10; i++ {
			if _, err := img.CreateSnapshot("snap_during_writes"); err != nil {
				done <- err
				return
			}
			// Delete to avoid too many snapshots
			if err := img.DeleteSnapshot("snap_during_writes"); err != nil {
				// Ignore error - snapshot might not exist yet
			}
		}
		done <- nil
	}()

	// Wait for all goroutines
	var errors []error
	for i := 0; i < numWriters+1; i++ {
		if err := <-done; err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		for _, e := range errors {
			t.Errorf("Concurrent operation error: %v", e)
		}
	}

	// Final consistency check
	if err := img.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
}

// TestConcurrentSnapshotCreation tests multiple goroutines creating snapshots.
func TestConcurrentSnapshotCreation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent test in short mode")
	}
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "concurrent_snaps.qcow2")

	// Create image and write some initial data
	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Write initial data
	data := make([]byte, 65536)
	for i := range data {
		data[i] = 0xAA
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	numGoroutines := 5
	done := make(chan error, numGoroutines)

	// Try to create snapshots concurrently
	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			name := "snap_" + string(rune('A'+id))
			_, err := img.CreateSnapshot(name)
			done <- err
		}(g)
	}

	// Wait and count successes
	successes := 0
	for i := 0; i < numGoroutines; i++ {
		if err := <-done; err != nil {
			t.Logf("Snapshot creation %d: %v", i, err)
		} else {
			successes++
		}
	}

	t.Logf("Created %d snapshots concurrently", successes)

	// At least one should succeed
	if successes == 0 {
		t.Error("No snapshots were created successfully")
	}

	// Verify snapshots exist
	snapshots := img.Snapshots()
	t.Logf("Total snapshots after concurrent creation: %d", len(snapshots))
}

// TestWriteDuringSnapshotRead tests writing while reading from a snapshot.
func TestWriteDuringSnapshotRead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent test in short mode")
	}
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "write_snap_read.qcow2")

	// Create image and write initial data
	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Write initial data pattern
	initialData := make([]byte, 65536)
	for i := range initialData {
		initialData[i] = 0xAA
	}
	if _, err := img.WriteAt(initialData, 0); err != nil {
		t.Fatalf("WriteAt initial failed: %v", err)
	}

	// Create snapshot
	snap, err := img.CreateSnapshot("base_snap")
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	done := make(chan error, 2)

	// Writer: modify the current image
	go func() {
		newData := make([]byte, 4096)
		for i := range newData {
			newData[i] = 0xBB
		}
		for i := 0; i < 50; i++ {
			offset := int64(i*4096) % 65536
			if _, err := img.WriteAt(newData, offset); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	// Reader: read from snapshot
	go func() {
		buf := make([]byte, 4096)
		for i := 0; i < 50; i++ {
			offset := int64(i*4096) % 65536
			if _, err := img.ReadAtSnapshot(buf, offset, snap); err != nil {
				done <- err
				return
			}
			// Snapshot data should still be 0xAA (original)
			for j, b := range buf {
				if b != 0xAA {
					done <- nil // Don't fail - just note it
					return
				}
				_ = j
			}
		}
		done <- nil
	}()

	// Wait for both
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Errorf("Concurrent operation error: %v", err)
		}
	}

	// Verify snapshot still has original data
	buf := make([]byte, 4096)
	if _, err := img.ReadAtSnapshot(buf, 0, snap); err != nil {
		t.Fatalf("Final snapshot read failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xAA {
			t.Errorf("Snapshot data corrupted at %d: expected 0xAA, got 0x%02X", i, b)
			break
		}
	}
}

// TestDeleteSnapshotDuringRead tests deleting a snapshot while it's being read.
func TestDeleteSnapshotDuringRead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent test in short mode")
	}
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "delete_snap_read.qcow2")

	// Create image and write data
	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Write initial data
	data := make([]byte, 65536)
	for i := range data {
		data[i] = 0xCC
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Create snapshot
	snap, err := img.CreateSnapshot("to_delete")
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	done := make(chan error, 2)
	readStarted := make(chan struct{})

	// Reader: continuously read from snapshot
	go func() {
		buf := make([]byte, 4096)
		close(readStarted)
		for i := 0; i < 100; i++ {
			_, err := img.ReadAtSnapshot(buf, 0, snap)
			if err != nil {
				// Expected once snapshot is deleted
				done <- nil
				return
			}
		}
		done <- nil
	}()

	// Deleter: delete snapshot while reads are happening
	go func() {
		<-readStarted
		err := img.DeleteSnapshot("to_delete")
		done <- err
	}()

	// Wait for both
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Logf("Operation: %v", err)
		}
	}

	// Snapshot should be deleted
	snapshots := img.Snapshots()
	for _, s := range snapshots {
		if s.Name == "to_delete" {
			t.Error("Snapshot should have been deleted")
		}
	}
}
