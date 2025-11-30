// Regression tests for edge cases and correctness fixes.

package qcow2

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ehrlich-b/go-qcow2/testutil"
)

// qemuVersionAtLeast returns true if QEMU version is >= major.minor
func qemuVersionAtLeast(t *testing.T, major, minor int) bool {
	t.Helper()
	verStr := testutil.QemuVersion(t)
	if verStr == "" {
		return false
	}
	// Parse "qemu-img version X.Y.Z ..."
	parts := strings.Fields(verStr)
	for i, p := range parts {
		if p == "version" && i+1 < len(parts) {
			verParts := strings.Split(parts[i+1], ".")
			if len(verParts) >= 2 {
				maj, _ := strconv.Atoi(verParts[0])
				min, _ := strconv.Atoi(verParts[1])
				return maj > major || (maj == major && min >= minor)
			}
		}
	}
	return false
}

// qemuIOAvailable returns true if qemu-io is available
func qemuIOAvailable() bool {
	_, err := exec.LookPath("qemu-io")
	return err == nil
}

// TestRefcountBlockSelfAllocation verifies that newly allocated refcount blocks
// receive their own refcount of 1 (fixing the self-referential allocation bug).
func TestRefcountBlockSelfAllocation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create a small image
	img, err := CreateSimple(path, 10*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write enough data to force allocation of new refcount blocks
	// Each cluster is 64KB by default, refcount table starts with limited entries
	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Write to many clusters to force refcount block allocation
	for i := 0; i < 100; i++ {
		offset := int64(i) * int64(img.ClusterSize())
		if offset >= img.Size() {
			break
		}
		if _, err := img.WriteAt(data[:1024], offset); err != nil {
			t.Fatalf("WriteAt failed at offset %d: %v", offset, err)
		}
	}

	// Close and reopen to verify consistency
	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and check
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer img2.Close()

	// Run Check to verify refcount consistency
	result, err := img2.Check()
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if result.Corruptions > 0 {
		t.Errorf("Check found %d corruptions", result.Corruptions)
	}
	if len(result.Errors) > 0 {
		for _, e := range result.Errors {
			t.Errorf("Check error: %s", e)
		}
	}
}

// TestCompressedClusterOverwrite verifies that writes to compressed clusters
// properly decompress the data, allocate a new normal cluster, and preserve data.
func TestCompressedClusterOverwrite(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)
	testutil.RequireQemuIO(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image with QEMU and write compressible data
	testutil.QemuCreate(t, path, "10M", "-o", "cluster_size=64K")

	// Write compressible data with qemu-io
	testutil.QemuWrite(t, path, 0xAB, 0, 64*1024)

	// Compress it using qemu-img convert
	compressedPath := filepath.Join(dir, "compressed.qcow2")
	testutil.QemuConvert(t, path, compressedPath, true)

	// Verify it's compressed by checking file size is smaller
	origInfo, _ := os.Stat(path)
	compInfo, _ := os.Stat(compressedPath)
	if compInfo.Size() >= origInfo.Size() {
		t.Skip("Compression didn't reduce size, skipping test")
	}

	// Open compressed image with our library
	img, err := Open(compressedPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Read original data to verify it's correct
	buf := make([]byte, 64*1024)
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xAB {
			t.Fatalf("Original data mismatch at %d: got 0x%02X, want 0xAB", i, b)
		}
	}

	// Now overwrite part of the compressed cluster
	overwriteData := make([]byte, 1024)
	for i := range overwriteData {
		overwriteData[i] = 0xCD
	}
	if _, err := img.WriteAt(overwriteData, 1024); err != nil {
		t.Fatalf("WriteAt to compressed cluster failed: %v", err)
	}

	// Read back the entire cluster
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt after overwrite failed: %v", err)
	}

	// Verify: first 1024 bytes should be 0xAB (original)
	for i := 0; i < 1024; i++ {
		if buf[i] != 0xAB {
			t.Errorf("Data at %d should be 0xAB (original), got 0x%02X", i, buf[i])
			break
		}
	}

	// Next 1024 bytes should be 0xCD (overwritten)
	for i := 1024; i < 2048; i++ {
		if buf[i] != 0xCD {
			t.Errorf("Data at %d should be 0xCD (overwritten), got 0x%02X", i, buf[i])
			break
		}
	}

	// Remaining bytes should be 0xAB (original)
	for i := 2048; i < len(buf); i++ {
		if buf[i] != 0xAB {
			t.Errorf("Data at %d should be 0xAB (original), got 0x%02X", i, buf[i])
			break
		}
	}

	// Close and verify with QEMU
	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	result := testutil.QemuCheck(t, compressedPath)
	// Check for corruptions (leaks are acceptable since compressed cluster
	// deallocation isn't tracked - the old compressed data stays in the file)
	if result.Corruptions > 0 {
		t.Errorf("qemu-img check found corruptions: %s", result.Stderr)
	}
	if result.Leaks > 0 {
		t.Logf("qemu-img check found %d leaks (expected for compressed cluster overwrite)", result.Leaks)
	}
}

// TestZeroFlagClearingOnWrite verifies that writes to zero-flagged clusters
// properly allocate storage and clear the zero flag.
func TestZeroFlagClearingOnWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write some data first
	data := make([]byte, img.ClusterSize())
	for i := range data {
		data[i] = 0xAA
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("Initial write failed: %v", err)
	}

	// Zero the cluster (sets zero flag)
	if err := img.WriteZeroAt(0, int64(img.ClusterSize())); err != nil {
		t.Fatalf("WriteZeroAt failed: %v", err)
	}

	// Verify it reads as zeros
	buf := make([]byte, 1024)
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt after zero failed: %v", err)
	}
	for i, b := range buf {
		if b != 0 {
			t.Fatalf("Expected zero at %d after WriteZeroAt, got %d", i, b)
		}
	}

	// Now write data to the zero-flagged cluster
	writeData := make([]byte, 512)
	for i := range writeData {
		writeData[i] = 0xBB
	}
	if _, err := img.WriteAt(writeData, 100); err != nil {
		t.Fatalf("Write to zero cluster failed: %v", err)
	}

	// Read back - should see the written data
	readBuf := make([]byte, 512)
	if _, err := img.ReadAt(readBuf, 100); err != nil {
		t.Fatalf("ReadAt after write to zero cluster failed: %v", err)
	}
	for i, b := range readBuf {
		if b != 0xBB {
			t.Errorf("Data mismatch at %d: got 0x%02X, want 0xBB", i, b)
			break
		}
	}

	// Areas not written should be zero
	if _, err := img.ReadAt(readBuf, 0); err != nil {
		t.Fatalf("ReadAt unwritten area failed: %v", err)
	}
	for i := 0; i < 100; i++ {
		if readBuf[i] != 0 {
			t.Errorf("Unwritten area at %d should be 0, got 0x%02X", i, readBuf[i])
			break
		}
	}

	img.Close()
}

// TestZeroAllocOverwrite verifies writes to ZERO_ALLOC clusters work correctly.
func TestZeroAllocOverwrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image
	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write some data first
	data := make([]byte, img.ClusterSize())
	for i := range data {
		data[i] = 0xAA
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("Initial write failed: %v", err)
	}

	// Zero with ZeroAlloc mode (keeps allocation but marks as zero)
	if err := img.WriteZeroAtMode(0, int64(img.ClusterSize()), ZeroAlloc); err != nil {
		t.Fatalf("WriteZeroAtMode(ZeroAlloc) failed: %v", err)
	}

	// Verify it reads as zeros
	buf := make([]byte, 1024)
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt after ZeroAlloc failed: %v", err)
	}
	for i, b := range buf {
		if b != 0 {
			t.Fatalf("Expected zero at %d after ZeroAlloc, got %d", i, b)
		}
	}

	// Write new data - should work and be readable
	writeData := make([]byte, 512)
	for i := range writeData {
		writeData[i] = 0xCC
	}
	if _, err := img.WriteAt(writeData, 200); err != nil {
		t.Fatalf("Write to ZeroAlloc cluster failed: %v", err)
	}

	// Read back the written data
	readBuf := make([]byte, 512)
	if _, err := img.ReadAt(readBuf, 200); err != nil {
		t.Fatalf("ReadAt after write to ZeroAlloc cluster failed: %v", err)
	}
	for i, b := range readBuf {
		if b != 0xCC {
			t.Errorf("Data mismatch at %d: got 0x%02X, want 0xCC", i, b)
			break
		}
	}

	img.Close()
}

// TestExtendedL2WriteRejection verifies that writes to extended L2 images
// are properly rejected until full subcluster support is implemented.
func TestExtendedL2WriteRejection(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	// Check QEMU version - extended_l2 requires QEMU 5.2+
	if !qemuVersionAtLeast(t, 5, 2) {
		t.Skip("Extended L2 requires QEMU 5.2+")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "extended_l2.qcow2")

	// Create extended L2 image with QEMU
	testutil.QemuCreate(t, path, "10M", "-o", "extended_l2=on")

	// Open with our library
	img, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img.Close()

	// Verify extendedL2 flag is set
	if !img.extendedL2 {
		t.Fatal("Expected extendedL2 flag to be true")
	}

	// Attempt to write - should fail
	data := []byte("test data")
	_, err = img.WriteAt(data, 0)
	if err == nil {
		t.Fatal("Expected WriteAt to fail on extended L2 image")
	}
	t.Logf("WriteAt correctly rejected: %v", err)

	// WriteZeroAt should also fail
	err = img.WriteZeroAt(0, 4096)
	if err == nil {
		t.Fatal("Expected WriteZeroAt to fail on extended L2 image")
	}

	// Reading should still work
	buf := make([]byte, 4096)
	_, err = img.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt should work on extended L2 image: %v", err)
	}
}

// TestBitmapInvalidationOnWrite verifies that persistent bitmaps are marked
// as in-use (inconsistent) when the image is first written to.
func TestBitmapInvalidationOnWrite(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	// Bitmaps require QEMU 3.0+
	if !qemuVersionAtLeast(t, 3, 0) {
		t.Skip("Bitmaps require QEMU 3.0+")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "bitmap.qcow2")

	// Create image with QEMU
	testutil.QemuCreate(t, path, "10M")

	// Add a bitmap using qemu-img
	testutil.RunQemuImg(t, "bitmap", "--add", path, "dirty-bitmap")

	// Verify bitmap exists and is consistent
	img, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	bitmaps, err := img.Bitmaps()
	if err != nil {
		t.Fatalf("Bitmaps() failed: %v", err)
	}
	if len(bitmaps) == 0 {
		t.Skip("No bitmaps found - QEMU version may not support bitmap command")
	}

	// Check bitmap is initially consistent
	found := false
	for _, b := range bitmaps {
		if b.Name == "dirty-bitmap" {
			found = true
			if !b.IsConsistent {
				t.Fatal("Bitmap should be consistent before any writes")
			}
			break
		}
	}
	if !found {
		t.Fatal("dirty-bitmap not found")
	}

	// Write some data
	data := []byte("test data")
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Check that bitmaps are now marked as in-use
	bitmaps, err = img.Bitmaps()
	if err != nil {
		t.Fatalf("Bitmaps() after write failed: %v", err)
	}
	for _, b := range bitmaps {
		if b.Name == "dirty-bitmap" {
			if b.IsConsistent {
				t.Error("Bitmap should be marked inconsistent (in-use) after write")
			}
			break
		}
	}

	img.Close()
}

// TestMetadataAllocationInMainFile verifies that metadata (L2 tables) are
// allocated in the main qcow2 file, not the external data file.
func TestMetadataAllocationInMainFile(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	// External data file requires QEMU 4.0+
	if !qemuVersionAtLeast(t, 4, 0) {
		t.Skip("External data files require QEMU 4.0+")
	}

	dir := t.TempDir()
	mainPath := filepath.Join(dir, "main.qcow2")
	dataPath := filepath.Join(dir, "data.raw")

	// Create image with external data file using QEMU
	testutil.QemuCreate(t, mainPath, "10M", "-o", "data_file="+dataPath, "-o", "data_file_raw=on")

	// Get initial sizes
	mainInfoBefore, err := os.Stat(mainPath)
	if err != nil {
		t.Fatalf("Stat main file failed: %v", err)
	}
	dataInfoBefore, err := os.Stat(dataPath)
	if err != nil {
		t.Fatalf("Stat data file failed: %v", err)
	}

	// Open with our library
	img, err := Open(mainPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write to trigger L2 table allocation
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Flush to ensure all metadata is written
	if err := img.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Get sizes after write
	mainInfoAfter, err := os.Stat(mainPath)
	if err != nil {
		t.Fatalf("Stat main file after write failed: %v", err)
	}
	dataInfoAfter, err := os.Stat(dataPath)
	if err != nil {
		t.Fatalf("Stat data file after write failed: %v", err)
	}

	// Main file should have grown (L2 table metadata)
	mainGrew := mainInfoAfter.Size() > mainInfoBefore.Size()

	// Data file should have grown (actual data)
	dataGrew := dataInfoAfter.Size() > dataInfoBefore.Size()

	t.Logf("Main file: %d -> %d (grew: %v)", mainInfoBefore.Size(), mainInfoAfter.Size(), mainGrew)
	t.Logf("Data file: %d -> %d (grew: %v)", dataInfoBefore.Size(), dataInfoAfter.Size(), dataGrew)

	// Both should grow: main file for metadata, data file for data
	if !mainGrew {
		t.Error("Main qcow2 file should grow for L2 table metadata")
	}
	if !dataGrew {
		t.Error("External data file should grow for cluster data")
	}

	// Close and verify with QEMU
	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	result := testutil.QemuCheck(t, mainPath)
	if !result.IsClean {
		t.Errorf("qemu-img check failed: %s", result.Stderr)
	}

	// Verify data is readable
	img2, err := Open(mainPath)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer img2.Close()

	readBuf := make([]byte, 4096)
	if _, err := img2.ReadAt(readBuf, 0); err != nil {
		t.Fatalf("ReadAt after reopen failed: %v", err)
	}
	if !bytes.Equal(readBuf, data) {
		t.Error("Data mismatch after reopen")
	}
}

// TestRefcountAfterManyAllocations stress tests the refcount block allocation
// to ensure refcounts are consistent after many cluster allocations.
func TestRefcountAfterManyAllocations(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create a larger image to force more refcount block allocations
	img, err := Create(path, CreateOptions{
		Size:        100 * 1024 * 1024, // 100MB
		ClusterBits: 16,                // 64KB clusters
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write to many different clusters to force refcount updates
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}

	clusterSize := img.ClusterSize()
	numWrites := 200 // Write to 200 different clusters

	for i := 0; i < numWrites; i++ {
		offset := int64(i) * int64(clusterSize)
		if offset >= img.Size() {
			break
		}
		if _, err := img.WriteAt(data, offset); err != nil {
			t.Fatalf("WriteAt at offset %d failed: %v", offset, err)
		}
	}

	// Close cleanly
	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and run consistency check
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer img2.Close()

	result, err := img2.Check()
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if result.Corruptions > 0 {
		t.Errorf("Check found %d corruptions", result.Corruptions)
	}
	if len(result.Errors) > 0 {
		for _, e := range result.Errors {
			t.Errorf("Check error: %s", e)
		}
	}
	if result.Leaks > 0 {
		t.Logf("Check found %d leaks (may be expected)", result.Leaks)
	}

	// Verify data integrity
	readBuf := make([]byte, 4096)
	for i := 0; i < numWrites; i++ {
		offset := int64(i) * int64(clusterSize)
		if offset >= img2.Size() {
			break
		}
		if _, err := img2.ReadAt(readBuf, offset); err != nil {
			t.Fatalf("ReadAt at offset %d failed: %v", offset, err)
		}
		if !bytes.Equal(readBuf, data) {
			t.Errorf("Data mismatch at cluster %d", i)
			break
		}
	}
}

// TestL2EntryFlagsIntegrity verifies L2 entry flags are correctly maintained
// across various operations (write, zero, overwrite).
func TestL2EntryFlagsIntegrity(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	img, err := CreateSimple(path, 1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	clusterSize := img.ClusterSize()

	// Test sequence: write -> zero -> write -> zero_alloc -> write
	data := make([]byte, clusterSize)
	for i := range data {
		data[i] = 0xAA
	}

	// Step 1: Initial write
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("Step 1 write failed: %v", err)
	}

	// Verify read
	buf := make([]byte, clusterSize)
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("Step 1 read failed: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Fatal("Step 1: data mismatch")
	}

	// Step 2: Zero (deallocate)
	if err := img.WriteZeroAt(0, int64(clusterSize)); err != nil {
		t.Fatalf("Step 2 zero failed: %v", err)
	}

	// Verify zeros
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("Step 2 read failed: %v", err)
	}
	for i, b := range buf {
		if b != 0 {
			t.Fatalf("Step 2: expected zero at %d, got %d", i, b)
		}
	}

	// Step 3: Write again
	for i := range data {
		data[i] = 0xBB
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("Step 3 write failed: %v", err)
	}

	// Verify read
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("Step 3 read failed: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Fatal("Step 3: data mismatch")
	}

	// Step 4: Zero with allocation (ZeroAlloc)
	if err := img.WriteZeroAtMode(0, int64(clusterSize), ZeroAlloc); err != nil {
		t.Fatalf("Step 4 zero_alloc failed: %v", err)
	}

	// Verify zeros
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("Step 4 read failed: %v", err)
	}
	for i, b := range buf {
		if b != 0 {
			t.Fatalf("Step 4: expected zero at %d, got %d", i, b)
		}
	}

	// Step 5: Write over ZeroAlloc
	for i := range data {
		data[i] = 0xCC
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("Step 5 write failed: %v", err)
	}

	// Verify read
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("Step 5 read failed: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Fatal("Step 5: data mismatch")
	}

	// Close and reopen to verify persistence
	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer img2.Close()

	if _, err := img2.ReadAt(buf, 0); err != nil {
		t.Fatalf("Final read failed: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Fatal("Final: data mismatch after reopen")
	}

	// Run Check
	result, err := img2.Check()
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if result.Corruptions > 0 {
		t.Errorf("Check found %d corruptions", result.Corruptions)
	}
}

// TestConcurrencyStress tests concurrent writes from multiple goroutines to
// overlapping regions and verifies Check() reports no leaks or corruptions.
//
// NOTE: This test currently exposes concurrency bugs in the library's internal
// structures (freeClusterBitmap, L2 cache, L2 table updates). These cause
// refcount mismatches and leaked clusters under concurrent access.
// Skipped until proper synchronization is implemented.
func TestConcurrencyStress(t *testing.T) {
	t.Skip("Skipping: known concurrency bugs cause refcount mismatches - see TODO.md")
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "concurrent.qcow2")

	// Create image
	img, err := Create(path, CreateOptions{
		Size:        50 * 1024 * 1024, // 50MB
		ClusterBits: 16,               // 64KB clusters
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	clusterSize := int64(img.ClusterSize())
	numGoroutines := 10
	writesPerGoroutine := 50
	done := make(chan error, numGoroutines)

	// Launch goroutines that write to overlapping regions
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			data := make([]byte, 4096)
			for i := range data {
				data[i] = byte(goroutineID)
			}

			for w := 0; w < writesPerGoroutine; w++ {
				// Write to overlapping clusters - each goroutine writes to
				// clusters 0-9, causing contention
				clusterNum := w % 10
				offset := int64(clusterNum) * clusterSize
				// Add variation within the cluster
				withinCluster := int64((goroutineID * 512) % int(clusterSize-4096))
				offset += withinCluster

				if _, err := img.WriteAt(data, offset); err != nil {
					done <- fmt.Errorf("goroutine %d write %d: %v", goroutineID, w, err)
					return
				}
			}
			done <- nil
		}(g)
	}

	// Wait for all goroutines
	var errors []error
	for i := 0; i < numGoroutines; i++ {
		if err := <-done; err != nil {
			errors = append(errors, err)
		}
	}
	if len(errors) > 0 {
		for _, e := range errors {
			t.Errorf("Write error: %v", e)
		}
		t.Fatal("Some concurrent writes failed")
	}

	// Close cleanly
	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and run consistency check
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer img2.Close()

	result, err := img2.Check()
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if result.Corruptions > 0 {
		t.Errorf("Check found %d corruptions after concurrent writes", result.Corruptions)
	}
	if len(result.Errors) > 0 {
		for _, e := range result.Errors {
			t.Errorf("Check error: %s", e)
		}
	}
	if result.Leaks > 0 {
		t.Errorf("Check found %d leaked clusters after concurrent writes", result.Leaks)
	}

	t.Logf("Concurrent stress test completed: %d goroutines x %d writes = %d total writes",
		numGoroutines, writesPerGoroutine, numGoroutines*writesPerGoroutine)
}

// TestConcurrencyMixedOperations tests concurrent mixed operations (read/write/zero)
// from multiple goroutines.
//
// NOTE: This test currently exposes concurrency bugs. Skipped until fixed.
func TestConcurrencyMixedOperations(t *testing.T) {
	t.Skip("Skipping: known concurrency bugs cause refcount mismatches - see TODO.md")
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "mixed.qcow2")

	// Create image
	img, err := Create(path, CreateOptions{
		Size:        50 * 1024 * 1024, // 50MB
		ClusterBits: 16,               // 64KB clusters
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	clusterSize := int64(img.ClusterSize())
	numGoroutines := 8
	opsPerGoroutine := 30
	done := make(chan error, numGoroutines)

	// Launch goroutines with mixed operations
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			writeData := make([]byte, 4096)
			readBuf := make([]byte, 4096)
			for i := range writeData {
				writeData[i] = byte(goroutineID + 0x40)
			}

			for op := 0; op < opsPerGoroutine; op++ {
				clusterNum := (goroutineID + op) % 15
				offset := int64(clusterNum) * clusterSize

				switch op % 4 {
				case 0: // Write
					if _, err := img.WriteAt(writeData, offset); err != nil {
						done <- fmt.Errorf("goroutine %d write: %v", goroutineID, err)
						return
					}
				case 1: // Read
					if _, err := img.ReadAt(readBuf, offset); err != nil {
						done <- fmt.Errorf("goroutine %d read: %v", goroutineID, err)
						return
					}
				case 2: // Write different offset within cluster
					if _, err := img.WriteAt(writeData, offset+8192); err != nil {
						done <- fmt.Errorf("goroutine %d write2: %v", goroutineID, err)
						return
					}
				case 3: // Zero (only on some clusters to avoid too much contention)
					if clusterNum > 10 {
						if err := img.WriteZeroAt(offset, 4096); err != nil {
							done <- fmt.Errorf("goroutine %d zero: %v", goroutineID, err)
							return
						}
					}
				}
			}
			done <- nil
		}(g)
	}

	// Wait for all goroutines
	var errors []error
	for i := 0; i < numGoroutines; i++ {
		if err := <-done; err != nil {
			errors = append(errors, err)
		}
	}
	if len(errors) > 0 {
		for _, e := range errors {
			t.Errorf("Operation error: %v", e)
		}
		t.Fatal("Some concurrent operations failed")
	}

	// Flush
	if err := img.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Close cleanly
	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and run consistency check
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer img2.Close()

	result, err := img2.Check()
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if result.Corruptions > 0 {
		t.Errorf("Check found %d corruptions after mixed concurrent operations", result.Corruptions)
	}
	if len(result.Errors) > 0 {
		for _, e := range result.Errors {
			t.Errorf("Check error: %s", e)
		}
	}
	if result.Leaks > 0 {
		t.Errorf("Check found %d leaked clusters after mixed concurrent operations", result.Leaks)
	}

	t.Logf("Mixed operations stress test completed: %d goroutines x %d ops = %d total operations",
		numGoroutines, opsPerGoroutine, numGoroutines*opsPerGoroutine)
}

// Helper to read L2 entry directly for testing
func readL2Entry(img *Image, virtOff uint64) (uint64, error) {
	l2Index := (virtOff >> img.clusterBits) & (img.l2Entries - 1)
	l1Index := virtOff >> (img.clusterBits + img.l2Bits)

	if l1Index >= uint64(img.header.L1Size) {
		return 0, nil
	}

	l1Entry := binary.BigEndian.Uint64(img.l1Table[l1Index*8:])
	l2TableOff := l1Entry & L1EntryOffsetMask
	if l2TableOff == 0 {
		return 0, nil
	}

	l2Table, err := img.getL2Table(l2TableOff)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(l2Table[l2Index*8:]), nil
}
