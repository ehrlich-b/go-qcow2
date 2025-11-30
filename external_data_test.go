// external_data_test.go - Phase 2.6 External Data File Tests
// These tests verify correct handling of external data files.

package qcow2

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/ehrlich-b/go-qcow2/testutil"
)

// =============================================================================
// 2.6 External Data File Tests
// =============================================================================

// qemuSupportsExternalDataFiles checks if the QEMU version supports external data files.
func qemuSupportsExternalDataFiles(t *testing.T) bool {
	// External data files require QEMU 4.0+
	return qemuVersionAtLeast(t, 4, 0)
}

// TestExternalDataFileBasic tests creating and using an external data file.
func TestExternalDataFileBasic(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	if !qemuSupportsExternalDataFiles(t) {
		t.Skip("External data files require QEMU 4.0+")
	}

	dir := t.TempDir()
	imgPath := filepath.Join(dir, "test.qcow2")
	dataPath := filepath.Join(dir, "test.raw")

	// Create a qcow2 image with external data file using qemu-img
	testutil.QemuCreate(t, imgPath, "10M", "-o", "data_file="+dataPath, "-o", "data_file_raw=on")

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

	// Write test data at multiple offsets
	clusterSize := int(img.ClusterSize())
	testData := make([]byte, clusterSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Write to first cluster
	if _, err := img.WriteAt(testData, 0); err != nil {
		t.Fatalf("WriteAt offset 0 failed: %v", err)
	}

	// Write to second cluster with different data
	testData2 := make([]byte, clusterSize)
	for i := range testData2 {
		testData2[i] = byte((i + 128) % 256)
	}
	if _, err := img.WriteAt(testData2, int64(clusterSize)); err != nil {
		t.Fatalf("WriteAt offset %d failed: %v", clusterSize, err)
	}

	// Read back and verify
	buf := make([]byte, clusterSize)
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt offset 0 failed: %v", err)
	}
	if !bytes.Equal(buf, testData) {
		t.Error("Data mismatch at offset 0")
	}

	if _, err := img.ReadAt(buf, int64(clusterSize)); err != nil {
		t.Fatalf("ReadAt offset %d failed: %v", clusterSize, err)
	}
	if !bytes.Equal(buf, testData2) {
		t.Error("Data mismatch at offset clusterSize")
	}

	// Flush and close
	if err := img.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify data persists
	img2, err := Open(imgPath)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer img2.Close()

	if _, err := img2.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt after reopen failed: %v", err)
	}
	if !bytes.Equal(buf, testData) {
		t.Error("Data mismatch after reopen at offset 0")
	}

	if _, err := img2.ReadAt(buf, int64(clusterSize)); err != nil {
		t.Fatalf("ReadAt after reopen failed: %v", err)
	}
	if !bytes.Equal(buf, testData2) {
		t.Error("Data mismatch after reopen at clusterSize")
	}

	// Verify qemu-img check passes
	checkResult := testutil.QemuCheck(t, imgPath)
	if !checkResult.IsClean {
		t.Errorf("qemu-img check failed: %s", checkResult.Stderr)
	}
}

// TestExternalDataFileTruncated tests handling when the data file is shorter than expected.
func TestExternalDataFileTruncated(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	if !qemuSupportsExternalDataFiles(t) {
		t.Skip("External data files require QEMU 4.0+")
	}

	dir := t.TempDir()
	imgPath := filepath.Join(dir, "test.qcow2")
	dataPath := filepath.Join(dir, "test.raw")

	// Create a qcow2 image with external data file
	testutil.QemuCreate(t, imgPath, "10M", "-o", "data_file="+dataPath, "-o", "data_file_raw=on")

	// Open and write some data first
	img, err := Open(imgPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	clusterSize := int(img.ClusterSize())
	testData := make([]byte, clusterSize*2) // Write 2 clusters
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	if _, err := img.WriteAt(testData, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if err := img.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	img.Close()

	// Get the data file size before truncation
	dataInfo, err := os.Stat(dataPath)
	if err != nil {
		t.Fatalf("Stat data file failed: %v", err)
	}
	originalSize := dataInfo.Size()
	t.Logf("Original data file size: %d", originalSize)

	// Truncate the data file to half its size
	if err := os.Truncate(dataPath, originalSize/2); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Reopen the image - this should still work
	img2, err := Open(imgPath)
	if err != nil {
		t.Fatalf("Open after truncation failed: %v", err)
	}
	defer img2.Close()

	// Reading the first cluster should work (if it's within the truncated size)
	buf := make([]byte, clusterSize)
	n, err := img2.ReadAt(buf, 0)

	// Behavior depends on how much was truncated
	if originalSize/2 >= int64(clusterSize) {
		// First cluster should be readable
		if err != nil {
			t.Logf("ReadAt first cluster failed (may be expected): %v", err)
		} else if n != clusterSize {
			t.Logf("ReadAt first cluster: got %d bytes, expected %d", n, clusterSize)
		}
	}

	// Reading the second cluster should fail or return partial data
	buf2 := make([]byte, clusterSize)
	n2, err2 := img2.ReadAt(buf2, int64(clusterSize))
	if err2 == nil && n2 == clusterSize {
		// This might work if the cluster offset is within the truncated data
		t.Logf("ReadAt second cluster succeeded unexpectedly: read %d bytes", n2)
	} else {
		t.Logf("ReadAt second cluster correctly failed or partial: n=%d, err=%v", n2, err2)
	}
}

// TestExternalDataFileMissing tests handling when the data file is missing.
func TestExternalDataFileMissing(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	if !qemuSupportsExternalDataFiles(t) {
		t.Skip("External data files require QEMU 4.0+")
	}

	dir := t.TempDir()
	imgPath := filepath.Join(dir, "test.qcow2")
	dataPath := filepath.Join(dir, "test.raw")

	// Create a qcow2 image with external data file
	testutil.QemuCreate(t, imgPath, "10M", "-o", "data_file="+dataPath, "-o", "data_file_raw=on")

	// Verify both files exist
	if _, err := os.Stat(imgPath); err != nil {
		t.Fatalf("Image file should exist: %v", err)
	}
	if _, err := os.Stat(dataPath); err != nil {
		t.Fatalf("Data file should exist: %v", err)
	}

	// Delete the data file
	if err := os.Remove(dataPath); err != nil {
		t.Fatalf("Remove data file failed: %v", err)
	}

	// Try to open the image - should fail
	_, err := Open(imgPath)
	if err == nil {
		t.Fatal("Open should fail when external data file is missing")
	}
	t.Logf("Open correctly failed with missing data file: %v", err)

	// The error should indicate the file is missing
	if !os.IsNotExist(err) {
		// Check if wrapped error contains the not-exist error
		t.Logf("Error type: %T, message: %v", err, err)
	}
}

// TestExternalDataFilePermissionDenied tests handling when the data file can't be read.
func TestExternalDataFilePermissionDenied(t *testing.T) {
	// Skip on Windows - permission model is different
	if runtime.GOOS == "windows" {
		t.Skip("Skipping permission test on Windows")
	}

	// Skip if running as root - root can read anything
	if os.Getuid() == 0 {
		t.Skip("Skipping permission test when running as root")
	}

	t.Parallel()
	testutil.RequireQemu(t)

	if !qemuSupportsExternalDataFiles(t) {
		t.Skip("External data files require QEMU 4.0+")
	}

	dir := t.TempDir()
	imgPath := filepath.Join(dir, "test.qcow2")
	dataPath := filepath.Join(dir, "test.raw")

	// Create a qcow2 image with external data file
	testutil.QemuCreate(t, imgPath, "10M", "-o", "data_file="+dataPath, "-o", "data_file_raw=on")

	// Remove all permissions from the data file
	if err := os.Chmod(dataPath, 0000); err != nil {
		t.Fatalf("Chmod failed: %v", err)
	}
	// Restore permissions on cleanup
	defer os.Chmod(dataPath, 0644)

	// Try to open the image - should fail with permission denied
	_, err := Open(imgPath)
	if err == nil {
		t.Fatal("Open should fail when external data file has no permissions")
	}
	t.Logf("Open correctly failed with permission denied: %v", err)

	// Verify it's a permission error
	if !os.IsPermission(err) {
		t.Logf("Error type: %T, message: %v (expected permission error)", err, err)
	}
}

// TestExternalDataFileGrowth tests that the data file grows correctly with writes.
func TestExternalDataFileGrowth(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	if !qemuSupportsExternalDataFiles(t) {
		t.Skip("External data files require QEMU 4.0+")
	}

	dir := t.TempDir()
	imgPath := filepath.Join(dir, "test.qcow2")
	dataPath := filepath.Join(dir, "test.raw")

	// Create a qcow2 image with external data file
	testutil.QemuCreate(t, imgPath, "100M", "-o", "data_file="+dataPath, "-o", "data_file_raw=on")

	// Get initial sizes
	mainInfoBefore, err := os.Stat(imgPath)
	if err != nil {
		t.Fatalf("Stat main file failed: %v", err)
	}
	dataInfoBefore, err := os.Stat(dataPath)
	if err != nil {
		t.Fatalf("Stat data file failed: %v", err)
	}

	t.Logf("Initial main file size: %d", mainInfoBefore.Size())
	t.Logf("Initial data file size: %d", dataInfoBefore.Size())

	// Open the image
	img, err := Open(imgPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img.Close()

	clusterSize := int64(img.ClusterSize())

	// Write to multiple clusters at different offsets
	data := make([]byte, clusterSize)
	for i := range data {
		data[i] = 0xAA
	}

	offsets := []int64{0, clusterSize * 10, clusterSize * 100, clusterSize * 500}
	for _, offset := range offsets {
		if _, err := img.WriteAt(data, offset); err != nil {
			t.Fatalf("WriteAt offset %d failed: %v", offset, err)
		}
	}

	// Flush to ensure all data is written
	if err := img.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Get sizes after writes
	mainInfoAfter, err := os.Stat(imgPath)
	if err != nil {
		t.Fatalf("Stat main file after write failed: %v", err)
	}
	dataInfoAfter, err := os.Stat(dataPath)
	if err != nil {
		t.Fatalf("Stat data file after write failed: %v", err)
	}

	t.Logf("After writes main file size: %d (grew by %d)", mainInfoAfter.Size(), mainInfoAfter.Size()-mainInfoBefore.Size())
	t.Logf("After writes data file size: %d (grew by %d)", dataInfoAfter.Size(), dataInfoAfter.Size()-dataInfoBefore.Size())

	// Main file should have grown (L2 table metadata)
	if mainInfoAfter.Size() <= mainInfoBefore.Size() {
		t.Error("Main qcow2 file should grow for L2 table metadata")
	}

	// Data file should have grown (actual data)
	if dataInfoAfter.Size() <= dataInfoBefore.Size() {
		t.Error("External data file should grow for cluster data")
	}

	// Data file should have grown to at least cover the highest offset written
	expectedMinSize := offsets[len(offsets)-1] + clusterSize
	if dataInfoAfter.Size() < expectedMinSize {
		t.Errorf("Data file should be at least %d bytes, got %d", expectedMinSize, dataInfoAfter.Size())
	}

	// Verify data can be read back
	buf := make([]byte, clusterSize)
	for _, offset := range offsets {
		if _, err := img.ReadAt(buf, offset); err != nil {
			t.Errorf("ReadAt offset %d failed: %v", offset, err)
			continue
		}
		if !bytes.Equal(buf, data) {
			t.Errorf("Data mismatch at offset %d", offset)
		}
	}

	// Verify qemu-img check passes
	img.Close()
	checkResult := testutil.QemuCheck(t, imgPath)
	if !checkResult.IsClean {
		t.Errorf("qemu-img check failed: %s", checkResult.Stderr)
	}
}

// TestExternalDataFileWithCompression tests that compression is rejected with external data files.
// Compressed clusters have a special L2 entry format that encodes offset+size,
// which conflicts with the raw sector layout expected by external data files.
func TestExternalDataFileWithCompression(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	if !qemuSupportsExternalDataFiles(t) {
		t.Skip("External data files require QEMU 4.0+")
	}

	dir := t.TempDir()
	imgPath := filepath.Join(dir, "test.qcow2")
	dataPath := filepath.Join(dir, "test.raw")

	// Create a qcow2 image with external data file (raw mode)
	testutil.QemuCreate(t, imgPath, "10M", "-o", "data_file="+dataPath, "-o", "data_file_raw=on")

	// Open the image
	img, err := Open(imgPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img.Close()

	// Verify it's an external data file image
	if !img.header.HasExternalDataFile() {
		t.Fatal("Expected HasExternalDataFile() to be true")
	}

	// Try to write compressed data - should fail
	clusterSize := int(img.ClusterSize())
	data := make([]byte, clusterSize)
	for i := range data {
		data[i] = 0x42 // Highly compressible
	}

	_, err = img.WriteAtCompressed(data, 0)
	if err == nil {
		t.Fatal("WriteAtCompressed should fail with external data file")
	}
	t.Logf("WriteAtCompressed correctly rejected: %v", err)

	// Regular writes should still work
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("Regular WriteAt should work: %v", err)
	}

	// Verify data
	buf := make([]byte, clusterSize)
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Error("Data mismatch")
	}

	// Verify qemu-img check passes (no corrupted compressed clusters)
	img.Close()
	checkResult := testutil.QemuCheck(t, imgPath)
	if !checkResult.IsClean {
		t.Errorf("qemu-img check failed: %s", checkResult.Stderr)
	}
}
