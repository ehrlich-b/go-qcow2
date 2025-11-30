// backing_chain_test.go - Phase 2.4 Backing Chain Tests
// These tests verify correct handling of backing file chains.

package qcow2

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ehrlich-b/go-qcow2/testutil"
)

// =============================================================================
// 2.4 Backing Chain Tests
// =============================================================================

// TestBackingChainDepth3 tests a 3-level backing chain: base -> overlay1 -> overlay2.
func TestBackingChainDepth3(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.qcow2")
	overlay1Path := filepath.Join(dir, "overlay1.qcow2")
	overlay2Path := filepath.Join(dir, "overlay2.qcow2")

	// Create base image and write data
	testutil.QemuCreate(t, basePath, "10M")
	testutil.RequireQemuIO(t)
	testutil.QemuWrite(t, basePath, 0xAA, 0, 65536)

	// Create overlay1 on top of base
	testutil.QemuCreateOverlay(t, overlay1Path, basePath)
	testutil.QemuWrite(t, overlay1Path, 0xBB, 65536, 65536)

	// Create overlay2 on top of overlay1
	testutil.QemuCreateOverlay(t, overlay2Path, overlay1Path)
	testutil.QemuWrite(t, overlay2Path, 0xCC, 131072, 65536)

	// Open the top overlay
	img, err := Open(overlay2Path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img.Close()

	// Read data from base (should see 0xAA)
	buf := make([]byte, 4096)
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt base region failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xAA {
			t.Errorf("Base region byte %d: expected 0xAA, got 0x%02X", i, b)
			break
		}
	}

	// Read data from overlay1 (should see 0xBB)
	if _, err := img.ReadAt(buf, 65536); err != nil {
		t.Fatalf("ReadAt overlay1 region failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xBB {
			t.Errorf("Overlay1 region byte %d: expected 0xBB, got 0x%02X", i, b)
			break
		}
	}

	// Read data from overlay2 (should see 0xCC)
	if _, err := img.ReadAt(buf, 131072); err != nil {
		t.Fatalf("ReadAt overlay2 region failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xCC {
			t.Errorf("Overlay2 region byte %d: expected 0xCC, got 0x%02X", i, b)
			break
		}
	}
}

// TestBackingChainDepth10 tests a 10-level deep backing chain.
func TestBackingChainDepth10(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	depth := 10
	paths := make([]string, depth)

	// Create base
	paths[0] = filepath.Join(dir, "layer0.qcow2")
	testutil.QemuCreate(t, paths[0], "10M")
	testutil.RequireQemuIO(t)
	testutil.QemuWrite(t, paths[0], 0x00, 0, 4096)

	// Create chain of overlays
	for i := 1; i < depth; i++ {
		paths[i] = filepath.Join(dir, "layer"+string(rune('0'+i))+".qcow2")
		testutil.QemuCreateOverlay(t, paths[i], paths[i-1])
		// Write unique data to each layer
		testutil.QemuWrite(t, paths[i], byte(i), int64(i)*4096, 4096)
	}

	// Open the top layer
	img, err := Open(paths[depth-1])
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img.Close()

	// Verify we can read data from each layer
	buf := make([]byte, 4096)
	for i := 0; i < depth; i++ {
		if _, err := img.ReadAt(buf, int64(i)*4096); err != nil {
			t.Fatalf("ReadAt layer %d failed: %v", i, err)
		}
		expected := byte(i)
		if buf[0] != expected {
			t.Errorf("Layer %d data mismatch: expected 0x%02X, got 0x%02X",
				i, expected, buf[0])
		}
	}
}

// TestBackingChainDeepDepthLimit tests hitting the 64-level backing chain limit.
func TestBackingChainDeepDepthLimit(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	// Need MaxBackingChainDepth + 2 = 66 images to exceed the limit
	// The check is depth > 64, so we need depth 65 to fail
	numImages := MaxBackingChainDepth + 2 // 66 images
	paths := make([]string, numImages)

	// Create base
	paths[0] = filepath.Join(dir, "layer00.qcow2")
	testutil.QemuCreate(t, paths[0], "1M")

	// Create chain up to limit+1
	for i := 1; i < numImages; i++ {
		// Generate unique filename like layer01.qcow2, layer02.qcow2, ..., layer65.qcow2
		paths[i] = filepath.Join(dir, fmt.Sprintf("layer%02d.qcow2", i))
		testutil.QemuCreateOverlay(t, paths[i], paths[i-1])
	}

	// Opening the 66th layer should fail due to depth limit
	_, err := Open(paths[numImages-1])
	if err == nil {
		t.Error("Open should fail when backing chain exceeds 64 levels")
	} else {
		t.Logf("Open correctly failed with deep chain: %v", err)
	}
}

// TestBackingChainDifferentClusterSizes tests mixed 4K and 64K cluster sizes.
func TestBackingChainDifferentClusterSizes(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	basePath := filepath.Join(dir, "base_4k.qcow2")
	overlayPath := filepath.Join(dir, "overlay_64k.qcow2")

	// Create base with 4K clusters
	testutil.QemuCreate(t, basePath, "10M", "-o", "cluster_size=4K")
	testutil.RequireQemuIO(t)
	testutil.QemuWrite(t, basePath, 0xAA, 0, 8192)

	// Create overlay with 64K clusters (different from base)
	testutil.QemuCreateOverlay(t, overlayPath, basePath, "-o", "cluster_size=64K")
	testutil.QemuWrite(t, overlayPath, 0xBB, 8192, 4096)

	// Open and verify
	img, err := Open(overlayPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img.Close()

	t.Logf("Overlay cluster size: %d", img.ClusterSize())

	// Read from base region
	buf := make([]byte, 4096)
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt base region failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xAA {
			t.Errorf("Base region byte %d: expected 0xAA, got 0x%02X", i, b)
			break
		}
	}

	// Read from overlay region
	if _, err := img.ReadAt(buf, 8192); err != nil {
		t.Fatalf("ReadAt overlay region failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xBB {
			t.Errorf("Overlay region byte %d: expected 0xBB, got 0x%02X", i, b)
			break
		}
	}
}

// TestBackingChainMixedVersions tests v2 base with v3 overlay.
func TestBackingChainMixedVersions(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	basePath := filepath.Join(dir, "base_v2.qcow2")
	overlayPath := filepath.Join(dir, "overlay_v3.qcow2")

	// Create base with v2 format
	testutil.QemuCreate(t, basePath, "10M", "-o", "compat=0.10")
	testutil.RequireQemuIO(t)
	testutil.QemuWrite(t, basePath, 0xDD, 0, 65536)

	// Create overlay with v3 format (default)
	testutil.QemuCreateOverlay(t, overlayPath, basePath, "-o", "compat=1.1")
	testutil.QemuWrite(t, overlayPath, 0xEE, 65536, 4096)

	// Open and verify
	img, err := Open(overlayPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img.Close()

	t.Logf("Overlay version: %d", img.header.Version)

	// Read from base (v2)
	buf := make([]byte, 4096)
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt base region failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xDD {
			t.Errorf("Base (v2) region byte %d: expected 0xDD, got 0x%02X", i, b)
			break
		}
	}

	// Read from overlay (v3)
	if _, err := img.ReadAt(buf, 65536); err != nil {
		t.Fatalf("ReadAt overlay region failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xEE {
			t.Errorf("Overlay (v3) region byte %d: expected 0xEE, got 0x%02X", i, b)
			break
		}
	}
}

// TestBackingChainWithCompressedBase tests backing chain with compressed clusters in base.
func TestBackingChainWithCompressedBase(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.qcow2")
	basePath := filepath.Join(dir, "base_compressed.qcow2")
	overlayPath := filepath.Join(dir, "overlay.qcow2")

	// Create source with compressible data
	testutil.QemuCreate(t, srcPath, "10M")
	testutil.RequireQemuIO(t)
	testutil.QemuWrite(t, srcPath, 0x00, 0, 65536) // Zeros compress well

	// Convert to compressed
	testutil.QemuConvert(t, srcPath, basePath, true)

	// Create overlay
	testutil.QemuCreateOverlay(t, overlayPath, basePath)
	testutil.QemuWrite(t, overlayPath, 0xFF, 4096, 4096)

	// Open and verify
	img, err := Open(overlayPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img.Close()

	// Read from compressed base (should be zeros)
	buf := make([]byte, 4096)
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt compressed region failed: %v", err)
	}
	for i, b := range buf {
		if b != 0x00 {
			t.Errorf("Compressed region byte %d: expected 0x00, got 0x%02X", i, b)
			break
		}
	}

	// Read from overlay
	if _, err := img.ReadAt(buf, 4096); err != nil {
		t.Fatalf("ReadAt overlay region failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xFF {
			t.Errorf("Overlay region byte %d: expected 0xFF, got 0x%02X", i, b)
			break
		}
	}
}

// TestBackingChainWithZeroFlaggedBase tests backing chain with zero-flagged clusters in base.
func TestBackingChainWithZeroFlaggedBase(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	basePath := filepath.Join(dir, "base_zeros.qcow2")
	overlayPath := filepath.Join(dir, "overlay.qcow2")

	// Create base with zero clusters
	testutil.QemuCreate(t, basePath, "10M")
	testutil.RequireQemuIO(t)
	// Write and then zero
	testutil.QemuWrite(t, basePath, 0xAA, 0, 65536)
	testutil.QemuZero(t, basePath, 0, 65536) // Zero-flag the cluster

	// Create overlay
	testutil.QemuCreateOverlay(t, overlayPath, basePath)
	testutil.QemuWrite(t, overlayPath, 0xBB, 4096, 4096)

	// Open and verify
	img, err := Open(overlayPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img.Close()

	// Read from zero-flagged region in base (should be zeros)
	buf := make([]byte, 4096)
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt zero-flagged region failed: %v", err)
	}
	for i, b := range buf {
		if b != 0x00 {
			t.Errorf("Zero-flagged region byte %d: expected 0x00, got 0x%02X", i, b)
			break
		}
	}

	// Read from overlay
	if _, err := img.ReadAt(buf, 4096); err != nil {
		t.Fatalf("ReadAt overlay region failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xBB {
			t.Errorf("Overlay region byte %d: expected 0xBB, got 0x%02X", i, b)
			break
		}
	}
}

// TestBackingChainCOWPartialCluster tests COW behavior on partial cluster write.
func TestBackingChainCOWPartialCluster(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.qcow2")
	overlayPath := filepath.Join(dir, "overlay.qcow2")

	// Create base with full cluster of data
	testutil.QemuCreate(t, basePath, "10M")
	testutil.RequireQemuIO(t)
	testutil.QemuWrite(t, basePath, 0xAA, 0, 65536) // Full cluster

	// Create overlay
	testutil.QemuCreateOverlay(t, overlayPath, basePath)

	// Open overlay with our library and do partial write
	img, err := Open(overlayPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img.Close()

	// Write to middle of cluster (should trigger COW)
	smallData := make([]byte, 4096)
	for i := range smallData {
		smallData[i] = 0xBB
	}
	if _, err := img.WriteAt(smallData, 16384); err != nil {
		t.Fatalf("WriteAt partial cluster failed: %v", err)
	}

	// Verify: first part should still be from base (0xAA)
	buf := make([]byte, 4096)
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt first part failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xAA {
			t.Errorf("First part byte %d: expected 0xAA, got 0x%02X", i, b)
			break
		}
	}

	// Verify: middle part should be our write (0xBB)
	if _, err := img.ReadAt(buf, 16384); err != nil {
		t.Fatalf("ReadAt middle part failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xBB {
			t.Errorf("Middle part byte %d: expected 0xBB, got 0x%02X", i, b)
			break
		}
	}

	// Verify: last part should still be from base (0xAA)
	if _, err := img.ReadAt(buf, 32768); err != nil {
		t.Fatalf("ReadAt last part failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xAA {
			t.Errorf("Last part byte %d: expected 0xAA, got 0x%02X", i, b)
			break
		}
	}
}

// TestBackingChainMissingFile tests handling of missing backing file.
func TestBackingChainMissingFile(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.qcow2")
	overlayPath := filepath.Join(dir, "overlay.qcow2")

	// Create base and overlay
	testutil.QemuCreate(t, basePath, "10M")
	testutil.QemuCreateOverlay(t, overlayPath, basePath)

	// Delete the base file
	if err := os.Remove(basePath); err != nil {
		t.Fatalf("Remove base failed: %v", err)
	}

	// Try to open overlay - should fail
	_, err := Open(overlayPath)
	if err == nil {
		t.Error("Open should fail when backing file is missing")
	} else {
		t.Logf("Open correctly failed with missing backing file: %v", err)
	}
}

// TestBackingChainCircularReference tests handling of circular backing reference.
func TestBackingChainCircularReference(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	imagePath := filepath.Join(dir, "circular.qcow2")

	// Create an image
	testutil.QemuCreate(t, imagePath, "10M")

	// Manually set backing file to itself (requires direct file manipulation)
	// Read header, modify backing file path
	f, err := os.OpenFile(imagePath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	// Read header
	headerBuf := make([]byte, HeaderSizeV3)
	if _, err := f.ReadAt(headerBuf, 0); err != nil {
		f.Close()
		t.Fatalf("ReadAt header failed: %v", err)
	}

	// Get file info
	info, _ := f.Stat()
	fileSize := info.Size()

	// Write backing file path at end of file
	backingPath := []byte(imagePath)
	backingOffset := fileSize
	if _, err := f.WriteAt(backingPath, backingOffset); err != nil {
		f.Close()
		t.Fatalf("WriteAt backing path failed: %v", err)
	}

	// Update header with backing file offset and size
	// Offset 8: backing_file_offset (8 bytes)
	// Offset 16: backing_file_size (4 bytes)
	offsetBuf := make([]byte, 8)
	offsetBuf[0] = byte(backingOffset >> 56)
	offsetBuf[1] = byte(backingOffset >> 48)
	offsetBuf[2] = byte(backingOffset >> 40)
	offsetBuf[3] = byte(backingOffset >> 32)
	offsetBuf[4] = byte(backingOffset >> 24)
	offsetBuf[5] = byte(backingOffset >> 16)
	offsetBuf[6] = byte(backingOffset >> 8)
	offsetBuf[7] = byte(backingOffset)
	if _, err := f.WriteAt(offsetBuf, 8); err != nil {
		f.Close()
		t.Fatalf("WriteAt backing offset failed: %v", err)
	}

	sizeBuf := make([]byte, 4)
	sizeBuf[0] = byte(len(backingPath) >> 24)
	sizeBuf[1] = byte(len(backingPath) >> 16)
	sizeBuf[2] = byte(len(backingPath) >> 8)
	sizeBuf[3] = byte(len(backingPath))
	if _, err := f.WriteAt(sizeBuf, 16); err != nil {
		f.Close()
		t.Fatalf("WriteAt backing size failed: %v", err)
	}
	f.Close()

	// Try to open - should fail due to circular reference
	_, err = Open(imagePath)
	if err == nil {
		t.Error("Open should fail with circular backing reference")
	} else {
		t.Logf("Open correctly failed with circular reference: %v", err)
	}
}

// TestBackingChainRelativePath tests relative path resolution.
func TestBackingChainRelativePath(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	subdir := filepath.Join(dir, "subdir")
	if err := os.MkdirAll(subdir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	basePath := filepath.Join(dir, "base.qcow2")
	overlayPath := filepath.Join(subdir, "overlay.qcow2")

	// Create base in parent directory
	testutil.QemuCreate(t, basePath, "10M")
	testutil.RequireQemuIO(t)
	testutil.QemuWrite(t, basePath, 0xAA, 0, 4096)

	// Create overlay in subdirectory with relative backing path
	// Use -F to specify backing format and relative path
	testutil.QemuCreateOverlay(t, overlayPath, "../base.qcow2")

	// Open and verify
	img, err := Open(overlayPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img.Close()

	buf := make([]byte, 4096)
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xAA {
			t.Errorf("Byte %d: expected 0xAA, got 0x%02X", i, b)
			break
		}
	}
}

// TestBackingChainAbsolutePath tests absolute path handling.
func TestBackingChainAbsolutePath(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.qcow2")
	overlayPath := filepath.Join(dir, "overlay.qcow2")

	// Create base
	testutil.QemuCreate(t, basePath, "10M")
	testutil.RequireQemuIO(t)
	testutil.QemuWrite(t, basePath, 0xBB, 0, 4096)

	// Create overlay with absolute backing path (default behavior)
	testutil.QemuCreateOverlay(t, overlayPath, basePath)

	// Open and verify
	img, err := Open(overlayPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img.Close()

	// Verify backing file path is correct
	if img.BackingFile() == "" {
		t.Error("BackingFile() returned empty string")
	}
	t.Logf("Backing file path: %s", img.BackingFile())

	buf := make([]byte, 4096)
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(buf, bytes.Repeat([]byte{0xBB}, 4096)) {
		t.Error("Data mismatch from backing file")
	}
}
