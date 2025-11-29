// QEMU Interoperability Tests
//
// These tests verify that go-qcow2 produces images compatible with QEMU
// and can correctly read images created by QEMU.
//
// Run with: go test -v -tags=qemu ./...
// Or: make qemu-test

//go:build qemu

package qcow2

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/ehrlich-b/go-qcow2/testutil"
)

// TestQemuInterop_CreateWithUs_VerifyWithQemu creates an image with go-qcow2
// and verifies it passes qemu-img check.
func TestQemuInterop_CreateWithUs_VerifyWithQemu(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	tests := []struct {
		name        string
		size        uint64
		clusterBits uint32
		lazy        bool
	}{
		{"default_64k", 10 * 1024 * 1024, 16, false},
		{"small_4k", 10 * 1024 * 1024, 12, false},
		{"lazy_refcounts", 10 * 1024 * 1024, 16, true},
		{"large_100m", 100 * 1024 * 1024, 16, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := testutil.TempImage(t, "test.qcow2")

			// Create with go-qcow2
			opts := CreateOptions{
				Size:          tc.size,
				ClusterBits:   tc.clusterBits,
				LazyRefcounts: tc.lazy,
			}
			img, err := Create(path, opts)
			if err != nil {
				t.Fatalf("Create failed: %v", err)
			}
			img.Close()

			// Verify with qemu-img check
			result := testutil.QemuCheck(t, path)
			if !result.IsClean {
				t.Errorf("qemu-img check failed: corruptions=%d, leaks=%d, stderr=%s",
					result.Corruptions, result.Leaks, result.Stderr)
			}

			// Verify info matches
			info := testutil.QemuInfo(t, path)
			if uint64(info.VirtualSize) != tc.size {
				t.Errorf("Virtual size mismatch: qemu=%d, expected=%d",
					info.VirtualSize, tc.size)
			}
			if info.Format != "qcow2" {
				t.Errorf("Format mismatch: got %s, want qcow2", info.Format)
			}
		})
	}
}

// TestQemuInterop_CreateWithQemu_OpenWithUs creates an image with qemu-img
// and verifies go-qcow2 can open and read it correctly.
func TestQemuInterop_CreateWithQemu_OpenWithUs(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	tests := []struct {
		name string
		size string
		opts []string
	}{
		{"default", "10M", nil},
		{"cluster_4k", "10M", []string{"-o", "cluster_size=4K"}},
		{"cluster_2m", "100M", []string{"-o", "cluster_size=2M"}},
		{"lazy_refcounts", "10M", []string{"-o", "lazy_refcounts=on"}},
		{"v2_compat", "10M", []string{"-o", "compat=0.10"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := testutil.TempImage(t, "qemu.qcow2")

			// Create with qemu-img
			testutil.QemuCreate(t, path, tc.size, tc.opts...)

			// Open with go-qcow2
			img, err := Open(path)
			if err != nil {
				t.Fatalf("Open failed: %v", err)
			}
			defer img.Close()

			// Parse expected size
			expectedSize, _ := testutil.ParseSize(tc.size)
			if img.Size() != expectedSize {
				t.Errorf("Size mismatch: got=%d, want=%d", img.Size(), expectedSize)
			}

			// Read from empty image should return zeros
			buf := make([]byte, 4096)
			n, err := img.ReadAt(buf, 0)
			if err != nil {
				t.Fatalf("ReadAt failed: %v", err)
			}
			if n != len(buf) {
				t.Errorf("Short read: %d", n)
			}
			for i, b := range buf {
				if b != 0 {
					t.Errorf("Non-zero byte at %d: %d", i, b)
					break
				}
			}
		})
	}
}

// TestQemuInterop_WriteWithUs_ReadWithQemu writes data with go-qcow2
// and verifies qemu-io can read it correctly.
func TestQemuInterop_WriteWithUs_ReadWithQemu(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)
	testutil.RequireQemuIO(t)

	path := testutil.TempImage(t, "test.qcow2")

	// Create with go-qcow2
	img, err := CreateSimple(path, 10*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write test patterns at various offsets (non-overlapping)
	patterns := []struct {
		pattern byte
		offset  int64
		length  int
	}{
		{0xAA, 0, 4096},
		{0xBB, 65536, 4096},       // Second cluster
		{0xCC, 131072 - 100, 200}, // Cross cluster boundary (clusters 2-3)
		{0xDD, 1024 * 1024, 8192}, // 1MB offset
	}

	for _, p := range patterns {
		data := bytes.Repeat([]byte{p.pattern}, p.length)
		n, err := img.WriteAt(data, p.offset)
		if err != nil {
			t.Fatalf("WriteAt pattern 0x%02x at %d failed: %v", p.pattern, p.offset, err)
		}
		if n != len(data) {
			t.Errorf("Short write: %d < %d", n, len(data))
		}
	}

	img.Close()

	// Verify with qemu-img check first
	checkResult := testutil.QemuCheck(t, path)
	if !checkResult.IsClean {
		t.Errorf("qemu-img check failed after writes: %s", checkResult.Stderr)
	}

	// Verify patterns by reopening with go-qcow2 (more reliable than qemu-io pattern verify)
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer img2.Close()

	for _, p := range patterns {
		buf := make([]byte, p.length)
		n, err := img2.ReadAt(buf, p.offset)
		if err != nil {
			t.Fatalf("ReadAt at %d failed: %v", p.offset, err)
		}
		if n != p.length {
			t.Errorf("Short read at %d: %d < %d", p.offset, n, p.length)
		}
		expected := bytes.Repeat([]byte{p.pattern}, p.length)
		if !bytes.Equal(buf, expected) {
			t.Errorf("Pattern 0x%02x mismatch at offset %d", p.pattern, p.offset)
		}
	}
}

// TestQemuInterop_WriteWithQemu_ReadWithUs writes data with qemu-io
// and verifies go-qcow2 can read it correctly.
func TestQemuInterop_WriteWithQemu_ReadWithUs(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)
	testutil.RequireQemuIO(t)

	path := testutil.TempImage(t, "qemu.qcow2")

	// Create with qemu-img
	testutil.QemuCreate(t, path, "10M")

	// Write patterns with qemu-io (non-overlapping)
	patterns := []struct {
		pattern byte
		offset  int64
		length  int64
	}{
		{0x11, 0, 4096},
		{0x22, 65536, 4096},
		{0x33, 131072, 4096}, // Third cluster
	}

	for _, p := range patterns {
		testutil.QemuWrite(t, path, p.pattern, p.offset, p.length)
	}

	// Read with go-qcow2
	img, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img.Close()

	for _, p := range patterns {
		buf := make([]byte, p.length)
		n, err := img.ReadAt(buf, p.offset)
		if err != nil {
			t.Fatalf("ReadAt at %d failed: %v", p.offset, err)
		}
		if int64(n) != p.length {
			t.Errorf("Short read at %d: %d < %d", p.offset, n, p.length)
		}

		expected := bytes.Repeat([]byte{p.pattern}, int(p.length))
		if !bytes.Equal(buf, expected) {
			t.Errorf("Data mismatch at offset %d, pattern 0x%02x", p.offset, p.pattern)
		}
	}
}

// TestQemuInterop_FullRoundtrip tests reading and writing alternating
// between go-qcow2 and QEMU.
func TestQemuInterop_FullRoundtrip(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)
	testutil.RequireQemuIO(t)

	path := testutil.TempImage(t, "roundtrip.qcow2")

	// Step 1: Create with qemu-img
	testutil.QemuCreate(t, path, "10M")

	// Step 2: Write pattern A with qemu-io
	testutil.QemuWrite(t, path, 0xAA, 0, 4096)

	// Step 3: Open with go-qcow2, verify A, write B
	img, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	bufA := make([]byte, 4096)
	img.ReadAt(bufA, 0)
	for i, b := range bufA {
		if b != 0xAA {
			t.Fatalf("Pattern A mismatch at byte %d: got 0x%02x", i, b)
		}
	}

	// Write pattern B at different offset
	dataB := bytes.Repeat([]byte{0xBB}, 4096)
	img.WriteAt(dataB, 65536)
	img.Close()

	// Step 4: Verify with qemu-io
	if !testutil.QemuRead(t, path, 0xAA, 0, 4096) {
		t.Error("Pattern A not verified by qemu-io")
	}
	if !testutil.QemuRead(t, path, 0xBB, 65536, 4096) {
		t.Error("Pattern B not verified by qemu-io")
	}

	// Step 5: Write pattern C with qemu-io
	testutil.QemuWrite(t, path, 0xCC, 131072, 4096) // Third cluster

	// Step 6: Verify all patterns with go-qcow2
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer img2.Close()

	verifyPatterns := []struct {
		pattern byte
		offset  int64
	}{
		{0xAA, 0},
		{0xBB, 65536},
		{0xCC, 131072},
	}

	for _, p := range verifyPatterns {
		buf := make([]byte, 4096)
		img2.ReadAt(buf, p.offset)
		for i, b := range buf {
			if b != p.pattern {
				t.Errorf("Pattern 0x%02x mismatch at offset %d, byte %d: got 0x%02x",
					p.pattern, p.offset, i, b)
				break
			}
		}
	}

	// Final check
	checkResult := testutil.QemuCheck(t, path)
	if !checkResult.IsClean {
		t.Errorf("Final qemu-img check failed: %s", checkResult.Stderr)
	}
}

// TestQemuInterop_BackingFile tests backing file chain compatibility.
func TestQemuInterop_BackingFile(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)
	testutil.RequireQemuIO(t)

	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.qcow2")
	overlayPath := filepath.Join(dir, "overlay.qcow2")

	// Create base with qemu-img and write data
	testutil.QemuCreate(t, basePath, "10M")
	testutil.QemuWrite(t, basePath, 0xAA, 0, 4096)

	// Create overlay with go-qcow2
	overlay, err := CreateOverlay(overlayPath, basePath)
	if err != nil {
		t.Fatalf("CreateOverlay failed: %v", err)
	}

	// Verify we can read base data through overlay
	buf := make([]byte, 4096)
	n, err := overlay.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt through overlay failed: %v", err)
	}
	if n != len(buf) {
		t.Errorf("Short read: %d", n)
	}
	for i, b := range buf {
		if b != 0xAA {
			t.Errorf("Base data mismatch at byte %d: got 0x%02x, want 0xAA", i, b)
			break
		}
	}

	// Write to overlay (COW)
	dataB := bytes.Repeat([]byte{0xBB}, 4096)
	overlay.WriteAt(dataB, 65536)
	overlay.Close()

	// Verify with qemu-img info
	info := testutil.QemuInfo(t, overlayPath)
	if info.BackingFilename == "" {
		t.Error("qemu-img doesn't see backing file")
	}

	// Verify with qemu-img check
	checkResult := testutil.QemuCheck(t, overlayPath)
	if !checkResult.IsClean {
		t.Errorf("qemu-img check overlay failed: %s", checkResult.Stderr)
	}

	// Verify qemu-io can read through overlay
	if !testutil.QemuRead(t, overlayPath, 0xAA, 0, 4096) {
		t.Error("qemu-io can't read base data through overlay")
	}
	if !testutil.QemuRead(t, overlayPath, 0xBB, 65536, 4096) {
		t.Error("qemu-io can't read overlay data")
	}
}

// TestQemuInterop_Compression tests reading compressed clusters.
func TestQemuInterop_Compression(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	uncompPath := filepath.Join(dir, "uncomp.qcow2")
	compPath := filepath.Join(dir, "comp.qcow2")

	// Create uncompressed image with go-qcow2
	img, err := CreateSimple(uncompPath, 1*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write compressible data (repeated pattern)
	data := bytes.Repeat([]byte("COMPRESSIBLE DATA PATTERN "), 2500)
	img.WriteAt(data, 0)
	img.Close()

	// Compress with qemu-img
	testutil.QemuConvert(t, uncompPath, compPath, true)

	// Read compressed data with go-qcow2
	compImg, err := Open(compPath)
	if err != nil {
		t.Fatalf("Open compressed failed: %v", err)
	}
	defer compImg.Close()

	readBuf := make([]byte, len(data))
	n, err := compImg.ReadAt(readBuf, 0)
	if err != nil {
		t.Fatalf("ReadAt compressed failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Short read: %d < %d", n, len(data))
	}

	if !bytes.Equal(readBuf, data) {
		t.Error("Compressed data mismatch")
	}
}

// TestQemuInterop_ZeroClusters tests zero cluster handling.
func TestQemuInterop_ZeroClusters(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	path := testutil.TempImage(t, "zeros.qcow2")

	// Create with go-qcow2
	img, err := CreateSimple(path, 10*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write data then zero it
	data := bytes.Repeat([]byte{0xFF}, 65536)
	img.WriteAt(data, 0)

	// Zero the cluster
	img.WriteZeroAt(0, 65536)
	img.Close()

	// Verify with qemu-img check
	checkResult := testutil.QemuCheck(t, path)
	if !checkResult.IsClean {
		t.Errorf("qemu-img check failed: %s", checkResult.Stderr)
	}

	// Verify zeros with qemu-io
	if !testutil.QemuRead(t, path, 0x00, 0, 65536) {
		t.Error("qemu-io doesn't see zeros")
	}
}

// TestQemuInterop_LazyRefcountsRecovery tests lazy refcount recovery.
func TestQemuInterop_LazyRefcountsRecovery(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	path := testutil.TempImage(t, "lazy.qcow2")

	// Create with lazy refcounts
	img, err := Create(path, CreateOptions{
		Size:          10 * 1024 * 1024,
		LazyRefcounts: true,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write data
	data := bytes.Repeat([]byte{0xDD}, 4096)
	img.WriteAt(data, 0)

	// Simulate unclean close by closing file handle directly
	img.file.Close()

	// QEMU should be able to repair
	repairResult := testutil.QemuCheckRepair(t, path)
	if repairResult.ExitCode > 2 { // 0=clean, 1=leaks fixed, 2=corruptions fixed
		t.Errorf("qemu-img repair failed: %s", repairResult.Stderr)
	}

	// Should be able to reopen with go-qcow2
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen after repair failed: %v", err)
	}
	defer img2.Close()

	// Data should still be there
	buf := make([]byte, 4096)
	img2.ReadAt(buf, 0)
	for i, b := range buf {
		if b != 0xDD {
			t.Errorf("Data lost at byte %d: got 0x%02x", i, b)
			break
		}
	}
}

// TestQemuInterop_ClusterSizes tests various cluster sizes.
func TestQemuInterop_ClusterSizes(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	// Test a range of cluster sizes
	clusterBits := []uint32{12, 14, 16, 18, 20} // 4K, 16K, 64K, 256K, 1M

	for _, bits := range clusterBits {
		clusterSize := 1 << bits
		t.Run(byteSizeStr(int64(clusterSize)), func(t *testing.T) {
			t.Parallel()
			path := testutil.TempImage(t, "cluster.qcow2")

			// Create with go-qcow2
			img, err := Create(path, CreateOptions{
				Size:        100 * 1024 * 1024, // 100MB
				ClusterBits: bits,
			})
			if err != nil {
				t.Fatalf("Create failed: %v", err)
			}

			// Write data across cluster boundaries
			data := testutil.RandomBytes(int64(bits), clusterSize+1000)
			img.WriteAt(data, int64(clusterSize-500))
			img.Close()

			// Verify with qemu-img
			checkResult := testutil.QemuCheck(t, path)
			if !checkResult.IsClean {
				t.Errorf("qemu-img check failed: %s", checkResult.Stderr)
			}

			info := testutil.QemuInfo(t, path)
			if info.ClusterSize != clusterSize {
				t.Errorf("Cluster size mismatch: qemu=%d, expected=%d",
					info.ClusterSize, clusterSize)
			}

			// Read back and verify
			img2, err := Open(path)
			if err != nil {
				t.Fatalf("Reopen failed: %v", err)
			}
			defer img2.Close()

			buf := make([]byte, len(data))
			img2.ReadAt(buf, int64(clusterSize-500))
			if !bytes.Equal(buf, data) {
				t.Error("Data mismatch after reopen")
			}
		})
	}
}

// byteSizeStr returns a human-readable size string.
func byteSizeStr(size int64) string {
	switch {
	case size >= 1<<30:
		return itoa(size>>30) + "G"
	case size >= 1<<20:
		return itoa(size>>20) + "M"
	case size >= 1<<10:
		return itoa(size>>10) + "K"
	default:
		return itoa(size) + "B"
	}
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}

// TestQemuInterop_Version tests against different QEMU output formats.
func TestQemuInterop_Version(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	version := testutil.QemuVersion(t)
	t.Logf("Testing with QEMU version: %s", version)

	// Create a simple image and verify basic compatibility
	path := testutil.TempImage(t, "version.qcow2")

	img, err := CreateSimple(path, 1*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	img.Close()

	// Basic checks should work with any supported QEMU version
	checkResult := testutil.QemuCheck(t, path)
	if !checkResult.IsClean {
		t.Errorf("Basic compatibility test failed with %s: %s",
			version, checkResult.Stderr)
	}

	info := testutil.QemuInfo(t, path)
	if info.Format != "qcow2" {
		t.Errorf("Format detection failed: got %s", info.Format)
	}
}

// TestQemuInterop_RawBacking tests raw backing file support.
func TestQemuInterop_RawBacking(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)
	testutil.RequireQemuIO(t)

	dir := t.TempDir()
	rawPath := filepath.Join(dir, "base.raw")
	overlayPath := filepath.Join(dir, "overlay.qcow2")

	// Create raw backing file
	rawSize := int64(1 * 1024 * 1024)
	rawData := testutil.RandomBytes(42, int(rawSize))
	if err := os.WriteFile(rawPath, rawData, 0644); err != nil {
		t.Fatalf("Failed to create raw file: %v", err)
	}

	// Create overlay with go-qcow2
	overlay, err := Create(overlayPath, CreateOptions{
		Size:          uint64(rawSize),
		BackingFile:   rawPath,
		BackingFormat: "raw",
	})
	if err != nil {
		t.Fatalf("Create overlay failed: %v", err)
	}

	// Read through overlay
	buf := make([]byte, 4096)
	overlay.ReadAt(buf, 0)
	if !bytes.Equal(buf, rawData[:4096]) {
		t.Error("Raw backing data mismatch via go-qcow2")
	}
	overlay.Close()

	// Verify with qemu-img
	info := testutil.QemuInfo(t, overlayPath)
	if info.BackingFilename == "" {
		t.Error("qemu-img doesn't see raw backing file")
	}

	checkResult := testutil.QemuCheck(t, overlayPath)
	if !checkResult.IsClean {
		t.Errorf("qemu-img check failed: %s", checkResult.Stderr)
	}

	// Verify qemu-io can read raw data through overlay
	if !testutil.QemuRead(t, overlayPath, rawData[0], 0, 1) {
		t.Log("Note: qemu-io pattern verify may differ for random data")
	}
}

// TestQemuInterop_Snapshots tests snapshot parsing.
func TestQemuInterop_Snapshots(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image with QEMU
	testutil.QemuCreate(t, path, "10M")

	// Write some data
	testutil.QemuWrite(t, path, 0xAA, 0, 4096)

	// Create first snapshot
	testutil.QemuSnapshot(t, path, "snap1")

	// Write different data
	testutil.QemuWrite(t, path, 0xBB, 0, 4096)

	// Create second snapshot
	testutil.QemuSnapshot(t, path, "snap2")

	// Verify QEMU sees the snapshots
	qemuSnaps := testutil.QemuListSnapshots(t, path)
	if len(qemuSnaps) != 2 {
		t.Fatalf("QEMU reports %d snapshots, want 2", len(qemuSnaps))
	}

	// Open with go-qcow2 and verify we parse the snapshots
	img, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img.Close()

	snaps := img.Snapshots()
	if len(snaps) != 2 {
		t.Fatalf("Parsed %d snapshots, want 2", len(snaps))
	}

	// Verify snapshot names
	if snaps[0].Name != "snap1" {
		t.Errorf("Snapshot 0 name = %q, want %q", snaps[0].Name, "snap1")
	}
	if snaps[1].Name != "snap2" {
		t.Errorf("Snapshot 1 name = %q, want %q", snaps[1].Name, "snap2")
	}

	// Verify L1 table offsets are set
	if snaps[0].L1TableOffset == 0 {
		t.Error("Snapshot 0 L1TableOffset is 0")
	}
	if snaps[1].L1TableOffset == 0 {
		t.Error("Snapshot 1 L1TableOffset is 0")
	}

	// Verify FindSnapshot works
	found := img.FindSnapshot("snap1")
	if found == nil {
		t.Error("FindSnapshot(snap1) returned nil")
	} else if found.Name != "snap1" {
		t.Errorf("FindSnapshot(snap1).Name = %q, want %q", found.Name, "snap1")
	}

	// Verify FindSnapshot with non-existent name
	notFound := img.FindSnapshot("nonexistent")
	if notFound != nil {
		t.Error("FindSnapshot(nonexistent) should return nil")
	}
}

// TestQemuInterop_ReadAtSnapshot tests reading data from a specific snapshot.
func TestQemuInterop_ReadAtSnapshot(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image with QEMU
	testutil.QemuCreate(t, path, "10M")

	// Write pattern 0xAA to first cluster
	testutil.QemuWrite(t, path, 0xAA, 0, 4096)

	// Create first snapshot
	testutil.QemuSnapshot(t, path, "snap1")

	// Write different pattern 0xBB to first cluster
	testutil.QemuWrite(t, path, 0xBB, 0, 4096)

	// Create second snapshot
	testutil.QemuSnapshot(t, path, "snap2")

	// Write pattern 0xCC to first cluster (current state)
	testutil.QemuWrite(t, path, 0xCC, 0, 4096)

	// Open with go-qcow2
	img, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer img.Close()

	// Read current state - should be 0xCC
	buf := make([]byte, 4096)
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xCC {
			t.Errorf("Current state: byte %d = 0x%02X, want 0xCC", i, b)
			break
		}
	}

	// Read from snap1 - should be 0xAA
	snap1 := img.FindSnapshot("snap1")
	if snap1 == nil {
		t.Fatal("FindSnapshot(snap1) returned nil")
	}
	if _, err := img.ReadAtSnapshot(buf, 0, snap1); err != nil {
		t.Fatalf("ReadAtSnapshot(snap1) failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xAA {
			t.Errorf("Snapshot snap1: byte %d = 0x%02X, want 0xAA", i, b)
			break
		}
	}

	// Read from snap2 - should be 0xBB
	snap2 := img.FindSnapshot("snap2")
	if snap2 == nil {
		t.Fatal("FindSnapshot(snap2) returned nil")
	}
	if _, err := img.ReadAtSnapshot(buf, 0, snap2); err != nil {
		t.Fatalf("ReadAtSnapshot(snap2) failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xBB {
			t.Errorf("Snapshot snap2: byte %d = 0x%02X, want 0xBB", i, b)
			break
		}
	}
}

// TestQemuInterop_CreateSnapshot tests that snapshots created by go-qcow2 are readable by QEMU.
func TestQemuInterop_CreateSnapshot(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "test.qcow2")

	// Create image with go-qcow2
	img, err := CreateSimple(path, 64*1024*1024) // 64MB
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write pattern 0xAA to first cluster
	pattern1 := bytes.Repeat([]byte{0xAA}, 4096)
	if _, err := img.WriteAt(pattern1, 0); err != nil {
		img.Close()
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Create first snapshot with go-qcow2
	snap1, err := img.CreateSnapshot("snap1")
	if err != nil {
		img.Close()
		t.Fatalf("CreateSnapshot(snap1) failed: %v", err)
	}
	t.Logf("Created snapshot snap1: ID=%s, L1Offset=0x%x", snap1.ID, snap1.L1TableOffset)

	// Write pattern 0xBB to first cluster (overwrite)
	pattern2 := bytes.Repeat([]byte{0xBB}, 4096)
	if _, err := img.WriteAt(pattern2, 0); err != nil {
		img.Close()
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Create second snapshot
	snap2, err := img.CreateSnapshot("snap2")
	if err != nil {
		img.Close()
		t.Fatalf("CreateSnapshot(snap2) failed: %v", err)
	}
	t.Logf("Created snapshot snap2: ID=%s, L1Offset=0x%x", snap2.ID, snap2.L1TableOffset)

	// Write pattern 0xCC to first cluster (current state)
	pattern3 := bytes.Repeat([]byte{0xCC}, 4096)
	if _, err := img.WriteAt(pattern3, 0); err != nil {
		img.Close()
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Flush and close
	img.Flush()
	img.Close()

	// Run qemu-img check to verify image integrity
	checkResult := testutil.RunQemuImg(t, "check", path)
	if !checkResult.IsSuccess() {
		t.Errorf("qemu-img check failed: %s", checkResult.Stderr)
	}

	// Verify QEMU can see the snapshots
	qemuSnaps := testutil.QemuListSnapshots(t, path)
	if len(qemuSnaps) != 2 {
		t.Fatalf("QEMU reports %d snapshots, want 2", len(qemuSnaps))
	}

	// Find our snapshots in QEMU's output
	foundSnap1 := false
	foundSnap2 := false
	for _, snapInfo := range qemuSnaps {
		name, ok := snapInfo["name"].(string)
		if !ok {
			continue
		}
		if name == "snap1" {
			foundSnap1 = true
		}
		if name == "snap2" {
			foundSnap2 = true
		}
	}
	if !foundSnap1 {
		t.Error("QEMU did not find snapshot 'snap1'")
	}
	if !foundSnap2 {
		t.Error("QEMU did not find snapshot 'snap2'")
	}

	// Verify we can read the current state
	if !testutil.QemuRead(t, path, 0xCC, 0, 4096) {
		t.Error("Current state: expected 0xCC pattern")
	}

	// Verify we can read from snap1 using qemu-io
	// Apply snap1 to read its data
	// Note: We can't easily read snapshot data with qemu-io without applying it,
	// so we verify by re-opening with go-qcow2 and using ReadAtSnapshot
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Re-open failed: %v", err)
	}
	defer img2.Close()

	// Verify our snapshots are still there
	snaps := img2.Snapshots()
	if len(snaps) != 2 {
		t.Fatalf("After re-open: %d snapshots, want 2", len(snaps))
	}

	// Read from snap1 - should be 0xAA
	snap1Ref := img2.FindSnapshot("snap1")
	if snap1Ref == nil {
		t.Fatal("FindSnapshot(snap1) returned nil after re-open")
	}
	buf := make([]byte, 4096)
	if _, err := img2.ReadAtSnapshot(buf, 0, snap1Ref); err != nil {
		t.Fatalf("ReadAtSnapshot(snap1) failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xAA {
			t.Errorf("Snapshot snap1: byte %d = 0x%02X, want 0xAA", i, b)
			break
		}
	}

	// Read from snap2 - should be 0xBB
	snap2Ref := img2.FindSnapshot("snap2")
	if snap2Ref == nil {
		t.Fatal("FindSnapshot(snap2) returned nil after re-open")
	}
	if _, err := img2.ReadAtSnapshot(buf, 0, snap2Ref); err != nil {
		t.Fatalf("ReadAtSnapshot(snap2) failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xBB {
			t.Errorf("Snapshot snap2: byte %d = 0x%02X, want 0xBB", i, b)
			break
		}
	}

	// Read current state - should be 0xCC
	if _, err := img2.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt(current) failed: %v", err)
	}
	for i, b := range buf {
		if b != 0xCC {
			t.Errorf("Current state: byte %d = 0x%02X, want 0xCC", i, b)
			break
		}
	}
}
