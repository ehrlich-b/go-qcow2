// l1growth_test.go - Phase 2.3 L1 Table Growth Tests
// These tests verify correct handling of L1 table growth scenarios.

package qcow2

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/ehrlich-b/go-qcow2/testutil"
)

// =============================================================================
// 2.3 L1 Table Growth Tests
// =============================================================================

// TestL1TableGrowsOnWrite tests writing beyond current L1 table coverage.
func TestL1TableGrowsOnWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "l1_grow.qcow2")

	// Create a small image initially
	// With 64KB clusters, each L2 table covers 8192 * 64KB = 512MB
	// So a 1GB image needs 2 L1 entries
	img, err := Create(path, CreateOptions{
		Size:        1024 * 1024 * 1024, // 1GB
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	initialL1Size := img.header.L1Size
	t.Logf("Initial L1 size: %d entries", initialL1Size)

	clusterSize := int64(img.ClusterSize())
	l2Entries := clusterSize / 8           // 8192 entries per L2 table
	l2Coverage := l2Entries * clusterSize  // 512MB per L2 table

	// Write to first L1 region (covered by L1[0])
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xAA
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt first region failed: %v", err)
	}

	// Write to second L1 region (covered by L1[1])
	for i := range data {
		data[i] = 0xBB
	}
	if _, err := img.WriteAt(data, l2Coverage); err != nil {
		t.Fatalf("WriteAt second region failed: %v", err)
	}

	// Verify both regions
	buf := make([]byte, 4096)
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt first region failed: %v", err)
	}
	for i := range buf {
		if buf[i] != 0xAA {
			t.Errorf("First region byte %d: expected 0xAA, got 0x%02X", i, buf[i])
			break
		}
	}

	if _, err := img.ReadAt(buf, l2Coverage); err != nil {
		t.Fatalf("ReadAt second region failed: %v", err)
	}
	for i := range buf {
		if buf[i] != 0xBB {
			t.Errorf("Second region byte %d: expected 0xBB, got 0x%02X", i, buf[i])
			break
		}
	}

	// Write near the end (last L1 entry)
	lastOffset := int64(img.Size()) - 4096
	for i := range data {
		data[i] = 0xCC
	}
	if _, err := img.WriteAt(data, lastOffset); err != nil {
		t.Fatalf("WriteAt last region failed: %v", err)
	}

	if _, err := img.ReadAt(buf, lastOffset); err != nil {
		t.Fatalf("ReadAt last region failed: %v", err)
	}
	for i := range buf {
		if buf[i] != 0xCC {
			t.Errorf("Last region byte %d: expected 0xCC, got 0x%02X", i, buf[i])
			break
		}
	}
}

// TestL1TableMaxSize tests creating an image requiring a large L1 table.
func TestL1TableMaxSize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "l1_max.qcow2")

	// Create a large virtual image
	// 1TB virtual size with 64KB clusters
	// Each L2 covers 512MB, so 1TB needs 2048 L1 entries
	virtualSize := uint64(1024) * 1024 * 1024 * 1024 // 1TB

	img, err := Create(path, CreateOptions{
		Size:        virtualSize,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	t.Logf("L1 size for 1TB image: %d entries", img.header.L1Size)

	clusterSize := int64(img.ClusterSize())
	l2Entries := clusterSize / 8
	l2Coverage := l2Entries * clusterSize

	// Write to start
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0x11
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt start failed: %v", err)
	}

	// Write near middle (512GB offset)
	middleOffset := int64(virtualSize / 2)
	for i := range data {
		data[i] = 0x22
	}
	if _, err := img.WriteAt(data, middleOffset); err != nil {
		t.Fatalf("WriteAt middle failed: %v", err)
	}

	// Write near end
	endOffset := int64(virtualSize) - 4096
	for i := range data {
		data[i] = 0x33
	}
	if _, err := img.WriteAt(data, endOffset); err != nil {
		t.Fatalf("WriteAt end failed: %v", err)
	}

	// Verify all writes
	buf := make([]byte, 4096)

	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt start failed: %v", err)
	}
	if buf[0] != 0x11 {
		t.Errorf("Start data mismatch: expected 0x11, got 0x%02X", buf[0])
	}

	if _, err := img.ReadAt(buf, middleOffset); err != nil {
		t.Fatalf("ReadAt middle failed: %v", err)
	}
	if buf[0] != 0x22 {
		t.Errorf("Middle data mismatch: expected 0x22, got 0x%02X", buf[0])
	}

	if _, err := img.ReadAt(buf, endOffset); err != nil {
		t.Fatalf("ReadAt end failed: %v", err)
	}
	if buf[0] != 0x33 {
		t.Errorf("End data mismatch: expected 0x33, got 0x%02X", buf[0])
	}

	// Calculate expected L1 entries
	expectedL1 := (virtualSize + uint64(l2Coverage) - 1) / uint64(l2Coverage)
	if uint64(img.header.L1Size) < expectedL1 {
		t.Errorf("L1 size %d less than expected %d for %d byte image",
			img.header.L1Size, expectedL1, virtualSize)
	}
}

// TestL1TableReallocation tests L1 table needing to move during growth.
func TestL1TableReallocation(t *testing.T) {
	t.Parallel()
	testutil.RequireQemu(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "l1_realloc.qcow2")

	// Create a small image first
	testutil.QemuCreate(t, path, "100M")

	img, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	initialL1Offset := img.header.L1TableOffset
	initialL1Size := img.header.L1Size
	t.Logf("Initial L1: offset=%d, size=%d", initialL1Offset, initialL1Size)

	// Write data to several locations
	clusterSize := int64(img.ClusterSize())
	data := make([]byte, clusterSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Write to multiple clusters
	for i := 0; i < 10; i++ {
		offset := int64(i) * clusterSize * 100
		if offset >= int64(img.Size()) {
			break
		}
		if _, err := img.WriteAt(data[:4096], offset); err != nil {
			t.Fatalf("WriteAt offset %d failed: %v", offset, err)
		}
	}

	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify
	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer img2.Close()

	t.Logf("After writes L1: offset=%d, size=%d", img2.header.L1TableOffset, img2.header.L1Size)

	// Verify data is still readable
	buf := make([]byte, 4096)
	if _, err := img2.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt after reopen failed: %v", err)
	}
	if !bytes.Equal(buf, data[:4096]) {
		t.Error("Data mismatch after L1 operations")
	}

	// Check integrity
	result, err := img2.Check()
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if result.Corruptions > 0 {
		t.Errorf("Check found %d corruptions", result.Corruptions)
	}
}

// TestL1TableMultipleL2Allocations tests allocating many L2 tables.
func TestL1TableMultipleL2Allocations(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "l1_multi_l2.qcow2")

	// Create image that spans multiple L2 tables
	// 2GB with 64KB clusters = 4 L2 tables
	img, err := Create(path, CreateOptions{
		Size:        2 * 1024 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	clusterSize := int64(img.ClusterSize())
	l2Entries := clusterSize / 8
	l2Coverage := l2Entries * clusterSize

	// Write to each L2 table's region
	numL2Tables := 4
	data := make([]byte, 4096)

	for i := 0; i < numL2Tables; i++ {
		offset := int64(i) * l2Coverage
		for j := range data {
			data[j] = byte(i)
		}
		if _, err := img.WriteAt(data, offset); err != nil {
			t.Fatalf("WriteAt L2 region %d failed: %v", i, err)
		}
	}

	// Verify all writes
	buf := make([]byte, 4096)
	for i := 0; i < numL2Tables; i++ {
		offset := int64(i) * l2Coverage
		if _, err := img.ReadAt(buf, offset); err != nil {
			t.Fatalf("ReadAt L2 region %d failed: %v", i, err)
		}
		expected := byte(i)
		if buf[0] != expected {
			t.Errorf("L2 region %d data mismatch: expected 0x%02X, got 0x%02X",
				i, expected, buf[0])
		}
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

	for i := 0; i < numL2Tables; i++ {
		offset := int64(i) * l2Coverage
		if _, err := img2.ReadAt(buf, offset); err != nil {
			t.Fatalf("ReadAt after reopen L2 region %d failed: %v", i, err)
		}
		expected := byte(i)
		if buf[0] != expected {
			t.Errorf("After reopen L2 region %d data mismatch: expected 0x%02X, got 0x%02X",
				i, expected, buf[0])
		}
	}
}

// TestL1IndexCalculation tests L1 index calculation at various offsets.
func TestL1IndexCalculation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "l1_index.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        4 * 1024 * 1024 * 1024, // 4GB
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	clusterSize := int64(img.ClusterSize())
	l2Entries := clusterSize / 8
	l2Coverage := l2Entries * clusterSize

	// Test specific offsets that should hit different L1 entries
	testCases := []struct {
		offset      int64
		expectedL1  int
	}{
		{0, 0},
		{l2Coverage - 1, 0},
		{l2Coverage, 1},
		{l2Coverage * 2, 2},
		{l2Coverage*2 + clusterSize*100, 2},
		{l2Coverage * 7, 7},
	}

	data := make([]byte, 4096)
	buf := make([]byte, 4096)

	for _, tc := range testCases {
		if tc.offset >= int64(img.Size()) {
			continue
		}

		// Write unique data for this L1 index
		for i := range data {
			data[i] = byte(tc.expectedL1)
		}

		if _, err := img.WriteAt(data, tc.offset); err != nil {
			t.Fatalf("WriteAt offset %d (L1=%d) failed: %v", tc.offset, tc.expectedL1, err)
		}

		if _, err := img.ReadAt(buf, tc.offset); err != nil {
			t.Fatalf("ReadAt offset %d (L1=%d) failed: %v", tc.offset, tc.expectedL1, err)
		}

		if !bytes.Equal(buf, data) {
			t.Errorf("Data mismatch at offset %d (L1=%d)", tc.offset, tc.expectedL1)
		}
	}
}
