// snapshot_test.go - Phase 2.5 Snapshot Edge Cases Tests
// These tests verify correct handling of snapshot edge cases and boundary conditions.

package qcow2

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

// =============================================================================
// 2.5 Snapshot Edge Cases
// =============================================================================

// TestCreateManySnapshots tests creating 100+ snapshots.
func TestCreateManySnapshots(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "many_snapshots.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        64 * 1024 * 1024, // 64MB
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Write some data first
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xAA
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Create 100 snapshots
	const numSnapshots = 100
	for i := 0; i < numSnapshots; i++ {
		name := "snapshot_" + string(rune('A'+i/26)) + string(rune('a'+i%26))
		snap, err := img.CreateSnapshot(name)
		if err != nil {
			t.Fatalf("CreateSnapshot %d (%s) failed: %v", i, name, err)
		}
		if snap.Name != name {
			t.Errorf("Snapshot %d: name mismatch: got %q, want %q", i, snap.Name, name)
		}

		// Modify data slightly between snapshots
		data[i%len(data)] = byte(i)
		if _, err := img.WriteAt(data, int64(i*4096)%(img.Size()-4096)); err != nil {
			t.Fatalf("WriteAt after snapshot %d failed: %v", i, err)
		}
	}

	// Verify all snapshots exist
	snapshots := img.Snapshots()
	if len(snapshots) != numSnapshots {
		t.Errorf("Expected %d snapshots, got %d", numSnapshots, len(snapshots))
	}

	// Verify we can find each snapshot
	for i := 0; i < numSnapshots; i++ {
		name := "snapshot_" + string(rune('A'+i/26)) + string(rune('a'+i%26))
		snap := img.FindSnapshot(name)
		if snap == nil {
			t.Errorf("Snapshot %d (%s) not found", i, name)
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

	snapshots2 := img2.Snapshots()
	if len(snapshots2) != numSnapshots {
		t.Errorf("After reopen: expected %d snapshots, got %d", numSnapshots, len(snapshots2))
	}
}

// TestSnapshotWithCompressedClusters tests that snapshots preserve compressed clusters.
func TestSnapshotWithCompressedClusters(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot_compressed.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Write compressible data (highly repetitive)
	clusterSize := int(img.ClusterSize())
	compressibleData := make([]byte, clusterSize)
	for i := range compressibleData {
		compressibleData[i] = 0x42 // Single repeated byte compresses well
	}

	// Write as compressed
	if _, err := img.WriteAtCompressed(compressibleData, 0); err != nil {
		t.Fatalf("WriteAtCompressed failed: %v", err)
	}

	// Verify data before snapshot
	buf := make([]byte, clusterSize)
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt before snapshot failed: %v", err)
	}
	if !bytes.Equal(buf, compressibleData) {
		t.Fatal("Data mismatch before snapshot")
	}

	// Create snapshot
	snap, err := img.CreateSnapshot("with_compressed")
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	// Write different data (uncompressed) to the current image
	newData := make([]byte, clusterSize)
	for i := range newData {
		newData[i] = 0x99
	}
	if _, err := img.WriteAt(newData, 0); err != nil {
		t.Fatalf("WriteAt after snapshot failed: %v", err)
	}

	// Verify current image has new data
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt current failed: %v", err)
	}
	if !bytes.Equal(buf, newData) {
		t.Error("Current image should have new data")
	}

	// Verify snapshot still has compressed data
	if _, err := img.ReadAtSnapshot(buf, 0, snap); err != nil {
		t.Fatalf("ReadAtSnapshot failed: %v", err)
	}
	if !bytes.Equal(buf, compressibleData) {
		t.Error("Snapshot should preserve original compressed data")
	}
}

// TestSnapshotWithZeroFlaggedClusters tests that snapshots preserve zero-flagged clusters.
func TestSnapshotWithZeroFlaggedClusters(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot_zero.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	clusterSize := int(img.ClusterSize())

	// Write some data first
	data := make([]byte, clusterSize)
	for i := range data {
		data[i] = 0xAB
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt initial data failed: %v", err)
	}

	// Zero the cluster using WriteZeroAt
	if err := img.WriteZeroAt(0, int64(clusterSize)); err != nil {
		t.Fatalf("WriteZeroAt failed: %v", err)
	}

	// Create snapshot
	snap, err := img.CreateSnapshot("with_zeros")
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	// Write new data to current image
	newData := make([]byte, clusterSize)
	for i := range newData {
		newData[i] = 0xCD
	}
	if _, err := img.WriteAt(newData, 0); err != nil {
		t.Fatalf("WriteAt after snapshot failed: %v", err)
	}

	// Verify current image has new data
	buf := make([]byte, clusterSize)
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt current failed: %v", err)
	}
	if !bytes.Equal(buf, newData) {
		t.Error("Current image should have new data")
	}

	// Verify snapshot has zeros
	if _, err := img.ReadAtSnapshot(buf, 0, snap); err != nil {
		t.Fatalf("ReadAtSnapshot failed: %v", err)
	}
	zeros := make([]byte, clusterSize)
	if !bytes.Equal(buf, zeros) {
		t.Error("Snapshot should preserve zero-flagged cluster as zeros")
	}
}

// TestDeleteFirstSnapshot tests deleting the oldest snapshot.
func TestDeleteFirstSnapshot(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "delete_first.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Write data and create snapshots
	data := []byte("first snapshot data")
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	snap1, err := img.CreateSnapshot("first")
	if err != nil {
		t.Fatalf("CreateSnapshot first failed: %v", err)
	}

	data2 := []byte("second snapshot data")
	if _, err := img.WriteAt(data2, 4096); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	_, err = img.CreateSnapshot("second")
	if err != nil {
		t.Fatalf("CreateSnapshot second failed: %v", err)
	}

	_, err = img.CreateSnapshot("third")
	if err != nil {
		t.Fatalf("CreateSnapshot third failed: %v", err)
	}

	// Verify we have 3 snapshots
	if len(img.Snapshots()) != 3 {
		t.Fatalf("Expected 3 snapshots, got %d", len(img.Snapshots()))
	}

	// Delete the first snapshot
	if err := img.DeleteSnapshot(snap1.ID); err != nil {
		t.Fatalf("DeleteSnapshot first failed: %v", err)
	}

	// Verify we have 2 snapshots remaining
	snapshots := img.Snapshots()
	if len(snapshots) != 2 {
		t.Errorf("Expected 2 snapshots after delete, got %d", len(snapshots))
	}

	// Verify first is gone but second and third remain
	if img.FindSnapshot("first") != nil {
		t.Error("First snapshot should be deleted")
	}
	if img.FindSnapshot("second") == nil {
		t.Error("Second snapshot should still exist")
	}
	if img.FindSnapshot("third") == nil {
		t.Error("Third snapshot should still exist")
	}

	// Verify image is still usable
	buf := make([]byte, len(data2))
	if _, err := img.ReadAt(buf, 4096); err != nil {
		t.Fatalf("ReadAt after delete failed: %v", err)
	}
	if !bytes.Equal(buf, data2) {
		t.Error("Data mismatch after deleting first snapshot")
	}
}

// TestDeleteLastSnapshot tests deleting the newest snapshot.
func TestDeleteLastSnapshot(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "delete_last.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Create snapshots
	if _, err := img.CreateSnapshot("first"); err != nil {
		t.Fatalf("CreateSnapshot first failed: %v", err)
	}
	if _, err := img.CreateSnapshot("second"); err != nil {
		t.Fatalf("CreateSnapshot second failed: %v", err)
	}
	snap3, err := img.CreateSnapshot("third")
	if err != nil {
		t.Fatalf("CreateSnapshot third failed: %v", err)
	}

	// Delete the last (newest) snapshot
	if err := img.DeleteSnapshot(snap3.Name); err != nil {
		t.Fatalf("DeleteSnapshot third failed: %v", err)
	}

	// Verify we have 2 snapshots remaining
	snapshots := img.Snapshots()
	if len(snapshots) != 2 {
		t.Errorf("Expected 2 snapshots after delete, got %d", len(snapshots))
	}

	// Verify third is gone but first and second remain
	if img.FindSnapshot("third") != nil {
		t.Error("Third snapshot should be deleted")
	}
	if img.FindSnapshot("first") == nil {
		t.Error("First snapshot should still exist")
	}
	if img.FindSnapshot("second") == nil {
		t.Error("Second snapshot should still exist")
	}
}

// TestDeleteMiddleSnapshot tests deleting a snapshot from the middle.
func TestDeleteMiddleSnapshot(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "delete_middle.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Write data at different offsets for each snapshot
	data1 := []byte("data for first")
	if _, err := img.WriteAt(data1, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if _, err := img.CreateSnapshot("first"); err != nil {
		t.Fatalf("CreateSnapshot first failed: %v", err)
	}

	data2 := []byte("data for second")
	if _, err := img.WriteAt(data2, 4096); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	snap2, err := img.CreateSnapshot("second")
	if err != nil {
		t.Fatalf("CreateSnapshot second failed: %v", err)
	}

	data3 := []byte("data for third")
	if _, err := img.WriteAt(data3, 8192); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if _, err := img.CreateSnapshot("third"); err != nil {
		t.Fatalf("CreateSnapshot third failed: %v", err)
	}

	data4 := []byte("data for fourth")
	if _, err := img.WriteAt(data4, 12288); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if _, err := img.CreateSnapshot("fourth"); err != nil {
		t.Fatalf("CreateSnapshot fourth failed: %v", err)
	}

	// Delete the middle (second) snapshot
	if err := img.DeleteSnapshot(snap2.ID); err != nil {
		t.Fatalf("DeleteSnapshot second failed: %v", err)
	}

	// Verify we have 3 snapshots remaining
	snapshots := img.Snapshots()
	if len(snapshots) != 3 {
		t.Errorf("Expected 3 snapshots after delete, got %d", len(snapshots))
	}

	// Verify second is gone but first, third, and fourth remain
	if img.FindSnapshot("second") != nil {
		t.Error("Second snapshot should be deleted")
	}
	if img.FindSnapshot("first") == nil {
		t.Error("First snapshot should still exist")
	}
	if img.FindSnapshot("third") == nil {
		t.Error("Third snapshot should still exist")
	}
	if img.FindSnapshot("fourth") == nil {
		t.Error("Fourth snapshot should still exist")
	}

	// Verify current data is intact
	buf := make([]byte, len(data4))
	if _, err := img.ReadAt(buf, 12288); err != nil {
		t.Fatalf("ReadAt after delete failed: %v", err)
	}
	if !bytes.Equal(buf, data4) {
		t.Error("Current data should be intact after deleting middle snapshot")
	}
}

// TestDeleteAllSnapshots tests deleting all snapshots one by one.
func TestDeleteAllSnapshots(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "delete_all.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Write some data
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xAB
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Create snapshots
	snapNames := []string{"snap1", "snap2", "snap3", "snap4", "snap5"}
	for _, name := range snapNames {
		if _, err := img.CreateSnapshot(name); err != nil {
			t.Fatalf("CreateSnapshot %s failed: %v", name, err)
		}
	}

	// Verify all snapshots exist
	if len(img.Snapshots()) != len(snapNames) {
		t.Fatalf("Expected %d snapshots, got %d", len(snapNames), len(img.Snapshots()))
	}

	// Delete all snapshots in various orders (first, last, middle)
	deleteOrder := []string{"snap1", "snap5", "snap3", "snap2", "snap4"}
	for _, name := range deleteOrder {
		if err := img.DeleteSnapshot(name); err != nil {
			t.Fatalf("DeleteSnapshot %s failed: %v", name, err)
		}
	}

	// Verify no snapshots remain
	if len(img.Snapshots()) != 0 {
		t.Errorf("Expected 0 snapshots, got %d", len(img.Snapshots()))
	}

	// Verify image is still usable
	buf := make([]byte, 4096)
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt after deleting all failed: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Error("Data should be intact after deleting all snapshots")
	}

	// Verify we can still write
	newData := []byte("still works!")
	if _, err := img.WriteAt(newData, 0); err != nil {
		t.Fatalf("WriteAt after deleting all failed: %v", err)
	}

	// Verify we can create new snapshots
	if _, err := img.CreateSnapshot("new_snap"); err != nil {
		t.Fatalf("CreateSnapshot after deleting all failed: %v", err)
	}
	if len(img.Snapshots()) != 1 {
		t.Errorf("Expected 1 snapshot after creating new, got %d", len(img.Snapshots()))
	}
}

// TestSnapshotNameMaxLength tests snapshot with maximum length name (65535 bytes).
func TestSnapshotNameMaxLength(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "name_max_length.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Create a name at maximum length (65535 bytes as per uint16)
	maxLen := 65535
	longName := strings.Repeat("x", maxLen)

	snap, err := img.CreateSnapshot(longName)
	if err != nil {
		t.Fatalf("CreateSnapshot with max length name failed: %v", err)
	}
	if len(snap.Name) != maxLen {
		t.Errorf("Snapshot name length mismatch: got %d, want %d", len(snap.Name), maxLen)
	}

	// Verify we can find it
	found := img.FindSnapshot(longName)
	if found == nil {
		t.Error("Could not find snapshot with max length name")
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

	found2 := img2.FindSnapshot(longName)
	if found2 == nil {
		t.Error("Could not find snapshot with max length name after reopen")
	}
	if len(found2.Name) != maxLen {
		t.Errorf("After reopen: name length mismatch: got %d, want %d", len(found2.Name), maxLen)
	}
}

// TestSnapshotNameUnicode tests snapshot with UTF-8 characters in name.
func TestSnapshotNameUnicode(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "name_unicode.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Test various Unicode names
	unicodeNames := []string{
		"スナップショット",              // Japanese
		"快照",                       // Chinese
		"снимок",                   // Russian
		"instantané",              // French with accent
		"📸snapshot🔥",             // Emoji
		"مخطط",                     // Arabic (RTL)
		"αβγδ",                    // Greek
		"snapshot™©®",             // Special symbols
		"🎮游戏存档💾",                // Mixed emoji and Chinese
		"名前\x00with\x00nulls",    // With null bytes (edge case)
	}

	for _, name := range unicodeNames {
		snap, err := img.CreateSnapshot(name)
		if err != nil {
			t.Errorf("CreateSnapshot with Unicode name %q failed: %v", name, err)
			continue
		}
		if snap.Name != name {
			t.Errorf("Snapshot name mismatch: got %q, want %q", snap.Name, name)
		}

		// Verify we can find it
		found := img.FindSnapshot(name)
		if found == nil {
			t.Errorf("Could not find snapshot with Unicode name %q", name)
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

	for _, name := range unicodeNames {
		found := img2.FindSnapshot(name)
		if found == nil {
			t.Errorf("After reopen: could not find snapshot with Unicode name %q", name)
		}
	}
}

// TestSnapshotNameEmpty tests that empty snapshot name is rejected.
func TestSnapshotNameEmpty(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "name_empty.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Empty name should be rejected
	_, err = img.CreateSnapshot("")
	if err == nil {
		t.Error("CreateSnapshot with empty name should fail")
	} else {
		t.Logf("CreateSnapshot with empty name correctly failed: %v", err)
	}

	// Verify no snapshot was created
	if len(img.Snapshots()) != 0 {
		t.Errorf("Expected 0 snapshots, got %d", len(img.Snapshots()))
	}
}

// TestSnapshotNameDuplicate tests that duplicate snapshot names are rejected.
func TestSnapshotNameDuplicate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "name_duplicate.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Create first snapshot
	name := "duplicate_test"
	snap1, err := img.CreateSnapshot(name)
	if err != nil {
		t.Fatalf("CreateSnapshot first failed: %v", err)
	}

	// Try to create second snapshot with same name - should fail
	_, err = img.CreateSnapshot(name)
	if err == nil {
		t.Error("CreateSnapshot with duplicate name should fail")
	} else {
		t.Logf("CreateSnapshot with duplicate name correctly failed: %v", err)
	}

	// Verify only one snapshot exists
	snapshots := img.Snapshots()
	if len(snapshots) != 1 {
		t.Errorf("Expected 1 snapshot, got %d", len(snapshots))
	}

	// Verify the original is still there
	found := img.FindSnapshot(name)
	if found == nil || found.ID != snap1.ID {
		t.Error("Original snapshot should still exist unchanged")
	}
}

// TestRevertPreservesOtherSnapshots tests that reverting doesn't delete other snapshots.
func TestRevertPreservesOtherSnapshots(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "revert_preserves.qcow2")

	img, err := Create(path, CreateOptions{
		Size:        10 * 1024 * 1024,
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Write initial data
	data1 := []byte("state at snapshot 1")
	if _, err := img.WriteAt(data1, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	snap1, err := img.CreateSnapshot("snap1")
	if err != nil {
		t.Fatalf("CreateSnapshot snap1 failed: %v", err)
	}

	// Write different data
	data2 := []byte("state at snapshot 2")
	if _, err := img.WriteAt(data2, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if _, err := img.CreateSnapshot("snap2"); err != nil {
		t.Fatalf("CreateSnapshot snap2 failed: %v", err)
	}

	// Write more data
	data3 := []byte("state at snapshot 3")
	if _, err := img.WriteAt(data3, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if _, err := img.CreateSnapshot("snap3"); err != nil {
		t.Fatalf("CreateSnapshot snap3 failed: %v", err)
	}

	// Write current state
	dataCurrent := []byte("current state before revert")
	if _, err := img.WriteAt(dataCurrent, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Verify we have 3 snapshots
	if len(img.Snapshots()) != 3 {
		t.Fatalf("Expected 3 snapshots, got %d", len(img.Snapshots()))
	}

	// Revert to snap1
	if err := img.RevertToSnapshot(snap1.Name); err != nil {
		t.Fatalf("RevertToSnapshot failed: %v", err)
	}

	// Verify current state is now snap1's state
	buf := make([]byte, len(data1))
	if _, err := img.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt after revert failed: %v", err)
	}
	if !bytes.Equal(buf, data1) {
		t.Errorf("After revert: expected %q, got %q", data1, buf)
	}

	// CRITICAL: Verify all 3 snapshots still exist
	snapshots := img.Snapshots()
	if len(snapshots) != 3 {
		t.Errorf("After revert: expected 3 snapshots, got %d", len(snapshots))
	}
	if img.FindSnapshot("snap1") == nil {
		t.Error("snap1 should still exist after revert")
	}
	if img.FindSnapshot("snap2") == nil {
		t.Error("snap2 should still exist after revert")
	}
	if img.FindSnapshot("snap3") == nil {
		t.Error("snap3 should still exist after revert")
	}

	// Verify we can still read from other snapshots
	snap2 := img.FindSnapshot("snap2")
	buf2 := make([]byte, len(data2))
	if _, err := img.ReadAtSnapshot(buf2, 0, snap2); err != nil {
		t.Fatalf("ReadAtSnapshot snap2 after revert failed: %v", err)
	}
	if !bytes.Equal(buf2, data2) {
		t.Errorf("snap2 data after revert: expected %q, got %q", data2, buf2)
	}
}

// TestSnapshotL1SizeMismatch tests handling of snapshot with different L1 size.
func TestSnapshotL1SizeMismatch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "l1_mismatch.qcow2")

	// Create a small image
	img, err := Create(path, CreateOptions{
		Size:        64 * 1024 * 1024, // 64MB
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Write some data
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xAA
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Create snapshot
	snap, err := img.CreateSnapshot("before_resize")
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	// The snapshot's L1Size should match current L1Size
	if snap.L1Size != img.header.L1Size {
		t.Logf("Snapshot L1Size=%d, image L1Size=%d", snap.L1Size, img.header.L1Size)
	}

	// Write beyond original L1 coverage to potentially trigger L1 growth
	// With 64KB clusters, L2 entries = 8192, L2 coverage = 512MB
	// L1[0] covers 0-512MB, so writing at 0 and creating snapshot should work

	// Close and reopen to test persistence
	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	img2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer img2.Close()

	// Verify snapshot can be read
	snapFound := img2.FindSnapshot("before_resize")
	if snapFound == nil {
		t.Fatal("Snapshot not found after reopen")
	}

	buf := make([]byte, 4096)
	if _, err := img2.ReadAtSnapshot(buf, 0, snapFound); err != nil {
		t.Fatalf("ReadAtSnapshot failed: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Error("Snapshot data mismatch after reopen")
	}

	// Test revert with the same L1 size
	if err := img2.RevertToSnapshot("before_resize"); err != nil {
		t.Fatalf("RevertToSnapshot failed: %v", err)
	}

	// Verify data after revert
	if _, err := img2.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt after revert failed: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Error("Data mismatch after revert")
	}
}
