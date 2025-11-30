package qcow2

import (
	"testing"
)

func TestFreeClusterBitmapBasic(t *testing.T) {
	t.Parallel()

	// Create bitmap for 1000 clusters, skipping first 4
	b := newFreeClusterBitmap(1000, 4)

	// Initially all clusters should be marked as used (0)
	for i := uint64(0); i < 1000; i++ {
		if b.isFree(i) {
			t.Errorf("Cluster %d should not be free initially", i)
		}
	}

	// Mark some clusters as free
	b.setFree(10)
	b.setFree(50)
	b.setFree(100)

	if !b.isFree(10) {
		t.Error("Cluster 10 should be free")
	}
	if !b.isFree(50) {
		t.Error("Cluster 50 should be free")
	}
	if !b.isFree(100) {
		t.Error("Cluster 100 should be free")
	}
	if b.isFree(11) {
		t.Error("Cluster 11 should not be free")
	}

	// Count free clusters
	count := b.countFree()
	if count != 3 {
		t.Errorf("Expected 3 free clusters, got %d", count)
	}
}

func TestFreeClusterBitmapFindFree(t *testing.T) {
	t.Parallel()

	b := newFreeClusterBitmap(1000, 4)

	// No free clusters initially
	_, found := b.findFree()
	if found {
		t.Error("Should not find free cluster when none marked")
	}

	// Mark cluster 100 as free
	b.setFree(100)

	clusterIdx, found := b.findFree()
	if !found {
		t.Error("Should find free cluster")
	}
	if clusterIdx != 100 {
		t.Errorf("Expected cluster 100, got %d", clusterIdx)
	}

	// After findFree, cluster should be marked used
	if b.isFree(100) {
		t.Error("Cluster 100 should be used after findFree")
	}

	// No more free clusters
	_, found = b.findFree()
	if found {
		t.Error("Should not find another free cluster")
	}
}

func TestFreeClusterBitmapMinCluster(t *testing.T) {
	t.Parallel()

	// Create bitmap with minCluster = 10
	b := newFreeClusterBitmap(100, 10)

	// Mark clusters below minCluster as free - they should be ignored
	b.setFree(5)
	b.setFree(8)

	// These should not be findable
	_, found := b.findFree()
	if found {
		t.Error("Should not find clusters below minCluster")
	}

	// Mark a cluster at minCluster
	b.setFree(10)
	clusterIdx, found := b.findFree()
	if !found {
		t.Error("Should find cluster at minCluster")
	}
	if clusterIdx != 10 {
		t.Errorf("Expected cluster 10, got %d", clusterIdx)
	}
}

func TestFreeClusterBitmapGrow(t *testing.T) {
	t.Parallel()

	b := newFreeClusterBitmap(100, 4)

	// Mark cluster 50 as free
	b.setFree(50)

	// Grow to 200 clusters
	b.grow(200)

	// Original free cluster should still be there
	if !b.isFree(50) {
		t.Error("Cluster 50 should still be free after grow")
	}

	// Can mark new clusters as free
	b.setFree(150)
	if !b.isFree(150) {
		t.Error("Cluster 150 should be free")
	}

	// Find should work for new clusters
	clusterIdx, found := b.findFree()
	if !found {
		t.Error("Should find free cluster after grow")
	}
	if clusterIdx != 50 && clusterIdx != 150 {
		t.Errorf("Expected cluster 50 or 150, got %d", clusterIdx)
	}
}

func TestFreeClusterBitmapWrapAround(t *testing.T) {
	t.Parallel()

	b := newFreeClusterBitmap(200, 4)

	// Mark clusters 10 and 150 as free
	b.setFree(10)
	b.setFree(150)

	// Find first (should get 10)
	clusterIdx, found := b.findFree()
	if !found || clusterIdx != 10 {
		t.Errorf("First findFree: expected 10, got %d (found=%v)", clusterIdx, found)
	}

	// Mark cluster 180 as free
	b.setFree(180)

	// Find next (should get 150 or 180)
	clusterIdx, found = b.findFree()
	if !found {
		t.Error("Second findFree should succeed")
	}
	if clusterIdx != 150 && clusterIdx != 180 {
		t.Errorf("Second findFree: expected 150 or 180, got %d", clusterIdx)
	}

	// Mark cluster 5 as free (below hint)
	b.setFree(5)

	// Find next - should wrap around and find remaining
	_, found = b.findFree()
	if !found {
		t.Error("Third findFree should succeed")
	}

	// Last findFree should wrap around
	_, found = b.findFree()
	if !found {
		t.Error("Fourth findFree should succeed")
	}

	// Now all should be used
	if b.countFree() != 0 {
		t.Errorf("All clusters should be used, got %d free", b.countFree())
	}
}

func TestFreeClusterBitmapConcurrent(t *testing.T) {
	t.Parallel()

	b := newFreeClusterBitmap(10000, 4)

	// Mark many clusters as free
	for i := uint64(100); i < 500; i++ {
		b.setFree(i)
	}

	// Concurrent findFree calls
	done := make(chan uint64, 400)
	for i := 0; i < 400; i++ {
		go func() {
			clusterIdx, found := b.findFree()
			if found {
				done <- clusterIdx
			} else {
				done <- 0
			}
		}()
	}

	// Collect results
	found := make(map[uint64]bool)
	for i := 0; i < 400; i++ {
		idx := <-done
		if idx != 0 {
			if found[idx] {
				t.Errorf("Cluster %d allocated twice", idx)
			}
			found[idx] = true
		}
	}

	// Should have found all 400 clusters
	if len(found) != 400 {
		t.Errorf("Expected 400 unique clusters, got %d", len(found))
	}
}

func TestFreeClusterBitmapEdgeCases(t *testing.T) {
	t.Parallel()

	// Test at word boundaries (64-bit)
	b := newFreeClusterBitmap(256, 4)

	// Mark clusters at word boundaries
	b.setFree(63)  // End of first word
	b.setFree(64)  // Start of second word
	b.setFree(127) // End of second word
	b.setFree(128) // Start of third word

	// All should be findable
	for i := 0; i < 4; i++ {
		_, found := b.findFree()
		if !found {
			t.Errorf("Iteration %d: should find free cluster at word boundary", i)
		}
	}

	// No more free
	_, found := b.findFree()
	if found {
		t.Error("Should not find more free clusters")
	}
}
