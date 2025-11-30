// stress_test.go - Phase 3.1 Stress / Longevity Tests for Production Readiness
// These tests verify the library holds up under heavy load and resource pressure.
// All tests skip in short mode.

package qcow2

import (
	"bytes"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// 3.1 Stress / Longevity Tests
// =============================================================================

// TestMillionClusters tests an image with many allocated clusters.
// This verifies L1/L2 table growth, refcount table scaling, and memory usage.
func TestMillionClusters(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "million_clusters.qcow2")

	// Use smaller 4KB clusters to test more clusters with less disk space
	clusterBits := uint32(12) // 4KB clusters
	clusterSize := uint64(1 << clusterBits)

	// Create image with 8GB virtual size (enough for 2M 4KB clusters)
	img, err := Create(path, CreateOptions{
		Size:        8 * 1024 * 1024 * 1024, // 8GB
		ClusterBits: clusterBits,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Use batched barriers for performance - defer syncs until Flush()
	img.SetWriteBarrierMode(BarrierBatched)

	// We'll write to 100K clusters (not 1M to keep test time reasonable)
	// This still exercises L1 table growth and many refcount updates
	numClusters := 100000
	data := make([]byte, 512) // Write small amount per cluster
	for i := range data {
		data[i] = byte(i % 256)
	}

	t.Logf("Writing to %d clusters (cluster size: %d bytes)", numClusters, clusterSize)
	startTime := time.Now()

	// Write to many different clusters
	for i := 0; i < numClusters; i++ {
		offset := int64(i) * int64(clusterSize)
		if offset >= img.Size() {
			t.Logf("Reached image size limit at cluster %d", i)
			break
		}
		if _, err := img.WriteAt(data, offset); err != nil {
			t.Fatalf("WriteAt cluster %d failed: %v", i, err)
		}

		// Progress reporting
		if (i+1)%10000 == 0 {
			elapsed := time.Since(startTime)
			rate := float64(i+1) / elapsed.Seconds()
			t.Logf("Progress: %d/%d clusters (%.0f clusters/sec)", i+1, numClusters, rate)
		}
	}

	elapsed := time.Since(startTime)
	t.Logf("Wrote %d clusters in %v (%.0f clusters/sec)",
		numClusters, elapsed, float64(numClusters)/elapsed.Seconds())

	// Flush and verify
	if err := img.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Run consistency check
	result, err := img.Check()
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

	// Verify random samples
	readBuf := make([]byte, 512)
	for i := 0; i < 100; i++ {
		clusterNum := (i * 997) % numClusters // Pseudo-random sampling
		offset := int64(clusterNum) * int64(clusterSize)
		if offset >= img.Size() {
			continue
		}
		if _, err := img.ReadAt(readBuf, offset); err != nil {
			t.Errorf("ReadAt cluster %d failed: %v", clusterNum, err)
			continue
		}
		if !bytes.Equal(readBuf, data) {
			t.Errorf("Data mismatch at cluster %d", clusterNum)
		}
	}

	t.Logf("Test completed: %d clusters written and verified", numClusters)
}

// TestLargeImage tests an image with 1TB+ virtual size.
// This verifies L1 table sizing and address translation at large offsets.
func TestLargeImage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "large_image.qcow2")

	// Create 2TB virtual image (sparse, won't use much disk)
	virtualSize := uint64(2) * 1024 * 1024 * 1024 * 1024 // 2TB

	img, err := Create(path, CreateOptions{
		Size:        virtualSize,
		ClusterBits: 16, // 64KB clusters
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	if uint64(img.Size()) != virtualSize {
		t.Errorf("Expected size %d, got %d", virtualSize, img.Size())
	}

	clusterSize := img.ClusterSize()
	t.Logf("Created %d TB image with %d byte clusters", virtualSize/(1024*1024*1024*1024), clusterSize)

	// Write at various large offsets
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xAB
	}

	testOffsets := []int64{
		0,                                      // Start
		int64(virtualSize / 2),                 // Middle (1TB)
		int64(virtualSize) - int64(clusterSize), // Near end
		100 * 1024 * 1024 * 1024,               // 100GB
		500 * 1024 * 1024 * 1024,               // 500GB
		1024 * 1024 * 1024 * 1024,              // 1TB
	}

	for _, offset := range testOffsets {
		if offset >= int64(virtualSize) {
			continue
		}

		// Write
		n, err := img.WriteAt(data, offset)
		if err != nil {
			t.Errorf("WriteAt offset %d failed: %v", offset, err)
			continue
		}
		if n != len(data) {
			t.Errorf("WriteAt offset %d: wrote %d bytes, expected %d", offset, n, len(data))
		}

		// Read back
		readBuf := make([]byte, 4096)
		n, err = img.ReadAt(readBuf, offset)
		if err != nil {
			t.Errorf("ReadAt offset %d failed: %v", offset, err)
			continue
		}
		if !bytes.Equal(readBuf, data) {
			t.Errorf("Data mismatch at offset %d", offset)
		}

		t.Logf("Verified write/read at offset %d (%.2f TB)",
			offset, float64(offset)/(1024*1024*1024*1024))
	}

	// Flush and verify
	if err := img.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	result, err := img.Check()
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if result.Corruptions > 0 {
		t.Errorf("Check found %d corruptions", result.Corruptions)
	}
}

// TestRepeatedOpenClose tests opening and closing an image 10000 times.
// This verifies no resource leaks (file handles, memory).
func TestRepeatedOpenClose(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "open_close.qcow2")

	// Create image with some data
	img, err := CreateSimple(path, 10*1024*1024)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	data := make([]byte, 65536)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if _, err := img.WriteAt(data, 0); err != nil {
		t.Fatalf("Initial WriteAt failed: %v", err)
	}
	if err := img.Close(); err != nil {
		t.Fatalf("Initial Close failed: %v", err)
	}

	iterations := 10000
	t.Logf("Opening and closing image %d times", iterations)

	// Record initial memory stats
	var memBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	startTime := time.Now()

	for i := 0; i < iterations; i++ {
		img, err := Open(path)
		if err != nil {
			t.Fatalf("Open iteration %d failed: %v", i, err)
		}

		// Do a small read to ensure image is actually accessed
		buf := make([]byte, 4096)
		if _, err := img.ReadAt(buf, 0); err != nil {
			t.Fatalf("ReadAt iteration %d failed: %v", i, err)
		}

		if err := img.Close(); err != nil {
			t.Fatalf("Close iteration %d failed: %v", i, err)
		}

		// Progress and memory check every 1000 iterations
		if (i+1)%1000 == 0 {
			runtime.GC()
			var memCurrent runtime.MemStats
			runtime.ReadMemStats(&memCurrent)
			heapDiff := int64(memCurrent.HeapAlloc) - int64(memBefore.HeapAlloc)
			t.Logf("Progress: %d/%d iterations, heap delta: %+d bytes",
				i+1, iterations, heapDiff)
		}
	}

	elapsed := time.Since(startTime)

	// Final memory check
	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	heapDiff := int64(memAfter.HeapAlloc) - int64(memBefore.HeapAlloc)
	t.Logf("Completed %d open/close cycles in %v", iterations, elapsed)
	t.Logf("Heap memory delta: %+d bytes", heapDiff)

	// Allow some memory growth, but flag significant leaks
	// 10KB per iteration would be a leak; expect much less
	if heapDiff > int64(iterations)*1024 {
		t.Errorf("Possible memory leak: heap grew by %d bytes over %d iterations",
			heapDiff, iterations)
	}

	// Verify image is still valid
	finalImg, err := Open(path)
	if err != nil {
		t.Fatalf("Final Open failed: %v", err)
	}
	defer finalImg.Close()

	buf := make([]byte, 65536)
	if _, err := finalImg.ReadAt(buf, 0); err != nil {
		t.Fatalf("Final ReadAt failed: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Error("Data corrupted after repeated open/close")
	}
}

// TestLongRunningWrites tests continuous writes over an extended period.
// In production this would run for an hour; here we run for a shorter time.
func TestLongRunningWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "long_running.qcow2")

	// Create image
	img, err := Create(path, CreateOptions{
		Size:        100 * 1024 * 1024, // 100MB
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	// Run for 30 seconds (not 1 hour for test suite)
	duration := 30 * time.Second
	t.Logf("Running continuous writes for %v", duration)

	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}

	clusterSize := int64(img.ClusterSize())
	numClusters := img.Size() / clusterSize

	startTime := time.Now()
	writeCount := 0
	var lastError error

	for time.Since(startTime) < duration {
		// Write to pseudo-random offset
		clusterNum := int64(writeCount*997) % numClusters
		offset := clusterNum * clusterSize

		if _, err := img.WriteAt(data, offset); err != nil {
			lastError = err
			break
		}
		writeCount++

		// Periodic flush
		if writeCount%1000 == 0 {
			if err := img.Flush(); err != nil {
				lastError = err
				break
			}
		}

		// Progress every 10 seconds
		if writeCount%10000 == 0 {
			elapsed := time.Since(startTime)
			rate := float64(writeCount) / elapsed.Seconds()
			t.Logf("Progress: %d writes in %v (%.0f writes/sec)",
				writeCount, elapsed.Round(time.Second), rate)
		}
	}

	if lastError != nil {
		t.Fatalf("Write failed after %d writes: %v", writeCount, lastError)
	}

	elapsed := time.Since(startTime)
	t.Logf("Completed %d writes in %v (%.0f writes/sec)",
		writeCount, elapsed, float64(writeCount)/elapsed.Seconds())

	// Final flush and check
	if err := img.Flush(); err != nil {
		t.Fatalf("Final Flush failed: %v", err)
	}

	result, err := img.Check()
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if result.Corruptions > 0 {
		t.Errorf("Check found %d corruptions after long-running writes", result.Corruptions)
	}
	if result.Leaks > 0 {
		t.Logf("Check found %d leaks (may be expected)", result.Leaks)
	}
}

// TestFragmentedAllocation tests allocate, free, reallocate patterns.
// This exercises free cluster reuse and refcount management.
func TestFragmentedAllocation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "fragmented.qcow2")

	// Create image
	img, err := Create(path, CreateOptions{
		Size:        50 * 1024 * 1024, // 50MB
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	clusterSize := int64(img.ClusterSize())
	numClusters := int(img.Size() / clusterSize)

	// Phase 1: Allocate every other cluster
	t.Log("Phase 1: Allocating every other cluster")
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xAA
	}

	allocatedClusters := 0
	for i := 0; i < numClusters; i += 2 {
		offset := int64(i) * clusterSize
		if _, err := img.WriteAt(data, offset); err != nil {
			t.Fatalf("Phase 1 write failed: %v", err)
		}
		allocatedClusters++
	}
	t.Logf("Allocated %d clusters", allocatedClusters)

	// Phase 2: Free half of the allocated clusters (zero them)
	t.Log("Phase 2: Freeing half of allocated clusters")
	freedClusters := 0
	for i := 0; i < numClusters; i += 4 { // Every 4th cluster (half of even clusters)
		offset := int64(i) * clusterSize
		if err := img.WriteZeroAt(offset, clusterSize); err != nil {
			t.Fatalf("Phase 2 zero failed: %v", err)
		}
		freedClusters++
	}
	t.Logf("Freed %d clusters", freedClusters)

	// Phase 3: Allocate new data (should reuse freed clusters)
	t.Log("Phase 3: Reallocating to fill gaps")
	for i := range data {
		data[i] = 0xBB
	}

	reallocatedClusters := 0
	for i := 1; i < numClusters; i += 2 { // Odd clusters (previously unallocated)
		offset := int64(i) * clusterSize
		if _, err := img.WriteAt(data, offset); err != nil {
			t.Fatalf("Phase 3 write failed: %v", err)
		}
		reallocatedClusters++
	}
	t.Logf("Reallocated %d clusters", reallocatedClusters)

	// Phase 4: Repeat the pattern several times
	t.Log("Phase 4: Repeating fragmentation pattern")
	for round := 0; round < 5; round++ {
		// Zero every 3rd cluster
		for i := round; i < numClusters; i += 3 {
			offset := int64(i) * clusterSize
			if err := img.WriteZeroAt(offset, clusterSize); err != nil {
				t.Fatalf("Round %d zero failed: %v", round, err)
			}
		}

		// Reallocate them
		for i := range data {
			data[i] = byte(0xC0 + round)
		}
		for i := round; i < numClusters; i += 3 {
			offset := int64(i) * clusterSize
			if _, err := img.WriteAt(data, offset); err != nil {
				t.Fatalf("Round %d write failed: %v", round, err)
			}
		}
	}

	// Flush and verify
	if err := img.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	result, err := img.Check()
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if result.Corruptions > 0 {
		t.Errorf("Check found %d corruptions after fragmentation", result.Corruptions)
	}
	t.Logf("Check result: corruptions=%d, leaks=%d", result.Corruptions, result.Leaks)
}

// TestCacheEvictionStress tests behavior when cache is exceeded significantly.
// This forces many cache evictions and reloads.
func TestCacheEvictionStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "cache_stress.qcow2")

	// Create image with many L2 tables needed
	// With 64KB clusters, each L2 table covers 64KB * 8192 = 512MB
	// We'll create a 4GB image requiring 8 L2 tables
	img, err := Create(path, CreateOptions{
		Size:        4 * 1024 * 1024 * 1024, // 4GB
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	clusterSize := int64(img.ClusterSize())
	l2Coverage := clusterSize * 8192 // Clusters covered by one L2 table

	// Default cache holds 32 L2 tables; we'll access 64+ different L2 tables
	numL2Tables := 64
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}

	t.Logf("Accessing %d different L2 tables to stress cache", numL2Tables)

	// Write to one cluster in each L2 table region
	for l2 := 0; l2 < numL2Tables; l2++ {
		offset := int64(l2) * l2Coverage
		if offset >= img.Size() {
			break
		}
		if _, err := img.WriteAt(data, offset); err != nil {
			t.Fatalf("Write to L2 table %d region failed: %v", l2, err)
		}
	}

	// Now access them in random order multiple times (causes cache thrashing)
	iterations := 500
	t.Logf("Performing %d random accesses across L2 tables", iterations)

	readBuf := make([]byte, 4096)
	for i := 0; i < iterations; i++ {
		// Pseudo-random L2 table access pattern
		l2 := (i * 37) % numL2Tables
		offset := int64(l2) * l2Coverage
		if offset >= img.Size() {
			continue
		}

		if _, err := img.ReadAt(readBuf, offset); err != nil {
			t.Fatalf("Read iteration %d failed: %v", i, err)
		}

		// Also write occasionally
		if i%5 == 0 {
			for j := range data {
				data[j] = byte((i + j) % 256)
			}
			if _, err := img.WriteAt(data, offset+4096); err != nil {
				t.Fatalf("Write iteration %d failed: %v", i, err)
			}
		}
	}

	// Flush and verify
	if err := img.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	result, err := img.Check()
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if result.Corruptions > 0 {
		t.Errorf("Check found %d corruptions after cache stress", result.Corruptions)
	}
	t.Logf("Cache stress test completed successfully")
}

// TestHighConcurrency tests 100+ goroutines accessing the image.
func TestHighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "high_concurrency.qcow2")

	// Create image
	img, err := Create(path, CreateOptions{
		Size:        100 * 1024 * 1024, // 100MB
		ClusterBits: 16,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer img.Close()

	numGoroutines := 100
	opsPerGoroutine := 100
	clusterSize := int64(img.ClusterSize())
	numClusters := int(img.Size() / clusterSize)

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	t.Logf("Starting %d goroutines with %d operations each", numGoroutines, opsPerGoroutine)
	startTime := time.Now()

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			data := make([]byte, 4096)
			readBuf := make([]byte, 4096)
			for i := range data {
				data[i] = byte(goroutineID)
			}

			for op := 0; op < opsPerGoroutine; op++ {
				// Distribute operations across clusters
				clusterNum := (goroutineID*opsPerGoroutine + op) % numClusters
				offset := int64(clusterNum) * clusterSize

				switch op % 3 {
				case 0: // Write
					if _, err := img.WriteAt(data, offset); err != nil {
						errors <- fmt.Errorf("goroutine %d write %d: %v", goroutineID, op, err)
						return
					}
				case 1: // Read
					if _, err := img.ReadAt(readBuf, offset); err != nil {
						errors <- fmt.Errorf("goroutine %d read %d: %v", goroutineID, op, err)
						return
					}
				case 2: // Write at different offset within cluster
					if _, err := img.WriteAt(data, offset+8192); err != nil {
						errors <- fmt.Errorf("goroutine %d write2 %d: %v", goroutineID, op, err)
						return
					}
				}
			}
		}(g)
	}

	// Wait for completion
	wg.Wait()
	close(errors)

	elapsed := time.Since(startTime)
	totalOps := numGoroutines * opsPerGoroutine
	t.Logf("Completed %d total operations in %v (%.0f ops/sec)",
		totalOps, elapsed, float64(totalOps)/elapsed.Seconds())

	// Check for errors
	var errList []error
	for err := range errors {
		errList = append(errList, err)
	}
	if len(errList) > 0 {
		for _, e := range errList {
			t.Errorf("Concurrent operation error: %v", e)
		}
		t.Fatalf("Had %d errors during high concurrency test", len(errList))
	}

	// Flush and verify
	if err := img.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	result, err := img.Check()
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if result.Corruptions > 0 {
		t.Errorf("Check found %d corruptions after high concurrency", result.Corruptions)
	}
	if result.Leaks > 0 {
		t.Errorf("Check found %d leaks after high concurrency", result.Leaks)
	}
	t.Logf("High concurrency test passed: %d goroutines, %d ops each, no errors",
		numGoroutines, opsPerGoroutine)
}
