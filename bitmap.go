package qcow2

import (
	"math/bits"
	"sync"
)

// freeClusterBitmap tracks free clusters using a bitmap for O(1) allocation.
// Each bit represents one cluster: 1 = free (refcount == 0), 0 = in use.
type freeClusterBitmap struct {
	// words stores the bitmap; each uint64 tracks 64 clusters
	words []uint64

	// hintWord is a hint for where to start searching for free clusters
	hintWord uint64

	// numClusters is the total number of clusters tracked
	numClusters uint64

	// minCluster is the minimum cluster index to consider (skip metadata)
	minCluster uint64

	mu sync.RWMutex
}

// newFreeClusterBitmap creates a new bitmap for tracking free clusters.
func newFreeClusterBitmap(numClusters, minCluster uint64) *freeClusterBitmap {
	numWords := (numClusters + 63) / 64
	return &freeClusterBitmap{
		words:       make([]uint64, numWords),
		numClusters: numClusters,
		minCluster:  minCluster,
		hintWord:    minCluster / 64,
	}
}

// setFree marks a cluster as free (available for allocation).
func (b *freeClusterBitmap) setFree(clusterIdx uint64) {
	if clusterIdx >= b.numClusters || clusterIdx < b.minCluster {
		return
	}
	wordIdx := clusterIdx / 64
	bitIdx := clusterIdx % 64

	b.mu.Lock()
	b.words[wordIdx] |= 1 << bitIdx
	b.mu.Unlock()
}

// setUsed marks a cluster as used (not available for allocation).
func (b *freeClusterBitmap) setUsed(clusterIdx uint64) {
	if clusterIdx >= b.numClusters {
		return
	}
	wordIdx := clusterIdx / 64
	bitIdx := clusterIdx % 64

	b.mu.Lock()
	b.words[wordIdx] &^= 1 << bitIdx
	b.mu.Unlock()
}

// findFree finds a free cluster using O(1) bit scanning.
// Returns the cluster index and true if found, or 0 and false if none available.
func (b *freeClusterBitmap) findFree() (uint64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	numWords := uint64(len(b.words))
	if numWords == 0 {
		return 0, false
	}

	// Start from hint word
	startWord := b.hintWord
	if startWord >= numWords {
		startWord = b.minCluster / 64
	}

	// Search from startWord to end
	for i := startWord; i < numWords; i++ {
		if b.words[i] != 0 {
			// Found a word with free clusters
			bitIdx := uint64(bits.TrailingZeros64(b.words[i]))
			clusterIdx := i*64 + bitIdx

			if clusterIdx >= b.numClusters {
				continue
			}
			if clusterIdx < b.minCluster {
				// Mask out bits below minCluster and try again
				mask := ^uint64(0) << (b.minCluster % 64)
				masked := b.words[i] & mask
				if masked == 0 {
					continue
				}
				bitIdx = uint64(bits.TrailingZeros64(masked))
				clusterIdx = i*64 + bitIdx
				if clusterIdx >= b.numClusters || clusterIdx < b.minCluster {
					continue
				}
			}

			// Mark as used and update hint
			b.words[i] &^= 1 << bitIdx
			b.hintWord = i
			return clusterIdx, true
		}
	}

	// Wrap around: search from beginning to startWord
	minWord := b.minCluster / 64
	for i := minWord; i < startWord; i++ {
		if b.words[i] != 0 {
			bitIdx := uint64(bits.TrailingZeros64(b.words[i]))
			clusterIdx := i*64 + bitIdx

			if clusterIdx >= b.numClusters || clusterIdx < b.minCluster {
				// Mask out bits below minCluster
				mask := ^uint64(0) << (b.minCluster % 64)
				masked := b.words[i] & mask
				if masked == 0 {
					continue
				}
				bitIdx = uint64(bits.TrailingZeros64(masked))
				clusterIdx = i*64 + bitIdx
				if clusterIdx >= b.numClusters || clusterIdx < b.minCluster {
					continue
				}
			}

			// Mark as used and update hint
			b.words[i] &^= 1 << bitIdx
			b.hintWord = i
			return clusterIdx, true
		}
	}

	return 0, false
}

// isFree checks if a cluster is marked as free.
func (b *freeClusterBitmap) isFree(clusterIdx uint64) bool {
	if clusterIdx >= b.numClusters {
		return false
	}
	wordIdx := clusterIdx / 64
	bitIdx := clusterIdx % 64

	b.mu.RLock()
	defer b.mu.RUnlock()
	return (b.words[wordIdx] & (1 << bitIdx)) != 0
}

// countFree returns the number of free clusters in the bitmap.
func (b *freeClusterBitmap) countFree() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var count uint64
	for _, word := range b.words {
		count += uint64(bits.OnesCount64(word))
	}
	return count
}

// grow expands the bitmap to track more clusters.
func (b *freeClusterBitmap) grow(newNumClusters uint64) {
	if newNumClusters <= b.numClusters {
		return
	}

	newNumWords := (newNumClusters + 63) / 64
	oldNumWords := uint64(len(b.words))

	b.mu.Lock()
	defer b.mu.Unlock()

	if newNumWords > oldNumWords {
		newWords := make([]uint64, newNumWords)
		copy(newWords, b.words)
		b.words = newWords
	}
	b.numClusters = newNumClusters
}
