package qcow2

import (
	"encoding/binary"
	"fmt"
)

// CheckResult contains the results of an image consistency check.
type CheckResult struct {
	// Leaks is the number of clusters with refcount > 0 that are not referenced.
	Leaks int

	// LeakedClusters is the total size of leaked clusters in bytes.
	LeakedClusters uint64

	// Corruptions is the number of corrupted entries found.
	Corruptions int

	// Errors contains descriptions of any errors found.
	Errors []string

	// AllocatedClusters is the total number of allocated clusters.
	AllocatedClusters uint64

	// ReferencedClusters is the number of clusters actually referenced.
	ReferencedClusters uint64

	// FragmentedClusters is the number of clusters that are not contiguous.
	FragmentedClusters uint64
}

// IsClean returns true if no errors, corruptions, or leaks were found.
func (r *CheckResult) IsClean() bool {
	return r.Corruptions == 0 && r.Leaks == 0 && len(r.Errors) == 0
}

// Check performs a consistency check on the image.
// This is similar to `qemu-img check`.
func (img *Image) Check() (*CheckResult, error) {
	result := &CheckResult{}

	// Load refcount table if not already loaded
	if err := img.loadRefcountTable(); err != nil {
		return nil, fmt.Errorf("qcow2: failed to load refcount table: %w", err)
	}

	// Build a map of expected refcounts by scanning L1/L2 tables
	expectedRefcounts := make(map[uint64]uint64) // cluster index -> expected refcount

	// Header cluster is always referenced
	expectedRefcounts[0] = 1

	// L1 table clusters
	l1Start := img.header.L1TableOffset >> img.clusterBits
	l1Size := uint64(img.header.L1Size) * 8
	l1Clusters := (l1Size + img.clusterSize - 1) >> img.clusterBits
	for i := uint64(0); i < l1Clusters; i++ {
		expectedRefcounts[l1Start+i] = 1
	}

	// Refcount table clusters
	refStart := img.header.RefcountTableOffset >> img.clusterBits
	refClusters := uint64(img.header.RefcountTableClusters)
	for i := uint64(0); i < refClusters; i++ {
		expectedRefcounts[refStart+i] = 1
	}

	// Refcount blocks (from refcount table entries)
	tableEntries := uint64(len(img.refcountTable)) / 8
	for i := uint64(0); i < tableEntries; i++ {
		blockOffset := binary.BigEndian.Uint64(img.refcountTable[i*8:])
		if blockOffset == 0 {
			continue
		}
		expectedRefcounts[blockOffset>>img.clusterBits] = 1
	}

	// Scan L1 table for L2 tables and data clusters
	img.l1Mu.RLock()
	l1Entries := uint64(img.header.L1Size)
	var lastDataCluster uint64
	for i := uint64(0); i < l1Entries; i++ {
		l1Entry := binary.BigEndian.Uint64(img.l1Table[i*8:])
		if l1Entry == 0 {
			continue
		}

		l2Offset := l1Entry & L1EntryOffsetMask
		if l2Offset == 0 {
			continue
		}

		// Validate L2 table offset
		if l2Offset&uint64(img.clusterSize-1) != 0 {
			result.Corruptions++
			result.Errors = append(result.Errors,
				fmt.Sprintf("L1[%d]: L2 table offset 0x%x is not cluster-aligned", i, l2Offset))
			continue
		}

		// L2 table is referenced
		l2ClusterIdx := l2Offset >> img.clusterBits
		expectedRefcounts[l2ClusterIdx]++

		// Scan L2 table for data clusters
		l2Table := make([]byte, img.clusterSize)
		if _, err := img.file.ReadAt(l2Table, int64(l2Offset)); err != nil {
			result.Errors = append(result.Errors,
				fmt.Sprintf("L1[%d]: failed to read L2 table at 0x%x: %v", i, l2Offset, err))
			continue
		}

		for j := uint64(0); j < img.l2Entries; j++ {
			l2Entry := binary.BigEndian.Uint64(l2Table[j*8:])
			if l2Entry == 0 {
				continue
			}

			// Skip compressed clusters (bit 62) - they use raw sector offsets
			if l2Entry&L2EntryCompressed != 0 {
				continue
			}

			// Skip zero clusters with no allocation
			if l2Entry&L2EntryZeroFlag != 0 && l2Entry&L2EntryOffsetMask == 0 {
				continue
			}

			dataOffset := l2Entry & L2EntryOffsetMask
			if dataOffset == 0 {
				continue
			}

			// Validate data cluster offset alignment
			if dataOffset&uint64(img.clusterSize-1) != 0 {
				result.Corruptions++
				result.Errors = append(result.Errors,
					fmt.Sprintf("L2[%d][%d]: data offset 0x%x is not cluster-aligned", i, j, dataOffset))
				continue
			}

			dataClusterIdx := dataOffset >> img.clusterBits
			expectedRefcounts[dataClusterIdx]++

			// Track fragmentation
			if lastDataCluster != 0 && dataClusterIdx != lastDataCluster+1 {
				result.FragmentedClusters++
			}
			lastDataCluster = dataClusterIdx
		}
	}
	img.l1Mu.RUnlock()

	result.ReferencedClusters = uint64(len(expectedRefcounts))

	// Get file size to determine max cluster
	info, err := img.file.Stat()
	if err != nil {
		return nil, fmt.Errorf("qcow2: failed to stat file: %w", err)
	}
	maxCluster := uint64(info.Size()) >> img.clusterBits

	// Check all clusters in the file
	for clusterIdx := uint64(0); clusterIdx < maxCluster; clusterIdx++ {
		// Get actual refcount
		actualRefcount, err := img.getRefcount(clusterIdx << img.clusterBits)
		if err != nil {
			continue
		}

		expectedRefcount := expectedRefcounts[clusterIdx]

		if actualRefcount > 0 {
			result.AllocatedClusters++
		}

		if actualRefcount != expectedRefcount {
			if expectedRefcount == 0 && actualRefcount > 0 {
				// Leak: cluster has refcount but is not referenced
				result.Leaks++
				result.LeakedClusters += img.clusterSize
			} else if expectedRefcount > 0 && actualRefcount == 0 {
				// Corruption: cluster is referenced but has refcount 0
				result.Corruptions++
				result.Errors = append(result.Errors,
					fmt.Sprintf("cluster %d: referenced but refcount is 0", clusterIdx))
			} else if expectedRefcount != actualRefcount {
				// Refcount mismatch
				result.Errors = append(result.Errors,
					fmt.Sprintf("cluster %d: refcount mismatch (actual=%d, expected=%d)",
						clusterIdx, actualRefcount, expectedRefcount))
			}
		}
	}

	// Check for snapshot table if present
	if img.header.NbSnapshots > 0 && img.header.SnapshotsOffset != 0 {
		// Mark snapshot table clusters as referenced
		// (For now, just note that snapshots exist)
		result.Errors = append(result.Errors,
			fmt.Sprintf("image has %d snapshots (snapshot checking not implemented)", img.header.NbSnapshots))
	}

	return result, nil
}

// Repair attempts to fix consistency issues in the image.
// Currently this rebuilds refcounts from L1/L2 tables.
// Returns the CheckResult after repair.
func (img *Image) Repair() (*CheckResult, error) {
	if img.readOnly {
		return nil, ErrReadOnly
	}

	// Rebuild refcounts (reuses the lazy refcounts implementation)
	if err := img.rebuildRefcounts(); err != nil {
		return nil, fmt.Errorf("qcow2: repair failed: %w", err)
	}

	// Run check to verify repair was successful
	return img.Check()
}

// CheckOptions configures the check operation.
type CheckOptions struct {
	// Repair enables automatic repair of fixable issues.
	Repair bool
}

// CheckWithOptions performs a check with the given options.
func (img *Image) CheckWithOptions(opts CheckOptions) (*CheckResult, error) {
	result, err := img.Check()
	if err != nil {
		return nil, err
	}

	if opts.Repair && !result.IsClean() && !img.readOnly {
		return img.Repair()
	}

	return result, nil
}
