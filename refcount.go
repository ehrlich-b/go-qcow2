package qcow2

import (
	"encoding/binary"
	"fmt"
)

// loadRefcountTable loads the refcount table into memory.
// The refcount table is a two-level structure:
// - Level 1: Refcount table (array of 64-bit offsets to refcount blocks)
// - Level 2: Refcount blocks (array of refcount entries)
func (img *Image) loadRefcountTable() error {
	if img.refcountTable != nil {
		return nil // Already loaded
	}

	tableSize := uint64(img.header.RefcountTableClusters) * img.clusterSize
	img.refcountTable = make([]byte, tableSize)

	_, err := img.file.ReadAt(img.refcountTable, int64(img.header.RefcountTableOffset))
	if err != nil {
		return fmt.Errorf("qcow2: failed to read refcount table: %w", err)
	}

	return nil
}

// getRefcount returns the refcount for a cluster at the given host offset.
func (img *Image) getRefcount(hostOffset uint64) (uint64, error) {
	if err := img.loadRefcountTable(); err != nil {
		return 0, err
	}

	// Calculate which cluster index this offset represents
	clusterIndex := hostOffset >> img.clusterBits

	// Calculate refcount table entry index
	refcountBits := img.header.RefcountBits()
	refcountBytes := refcountBits / 8
	if refcountBytes == 0 {
		refcountBytes = 1
	}
	entriesPerBlock := img.clusterSize / uint64(refcountBytes)

	refcountTableIndex := clusterIndex / entriesPerBlock
	refcountBlockIndex := clusterIndex % entriesPerBlock

	// Check bounds
	tableEntries := uint64(len(img.refcountTable)) / 8
	if refcountTableIndex >= tableEntries {
		return 0, nil // Beyond table, refcount is 0
	}

	// Get refcount block offset from table
	blockOffset := binary.BigEndian.Uint64(img.refcountTable[refcountTableIndex*8:])
	if blockOffset == 0 {
		return 0, nil // Block not allocated, refcount is 0
	}

	// Check cache first
	block := img.refcountBlockCache.get(blockOffset)
	if block == nil {
		// Cache miss - read from disk
		block = make([]byte, img.clusterSize)
		_, err := img.file.ReadAt(block, int64(blockOffset))
		if err != nil {
			return 0, fmt.Errorf("qcow2: failed to read refcount block: %w", err)
		}
		// Add to cache
		img.refcountBlockCache.put(blockOffset, block)
	}

	// Read the specific refcount entry
	return readRefcountEntry(block, refcountBlockIndex, refcountBits), nil
}

// readRefcountEntry reads a single refcount entry from a block.
func readRefcountEntry(block []byte, index uint64, bits uint32) uint64 {
	switch bits {
	case 1:
		byteIndex := index / 8
		bitIndex := 7 - (index % 8) // Big-endian bit order
		return uint64((block[byteIndex] >> bitIndex) & 1)
	case 2:
		byteIndex := index / 4
		bitIndex := 6 - (index%4)*2
		return uint64((block[byteIndex] >> bitIndex) & 3)
	case 4:
		byteIndex := index / 2
		bitIndex := 4 - (index%2)*4
		return uint64((block[byteIndex] >> bitIndex) & 0xf)
	case 8:
		return uint64(block[index])
	case 16:
		return uint64(binary.BigEndian.Uint16(block[index*2:]))
	case 32:
		return uint64(binary.BigEndian.Uint32(block[index*4:]))
	case 64:
		return binary.BigEndian.Uint64(block[index*8:])
	default:
		return 0
	}
}

// ClusterRefcount returns the reference count for a cluster.
// This is useful for debugging and verification.
func (img *Image) ClusterRefcount(clusterOffset uint64) (uint64, error) {
	// Align to cluster boundary
	aligned := clusterOffset & ^img.offsetMask
	return img.getRefcount(aligned)
}

// IsClusterFree returns true if the cluster at the given offset is free.
func (img *Image) IsClusterFree(clusterOffset uint64) (bool, error) {
	refcount, err := img.getRefcount(clusterOffset)
	if err != nil {
		return false, err
	}
	return refcount == 0, nil
}

// refcountInfo returns information about the refcount structure.
type RefcountInfo struct {
	RefcountBits    uint32
	EntriesPerBlock uint64
	TableClusters   uint32
	TableEntries    uint64
	AllocatedBlocks uint64
}

// GetRefcountInfo returns information about the refcount structure.
func (img *Image) GetRefcountInfo() (RefcountInfo, error) {
	if err := img.loadRefcountTable(); err != nil {
		return RefcountInfo{}, err
	}

	refcountBits := img.header.RefcountBits()
	refcountBytes := refcountBits / 8
	if refcountBytes == 0 {
		refcountBytes = 1
	}
	entriesPerBlock := img.clusterSize / uint64(refcountBytes)
	tableEntries := uint64(len(img.refcountTable)) / 8

	// Count allocated blocks
	var allocatedBlocks uint64
	for i := uint64(0); i < tableEntries; i++ {
		offset := binary.BigEndian.Uint64(img.refcountTable[i*8:])
		if offset != 0 {
			allocatedBlocks++
		}
	}

	return RefcountInfo{
		RefcountBits:    refcountBits,
		EntriesPerBlock: entriesPerBlock,
		TableClusters:   img.header.RefcountTableClusters,
		TableEntries:    tableEntries,
		AllocatedBlocks: allocatedBlocks,
	}, nil
}

// writeRefcountEntry writes a single refcount entry to a block.
func writeRefcountEntry(block []byte, index uint64, bits uint32, value uint64) {
	switch bits {
	case 1:
		byteIndex := index / 8
		bitIndex := 7 - (index % 8) // Big-endian bit order
		if value != 0 {
			block[byteIndex] |= 1 << bitIndex
		} else {
			block[byteIndex] &^= 1 << bitIndex
		}
	case 2:
		byteIndex := index / 4
		bitIndex := 6 - (index%4)*2
		block[byteIndex] = (block[byteIndex] &^ (3 << bitIndex)) | (byte(value&3) << bitIndex)
	case 4:
		byteIndex := index / 2
		bitIndex := 4 - (index%2)*4
		block[byteIndex] = (block[byteIndex] &^ (0xf << bitIndex)) | (byte(value&0xf) << bitIndex)
	case 8:
		block[index] = byte(value)
	case 16:
		binary.BigEndian.PutUint16(block[index*2:], uint16(value))
	case 32:
		binary.BigEndian.PutUint32(block[index*4:], uint32(value))
	case 64:
		binary.BigEndian.PutUint64(block[index*8:], value)
	}
}

// updateRefcount updates the refcount for a cluster.
// delta can be positive (allocate) or negative (deallocate).
func (img *Image) updateRefcount(hostOffset uint64, delta int64) error {
	img.refcountTableLock.Lock()
	defer img.refcountTableLock.Unlock()

	if err := img.loadRefcountTable(); err != nil {
		return err
	}

	// Calculate which cluster index this offset represents
	clusterIndex := hostOffset >> img.clusterBits

	// Calculate refcount table entry index
	refcountBits := img.header.RefcountBits()
	refcountBytes := refcountBits / 8
	if refcountBytes == 0 {
		refcountBytes = 1
	}
	entriesPerBlock := img.clusterSize / uint64(refcountBytes)

	refcountTableIndex := clusterIndex / entriesPerBlock
	refcountBlockIndex := clusterIndex % entriesPerBlock

	// Check bounds and expand if needed
	tableEntries := uint64(len(img.refcountTable)) / 8
	if refcountTableIndex >= tableEntries {
		return fmt.Errorf("qcow2: refcount table too small for cluster 0x%x", hostOffset)
	}

	// Get refcount block offset from table
	blockOffset := binary.BigEndian.Uint64(img.refcountTable[refcountTableIndex*8:])

	// Allocate refcount block if needed
	if blockOffset == 0 {
		var err error
		blockOffset, err = img.allocateRefcountBlock(refcountTableIndex)
		if err != nil {
			return err
		}
	}

	// Check cache first, otherwise read from disk
	block := img.refcountBlockCache.get(blockOffset)
	if block == nil {
		block = make([]byte, img.clusterSize)
		_, err := img.file.ReadAt(block, int64(blockOffset))
		if err != nil {
			return fmt.Errorf("qcow2: failed to read refcount block: %w", err)
		}
	}

	// Read current refcount
	currentRefcount := readRefcountEntry(block, refcountBlockIndex, refcountBits)

	// Calculate new refcount
	var newRefcount uint64
	if delta > 0 {
		newRefcount = currentRefcount + uint64(delta)
	} else if uint64(-delta) > currentRefcount {
		return fmt.Errorf("qcow2: refcount underflow for cluster 0x%x", hostOffset)
	} else {
		newRefcount = currentRefcount - uint64(-delta)
	}

	// Check for overflow
	maxRefcount := (uint64(1) << refcountBits) - 1
	if newRefcount > maxRefcount {
		return fmt.Errorf("qcow2: refcount overflow for cluster 0x%x", hostOffset)
	}

	// Write new refcount
	writeRefcountEntry(block, refcountBlockIndex, refcountBits, newRefcount)

	// Write block back to disk
	_, err := img.file.WriteAt(block, int64(blockOffset))
	if err != nil {
		return fmt.Errorf("qcow2: failed to write refcount block: %w", err)
	}

	// Update cache
	img.refcountBlockCache.put(blockOffset, block)

	return nil
}

// allocateRefcountBlock allocates a new refcount block and updates the table.
// Must be called with refcountTableLock held.
func (img *Image) allocateRefcountBlock(tableIndex uint64) (uint64, error) {
	// Get current file size for allocation
	info, err := img.file.Stat()
	if err != nil {
		return 0, err
	}

	// Align to cluster boundary
	offset := uint64(info.Size())
	if offset&img.offsetMask != 0 {
		offset = (offset + img.clusterSize) & ^img.offsetMask
	}

	// Extend file
	if err := img.file.Truncate(int64(offset + img.clusterSize)); err != nil {
		return 0, err
	}

	// Zero the new block
	zeros := make([]byte, img.clusterSize)
	if _, err := img.file.WriteAt(zeros, int64(offset)); err != nil {
		return 0, err
	}

	// Update refcount table entry
	binary.BigEndian.PutUint64(img.refcountTable[tableIndex*8:], offset)

	// Write updated table entry to disk
	_, err = img.file.WriteAt(img.refcountTable[tableIndex*8:tableIndex*8+8],
		int64(img.header.RefcountTableOffset+tableIndex*8))
	if err != nil {
		return 0, fmt.Errorf("qcow2: failed to update refcount table: %w", err)
	}

	// The new refcount block itself needs a refcount of 1
	// But we need to be careful not to recurse infinitely
	// The block we just allocated might be tracked by itself or by an existing block
	// For now, we'll update the refcount after returning
	// This is handled by the caller

	return offset, nil
}

// incrementRefcount increments the refcount for a cluster by 1.
// In lazy refcounts mode, this is a no-op (refcounts are rebuilt on open).
func (img *Image) incrementRefcount(hostOffset uint64) error {
	if img.lazyRefcounts {
		return nil // Skip in lazy mode
	}
	return img.updateRefcount(hostOffset, 1)
}

// decrementRefcount decrements the refcount for a cluster by 1.
// In lazy refcounts mode, this is a no-op (refcounts are rebuilt on open).
func (img *Image) decrementRefcount(hostOffset uint64) error {
	if img.lazyRefcounts {
		return nil // Skip in lazy mode
	}
	return img.updateRefcount(hostOffset, -1)
}

// rebuildRefcounts scans the L1/L2 tables and rebuilds all refcounts.
// This is called when opening a dirty image with lazy refcounts enabled.
func (img *Image) rebuildRefcounts() error {
	img.refcountTableLock.Lock()
	defer img.refcountTableLock.Unlock()

	if err := img.loadRefcountTable(); err != nil {
		return err
	}

	// Clear refcount block cache since we're rebuilding everything
	img.refcountBlockCache.clear()

	// Get refcount configuration
	refcountBits := img.header.RefcountBits()
	refcountBytes := refcountBits / 8
	if refcountBytes == 0 {
		refcountBytes = 1
	}

	// First, zero all existing refcount blocks
	tableEntries := uint64(len(img.refcountTable)) / 8
	for i := uint64(0); i < tableEntries; i++ {
		blockOffset := binary.BigEndian.Uint64(img.refcountTable[i*8:])
		if blockOffset == 0 {
			continue
		}
		zeros := make([]byte, img.clusterSize)
		if _, err := img.file.WriteAt(zeros, int64(blockOffset)); err != nil {
			return fmt.Errorf("qcow2: failed to zero refcount block: %w", err)
		}
	}

	// Track which clusters are referenced
	refcounts := make(map[uint64]uint64) // cluster index -> refcount

	// Header cluster is always referenced
	refcounts[0] = 1

	// L1 table
	l1Start := img.header.L1TableOffset >> img.clusterBits
	l1Size := uint64(img.header.L1Size) * 8
	l1Clusters := (l1Size + img.clusterSize - 1) >> img.clusterBits
	for i := uint64(0); i < l1Clusters; i++ {
		refcounts[l1Start+i] = 1
	}

	// Refcount table
	refStart := img.header.RefcountTableOffset >> img.clusterBits
	refClusters := uint64(img.header.RefcountTableClusters)
	for i := uint64(0); i < refClusters; i++ {
		refcounts[refStart+i] = 1
	}

	// Refcount blocks (from refcount table entries)
	for i := uint64(0); i < tableEntries; i++ {
		blockOffset := binary.BigEndian.Uint64(img.refcountTable[i*8:])
		if blockOffset == 0 {
			continue
		}
		refcounts[blockOffset>>img.clusterBits] = 1
	}

	// Scan L1 table for L2 tables and data clusters
	img.l1Mu.RLock()
	l1Entries := uint64(img.header.L1Size)
	for i := uint64(0); i < l1Entries; i++ {
		l1Entry := binary.BigEndian.Uint64(img.l1Table[i*8:])
		if l1Entry == 0 {
			continue
		}
		l2Offset := l1Entry & L1EntryOffsetMask
		if l2Offset == 0 {
			continue
		}

		// L2 table is referenced
		l2ClusterIdx := l2Offset >> img.clusterBits
		refcounts[l2ClusterIdx]++

		// Scan L2 table for data clusters
		l2Table := make([]byte, img.clusterSize)
		if _, err := img.file.ReadAt(l2Table, int64(l2Offset)); err != nil {
			img.l1Mu.RUnlock()
			return fmt.Errorf("qcow2: failed to read L2 table during rebuild: %w", err)
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
			// Skip zero clusters
			if l2Entry&L2EntryZeroFlag != 0 && l2Entry&L2EntryOffsetMask == 0 {
				continue
			}
			dataOffset := l2Entry & L2EntryOffsetMask
			if dataOffset != 0 {
				refcounts[dataOffset>>img.clusterBits]++
			}
		}
	}
	img.l1Mu.RUnlock()

	// Write refcounts back to disk
	// Group updates by block to avoid overwriting previous writes
	entriesPerBlock := img.clusterSize / uint64(refcountBytes)
	blockUpdates := make(map[uint64]map[uint64]uint64) // blockOffset -> (blockIndex -> refcount)

	for clusterIdx, refcount := range refcounts {
		tableIndex := clusterIdx / entriesPerBlock
		blockIndex := clusterIdx % entriesPerBlock

		if tableIndex >= tableEntries {
			continue // Beyond refcount table
		}

		blockOffset := binary.BigEndian.Uint64(img.refcountTable[tableIndex*8:])
		if blockOffset == 0 {
			continue // No refcount block for this range
		}

		if blockUpdates[blockOffset] == nil {
			blockUpdates[blockOffset] = make(map[uint64]uint64)
		}
		blockUpdates[blockOffset][blockIndex] = refcount
	}

	// Now write each block once with all its updates
	for blockOffset, updates := range blockUpdates {
		// Blocks were already zeroed, so start with a clean block
		block := make([]byte, img.clusterSize)
		for blockIndex, refcount := range updates {
			writeRefcountEntry(block, blockIndex, refcountBits, refcount)
		}

		if _, err := img.file.WriteAt(block, int64(blockOffset)); err != nil {
			return fmt.Errorf("qcow2: failed to write refcount block during rebuild: %w", err)
		}
	}

	return img.file.Sync()
}

// HasLazyRefcounts returns true if lazy refcounts mode is enabled.
func (img *Image) HasLazyRefcounts() bool {
	return img.lazyRefcounts
}
