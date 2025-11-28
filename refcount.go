package qcow2

import (
	"encoding/binary"
	"fmt"
)

// refcountTable manages the two-level refcount structure.
// Level 1: Refcount table (array of 64-bit offsets to refcount blocks)
// Level 2: Refcount blocks (array of refcount entries)
type refcountTable struct {
	img *Image

	// Refcount entry configuration
	refcountBits    uint32 // Bits per refcount entry (1, 2, 4, 8, 16, 32, 64)
	refcountBytes   uint32 // Bytes per refcount entry
	entriesPerBlock uint64 // Number of refcount entries per block

	// Cached refcount table (level 1)
	table []byte
}

// loadRefcountTable loads the refcount table into memory.
func (img *Image) loadRefcountTable() error {
	if img.refcountTable != nil {
		return nil // Already loaded
	}

	refcountBits := img.header.RefcountBits()
	refcountBytes := refcountBits / 8
	if refcountBytes == 0 {
		refcountBytes = 1 // Minimum 1 byte for sub-byte refcounts
	}

	rt := &refcountTable{
		img:             img,
		refcountBits:    refcountBits,
		refcountBytes:   refcountBytes,
		entriesPerBlock: img.clusterSize / uint64(refcountBytes),
	}

	// Load refcount table
	tableSize := uint64(img.header.RefcountTableClusters) * img.clusterSize
	rt.table = make([]byte, tableSize)

	_, err := img.file.ReadAt(rt.table, int64(img.header.RefcountTableOffset))
	if err != nil {
		return fmt.Errorf("qcow2: failed to read refcount table: %w", err)
	}

	img.refcountTable = rt.table
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

	// Read the refcount block
	block := make([]byte, img.clusterSize)
	_, err := img.file.ReadAt(block, int64(blockOffset))
	if err != nil {
		return 0, fmt.Errorf("qcow2: failed to read refcount block: %w", err)
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
