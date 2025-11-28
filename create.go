package qcow2

import (
	"encoding/binary"
	"fmt"
	"os"
)

// CreateOptions configures a new QCOW2 image.
type CreateOptions struct {
	// Size is the virtual disk size in bytes (required).
	Size uint64

	// ClusterBits is log2 of cluster size. Default is 16 (64KB clusters).
	// Valid range: 9-21.
	ClusterBits uint32

	// Version is the QCOW2 version. Default is 3.
	Version uint32

	// LazyRefcounts enables lazy refcount updates for better write performance.
	// The tradeoff is that the image needs repair after unclean shutdown.
	LazyRefcounts bool

	// BackingFile is the path to a backing file for COW chains.
	BackingFile string
}

// Create creates a new QCOW2 image file.
func Create(path string, opts CreateOptions) (*Image, error) {
	if opts.Size == 0 {
		return nil, fmt.Errorf("qcow2: size is required")
	}

	// Apply defaults
	if opts.ClusterBits == 0 {
		opts.ClusterBits = DefaultClusterBits
	}
	if opts.Version == 0 {
		opts.Version = Version3
	}

	// Validate
	if opts.ClusterBits < MinClusterBits || opts.ClusterBits > MaxClusterBits {
		return nil, fmt.Errorf("%w: %d", ErrInvalidClusterBits, opts.ClusterBits)
	}
	if opts.Version != Version2 && opts.Version != Version3 {
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedVersion, opts.Version)
	}

	clusterSize := uint64(1) << opts.ClusterBits
	l2Entries := clusterSize / 8

	// Calculate L1 table size
	// Each L2 table covers: l2_entries * cluster_size bytes
	l2Coverage := l2Entries * clusterSize
	l1Size := (opts.Size + l2Coverage - 1) / l2Coverage
	if l1Size == 0 {
		l1Size = 1
	}

	// L1 table must be cluster-aligned in size for v3
	l1TableBytes := l1Size * 8
	if opts.Version >= Version3 && l1TableBytes%clusterSize != 0 {
		l1TableBytes = ((l1TableBytes / clusterSize) + 1) * clusterSize
		l1Size = l1TableBytes / 8
	}

	// Layout the image:
	// Cluster 0: Header
	// Cluster 1: L1 table
	// Cluster 2: Refcount table
	// Cluster 3: First refcount block
	// Cluster 4+: Data clusters

	headerLength := uint32(HeaderSizeV3)
	if opts.Version == Version2 {
		headerLength = HeaderSizeV2
	}

	l1TableOffset := clusterSize                    // Cluster 1
	refcountTableOffset := clusterSize * 2          // Cluster 2
	firstRefcountBlockOffset := clusterSize * 3     // Cluster 3

	// Calculate refcount table size
	// With 16-bit refcounts and 64KB clusters, one refcount block covers:
	// 65536 / 2 = 32768 clusters = 2GB
	// For simplicity, start with 1 cluster for refcount table
	refcountTableClusters := uint32(1)

	// Build header
	header := &Header{
		Magic:                 Magic,
		Version:               opts.Version,
		ClusterBits:           opts.ClusterBits,
		Size:                  opts.Size,
		L1Size:                uint32(l1Size),
		L1TableOffset:         l1TableOffset,
		RefcountTableOffset:   refcountTableOffset,
		RefcountTableClusters: refcountTableClusters,
		RefcountOrder:         4, // 16-bit refcounts
		HeaderLength:          headerLength,
	}

	if opts.LazyRefcounts {
		header.CompatibleFeatures |= CompatLazyRefcounts
	}

	// Create file
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, fmt.Errorf("qcow2: failed to create file: %w", err)
	}

	// Write header
	headerBytes := header.Encode()
	if _, err := f.WriteAt(headerBytes, 0); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("qcow2: failed to write header: %w", err)
	}

	// Write L1 table (all zeros = unallocated)
	l1Table := make([]byte, l1TableBytes)
	if _, err := f.WriteAt(l1Table, int64(l1TableOffset)); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("qcow2: failed to write L1 table: %w", err)
	}

	// Write refcount table
	// First entry points to the first refcount block
	refcountTable := make([]byte, clusterSize)
	binary.BigEndian.PutUint64(refcountTable[0:8], firstRefcountBlockOffset)
	if _, err := f.WriteAt(refcountTable, int64(refcountTableOffset)); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("qcow2: failed to write refcount table: %w", err)
	}

	// Write first refcount block
	// Mark clusters 0-3 as used (refcount = 1)
	refcountBlock := make([]byte, clusterSize)
	// 16-bit refcounts, big-endian
	binary.BigEndian.PutUint16(refcountBlock[0:2], 1) // Cluster 0: header
	binary.BigEndian.PutUint16(refcountBlock[2:4], 1) // Cluster 1: L1 table
	binary.BigEndian.PutUint16(refcountBlock[4:6], 1) // Cluster 2: refcount table
	binary.BigEndian.PutUint16(refcountBlock[6:8], 1) // Cluster 3: refcount block
	if _, err := f.WriteAt(refcountBlock, int64(firstRefcountBlockOffset)); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("qcow2: failed to write refcount block: %w", err)
	}

	// Extend file to include all initial clusters
	initialSize := clusterSize * 4 // Header + L1 + RefcountTable + RefcountBlock
	if err := f.Truncate(int64(initialSize)); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("qcow2: failed to set file size: %w", err)
	}

	// Sync to disk
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("qcow2: failed to sync: %w", err)
	}

	// Now open as normal image
	img, err := newImage(f, false)
	if err != nil {
		f.Close()
		os.Remove(path)
		return nil, err
	}

	return img, nil
}

// CreateSimple creates a new QCOW2 image with default options.
// This is the simplest way to create a new image:
//
//	img, err := qcow2.CreateSimple("disk.qcow2", 10*1024*1024*1024) // 10GB
func CreateSimple(path string, size uint64) (*Image, error) {
	return Create(path, CreateOptions{Size: size})
}
