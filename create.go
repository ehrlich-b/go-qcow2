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

	// BackingFormat specifies the format of the backing file (e.g., "qcow2", "raw").
	// If empty and BackingFile is set, defaults to "qcow2".
	BackingFormat string
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
	// Cluster 1+: L1 table (may span multiple clusters)
	// Next cluster: Refcount table
	// Next cluster: First refcount block
	// Remaining: Data clusters

	headerLength := uint32(HeaderSizeV3)
	if opts.Version == Version2 {
		headerLength = HeaderSizeV2
	}

	// Calculate how many clusters the L1 table needs
	l1Clusters := (l1TableBytes + clusterSize - 1) / clusterSize
	if l1Clusters == 0 {
		l1Clusters = 1
	}

	l1TableOffset := clusterSize                                  // Starts at cluster 1
	refcountTableOffset := clusterSize + l1Clusters*clusterSize   // After L1 table
	firstRefcountBlockOffset := refcountTableOffset + clusterSize // After refcount table

	// Calculate refcount table size
	// With 16-bit refcounts and 64KB clusters, one refcount block covers:
	// 65536 / 2 = 32768 clusters = 2GB
	// For simplicity, start with 1 cluster for refcount table
	refcountTableClusters := uint32(1)

	// Calculate extension area size
	extensionAreaOffset := uint64(headerLength)
	extensionAreaSize := uint64(0)

	// Backing format extension (if backing file with format specified)
	if opts.BackingFile != "" && opts.BackingFormat != "" {
		// Extension header: 4 bytes type + 4 bytes length + data + padding to 8 bytes
		extDataLen := len(opts.BackingFormat)
		extPaddedLen := (extDataLen + 7) & ^7
		extensionAreaSize += 8 + uint64(extPaddedLen) // type + len + padded data
	}

	// End-of-header marker (8 bytes: type=0 + length=0)
	if extensionAreaSize > 0 {
		extensionAreaSize += 8
	}

	// Handle backing file
	var backingFileOffset uint64
	var backingFileSize uint32
	if opts.BackingFile != "" {
		// Backing file path goes after extensions
		backingFileOffset = extensionAreaOffset + extensionAreaSize
		backingFileSize = uint32(len(opts.BackingFile))
	}

	// Build header
	header := &Header{
		Magic:                 Magic,
		Version:               opts.Version,
		BackingFileOffset:     backingFileOffset,
		BackingFileSize:       backingFileSize,
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

	// Write header extensions if needed
	if opts.BackingFile != "" && opts.BackingFormat != "" {
		extOffset := int64(extensionAreaOffset)

		// Write backing format extension
		extHeader := make([]byte, 8)
		binary.BigEndian.PutUint32(extHeader[0:4], ExtensionBackingFormat)
		binary.BigEndian.PutUint32(extHeader[4:8], uint32(len(opts.BackingFormat)))
		if _, err := f.WriteAt(extHeader, extOffset); err != nil {
			f.Close()
			os.Remove(path)
			return nil, fmt.Errorf("qcow2: failed to write backing format extension header: %w", err)
		}
		extOffset += 8

		// Write extension data (padded to 8-byte boundary)
		extPaddedLen := (len(opts.BackingFormat) + 7) & ^7
		extData := make([]byte, extPaddedLen)
		copy(extData, opts.BackingFormat)
		if _, err := f.WriteAt(extData, extOffset); err != nil {
			f.Close()
			os.Remove(path)
			return nil, fmt.Errorf("qcow2: failed to write backing format extension data: %w", err)
		}
		extOffset += int64(extPaddedLen)

		// Write end-of-header marker
		endMarker := make([]byte, 8) // All zeros = end marker
		if _, err := f.WriteAt(endMarker, extOffset); err != nil {
			f.Close()
			os.Remove(path)
			return nil, fmt.Errorf("qcow2: failed to write end-of-header marker: %w", err)
		}
	}

	// Write backing file path if specified
	if opts.BackingFile != "" {
		if _, err := f.WriteAt([]byte(opts.BackingFile), int64(backingFileOffset)); err != nil {
			f.Close()
			os.Remove(path)
			return nil, fmt.Errorf("qcow2: failed to write backing file path: %w", err)
		}
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
	// Mark all initial clusters as used (refcount = 1)
	// Cluster 0: header
	// Clusters 1 to l1Clusters: L1 table
	// Next cluster: refcount table
	// Next cluster: refcount block
	refcountBlock := make([]byte, clusterSize)

	// Calculate total initial clusters
	// 1 (header) + l1Clusters (L1 table) + 1 (refcount table) + 1 (refcount block)
	initialClusters := 1 + l1Clusters + 2

	// Mark each initial cluster with refcount = 1
	for i := uint64(0); i < initialClusters; i++ {
		binary.BigEndian.PutUint16(refcountBlock[i*2:(i+1)*2], 1)
	}

	if _, err := f.WriteAt(refcountBlock, int64(firstRefcountBlockOffset)); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("qcow2: failed to write refcount block: %w", err)
	}

	// Extend file to include all initial clusters
	initialSize := initialClusters * clusterSize
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

	// Now open as normal image (depth=0 for newly created image)
	img, err := newImage(f, false, 0)
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

// CreateOverlay creates a new QCOW2 image backed by an existing image.
// The new image starts empty and reads fall through to the backing file.
// Writes go to the new image (copy-on-write).
//
//	overlay, err := qcow2.CreateOverlay("snapshot.qcow2", "base.qcow2")
func CreateOverlay(path, backingFile string) (*Image, error) {
	// Open backing file to get its size
	backing, err := OpenFile(backingFile, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("qcow2: failed to open backing file: %w", err)
	}
	size := uint64(backing.Size())
	clusterBits := backing.header.ClusterBits
	backing.Close()

	return Create(path, CreateOptions{
		Size:        size,
		ClusterBits: clusterBits,
		BackingFile: backingFile,
	})
}
