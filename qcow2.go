package qcow2

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

// Image is the primary interface for interacting with a QCOW2 image.
// It implements io.ReaderAt and io.WriterAt for random access.
type Image struct {
	file   *os.File
	header *Header

	// Derived values cached for performance
	clusterSize uint64
	clusterBits uint32
	l2Entries   uint64
	l2Bits      uint32 // log2(l2Entries)
	offsetMask  uint64 // Mask for offset within cluster

	// L1 table - loaded entirely into memory (typically small)
	l1Table []byte
	l1Mu    sync.RWMutex

	// L2 cache - keeps recently used L2 tables in memory
	l2Cache *l2Cache

	// Refcount table offset cache
	refcountTable     []byte
	refcountTableLock sync.RWMutex

	// Write tracking
	readOnly bool
	dirty    bool

	// Backing file for COW chains
	backing *Image
}

// Open opens an existing QCOW2 image file.
func Open(path string) (*Image, error) {
	return OpenFile(path, os.O_RDWR, 0)
}

// OpenFile opens a QCOW2 image with specific flags.
func OpenFile(path string, flag int, perm os.FileMode) (*Image, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, fmt.Errorf("qcow2: failed to open file: %w", err)
	}

	img, err := newImage(f, flag&os.O_RDWR == 0 || flag == os.O_RDONLY)
	if err != nil {
		f.Close()
		return nil, err
	}

	return img, nil
}

// newImage creates an Image from an already-open file.
func newImage(f *os.File, readOnly bool) (*Image, error) {
	// Read header
	headerBuf := make([]byte, HeaderSizeV3)
	n, err := f.ReadAt(headerBuf, 0)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("qcow2: failed to read header: %w", err)
	}
	if n < HeaderSizeV2 {
		return nil, fmt.Errorf("qcow2: file too small for header: %d bytes", n)
	}

	header, err := ParseHeader(headerBuf[:n])
	if err != nil {
		return nil, err
	}

	if err := header.Validate(); err != nil {
		return nil, err
	}

	img := &Image{
		file:        f,
		header:      header,
		clusterSize: header.ClusterSize(),
		clusterBits: header.ClusterBits,
		l2Entries:   header.L2Entries(),
		offsetMask:  header.ClusterSize() - 1,
		readOnly:    readOnly,
	}

	// Calculate l2Bits (log2 of l2Entries)
	img.l2Bits = header.ClusterBits - 3 // Each L2 entry is 8 bytes

	// Load L1 table
	if err := img.loadL1Table(); err != nil {
		return nil, fmt.Errorf("qcow2: failed to load L1 table: %w", err)
	}

	// Initialize L2 cache (default 32 entries = 2MB with 64KB clusters)
	img.l2Cache = newL2Cache(32, int(img.clusterSize))

	// Mark image dirty if opened for writing (v3 only)
	if !readOnly && header.Version >= Version3 {
		if err := img.markDirty(); err != nil {
			return nil, fmt.Errorf("qcow2: failed to mark image dirty: %w", err)
		}
	}

	// Open backing file if present
	if err := img.openBackingFile(); err != nil {
		return nil, err
	}

	return img, nil
}

// markDirty sets the dirty bit in the header.
func (img *Image) markDirty() error {
	if img.header.IncompatibleFeatures&IncompatDirtyBit != 0 {
		return nil // Already dirty
	}

	img.header.IncompatibleFeatures |= IncompatDirtyBit
	return img.writeHeader()
}

// clearDirty clears the dirty bit in the header.
func (img *Image) clearDirty() error {
	if img.header.IncompatibleFeatures&IncompatDirtyBit == 0 {
		return nil // Already clean
	}

	img.header.IncompatibleFeatures &^= IncompatDirtyBit
	return img.writeHeader()
}

// writeHeader writes the current header to disk.
func (img *Image) writeHeader() error {
	headerBytes := img.header.Encode()
	_, err := img.file.WriteAt(headerBytes, 0)
	if err != nil {
		return err
	}
	return img.file.Sync()
}

// loadL1Table reads the entire L1 table into memory.
func (img *Image) loadL1Table() error {
	size := uint64(img.header.L1Size) * 8 // 8 bytes per entry
	img.l1Table = make([]byte, size)

	_, err := img.file.ReadAt(img.l1Table, int64(img.header.L1TableOffset))
	if err != nil {
		return err
	}

	return nil
}

// Size returns the virtual size of the image in bytes.
func (img *Image) Size() int64 {
	return int64(img.header.Size)
}

// ClusterSize returns the cluster size in bytes.
func (img *Image) ClusterSize() int {
	return int(img.clusterSize)
}

// ReadAt reads len(p) bytes from the image at offset off.
// It implements io.ReaderAt.
func (img *Image) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, ErrOffsetOutOfRange
	}

	size := img.Size()
	if off >= size {
		return 0, io.EOF
	}

	// Clamp read to image size
	if off+int64(len(p)) > size {
		p = p[:size-off]
	}

	for len(p) > 0 {
		// Calculate how much we can read in this cluster
		clusterOff := uint64(off) & img.offsetMask
		toRead := img.clusterSize - clusterOff
		if toRead > uint64(len(p)) {
			toRead = uint64(len(p))
		}

		// Translate virtual offset to cluster info
		info, err := img.translate(uint64(off))
		if err != nil {
			return n, err
		}

		switch info.ctype {
		case clusterNormal:
			// Read from allocated cluster
			read, err := img.file.ReadAt(p[:toRead], int64(info.physOff))
			n += read
			if err != nil {
				return n, err
			}

		case clusterZero:
			// Zero cluster - return zeros without disk I/O
			for i := uint64(0); i < toRead; i++ {
				p[i] = 0
			}
			n += int(toRead)

		case clusterUnallocated:
			// Unallocated cluster - read from backing file or return zeros
			if img.backing != nil {
				read, err := img.backing.ReadAt(p[:toRead], off)
				n += read
				if err != nil && err != io.EOF {
					return n, err
				}
			} else {
				// Zero fill
				for i := uint64(0); i < toRead; i++ {
					p[i] = 0
				}
				n += int(toRead)
			}

		case clusterCompressed:
			return n, fmt.Errorf("qcow2: compressed clusters not yet supported")
		}

		p = p[toRead:]
		off += int64(toRead)
	}

	return n, nil
}

// WriteAt writes len(p) bytes to the image at offset off.
// It implements io.WriterAt.
func (img *Image) WriteAt(p []byte, off int64) (n int, err error) {
	if img.readOnly {
		return 0, ErrReadOnly
	}

	if off < 0 {
		return 0, ErrOffsetOutOfRange
	}

	size := img.Size()
	if off >= size {
		return 0, ErrOffsetOutOfRange
	}

	// Clamp write to image size
	if off+int64(len(p)) > size {
		p = p[:size-off]
	}

	for len(p) > 0 {
		// Calculate how much we can write in this cluster
		clusterOff := uint64(off) & img.offsetMask
		toWrite := img.clusterSize - clusterOff
		if toWrite > uint64(len(p)) {
			toWrite = uint64(len(p))
		}

		// Get or allocate physical cluster
		physOff, err := img.getClusterForWrite(uint64(off))
		if err != nil {
			return n, err
		}

		// Write to allocated cluster
		written, err := img.file.WriteAt(p[:toWrite], int64(physOff))
		n += written
		if err != nil {
			return n, err
		}

		p = p[toWrite:]
		off += int64(toWrite)
	}

	img.dirty = true
	return n, nil
}

// clusterType represents the type of a cluster
type clusterType int

const (
	clusterUnallocated clusterType = iota
	clusterZero                    // All zeros (no backing storage needed)
	clusterNormal                  // Normal allocated cluster
	clusterCompressed              // Compressed cluster (not yet supported)
)

// clusterInfo contains information about a cluster's location and type
type clusterInfo struct {
	ctype   clusterType
	physOff uint64 // Physical offset (0 for unallocated/zero)
}

// translate converts a virtual offset to cluster information.
func (img *Image) translate(virtOff uint64) (clusterInfo, error) {
	// Calculate L1 and L2 indices
	l2Index := (virtOff >> img.clusterBits) & (img.l2Entries - 1)
	l1Index := virtOff >> (img.clusterBits + img.l2Bits)

	// Check L1 bounds
	if l1Index >= uint64(img.header.L1Size) {
		return clusterInfo{ctype: clusterUnallocated}, nil
	}

	// Read L1 entry
	img.l1Mu.RLock()
	l1Entry := binary.BigEndian.Uint64(img.l1Table[l1Index*8:])
	img.l1Mu.RUnlock()

	// Extract L2 table offset
	l2TableOff := l1Entry & L1EntryOffsetMask
	if l2TableOff == 0 {
		return clusterInfo{ctype: clusterUnallocated}, nil
	}

	// Get L2 table (from cache or disk)
	l2Table, err := img.getL2Table(l2TableOff)
	if err != nil {
		return clusterInfo{}, err
	}

	// Read L2 entry
	l2Entry := binary.BigEndian.Uint64(l2Table[l2Index*8:])

	// Check if compressed (we don't support this yet)
	if l2Entry&L2EntryCompressed != 0 {
		return clusterInfo{}, fmt.Errorf("qcow2: compressed clusters not yet supported")
	}

	// Check for zero cluster (bit 0 set)
	if l2Entry&L2EntryZeroFlag != 0 {
		return clusterInfo{ctype: clusterZero}, nil
	}

	// Extract physical offset
	physOff := l2Entry & L2EntryOffsetMask
	if physOff == 0 {
		return clusterInfo{ctype: clusterUnallocated}, nil
	}

	// Add intra-cluster offset
	return clusterInfo{
		ctype:   clusterNormal,
		physOff: physOff + (virtOff & img.offsetMask),
	}, nil
}

// getL2Table retrieves an L2 table, using cache when possible.
func (img *Image) getL2Table(offset uint64) ([]byte, error) {
	// Check cache first
	if table := img.l2Cache.get(offset); table != nil {
		return table, nil
	}

	// Read from disk
	table := make([]byte, img.clusterSize)
	_, err := img.file.ReadAt(table, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("qcow2: failed to read L2 table at 0x%x: %w", offset, err)
	}

	// Add to cache
	img.l2Cache.put(offset, table)

	return table, nil
}

// getClusterForWrite returns the physical offset for writing.
// Allocates a new cluster if needed.
func (img *Image) getClusterForWrite(virtOff uint64) (uint64, error) {
	// Calculate L1 and L2 indices
	l2Index := (virtOff >> img.clusterBits) & (img.l2Entries - 1)
	l1Index := virtOff >> (img.clusterBits + img.l2Bits)

	// Ensure L1 table is large enough
	if l1Index >= uint64(img.header.L1Size) {
		return 0, fmt.Errorf("qcow2: write beyond L1 table bounds")
	}

	// Get or allocate L2 table
	img.l1Mu.Lock()
	l1Entry := binary.BigEndian.Uint64(img.l1Table[l1Index*8:])
	l2TableOff := l1Entry & L1EntryOffsetMask

	if l2TableOff == 0 {
		// Need to allocate L2 table
		var err error
		l2TableOff, err = img.allocateCluster()
		if err != nil {
			img.l1Mu.Unlock()
			return 0, err
		}

		// Zero the new L2 table
		zeros := make([]byte, img.clusterSize)
		if _, err := img.file.WriteAt(zeros, int64(l2TableOff)); err != nil {
			img.l1Mu.Unlock()
			return 0, err
		}

		// Update L1 entry with COPIED flag set
		newL1Entry := l2TableOff | L1EntryCopied
		binary.BigEndian.PutUint64(img.l1Table[l1Index*8:], newL1Entry)

		// Write L1 entry to disk
		if _, err := img.file.WriteAt(img.l1Table[l1Index*8:l1Index*8+8],
			int64(img.header.L1TableOffset+l1Index*8)); err != nil {
			img.l1Mu.Unlock()
			return 0, err
		}
	}
	img.l1Mu.Unlock()

	// Get L2 table
	l2Table, err := img.getL2Table(l2TableOff)
	if err != nil {
		return 0, err
	}

	// Check L2 entry
	l2Entry := binary.BigEndian.Uint64(l2Table[l2Index*8:])
	physOff := l2Entry & L2EntryOffsetMask

	if physOff == 0 {
		// Allocate data cluster
		physOff, err = img.allocateCluster()
		if err != nil {
			return 0, err
		}

		// COW: If we have a backing file, copy the cluster data first
		if img.backing != nil {
			clusterStart := virtOff & ^img.offsetMask // Align to cluster boundary
			clusterData := make([]byte, img.clusterSize)

			// Read from backing file (may be zeros if unallocated there too)
			_, err := img.backing.ReadAt(clusterData, int64(clusterStart))
			if err != nil && err != io.EOF {
				return 0, fmt.Errorf("qcow2: COW read from backing failed: %w", err)
			}

			// Write the backing data to our new cluster
			if _, err := img.file.WriteAt(clusterData, int64(physOff)); err != nil {
				return 0, fmt.Errorf("qcow2: COW write failed: %w", err)
			}
		}

		// Update L2 entry with COPIED flag
		newL2Entry := physOff | L2EntryCopied
		binary.BigEndian.PutUint64(l2Table[l2Index*8:], newL2Entry)

		// Write L2 entry to disk
		if _, err := img.file.WriteAt(l2Table[l2Index*8:l2Index*8+8],
			int64(l2TableOff+l2Index*8)); err != nil {
			return 0, err
		}

		// Update cache
		img.l2Cache.put(l2TableOff, l2Table)
	}

	// Add intra-cluster offset
	return physOff + (virtOff & img.offsetMask), nil
}

// allocateCluster finds and allocates a new cluster.
// This is a simple implementation that grows the file.
func (img *Image) allocateCluster() (uint64, error) {
	// Get current file size (new cluster will be at the end)
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

	return offset, nil
}

// Flush syncs all pending writes to disk.
func (img *Image) Flush() error {
	if img.dirty {
		if err := img.file.Sync(); err != nil {
			return err
		}
		img.dirty = false
	}
	return nil
}

// Close closes the image file.
// On clean close, the dirty bit is cleared.
func (img *Image) Close() error {
	if err := img.Flush(); err != nil {
		return err
	}

	// Clear dirty bit on clean close (v3 only, RW only)
	if !img.readOnly && img.header.Version >= Version3 {
		if err := img.clearDirty(); err != nil {
			// Log but don't fail - data is already flushed
			// The image will just need repair on next open
		}
	}

	if img.backing != nil {
		if err := img.backing.Close(); err != nil {
			return err
		}
	}
	return img.file.Close()
}

// Header returns the image header (read-only).
func (img *Image) Header() Header {
	return *img.header
}

// IsDirty returns true if the image is marked dirty.
// A dirty image was not cleanly closed and may need repair.
func (img *Image) IsDirty() bool {
	return img.header.IsDirty()
}
