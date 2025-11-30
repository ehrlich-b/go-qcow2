package qcow2

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// BackingStore is the interface for backing files (qcow2 or raw).
type BackingStore interface {
	io.ReaderAt
	io.Closer
}

// Image is the primary interface for interacting with a QCOW2 image.
// It implements io.ReaderAt and io.WriterAt for random access.
type Image struct {
	file             *os.File
	externalDataFile *os.File // External data file (when IncompatExternalData is set)
	header           *Header

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

	// Compressed cluster cache - keeps decompressed clusters
	compressedCache *compressedClusterCache

	// Refcount table (level 1) - loaded entirely into memory
	refcountTable     []byte
	refcountTableLock sync.RWMutex

	// Refcount block cache (level 2) - LRU cache of refcount blocks
	refcountBlockCache *l2Cache

	// Write tracking
	readOnly bool
	dirty    bool

	// Lazy refcounts mode - defer refcount updates for better write performance
	lazyRefcounts bool

	// Free cluster tracking - bitmap for O(1) allocation
	freeBitmap     *freeClusterBitmap
	freeBitmapOnce sync.Once

	// Backing file for COW chains
	backing BackingStore

	// Chain depth - how deep this image is in the backing chain (0 = top level)
	chainDepth int

	// Header extensions
	extensions *HeaderExtensions

	// Snapshots
	snapshots []*Snapshot

	// Write ordering barrier mode
	barrierMode WriteBarrierMode

	// Pending sync flag for batched barrier mode
	pendingSync bool

	// Compression level for write operations (CompressionDisabled by default)
	compressionLevel CompressionLevel

	// Compression type for write operations (deflate by default, can be zstd)
	compressionType uint8

	// AES decryptor for legacy encrypted images (method=1)
	aesDecryptor *AESDecryptor

	// LUKS decryptor for modern encrypted images (method=2)
	luksDecryptor *LUKSDecryptor

	// Extended L2 entries support (128-bit entries with 32 subclusters)
	extendedL2     bool   // True if IncompatExtendedL2 feature is set
	l2EntrySize    uint32 // 8 for standard, 16 for extended L2
	subclusterSize uint64 // Cluster size / 32 (only used for extended L2)
	subclusters    uint32 // Number of subclusters per cluster (32 for extended L2, 1 otherwise)

	// Buffer pool for cluster-sized allocations
	clusterPool sync.Pool
}

// getClusterBuffer retrieves a cluster-sized buffer from the pool.
// The buffer contents are undefined; caller must initialize if needed.
func (img *Image) getClusterBuffer() []byte {
	buf, ok := img.clusterPool.Get().([]byte)
	if !ok {
		panic("qcow2: cluster pool type assertion failed")
	}
	return buf
}

// putClusterBuffer returns a cluster-sized buffer to the pool.
func (img *Image) putClusterBuffer(buf []byte) {
	//nolint:staticcheck // SA6002: []byte is reference type, underlying array is heap-allocated
	img.clusterPool.Put(buf)
}

// getZeroedClusterBuffer retrieves a zeroed cluster-sized buffer from the pool.
func (img *Image) getZeroedClusterBuffer() []byte {
	buf := img.getClusterBuffer()
	for i := range buf {
		buf[i] = 0
	}
	return buf
}

// dataFile returns the file handle for cluster data I/O.
// If an external data file is configured, returns that; otherwise returns the main image file.
func (img *Image) dataFile() *os.File {
	if img.externalDataFile != nil {
		return img.externalDataFile
	}
	return img.file
}

// metadataBarrier issues a sync if barrier mode requires it for metadata updates.
func (img *Image) metadataBarrier() error {
	switch img.barrierMode {
	case BarrierNone:
		return nil
	case BarrierBatched:
		img.pendingSync = true
		return nil
	default: // BarrierMetadata, BarrierFull
		return img.file.Sync()
	}
}

// dataBarrier issues a sync if barrier mode requires it for data writes.
func (img *Image) dataBarrier() error {
	switch img.barrierMode {
	case BarrierNone:
		return nil
	case BarrierBatched:
		img.pendingSync = true
		return nil
	case BarrierMetadata:
		return nil
	case BarrierFull:
		// Sync the data file (external or main)
		return img.dataFile().Sync()
	}
	return nil
}

// SetWriteBarrierMode sets the write ordering barrier mode.
// This can be changed at any time; the new mode applies to subsequent writes.
func (img *Image) SetWriteBarrierMode(mode WriteBarrierMode) {
	img.barrierMode = mode
}

// WriteBarrierMode returns the current write ordering barrier mode.
func (img *Image) WriteBarrierMode() WriteBarrierMode {
	return img.barrierMode
}

// SetCompressionLevel sets the compression level for write operations.
// When set to a value other than CompressionDisabled, full cluster writes
// will be compressed if beneficial. Partial cluster writes are never compressed.
func (img *Image) SetCompressionLevel(level CompressionLevel) {
	img.compressionLevel = level
}

// CompressionLevel returns the current compression level setting.
func (img *Image) GetCompressionLevel() CompressionLevel {
	return img.compressionLevel
}

// SetCompressionType sets the compression type for write operations.
// Use CompressionZlib (default) or CompressionZstd.
// Note: When writing compressed clusters, the type is stored in the header.
func (img *Image) SetCompressionType(ctype uint8) {
	img.compressionType = ctype
}

// GetCompressionType returns the current compression type setting.
func (img *Image) GetCompressionType() uint8 {
	return img.compressionType
}

// Open opens an existing QCOW2 image file.
func Open(path string) (*Image, error) {
	return OpenFile(path, os.O_RDWR, 0)
}

// OpenFile opens a QCOW2 image with specific flags.
func OpenFile(path string, flag int, perm os.FileMode) (*Image, error) {
	return openFileWithDepth(path, flag, perm, 0)
}

// openFileWithDepth opens a QCOW2 image tracking backing chain depth.
func openFileWithDepth(path string, flag int, perm os.FileMode, depth int) (*Image, error) {
	if depth > MaxBackingChainDepth {
		return nil, ErrBackingChainTooDeep
	}

	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, fmt.Errorf("qcow2: failed to open file: %w", err)
	}

	img, err := newImage(f, flag&os.O_RDWR == 0 || flag == os.O_RDONLY, depth)
	if err != nil {
		f.Close()
		return nil, err
	}

	return img, nil
}

// newImage creates an Image from an already-open file.
func newImage(f *os.File, readOnly bool, chainDepth int) (*Image, error) {
	// Read header (include extra byte for compression type at offset 104)
	headerBuf := make([]byte, HeaderSizeV3+1)
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
		file:          f,
		header:        header,
		clusterSize:   header.ClusterSize(),
		clusterBits:   header.ClusterBits,
		l2Entries:     header.L2Entries(),
		offsetMask:    header.ClusterSize() - 1,
		readOnly:      readOnly,
		lazyRefcounts: header.HasLazyRefcounts(),
		chainDepth:    chainDepth,
		barrierMode:   BarrierMetadata, // Default: sync after metadata updates
	}

	// Configure L2 entry handling based on extended L2 feature
	if header.HasExtendedL2() {
		img.extendedL2 = true
		img.l2EntrySize = 16                      // 128-bit entries
		img.l2Bits = header.ClusterBits - 4       // Each L2 entry is 16 bytes
		img.l2Entries = img.clusterSize / 16      // Fewer entries per table
		img.subclusters = 32                      // 32 subclusters per cluster
		img.subclusterSize = img.clusterSize / 32 // Each subcluster is cluster/32 bytes
	} else {
		img.extendedL2 = false
		img.l2EntrySize = 8                  // 64-bit entries
		img.l2Bits = header.ClusterBits - 3  // Each L2 entry is 8 bytes
		img.subclusters = 1                  // No subclusters
		img.subclusterSize = img.clusterSize // Subcluster = full cluster
	}

	// Load L1 table
	if err := img.loadL1Table(); err != nil {
		return nil, fmt.Errorf("qcow2: failed to load L1 table: %w", err)
	}

	// Initialize L2 cache (default 32 entries = 2MB with 64KB clusters)
	img.l2Cache = newL2Cache(32, int(img.clusterSize))

	// Initialize compressed cluster cache (default 16 entries)
	img.compressedCache = newCompressedClusterCache(16, int(img.clusterSize))

	// Initialize refcount block cache (default 16 entries)
	img.refcountBlockCache = newL2Cache(16, int(img.clusterSize))

	// Initialize cluster buffer pool
	clusterSize := img.clusterSize
	img.clusterPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, clusterSize)
		},
	}

	// If lazy refcounts enabled and image is dirty, rebuild refcounts
	if !readOnly && header.HasLazyRefcounts() && header.IsDirty() {
		if err := img.rebuildRefcounts(); err != nil {
			return nil, fmt.Errorf("qcow2: failed to rebuild refcounts: %w", err)
		}
	}

	// Mark image dirty if opened for writing (v3 only)
	if !readOnly && header.Version >= Version3 {
		if err := img.markDirty(); err != nil {
			return nil, fmt.Errorf("qcow2: failed to mark image dirty: %w", err)
		}
	}

	// Parse header extensions
	extensions, err := img.parseHeaderExtensions()
	if err != nil {
		return nil, fmt.Errorf("qcow2: failed to parse header extensions: %w", err)
	}
	img.extensions = extensions

	// Open external data file if required
	if err := img.openExternalDataFile(f.Name(), readOnly); err != nil {
		return nil, err
	}

	// Load snapshots if present
	if err := img.loadSnapshots(); err != nil {
		return nil, fmt.Errorf("qcow2: failed to load snapshots: %w", err)
	}

	// Open backing file if present
	if err := img.openBackingFile(); err != nil {
		return nil, err
	}

	return img, nil
}

// openExternalDataFile opens the external data file if the image requires one.
func (img *Image) openExternalDataFile(imagePath string, readOnly bool) error {
	if !img.header.HasExternalDataFile() {
		return nil // No external data file required
	}

	// External data file name should be in the header extensions
	if img.extensions == nil || img.extensions.ExternalDataFile == "" {
		return ErrExternalDataFileMissing
	}

	dataPath := img.extensions.ExternalDataFile

	// Validate path
	if strings.ContainsRune(dataPath, 0) {
		return fmt.Errorf("qcow2: external data file path contains null byte")
	}
	dataPath = strings.TrimSpace(dataPath)
	if dataPath == "" {
		return ErrExternalDataFileMissing
	}

	// Resolve relative paths relative to the image file
	if !filepath.IsAbs(dataPath) {
		imgDir := filepath.Dir(imagePath)
		dataPath = filepath.Join(imgDir, dataPath)
	}

	// Open the external data file
	flag := os.O_RDWR
	if readOnly {
		flag = os.O_RDONLY
	}

	f, err := os.OpenFile(dataPath, flag, 0)
	if err != nil {
		return fmt.Errorf("qcow2: failed to open external data file %q: %w", dataPath, err)
	}

	img.externalDataFile = f
	return nil
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
			// Read from allocated cluster (use dataFile for external data file support)
			switch img.header.EncryptMethod {
			case EncryptionAES:
				// Legacy AES encryption - need to decrypt
				read, err := img.readEncrypted(p[:toRead], info.physOff, uint64(off))
				n += read
				if err != nil {
					return n, err
				}
			case EncryptionLUKS:
				// LUKS encryption - need to decrypt
				read, err := img.readLUKSEncrypted(p[:toRead], info.physOff, uint64(off))
				n += read
				if err != nil {
					return n, err
				}
			default:
				// Normal unencrypted read
				read, err := img.dataFile().ReadAt(p[:toRead], int64(info.physOff))
				n += read
				if err != nil {
					return n, err
				}
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
			// Get decompressed cluster data (from cache or decompress)
			clusterStart := uint64(off) & ^img.offsetMask
			cacheKey := info.l2Entry // Use L2 entry as cache key

			decompressed := img.compressedCache.get(cacheKey)
			if decompressed == nil {
				var err error
				decompressed, err = img.decompressCluster(info.l2Entry)
				if err != nil {
					return n, err
				}
				img.compressedCache.put(cacheKey, decompressed)
			}

			// Read from decompressed data
			clusterOff := uint64(off) - clusterStart
			copy(p[:toRead], decompressed[clusterOff:clusterOff+toRead])
			n += int(toRead)
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

	// Check encryption support
	switch img.header.EncryptMethod {
	case EncryptionNone:
		// No encryption, continue normally
	case EncryptionLUKS:
		// LUKS encryption supported - use encrypted write path
		if img.luksDecryptor == nil {
			return 0, fmt.Errorf("qcow2: LUKS encrypted image requires password (call SetPasswordLUKS)")
		}
		return img.writeAtLUKS(p, off)
	default:
		// Legacy AES encryption not supported for writes
		return 0, fmt.Errorf("qcow2: writing to AES-encrypted images is not supported")
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

		// Write to allocated cluster (use dataFile for external data file support)
		written, err := img.dataFile().WriteAt(p[:toWrite], int64(physOff))
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

// writeAtLUKS handles writes to LUKS-encrypted images.
// It encrypts data before writing and handles partial cluster writes correctly.
func (img *Image) writeAtLUKS(p []byte, off int64) (n int, err error) {
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

		// Check if cluster was previously allocated (before getClusterForWrite may allocate it)
		wasAllocated := img.isClusterAllocated(uint64(off))

		// Get or allocate physical cluster
		physOff, err := img.getClusterForWrite(uint64(off))
		if err != nil {
			return n, err
		}

		// For LUKS: handle encryption
		// isNewCluster means the cluster was NOT previously allocated
		isNewCluster := !wasAllocated

		// Write encrypted data
		written, err := img.writeLUKSEncrypted(p[:toWrite], physOff, isNewCluster)
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

// isClusterAllocated checks if a cluster at the given virtual offset is already allocated.
// This is used to determine if we need to read existing data before a partial write.
func (img *Image) isClusterAllocated(virtOff uint64) bool {
	l2Index := (virtOff >> img.clusterBits) & (img.l2Entries - 1)
	l1Index := virtOff >> (img.clusterBits + img.l2Bits)

	// Check L1 bounds
	if l1Index >= uint64(len(img.l1Table)/8) {
		return false
	}

	// Get L2 table offset from L1
	l1Entry := binary.BigEndian.Uint64(img.l1Table[l1Index*8:])
	l2TableOff := l1Entry & L2EntryOffsetMask
	if l2TableOff == 0 {
		return false
	}

	// Get L2 table
	l2Table, err := img.getL2Table(l2TableOff)
	if err != nil {
		return false
	}

	// Check L2 entry
	l2Entry := binary.BigEndian.Uint64(l2Table[l2Index*8:])
	physOff := l2Entry & L2EntryOffsetMask

	return physOff != 0
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
	l2Entry uint64 // Raw L2 entry (needed for compressed clusters)

	// Extended L2 fields (for subcluster-level allocation)
	extL2Bitmap uint64 // Second 64-bit word of extended L2 entry
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

	// Read L2 entry (8 bytes for standard, first 8 of 16 for extended)
	entryOffset := l2Index * uint64(img.l2EntrySize)
	l2Entry := binary.BigEndian.Uint64(l2Table[entryOffset:])

	// Read extended L2 bitmap if applicable
	var extL2Bitmap uint64
	if img.extendedL2 {
		extL2Bitmap = binary.BigEndian.Uint64(l2Table[entryOffset+8:])
	}

	// Check if compressed (not supported with extended L2 in QEMU)
	if l2Entry&L2EntryCompressed != 0 {
		return clusterInfo{
			ctype:   clusterCompressed,
			l2Entry: l2Entry,
		}, nil
	}

	// For extended L2, check subcluster status
	if img.extendedL2 {
		return img.translateExtendedL2(virtOff, l2Entry, extL2Bitmap)
	}

	// Standard L2: Check for zero cluster (bit 0 set)
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

// translateExtendedL2 handles translation for extended L2 entries with subclusters.
func (img *Image) translateExtendedL2(virtOff uint64, l2Entry uint64, extL2Bitmap uint64) (clusterInfo, error) {
	// Calculate which subcluster within the cluster
	intraClusterOff := virtOff & img.offsetMask
	subclusterIndex := intraClusterOff / img.subclusterSize

	// Check the allocation and zero bitmaps for this subcluster
	allocBit := (extL2Bitmap >> subclusterIndex) & 1
	zeroBit := (extL2Bitmap >> (32 + subclusterIndex)) & 1

	// Extract physical cluster offset
	physClusterOff := l2Entry & L2EntryOffsetMask

	// Interpret the subcluster status
	if allocBit == 0 {
		if zeroBit != 0 {
			// Subcluster is explicitly zero
			return clusterInfo{ctype: clusterZero}, nil
		}
		// Subcluster is unallocated (read from backing or return zeros)
		return clusterInfo{ctype: clusterUnallocated}, nil
	}

	// allocBit == 1: subcluster is allocated
	if physClusterOff == 0 {
		// This shouldn't happen for allocated subclusters
		return clusterInfo{ctype: clusterUnallocated}, nil
	}

	// Return physical offset including intra-cluster offset
	return clusterInfo{
		ctype:       clusterNormal,
		physOff:     physClusterOff + intraClusterOff,
		extL2Bitmap: extL2Bitmap,
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

// getOrAllocateL2Table returns the offset of the L2 table for the given L1 index,
// allocating a new L2 table if necessary, or COW'ing a shared L2 table.
func (img *Image) getOrAllocateL2Table(l1Index uint64) (uint64, error) {
	// Ensure L1 table is large enough
	if l1Index >= uint64(img.header.L1Size) {
		return 0, fmt.Errorf("qcow2: write beyond L1 table bounds")
	}

	img.l1Mu.Lock()
	defer img.l1Mu.Unlock()

	l1Entry := binary.BigEndian.Uint64(img.l1Table[l1Index*8:])
	l2TableOff := l1Entry & L1EntryOffsetMask
	isCopied := l1Entry&L1EntryCopied != 0

	if l2TableOff != 0 {
		// L2 table exists - check if we need to COW it
		if !isCopied {
			// COPIED flag not set - L2 table may be shared
			refcount, err := img.getRefcount(l2TableOff)
			if err != nil {
				return 0, fmt.Errorf("qcow2: failed to get L2 table refcount: %w", err)
			}

			if refcount > 1 {
				// L2 table is shared - need to COW
				newL2TableOff, err := img.allocateCluster()
				if err != nil {
					return 0, fmt.Errorf("qcow2: failed to allocate L2 table for COW: %w", err)
				}

				// Copy L2 table content
				l2Data := make([]byte, img.clusterSize)
				if _, err := img.file.ReadAt(l2Data, int64(l2TableOff)); err != nil {
					return 0, fmt.Errorf("qcow2: failed to read L2 table for COW: %w", err)
				}
				if _, err := img.file.WriteAt(l2Data, int64(newL2TableOff)); err != nil {
					return 0, fmt.Errorf("qcow2: failed to write L2 table COW: %w", err)
				}

				// Decrement refcount for old L2 table
				if err := img.decrementRefcount(l2TableOff); err != nil {
					return 0, fmt.Errorf("qcow2: failed to decrement old L2 table refcount: %w", err)
				}

				// Barrier: ensure L2 table is on disk before L1 points to it
				if err := img.metadataBarrier(); err != nil {
					return 0, fmt.Errorf("qcow2: L2 table COW barrier failed: %w", err)
				}

				// Update L1 entry to point to new L2 table with COPIED flag
				newL1Entry := newL2TableOff | L1EntryCopied
				binary.BigEndian.PutUint64(img.l1Table[l1Index*8:], newL1Entry)

				// Write L1 entry to disk
				if _, err := img.file.WriteAt(img.l1Table[l1Index*8:l1Index*8+8],
					int64(img.header.L1TableOffset+l1Index*8)); err != nil {
					return 0, err
				}

				// Invalidate old L2 table from cache
				img.l2Cache.invalidate(l2TableOff)

				// Update cache with new L2 table
				img.l2Cache.put(newL2TableOff, l2Data)

				return newL2TableOff, nil
			}

			// Refcount is 1 - safe to modify, just set COPIED flag
			newL1Entry := l2TableOff | L1EntryCopied
			binary.BigEndian.PutUint64(img.l1Table[l1Index*8:], newL1Entry)
			if _, err := img.file.WriteAt(img.l1Table[l1Index*8:l1Index*8+8],
				int64(img.header.L1TableOffset+l1Index*8)); err != nil {
				return 0, err
			}
		}

		return l2TableOff, nil
	}

	// Need to allocate new L2 table
	var err error
	l2TableOff, err = img.allocateCluster()
	if err != nil {
		return 0, err
	}

	// Zero the new L2 table using pooled buffer
	zeros := img.getZeroedClusterBuffer()
	_, writeErr := img.file.WriteAt(zeros, int64(l2TableOff))
	img.putClusterBuffer(zeros)
	if writeErr != nil {
		return 0, writeErr
	}

	// Barrier: ensure L2 table is on disk before L1 points to it
	if err := img.metadataBarrier(); err != nil {
		return 0, fmt.Errorf("qcow2: L2 table barrier failed: %w", err)
	}

	// Update L1 entry with COPIED flag set
	newL1Entry := l2TableOff | L1EntryCopied
	binary.BigEndian.PutUint64(img.l1Table[l1Index*8:], newL1Entry)

	// Write L1 entry to disk
	if _, err := img.file.WriteAt(img.l1Table[l1Index*8:l1Index*8+8],
		int64(img.header.L1TableOffset+l1Index*8)); err != nil {
		return 0, err
	}

	// Barrier: ensure L1 update is on disk
	if err := img.metadataBarrier(); err != nil {
		return 0, fmt.Errorf("qcow2: L1 update barrier failed: %w", err)
	}

	return l2TableOff, nil
}

// getClusterForWrite returns the physical offset for writing.
// Allocates a new cluster if needed, or performs COW if the cluster is shared.
func (img *Image) getClusterForWrite(virtOff uint64) (uint64, error) {
	// Calculate L1 and L2 indices
	l2Index := (virtOff >> img.clusterBits) & (img.l2Entries - 1)
	l1Index := virtOff >> (img.clusterBits + img.l2Bits)

	// Get or allocate L2 table
	l2TableOff, err := img.getOrAllocateL2Table(l1Index)
	if err != nil {
		return 0, err
	}

	// Get L2 table
	l2Table, err := img.getL2Table(l2TableOff)
	if err != nil {
		return 0, err
	}

	// Check L2 entry
	l2Entry := binary.BigEndian.Uint64(l2Table[l2Index*8:])
	physOff := l2Entry & L2EntryOffsetMask
	isCopied := l2Entry&L2EntryCopied != 0

	// Check if we need to allocate or COW
	needsAlloc := physOff == 0
	needsCOW := false

	if physOff != 0 && !isCopied {
		// COPIED flag is not set - cluster may be shared
		// Check refcount to decide if we need COW
		refcount, err := img.getRefcount(physOff)
		if err != nil {
			return 0, fmt.Errorf("qcow2: failed to get refcount for COW check: %w", err)
		}
		if refcount > 1 {
			needsCOW = true
		} else if refcount == 1 {
			// Refcount is 1, we can safely write and set COPIED flag
			newL2Entry := physOff | L2EntryCopied
			binary.BigEndian.PutUint64(l2Table[l2Index*8:], newL2Entry)
			if _, err := img.file.WriteAt(l2Table[l2Index*8:l2Index*8+8],
				int64(l2TableOff+l2Index*8)); err != nil {
				return 0, err
			}
			img.l2Cache.put(l2TableOff, l2Table)
		}
	}

	if needsAlloc || needsCOW {
		oldPhysOff := physOff

		// Allocate new data cluster
		physOff, err = img.allocateCluster()
		if err != nil {
			return 0, err
		}

		// COW: Copy existing data to new cluster
		dataFile := img.dataFile() // Use external data file if present
		if needsCOW {
			// Read from old cluster
			clusterData := make([]byte, img.clusterSize)
			if _, err := dataFile.ReadAt(clusterData, int64(oldPhysOff)); err != nil {
				return 0, fmt.Errorf("qcow2: COW read failed: %w", err)
			}

			// Write to new cluster
			if _, err := dataFile.WriteAt(clusterData, int64(physOff)); err != nil {
				return 0, fmt.Errorf("qcow2: COW write failed: %w", err)
			}

			// Decrement refcount for old cluster (now one less reference)
			if err := img.decrementRefcount(oldPhysOff); err != nil {
				return 0, fmt.Errorf("qcow2: failed to decrement old cluster refcount: %w", err)
			}
		} else if img.backing != nil {
			// No existing data but have backing file - copy from backing
			clusterStart := virtOff & ^img.offsetMask // Align to cluster boundary
			clusterData := make([]byte, img.clusterSize)

			// Read from backing file (may be zeros if unallocated there too)
			_, err := img.backing.ReadAt(clusterData, int64(clusterStart))
			if err != nil && err != io.EOF {
				return 0, fmt.Errorf("qcow2: COW read from backing failed: %w", err)
			}

			// Write the backing data to our new cluster
			if _, err := dataFile.WriteAt(clusterData, int64(physOff)); err != nil {
				return 0, fmt.Errorf("qcow2: COW write failed: %w", err)
			}
		}

		// Barrier: ensure data is on disk before L2 points to it
		if err := img.dataBarrier(); err != nil {
			return 0, fmt.Errorf("qcow2: data barrier failed: %w", err)
		}

		// Update L2 entry with COPIED flag
		newL2Entry := physOff | L2EntryCopied
		binary.BigEndian.PutUint64(l2Table[l2Index*8:], newL2Entry)

		// Write L2 entry to disk
		if _, err := img.file.WriteAt(l2Table[l2Index*8:l2Index*8+8],
			int64(l2TableOff+l2Index*8)); err != nil {
			return 0, err
		}

		// Barrier: ensure L2 update is on disk
		if err := img.metadataBarrier(); err != nil {
			return 0, fmt.Errorf("qcow2: L2 update barrier failed: %w", err)
		}

		// Update cache
		img.l2Cache.put(l2TableOff, l2Table)
	}

	// Add intra-cluster offset
	return physOff + (virtOff & img.offsetMask), nil
}

// allocateCluster finds and allocates a new cluster.
// First tries to reuse a free cluster (refcount == 0), then grows the file.
// It also updates the refcount for the new cluster.
func (img *Image) allocateCluster() (uint64, error) {
	// In lazy refcounts mode, skip free cluster search - always grow file.
	// This is because refcounts aren't updated in lazy mode, so we can't
	// reliably determine which clusters are free.
	dataFile := img.dataFile() // Use external data file if present
	if !img.lazyRefcounts {
		// Try to find a free cluster first
		if offset, found := img.findFreeCluster(); found {
			// Zero the cluster before reuse
			zeros := make([]byte, img.clusterSize)
			if _, err := dataFile.WriteAt(zeros, int64(offset)); err != nil {
				return 0, err
			}

			// Update refcount for the reused cluster
			if err := img.incrementRefcount(offset); err != nil {
				return 0, fmt.Errorf("qcow2: failed to update refcount for reused cluster: %w", err)
			}

			return offset, nil
		}
	}

	// No free clusters found, grow the file
	info, err := dataFile.Stat()
	if err != nil {
		return 0, err
	}

	// Align to cluster boundary
	offset := uint64(info.Size())
	if offset&img.offsetMask != 0 {
		offset = (offset + img.clusterSize) & ^img.offsetMask
	}

	// Extend file
	if err := dataFile.Truncate(int64(offset + img.clusterSize)); err != nil {
		return 0, err
	}

	// Grow bitmap if it exists to track the new cluster
	if img.freeBitmap != nil {
		newNumClusters := (offset + img.clusterSize) >> img.clusterBits
		img.freeBitmap.grow(newNumClusters)
	}

	// Update refcount for the new cluster
	if err := img.incrementRefcount(offset); err != nil {
		return 0, fmt.Errorf("qcow2: failed to update refcount for new cluster: %w", err)
	}

	return offset, nil
}

// buildFreeBitmap scans refcounts and builds the free cluster bitmap.
// Called once lazily on first free cluster search.
func (img *Image) buildFreeBitmap() {
	if err := img.loadRefcountTable(); err != nil {
		return
	}

	// Calculate file size to determine number of clusters
	info, err := img.file.Stat()
	if err != nil {
		return
	}
	numClusters := uint64(info.Size()) >> img.clusterBits
	if numClusters == 0 {
		return
	}

	// Skip first 4 clusters (header and initial metadata)
	minCluster := uint64(4)

	img.freeBitmap = newFreeClusterBitmap(numClusters, minCluster)

	// Scan refcounts and mark free clusters
	for clusterIdx := minCluster; clusterIdx < numClusters; clusterIdx++ {
		refcount, err := img.getRefcount(clusterIdx << img.clusterBits)
		if err != nil {
			continue
		}
		if refcount == 0 {
			clusterOff := clusterIdx << img.clusterBits
			if !img.isMetadataCluster(clusterOff) {
				img.freeBitmap.setFree(clusterIdx)
			}
		}
	}
}

// findFreeCluster searches for a cluster with refcount == 0 using O(1) bitmap lookup.
// Returns the cluster offset and true if found, or 0 and false if none available.
func (img *Image) findFreeCluster() (uint64, bool) {
	// Build bitmap lazily on first use
	img.freeBitmapOnce.Do(img.buildFreeBitmap)

	if img.freeBitmap == nil {
		return 0, false
	}

	// O(1) lookup using bitmap
	clusterIdx, found := img.freeBitmap.findFree()
	if !found {
		return 0, false
	}

	clusterOff := clusterIdx << img.clusterBits

	// Double-check this isn't metadata (bitmap should already exclude, but be safe)
	if img.isMetadataCluster(clusterOff) {
		// Mark as used in bitmap and try again
		img.freeBitmap.setUsed(clusterIdx)
		return img.findFreeCluster()
	}

	return clusterOff, true
}

// isMetadataCluster checks if a cluster offset contains QCOW2 metadata.
// Returns true for header, L1 table, L2 tables, refcount table, and refcount blocks.
func (img *Image) isMetadataCluster(offset uint64) bool {
	// Align offset to cluster boundary
	clusterOffset := offset & ^img.offsetMask

	// Header cluster
	if clusterOffset < img.clusterSize {
		return true
	}

	// L1 table
	l1Start := img.header.L1TableOffset & ^img.offsetMask
	l1Size := uint64(img.header.L1Size) * 8
	if l1Size > 0 {
		l1LastByte := img.header.L1TableOffset + l1Size - 1
		l1EndCluster := l1LastByte & ^img.offsetMask
		if clusterOffset >= l1Start && clusterOffset <= l1EndCluster {
			return true
		}
	}

	// Refcount table
	refStart := img.header.RefcountTableOffset & ^img.offsetMask
	refSize := uint64(img.header.RefcountTableClusters) * img.clusterSize
	if clusterOffset >= refStart && clusterOffset < refStart+refSize {
		return true
	}

	// Check if it's an L2 table (scan L1 entries)
	if img.isL2TableCluster(clusterOffset) {
		return true
	}

	// Check if it's a refcount block (scan refcount table entries)
	if img.isRefcountBlockCluster(clusterOffset) {
		return true
	}

	return false
}

// isL2TableCluster checks if an offset points to an L2 table cluster.
func (img *Image) isL2TableCluster(offset uint64) bool {
	if len(img.l1Table) == 0 {
		return false
	}

	l1Entries := uint64(img.header.L1Size)
	for i := uint64(0); i < l1Entries; i++ {
		l1Entry := binary.BigEndian.Uint64(img.l1Table[i*8:])
		if l1Entry == 0 {
			continue
		}
		l2Offset := l1Entry & L2EntryOffsetMask
		l2ClusterStart := l2Offset & ^img.offsetMask
		if offset >= l2ClusterStart && offset < l2ClusterStart+img.clusterSize {
			return true
		}
	}
	return false
}

// isRefcountBlockCluster checks if an offset points to a refcount block cluster.
func (img *Image) isRefcountBlockCluster(offset uint64) bool {
	if len(img.refcountTable) == 0 {
		return false
	}

	tableEntries := uint64(len(img.refcountTable)) / 8
	for i := uint64(0); i < tableEntries; i++ {
		blockOffset := binary.BigEndian.Uint64(img.refcountTable[i*8:])
		if blockOffset == 0 {
			continue
		}
		blockClusterStart := blockOffset & ^img.offsetMask
		if offset >= blockClusterStart && offset < blockClusterStart+img.clusterSize {
			return true
		}
	}
	return false
}

// OverlapCheckResult describes what type of metadata a host offset overlaps with.
type OverlapCheckResult struct {
	Overlaps       bool
	MetadataType   string // "header", "l1_table", "l2_table", "refcount_table", "refcount_block", or ""
	MetadataOffset uint64
}

// CheckOverlap checks if a host offset overlaps with any QCOW2 metadata.
// This is useful for debugging and verification.
func (img *Image) CheckOverlap(hostOffset uint64) OverlapCheckResult {
	clusterOffset := hostOffset & ^img.offsetMask

	// Header cluster
	if clusterOffset < img.clusterSize {
		return OverlapCheckResult{Overlaps: true, MetadataType: "header", MetadataOffset: 0}
	}

	// L1 table
	l1Start := img.header.L1TableOffset & ^img.offsetMask
	l1Size := uint64(img.header.L1Size) * 8
	if l1Size > 0 {
		l1LastByte := img.header.L1TableOffset + l1Size - 1
		l1EndCluster := l1LastByte & ^img.offsetMask
		if clusterOffset >= l1Start && clusterOffset <= l1EndCluster {
			return OverlapCheckResult{Overlaps: true, MetadataType: "l1_table", MetadataOffset: img.header.L1TableOffset}
		}
	}

	// Refcount table
	refStart := img.header.RefcountTableOffset & ^img.offsetMask
	refSize := uint64(img.header.RefcountTableClusters) * img.clusterSize
	if clusterOffset >= refStart && clusterOffset < refStart+refSize {
		return OverlapCheckResult{Overlaps: true, MetadataType: "refcount_table", MetadataOffset: img.header.RefcountTableOffset}
	}

	// L2 tables
	if len(img.l1Table) > 0 {
		l1Entries := uint64(img.header.L1Size)
		for i := uint64(0); i < l1Entries; i++ {
			l1Entry := binary.BigEndian.Uint64(img.l1Table[i*8:])
			if l1Entry == 0 {
				continue
			}
			l2Offset := l1Entry & L2EntryOffsetMask
			l2ClusterStart := l2Offset & ^img.offsetMask
			if clusterOffset >= l2ClusterStart && clusterOffset < l2ClusterStart+img.clusterSize {
				return OverlapCheckResult{Overlaps: true, MetadataType: "l2_table", MetadataOffset: l2Offset}
			}
		}
	}

	// Refcount blocks
	if len(img.refcountTable) > 0 {
		tableEntries := uint64(len(img.refcountTable)) / 8
		for i := uint64(0); i < tableEntries; i++ {
			blockOffset := binary.BigEndian.Uint64(img.refcountTable[i*8:])
			if blockOffset == 0 {
				continue
			}
			blockClusterStart := blockOffset & ^img.offsetMask
			if clusterOffset >= blockClusterStart && clusterOffset < blockClusterStart+img.clusterSize {
				return OverlapCheckResult{Overlaps: true, MetadataType: "refcount_block", MetadataOffset: blockOffset}
			}
		}
	}

	return OverlapCheckResult{Overlaps: false}
}

// Flush syncs all pending writes to disk.
func (img *Image) Flush() error {
	if img.dirty || img.pendingSync {
		// Sync external data file first if present
		if img.externalDataFile != nil {
			if err := img.externalDataFile.Sync(); err != nil {
				return err
			}
		}
		// Then sync main metadata file
		if err := img.file.Sync(); err != nil {
			return err
		}
		img.dirty = false
		img.pendingSync = false
	}
	return nil
}

// Close closes the image file.
// On clean close, the dirty bit is cleared (unless lazy refcounts are enabled,
// in which case the image stays dirty and refcounts are rebuilt on next open).
func (img *Image) Close() error {
	if err := img.Flush(); err != nil {
		return err
	}

	// Clear dirty bit on clean close (v3 only, RW only)
	// Skip if lazy refcounts is enabled - keep dirty bit for refcount rebuild
	if !img.readOnly && img.header.Version >= Version3 && !img.lazyRefcounts {
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

	// Close external data file if present
	if img.externalDataFile != nil {
		if err := img.externalDataFile.Close(); err != nil {
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

// WriteZeroAt writes zeros efficiently using the zero cluster flag.
// This avoids allocating storage for all-zero data.
// It writes zeros from offset off for length bytes.
// Uses ZeroPlain mode which deallocates clusters.
func (img *Image) WriteZeroAt(off int64, length int64) error {
	return img.WriteZeroAtMode(off, length, ZeroPlain)
}

// WriteZeroAtMode writes zeros with the specified zero mode.
// ZeroPlain deallocates clusters for space efficiency.
// ZeroAlloc keeps clusters allocated (useful for preallocated images).
func (img *Image) WriteZeroAtMode(off int64, length int64, mode ZeroMode) error {
	if img.readOnly {
		return ErrReadOnly
	}

	if off < 0 {
		return ErrOffsetOutOfRange
	}

	size := img.Size()
	if off >= size {
		return ErrOffsetOutOfRange
	}

	// Clamp to image size
	if off+length > size {
		length = size - off
	}

	for length > 0 {
		// Calculate cluster boundaries
		clusterStart := uint64(off) & ^img.offsetMask
		clusterOff := uint64(off) & img.offsetMask

		// If this is a partial cluster at the start, we need to do partial zero
		if clusterOff != 0 {
			// Partial cluster - use regular write with zeros
			toWrite := img.clusterSize - clusterOff
			if toWrite > uint64(length) {
				toWrite = uint64(length)
			}
			zeros := make([]byte, toWrite)
			if _, err := img.WriteAt(zeros, off); err != nil {
				return err
			}
			off += int64(toWrite)
			length -= int64(toWrite)
			continue
		}

		// Full cluster - use zero flag
		if uint64(length) >= img.clusterSize {
			if err := img.setZeroCluster(clusterStart, mode); err != nil {
				return err
			}
			off += int64(img.clusterSize)
			length -= int64(img.clusterSize)
			continue
		}

		// Partial cluster at the end
		zeros := make([]byte, length)
		if _, err := img.WriteAt(zeros, off); err != nil {
			return err
		}
		break
	}

	img.dirty = true
	return nil
}

// setZeroCluster marks a cluster as zero using the specified mode.
// ZeroPlain: clears the offset and decrements refcount (space efficient).
// ZeroAlloc: keeps the offset and refcount (preserves allocation).
func (img *Image) setZeroCluster(virtOff uint64, mode ZeroMode) error {
	// Calculate L1 and L2 indices
	l2Index := (virtOff >> img.clusterBits) & (img.l2Entries - 1)
	l1Index := virtOff >> (img.clusterBits + img.l2Bits)

	// Get or allocate L2 table
	l2TableOff, err := img.getOrAllocateL2Table(l1Index)
	if err != nil {
		return err
	}

	// Get L2 table
	l2Table, err := img.getL2Table(l2TableOff)
	if err != nil {
		return err
	}

	// Check current L2 entry
	l2Entry := binary.BigEndian.Uint64(l2Table[l2Index*8:])
	oldOffset := l2Entry & L2EntryOffsetMask

	// Check if already in desired state
	if l2Entry&L2EntryZeroFlag != 0 {
		if mode == ZeroPlain && oldOffset == 0 {
			// Already ZERO_PLAIN
			return nil
		}
		if mode == ZeroAlloc && oldOffset != 0 {
			// Already ZERO_ALLOC
			return nil
		}
	}

	var newL2Entry uint64
	if mode == ZeroAlloc {
		// ZERO_ALLOC: keep the offset, just add zero flag
		if oldOffset == 0 {
			// Can't use ZeroAlloc on unallocated cluster - allocate first
			var allocErr error
			oldOffset, allocErr = img.allocateCluster()
			if allocErr != nil {
				return allocErr
			}
		}
		// Preserve offset and COPIED flag, add zero flag
		newL2Entry = (oldOffset | L2EntryCopied | L2EntryZeroFlag)
	} else {
		// ZERO_PLAIN: clear offset, decrement refcount if was allocated
		if oldOffset != 0 {
			if err := img.decrementRefcount(oldOffset); err != nil {
				return fmt.Errorf("qcow2: failed to decrement refcount for deallocated cluster: %w", err)
			}
		}
		newL2Entry = L2EntryZeroFlag
	}

	binary.BigEndian.PutUint64(l2Table[l2Index*8:], newL2Entry)

	// Write L2 entry to disk
	if _, err := img.file.WriteAt(l2Table[l2Index*8:l2Index*8+8],
		int64(l2TableOff+l2Index*8)); err != nil {
		return err
	}

	// Barrier: ensure L2 update is on disk
	if err := img.metadataBarrier(); err != nil {
		return fmt.Errorf("qcow2: L2 zero update barrier failed: %w", err)
	}

	// Update cache
	img.l2Cache.put(l2TableOff, l2Table)

	return nil
}
