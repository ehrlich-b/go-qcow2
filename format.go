// Package qcow2 provides a pure Go implementation of the QCOW2 disk image format.
package qcow2

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// QCOW2 magic number: "QFI\xfb"
const Magic = 0x514649fb

// QCOW2 versions
const (
	Version2 = 2
	Version3 = 3
)

// Header size constants
const (
	HeaderSizeV2 = 72  // Minimum header size for version 2
	HeaderSizeV3 = 104 // Minimum header size for version 3
)

// Default cluster size is 64KB (1 << 16)
const (
	DefaultClusterBits = 16
	DefaultClusterSize = 1 << DefaultClusterBits
	MinClusterBits     = 9  // 512 bytes
	MaxClusterBits     = 21 // 2MB
)

// Encryption methods
const (
	EncryptionNone = 0
	EncryptionAES  = 1
	EncryptionLUKS = 2
)

// Compression types (when IncompatCompression feature is set)
const (
	CompressionZlib = 0 // Deflate (default)
	CompressionZstd = 1 // Zstandard
)

// Incompatible feature bits (must understand to open)
const (
	IncompatDirtyBit     = 1 << 0 // Image needs repair
	IncompatCorruptBit   = 1 << 1 // Image is corrupt
	IncompatExternalData = 1 << 2 // Data in external file
	IncompatCompression  = 1 << 3 // Compression type in header
	IncompatExtendedL2   = 1 << 4 // Extended L2 entries
)

// Compatible feature bits (can ignore if unknown)
const (
	CompatLazyRefcounts = 1 << 0 // Lazy refcount updates
)

// Autoclear feature bits (clear on open for RW)
const (
	AutoclearBitmaps     = 1 << 0
	AutoclearRawExternal = 1 << 1
)

// WriteBarrierMode controls how write ordering barriers are applied.
// Barriers ensure crash consistency by syncing data to disk before
// updating metadata that references it.
type WriteBarrierMode int

const (
	// BarrierNone disables write ordering barriers (fastest, least safe).
	// Use only for testing or when the underlying storage provides barriers.
	BarrierNone WriteBarrierMode = iota

	// BarrierBatched defers syncs until Flush() is called (fast, reasonably safe).
	// Metadata updates are tracked but syncs are batched for performance.
	// Call Flush() periodically or before Close() to ensure durability.
	// This reduces the 4+ fsyncs per cluster allocation to 1 per Flush.
	BarrierBatched

	// BarrierMetadata syncs after metadata updates (L1/L2 table changes).
	// This ensures metadata is consistent on disk but data writes may be lost.
	BarrierMetadata

	// BarrierFull syncs after every write operation (slowest, safest).
	// Guarantees data written before metadata updates are on disk.
	BarrierFull
)

// ZeroMode controls how zero clusters are written.
type ZeroMode int

const (
	// ZeroPlain deallocates the cluster and sets the zero flag.
	// The cluster offset is cleared and refcount decremented.
	// This is the most space-efficient mode.
	ZeroPlain ZeroMode = iota

	// ZeroAlloc keeps the cluster allocated but sets the zero flag.
	// The cluster offset is preserved and refcount unchanged.
	// Reads still return zeros, but the space remains reserved.
	// Useful for preallocated images or security (avoid cluster reuse).
	ZeroAlloc
)

// L2 entry flags (in the high bits of the 64-bit entry)
const (
	L2EntryCompressed = uint64(1) << 62
	L2EntryCopied     = uint64(1) << 63               // Refcount is exactly 1
	L2EntryOffsetMask = (uint64(1) << 62) - 1 - 0x1ff // Bits 9-61
	L2EntryZeroFlag   = uint64(1) << 0                // Standard cluster - all zeros
)

// L1 entry flags
const (
	L1EntryCopied     = uint64(1) << 63
	L1EntryOffsetMask = (uint64(1) << 62) - 1 - 0x1ff // Bits 9-61
)

// Refcount entry widths
const (
	RefcountBits16 = 16 // Default
	RefcountBits1  = 1
	RefcountBits2  = 2
	RefcountBits4  = 4
	RefcountBits8  = 8
	RefcountBits32 = 32
	RefcountBits64 = 64
)

// Header represents the QCOW2 file header.
// This is read once on open, so struct overhead is acceptable.
type Header struct {
	Magic                 uint32
	Version               uint32
	BackingFileOffset     uint64
	BackingFileSize       uint32
	ClusterBits           uint32
	Size                  uint64 // Virtual size in bytes
	EncryptMethod         uint32
	L1Size                uint32 // Number of entries in L1 table
	L1TableOffset         uint64
	RefcountTableOffset   uint64
	RefcountTableClusters uint32
	NbSnapshots           uint32
	SnapshotsOffset       uint64

	// Version 3+ fields
	IncompatibleFeatures uint64
	CompatibleFeatures   uint64
	AutoclearFeatures    uint64
	RefcountOrder        uint32 // Refcount bits = 1 << refcount_order
	HeaderLength         uint32

	// Compression type (when IncompatCompression feature is set)
	CompressionType uint8
}

// MaxBackingChainDepth is the maximum depth of the backing file chain.
// This matches QEMU's limit and prevents resource exhaustion from malicious images.
const MaxBackingChainDepth = 64

// Errors
var (
	ErrInvalidMagic           = errors.New("qcow2: invalid magic number")
	ErrUnsupportedVersion     = errors.New("qcow2: unsupported version")
	ErrInvalidClusterBits     = errors.New("qcow2: invalid cluster bits")
	ErrIncompatFeatures       = errors.New("qcow2: unsupported incompatible features")
	ErrCorruptImage           = errors.New("qcow2: image is marked corrupt")
	ErrImageDirty             = errors.New("qcow2: image is marked dirty, needs repair")
	ErrOffsetOutOfRange       = errors.New("qcow2: offset out of range")
	ErrReadOnly                 = errors.New("qcow2: image is read-only")
	ErrBackingChainTooDeep      = errors.New("qcow2: backing file chain exceeds maximum depth")
	ErrUnsupportedCompression   = errors.New("qcow2: unsupported compression type (zstd requires external library)")
	ErrCompressionNotBeneficial = errors.New("qcow2: compression not beneficial for this data")
	ErrEncryptedImage           = errors.New("qcow2: encrypted images are not supported")
	ErrExternalDataFileMissing  = errors.New("qcow2: external data file name not specified in header extension")
)

// ParseHeader reads and validates a QCOW2 header from raw bytes.
// The input must be at least HeaderSizeV2 bytes.
func ParseHeader(data []byte) (*Header, error) {
	if len(data) < HeaderSizeV2 {
		return nil, fmt.Errorf("qcow2: header too short: %d bytes", len(data))
	}

	h := &Header{
		Magic:                 binary.BigEndian.Uint32(data[0:4]),
		Version:               binary.BigEndian.Uint32(data[4:8]),
		BackingFileOffset:     binary.BigEndian.Uint64(data[8:16]),
		BackingFileSize:       binary.BigEndian.Uint32(data[16:20]),
		ClusterBits:           binary.BigEndian.Uint32(data[20:24]),
		Size:                  binary.BigEndian.Uint64(data[24:32]),
		EncryptMethod:         binary.BigEndian.Uint32(data[32:36]),
		L1Size:                binary.BigEndian.Uint32(data[36:40]),
		L1TableOffset:         binary.BigEndian.Uint64(data[40:48]),
		RefcountTableOffset:   binary.BigEndian.Uint64(data[48:56]),
		RefcountTableClusters: binary.BigEndian.Uint32(data[56:60]),
		NbSnapshots:           binary.BigEndian.Uint32(data[60:64]),
		SnapshotsOffset:       binary.BigEndian.Uint64(data[64:72]),
	}

	if h.Magic != Magic {
		return nil, ErrInvalidMagic
	}

	if h.Version != Version2 && h.Version != Version3 {
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedVersion, h.Version)
	}

	if h.ClusterBits < MinClusterBits || h.ClusterBits > MaxClusterBits {
		return nil, fmt.Errorf("%w: %d", ErrInvalidClusterBits, h.ClusterBits)
	}

	// Parse version 3 fields
	if h.Version >= Version3 {
		if len(data) < HeaderSizeV3 {
			return nil, fmt.Errorf("qcow2: v3 header too short: %d bytes", len(data))
		}
		h.IncompatibleFeatures = binary.BigEndian.Uint64(data[72:80])
		h.CompatibleFeatures = binary.BigEndian.Uint64(data[80:88])
		h.AutoclearFeatures = binary.BigEndian.Uint64(data[88:96])
		h.RefcountOrder = binary.BigEndian.Uint32(data[96:100])
		h.HeaderLength = binary.BigEndian.Uint32(data[100:104])

		// Parse compression type if the feature bit is set
		if h.IncompatibleFeatures&IncompatCompression != 0 && len(data) > 104 {
			h.CompressionType = data[104]
		}
	} else {
		// Version 2 defaults
		h.RefcountOrder = 4 // 16-bit refcounts
		h.HeaderLength = HeaderSizeV2
	}

	return h, nil
}

// Validate checks if the header is valid and if we support all required features.
func (h *Header) Validate() error {
	// Check for encryption - we support legacy AES (read-only) and reject LUKS for now
	switch h.EncryptMethod {
	case EncryptionNone:
		// No encryption, all good
	case EncryptionAES:
		// Legacy AES encryption supported (read-only, requires SetPassword)
		// Note: This is insecure and deprecated, only for data recovery
	case EncryptionLUKS:
		// LUKS not yet supported
		return fmt.Errorf("%w: LUKS encryption (method=2) not yet supported", ErrEncryptedImage)
	default:
		return fmt.Errorf("%w: unknown method=%d", ErrEncryptedImage, h.EncryptMethod)
	}

	// Check for unsupported incompatible features
	// We support dirty bit, compression type in header, and external data files
	supportedIncompat := uint64(IncompatDirtyBit | IncompatCompression | IncompatExternalData)
	if h.IncompatibleFeatures & ^supportedIncompat != 0 {
		return fmt.Errorf("%w: 0x%x", ErrIncompatFeatures, h.IncompatibleFeatures)
	}

	if h.IncompatibleFeatures&IncompatCorruptBit != 0 {
		return ErrCorruptImage
	}

	return nil
}

// IsEncrypted returns true if the image uses encryption.
func (h *Header) IsEncrypted() bool {
	return h.EncryptMethod != EncryptionNone
}

// EncryptionMethod returns the encryption method used by this image.
// Returns EncryptionNone (0), EncryptionAES (1), or EncryptionLUKS (2).
func (h *Header) EncryptionMethod() uint32 {
	return h.EncryptMethod
}

// ClusterSize returns the cluster size in bytes.
func (h *Header) ClusterSize() uint64 {
	return 1 << h.ClusterBits
}

// L2Entries returns the number of L2 entries per L2 table.
func (h *Header) L2Entries() uint64 {
	// Each L2 entry is 8 bytes
	return h.ClusterSize() / 8
}

// RefcountBits returns the number of bits per refcount entry.
func (h *Header) RefcountBits() uint32 {
	return 1 << h.RefcountOrder
}

// IsDirty returns true if the image needs repair.
func (h *Header) IsDirty() bool {
	return h.IncompatibleFeatures&IncompatDirtyBit != 0
}

// HasLazyRefcounts returns true if lazy refcount updates are enabled.
func (h *Header) HasLazyRefcounts() bool {
	return h.CompatibleFeatures&CompatLazyRefcounts != 0
}

// HasExternalDataFile returns true if cluster data is stored in an external file.
func (h *Header) HasExternalDataFile() bool {
	return h.IncompatibleFeatures&IncompatExternalData != 0
}

// Encode serializes the header to bytes.
func (h *Header) Encode() []byte {
	var buf []byte
	if h.Version >= Version3 {
		buf = make([]byte, h.HeaderLength)
	} else {
		buf = make([]byte, HeaderSizeV2)
	}

	binary.BigEndian.PutUint32(buf[0:4], h.Magic)
	binary.BigEndian.PutUint32(buf[4:8], h.Version)
	binary.BigEndian.PutUint64(buf[8:16], h.BackingFileOffset)
	binary.BigEndian.PutUint32(buf[16:20], h.BackingFileSize)
	binary.BigEndian.PutUint32(buf[20:24], h.ClusterBits)
	binary.BigEndian.PutUint64(buf[24:32], h.Size)
	binary.BigEndian.PutUint32(buf[32:36], h.EncryptMethod)
	binary.BigEndian.PutUint32(buf[36:40], h.L1Size)
	binary.BigEndian.PutUint64(buf[40:48], h.L1TableOffset)
	binary.BigEndian.PutUint64(buf[48:56], h.RefcountTableOffset)
	binary.BigEndian.PutUint32(buf[56:60], h.RefcountTableClusters)
	binary.BigEndian.PutUint32(buf[60:64], h.NbSnapshots)
	binary.BigEndian.PutUint64(buf[64:72], h.SnapshotsOffset)

	if h.Version >= Version3 {
		binary.BigEndian.PutUint64(buf[72:80], h.IncompatibleFeatures)
		binary.BigEndian.PutUint64(buf[80:88], h.CompatibleFeatures)
		binary.BigEndian.PutUint64(buf[88:96], h.AutoclearFeatures)
		binary.BigEndian.PutUint32(buf[96:100], h.RefcountOrder)
		binary.BigEndian.PutUint32(buf[100:104], h.HeaderLength)
	}

	return buf
}
