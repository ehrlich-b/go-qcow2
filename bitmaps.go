package qcow2

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
)

// Bitmap extension constants
const (
	// BitmapTypeTracking is the dirty tracking bitmap type.
	BitmapTypeTracking = 1
)

// Bitmap directory entry flags
const (
	// BitmapFlagInUse indicates the bitmap was not saved correctly
	// and may be inconsistent. Set when bitmap is modified in memory.
	BitmapFlagInUse = 1 << 0

	// BitmapFlagAuto indicates the bitmap must reflect all changes
	// to the virtual disk by having its in-use flag set when the
	// image is opened for writing.
	BitmapFlagAuto = 1 << 1

	// BitmapFlagExtraDataCompatible controls whether a bitmap with
	// unknown extra_data can be used (set) or must be ignored (unset).
	BitmapFlagExtraDataCompatible = 1 << 2
)

// Bitmap table entry constants
const (
	// BMETableEntryOffsetMask extracts the cluster offset (bits 9-55).
	BMETableEntryOffsetMask = uint64(0x00FFFFFFFFFFFE00)

	// BMETableEntryReservedMask checks for reserved bits that must be zero.
	BMETableEntryReservedMask = uint64(0xFF000000000001FE)

	// BMETableEntryFlagAllOnes indicates all bits in this range are set (1)
	// when the offset is zero (unallocated). If this flag is 0 and offset
	// is 0, all bits are zero.
	BMETableEntryFlagAllOnes = uint64(1) << 0
)

// Errors for bitmap operations
var (
	ErrBitmapNotFound        = errors.New("qcow2: bitmap not found")
	ErrBitmapInconsistent    = errors.New("qcow2: bitmap is inconsistent (in_use flag set)")
	ErrBitmapTypeUnsupported = errors.New("qcow2: unsupported bitmap type")
	ErrBitmapCorrupt         = errors.New("qcow2: bitmap data is corrupt")
)

// BitmapInfo contains metadata about a single bitmap.
type BitmapInfo struct {
	// Name is the bitmap identifier (unique within the image).
	Name string

	// Type is the bitmap type (currently only BitmapTypeTracking=1).
	Type uint8

	// Granularity is the number of bytes each bit represents.
	// Calculated as 1 << GranularityBits.
	Granularity uint64

	// GranularityBits is the log2 of the granularity.
	GranularityBits uint8

	// Flags contains the bitmap flags (InUse, Auto, ExtraDataCompatible).
	Flags uint32

	// TableOffset is the cluster-aligned offset to the bitmap table.
	TableOffset uint64

	// TableSize is the number of entries in the bitmap table.
	TableSize uint32

	// IsEnabled returns true if this bitmap is actively tracking changes.
	IsEnabled bool

	// IsConsistent returns true if the bitmap data is reliable.
	IsConsistent bool
}

// bitmapExtension holds parsed bitmap extension header data.
type bitmapExtension struct {
	nbBitmaps       uint32
	directorySize   uint64
	directoryOffset uint64
}

// Bitmap provides read access to a QCOW2 dirty tracking bitmap.
type Bitmap struct {
	img   *Image
	info  BitmapInfo
	table []uint64 // Bitmap table entries
	mu    sync.RWMutex
}

// parseBitmapExtension parses the bitmaps extension header data.
func parseBitmapExtension(data []byte) (*bitmapExtension, error) {
	if len(data) < 24 {
		return nil, fmt.Errorf("qcow2: bitmap extension too short: %d bytes", len(data))
	}

	ext := &bitmapExtension{
		nbBitmaps:       binary.BigEndian.Uint32(data[0:4]),
		directorySize:   binary.BigEndian.Uint64(data[8:16]),
		directoryOffset: binary.BigEndian.Uint64(data[16:24]),
	}

	// Validate reserved field
	reserved := binary.BigEndian.Uint32(data[4:8])
	if reserved != 0 {
		return nil, fmt.Errorf("qcow2: bitmap extension reserved field not zero: %d", reserved)
	}

	if ext.nbBitmaps == 0 {
		return nil, fmt.Errorf("qcow2: bitmap extension has zero bitmaps")
	}

	return ext, nil
}

// parseBitmapDirectoryEntry parses a single bitmap directory entry.
// Returns the entry info and the number of bytes consumed (including padding).
func parseBitmapDirectoryEntry(data []byte) (*BitmapInfo, int, error) {
	if len(data) < 24 {
		return nil, 0, fmt.Errorf("qcow2: bitmap directory entry too short: %d bytes", len(data))
	}

	info := &BitmapInfo{
		TableOffset:     binary.BigEndian.Uint64(data[0:8]),
		TableSize:       binary.BigEndian.Uint32(data[8:12]),
		Flags:           binary.BigEndian.Uint32(data[12:16]),
		Type:            data[16],
		GranularityBits: data[17],
	}
	nameSize := binary.BigEndian.Uint16(data[18:20])
	extraDataSize := binary.BigEndian.Uint32(data[20:24])

	// Calculate total entry size
	fixedSize := 24
	entrySize := fixedSize + int(extraDataSize) + int(nameSize)
	// Align to 8 bytes
	paddedSize := (entrySize + 7) & ^7

	if len(data) < paddedSize {
		return nil, 0, fmt.Errorf("qcow2: bitmap directory entry data too short for name")
	}

	// Extract name (after extra_data)
	nameStart := fixedSize + int(extraDataSize)
	info.Name = string(data[nameStart : nameStart+int(nameSize)])

	// Calculate granularity
	info.Granularity = uint64(1) << info.GranularityBits

	// Determine status flags
	info.IsEnabled = info.Flags&BitmapFlagAuto != 0
	info.IsConsistent = info.Flags&BitmapFlagInUse == 0

	return info, paddedSize, nil
}

// Bitmaps returns information about all bitmaps in the image.
// Returns nil if the image has no bitmaps extension.
func (img *Image) Bitmaps() ([]BitmapInfo, error) {
	// Check if bitmaps extension is present
	if img.bitmapExt == nil {
		return nil, nil
	}

	// Check autoclear bit for consistency
	if img.header.AutoclearFeatures&AutoclearBitmaps == 0 {
		// Bitmap data may be inconsistent - mark all as inconsistent
	}

	// Read bitmap directory
	dirData := make([]byte, img.bitmapExt.directorySize)
	if _, err := img.file.ReadAt(dirData, int64(img.bitmapExt.directoryOffset)); err != nil {
		return nil, fmt.Errorf("qcow2: failed to read bitmap directory: %w", err)
	}

	autoclearSet := img.header.AutoclearFeatures&AutoclearBitmaps != 0

	// Parse directory entries
	var bitmaps []BitmapInfo
	offset := 0
	for i := uint32(0); i < img.bitmapExt.nbBitmaps && offset < len(dirData); i++ {
		info, consumed, err := parseBitmapDirectoryEntry(dirData[offset:])
		if err != nil {
			return nil, fmt.Errorf("qcow2: failed to parse bitmap %d: %w", i, err)
		}

		// If autoclear bit is not set, all bitmaps are inconsistent
		if !autoclearSet {
			info.IsConsistent = false
		}

		bitmaps = append(bitmaps, *info)
		offset += consumed
	}

	return bitmaps, nil
}

// FindBitmap returns information about a bitmap by name.
// Returns ErrBitmapNotFound if no bitmap with that name exists.
func (img *Image) FindBitmap(name string) (*BitmapInfo, error) {
	bitmaps, err := img.Bitmaps()
	if err != nil {
		return nil, err
	}

	for _, b := range bitmaps {
		if b.Name == name {
			return &b, nil
		}
	}

	return nil, ErrBitmapNotFound
}

// OpenBitmap opens a bitmap for reading.
// Returns ErrBitmapNotFound if the bitmap doesn't exist.
// Returns ErrBitmapInconsistent if the bitmap has the in_use flag set
// and the image was not cleanly closed.
func (img *Image) OpenBitmap(name string) (*Bitmap, error) {
	info, err := img.FindBitmap(name)
	if err != nil {
		return nil, err
	}

	// Check consistency
	if !info.IsConsistent {
		return nil, fmt.Errorf("%w: bitmap '%s'", ErrBitmapInconsistent, name)
	}

	// Only support dirty tracking bitmaps for now
	if info.Type != BitmapTypeTracking {
		return nil, fmt.Errorf("%w: type=%d", ErrBitmapTypeUnsupported, info.Type)
	}

	// Load bitmap table
	tableBytes := int(info.TableSize) * 8
	tableData := make([]byte, tableBytes)
	if _, err := img.file.ReadAt(tableData, int64(info.TableOffset)); err != nil {
		return nil, fmt.Errorf("qcow2: failed to read bitmap table: %w", err)
	}

	table := make([]uint64, info.TableSize)
	for i := range table {
		entry := binary.BigEndian.Uint64(tableData[i*8:])
		// Validate reserved bits
		if entry&BMETableEntryReservedMask != 0 {
			return nil, fmt.Errorf("%w: invalid table entry %d: 0x%x", ErrBitmapCorrupt, i, entry)
		}
		table[i] = entry
	}

	return &Bitmap{
		img:   img,
		info:  *info,
		table: table,
	}, nil
}

// Info returns the bitmap metadata.
func (b *Bitmap) Info() BitmapInfo {
	return b.info
}

// Name returns the bitmap name.
func (b *Bitmap) Name() string {
	return b.info.Name
}

// Granularity returns the number of bytes each bit represents.
func (b *Bitmap) Granularity() uint64 {
	return b.info.Granularity
}

// IsSet returns true if the bit corresponding to the given virtual offset is set.
// This indicates the region was written to while the bitmap was enabled.
func (b *Bitmap) IsSet(virtualOffset uint64) (bool, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Calculate which bit we need
	bitIndex := virtualOffset / b.info.Granularity
	byteIndex := bitIndex / 8
	bitInByte := bitIndex % 8

	// Calculate which cluster contains this byte
	clusterIndex := byteIndex / b.img.clusterSize
	offsetInCluster := byteIndex % b.img.clusterSize

	// Check bounds
	if clusterIndex >= uint64(len(b.table)) {
		// Beyond bitmap extent - treat as not set
		return false, nil
	}

	entry := b.table[clusterIndex]
	clusterOffset := entry & BMETableEntryOffsetMask

	// Handle unallocated entries
	if clusterOffset == 0 {
		// Check the all-ones flag
		return entry&BMETableEntryFlagAllOnes != 0, nil
	}

	// Read the byte from the bitmap data cluster
	buf := make([]byte, 1)
	readOffset := int64(clusterOffset + offsetInCluster)
	if _, err := b.img.file.ReadAt(buf, readOffset); err != nil {
		return false, fmt.Errorf("qcow2: failed to read bitmap data: %w", err)
	}

	return (buf[0] & (1 << bitInByte)) != 0, nil
}

// GetDirtyRanges returns all dirty ranges (regions where the bitmap bit is set).
// This is useful for incremental backup - only these ranges need to be copied.
// Returns ranges as (start, length) pairs where start is cluster-aligned to granularity.
func (b *Bitmap) GetDirtyRanges() ([][2]uint64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var ranges [][2]uint64
	var currentStart uint64
	inRange := false

	// Calculate total bits in bitmap
	virtualSize := b.img.header.Size
	totalBits := (virtualSize + b.info.Granularity - 1) / b.info.Granularity

	// Iterate through bitmap table entries
	for clusterIdx := uint64(0); clusterIdx < uint64(len(b.table)); clusterIdx++ {
		entry := b.table[clusterIdx]
		clusterOffset := entry & BMETableEntryOffsetMask

		// Calculate bits in this cluster
		bitsPerCluster := b.img.clusterSize * 8
		startBit := clusterIdx * bitsPerCluster
		endBit := startBit + bitsPerCluster
		if endBit > totalBits {
			endBit = totalBits
		}

		if clusterOffset == 0 {
			// Unallocated entry - check all-ones flag
			allSet := entry&BMETableEntryFlagAllOnes != 0

			for bit := startBit; bit < endBit; bit++ {
				bitSet := allSet
				virtualOff := bit * b.info.Granularity

				if bitSet && !inRange {
					currentStart = virtualOff
					inRange = true
				} else if !bitSet && inRange {
					ranges = append(ranges, [2]uint64{currentStart, virtualOff - currentStart})
					inRange = false
				}
			}
		} else {
			// Read allocated cluster
			clusterData := make([]byte, b.img.clusterSize)
			if _, err := b.img.file.ReadAt(clusterData, int64(clusterOffset)); err != nil {
				return nil, fmt.Errorf("qcow2: failed to read bitmap cluster: %w", err)
			}

			for bit := startBit; bit < endBit; bit++ {
				byteIdx := (bit - startBit) / 8
				bitIdx := (bit - startBit) % 8
				bitSet := (clusterData[byteIdx] & (1 << bitIdx)) != 0
				virtualOff := bit * b.info.Granularity

				if bitSet && !inRange {
					currentStart = virtualOff
					inRange = true
				} else if !bitSet && inRange {
					ranges = append(ranges, [2]uint64{currentStart, virtualOff - currentStart})
					inRange = false
				}
			}
		}
	}

	// Close any open range
	if inRange {
		ranges = append(ranges, [2]uint64{currentStart, virtualSize - currentStart})
	}

	return ranges, nil
}

// CountDirtyBytes returns the total number of dirty bytes in the bitmap.
func (b *Bitmap) CountDirtyBytes() (uint64, error) {
	ranges, err := b.GetDirtyRanges()
	if err != nil {
		return 0, err
	}

	var total uint64
	for _, r := range ranges {
		total += r[1]
	}
	return total, nil
}

// CountDirtyBits returns the number of set bits in the bitmap.
func (b *Bitmap) CountDirtyBits() (uint64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var count uint64
	virtualSize := b.img.header.Size
	totalBits := (virtualSize + b.info.Granularity - 1) / b.info.Granularity

	for clusterIdx := uint64(0); clusterIdx < uint64(len(b.table)); clusterIdx++ {
		entry := b.table[clusterIdx]
		clusterOffset := entry & BMETableEntryOffsetMask

		bitsPerCluster := b.img.clusterSize * 8
		startBit := clusterIdx * bitsPerCluster
		endBit := startBit + bitsPerCluster
		if endBit > totalBits {
			endBit = totalBits
		}
		bitsInCluster := endBit - startBit

		if clusterOffset == 0 {
			// Unallocated
			if entry&BMETableEntryFlagAllOnes != 0 {
				count += bitsInCluster
			}
		} else {
			// Read and count bits
			clusterData := make([]byte, b.img.clusterSize)
			if _, err := b.img.file.ReadAt(clusterData, int64(clusterOffset)); err != nil {
				return 0, fmt.Errorf("qcow2: failed to read bitmap cluster: %w", err)
			}

			bytesToCount := (bitsInCluster + 7) / 8
			for i := uint64(0); i < bytesToCount; i++ {
				count += uint64(popcount8(clusterData[i]))
			}

			// Adjust for unused bits in last byte
			if bitsInCluster%8 != 0 {
				lastByte := clusterData[bytesToCount-1]
				unusedBits := 8 - (bitsInCluster % 8)
				mask := byte(0xFF) << (8 - unusedBits)
				count -= uint64(popcount8(lastByte & mask))
			}
		}
	}

	return count, nil
}

// invalidateBitmaps marks all bitmaps as in-use (inconsistent) when the image
// is modified. This ensures consumers know the bitmaps are stale after writes.
// Called on first write to an image with bitmaps.
func (img *Image) invalidateBitmaps() error {
	if img.bitmapExt == nil {
		return nil // No bitmaps
	}

	// Read bitmap directory
	dirData := make([]byte, img.bitmapExt.directorySize)
	if _, err := img.file.ReadAt(dirData, int64(img.bitmapExt.directoryOffset)); err != nil {
		return fmt.Errorf("qcow2: failed to read bitmap directory: %w", err)
	}

	// Parse and update each directory entry's flags
	modified := false
	offset := 0
	for i := uint32(0); i < img.bitmapExt.nbBitmaps && offset < len(dirData); i++ {
		// Parse entry to get size
		if len(dirData[offset:]) < 24 {
			break
		}

		flags := binary.BigEndian.Uint32(dirData[offset+12 : offset+16])
		nameSize := binary.BigEndian.Uint16(dirData[offset+18 : offset+20])
		extraDataSize := binary.BigEndian.Uint32(dirData[offset+20 : offset+24])

		// Calculate entry size
		entrySize := 24 + int(extraDataSize) + int(nameSize)
		paddedSize := (entrySize + 7) & ^7

		// Set the in-use flag if not already set
		if flags&BitmapFlagInUse == 0 {
			flags |= BitmapFlagInUse
			binary.BigEndian.PutUint32(dirData[offset+12:offset+16], flags)
			modified = true
		}

		offset += paddedSize
	}

	// Write back if modified
	if modified {
		if _, err := img.file.WriteAt(dirData, int64(img.bitmapExt.directoryOffset)); err != nil {
			return fmt.Errorf("qcow2: failed to update bitmap directory: %w", err)
		}
	}

	return nil
}

// hasBitmaps returns true if the image has any bitmaps.
func (img *Image) hasBitmaps() bool {
	return img.bitmapExt != nil && img.bitmapExt.nbBitmaps > 0
}

// popcount8 returns the number of set bits in a byte.
func popcount8(b byte) int {
	// Use lookup table for speed
	var popTable = [256]int{
		0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
		1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
		1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
		2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
		1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
		2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
		2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
		3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
		1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
		2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
		2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
		3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
		2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
		3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
		3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
		4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
	}
	return popTable[b]
}
