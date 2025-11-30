package qcow2

import (
	"encoding/binary"
	"fmt"
)

// Header extension types
const (
	ExtensionEndOfHeader      = 0x00000000
	ExtensionBackingFormat    = 0xE2792ACA
	ExtensionFeatureNameTable = 0x6803f857
	ExtensionBitmaps          = 0x23852875
	ExtensionFullDiskEncrypt  = 0x0537be77
	ExtensionExternalDataFile = 0x44415441 // "DATA"
)

// HeaderExtension represents a single header extension.
type HeaderExtension struct {
	Type   uint32
	Length uint32
	Data   []byte
}

// EncryptionHeaderPointer contains the location of the LUKS header in the image.
type EncryptionHeaderPointer struct {
	Offset uint64 // Offset to encryption header (cluster-aligned)
	Length uint64 // Length of the encryption header in bytes
}

// HeaderExtensions holds all parsed header extensions.
type HeaderExtensions struct {
	BackingFormat    string                   // Backing file format (e.g., "qcow2", "raw")
	FeatureNames     map[string]string        // Feature name table
	ExternalDataFile string                   // External data file name
	EncryptionHeader *EncryptionHeaderPointer // LUKS encryption header location (if present)
	Unknown          []HeaderExtension        // Unknown but compatible extensions
}

// parseHeaderExtensions reads all header extensions from the image file.
// Extensions start at:
// - V2: Byte 72 (after the fixed header)
// - V3: header.HeaderLength
// And end at either:
// - The backing file offset (if present)
// - The end of cluster 0
func (img *Image) parseHeaderExtensions() (*HeaderExtensions, error) {
	var startOffset uint64
	if img.header.Version >= Version3 {
		startOffset = uint64(img.header.HeaderLength)
	} else {
		// V2 extensions start right after the 72-byte header
		startOffset = HeaderSizeV2
	}
	endOffset := img.clusterSize // End of header cluster

	// If backing file is in cluster 0, stop there
	if img.header.BackingFileOffset > 0 && img.header.BackingFileOffset < endOffset {
		endOffset = img.header.BackingFileOffset
	}

	// Read extension area
	extSize := endOffset - startOffset
	if extSize == 0 || extSize > img.clusterSize {
		return &HeaderExtensions{}, nil
	}

	extData := make([]byte, extSize)
	if _, err := img.file.ReadAt(extData, int64(startOffset)); err != nil {
		return nil, fmt.Errorf("qcow2: failed to read header extensions: %w", err)
	}

	extensions := &HeaderExtensions{
		FeatureNames: make(map[string]string),
	}

	offset := uint64(0)
	for offset+8 <= uint64(len(extData)) {
		extType := binary.BigEndian.Uint32(extData[offset:])
		extLen := binary.BigEndian.Uint32(extData[offset+4:])

		// End marker
		if extType == ExtensionEndOfHeader {
			break
		}

		// Check bounds
		dataEnd := offset + 8 + uint64(extLen)
		if dataEnd > uint64(len(extData)) {
			return nil, fmt.Errorf("qcow2: header extension exceeds bounds")
		}

		data := extData[offset+8 : dataEnd]

		switch extType {
		case ExtensionBackingFormat:
			extensions.BackingFormat = string(data)

		case ExtensionFeatureNameTable:
			parseFeatureNameTable(data, extensions.FeatureNames)

		case ExtensionExternalDataFile:
			extensions.ExternalDataFile = string(data)

		case ExtensionFullDiskEncrypt:
			// Full disk encryption header pointer (16 bytes)
			// Bytes 0-7: Offset to encryption header
			// Bytes 8-15: Length of encryption header
			if len(data) >= 16 {
				extensions.EncryptionHeader = &EncryptionHeaderPointer{
					Offset: binary.BigEndian.Uint64(data[0:8]),
					Length: binary.BigEndian.Uint64(data[8:16]),
				}
			}

		default:
			// Store unknown extensions
			ext := HeaderExtension{
				Type:   extType,
				Length: extLen,
				Data:   make([]byte, len(data)),
			}
			copy(ext.Data, data)
			extensions.Unknown = append(extensions.Unknown, ext)
		}

		// Advance to next extension (8-byte aligned)
		paddedLen := (extLen + 7) & ^uint32(7)
		offset += 8 + uint64(paddedLen)
	}

	return extensions, nil
}

// parseFeatureNameTable parses the feature name table extension.
// Format: repeated entries of:
//   - 1 byte: feature type (0=incompatible, 1=compatible, 2=autoclear)
//   - 1 byte: bit number
//   - 46 bytes: null-terminated name
func parseFeatureNameTable(data []byte, names map[string]string) {
	const entrySize = 48
	for i := 0; i+entrySize <= len(data); i += entrySize {
		featureType := data[i]
		bitNumber := data[i+1]
		nameBytes := data[i+2 : i+48]

		// Find null terminator
		name := ""
		for j, b := range nameBytes {
			if b == 0 {
				name = string(nameBytes[:j])
				break
			}
		}
		if name == "" && nameBytes[0] != 0 {
			name = string(nameBytes)
		}

		// Create key like "incompat_0" or "compat_1"
		var typeStr string
		switch featureType {
		case 0:
			typeStr = "incompat"
		case 1:
			typeStr = "compat"
		case 2:
			typeStr = "autoclear"
		default:
			continue
		}

		key := fmt.Sprintf("%s_%d", typeStr, bitNumber)
		names[key] = name
	}
}

// Extensions returns the parsed header extensions.
// Returns nil if extensions haven't been parsed yet.
func (img *Image) Extensions() *HeaderExtensions {
	return img.extensions
}

// BackingFormat returns the format of the backing file (e.g., "qcow2", "raw").
// Returns empty string if not specified.
func (img *Image) BackingFormat() string {
	if img.extensions != nil {
		return img.extensions.BackingFormat
	}
	return ""
}
