package qcow2

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// Snapshot represents a QCOW2 internal snapshot.
type Snapshot struct {
	// L1 table offset for this snapshot
	L1TableOffset uint64
	// Number of L1 entries
	L1Size uint32
	// Unique ID string
	ID string
	// Human-readable name
	Name string
	// Time when snapshot was created
	Date time.Time
	// VM clock at time of snapshot (nanoseconds)
	VMClock uint64
	// Size of VM state in bytes (0 if no state saved)
	VMStateSize uint32
	// Extra data (version 3+)
	ExtraData []byte
	// Size of extra data for v3+ (parsed, not stored)
	extraDataSize uint32
}

// SnapshotHeader is the fixed-size portion of a snapshot entry.
// Variable-length fields (ID, Name) follow this header.
const snapshotHeaderSize = 40 // Fixed size without extra data, ID, or name

// parseSnapshot reads a single snapshot entry from the given reader.
func parseSnapshot(r io.ReaderAt, offset int64) (*Snapshot, int64, error) {
	// Read fixed header (40 bytes minimum)
	header := make([]byte, snapshotHeaderSize)
	if _, err := r.ReadAt(header, offset); err != nil {
		return nil, 0, fmt.Errorf("qcow2: failed to read snapshot header: %w", err)
	}

	snap := &Snapshot{
		L1TableOffset: binary.BigEndian.Uint64(header[0:8]),
		L1Size:        binary.BigEndian.Uint32(header[8:12]),
	}

	idSize := binary.BigEndian.Uint16(header[12:14])
	nameSize := binary.BigEndian.Uint16(header[14:16])
	dateSeconds := binary.BigEndian.Uint32(header[16:20])
	dateNanos := binary.BigEndian.Uint32(header[20:24])
	snap.VMClock = binary.BigEndian.Uint64(header[24:32])
	snap.VMStateSize = binary.BigEndian.Uint32(header[32:36])
	snap.extraDataSize = binary.BigEndian.Uint32(header[36:40])

	// Convert date
	snap.Date = time.Unix(int64(dateSeconds), int64(dateNanos))

	// Calculate total size and read variable portions
	pos := offset + snapshotHeaderSize

	// Read extra data if present
	if snap.extraDataSize > 0 {
		snap.ExtraData = make([]byte, snap.extraDataSize)
		if _, err := r.ReadAt(snap.ExtraData, pos); err != nil {
			return nil, 0, fmt.Errorf("qcow2: failed to read snapshot extra data: %w", err)
		}
		pos += int64(snap.extraDataSize)
	}

	// Read ID string
	if idSize > 0 {
		idBuf := make([]byte, idSize)
		if _, err := r.ReadAt(idBuf, pos); err != nil {
			return nil, 0, fmt.Errorf("qcow2: failed to read snapshot ID: %w", err)
		}
		snap.ID = string(idBuf)
		pos += int64(idSize)
	}

	// Read name string
	if nameSize > 0 {
		nameBuf := make([]byte, nameSize)
		if _, err := r.ReadAt(nameBuf, pos); err != nil {
			return nil, 0, fmt.Errorf("qcow2: failed to read snapshot name: %w", err)
		}
		snap.Name = string(nameBuf)
		pos += int64(nameSize)
	}

	// Calculate total entry size with padding to 8-byte boundary
	entrySize := snapshotHeaderSize + int64(snap.extraDataSize) + int64(idSize) + int64(nameSize)
	if entrySize%8 != 0 {
		entrySize = ((entrySize / 8) + 1) * 8
	}

	return snap, entrySize, nil
}

// loadSnapshots reads all snapshot entries from the snapshot table.
func (img *Image) loadSnapshots() error {
	if img.header.NbSnapshots == 0 || img.header.SnapshotsOffset == 0 {
		img.snapshots = nil
		return nil
	}

	img.snapshots = make([]*Snapshot, 0, img.header.NbSnapshots)
	offset := int64(img.header.SnapshotsOffset)

	for i := uint32(0); i < img.header.NbSnapshots; i++ {
		snap, size, err := parseSnapshot(img.file, offset)
		if err != nil {
			return fmt.Errorf("qcow2: failed to parse snapshot %d: %w", i, err)
		}
		img.snapshots = append(img.snapshots, snap)
		offset += size
	}

	return nil
}

// Snapshots returns the list of snapshots in the image.
// Returns nil if there are no snapshots.
func (img *Image) Snapshots() []*Snapshot {
	return img.snapshots
}

// FindSnapshot finds a snapshot by ID or name.
// Returns nil if not found.
func (img *Image) FindSnapshot(idOrName string) *Snapshot {
	for _, snap := range img.snapshots {
		if snap.ID == idOrName || snap.Name == idOrName {
			return snap
		}
	}
	return nil
}

// ReadAtSnapshot reads data from the image as it appeared at the given snapshot.
// This uses the snapshot's L1 table for address translation.
func (img *Image) ReadAtSnapshot(p []byte, off int64, snap *Snapshot) (int, error) {
	if snap == nil {
		return 0, fmt.Errorf("qcow2: nil snapshot")
	}

	// Load the snapshot's L1 table
	l1Table, err := img.loadSnapshotL1Table(snap)
	if err != nil {
		return 0, err
	}

	size := img.Size()
	if off >= size {
		return 0, io.EOF
	}

	// Clamp read to image size
	toRead := int64(len(p))
	if off+toRead > size {
		toRead = size - off
	}

	totalRead := 0
	for toRead > 0 {
		// Translate using snapshot's L1 table
		info, err := img.translateWithL1(uint64(off), l1Table)
		if err != nil {
			return totalRead, err
		}

		// Calculate how much to read from this cluster
		clusterRemaining := img.clusterSize - (uint64(off) & img.offsetMask)
		readLen := uint64(toRead)
		if readLen > clusterRemaining {
			readLen = clusterRemaining
		}

		switch info.ctype {
		case clusterUnallocated, clusterZero:
			// Fill with zeros
			for i := uint64(0); i < readLen; i++ {
				p[totalRead+int(i)] = 0
			}

		case clusterCompressed:
			// Read compressed cluster
			decompressed, err := img.decompressCluster(info.l2Entry)
			if err != nil {
				return totalRead, err
			}
			clusterOff := uint64(off) & img.offsetMask
			copy(p[totalRead:], decompressed[clusterOff:clusterOff+readLen])

		case clusterNormal:
			// Read from physical offset
			n, err := img.file.ReadAt(p[totalRead:totalRead+int(readLen)], int64(info.physOff))
			if err != nil && err != io.EOF {
				return totalRead, err
			}
			if n < int(readLen) {
				return totalRead + n, io.ErrUnexpectedEOF
			}
		}

		totalRead += int(readLen)
		off += int64(readLen)
		toRead -= int64(readLen)
	}

	return totalRead, nil
}

// loadSnapshotL1Table loads the L1 table for a snapshot.
func (img *Image) loadSnapshotL1Table(snap *Snapshot) ([]byte, error) {
	l1Size := uint64(snap.L1Size) * 8
	l1Table := make([]byte, l1Size)
	if _, err := img.file.ReadAt(l1Table, int64(snap.L1TableOffset)); err != nil {
		return nil, fmt.Errorf("qcow2: failed to read snapshot L1 table: %w", err)
	}
	return l1Table, nil
}

// translateWithL1 translates a virtual offset using a specific L1 table.
func (img *Image) translateWithL1(virtOff uint64, l1Table []byte) (clusterInfo, error) {
	// Calculate indices
	l2Index := (virtOff >> img.clusterBits) & (img.l2Entries - 1)
	l1Index := virtOff >> (img.clusterBits + img.l2Bits)

	// Check L1 bounds
	if l1Index*8 >= uint64(len(l1Table)) {
		return clusterInfo{ctype: clusterUnallocated}, nil
	}

	// Read L1 entry
	l1Entry := binary.BigEndian.Uint64(l1Table[l1Index*8:])

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

	// Check if compressed
	if l2Entry&L2EntryCompressed != 0 {
		return clusterInfo{
			ctype:   clusterCompressed,
			l2Entry: l2Entry,
		}, nil
	}

	// Check for zero cluster
	if l2Entry&L2EntryZeroFlag != 0 {
		return clusterInfo{ctype: clusterZero}, nil
	}

	// Extract physical offset
	physOff := l2Entry & L2EntryOffsetMask
	if physOff == 0 {
		return clusterInfo{ctype: clusterUnallocated}, nil
	}

	return clusterInfo{
		ctype:   clusterNormal,
		physOff: physOff + (virtOff & img.offsetMask),
	}, nil
}
