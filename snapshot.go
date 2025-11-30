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
			// Read from physical offset (use dataFile for external data file support)
			n, err := img.dataFile().ReadAt(p[totalRead:totalRead+int(readLen)], int64(info.physOff))
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

// serializeSnapshot encodes a snapshot to bytes for disk storage.
// Returns the serialized bytes including padding to 8-byte boundary.
func serializeSnapshot(snap *Snapshot) []byte {
	idBytes := []byte(snap.ID)
	nameBytes := []byte(snap.Name)
	extraDataSize := uint32(len(snap.ExtraData))

	// Calculate total size with 8-byte alignment
	size := snapshotHeaderSize + int(extraDataSize) + len(idBytes) + len(nameBytes)
	if size%8 != 0 {
		size = ((size / 8) + 1) * 8
	}

	buf := make([]byte, size)

	// Write fixed header
	binary.BigEndian.PutUint64(buf[0:8], snap.L1TableOffset)
	binary.BigEndian.PutUint32(buf[8:12], snap.L1Size)
	binary.BigEndian.PutUint16(buf[12:14], uint16(len(idBytes)))
	binary.BigEndian.PutUint16(buf[14:16], uint16(len(nameBytes)))
	binary.BigEndian.PutUint32(buf[16:20], uint32(snap.Date.Unix()))
	binary.BigEndian.PutUint32(buf[20:24], uint32(snap.Date.Nanosecond()))
	binary.BigEndian.PutUint64(buf[24:32], snap.VMClock)
	binary.BigEndian.PutUint32(buf[32:36], snap.VMStateSize)
	binary.BigEndian.PutUint32(buf[36:40], extraDataSize)

	// Write variable-length fields
	pos := snapshotHeaderSize
	if extraDataSize > 0 {
		copy(buf[pos:], snap.ExtraData)
		pos += int(extraDataSize)
	}
	copy(buf[pos:], idBytes)
	pos += len(idBytes)
	copy(buf[pos:], nameBytes)
	// Remaining bytes are already zero (padding)

	return buf
}

// CreateSnapshot creates a new internal snapshot with the given name.
// The snapshot captures the current state of the image by copying the L1 table
// and incrementing refcounts for all referenced clusters.
func (img *Image) CreateSnapshot(name string) (*Snapshot, error) {
	if img.readOnly {
		return nil, fmt.Errorf("qcow2: cannot create snapshot on read-only image")
	}

	if name == "" {
		return nil, fmt.Errorf("qcow2: snapshot name cannot be empty")
	}

	// Check for duplicate name
	if img.FindSnapshot(name) != nil {
		return nil, fmt.Errorf("qcow2: snapshot with name %q already exists", name)
	}

	// Generate unique ID (QEMU uses sequential numbers as strings)
	id := fmt.Sprintf("%d", len(img.snapshots)+1)
	// Ensure ID is unique
	for img.FindSnapshot(id) != nil {
		id = fmt.Sprintf("%d", len(img.snapshots)+100)
	}

	// Copy L1 table to new cluster(s)
	img.l1Mu.Lock()
	l1TableSize := uint64(img.header.L1Size) * 8
	l1TableCopy := make([]byte, l1TableSize)
	copy(l1TableCopy, img.l1Table)

	// Clear COPIED flags in the snapshot's L1 table copy (shared clusters)
	// and in the current image's L1 table (so writes trigger COW)
	for i := uint64(0); i < uint64(img.header.L1Size); i++ {
		entry := binary.BigEndian.Uint64(l1TableCopy[i*8:])
		if entry != 0 && entry&L1EntryOffsetMask != 0 {
			// Clear COPIED flag in snapshot's copy
			entry &^= L1EntryCopied
			binary.BigEndian.PutUint64(l1TableCopy[i*8:], entry)

			// Clear COPIED flag in current image's L1 table
			currentEntry := binary.BigEndian.Uint64(img.l1Table[i*8:])
			currentEntry &^= L1EntryCopied
			binary.BigEndian.PutUint64(img.l1Table[i*8:], currentEntry)
		}
	}

	// Write updated current L1 table to disk (with COPIED flags cleared)
	if _, err := img.file.WriteAt(img.l1Table, int64(img.header.L1TableOffset)); err != nil {
		img.l1Mu.Unlock()
		return nil, fmt.Errorf("qcow2: failed to update L1 table: %w", err)
	}
	img.l1Mu.Unlock()

	// Allocate clusters for the new L1 table
	l1Clusters := (l1TableSize + img.clusterSize - 1) / img.clusterSize
	newL1Offset, err := img.allocateCluster()
	if err != nil {
		return nil, fmt.Errorf("qcow2: failed to allocate cluster for snapshot L1 table: %w", err)
	}

	// If L1 table spans multiple clusters, allocate the rest
	for i := uint64(1); i < l1Clusters; i++ {
		nextCluster, err := img.allocateCluster()
		if err != nil {
			return nil, fmt.Errorf("qcow2: failed to allocate cluster for snapshot L1 table: %w", err)
		}
		// Verify clusters are contiguous (simplification - QEMU does this too)
		if nextCluster != newL1Offset+i*img.clusterSize {
			return nil, fmt.Errorf("qcow2: non-contiguous cluster allocation for L1 table")
		}
	}

	// Write the L1 table copy to disk (with COPIED flags cleared)
	// Pad to cluster boundary
	paddedL1 := make([]byte, l1Clusters*img.clusterSize)
	copy(paddedL1, l1TableCopy)
	if _, err := img.file.WriteAt(paddedL1, int64(newL1Offset)); err != nil {
		return nil, fmt.Errorf("qcow2: failed to write snapshot L1 table: %w", err)
	}

	// Increment refcounts for all L2 tables and data clusters referenced by this snapshot
	// Also clear COPIED flags in L2 tables
	if err := img.incrementSnapshotRefcounts(l1TableCopy); err != nil {
		return nil, fmt.Errorf("qcow2: failed to update refcounts for snapshot: %w", err)
	}

	// Build V3 extra data: vm_state_size_large (8 bytes) + disk_size (8 bytes)
	var extraData []byte
	if img.header.Version >= Version3 {
		extraData = make([]byte, 16)
		binary.BigEndian.PutUint64(extraData[0:8], 0)                // vm_state_size_large
		binary.BigEndian.PutUint64(extraData[8:16], img.header.Size) // disk_size
	}

	// Create snapshot entry
	snap := &Snapshot{
		L1TableOffset: newL1Offset,
		L1Size:        img.header.L1Size,
		ID:            id,
		Name:          name,
		Date:          time.Now(),
		VMClock:       0, // No VM state
		VMStateSize:   0,
		ExtraData:     extraData,
	}

	// Write new snapshot table
	if err := img.writeSnapshotTable(snap); err != nil {
		return nil, fmt.Errorf("qcow2: failed to write snapshot table: %w", err)
	}

	// Add to in-memory list
	img.snapshots = append(img.snapshots, snap)

	return snap, nil
}

// incrementSnapshotRefcounts increments refcounts for all L2 tables and data clusters
// referenced by the given L1 table. This is called when creating a snapshot to ensure
// clusters are not freed while still referenced by the snapshot.
// It also clears COPIED flags in L2 tables since the clusters are now shared.
func (img *Image) incrementSnapshotRefcounts(l1Table []byte) error {
	l1Entries := uint64(len(l1Table)) / 8

	for i := uint64(0); i < l1Entries; i++ {
		l1Entry := binary.BigEndian.Uint64(l1Table[i*8:])
		if l1Entry == 0 {
			continue
		}

		l2Offset := l1Entry & L1EntryOffsetMask
		if l2Offset == 0 {
			continue
		}

		// Increment refcount for L2 table
		if err := img.incrementRefcount(l2Offset); err != nil {
			return fmt.Errorf("failed to increment L2 table refcount at 0x%x: %w", l2Offset, err)
		}

		// Read L2 table and process entries
		l2Table, err := img.getL2Table(l2Offset)
		if err != nil {
			return fmt.Errorf("failed to read L2 table at 0x%x: %w", l2Offset, err)
		}

		// Make a copy of L2 table to modify
		l2Modified := make([]byte, len(l2Table))
		copy(l2Modified, l2Table)
		needsWrite := false

		for j := uint64(0); j < img.l2Entries; j++ {
			l2Entry := binary.BigEndian.Uint64(l2Modified[j*8:])
			if l2Entry == 0 {
				continue
			}

			// Skip compressed clusters - they have special offset format
			if l2Entry&L2EntryCompressed != 0 {
				continue
			}

			// Skip zero clusters without allocation
			if l2Entry&L2EntryZeroFlag != 0 && l2Entry&L2EntryOffsetMask == 0 {
				continue
			}

			dataOffset := l2Entry & L2EntryOffsetMask
			if dataOffset != 0 {
				if err := img.incrementRefcount(dataOffset); err != nil {
					return fmt.Errorf("failed to increment data cluster refcount at 0x%x: %w", dataOffset, err)
				}

				// Clear COPIED flag since this cluster is now shared
				if l2Entry&L2EntryCopied != 0 {
					l2Entry &^= L2EntryCopied
					binary.BigEndian.PutUint64(l2Modified[j*8:], l2Entry)
					needsWrite = true
				}
			}
		}

		// Write modified L2 table to disk if COPIED flags were cleared
		if needsWrite {
			if _, err := img.file.WriteAt(l2Modified, int64(l2Offset)); err != nil {
				return fmt.Errorf("failed to write L2 table at 0x%x: %w", l2Offset, err)
			}
			// Update L2 cache
			img.l2Cache.put(l2Offset, l2Modified)
		}
	}

	return nil
}

// writeSnapshotTable writes the complete snapshot table with the new snapshot appended.
// This allocates new cluster(s) for the snapshot table and updates the header.
func (img *Image) writeSnapshotTable(newSnap *Snapshot) error {
	// Serialize all existing snapshots plus the new one
	var tableData []byte
	for _, snap := range img.snapshots {
		tableData = append(tableData, serializeSnapshot(snap)...)
	}
	tableData = append(tableData, serializeSnapshot(newSnap)...)

	// Calculate clusters needed
	tableClusters := (uint64(len(tableData)) + img.clusterSize - 1) / img.clusterSize

	// Allocate clusters for the new snapshot table
	newTableOffset, err := img.allocateCluster()
	if err != nil {
		return fmt.Errorf("failed to allocate cluster for snapshot table: %w", err)
	}

	// Allocate remaining clusters if needed
	for i := uint64(1); i < tableClusters; i++ {
		nextCluster, err := img.allocateCluster()
		if err != nil {
			return fmt.Errorf("failed to allocate cluster for snapshot table: %w", err)
		}
		if nextCluster != newTableOffset+i*img.clusterSize {
			return fmt.Errorf("non-contiguous cluster allocation for snapshot table")
		}
	}

	// Pad table data to cluster boundary
	paddedTable := make([]byte, tableClusters*img.clusterSize)
	copy(paddedTable, tableData)

	// Write table to disk
	if _, err := img.file.WriteAt(paddedTable, int64(newTableOffset)); err != nil {
		return fmt.Errorf("failed to write snapshot table: %w", err)
	}

	// Decrement refcounts for old snapshot table clusters if present
	if img.header.SnapshotsOffset != 0 && img.header.NbSnapshots > 0 {
		// Calculate old table size
		oldTableSize := uint64(0)
		for _, snap := range img.snapshots {
			oldTableSize += uint64(len(serializeSnapshot(snap)))
		}
		oldClusters := (oldTableSize + img.clusterSize - 1) / img.clusterSize
		for i := uint64(0); i < oldClusters; i++ {
			if err := img.decrementRefcount(img.header.SnapshotsOffset + i*img.clusterSize); err != nil {
				// Log but don't fail - old table may not have proper refcounts
			}
		}
	}

	// Update header
	img.header.SnapshotsOffset = newTableOffset
	img.header.NbSnapshots = uint32(len(img.snapshots) + 1)

	if err := img.writeHeader(); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	if err := img.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync: %w", err)
	}

	return nil
}

// DeleteSnapshot deletes a snapshot by ID or name.
// This decrements refcounts for all clusters referenced by the snapshot,
// removes the snapshot from the table, and updates the header.
func (img *Image) DeleteSnapshot(idOrName string) error {
	if img.readOnly {
		return fmt.Errorf("qcow2: cannot delete snapshot on read-only image")
	}

	if idOrName == "" {
		return fmt.Errorf("qcow2: snapshot ID or name cannot be empty")
	}

	// Find the snapshot index
	snapIndex := -1
	for i, snap := range img.snapshots {
		if snap.ID == idOrName || snap.Name == idOrName {
			snapIndex = i
			break
		}
	}
	if snapIndex == -1 {
		return fmt.Errorf("qcow2: snapshot %q not found", idOrName)
	}

	snap := img.snapshots[snapIndex]

	// Decrement refcounts for all clusters referenced by this snapshot
	if err := img.decrementSnapshotRefcounts(snap); err != nil {
		return fmt.Errorf("qcow2: failed to decrement refcounts for snapshot: %w", err)
	}

	// Restore COPIED flags in current image's L1/L2 tables where refcount=1
	if err := img.restoreCopiedFlags(); err != nil {
		return fmt.Errorf("qcow2: failed to restore COPIED flags: %w", err)
	}

	// Remove snapshot from in-memory list
	img.snapshots = append(img.snapshots[:snapIndex], img.snapshots[snapIndex+1:]...)

	// Rewrite snapshot table
	if err := img.rewriteSnapshotTable(); err != nil {
		return fmt.Errorf("qcow2: failed to rewrite snapshot table: %w", err)
	}

	return nil
}

// decrementSnapshotRefcounts decrements refcounts for all clusters referenced
// by a snapshot. This includes:
// - The snapshot's L1 table clusters (exclusively owned)
// - All L2 tables referenced by the snapshot's L1 table
// - All data clusters referenced by those L2 tables
func (img *Image) decrementSnapshotRefcounts(snap *Snapshot) error {
	// Load the snapshot's L1 table
	l1Table, err := img.loadSnapshotL1Table(snap)
	if err != nil {
		return err
	}

	// Decrement refcounts for L2 tables and data clusters
	l1Entries := uint64(len(l1Table)) / 8
	for i := uint64(0); i < l1Entries; i++ {
		l1Entry := binary.BigEndian.Uint64(l1Table[i*8:])
		if l1Entry == 0 {
			continue
		}

		l2Offset := l1Entry & L1EntryOffsetMask
		if l2Offset == 0 {
			continue
		}

		// Read L2 table and decrement refcounts for data clusters
		l2Table, err := img.getL2Table(l2Offset)
		if err != nil {
			return fmt.Errorf("failed to read L2 table at 0x%x: %w", l2Offset, err)
		}

		for j := uint64(0); j < img.l2Entries; j++ {
			l2Entry := binary.BigEndian.Uint64(l2Table[j*8:])
			if l2Entry == 0 {
				continue
			}

			// Skip compressed clusters - they have special offset format
			if l2Entry&L2EntryCompressed != 0 {
				continue
			}

			// Skip zero clusters without allocation
			if l2Entry&L2EntryZeroFlag != 0 && l2Entry&L2EntryOffsetMask == 0 {
				continue
			}

			dataOffset := l2Entry & L2EntryOffsetMask
			if dataOffset != 0 {
				if err := img.decrementRefcount(dataOffset); err != nil {
					return fmt.Errorf("failed to decrement data cluster refcount at 0x%x: %w", dataOffset, err)
				}
			}
		}

		// Decrement refcount for L2 table itself
		if err := img.decrementRefcount(l2Offset); err != nil {
			return fmt.Errorf("failed to decrement L2 table refcount at 0x%x: %w", l2Offset, err)
		}
	}

	// Decrement refcounts for the snapshot's L1 table clusters
	l1TableSize := uint64(snap.L1Size) * 8
	l1Clusters := (l1TableSize + img.clusterSize - 1) / img.clusterSize
	for i := uint64(0); i < l1Clusters; i++ {
		if err := img.decrementRefcount(snap.L1TableOffset + i*img.clusterSize); err != nil {
			return fmt.Errorf("failed to decrement L1 table refcount at 0x%x: %w",
				snap.L1TableOffset+i*img.clusterSize, err)
		}
	}

	return nil
}

// rewriteSnapshotTable writes the current snapshot list to disk.
// This allocates new clusters if needed and updates the header.
func (img *Image) rewriteSnapshotTable() error {
	oldSnapshotTableOffset := img.header.SnapshotsOffset

	// If no snapshots remain, clear the snapshot table
	if len(img.snapshots) == 0 {
		img.header.SnapshotsOffset = 0
		img.header.NbSnapshots = 0

		if err := img.writeHeader(); err != nil {
			return fmt.Errorf("failed to write header: %w", err)
		}

		// Decrement refcount for old snapshot table cluster(s)
		if oldSnapshotTableOffset != 0 {
			if err := img.decrementRefcount(oldSnapshotTableOffset); err != nil {
				// Log but don't fail - old table may not have proper refcounts
			}
		}

		if err := img.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync: %w", err)
		}

		return nil
	}

	// Serialize all remaining snapshots
	var tableData []byte
	for _, snap := range img.snapshots {
		tableData = append(tableData, serializeSnapshot(snap)...)
	}

	// Calculate clusters needed
	tableClusters := (uint64(len(tableData)) + img.clusterSize - 1) / img.clusterSize

	// Allocate clusters for the new snapshot table
	newTableOffset, err := img.allocateCluster()
	if err != nil {
		return fmt.Errorf("failed to allocate cluster for snapshot table: %w", err)
	}

	// Allocate remaining clusters if needed
	for i := uint64(1); i < tableClusters; i++ {
		nextCluster, err := img.allocateCluster()
		if err != nil {
			return fmt.Errorf("failed to allocate cluster for snapshot table: %w", err)
		}
		if nextCluster != newTableOffset+i*img.clusterSize {
			return fmt.Errorf("non-contiguous cluster allocation for snapshot table")
		}
	}

	// Pad table data to cluster boundary
	paddedTable := make([]byte, tableClusters*img.clusterSize)
	copy(paddedTable, tableData)

	// Write table to disk
	if _, err := img.file.WriteAt(paddedTable, int64(newTableOffset)); err != nil {
		return fmt.Errorf("failed to write snapshot table: %w", err)
	}

	// Decrement refcounts for old snapshot table clusters if present
	if img.header.SnapshotsOffset != 0 {
		// Calculate old table size based on old count
		// Since we already removed the snapshot from the list, we need to estimate
		// Just decrement for one cluster as a simple approach
		// Full cleanup happens on refcount rebuild
		if err := img.decrementRefcount(img.header.SnapshotsOffset); err != nil {
			// Log but don't fail - old table may not have proper refcounts
		}
	}

	// Update header
	img.header.SnapshotsOffset = newTableOffset
	img.header.NbSnapshots = uint32(len(img.snapshots))

	if err := img.writeHeader(); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	if err := img.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync: %w", err)
	}

	return nil
}

// restoreCopiedFlags scans the current image's L1/L2 tables and restores the
// COPIED flag for any entries where refcount=1. This is called after deleting
// a snapshot to ensure the COPIED flag is consistent with refcounts.
func (img *Image) restoreCopiedFlags() error {
	img.l1Mu.Lock()
	defer img.l1Mu.Unlock()

	l1Modified := false

	for i := uint64(0); i < uint64(img.header.L1Size); i++ {
		l1Entry := binary.BigEndian.Uint64(img.l1Table[i*8:])
		if l1Entry == 0 {
			continue
		}

		l2Offset := l1Entry & L1EntryOffsetMask
		if l2Offset == 0 {
			continue
		}

		// Check if L2 table needs COPIED flag restored
		if l1Entry&L1EntryCopied == 0 {
			refcount, err := img.getRefcount(l2Offset)
			if err != nil {
				return fmt.Errorf("failed to get refcount for L2 table at 0x%x: %w", l2Offset, err)
			}
			if refcount == 1 {
				// Restore COPIED flag in L1 entry
				l1Entry |= L1EntryCopied
				binary.BigEndian.PutUint64(img.l1Table[i*8:], l1Entry)
				l1Modified = true
			}
		}

		// Read L2 table and check data clusters
		l2Table, err := img.getL2Table(l2Offset)
		if err != nil {
			return fmt.Errorf("failed to read L2 table at 0x%x: %w", l2Offset, err)
		}

		l2Modified := make([]byte, len(l2Table))
		copy(l2Modified, l2Table)
		needsL2Write := false

		for j := uint64(0); j < img.l2Entries; j++ {
			l2Entry := binary.BigEndian.Uint64(l2Modified[j*8:])
			if l2Entry == 0 {
				continue
			}

			// Skip compressed clusters
			if l2Entry&L2EntryCompressed != 0 {
				continue
			}

			// Skip zero clusters without allocation
			if l2Entry&L2EntryZeroFlag != 0 && l2Entry&L2EntryOffsetMask == 0 {
				continue
			}

			dataOffset := l2Entry & L2EntryOffsetMask
			if dataOffset == 0 {
				continue
			}

			// Check if COPIED flag needs to be restored
			if l2Entry&L2EntryCopied == 0 {
				refcount, err := img.getRefcount(dataOffset)
				if err != nil {
					return fmt.Errorf("failed to get refcount for data cluster at 0x%x: %w", dataOffset, err)
				}
				if refcount == 1 {
					// Restore COPIED flag
					l2Entry |= L2EntryCopied
					binary.BigEndian.PutUint64(l2Modified[j*8:], l2Entry)
					needsL2Write = true
				}
			}
		}

		// Write modified L2 table
		if needsL2Write {
			if _, err := img.file.WriteAt(l2Modified, int64(l2Offset)); err != nil {
				return fmt.Errorf("failed to write L2 table at 0x%x: %w", l2Offset, err)
			}
			img.l2Cache.put(l2Offset, l2Modified)
		}
	}

	// Write modified L1 table
	if l1Modified {
		if _, err := img.file.WriteAt(img.l1Table, int64(img.header.L1TableOffset)); err != nil {
			return fmt.Errorf("failed to write L1 table: %w", err)
		}
	}

	return nil
}

// RevertToSnapshot reverts the image to the state captured by the given snapshot.
// This discards all changes made since the snapshot was created.
// The snapshot itself remains intact and can be reverted to again.
func (img *Image) RevertToSnapshot(idOrName string) error {
	if img.readOnly {
		return fmt.Errorf("qcow2: cannot revert snapshot on read-only image")
	}

	if idOrName == "" {
		return fmt.Errorf("qcow2: snapshot ID or name cannot be empty")
	}

	// Find the snapshot
	snap := img.FindSnapshot(idOrName)
	if snap == nil {
		return fmt.Errorf("qcow2: snapshot %q not found", idOrName)
	}

	// Load the snapshot's L1 table
	snapL1Table, err := img.loadSnapshotL1Table(snap)
	if err != nil {
		return fmt.Errorf("qcow2: failed to load snapshot L1 table: %w", err)
	}

	// Decrement refcounts for all clusters in current L1/L2 tables
	// This "releases" the current state
	if err := img.decrementCurrentRefcounts(); err != nil {
		return fmt.Errorf("qcow2: failed to decrement current refcounts: %w", err)
	}

	// Copy snapshot's L1 table to current L1 table
	img.l1Mu.Lock()
	// Ensure the tables are the same size
	if uint32(len(snapL1Table)/8) != img.header.L1Size {
		img.l1Mu.Unlock()
		return fmt.Errorf("qcow2: snapshot L1 size mismatch: snapshot=%d, current=%d",
			len(snapL1Table)/8, img.header.L1Size)
	}
	copy(img.l1Table, snapL1Table)
	img.l1Mu.Unlock()

	// Increment refcounts for all clusters in the restored L1/L2 tables
	// This "adds" the snapshot state as the current state
	if err := img.incrementCurrentRefcounts(); err != nil {
		return fmt.Errorf("qcow2: failed to increment restored refcounts: %w", err)
	}

	// Write the restored L1 table to disk
	img.l1Mu.Lock()
	if _, err := img.file.WriteAt(img.l1Table, int64(img.header.L1TableOffset)); err != nil {
		img.l1Mu.Unlock()
		return fmt.Errorf("qcow2: failed to write restored L1 table: %w", err)
	}
	img.l1Mu.Unlock()

	// Clear L2 cache since the L2 tables may have changed
	img.l2Cache.clear()

	// Restore COPIED flags
	if err := img.restoreCopiedFlags(); err != nil {
		return fmt.Errorf("qcow2: failed to restore COPIED flags: %w", err)
	}

	if err := img.file.Sync(); err != nil {
		return fmt.Errorf("qcow2: failed to sync: %w", err)
	}

	return nil
}

// decrementCurrentRefcounts decrements refcounts for all L2 tables and data clusters
// referenced by the current image's L1 table.
func (img *Image) decrementCurrentRefcounts() error {
	img.l1Mu.RLock()
	defer img.l1Mu.RUnlock()

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

		// Read L2 table and decrement refcounts for data clusters
		l2Table, err := img.getL2Table(l2Offset)
		if err != nil {
			return fmt.Errorf("failed to read L2 table at 0x%x: %w", l2Offset, err)
		}

		for j := uint64(0); j < img.l2Entries; j++ {
			l2Entry := binary.BigEndian.Uint64(l2Table[j*8:])
			if l2Entry == 0 {
				continue
			}

			// Skip compressed clusters
			if l2Entry&L2EntryCompressed != 0 {
				continue
			}

			// Skip zero clusters without allocation
			if l2Entry&L2EntryZeroFlag != 0 && l2Entry&L2EntryOffsetMask == 0 {
				continue
			}

			dataOffset := l2Entry & L2EntryOffsetMask
			if dataOffset != 0 {
				if err := img.decrementRefcount(dataOffset); err != nil {
					return fmt.Errorf("failed to decrement data cluster refcount at 0x%x: %w", dataOffset, err)
				}
			}
		}

		// Decrement refcount for L2 table itself
		if err := img.decrementRefcount(l2Offset); err != nil {
			return fmt.Errorf("failed to decrement L2 table refcount at 0x%x: %w", l2Offset, err)
		}
	}

	return nil
}

// incrementCurrentRefcounts increments refcounts for all L2 tables and data clusters
// referenced by the current image's L1 table.
func (img *Image) incrementCurrentRefcounts() error {
	img.l1Mu.RLock()
	defer img.l1Mu.RUnlock()

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

		// Increment refcount for L2 table
		if err := img.incrementRefcount(l2Offset); err != nil {
			return fmt.Errorf("failed to increment L2 table refcount at 0x%x: %w", l2Offset, err)
		}

		// Read L2 table and increment refcounts for data clusters
		l2Table, err := img.getL2Table(l2Offset)
		if err != nil {
			return fmt.Errorf("failed to read L2 table at 0x%x: %w", l2Offset, err)
		}

		for j := uint64(0); j < img.l2Entries; j++ {
			l2Entry := binary.BigEndian.Uint64(l2Table[j*8:])
			if l2Entry == 0 {
				continue
			}

			// Skip compressed clusters
			if l2Entry&L2EntryCompressed != 0 {
				continue
			}

			// Skip zero clusters without allocation
			if l2Entry&L2EntryZeroFlag != 0 && l2Entry&L2EntryOffsetMask == 0 {
				continue
			}

			dataOffset := l2Entry & L2EntryOffsetMask
			if dataOffset != 0 {
				if err := img.incrementRefcount(dataOffset); err != nil {
					return fmt.Errorf("failed to increment data cluster refcount at 0x%x: %w", dataOffset, err)
				}
			}
		}
	}

	return nil
}
