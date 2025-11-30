package qcow2

import (
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// checkQemuImgBitmap checks if qemu-img supports bitmaps (QEMU 4.0+)
func checkQemuImgBitmap(t *testing.T) bool {
	t.Helper()
	// Try to create an image with a bitmap
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "bitmap_check.qcow2")

	cmd := exec.Command("qemu-img", "create", "-f", "qcow2", testPath, "1M")
	if err := cmd.Run(); err != nil {
		t.Skip("qemu-img not available")
		return false
	}

	// Try to add a bitmap
	cmd = exec.Command("qemu-img", "bitmap", "--add", testPath, "test-bitmap")
	if err := cmd.Run(); err != nil {
		t.Skip("qemu-img does not support bitmaps (requires QEMU 4.0+)")
		return false
	}

	return true
}

func TestBitmapsNone(t *testing.T) {
	// Create a simple image without bitmaps
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "no_bitmaps.qcow2")

	cmd := exec.Command("qemu-img", "create", "-f", "qcow2", testPath, "10M")
	if err := cmd.Run(); err != nil {
		t.Skip("qemu-img not available")
	}

	img, err := Open(testPath)
	if err != nil {
		t.Fatalf("Failed to open image: %v", err)
	}
	defer img.Close()

	bitmaps, err := img.Bitmaps()
	if err != nil {
		t.Fatalf("Bitmaps() returned error: %v", err)
	}

	if bitmaps != nil {
		t.Errorf("Expected nil bitmaps for image without bitmap extension, got %d bitmaps", len(bitmaps))
	}
}

func TestBitmapsBasic(t *testing.T) {
	if !checkQemuImgBitmap(t) {
		return
	}

	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "with_bitmaps.qcow2")

	// Create image
	cmd := exec.Command("qemu-img", "create", "-f", "qcow2", testPath, "10M")
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to create image: %v", err)
	}

	// Add a bitmap
	cmd = exec.Command("qemu-img", "bitmap", "--add", testPath, "backup-bitmap")
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to add bitmap: %v", err)
	}

	// Open and verify
	img, err := Open(testPath)
	if err != nil {
		t.Fatalf("Failed to open image: %v", err)
	}
	defer img.Close()

	bitmaps, err := img.Bitmaps()
	if err != nil {
		t.Fatalf("Bitmaps() returned error: %v", err)
	}

	if len(bitmaps) != 1 {
		t.Fatalf("Expected 1 bitmap, got %d", len(bitmaps))
	}

	bm := bitmaps[0]
	if bm.Name != "backup-bitmap" {
		t.Errorf("Expected bitmap name 'backup-bitmap', got %q", bm.Name)
	}

	if bm.Type != BitmapTypeTracking {
		t.Errorf("Expected bitmap type %d (tracking), got %d", BitmapTypeTracking, bm.Type)
	}

	// Granularity should be at least 512 bytes (2^9)
	if bm.Granularity < 512 {
		t.Errorf("Unexpected granularity: %d (expected >= 512)", bm.Granularity)
	}

	t.Logf("Bitmap: name=%s, type=%d, granularity=%d, granularity_bits=%d, flags=0x%x, consistent=%v, enabled=%v",
		bm.Name, bm.Type, bm.Granularity, bm.GranularityBits, bm.Flags, bm.IsConsistent, bm.IsEnabled)
}

func TestBitmapsMultiple(t *testing.T) {
	if !checkQemuImgBitmap(t) {
		return
	}

	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "multi_bitmaps.qcow2")

	// Create image
	cmd := exec.Command("qemu-img", "create", "-f", "qcow2", testPath, "10M")
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to create image: %v", err)
	}

	// Add multiple bitmaps
	bitmapNames := []string{"bitmap-1", "bitmap-2", "bitmap-3"}
	for _, name := range bitmapNames {
		cmd = exec.Command("qemu-img", "bitmap", "--add", testPath, name)
		if err := cmd.Run(); err != nil {
			t.Fatalf("Failed to add bitmap %s: %v", name, err)
		}
	}

	// Open and verify
	img, err := Open(testPath)
	if err != nil {
		t.Fatalf("Failed to open image: %v", err)
	}
	defer img.Close()

	bitmaps, err := img.Bitmaps()
	if err != nil {
		t.Fatalf("Bitmaps() returned error: %v", err)
	}

	if len(bitmaps) != len(bitmapNames) {
		t.Fatalf("Expected %d bitmaps, got %d", len(bitmapNames), len(bitmaps))
	}

	// Verify each bitmap can be found
	for _, name := range bitmapNames {
		info, err := img.FindBitmap(name)
		if err != nil {
			t.Errorf("FindBitmap(%q) returned error: %v", name, err)
			continue
		}
		if info.Name != name {
			t.Errorf("FindBitmap(%q) returned wrong name: %s", name, info.Name)
		}
	}

	// Verify non-existent bitmap returns error
	_, err = img.FindBitmap("nonexistent")
	if err != ErrBitmapNotFound {
		t.Errorf("Expected ErrBitmapNotFound for non-existent bitmap, got: %v", err)
	}
}

func TestBitmapOpenAndRead(t *testing.T) {
	if !checkQemuImgBitmap(t) {
		return
	}

	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "dirty_bitmap.qcow2")

	// Create image
	cmd := exec.Command("qemu-img", "create", "-f", "qcow2", testPath, "10M")
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to create image: %v", err)
	}

	// Add a bitmap with --enable to make it active
	cmd = exec.Command("qemu-img", "bitmap", "--add", "--enable", testPath, "dirty-bitmap")
	output, err := cmd.CombinedOutput()
	if err != nil {
		// --enable might not be supported, try without it
		cmd = exec.Command("qemu-img", "bitmap", "--add", testPath, "dirty-bitmap")
		if err := cmd.Run(); err != nil {
			t.Fatalf("Failed to add bitmap: %v (output: %s)", err, output)
		}
	}

	// Open and verify we can open the bitmap
	img, err := Open(testPath)
	if err != nil {
		t.Fatalf("Failed to open image: %v", err)
	}
	defer img.Close()

	// Try to open the bitmap
	bm, err := img.OpenBitmap("dirty-bitmap")
	if err != nil {
		// Bitmap may be inconsistent if it was just created
		if err == ErrBitmapInconsistent {
			t.Logf("Bitmap is inconsistent (expected for freshly created bitmap)")
			return
		}
		t.Fatalf("OpenBitmap returned error: %v", err)
	}

	// Verify basic properties
	if bm.Name() != "dirty-bitmap" {
		t.Errorf("Expected name 'dirty-bitmap', got %q", bm.Name())
	}

	info := bm.Info()
	t.Logf("Opened bitmap: granularity=%d bytes", info.Granularity)

	// Check a bit (should return false for empty image)
	isSet, err := bm.IsSet(0)
	if err != nil {
		t.Errorf("IsSet(0) returned error: %v", err)
	}
	t.Logf("Bit at offset 0 is set: %v", isSet)

	// Get dirty ranges (should be empty or all dirty depending on bitmap state)
	ranges, err := bm.GetDirtyRanges()
	if err != nil {
		t.Errorf("GetDirtyRanges() returned error: %v", err)
	}
	t.Logf("Dirty ranges: %d", len(ranges))
	for i, r := range ranges {
		t.Logf("  Range %d: offset=%d, length=%d", i, r[0], r[1])
	}

	// Count dirty bits
	count, err := bm.CountDirtyBits()
	if err != nil {
		t.Errorf("CountDirtyBits() returned error: %v", err)
	}
	t.Logf("Dirty bits: %d", count)
}

func TestBitmapWithWrites(t *testing.T) {
	if !checkQemuImgBitmap(t) {
		return
	}

	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "write_bitmap.qcow2")

	// Create image with qemu-img
	cmd := exec.Command("qemu-img", "create", "-f", "qcow2", testPath, "10M")
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to create image: %v", err)
	}

	// Use qemu-io to write some data (this may enable bitmap tracking)
	cmd = exec.Command("qemu-io", "-c", "write -P 0xAB 0 64k", testPath)
	if err := cmd.Run(); err != nil {
		t.Logf("qemu-io write failed (may not be available): %v", err)
	}

	// Add bitmap after writes
	cmd = exec.Command("qemu-img", "bitmap", "--add", testPath, "incremental")
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to add bitmap: %v", err)
	}

	// Write more data with bitmap active
	cmd = exec.Command("qemu-io", "-c", "write -P 0xCD 1M 64k", testPath)
	if err := cmd.Run(); err != nil {
		t.Logf("qemu-io write failed: %v", err)
	}

	// Open and check
	img, err := Open(testPath)
	if err != nil {
		t.Fatalf("Failed to open image: %v", err)
	}
	defer img.Close()

	info, err := img.FindBitmap("incremental")
	if err != nil {
		t.Fatalf("FindBitmap returned error: %v", err)
	}

	t.Logf("Bitmap info: consistent=%v, enabled=%v, granularity=%d",
		info.IsConsistent, info.IsEnabled, info.Granularity)
}

func TestBitmapGranularity(t *testing.T) {
	if !checkQemuImgBitmap(t) {
		return
	}

	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "granularity_bitmap.qcow2")

	// Create image
	cmd := exec.Command("qemu-img", "create", "-f", "qcow2", testPath, "10M")
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to create image: %v", err)
	}

	// Add bitmap with specific granularity (try 1M = 2^20)
	cmd = exec.Command("qemu-img", "bitmap", "--add", "-g", "1M", testPath, "coarse-bitmap")
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Granularity option might not be supported
		t.Logf("Failed to add bitmap with granularity (may not be supported): %v (output: %s)", err, output)
		// Try without granularity
		cmd = exec.Command("qemu-img", "bitmap", "--add", testPath, "coarse-bitmap")
		if err := cmd.Run(); err != nil {
			t.Fatalf("Failed to add bitmap: %v", err)
		}
	}

	// Open and verify
	img, err := Open(testPath)
	if err != nil {
		t.Fatalf("Failed to open image: %v", err)
	}
	defer img.Close()

	info, err := img.FindBitmap("coarse-bitmap")
	if err != nil {
		t.Fatalf("FindBitmap returned error: %v", err)
	}

	t.Logf("Bitmap granularity: %d bytes (2^%d)", info.Granularity, info.GranularityBits)

	// Verify granularity is a power of 2
	if info.Granularity != (1 << info.GranularityBits) {
		t.Errorf("Granularity mismatch: %d != 2^%d", info.Granularity, info.GranularityBits)
	}
}

func TestBitmapInfo(t *testing.T) {
	if !checkQemuImgBitmap(t) {
		return
	}

	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "info_bitmap.qcow2")

	// Create image
	cmd := exec.Command("qemu-img", "create", "-f", "qcow2", testPath, "100M")
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to create image: %v", err)
	}

	// Add bitmap
	cmd = exec.Command("qemu-img", "bitmap", "--add", testPath, "test")
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to add bitmap: %v", err)
	}

	// Get qemu-img info for comparison
	cmd = exec.Command("qemu-img", "info", testPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("qemu-img info failed: %v", err)
	} else {
		t.Logf("qemu-img info output:\n%s", output)
	}

	// Open and verify
	img, err := OpenFile(testPath, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Failed to open image: %v", err)
	}
	defer img.Close()

	bitmaps, err := img.Bitmaps()
	if err != nil {
		t.Fatalf("Bitmaps() returned error: %v", err)
	}

	for _, bm := range bitmaps {
		t.Logf("Bitmap: %+v", bm)
	}
}

// Unit tests for bitmap parsing (don't require qemu-img bitmap support)

func TestParseBitmapExtension(t *testing.T) {
	tests := []struct {
		name      string
		data      []byte
		wantErr   bool
		wantCount uint32
	}{
		{
			name: "valid single bitmap",
			data: func() []byte {
				d := make([]byte, 24)
				binary.BigEndian.PutUint32(d[0:4], 1)         // nb_bitmaps
				binary.BigEndian.PutUint32(d[4:8], 0)         // reserved
				binary.BigEndian.PutUint64(d[8:16], 64)       // directory_size
				binary.BigEndian.PutUint64(d[16:24], 0x10000) // directory_offset
				return d
			}(),
			wantErr:   false,
			wantCount: 1,
		},
		{
			name: "valid multiple bitmaps",
			data: func() []byte {
				d := make([]byte, 24)
				binary.BigEndian.PutUint32(d[0:4], 5)         // nb_bitmaps
				binary.BigEndian.PutUint32(d[4:8], 0)         // reserved
				binary.BigEndian.PutUint64(d[8:16], 320)      // directory_size
				binary.BigEndian.PutUint64(d[16:24], 0x20000) // directory_offset
				return d
			}(),
			wantErr:   false,
			wantCount: 5,
		},
		{
			name:    "too short",
			data:    make([]byte, 20),
			wantErr: true,
		},
		{
			name: "reserved not zero",
			data: func() []byte {
				d := make([]byte, 24)
				binary.BigEndian.PutUint32(d[0:4], 1)
				binary.BigEndian.PutUint32(d[4:8], 1) // reserved not zero
				binary.BigEndian.PutUint64(d[8:16], 64)
				binary.BigEndian.PutUint64(d[16:24], 0x10000)
				return d
			}(),
			wantErr: true,
		},
		{
			name: "zero bitmaps",
			data: func() []byte {
				d := make([]byte, 24)
				binary.BigEndian.PutUint32(d[0:4], 0) // zero bitmaps
				binary.BigEndian.PutUint32(d[4:8], 0)
				binary.BigEndian.PutUint64(d[8:16], 0)
				binary.BigEndian.PutUint64(d[16:24], 0)
				return d
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ext, err := parseBitmapExtension(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseBitmapExtension() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && ext.nbBitmaps != tt.wantCount {
				t.Errorf("parseBitmapExtension() nbBitmaps = %d, want %d", ext.nbBitmaps, tt.wantCount)
			}
		})
	}
}

func TestParseBitmapDirectoryEntry(t *testing.T) {
	tests := []struct {
		name            string
		data            []byte
		wantErr         bool
		wantName        string
		wantType        uint8
		wantGranularity uint64
		wantConsumed    int
	}{
		{
			name: "basic entry",
			data: func() []byte {
				d := make([]byte, 40)
				binary.BigEndian.PutUint64(d[0:8], 0x30000) // table_offset
				binary.BigEndian.PutUint32(d[8:12], 10)     // table_size
				binary.BigEndian.PutUint32(d[12:16], 0)     // flags
				d[16] = BitmapTypeTracking                  // type
				d[17] = 16                                  // granularity_bits (64KB)
				binary.BigEndian.PutUint16(d[18:20], 4)     // name_size
				binary.BigEndian.PutUint32(d[20:24], 0)     // extra_data_size
				copy(d[24:28], "test")                      // name
				return d
			}(),
			wantErr:         false,
			wantName:        "test",
			wantType:        BitmapTypeTracking,
			wantGranularity: 65536, // 2^16
			wantConsumed:    32,    // 24 + 4 name, aligned to 8
		},
		{
			name: "longer name with padding",
			data: func() []byte {
				d := make([]byte, 48)
				binary.BigEndian.PutUint64(d[0:8], 0x40000)
				binary.BigEndian.PutUint32(d[8:12], 5)
				binary.BigEndian.PutUint32(d[12:16], BitmapFlagAuto)
				d[16] = BitmapTypeTracking
				d[17] = 20                               // 1MB granularity
				binary.BigEndian.PutUint16(d[18:20], 14) // name_size
				binary.BigEndian.PutUint32(d[20:24], 0)
				copy(d[24:38], "backup-bitmap1")
				return d
			}(),
			wantErr:         false,
			wantName:        "backup-bitmap1",
			wantType:        BitmapTypeTracking,
			wantGranularity: 1048576, // 2^20 = 1MB
			wantConsumed:    40,      // 24 + 14 name = 38, aligned to 40
		},
		{
			name:    "too short",
			data:    make([]byte, 20),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, consumed, err := parseBitmapDirectoryEntry(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseBitmapDirectoryEntry() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if info.Name != tt.wantName {
				t.Errorf("Name = %q, want %q", info.Name, tt.wantName)
			}
			if info.Type != tt.wantType {
				t.Errorf("Type = %d, want %d", info.Type, tt.wantType)
			}
			if info.Granularity != tt.wantGranularity {
				t.Errorf("Granularity = %d, want %d", info.Granularity, tt.wantGranularity)
			}
			if consumed != tt.wantConsumed {
				t.Errorf("consumed = %d, want %d", consumed, tt.wantConsumed)
			}
		})
	}
}

func TestBitmapTableEntryMasks(t *testing.T) {
	// Test the bitmap table entry masks
	tests := []struct {
		name       string
		entry      uint64
		wantOffset uint64
		wantAllOne bool
	}{
		{
			name:       "unallocated zeros",
			entry:      0,
			wantOffset: 0,
			wantAllOne: false,
		},
		{
			name:       "unallocated ones",
			entry:      BMETableEntryFlagAllOnes,
			wantOffset: 0,
			wantAllOne: true,
		},
		{
			name:       "allocated at offset",
			entry:      0x0000000012345000,
			wantOffset: 0x0000000012345000,
			wantAllOne: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offset := tt.entry & BMETableEntryOffsetMask
			allOne := tt.entry&BMETableEntryFlagAllOnes != 0

			if offset != tt.wantOffset {
				t.Errorf("offset = 0x%x, want 0x%x", offset, tt.wantOffset)
			}
			if allOne != tt.wantAllOne {
				t.Errorf("allOne = %v, want %v", allOne, tt.wantAllOne)
			}
		})
	}
}

func TestBitmapFlags(t *testing.T) {
	tests := []struct {
		flags       uint32
		wantInUse   bool
		wantAuto    bool
		wantExtraOk bool
	}{
		{0, false, false, false},
		{BitmapFlagInUse, true, false, false},
		{BitmapFlagAuto, false, true, false},
		{BitmapFlagExtraDataCompatible, false, false, true},
		{BitmapFlagInUse | BitmapFlagAuto, true, true, false},
		{BitmapFlagInUse | BitmapFlagAuto | BitmapFlagExtraDataCompatible, true, true, true},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			inUse := tt.flags&BitmapFlagInUse != 0
			auto := tt.flags&BitmapFlagAuto != 0
			extraOk := tt.flags&BitmapFlagExtraDataCompatible != 0

			if inUse != tt.wantInUse {
				t.Errorf("inUse = %v, want %v (flags=0x%x)", inUse, tt.wantInUse, tt.flags)
			}
			if auto != tt.wantAuto {
				t.Errorf("auto = %v, want %v (flags=0x%x)", auto, tt.wantAuto, tt.flags)
			}
			if extraOk != tt.wantExtraOk {
				t.Errorf("extraOk = %v, want %v (flags=0x%x)", extraOk, tt.wantExtraOk, tt.flags)
			}
		})
	}
}

func TestPopcount8(t *testing.T) {
	tests := []struct {
		input byte
		want  int
	}{
		{0x00, 0},
		{0x01, 1},
		{0x03, 2},
		{0x07, 3},
		{0x0F, 4},
		{0xFF, 8},
		{0xAA, 4}, // 10101010
		{0x55, 4}, // 01010101
		{0x80, 1},
		{0xF0, 4},
	}

	for _, tt := range tests {
		got := popcount8(tt.input)
		if got != tt.want {
			t.Errorf("popcount8(0x%02x) = %d, want %d", tt.input, got, tt.want)
		}
	}
}
