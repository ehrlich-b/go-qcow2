package qcow2

import (
	"math/rand"
	"path/filepath"
	"testing"
)

// Helper to create a preallocated image for read benchmarks
func setupBenchImage(b *testing.B, size uint64, preallocate bool) *Image {
	b.Helper()
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.qcow2")

	img, err := CreateSimple(path, size)
	if err != nil {
		b.Fatalf("Create failed: %v", err)
	}

	if preallocate {
		// Use BarrierNone during setup for speed (no fsync per write)
		img.SetWriteBarrierMode(BarrierNone)

		// Preallocate by writing to fill the image
		// Use large buffer for efficiency
		buf := make([]byte, 1024*1024) // 1MB buffer
		for i := range buf {
			buf[i] = byte(i & 0xff)
		}
		for off := uint64(0); off < size; off += uint64(len(buf)) {
			toWrite := uint64(len(buf))
			if off+toWrite > size {
				toWrite = size - off
			}
			if _, err := img.WriteAt(buf[:toWrite], int64(off)); err != nil {
				b.Fatalf("Preallocate write failed: %v", err)
			}
		}
		// Flush and restore default barrier mode
		img.Flush()
		img.SetWriteBarrierMode(BarrierFull)
	}

	return img
}

// BenchmarkReadAt4K benchmarks 4KB sequential reads
func BenchmarkReadAt4K(b *testing.B) {
	const imageSize = 64 * 1024 * 1024 // 64MB
	const readSize = 4096

	img := setupBenchImage(b, imageSize, true)
	defer img.Close()

	buf := make([]byte, readSize)
	b.SetBytes(readSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		off := int64((i * readSize) % (imageSize - readSize))
		if _, err := img.ReadAt(buf, off); err != nil {
			b.Fatalf("ReadAt failed: %v", err)
		}
	}
}

// BenchmarkReadAt64K benchmarks cluster-sized sequential reads (64KB)
func BenchmarkReadAt64K(b *testing.B) {
	const imageSize = 64 * 1024 * 1024 // 64MB
	const readSize = 64 * 1024         // 64KB = cluster size

	img := setupBenchImage(b, imageSize, true)
	defer img.Close()

	buf := make([]byte, readSize)
	b.SetBytes(readSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		off := int64((i * readSize) % (imageSize - readSize))
		if _, err := img.ReadAt(buf, off); err != nil {
			b.Fatalf("ReadAt failed: %v", err)
		}
	}
}

// BenchmarkReadAt1M benchmarks 1MB reads spanning multiple clusters
func BenchmarkReadAt1M(b *testing.B) {
	const imageSize = 128 * 1024 * 1024 // 128MB
	const readSize = 1024 * 1024        // 1MB

	img := setupBenchImage(b, imageSize, true)
	defer img.Close()

	buf := make([]byte, readSize)
	b.SetBytes(readSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		off := int64((i * readSize) % (imageSize - readSize))
		if _, err := img.ReadAt(buf, off); err != nil {
			b.Fatalf("ReadAt failed: %v", err)
		}
	}
}

// BenchmarkReadAtRandom4K benchmarks 4KB random reads
func BenchmarkReadAtRandom4K(b *testing.B) {
	const imageSize = 64 * 1024 * 1024 // 64MB
	const readSize = 4096

	img := setupBenchImage(b, imageSize, true)
	defer img.Close()

	rng := rand.New(rand.NewSource(42))
	buf := make([]byte, readSize)
	b.SetBytes(readSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		off := int64(rng.Intn(imageSize - readSize))
		if _, err := img.ReadAt(buf, off); err != nil {
			b.Fatalf("ReadAt failed: %v", err)
		}
	}
}

// BenchmarkWriteAt4K benchmarks 4KB sequential writes
func BenchmarkWriteAt4K(b *testing.B) {
	const imageSize = 64 * 1024 * 1024 // 64MB
	const writeSize = 4096

	img := setupBenchImage(b, imageSize, false)
	defer img.Close()

	buf := make([]byte, writeSize)
	for i := range buf {
		buf[i] = byte(i)
	}
	b.SetBytes(writeSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		off := int64((i * writeSize) % (imageSize - writeSize))
		if _, err := img.WriteAt(buf, off); err != nil {
			b.Fatalf("WriteAt failed: %v", err)
		}
	}
}

// BenchmarkWriteAt64K benchmarks cluster-sized sequential writes
func BenchmarkWriteAt64K(b *testing.B) {
	const imageSize = 64 * 1024 * 1024 // 64MB
	const writeSize = 64 * 1024        // 64KB = cluster size

	img := setupBenchImage(b, imageSize, false)
	defer img.Close()

	buf := make([]byte, writeSize)
	for i := range buf {
		buf[i] = byte(i)
	}
	b.SetBytes(writeSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		off := int64((i * writeSize) % (imageSize - writeSize))
		if _, err := img.WriteAt(buf, off); err != nil {
			b.Fatalf("WriteAt failed: %v", err)
		}
	}
}

// BenchmarkWriteAtRandom4K benchmarks 4KB random writes
func BenchmarkWriteAtRandom4K(b *testing.B) {
	const imageSize = 64 * 1024 * 1024 // 64MB
	const writeSize = 4096

	img := setupBenchImage(b, imageSize, false)
	defer img.Close()

	rng := rand.New(rand.NewSource(42))
	buf := make([]byte, writeSize)
	for i := range buf {
		buf[i] = byte(i)
	}
	b.SetBytes(writeSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		off := int64(rng.Intn(imageSize - writeSize))
		if _, err := img.WriteAt(buf, off); err != nil {
			b.Fatalf("WriteAt failed: %v", err)
		}
	}
}

// BenchmarkReadUnallocated benchmarks reads from unallocated clusters (returns zeros)
func BenchmarkReadUnallocated(b *testing.B) {
	const imageSize = 1024 * 1024 * 1024 // 1GB sparse
	const readSize = 4096

	img := setupBenchImage(b, imageSize, false) // No preallocation
	defer img.Close()

	buf := make([]byte, readSize)
	b.SetBytes(readSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Read from sparse regions (never written)
		off := int64((i * readSize) % (imageSize - readSize))
		if _, err := img.ReadAt(buf, off); err != nil {
			b.Fatalf("ReadAt failed: %v", err)
		}
	}
}

// BenchmarkL2CacheHit benchmarks repeated reads to same cluster (L2 cache hit)
func BenchmarkL2CacheHit(b *testing.B) {
	const imageSize = 64 * 1024 * 1024 // 64MB
	const readSize = 4096

	img := setupBenchImage(b, imageSize, true)
	defer img.Close()

	buf := make([]byte, readSize)
	b.SetBytes(readSize)
	b.ResetTimer()

	// Always read from the same location (L2 cache hit)
	for i := 0; i < b.N; i++ {
		if _, err := img.ReadAt(buf, 0); err != nil {
			b.Fatalf("ReadAt failed: %v", err)
		}
	}
}

// BenchmarkOverwrite benchmarks writing to already-allocated clusters
func BenchmarkOverwrite(b *testing.B) {
	const imageSize = 64 * 1024 * 1024 // 64MB
	const writeSize = 4096

	img := setupBenchImage(b, imageSize, true) // Preallocated
	defer img.Close()

	buf := make([]byte, writeSize)
	for i := range buf {
		buf[i] = byte(i)
	}
	b.SetBytes(writeSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Always write to the same location (overwrite, no allocation)
		if _, err := img.WriteAt(buf, 0); err != nil {
			b.Fatalf("WriteAt failed: %v", err)
		}
	}
}

// BenchmarkCOWRead benchmarks reads through backing chain
func BenchmarkCOWRead(b *testing.B) {
	const imageSize = 64 * 1024 * 1024 // 64MB
	const readSize = 4096

	dir := b.TempDir()
	basePath := filepath.Join(dir, "base.qcow2")
	overlayPath := filepath.Join(dir, "overlay.qcow2")

	// Create base image with data
	base, err := CreateSimple(basePath, imageSize)
	if err != nil {
		b.Fatalf("Create base failed: %v", err)
	}
	data := make([]byte, imageSize)
	for i := range data {
		data[i] = byte(i)
	}
	if _, err := base.WriteAt(data, 0); err != nil {
		b.Fatalf("Write base failed: %v", err)
	}
	base.Close()

	// Create overlay
	overlay, err := CreateOverlay(overlayPath, basePath)
	if err != nil {
		b.Fatalf("Create overlay failed: %v", err)
	}
	defer overlay.Close()

	buf := make([]byte, readSize)
	b.SetBytes(readSize)
	b.ResetTimer()

	// Reads should fall through to backing file
	for i := 0; i < b.N; i++ {
		off := int64((i * readSize) % (imageSize - readSize))
		if _, err := overlay.ReadAt(buf, off); err != nil {
			b.Fatalf("ReadAt failed: %v", err)
		}
	}
}
