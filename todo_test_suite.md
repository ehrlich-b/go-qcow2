# QCOW2 Test Suite - Comprehensive Plan

> **Goal**: A test suite worthy of a production disk format implementation.
> Data corruption is unacceptable. Every code path must be exercised.

## Table of Contents

1. [Philosophy & Principles](#philosophy--principles)
2. [Test Categories Overview](#test-categories-overview)
3. [QEMU Interoperability Testing](#1-qemu-interoperability-testing)
4. [Fuzz Testing](#2-fuzz-testing)
5. [Crash Recovery Testing](#3-crash-recovery-testing)
6. [Stress & Concurrency Testing](#4-stress--concurrency-testing)
7. [Edge Case Testing](#5-edge-case-testing)
8. [Data Integrity Testing](#6-data-integrity-testing)
9. [Feature-Specific Testing](#7-feature-specific-testing)
10. [Performance Regression Testing](#8-performance-regression-testing)
11. [Compatibility Matrix](#9-compatibility-matrix)
12. [Test Infrastructure](#10-test-infrastructure)
13. [CI/CD Integration](#11-cicd-integration)
14. [Implementation Roadmap](#12-implementation-roadmap)

---

## Philosophy & Principles

### Core Tenets

1. **QEMU is the Reference**: If `qemu-img check` passes, we're correct. If QEMU can read our images and we can read QEMU's, we're compatible.

2. **Deterministic Reproducibility**: Every test must be reproducible. Use seeded RNG for "random" data. Pin QEMU versions in Docker.

3. **Defense in Depth**: Multiple layers of validation:
   - Unit tests for individual functions
   - Integration tests for feature combinations
   - E2E tests against QEMU
   - Fuzz tests for unknown unknowns
   - Crash tests for durability

4. **Fail Fast, Fail Loud**: Tests should detect corruption immediately, not silently propagate bad data.

5. **Golden Master Comparison**: Like QEMU's iotests, compare output against known-good reference files.

### What "Correct" Means

- **Read Correctness**: Data read equals data written (or backing file data)
- **Write Correctness**: `qemu-img check` passes after our writes
- **Structural Correctness**: Metadata (L1/L2, refcounts) is valid
- **Crash Correctness**: Image is recoverable after simulated crash
- **Compatibility**: QEMU versions 2.x, 5.x, 8.x can all read/write our images

---

## Test Categories Overview

| Category | Purpose | Runtime | Frequency |
|----------|---------|---------|-----------|
| Unit Tests | Function-level correctness | <1s each | Every commit |
| Integration Tests | Feature combinations | 1-10s each | Every commit |
| QEMU Interop | Compatibility validation | 10-60s each | Every PR |
| Fuzz Tests | Unknown edge cases | Configurable | Nightly/Weekly |
| Crash Tests | Durability verification | 30-120s each | Every PR |
| Stress Tests | Race conditions, limits | 1-10min | Nightly |
| Benchmarks | Performance regression | 1-5min | Weekly |

---

## 1. QEMU Interoperability Testing

### 1.1 QEMU Version Matrix

Test against multiple QEMU versions to ensure broad compatibility:

```
| QEMU Version | Release Date | Priority | Notes |
|--------------|--------------|----------|-------|
| 2.12         | 2018         | Medium   | Old but still deployed |
| 5.2          | 2020         | High     | Widely used in production |
| 7.2          | 2022         | High     | Current stable in many distros |
| 8.2          | 2024         | Critical | Latest stable |
| 9.x          | 2024         | High     | Cutting edge |
```

### 1.2 Test Scenarios

#### A. Image Creation Cross-Validation

```
Test: create_with_us_verify_with_qemu
For each (cluster_size, version, features):
  1. Create image with go-qcow2
  2. Run: qemu-img check <image>
  3. Run: qemu-img info <image>
  4. Verify: No errors, metadata matches expected

Test: create_with_qemu_read_with_us
For each (cluster_size, version, features):
  1. Create image with qemu-img create
  2. Open with go-qcow2
  3. Verify: Header parsed correctly
  4. Verify: Size matches
```

#### B. Data Round-Trip Testing

```
Test: write_with_us_read_with_qemu
  1. Create image with go-qcow2
  2. Write test patterns at various offsets
  3. Close image
  4. Read with qemu-io: qemu-io -c "read -P <pattern> <offset> <size>" <image>
  5. Verify: All patterns match

Test: write_with_qemu_read_with_us
  1. Create image with qemu-img
  2. Write with qemu-io: qemu-io -c "write -P <pattern> <offset> <size>" <image>
  3. Open with go-qcow2
  4. Read at same offsets
  5. Verify: Data matches patterns

Test: full_roundtrip
  1. Create with qemu-img
  2. Write pattern A with qemu-io
  3. Open with go-qcow2, read pattern A, write pattern B
  4. Close, reopen with qemu-io
  5. Verify pattern A and B both correct
  6. Write pattern C with qemu-io
  7. Read with go-qcow2
  8. Verify all patterns correct
```

#### C. Feature-Specific Interop

```
Test: backing_file_interop
  1. Create base.qcow2 with qemu-img, write data
  2. Create overlay.qcow2 with go-qcow2 (backing=base.qcow2)
  3. Verify qemu-img info shows correct backing chain
  4. Read through overlay with qemu-io, verify base data
  5. Write to overlay with go-qcow2
  6. Verify with qemu-io: base unchanged, overlay has new data

Test: lazy_refcounts_interop
  1. Create with qemu-img -o lazy_refcounts=on
  2. Write with go-qcow2, close uncleanly (kill process)
  3. Run qemu-img check -r all
  4. Verify repair succeeded
  5. Data still intact

Test: compression_interop
  1. Create image with go-qcow2
  2. Write uncompressed data
  3. Compress with qemu-img convert -c
  4. Read compressed data with go-qcow2
  5. Verify decompression correct

Test: zero_clusters_interop
  1. Create with go-qcow2
  2. WriteZeroAt to create zero clusters
  3. Verify with qemu-img map: shows zero clusters
  4. Read with qemu-io: returns zeros
```

### 1.3 Image Variation Matrix

Test all combinations that make sense:

```go
var clusterSizes = []uint32{
    9,  // 512B - minimum
    12, // 4KB - common small
    16, // 64KB - default
    21, // 2MB - maximum
}

var versions = []uint32{2, 3}

var features = []struct {
    name string
    opts CreateOptions
}{
    {"plain", CreateOptions{}},
    {"lazy_refcounts", CreateOptions{LazyRefcounts: true}},
    {"with_backing", CreateOptions{BackingFile: "base.qcow2"}},
}

var refcountWidths = []int{1, 2, 4, 8, 16, 32, 64}
```

### 1.4 qemu-img Commands Reference

```bash
# Create image
qemu-img create -f qcow2 -o cluster_size=65536,lazy_refcounts=on test.qcow2 1G

# Check integrity
qemu-img check test.qcow2
qemu-img check -r leaks test.qcow2  # Repair leaks
qemu-img check -r all test.qcow2    # Repair all

# Image info
qemu-img info test.qcow2
qemu-img info --output=json test.qcow2

# Map (show allocation)
qemu-img map test.qcow2
qemu-img map --output=json test.qcow2

# I/O operations
qemu-io -c "read -P 0xaa 0 4096" test.qcow2      # Read and verify pattern
qemu-io -c "write -P 0xbb 0 4096" test.qcow2     # Write pattern
qemu-io -c "discard 0 65536" test.qcow2          # Discard (zero)
qemu-io -c "info" test.qcow2                      # Image info

# Convert/compress
qemu-img convert -c -f qcow2 -O qcow2 in.qcow2 out.qcow2

# Create overlay
qemu-img create -f qcow2 -b base.qcow2 -F qcow2 overlay.qcow2
```

---

## 2. Fuzz Testing

### 2.1 Fuzzing Strategy

Use Go's native fuzzing (go 1.18+) with custom corpus and structure-aware mutations.

### 2.2 Fuzz Targets

#### A. Header Parsing Fuzzer

```go
// fuzz_test.go
func FuzzParseHeader(f *testing.F) {
    // Seed corpus with valid headers
    f.Add(validV2Header)
    f.Add(validV3Header)
    f.Add(validV3WithExtensions)

    // Add edge cases
    f.Add(minimalValidHeader)
    f.Add(maxSizeHeader)

    f.Fuzz(func(t *testing.T, data []byte) {
        header, err := ParseHeader(data)
        if err != nil {
            return // Invalid input is fine
        }

        // If parsing succeeded, header should be usable
        _ = header.ClusterSize()
        _ = header.L2Entries()
        _ = header.Validate()

        // Round-trip: encode and re-parse should match
        encoded := header.Encode()
        reparsed, err := ParseHeader(encoded)
        if err != nil {
            t.Fatalf("Re-parse failed: %v", err)
        }
        if !headersEqual(header, reparsed) {
            t.Fatalf("Round-trip mismatch")
        }
    })
}
```

#### B. L2 Entry Fuzzer

```go
func FuzzL2Entry(f *testing.F) {
    // Seed with known L2 entry patterns
    f.Add(uint64(0))                           // Unallocated
    f.Add(uint64(L2EntryZeroFlag))             // Zero cluster
    f.Add(uint64(0x50000 | L2EntryCopied))     // Normal allocated
    f.Add(uint64(L2EntryCompressed | 0x1000))  // Compressed

    f.Fuzz(func(t *testing.T, entry uint64) {
        // Parse entry
        isCompressed := entry&L2EntryCompressed != 0
        isZero := entry&L2EntryZeroFlag != 0
        offset := entry & L2EntryOffsetMask

        // Sanity checks
        if isCompressed && isZero {
            // Invalid combination - should be handled gracefully
        }

        // Verify offset alignment for normal entries
        if !isCompressed && offset != 0 {
            if offset&0x1ff != 0 {
                // Unaligned - should trigger validation error
            }
        }
    })
}
```

#### C. Read/Write Offset Fuzzer

```go
func FuzzReadWrite(f *testing.F) {
    // Create test image once
    img := createTestImage(t, 10*1024*1024) // 10MB
    defer img.Close()

    f.Add(int64(0), []byte("test"))
    f.Add(int64(65535), []byte("boundary"))
    f.Add(int64(65536), []byte("cluster start"))

    f.Fuzz(func(t *testing.T, offset int64, data []byte) {
        if len(data) == 0 || len(data) > 1024*1024 {
            return // Skip empty or huge writes
        }
        if offset < 0 {
            return // Skip invalid offsets
        }

        n, err := img.WriteAt(data, offset)
        if err != nil {
            return // Write failed (e.g., out of bounds)
        }

        // Read back and verify
        readBuf := make([]byte, n)
        _, err = img.ReadAt(readBuf, offset)
        if err != nil {
            t.Fatalf("Read failed after successful write: %v", err)
        }

        if !bytes.Equal(data[:n], readBuf) {
            t.Fatalf("Data mismatch at offset %d", offset)
        }
    })
}
```

#### D. Full Image Fuzzer

```go
func FuzzFullImage(f *testing.F) {
    // Seed with valid QCOW2 images
    f.Add(readFile("testdata/minimal.qcow2"))
    f.Add(readFile("testdata/with_data.qcow2"))
    f.Add(readFile("testdata/with_backing.qcow2"))

    f.Fuzz(func(t *testing.T, imageData []byte) {
        // Write to temp file
        tmpFile := writeTempFile(t, imageData)

        // Try to open
        img, err := Open(tmpFile)
        if err != nil {
            return // Invalid image is expected
        }
        defer img.Close()

        // If opened successfully, basic operations shouldn't panic
        _ = img.Size()
        _ = img.ClusterSize()
        _ = img.IsDirty()

        // Try to read first cluster
        buf := make([]byte, 512)
        img.ReadAt(buf, 0)

        // Check should not panic
        img.Check()
    })
}
```

### 2.3 Differential Fuzzing

Compare our behavior against QEMU's for the same inputs:

```go
func FuzzDifferential(f *testing.F) {
    f.Fuzz(func(t *testing.T, seed int64, ops []byte) {
        // Create identical images
        ourImage := createTestImage(t, "ours.qcow2", 10*1024*1024)
        qemuImage := createQemuImage(t, "qemu.qcow2", 10*1024*1024)

        // Apply same operations to both
        rng := rand.New(rand.NewSource(seed))
        for _, op := range ops {
            offset := rng.Int63n(10 * 1024 * 1024)
            size := rng.Intn(65536)
            data := randomBytes(rng, size)

            switch op % 3 {
            case 0: // Write
                ourImage.WriteAt(data, offset)
                qemuWrite(qemuImage, data, offset)
            case 1: // Read
                ourBuf := make([]byte, size)
                qemuBuf := make([]byte, size)
                ourImage.ReadAt(ourBuf, offset)
                qemuRead(qemuImage, qemuBuf, offset)
                if !bytes.Equal(ourBuf, qemuBuf) {
                    t.Fatalf("Read mismatch at %d", offset)
                }
            case 2: // Zero
                ourImage.WriteZeroAt(offset, int64(size))
                qemuZero(qemuImage, offset, size)
            }
        }

        // Final comparison
        compareImages(t, ourImage, qemuImage)
    })
}
```

### 2.4 Fuzz Configuration

```makefile
# Makefile targets for fuzzing

.PHONY: fuzz-quick fuzz-medium fuzz-full fuzz-overnight

# Quick fuzz: 1 minute each target (CI)
fuzz-quick:
	go test -fuzz=FuzzParseHeader -fuzztime=1m ./...
	go test -fuzz=FuzzL2Entry -fuzztime=1m ./...
	go test -fuzz=FuzzReadWrite -fuzztime=1m ./...

# Medium fuzz: 10 minutes each (PR merge)
fuzz-medium:
	go test -fuzz=FuzzParseHeader -fuzztime=10m ./...
	go test -fuzz=FuzzL2Entry -fuzztime=10m ./...
	go test -fuzz=FuzzReadWrite -fuzztime=10m ./...
	go test -fuzz=FuzzFullImage -fuzztime=10m ./...

# Full fuzz: 1 hour each (nightly)
fuzz-full:
	go test -fuzz=FuzzParseHeader -fuzztime=1h ./...
	go test -fuzz=FuzzL2Entry -fuzztime=1h ./...
	go test -fuzz=FuzzReadWrite -fuzztime=1h ./...
	go test -fuzz=FuzzFullImage -fuzztime=1h ./...
	go test -fuzz=FuzzDifferential -fuzztime=1h ./...

# Overnight fuzz: 8 hours (weekend)
fuzz-overnight:
	go test -fuzz=FuzzFullImage -fuzztime=8h -parallel=4 ./...
```

---

## 3. Crash Recovery Testing

### 3.1 Crash Point Identification

Critical points where crash could cause corruption:

```
| Crash Point | Risk Level | Expected Recovery |
|-------------|------------|-------------------|
| During L2 table allocation | High | Orphaned cluster, repairable |
| After L2 write, before L1 update | High | L2 unreachable, repairable |
| During data cluster write | Medium | Partial data, L2 intact |
| During refcount update | High | Refcount mismatch, repairable |
| During header update (dirty bit) | Low | Image may appear clean |
| During backing file COW | High | Partial cluster, verify backing |
```

### 3.2 Crash Simulation Approaches

#### A. Process Kill Method (Simple)

```go
func TestCrashDuringWrite(t *testing.T) {
    dir := t.TempDir()
    imagePath := filepath.Join(dir, "test.qcow2")

    // Create image in subprocess that we'll kill
    cmd := exec.Command("go", "run", "./cmd/crash-writer",
        "-image", imagePath,
        "-write-count", "1000",
        "-crash-after", "500")

    cmd.Start()

    // Wait for writes to start
    time.Sleep(100 * time.Millisecond)

    // Kill process mid-write
    cmd.Process.Kill()

    // Verify image is recoverable
    result := runQemuCheck(t, imagePath)
    if result.ExitCode != 0 && !strings.Contains(result.Stderr, "repairable") {
        t.Fatalf("Image not recoverable: %s", result.Stderr)
    }

    // Repair with qemu-img
    repairResult := runQemuRepair(t, imagePath)
    if repairResult.ExitCode != 0 {
        t.Fatalf("Repair failed: %s", repairResult.Stderr)
    }

    // Verify data integrity of completed writes
    img, err := Open(imagePath)
    if err != nil {
        t.Fatalf("Cannot open repaired image: %v", err)
    }
    defer img.Close()

    // Verify first 500 writes succeeded
    verifyWrites(t, img, 0, 500)
}
```

#### B. Failpoint Injection Method (Precise)

```go
// failpoints.go (build tag: failpoint)

var (
    FailBeforeL1Update = false
    FailAfterL2Write   = false
    FailDuringRefcount = false
)

func (img *Image) maybeFailpoint(name string) error {
    switch name {
    case "before_l1_update":
        if FailBeforeL1Update {
            return ErrInjectedCrash
        }
    case "after_l2_write":
        if FailAfterL2Write {
            return ErrInjectedCrash
        }
    case "during_refcount":
        if FailDuringRefcount {
            return ErrInjectedCrash
        }
    }
    return nil
}

// In getClusterForWrite:
func (img *Image) getClusterForWrite(virtOff uint64) (uint64, error) {
    // ... allocate L2 table ...

    // Write L2 table to disk
    if _, err := img.file.WriteAt(zeros, int64(l2TableOff)); err != nil {
        return 0, err
    }

    // FAILPOINT: crash after L2 write but before L1 update
    if err := img.maybeFailpoint("after_l2_write"); err != nil {
        return 0, err
    }

    // Update L1 entry
    // ...
}
```

```go
// crash_test.go
func TestFailpointAfterL2Write(t *testing.T) {
    FailAfterL2Write = true
    defer func() { FailAfterL2Write = false }()

    img := createTestImage(t, 1024*1024)

    // This write should fail at the failpoint
    _, err := img.WriteAt([]byte("test"), 0)
    if !errors.Is(err, ErrInjectedCrash) {
        t.Fatalf("Expected injected crash, got: %v", err)
    }

    img.file.Close() // Simulate crash (don't clean close)

    // Verify recovery
    verifyRecovery(t, img.path)
}
```

#### C. Write Interception Method (Comprehensive)

```go
// interceptor.go - intercepts file writes for crash simulation

type CrashInterceptor struct {
    file        *os.File
    writeCount  int
    crashAfter  int
    crashBefore int
}

func (c *CrashInterceptor) WriteAt(p []byte, off int64) (int, error) {
    c.writeCount++

    if c.crashBefore > 0 && c.writeCount == c.crashBefore {
        return 0, ErrSimulatedCrash
    }

    n, err := c.file.WriteAt(p, off)

    if c.crashAfter > 0 && c.writeCount == c.crashAfter {
        return n, ErrSimulatedCrash
    }

    return n, err
}
```

### 3.3 Recovery Verification Matrix

```go
var crashScenarios = []struct {
    name         string
    failpoint    string
    expectedState string
    qemuRepairable bool
    ourRepairable  bool
}{
    {
        name:         "crash_during_l2_alloc",
        failpoint:    "during_l2_alloc",
        expectedState: "leaked_cluster",
        qemuRepairable: true,
        ourRepairable:  true,
    },
    {
        name:         "crash_after_l2_before_l1",
        failpoint:    "after_l2_write",
        expectedState: "orphaned_l2",
        qemuRepairable: true,
        ourRepairable:  true,
    },
    {
        name:         "crash_during_data_write",
        failpoint:    "during_data_write",
        expectedState: "partial_data",
        qemuRepairable: true,
        ourRepairable:  true,
    },
    {
        name:         "crash_during_refcount",
        failpoint:    "during_refcount",
        expectedState: "refcount_mismatch",
        qemuRepairable: true,
        ourRepairable:  true,
    },
}

func TestCrashRecoveryMatrix(t *testing.T) {
    for _, scenario := range crashScenarios {
        t.Run(scenario.name, func(t *testing.T) {
            img, cleanup := createCrashableImage(t)
            defer cleanup()

            // Inject crash
            injectCrash(img, scenario.failpoint)

            // Attempt operation that will crash
            img.WriteAt([]byte("test"), 0)

            // Simulate unclean close
            img.file.Close()

            // Verify QEMU can repair
            if scenario.qemuRepairable {
                result := runQemuCheck(t, img.path)
                // Should detect issues
                if result.IsClean {
                    t.Log("QEMU found no issues (may be expected)")
                }

                // Repair should succeed
                repairResult := runQemuRepair(t, img.path)
                if repairResult.ExitCode != 0 {
                    t.Errorf("QEMU repair failed")
                }
            }

            // Verify our Repair works
            if scenario.ourRepairable {
                img2, _ := Open(img.path)
                result, err := img2.Repair()
                if err != nil {
                    t.Errorf("Our repair failed: %v", err)
                }
                if !result.IsClean() {
                    t.Errorf("Image not clean after repair")
                }
                img2.Close()
            }
        })
    }
}
```

---

## 4. Stress & Concurrency Testing

### 4.1 Concurrent Access Tests

```go
func TestConcurrentReads(t *testing.T) {
    img := createPopulatedImage(t, 100*1024*1024) // 100MB with data
    defer img.Close()

    var wg sync.WaitGroup
    errors := make(chan error, 100)

    // 100 concurrent readers
    for i := 0; i < 100; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()

            rng := rand.New(rand.NewSource(int64(id)))
            buf := make([]byte, 4096)

            for j := 0; j < 1000; j++ {
                offset := rng.Int63n(100 * 1024 * 1024)
                _, err := img.ReadAt(buf, offset)
                if err != nil && err != io.EOF {
                    errors <- fmt.Errorf("reader %d: %w", id, err)
                    return
                }
            }
        }(i)
    }

    wg.Wait()
    close(errors)

    for err := range errors {
        t.Error(err)
    }
}

func TestConcurrentWrites(t *testing.T) {
    img := createTestImage(t, 100*1024*1024)
    defer img.Close()

    var wg sync.WaitGroup

    // Track what each goroutine writes
    type writeRecord struct {
        offset int64
        data   []byte
    }
    records := make([][]writeRecord, 10)

    // 10 concurrent writers, each to different regions
    for i := 0; i < 10; i++ {
        wg.Add(1)
        records[i] = make([]writeRecord, 0)

        go func(id int) {
            defer wg.Done()

            // Each writer gets a 10MB region
            baseOffset := int64(id) * 10 * 1024 * 1024
            rng := rand.New(rand.NewSource(int64(id)))

            for j := 0; j < 100; j++ {
                offset := baseOffset + rng.Int63n(10*1024*1024-4096)
                data := randomBytes(rng, 4096)

                _, err := img.WriteAt(data, offset)
                if err != nil {
                    t.Errorf("writer %d: %v", id, err)
                    return
                }

                records[id] = append(records[id], writeRecord{offset, data})
            }
        }(i)
    }

    wg.Wait()

    // Verify all writes
    for _, writerRecords := range records {
        for _, rec := range writerRecords {
            buf := make([]byte, len(rec.data))
            img.ReadAt(buf, rec.offset)
            if !bytes.Equal(buf, rec.data) {
                t.Errorf("Data mismatch at offset %d", rec.offset)
            }
        }
    }
}

func TestConcurrentReadWrite(t *testing.T) {
    img := createTestImage(t, 100*1024*1024)
    defer img.Close()

    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    var wg sync.WaitGroup

    // Writers
    for i := 0; i < 5; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            rng := rand.New(rand.NewSource(int64(id)))

            for {
                select {
                case <-ctx.Done():
                    return
                default:
                    offset := rng.Int63n(100 * 1024 * 1024)
                    data := randomBytes(rng, 4096)
                    img.WriteAt(data, offset)
                }
            }
        }(i)
    }

    // Readers
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            rng := rand.New(rand.NewSource(int64(id + 100)))
            buf := make([]byte, 4096)

            for {
                select {
                case <-ctx.Done():
                    return
                default:
                    offset := rng.Int63n(100 * 1024 * 1024)
                    img.ReadAt(buf, offset)
                }
            }
        }(i)
    }

    wg.Wait()

    // Verify image is still valid
    result, err := img.Check()
    if err != nil {
        t.Fatalf("Check failed: %v", err)
    }
    if !result.IsClean() {
        t.Errorf("Image corrupted after concurrent access: %v", result.Errors)
    }
}
```

### 4.2 Resource Exhaustion Tests

```go
func TestManyL2Tables(t *testing.T) {
    // Force allocation of many L2 tables
    img := createTestImage(t, 1024*1024*1024) // 1GB
    defer img.Close()

    // With 64KB clusters, each L2 table covers 512MB
    // So 1GB needs at least 2 L2 tables
    // Write to force allocation across many L2 entries

    data := []byte("x")
    for i := int64(0); i < 1024; i++ {
        offset := i * 1024 * 1024 // Every 1MB
        _, err := img.WriteAt(data, offset)
        if err != nil {
            t.Fatalf("Write at %d failed: %v", offset, err)
        }
    }

    // Verify L2 cache isn't causing issues
    result, _ := img.Check()
    if !result.IsClean() {
        t.Errorf("Image has issues: %v", result.Errors)
    }
}

func TestDeepBackingChain(t *testing.T) {
    dir := t.TempDir()

    // Create chain of 10 backing files
    var prevPath string
    for i := 0; i < 10; i++ {
        path := filepath.Join(dir, fmt.Sprintf("layer%d.qcow2", i))

        var img *Image
        var err error
        if prevPath == "" {
            img, err = CreateSimple(path, 10*1024*1024)
        } else {
            img, err = CreateOverlay(path, prevPath)
        }
        if err != nil {
            t.Fatalf("Create layer %d: %v", i, err)
        }

        // Write some data to each layer
        data := []byte(fmt.Sprintf("layer %d data", i))
        img.WriteAt(data, int64(i*4096))
        img.Close()

        prevPath = path
    }

    // Open final layer and verify all data
    img, err := Open(prevPath)
    if err != nil {
        t.Fatalf("Open chain: %v", err)
    }
    defer img.Close()

    for i := 0; i < 10; i++ {
        expected := []byte(fmt.Sprintf("layer %d data", i))
        buf := make([]byte, len(expected))
        img.ReadAt(buf, int64(i*4096))
        if !bytes.Equal(buf, expected) {
            t.Errorf("Layer %d data mismatch", i)
        }
    }
}

func TestLargeWrite(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping large write test in short mode")
    }

    img := createTestImage(t, 10*1024*1024*1024) // 10GB virtual
    defer img.Close()

    // Write 1GB of data
    data := make([]byte, 1024*1024) // 1MB buffer
    for i := range data {
        data[i] = byte(i)
    }

    for i := 0; i < 1024; i++ {
        offset := int64(i) * int64(len(data))
        _, err := img.WriteAt(data, offset)
        if err != nil {
            t.Fatalf("Write at %d failed: %v", offset, err)
        }
    }

    // Verify
    result, _ := img.Check()
    if !result.IsClean() {
        t.Errorf("Image issues after large write: %v", result.Errors)
    }
}
```

### 4.3 Race Detector Tests

```makefile
# Run with race detector
test-race:
	go test -race -count=1 ./...

# Extended race testing
test-race-stress:
	go test -race -count=10 -parallel=4 ./...
```

---

## 5. Edge Case Testing

### 5.1 Boundary Conditions

```go
func TestClusterBoundaryWrites(t *testing.T) {
    img := createTestImage(t, 1024*1024)
    defer img.Close()

    clusterSize := int64(img.ClusterSize())

    cases := []struct {
        name   string
        offset int64
        size   int
    }{
        {"cluster_start", 0, 100},
        {"cluster_end", clusterSize - 100, 100},
        {"cross_boundary", clusterSize - 50, 100},
        {"exact_cluster", 0, int(clusterSize)},
        {"multi_cluster", 0, int(clusterSize) * 3},
        {"one_byte_start", 0, 1},
        {"one_byte_end", clusterSize - 1, 1},
        {"one_byte_cross", clusterSize - 1, 2},
    }

    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            data := randomBytes(nil, tc.size)

            n, err := img.WriteAt(data, tc.offset)
            if err != nil {
                t.Fatalf("Write failed: %v", err)
            }
            if n != tc.size {
                t.Fatalf("Short write: %d < %d", n, tc.size)
            }

            buf := make([]byte, tc.size)
            n, err = img.ReadAt(buf, tc.offset)
            if err != nil {
                t.Fatalf("Read failed: %v", err)
            }
            if !bytes.Equal(buf, data) {
                t.Fatalf("Data mismatch")
            }
        })
    }
}

func TestVirtualSizeBoundary(t *testing.T) {
    size := int64(1024 * 1024)
    img := createTestImage(t, uint64(size))
    defer img.Close()

    // Write at last valid offset
    lastByte := size - 1
    _, err := img.WriteAt([]byte{0xAA}, lastByte)
    if err != nil {
        t.Errorf("Write at last byte failed: %v", err)
    }

    // Write beyond should fail
    _, err = img.WriteAt([]byte{0xBB}, size)
    if err == nil {
        t.Error("Write beyond size should fail")
    }

    // Read at last valid offset
    buf := make([]byte, 1)
    _, err = img.ReadAt(buf, lastByte)
    if err != nil {
        t.Errorf("Read at last byte failed: %v", err)
    }
    if buf[0] != 0xAA {
        t.Errorf("Wrong data at last byte")
    }

    // Read beyond should return EOF
    _, err = img.ReadAt(buf, size)
    if err != io.EOF {
        t.Errorf("Read beyond size: got %v, want EOF", err)
    }
}

func TestZeroLengthOperations(t *testing.T) {
    img := createTestImage(t, 1024*1024)
    defer img.Close()

    // Zero-length write should succeed
    n, err := img.WriteAt([]byte{}, 0)
    if err != nil {
        t.Errorf("Zero-length write failed: %v", err)
    }
    if n != 0 {
        t.Errorf("Zero-length write returned %d", n)
    }

    // Zero-length read should succeed
    n, err = img.ReadAt([]byte{}, 0)
    if err != nil {
        t.Errorf("Zero-length read failed: %v", err)
    }
    if n != 0 {
        t.Errorf("Zero-length read returned %d", n)
    }
}

func TestNegativeOffset(t *testing.T) {
    img := createTestImage(t, 1024*1024)
    defer img.Close()

    _, err := img.WriteAt([]byte("test"), -1)
    if err == nil {
        t.Error("Negative offset write should fail")
    }

    buf := make([]byte, 10)
    _, err = img.ReadAt(buf, -1)
    if err == nil {
        t.Error("Negative offset read should fail")
    }
}
```

### 5.2 Empty and Minimal Images

```go
func TestEmptyImage(t *testing.T) {
    img := createTestImage(t, 1024*1024)
    defer img.Close()

    // Read from empty image should return zeros
    buf := make([]byte, 4096)
    _, err := img.ReadAt(buf, 0)
    if err != nil {
        t.Fatalf("Read from empty image: %v", err)
    }

    for i, b := range buf {
        if b != 0 {
            t.Fatalf("Non-zero byte at %d: %d", i, b)
        }
    }

    // Check should pass
    result, err := img.Check()
    if err != nil {
        t.Fatalf("Check failed: %v", err)
    }
    if !result.IsClean() {
        t.Errorf("Empty image not clean: %v", result.Errors)
    }
}

func TestMinimalVirtualSize(t *testing.T) {
    // Minimum meaningful size: 1 cluster
    sizes := []uint64{
        512,   // Less than min cluster
        1024,  // Small
        65536, // One default cluster
    }

    for _, size := range sizes {
        t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
            img, err := CreateSimple(
                filepath.Join(t.TempDir(), "test.qcow2"),
                size,
            )
            if err != nil {
                t.Fatalf("Create failed: %v", err)
            }
            defer img.Close()

            if img.Size() != int64(size) {
                t.Errorf("Size = %d, want %d", img.Size(), size)
            }
        })
    }
}

func TestAllClusterSizes(t *testing.T) {
    for bits := uint32(9); bits <= 21; bits++ {
        t.Run(fmt.Sprintf("cluster_%d", 1<<bits), func(t *testing.T) {
            img, err := Create(
                filepath.Join(t.TempDir(), "test.qcow2"),
                CreateOptions{
                    Size:        10 * 1024 * 1024,
                    ClusterBits: bits,
                },
            )
            if err != nil {
                t.Fatalf("Create with cluster_bits=%d: %v", bits, err)
            }
            defer img.Close()

            if img.ClusterSize() != int(1<<bits) {
                t.Errorf("ClusterSize = %d, want %d",
                    img.ClusterSize(), 1<<bits)
            }

            // Write and verify
            data := []byte("test data for cluster size verification")
            img.WriteAt(data, 0)

            buf := make([]byte, len(data))
            img.ReadAt(buf, 0)
            if !bytes.Equal(buf, data) {
                t.Error("Data mismatch")
            }
        })
    }
}
```

### 5.3 Invalid Input Handling

```go
func TestCorruptHeader(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "corrupt.qcow2")

    // Create valid image
    img, _ := CreateSimple(path, 1024*1024)
    img.Close()

    // Corrupt magic number
    f, _ := os.OpenFile(path, os.O_RDWR, 0)
    f.WriteAt([]byte{0, 0, 0, 0}, 0) // Corrupt magic
    f.Close()

    // Should fail to open
    _, err := Open(path)
    if err == nil {
        t.Error("Should reject corrupt magic")
    }
}

func TestInvalidVersion(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "badversion.qcow2")

    img, _ := CreateSimple(path, 1024*1024)
    img.Close()

    // Set version to 99
    f, _ := os.OpenFile(path, os.O_RDWR, 0)
    var ver = make([]byte, 4)
    binary.BigEndian.PutUint32(ver, 99)
    f.WriteAt(ver, 4)
    f.Close()

    _, err := Open(path)
    if err == nil {
        t.Error("Should reject invalid version")
    }
}

func TestUnknownIncompatFeatures(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "unknown.qcow2")

    img, _ := CreateSimple(path, 1024*1024)
    img.Close()

    // Set unknown incompatible feature bit
    f, _ := os.OpenFile(path, os.O_RDWR, 0)
    var features = make([]byte, 8)
    binary.BigEndian.PutUint64(features, 1<<63) // Unknown bit
    f.WriteAt(features, 72)
    f.Close()

    _, err := Open(path)
    if err == nil {
        t.Error("Should reject unknown incompatible features")
    }
}
```

---

## 6. Data Integrity Testing

### 6.1 Checksum Verification

```go
func TestDataIntegrityWithChecksum(t *testing.T) {
    img := createTestImage(t, 100*1024*1024)
    defer img.Close()

    // Write blocks with checksums
    type block struct {
        offset   int64
        data     []byte
        checksum [32]byte // SHA-256
    }

    var blocks []block
    rng := rand.New(rand.NewSource(42))

    for i := 0; i < 100; i++ {
        offset := rng.Int63n(100*1024*1024 - 4096)
        data := randomBytes(rng, 4096)

        b := block{
            offset:   offset,
            data:     data,
            checksum: sha256.Sum256(data),
        }
        blocks = append(blocks, b)

        img.WriteAt(data, offset)
    }

    // Close and reopen
    img.Close()
    img2, _ := Open(img.path)
    defer img2.Close()

    // Verify all checksums
    for i, b := range blocks {
        buf := make([]byte, len(b.data))
        img2.ReadAt(buf, b.offset)

        actual := sha256.Sum256(buf)
        if actual != b.checksum {
            t.Errorf("Block %d checksum mismatch at offset %d", i, b.offset)
        }
    }
}
```

### 6.2 Pattern-Based Verification

```go
var testPatterns = []struct {
    name    string
    pattern byte
}{
    {"zeros", 0x00},
    {"ones", 0xFF},
    {"alternating_01", 0x55},
    {"alternating_10", 0xAA},
    {"sequential", 0}, // Special: data[i] = byte(i)
}

func TestPatternIntegrity(t *testing.T) {
    for _, pat := range testPatterns {
        t.Run(pat.name, func(t *testing.T) {
            img := createTestImage(t, 1024*1024)
            defer img.Close()

            data := make([]byte, 65536)
            for i := range data {
                if pat.name == "sequential" {
                    data[i] = byte(i)
                } else {
                    data[i] = pat.pattern
                }
            }

            // Write pattern
            img.WriteAt(data, 0)

            // Read and verify
            buf := make([]byte, len(data))
            img.ReadAt(buf, 0)

            if !bytes.Equal(buf, data) {
                t.Error("Pattern mismatch")
            }
        })
    }
}
```

### 6.3 Compressibility-Aware Testing

```go
func TestCompressibleData(t *testing.T) {
    img := createTestImage(t, 10*1024*1024)
    defer img.Close()

    // Highly compressible: runs of same byte
    compressible := bytes.Repeat([]byte("A"), 65536)
    img.WriteAt(compressible, 0)

    // Incompressible: random data
    incompressible := randomBytes(nil, 65536)
    img.WriteAt(incompressible, 65536)

    // Verify both
    buf := make([]byte, 65536)

    img.ReadAt(buf, 0)
    if !bytes.Equal(buf, compressible) {
        t.Error("Compressible data corrupted")
    }

    img.ReadAt(buf, 65536)
    if !bytes.Equal(buf, incompressible) {
        t.Error("Incompressible data corrupted")
    }
}
```

---

## 7. Feature-Specific Testing

### 7.1 Backing File Tests

```go
func TestBackingFileReadThrough(t *testing.T) {
    dir := t.TempDir()

    // Create base with data
    basePath := filepath.Join(dir, "base.qcow2")
    base, _ := CreateSimple(basePath, 10*1024*1024)
    base.WriteAt([]byte("base data"), 0)
    base.Close()

    // Create overlay
    overlayPath := filepath.Join(dir, "overlay.qcow2")
    overlay, _ := CreateOverlay(overlayPath, basePath)
    defer overlay.Close()

    // Read should see base data
    buf := make([]byte, 9)
    overlay.ReadAt(buf, 0)
    if string(buf) != "base data" {
        t.Errorf("Expected 'base data', got '%s'", buf)
    }
}

func TestBackingFileCOW(t *testing.T) {
    dir := t.TempDir()

    // Create base
    basePath := filepath.Join(dir, "base.qcow2")
    base, _ := CreateSimple(basePath, 10*1024*1024)
    originalData := []byte("original base data here")
    base.WriteAt(originalData, 0)
    base.Close()

    // Create overlay and write
    overlayPath := filepath.Join(dir, "overlay.qcow2")
    overlay, _ := CreateOverlay(overlayPath, basePath)

    // Partial overwrite
    overlay.WriteAt([]byte("MODIFIED"), 9)
    overlay.Close()

    // Verify overlay has modified data
    overlay2, _ := Open(overlayPath)
    buf := make([]byte, len(originalData))
    overlay2.ReadAt(buf, 0)
    overlay2.Close()

    expected := []byte("original MODIFIED here")
    if !bytes.Equal(buf, expected) {
        t.Errorf("COW failed: got '%s', want '%s'", buf, expected)
    }

    // Verify base is unchanged
    base2, _ := Open(basePath)
    base2.ReadAt(buf, 0)
    base2.Close()

    if !bytes.Equal(buf[:len(originalData)], originalData) {
        t.Error("Base was modified!")
    }
}
```

### 7.2 Lazy Refcounts Tests

```go
func TestLazyRefcountsRecovery(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "lazy.qcow2")

    // Create with lazy refcounts
    img, _ := Create(path, CreateOptions{
        Size:          10 * 1024 * 1024,
        LazyRefcounts: true,
    })

    // Write data
    data := []byte("test data for lazy refcounts")
    img.WriteAt(data, 0)

    // Don't close cleanly - simulate crash
    img.file.Close()

    // Reopen - should trigger refcount rebuild
    img2, err := Open(path)
    if err != nil {
        t.Fatalf("Failed to reopen: %v", err)
    }
    defer img2.Close()

    // Verify data survived
    buf := make([]byte, len(data))
    img2.ReadAt(buf, 0)
    if !bytes.Equal(buf, data) {
        t.Error("Data lost after lazy refcount recovery")
    }

    // Check should pass
    result, _ := img2.Check()
    if !result.IsClean() {
        t.Errorf("Image not clean: %v", result.Errors)
    }
}
```

### 7.3 Zero Cluster Tests

```go
func TestZeroClusterEfficiency(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "zeros.qcow2")

    img, _ := CreateSimple(path, 100*1024*1024)

    // Write data then zero it
    data := randomBytes(nil, 65536)
    img.WriteAt(data, 0)

    // Get file size before zeroing
    info, _ := img.file.Stat()
    sizeBefore := info.Size()

    // Zero with WriteZeroAt (should use zero flag, not allocate)
    img.WriteZeroAt(65536, 65536*10)

    // Get file size after
    info, _ = img.file.Stat()
    sizeAfter := info.Size()

    // File shouldn't grow much (zero clusters don't need storage)
    if sizeAfter > sizeBefore+65536 {
        t.Errorf("File grew too much: %d -> %d", sizeBefore, sizeAfter)
    }

    img.Close()

    // Verify qemu sees zero clusters
    result := runQemuMap(t, path)
    // Should show zero regions
    if !strings.Contains(result.Stdout, "zero") {
        t.Log("qemu-img map doesn't show zero clusters (may be expected)")
    }
}
```

### 7.4 Compression Tests

```go
func TestReadCompressedCluster(t *testing.T) {
    dir := t.TempDir()

    // Create uncompressed image with data
    uncompPath := filepath.Join(dir, "uncomp.qcow2")
    img, _ := CreateSimple(uncompPath, 10*1024*1024)

    // Write compressible data
    data := bytes.Repeat([]byte("COMPRESSIBLE"), 5000)
    img.WriteAt(data, 0)
    img.Close()

    // Compress with qemu-img
    compPath := filepath.Join(dir, "comp.qcow2")
    exec.Command("qemu-img", "convert", "-c", "-f", "qcow2",
        "-O", "qcow2", uncompPath, compPath).Run()

    // Read compressed image with our library
    compImg, err := Open(compPath)
    if err != nil {
        t.Fatalf("Open compressed: %v", err)
    }
    defer compImg.Close()

    buf := make([]byte, len(data))
    compImg.ReadAt(buf, 0)

    if !bytes.Equal(buf, data) {
        t.Error("Compressed data mismatch")
    }
}
```

---

## 8. Performance Regression Testing

### 8.1 Benchmark Suite

```go
func BenchmarkSequentialWrite(b *testing.B) {
    img := createBenchImage(b, 1024*1024*1024)
    defer img.Close()

    data := make([]byte, 65536)
    b.SetBytes(65536)
    b.ResetTimer()

    for i := 0; i < b.N; i++ {
        offset := int64(i%1000) * 65536
        img.WriteAt(data, offset)
    }
}

func BenchmarkRandomWrite(b *testing.B) {
    img := createBenchImage(b, 1024*1024*1024)
    defer img.Close()

    rng := rand.New(rand.NewSource(42))
    data := make([]byte, 4096)
    b.SetBytes(4096)
    b.ResetTimer()

    for i := 0; i < b.N; i++ {
        offset := rng.Int63n(1024 * 1024 * 1024)
        img.WriteAt(data, offset)
    }
}

func BenchmarkSequentialRead(b *testing.B) {
    img := createPopulatedBenchImage(b, 1024*1024*1024)
    defer img.Close()

    buf := make([]byte, 65536)
    b.SetBytes(65536)
    b.ResetTimer()

    for i := 0; i < b.N; i++ {
        offset := int64(i%1000) * 65536
        img.ReadAt(buf, offset)
    }
}

func BenchmarkRandomRead(b *testing.B) {
    img := createPopulatedBenchImage(b, 1024*1024*1024)
    defer img.Close()

    rng := rand.New(rand.NewSource(42))
    buf := make([]byte, 4096)
    b.SetBytes(4096)
    b.ResetTimer()

    for i := 0; i < b.N; i++ {
        offset := rng.Int63n(1024 * 1024 * 1024)
        img.ReadAt(buf, offset)
    }
}

func BenchmarkL2CacheHit(b *testing.B) {
    img := createBenchImage(b, 100*1024*1024)
    defer img.Close()

    // Prime cache with one cluster
    img.WriteAt([]byte("x"), 0)

    buf := make([]byte, 4096)
    b.ResetTimer()

    // All reads hit same L2 table
    for i := 0; i < b.N; i++ {
        img.ReadAt(buf, 0)
    }
}

func BenchmarkL2CacheMiss(b *testing.B) {
    img := createBenchImage(b, 10*1024*1024*1024) // 10GB
    defer img.Close()

    // Write to many different L2 tables
    for i := 0; i < 100; i++ {
        img.WriteAt([]byte("x"), int64(i)*512*1024*1024)
    }

    buf := make([]byte, 4096)
    b.ResetTimer()

    // Reads miss cache (different L2 tables)
    for i := 0; i < b.N; i++ {
        offset := int64(i%100) * 512 * 1024 * 1024
        img.ReadAt(buf, offset)
    }
}
```

### 8.2 Comparison Benchmarks

```go
func BenchmarkCompareWithQemu(b *testing.B) {
    // This benchmark compares our performance with qemu-io
    // Run manually and compare results

    sizes := []int64{4096, 65536, 1024 * 1024}

    for _, size := range sizes {
        b.Run(fmt.Sprintf("go-qcow2_%d", size), func(b *testing.B) {
            img := createBenchImage(b, 1024*1024*1024)
            defer img.Close()

            data := make([]byte, size)
            b.SetBytes(size)
            b.ResetTimer()

            for i := 0; i < b.N; i++ {
                img.WriteAt(data, 0)
            }
        })
    }
}
```

---

## 9. Compatibility Matrix

### 9.1 Test Matrix Definition

```go
var compatibilityMatrix = []struct {
    goVersion   string
    qemuVersion string
    os          string
    arch        string
}{
    // Go versions
    {"1.21", "8.2", "linux", "amd64"},
    {"1.22", "8.2", "linux", "amd64"},
    {"1.23", "8.2", "linux", "amd64"},

    // QEMU versions
    {"1.23", "5.2", "linux", "amd64"},
    {"1.23", "7.2", "linux", "amd64"},
    {"1.23", "8.2", "linux", "amd64"},
    {"1.23", "9.0", "linux", "amd64"},

    // Architectures
    {"1.23", "8.2", "linux", "arm64"},

    // Operating systems (future)
    // {"1.23", "8.2", "darwin", "arm64"},
    // {"1.23", "8.2", "windows", "amd64"},
}
```

### 9.2 Feature Support Matrix

```
| Feature              | v2 | v3 | QEMU 5.x | QEMU 8.x |
|----------------------|----|-------|----------|----------|
| Basic read/write     | ✓  | ✓     | ✓        | ✓        |
| Backing files        | ✓  | ✓     | ✓        | ✓        |
| Lazy refcounts       | -  | ✓     | ✓        | ✓        |
| Zero clusters        | -  | ✓     | ✓        | ✓        |
| Compression (deflate)| ✓  | ✓     | ✓        | ✓        |
| Compression (zstd)   | -  | ✓     | -        | ✓        |
| Extended L2          | -  | ✓     | -        | ✓        |
```

---

## 10. Test Infrastructure

### 10.1 Docker Test Environment

```dockerfile
# Dockerfile.test
FROM golang:1.23

# Install QEMU tools
RUN apt-get update && apt-get install -y \
    qemu-utils \
    && rm -rf /var/lib/apt/lists/*

# Multiple QEMU versions (build from source for specific versions)
# Or use pre-built images

WORKDIR /app
COPY . .

RUN go mod download

CMD ["make", "test-all"]
```

```yaml
# docker-compose.test.yml
version: '3.8'

services:
  test-qemu52:
    build:
      context: .
      dockerfile: Dockerfile.test
      args:
        QEMU_VERSION: "5.2.0"
    volumes:
      - .:/app
    command: make test-qemu-interop

  test-qemu72:
    build:
      context: .
      dockerfile: Dockerfile.test
      args:
        QEMU_VERSION: "7.2.0"
    volumes:
      - .:/app
    command: make test-qemu-interop

  test-qemu82:
    build:
      context: .
      dockerfile: Dockerfile.test
      args:
        QEMU_VERSION: "8.2.0"
    volumes:
      - .:/app
    command: make test-qemu-interop
```

### 10.2 Test Helpers

```go
// testutil/helpers.go

package testutil

import (
    "os"
    "os/exec"
    "path/filepath"
    "testing"
)

// QemuResult holds qemu command output
type QemuResult struct {
    ExitCode int
    Stdout   string
    Stderr   string
    IsClean  bool
}

// RunQemuCheck runs qemu-img check on an image
func RunQemuCheck(t *testing.T, path string) QemuResult {
    t.Helper()

    cmd := exec.Command("qemu-img", "check", path)
    stdout, err := cmd.Output()

    result := QemuResult{
        Stdout: string(stdout),
    }

    if err != nil {
        if exitErr, ok := err.(*exec.ExitError); ok {
            result.ExitCode = exitErr.ExitCode()
            result.Stderr = string(exitErr.Stderr)
        }
    }

    result.IsClean = result.ExitCode == 0
    return result
}

// RunQemuRepair runs qemu-img check -r all
func RunQemuRepair(t *testing.T, path string) QemuResult {
    t.Helper()

    cmd := exec.Command("qemu-img", "check", "-r", "all", path)
    stdout, err := cmd.Output()

    result := QemuResult{
        Stdout: string(stdout),
    }

    if err != nil {
        if exitErr, ok := err.(*exec.ExitError); ok {
            result.ExitCode = exitErr.ExitCode()
            result.Stderr = string(exitErr.Stderr)
        }
    }

    return result
}

// RunQemuIO runs qemu-io commands
func RunQemuIO(t *testing.T, path string, cmds ...string) QemuResult {
    t.Helper()

    args := []string{}
    for _, c := range cmds {
        args = append(args, "-c", c)
    }
    args = append(args, path)

    cmd := exec.Command("qemu-io", args...)
    stdout, err := cmd.Output()

    result := QemuResult{
        Stdout: string(stdout),
    }

    if err != nil {
        if exitErr, ok := err.(*exec.ExitError); ok {
            result.ExitCode = exitErr.ExitCode()
            result.Stderr = string(exitErr.Stderr)
        }
    }

    return result
}

// CreateQemuImage creates an image using qemu-img
func CreateQemuImage(t *testing.T, path string, size string, opts ...string) {
    t.Helper()

    args := []string{"create", "-f", "qcow2"}
    args = append(args, opts...)
    args = append(args, path, size)

    cmd := exec.Command("qemu-img", args...)
    if err := cmd.Run(); err != nil {
        t.Fatalf("qemu-img create failed: %v", err)
    }
}

// RandomBytes generates deterministic random bytes
func RandomBytes(seed int64, size int) []byte {
    rng := rand.New(rand.NewSource(seed))
    data := make([]byte, size)
    rng.Read(data)
    return data
}
```

### 10.3 Golden File Management

```go
// golden/golden.go

package golden

import (
    "os"
    "path/filepath"
    "testing"
)

// Update controls whether to update golden files
var Update = os.Getenv("UPDATE_GOLDEN") != ""

// Compare compares output against golden file
func Compare(t *testing.T, name string, actual []byte) {
    t.Helper()

    goldenPath := filepath.Join("testdata", "golden", name)

    if Update {
        os.MkdirAll(filepath.Dir(goldenPath), 0755)
        os.WriteFile(goldenPath, actual, 0644)
        return
    }

    expected, err := os.ReadFile(goldenPath)
    if err != nil {
        t.Fatalf("Read golden file %s: %v", goldenPath, err)
    }

    if !bytes.Equal(actual, expected) {
        t.Errorf("Output differs from golden file %s", name)
        // Optionally show diff
    }
}
```

---

## 11. CI/CD Integration

### 11.1 GitHub Actions Workflow

```yaml
# .github/workflows/test.yml
name: Test Suite

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        go-version: ['1.21', '1.22', '1.23']

    steps:
      - uses: actions/checkout@v4

      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version: ${{ matrix.go-version }}

      - name: Run unit tests
        run: make test

      - name: Run race detector
        run: make test-race

  qemu-interop:
    runs-on: ubuntu-latest
    needs: unit-tests

    steps:
      - uses: actions/checkout@v4

      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version: '1.23'

      - name: Install QEMU
        run: sudo apt-get install -y qemu-utils

      - name: Run QEMU interop tests
        run: make test-qemu-interop

  fuzz-quick:
    runs-on: ubuntu-latest
    needs: unit-tests

    steps:
      - uses: actions/checkout@v4

      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version: '1.23'

      - name: Run quick fuzz tests
        run: make fuzz-quick

  benchmarks:
    runs-on: ubuntu-latest
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'

    steps:
      - uses: actions/checkout@v4

      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version: '1.23'

      - name: Run benchmarks
        run: make bench | tee benchmark-results.txt

      - name: Upload benchmark results
        uses: actions/upload-artifact@v4
        with:
          name: benchmark-results
          path: benchmark-results.txt
```

### 11.2 Nightly Testing Workflow

```yaml
# .github/workflows/nightly.yml
name: Nightly Tests

on:
  schedule:
    - cron: '0 3 * * *'  # 3 AM UTC daily
  workflow_dispatch:

jobs:
  extended-fuzz:
    runs-on: ubuntu-latest
    timeout-minutes: 480  # 8 hours

    steps:
      - uses: actions/checkout@v4

      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version: '1.23'

      - name: Run extended fuzz tests
        run: make fuzz-full

      - name: Upload crash artifacts
        if: failure()
        uses: actions/upload-artifact@v4
        with:
          name: fuzz-crashes
          path: testdata/fuzz/**/crash*

  stress-tests:
    runs-on: ubuntu-latest
    timeout-minutes: 60

    steps:
      - uses: actions/checkout@v4

      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version: '1.23'

      - name: Install QEMU
        run: sudo apt-get install -y qemu-utils

      - name: Run stress tests
        run: make test-stress

  multi-qemu-version:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        qemu-version: ['5.2', '7.2', '8.2', '9.0']

    steps:
      - uses: actions/checkout@v4

      - name: Set up test environment
        run: |
          docker build -t qcow2-test-${{ matrix.qemu-version }} \
            --build-arg QEMU_VERSION=${{ matrix.qemu-version }} .

      - name: Run QEMU version tests
        run: |
          docker run qcow2-test-${{ matrix.qemu-version }} \
            make test-qemu-interop
```

---

## 12. Implementation Roadmap

### Phase 1: Foundation ✅ COMPLETE

- [x] Set up test infrastructure
  - [x] Create `testutil` package with helpers
  - [ ] Set up Docker test environment
  - [x] Create Makefile targets
  - [ ] Set up GitHub Actions CI

- [x] Basic QEMU interop tests
  - [x] `qemu-img check` validation
  - [x] Simple read/write round-trip
  - [x] Version 2/3 compatibility

### Phase 2: Comprehensive Coverage ✅ MOSTLY COMPLETE

- [x] Complete QEMU interop matrix
  - [x] All cluster sizes (4K, 16K, 64K, 256K, 1M)
  - [x] Feature combinations (lazy refcounts, backing files, compression)
  - [ ] Multiple QEMU versions (need Docker setup)

- [x] Edge case tests (via existing unit tests)
  - [x] Boundary conditions
  - [x] Invalid input handling
  - [x] Empty/minimal images

- [x] Data integrity tests
  - [x] Pattern testing (via interop tests)
  - [x] Round-trip verification

### Phase 3: Fuzzing & Crash Testing ⏳ IN PROGRESS

- [x] Implement fuzz targets
  - [x] Header fuzzer (FuzzParseHeader)
  - [x] L2 entry fuzzer (FuzzL2Entry)
  - [x] Read/write fuzzer (FuzzReadWrite)
  - [x] Full image fuzzer (FuzzFullImage)
  - [x] Refcount entry fuzzer (FuzzRefcountEntry)

- [ ] Crash recovery tests
  - [ ] Failpoint injection
  - [ ] Process kill tests
  - [x] QEMU repair verification (TestQemuInterop_LazyRefcountsRecovery)

### Phase 4: Stress & Performance ⏳ PENDING

- [ ] Stress tests
  - [ ] Concurrent access
  - [ ] Resource exhaustion
  - [ ] Large operations

- [ ] Performance baselines
  - [ ] Establish benchmarks
  - [ ] Document expected performance
  - [ ] Set up regression detection

### Phase 5: Polish & Documentation ⏳ PENDING

- [ ] Nightly test workflow
- [ ] Performance comparison with QEMU
- [ ] Test documentation
- [ ] Coverage report generation

---

## Appendix A: Reference Commands

### QEMU Commands Quick Reference

```bash
# Image operations
qemu-img create -f qcow2 test.qcow2 1G
qemu-img create -f qcow2 -o cluster_size=4096 test.qcow2 1G
qemu-img create -f qcow2 -o lazy_refcounts=on test.qcow2 1G
qemu-img create -f qcow2 -b base.qcow2 -F qcow2 overlay.qcow2

# Verification
qemu-img check test.qcow2
qemu-img check -r leaks test.qcow2
qemu-img check -r all test.qcow2
qemu-img info test.qcow2
qemu-img info --output=json test.qcow2
qemu-img map test.qcow2
qemu-img map --output=json test.qcow2

# I/O testing
qemu-io -c "write -P 0xaa 0 4096" test.qcow2
qemu-io -c "read -P 0xaa 0 4096" test.qcow2
qemu-io -c "discard 0 65536" test.qcow2

# Conversion
qemu-img convert -f qcow2 -O qcow2 -c in.qcow2 out.qcow2
qemu-img convert -f qcow2 -O raw in.qcow2 out.raw
```

### Go Test Commands

```bash
# Basic testing
go test ./...
go test -v ./...
go test -race ./...

# Coverage
go test -cover ./...
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Benchmarks
go test -bench=. ./...
go test -bench=. -benchmem ./...
go test -bench=. -cpuprofile=cpu.prof ./...

# Fuzzing
go test -fuzz=FuzzParseHeader -fuzztime=10m ./...
go test -fuzz=. -fuzztime=1h ./...
```

---

## Appendix B: Known QEMU Bugs/Quirks

Document any QEMU behavior we need to work around:

1. **Backing file format handling**: If a new backing file is provided without a format, QEMU may blindly reuse the prior format.

2. **Lazy refcount repair**: After unclean shutdown with lazy refcounts, QEMU rebuilds refcounts which can be slow for large images.

3. **Zero cluster variants**: QCOW2 has multiple zero representations (ZERO_PLAIN, ZERO_ALLOC) - ensure we handle both.

---

## Appendix C: Test Naming Conventions

```
Test<Category>_<Feature>_<Scenario>

Examples:
- TestQemuInterop_Create_VerifyCheck
- TestQemuInterop_Roundtrip_AllClusterSizes
- TestFuzz_Header_InvalidMagic
- TestCrash_L2Allocation_Recovery
- TestStress_Concurrent_ReadWrite
- TestEdge_Boundary_CrossCluster
- BenchmarkWrite_Sequential_64KB
```

---

## References

- [QEMU qemu-iotests](https://wiki.qemu.org/Testing/QemuIoTests)
- [QEMU Testing Documentation](https://www.qemu.org/docs/master/devel/testing/main.html)
- [Go Fuzzing Tutorial](https://go.dev/doc/tutorial/fuzz)
- [CrashMonkey Paper](https://dl.acm.org/doi/10.1145/3320275)
- [QCOW2 Source (qcow2.c)](https://github.com/qemu/qemu/blob/master/block/qcow2.c)
