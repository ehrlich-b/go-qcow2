# TODO - go-qcow2 Roadmap

## Test Suite - Production Readiness

Test gaps identified during code review. Prioritized by criticality for production deployment.

### Current Coverage Summary

| Category | Status | Notes |
|----------|--------|-------|
| Basic Read/Write | ✅ Good | `qcow2_test.go` |
| QEMU Interop | ✅ Excellent | `qemu_interop_test.go` |
| Regression Tests | ✅ Good | `regression_test.go` |
| Fuzzing | ⚠️ Basic | `fuzz_test.go` - needs longer runs |
| LUKS Encryption | ✅ Good | `luks_test.go` |
| Bitmaps | ✅ Good | `bitmaps_test.go` |
| Concurrency | ✅ Good | `regression_test.go`, `critical_test.go` |
| Crash Recovery | ✅ Good | `critical_test.go` |
| Corruption Handling | ✅ Good | `critical_test.go` |
| Snapshots | ✅ Good | `snapshot_test.go` |
| External Data Files | ✅ Good | `external_data_test.go` |
| Stress Tests | ✅ Good | `stress_test.go` |

### Test Phase 1: Critical (Before Any Production Use)

#### 1.1 Crash Recovery / Power Failure Tests

- [x] `TestCrashDuringClusterAllocation` - Kill process mid-allocation
- [x] `TestCrashDuringL2TableWrite` - Partial L2 table on disk
- [x] `TestCrashDuringL1TableUpdate` - L2 allocated but L1 not updated
- [x] `TestCrashDuringRefcountUpdate` - Cluster allocated, refcount not incremented
- [x] `TestCrashWithLazyRefcounts` - Verify lazy refcount recovery works
- [x] `TestCrashDuringSnapshotCreation` - Partial snapshot state
- [x] `TestRecoveryAfterDirtyBitSet` - Image marked dirty, verify repair
- [x] `TestPartiallyWrittenCluster` - Crash mid-cluster write

#### 1.2 Corrupted/Malformed Image Tests

- [x] `TestCorruptedMagic` - Invalid magic number
- [x] `TestCorruptedVersion` - Unknown version
- [x] `TestCorruptedClusterBits` - Out of range (< 9 or > 21)
- [x] `TestCorruptedL1TableOffset` - Points beyond EOF
- [x] `TestCorruptedL1TableSize` - Impossibly large
- [x] `TestCorruptedL2Entry` - Points beyond EOF
- [x] `TestCorruptedL2EntryAlignment` - Non-512-byte aligned offset
- [x] `TestCorruptedRefcountTableOffset` - Invalid offset
- [x] `TestCorruptedRefcountBlock` - Block points to itself
- [x] `TestCircularL1L2Reference` - L2 table points back to L1
- [x] `TestOverlappingMetadata` - L1/L2/refcount tables overlap
- [x] `TestOverlappingDataClusters` - Two L2 entries point to same cluster
- [x] `TestInvalidCompressedDescriptor` - Bad compressed cluster size/offset
- [x] `TestTruncatedImage` - File shorter than header indicates
- [x] `TestZeroSizeImage` - Virtual size = 0
- [x] `TestHugeVirtualSize` - Virtual size > physical possibility

#### 1.3 Concurrent Snapshot Tests

- [x] `TestSnapshotDuringConcurrentWrites` - Create snapshot while writes in progress
- [x] `TestConcurrentSnapshotCreation` - Two goroutines create snapshots
- [x] `TestWriteDuringSnapshotRead` - Write while reading from snapshot
- [x] `TestDeleteSnapshotDuringRead` - Delete snapshot being read

### Test Phase 2: Important (Before Wide Deployment)

#### 2.1 Boundary Condition Tests

- [x] `TestWriteAtExactClusterBoundary` - Write starts exactly at boundary
- [x] `TestWriteEndsAtClusterBoundary` - Write ends exactly at boundary
- [x] `TestWriteSpansThreeClusters` - Single write across 3 clusters
- [x] `TestWriteSpansMultipleL2Tables` - Write crosses L2 table boundary
- [x] `TestWriteAtLastByte` - Write to virtual_size - 1
- [x] `TestWriteBeyondVirtualSize` - Should fail gracefully
- [x] `TestReadAtLastByte` - Read last byte of image
- [x] `TestZeroLengthWrite` - WriteAt with empty buffer
- [x] `TestZeroLengthRead` - ReadAt with empty buffer
- [x] `TestNegativeOffset` - Negative offset handling
- [x] `TestMaxInt64Offset` - Very large offset

#### 2.2 Refcount Bit Width Tests

- [x] `TestRefcount1Bit` - Max 1 reference per cluster
- [x] `TestRefcount2Bit` - Max 3 references
- [x] `TestRefcount4Bit` - Max 15 references
- [x] `TestRefcount8Bit` - Max 255 references
- [x] `TestRefcount16Bit` - Default, max 65535
- [x] `TestRefcount32Bit` - Max 4 billion
- [x] `TestRefcount64Bit` - Max huge
- [x] `TestRefcountOverflow` - Exceed max for each width
- [x] `TestRefcountTableGrowth` - Allocate enough to need new refcount blocks

#### 2.3 L1 Table Growth Tests

- [x] `TestL1TableGrowsOnWrite` - Write beyond current L1 coverage
- [x] `TestL1TableMaxSize` - Create image requiring large L1
- [x] `TestL1TableReallocation` - L1 table needs to move

#### 2.4 Backing Chain Tests

- [x] `TestBackingChainDepth3` - Base -> overlay1 -> overlay2
- [x] `TestBackingChainDepth10` - Deep chain
- [x] `TestBackingChainDepthLimit` - Hit the 64-level limit
- [x] `TestBackingChainDifferentClusterSizes` - Mixed 4K and 64K
- [x] `TestBackingChainMixedVersions` - v2 base, v3 overlay
- [x] `TestBackingChainWithCompressedBase` - Compressed clusters in base
- [x] `TestBackingChainWithZeroFlaggedBase` - Zero-flagged in base
- [x] `TestBackingChainCOWPartialCluster` - COW on partial cluster write
- [x] `TestBackingChainMissingFile` - Base file deleted/moved
- [x] `TestBackingChainCircularReference` - Image points to itself
- [x] `TestBackingChainRelativePath` - Relative path resolution
- [x] `TestBackingChainAbsolutePath` - Absolute path handling

#### 2.5 Snapshot Edge Cases

- [x] `TestCreateManySnapshots` - 100+ snapshots
- [x] `TestSnapshotWithCompressedClusters` - Snapshot preserves compressed
- [x] `TestSnapshotWithZeroFlaggedClusters` - Snapshot preserves zero flag
- [x] `TestDeleteFirstSnapshot` - Delete oldest snapshot
- [x] `TestDeleteLastSnapshot` - Delete newest snapshot
- [x] `TestDeleteMiddleSnapshot` - Delete from middle
- [x] `TestDeleteAllSnapshots` - Delete all, image still works
- [x] `TestSnapshotNameMaxLength` - 65535 byte name
- [x] `TestSnapshotNameUnicode` - UTF-8 characters in name
- [x] `TestSnapshotNameEmpty` - Empty string name
- [x] `TestSnapshotNameDuplicate` - Same name twice
- [x] `TestRevertPreservesOtherSnapshots` - Revert doesn't delete snapshots
- [x] `TestSnapshotL1SizeMismatch` - Snapshot with different L1 size

#### 2.6 External Data File Tests

- [x] `TestExternalDataFileBasic` - Create and use external data file
- [x] `TestExternalDataFileTruncated` - Data file shorter than expected
- [x] `TestExternalDataFileMissing` - Data file not found
- [x] `TestExternalDataFilePermissionDenied` - Can't read data file
- [x] `TestExternalDataFileGrowth` - Data file grows with writes
- [x] `TestExternalDataFileWithCompression` - Should fail (incompatible)

### Test Phase 3: Hardening

#### 3.1 Stress / Longevity Tests

- [x] `TestMillionClusters` - Image with 100K+ allocated clusters (4KB clusters)
- [x] `TestLargeImage` - 2TB virtual size
- [x] `TestRepeatedOpenClose` - Open/close 10000 times (leak check)
- [x] `TestLongRunningWrites` - Continuous writes for 30 seconds
- [x] `TestFragmentedAllocation` - Allocate, free, reallocate pattern
- [x] `TestCacheEvictionStress` - Exceed cache size significantly
- [x] `TestHighConcurrency` - 100 goroutines

#### 3.2 Error Path Tests

- [ ] `TestDiskFullDuringWrite` - ENOSPC handling
- [ ] `TestDiskFullDuringAllocation` - Can't allocate new cluster
- [ ] `TestReadOnlyFileSystem` - Write to RO mounted FS
- [ ] `TestPermissionDenied` - File permission issues
- [ ] `TestIOError` - Simulated I/O errors
- [ ] `TestInterruptedSyscall` - EINTR handling

#### 3.3 Encryption Edge Cases

- [ ] `TestLUKS1AllCiphers` - aes-xts, aes-cbc, etc.
- [ ] `TestLUKS2AllCiphers` - Modern cipher support
- [ ] `TestLUKSKeySlots` - Multiple key slots
- [ ] `TestLUKSCorruptedHeader` - Damaged LUKS header
- [ ] `TestLUKSWrongKeySlot` - Password in slot 2, not slot 0
- [ ] `TestAESLegacyReadCrossCluster` - Legacy AES across clusters
- [ ] `TestEncryptedWithCompression` - Both enabled (should work)

#### 3.4 Bitmap Edge Cases

- [ ] `TestBitmapLargeImage` - Bitmap for TB+ image
- [ ] `TestBitmapAllDirty` - Every bit set
- [ ] `TestBitmapAllClean` - No bits set
- [ ] `TestBitmapSparsePattern` - Alternating dirty/clean
- [ ] `TestBitmapInvalidation` - Write clears auto-bitmap
- [ ] `TestMultipleBitmaps` - 10+ bitmaps on one image
- [ ] `TestBitmapNameCollision` - Duplicate bitmap names

### Fuzzing Improvements

#### Extended Fuzzing Campaigns

- [ ] Run `FuzzParseHeader` for 24+ hours
- [ ] Run `FuzzReadWrite` for 24+ hours
- [ ] Run `FuzzFullImage` for 24+ hours
- [ ] Add coverage-guided corpus from real QEMU images

#### New Fuzz Targets

- [ ] `FuzzL2TableParsing` - Malformed L2 tables
- [ ] `FuzzRefcountBlock` - Malformed refcount blocks
- [ ] `FuzzCompressedCluster` - Invalid compressed data
- [ ] `FuzzSnapshotHeader` - Malformed snapshot headers
- [ ] `FuzzBitmapDirectory` - Malformed bitmap metadata
- [ ] `FuzzExtensions` - Unknown/malformed extensions
- [ ] `FuzzCheck` - Run Check() on corrupted images
- [ ] `FuzzRepair` - Run Repair() on corrupted images

### Test Infrastructure Improvements

- [ ] `createCorruptedImage()` helper - Generate image with specific corruption
- [ ] `createPartialWriteImage()` helper - Simulate crash mid-operation
- [ ] `mockIOErrors()` helper - Inject I/O errors at specific offsets
- [ ] Add nightly fuzzing job (24h runs)
- [ ] Add weekly stress test job
- [ ] Add coverage tracking and minimum threshold
- [ ] Test on multiple QEMU versions (4.0, 5.0, 6.0, 7.0, 8.0)

### Production Readiness Acceptance Criteria

1. **All Phase 1 tests pass** - Crash recovery and corruption handling
2. **All Phase 2 tests pass** - Boundary conditions and edge cases
3. **24-hour fuzz test** - No crashes found
4. **24-hour stress test** - No leaks, corruption, or errors
5. **Race detector clean** - `go test -race` finds no issues
6. **QEMU interop 100%** - All QEMU versions pass check
7. **Code coverage > 80%** - Measured by `go test -cover`

---

## Phase 1: Core Functionality ✅ COMPLETE

- [x] Project setup (go.mod, README, CLAUDE.md)
- [x] Header parsing (v2 and v3)
- [x] Header validation and feature flags
- [x] L1 table loading
- [x] L2 table caching (LRU)
- [x] Address translation (virtual -> physical)
- [x] `io.ReaderAt` implementation
- [x] `io.WriterAt` implementation
- [x] Basic cluster allocation (file-end growth)
- [x] Image creation (`Create()` function)
- [x] Basic unit tests

---

## Phase 2: Production-Ready ✅ COMPLETE

### Refcount Management ✅
- [x] Two-level refcount structure (table -> blocks)
- [x] Variable refcount width (1, 2, 4, 8, 16, 32, 64 bits)
- [x] Read refcount for cluster
- [x] Update refcount on allocation
- [x] Update refcount on deallocation (via WriteZeroAt)
- [x] Free-space tracking via refcounts (findFreeCluster)
- [x] Cache refcount blocks

### Backing Files ✅
- [x] Parse backing file path from header
- [x] Backing file format extension (0xe2792aca)
- [x] Open backing file chain recursively
- [x] Fall through to backing on unallocated read
- [x] Copy-on-write: copy from backing on partial cluster write
- [x] Path resolution relative to child image
- [x] CreateOverlay helper function
- [x] Raw backing file support
- [x] Add chain depth limit (max 64, like QEMU)

### Lazy Refcounts ✅
- [x] Detect `lazy_refcounts` compatible feature
- [x] Defer refcount updates during writes
- [x] Mark image dirty on write
- [x] Refcount rebuild on open if dirty
- [x] Scan L1/L2 tables to repair refcounts

### Safety Mechanisms ✅
- [x] Mark image dirty (incompatible bit 0) on RW open
- [x] Clear dirty bit on clean close
- [x] Detect corrupt bit (incompatible bit 1)
- [x] Refuse writes to corrupt images
- [x] Overlap checks (prevent metadata corruption)
- [x] Write ordering barriers (configurable via WriteBarrierMode)

### Repair Capabilities ✅
- [x] Scan L1/L2 tables for consistency
- [x] Rebuild refcounts from L1/L2
- [x] Check command equivalent
- [x] Detect and report inconsistencies

---

## Phase 3: Common Features ✅ COMPLETE

### Zero Clusters ✅
- [x] Detect zero flag in L2 entry (bit 0)
- [x] Return zeros without disk read
- [x] Write zero clusters (WriteZeroAt with ZERO_PLAIN mode)
- [x] QCOW2_CLUSTER_ZERO_ALLOC variant (WriteZeroAtMode with ZeroAlloc)

### Compression - Reading ✅
- [x] Detect compressed L2 entries (bit 62)
- [x] Parse compressed cluster offset/size
- [x] Deflate decompression
- [x] Zstd detection (returns clear error - pure Go constraint, no cgo)

### Header Extensions ✅
- [x] Parse extension headers
- [x] Feature name table (0x6803f857)
- [x] Backing file format (0xe2792aca)
- [x] Ignore unknown compatible extensions
- [x] Fail on unknown incompatible features (in Validate())
- [x] V2 extension parsing

### Snapshots - Internal ✅
- [x] Parse snapshot table structure
- [x] Snapshot directory entries (FindSnapshot, Snapshots)
- [x] Read from specific snapshot (ReadAtSnapshot)
- [x] Create snapshot (copy L1 table, update refcounts, COW support)
- [x] Delete snapshot
- [x] Revert to snapshot

---

## Phase 4: Advanced Features ✅ COMPLETE

### Compression - Writing
- [x] Compress clusters on write (optional)
- [x] Deflate compression
- [x] Compression level selection

### Zstd Compression (klauspost/compress/zstd - pure Go) ✅
- [x] Add klauspost/compress/zstd dependency
- [x] Implement zstd decompression for reading compressed clusters
- [x] Implement zstd compression for writing
- [x] Add compression type header extension support

### Encryption
- [x] Detect encryption method from header (returns ErrEncryptedImage)

### AES Encryption (legacy, method=1 - DEPRECATED) ✅
> Note: [Deprecated by QEMU in 2015](https://www.berrange.com/posts/2015/03/17/qemu-qcow2-built-in-encryption-just-say-no-deprecated-now-to-be-deleted-soon/), broken by design. Implement read-only for legacy image compatibility.
- [x] Implement AES-128-CBC decryption using crypto/aes (stdlib)
- [x] Derive key from passphrase (direct copy, no PBKDF - matches QEMU)
- [x] Per-sector IV generation (PLAIN64: sector offset as little-endian uint64)
- [x] Read-only support for legacy encrypted images
- [x] Clear security warning in API docs

### LUKS Encryption (modern, method=2)
> **Design Doc:** [docs/luks_design.md](docs/luks_design.md) | **Research:** [docs/luks_libraries.md](docs/luks_libraries.md)
>
> Hybrid approach: luksy for header parsing, custom key derivation + x/crypto/xts for random-access decryption.
> Standard LUKS libraries don't support random access (they auto-increment sector IVs), but QCOW2 needs to decrypt scattered clusters at arbitrary physical offsets.

#### Phase 1: LUKS1 Read-Only ✅
- [x] Parse Full Disk Encryption header extension (0x0537be77)
- [x] Add containers/luksy dependency for header parsing
- [x] Implement PBKDF2 key derivation for key slots
- [x] Implement Anti-Forensic (AF) merge algorithm
- [x] Implement master key verification against digest
- [x] Integrate x/crypto/xts for random-access sector decryption
- [x] Test with qemu-img LUKS1 images

#### Phase 2: LUKS2 Read-Only ✅
- [x] Add Argon2i/Argon2id key derivation (x/crypto/argon2)
- [x] Parse LUKS2 JSON metadata via luksy
- [x] Handle LUKS2-specific key slot format
- [x] Test with qemu-img LUKS2 images (skipped if qemu < 5.1)

#### Phase 3: Write Support ✅
- [x] Implement encrypted cluster writes
- [x] Handle IV generation for new clusters

### Extended L2 Entries ✅
- [x] Detect incompatible feature bit 4
- [x] Parse 128-bit extended L2 entries
- [x] 32 subclusters per cluster (read-only, test skipped if qemu < 5.2)

### Bitmaps / Dirty Tracking ✅
- [x] Bitmap extension parsing (0x23852875)
- [x] Bitmap directory entry parsing
- [x] Bitmap table reading with all-zeros/all-ones optimization
- [x] Dirty tracking bitmap API (Bitmaps, FindBitmap, OpenBitmap)
- [x] IsSet() for single-bit queries
- [x] GetDirtyRanges() for incremental backup enumeration
- [x] CountDirtyBits/CountDirtyBytes statistics

### External Data Files ✅
- [x] External data file name extension (0x44415441)
- [x] Incompatible feature bit 2
- [x] Route cluster data I/O through external file
- [x] Support read/write/compress operations with external data

---

## Phase 5: Performance Optimization

> Current estimated throughput: ~10-20% of qemu-img. With optimizations below, 2-5x improvement is achievable.

### P0 - Critical ✅
- [x] Eliminate L2 cache copy on hit - was 64KB allocation per cache hit
- [x] Add refcount block cache - was disk I/O per refcount lookup
- [x] sync.Pool for cluster buffers

### P1 - High Impact ✅
- [x] Batch fsync operations - BarrierBatched mode defers syncs until Flush()
- [x] Free cluster bitmap - O(1) allocation using bitmap with lazy build

### P2 - Medium Priority ✅
- [x] Lock sharding for L2 cache
- [x] Configurable cache sizes
- [x] Cache statistics/metrics

### I/O Optimization (Future)
- [ ] io_uring backend interface
- [ ] Direct I/O option (O_DIRECT)
- [ ] Async I/O support

---

## Phase 6: go-ublk Integration

- [ ] Define block device interface
- [ ] Request queue handling
- [ ] Create ublk target backend
- [ ] Performance testing vs qemu-nbd

---

## Notes

### Design Decisions
1. **Zero-struct L2 tables**: Keep as `[]byte`, access via `binary.BigEndian.Uint64`
2. **Sharded LRU cache**: 8-shard cache reduces lock contention under concurrent access; per-shard LRU with direct references (no copy on hit)
3. **Cluster allocation**: File-end growth with free cluster reuse via refcount scanning
4. **BackingStore interface**: Supports both qcow2 and raw backing files
5. **Lazy refcounts**: Skip refcount updates during writes, rebuild from L1/L2 on dirty open
6. **Write ordering barriers**: Configurable via WriteBarrierMode (None/Batched/Metadata/Full)

### Concurrency
The library is thread-safe for concurrent read/write operations:
- `freeClusterBitmap` operations are properly synchronized
- L2 cache returns copies to avoid concurrent modification
- Write operations (cluster allocation, L2 updates) are serialized via `writeMu`
- `dirty` flag uses atomic operations

Tests `TestConcurrencyStress` and `TestConcurrencyMixedOperations` in `regression_test.go` verify concurrent access.

### Open Questions
1. Should we support mmap for large images?
2. io_uring: goroutine-per-request vs event loop?
3. How aggressive should lazy refcount flushing be?
