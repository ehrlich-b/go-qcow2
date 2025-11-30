# TODO - go-qcow2 Roadmap

See also [REVIEW_TODO.md](REVIEW_TODO.md) for review-driven fixes and checks.

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

## Testing

### E2E / QEMU Interop ✅
- [x] Create images with `qemu-img` (various cluster sizes, versions)
- [x] Write patterns with `qemu-io`
- [x] Read QEMU images, verify checksums
- [x] Write with our lib, verify with `qemu-img check`
- [x] Round-trip: QEMU -> us -> QEMU

### Fuzz Testing ✅
- [x] Fuzz header parsing
- [x] Fuzz L2 table entries
- [x] Fuzz random offset read/write
- [x] Fuzz refcount entry read/write
- [x] Fuzz full image opening

### Test Infrastructure
- [x] Add GitHub Actions CI workflow
- [x] Add benchmarks

---

## Notes

### Design Decisions
1. **Zero-struct L2 tables**: Keep as `[]byte`, access via `binary.BigEndian.Uint64`
2. **Sharded LRU cache**: 8-shard cache reduces lock contention under concurrent access; per-shard LRU with direct references (no copy on hit)
3. **Cluster allocation**: File-end growth with free cluster reuse via refcount scanning
4. **BackingStore interface**: Supports both qcow2 and raw backing files
5. **Lazy refcounts**: Skip refcount updates during writes, rebuild from L1/L2 on dirty open
6. **Write ordering barriers**: Configurable via WriteBarrierMode (None/Batched/Metadata/Full)

### Known Issues
1. **Concurrency bugs**: Concurrent writes from multiple goroutines can cause refcount mismatches and leaked clusters. Race conditions exist in:
   - `freeClusterBitmap.setUsed()` / `grow()` - unsynchronized bitmap access
   - L2 cache - writes and reads racing on cached data
   - L2 table entry updates - modifications while being written to disk

   Tests `TestConcurrencyStress` and `TestConcurrencyMixedOperations` in `regression_test.go` expose these issues (currently skipped).

### Open Questions
1. Should we support mmap for large images?
2. io_uring: goroutine-per-request vs event loop?
3. How aggressive should lazy refcount flushing be?
4. How to properly synchronize concurrent writes without excessive lock contention?
