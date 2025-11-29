# TODO - go-qcow2 Roadmap

## Priority: REVIEW.md Findings

> These items were identified in the code review (REVIEW.md) and should be addressed first.

### Critical (Must Fix)
- [x] Fix `itoa()` in qemu_interop_test.go - only returned single digit
- [x] Add backing chain depth limit (QEMU uses 64) - security issue
- [x] Fix or remove `refcountTable` struct - dead code in refcount.go
- [x] Cache refcount blocks like L2 tables - currently reads from disk every time

### High Priority
- [x] Refactor L2 table allocation - duplicated in getClusterForWrite/setZeroCluster
- [x] Add zstd decompression - detection added, returns clear error (pure Go constraint)
- [x] Fix V2 extension parsing - currently skipped entirely
- [x] Validate backing file path - prevent null bytes, sanitize

### Code Quality
- [x] Add sync.Pool for cluster buffer reuse
- [ ] Add `t.Parallel()` to unit tests
- [x] Document compressed L2 entry bit math in compress.go
- [x] Remove unused `tableLen` field from l2Cache

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

## Phase 2: Production-Ready ⚠️ IN PROGRESS

> **Note**: Downgraded from "COMPLETE" per REVIEW.md - several edge cases remain.

### Refcount Management ⚠️
- [x] Two-level refcount structure (table -> blocks)
- [x] Variable refcount width (1, 2, 4, 8, 16, 32, 64 bits)
- [x] Read refcount for cluster
- [x] Update refcount on allocation
- [x] Update refcount on deallocation (via WriteZeroAt)
- [x] Free-space tracking via refcounts (findFreeCluster)
- [x] Cache refcount blocks (see REVIEW.md)

### Backing Files ✅
- [x] Parse backing file path from header
- [x] Backing file format extension (0xe2792aca)
- [x] Open backing file chain recursively
- [x] Fall through to backing on unallocated read
- [x] Copy-on-write: copy from backing on partial cluster write
- [x] Path resolution relative to child image
- [x] CreateOverlay helper function
- [x] Raw backing file support
- [x] Add chain depth limit (see REVIEW.md - security)

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

## Phase 3: Common Features

### Zero Clusters ⚠️ Partial
- [x] Detect zero flag in L2 entry (bit 0)
- [x] Return zeros without disk read
- [x] Write zero clusters (WriteZeroAt with ZERO_PLAIN mode)
- [ ] QCOW2_CLUSTER_ZERO_ALLOC variant

### Compression - Reading ⚠️ Partial
- [x] Detect compressed L2 entries (bit 62)
- [x] Parse compressed cluster offset/size
- [x] Deflate decompression
- [ ] Zstd decompression (see REVIEW.md - high priority)

### Header Extensions ⚠️ Partial
- [x] Parse extension headers
- [x] Feature name table (0x6803f857)
- [x] Backing file format (0xe2792aca)
- [x] Ignore unknown compatible extensions
- [x] Fail on unknown incompatible features (in Validate())
- [x] V2 extension parsing (see REVIEW.md - currently skipped)

### Snapshots - Internal
- [ ] Parse snapshot table structure
- [ ] Snapshot directory entries
- [ ] Read from specific snapshot
- [ ] Create snapshot (copy L1 table, update refcounts)
- [ ] Delete snapshot
- [ ] Revert to snapshot

---

## Phase 4: Advanced Features

### Compression - Writing
- [ ] Compress clusters on write (optional)
- [ ] Deflate compression
- [ ] Zstd compression
- [ ] Compression level selection

### Encryption
- [ ] Detect encryption method from header
- [ ] AES encryption (legacy, method=1)
- [ ] LUKS encryption (modern, method=2)

### Extended L2 Entries
- [ ] Detect incompatible feature bit 4
- [ ] Parse 128-bit extended L2 entries
- [ ] 32 subclusters per cluster

### Bitmaps / Dirty Tracking
- [ ] Bitmap directory parsing
- [ ] Dirty tracking for incremental backups

### External Data Files
- [ ] External data file name extension (0x44415441)
- [ ] Incompatible feature bit 2

---

## Phase 5: Performance Optimization

> See REVIEW.md "Performance Analysis" for detailed profiling recommendations.
> Current estimated throughput: ~10-20% of qemu-img.

### P0 - Critical (from REVIEW.md)
- [ ] Eliminate L2 cache copy on hit - 64KB allocation per cache hit
- [x] Add refcount block cache - disk I/O per refcount lookup currently

### P1 - High Impact
- [ ] Batch fsync operations - currently 4 fsyncs per cluster allocation
- [ ] Free cluster bitmap - O(1) allocation vs O(n) scan
- [x] sync.Pool for cluster buffers

### P2 - Medium Priority
- [ ] Lock sharding for L2 cache
- [ ] Configurable cache sizes
- [ ] Cache statistics/metrics

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
- [ ] Add GitHub Actions CI workflow
- [ ] Add benchmarks

---

## Notes

### Design Decisions
1. **Zero-struct L2 tables**: Keep as `[]byte`, access via `binary.BigEndian.Uint64`
2. **LRU cache**: Simple doubly-linked list, returns copies to avoid races
3. **Cluster allocation**: File-end growth with free cluster reuse via refcount scanning
4. **BackingStore interface**: Supports both qcow2 and raw backing files
5. **Lazy refcounts**: Skip refcount updates during writes, rebuild from L1/L2 on dirty open
6. **Write ordering barriers**: Configurable via WriteBarrierMode (None/Metadata/Full)

### Open Questions
1. Should we support mmap for large images?
2. io_uring: goroutine-per-request vs event loop?
3. How aggressive should lazy refcount flushing be?
