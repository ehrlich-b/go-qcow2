# TODO - go-qcow2 Roadmap

## Immediate Fixes (From Code Review)

### Critical Bugs
- [ ] Fix `itoa()` helper in qemu_interop_test.go (only returns single digit)
- [ ] Add backing chain depth limit (prevent infinite recursion, QEMU uses 64)
- [ ] Remove or use `refcountTable` struct (currently dead code in refcount.go)

### High Priority Issues
- [ ] Cache refcount blocks like L2 tables (currently reads from disk every time)
- [ ] Refactor L2 table allocation logic (duplicated in getClusterForWrite and setZeroCluster)
- [ ] Add zstd decompression support (deflate only currently)
- [ ] Fix V2 header extension parsing (currently skips extensions for v2)
- [ ] Validate backing file path (prevent null bytes, sanitize)
- [ ] Add file size limits to prevent OOM on malicious headers

---

## Phase 1: Core Functionality ✅ COMPLETE

### Completed
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

## Phase 2: Production-Ready ⚠️ MOSTLY COMPLETE

> **Review Note**: Downgraded from "COMPLETE" - Several edge cases and issues identified.

### Refcount Management ⚠️ (Works but needs improvement)
- [x] Two-level refcount structure (table -> blocks)
- [x] Variable refcount width (1, 2, 4, 8, 16, 32, 64 bits)
- [x] Read refcount for cluster
- [x] Update refcount on allocation
- [x] Update refcount on deallocation (via WriteZeroAt)
- [x] Free-space tracking via refcounts (findFreeCluster)
- [ ] **FIX**: Cache refcount blocks (performance)
- [ ] **FIX**: Resolve circular dependency in allocateRefcountBlock

### Backing Files ✅ (Complete)
- [x] Parse backing file path from header
- [x] Backing file format extension (0xe2792aca)
- [x] Open backing file chain recursively
- [x] Fall through to backing on unallocated read
- [x] Copy-on-write: copy from backing on partial cluster write
- [x] Path resolution relative to child image
- [x] CreateOverlay helper function
- [x] Raw backing file support
- [ ] **FIX**: Add chain depth limit (security)

### Lazy Refcounts ✅ (Complete)
- [x] Detect `lazy_refcounts` compatible feature
- [x] Defer refcount updates during writes
- [x] Mark image dirty on write
- [x] Refcount rebuild on open if dirty
- [x] Scan L1/L2 tables to repair refcounts

### Safety Mechanisms ✅ (Complete)
- [x] Mark image dirty (incompatible bit 0) on RW open
- [x] Clear dirty bit on clean close
- [x] Detect corrupt bit (incompatible bit 1)
- [x] Refuse writes to corrupt images
- [x] Overlap checks (prevent metadata corruption)
- [x] Write ordering barriers (configurable via WriteBarrierMode)

### Repair Capabilities ✅ (Complete)
- [x] Scan L1/L2 tables for consistency
- [x] Rebuild refcounts from L1/L2
- [x] Check command equivalent
- [x] Detect and report inconsistencies

---

## Phase 3: Common Features

### Zero Clusters ⚠️ (Partial)
- [x] Detect zero flag in L2 entry (bit 0)
- [x] Return zeros without disk read
- [x] Write zero clusters (WriteZeroAt with ZERO_PLAIN mode)
- [ ] QCOW2_CLUSTER_ZERO_ALLOC variant

### Compression - Reading ⚠️ (Partial)
- [x] Detect compressed L2 entries (bit 62)
- [x] Parse compressed cluster offset/size
- [x] Deflate decompression
- [ ] **HIGH**: Zstd decompression
- [ ] Handle compression type field (byte 104)

### Snapshots - Internal (Hard - Common)
- [ ] Parse snapshot table structure
- [ ] Snapshot directory entries
- [ ] Read from specific snapshot
- [ ] Create snapshot (copy L1 table, update refcounts)
- [ ] Delete snapshot
- [ ] Revert to snapshot
- [ ] Reconstruct COPIED flag from refcounts

### Header Extensions ⚠️ (Partial)
- [x] Parse extension headers
- [x] Feature name table (0x6803f857)
- [x] Backing file format (0xe2792aca)
- [x] Ignore unknown compatible extensions
- [x] Fail on unknown incompatible features (in Validate())
- [ ] **FIX**: V2 extension parsing (currently skipped)

---

## Phase 4: Advanced Features

### Compression - Writing (Medium)
- [ ] Compress clusters on write (optional)
- [ ] Deflate compression
- [ ] Zstd compression
- [ ] Compression level selection

### Encryption (Very Hard - Rare)
- [ ] Detect encryption method from header
- [ ] AES encryption (legacy, method=1)
- [ ] LUKS encryption (modern, method=2)
- [ ] Full disk encryption header (0x0537be77)

### Extended L2 Entries (Hard - Rare)
- [ ] Detect incompatible feature bit 4
- [ ] Parse 128-bit extended L2 entries
- [ ] 32 subclusters per cluster
- [ ] Subcluster allocation bitmap
- [ ] Subcluster read/write

### Bitmaps / Dirty Tracking (Medium - Growing)
- [ ] Bitmap directory parsing
- [ ] Bitmap table handling
- [ ] Autoclear feature bit 0 (bitmap consistency)
- [ ] Dirty tracking for incremental backups
- [ ] Changed-block tracking (CBT)

### External Data Files (Medium - Rare)
- [ ] External data file name extension (0x44415441)
- [ ] Incompatible feature bit 2
- [ ] Guest offset matching
- [ ] Raw external data (autoclear bit 1)

---

## Phase 5: Performance Optimization

> **See REVIEW.md "Performance Analysis" for detailed profiling recommendations.**
>
> Current estimated performance: ~10-20% of qemu-img throughput.
> Target: 2-5x improvement with optimizations below.

### Profiling Commands (Added)
```bash
make profile-cpu      # CPU profiling
make profile-mem      # Memory profiling
make profile-all      # Both CPU and memory
make profile-trace    # Execution tracer
make profile-block    # Block profiling (goroutine contention)
```

### P0 - Critical Performance Issues (Do First)
- [ ] **Eliminate L2 cache copy on hit** - Currently copies 64KB on every cache hit!
  - Return slice directly with RWMutex protection
  - Or use copy-on-write semantics
  - **Impact: ~30% read improvement**
- [ ] **Add refcount block cache** - Currently does disk I/O per refcount lookup
  - Same pattern as L2 cache (16-32 entry LRU)
  - **Impact: ~50% improvement for refcount operations**

### P1 - High Impact Optimizations
- [ ] **Batch fsync operations** - Currently 4 fsyncs per cluster allocation
  - Defer syncs until Flush() or Close()
  - Add BarrierBatched mode for collecting metadata updates
  - **Impact: ~80% write improvement**
- [ ] **Free cluster bitmap** - Currently O(n) scan for every allocation
  - Maintain in-memory bitmap of free clusters
  - O(1) allocation vs O(n)
  - **Impact: Critical for images >10GB**
- [ ] **sync.Pool for cluster buffers** - Currently allocates 64KB per operation
  - Pre-allocate and reuse buffers
  - **Impact: ~20% reduction in GC pressure**

### P2 - Medium Priority Optimizations
- [ ] Lock sharding for L2 cache (reduce contention)
- [ ] Vectorized zero fill (use memclr instead of loop)
- [ ] Cache statistics/metrics for observability
- [ ] Configurable cache sizes

### L2 Cache Improvements
- [ ] Configurable cache size
- [ ] Sharded cache (reduce lock contention)
- [ ] Cache statistics/metrics
- [ ] Dirty tracking for write-back
- [ ] Cache preloading

### Allocation Optimization
- [ ] Cluster pre-allocation (slab allocator)
- [ ] Contiguous allocation preference
- [ ] Defragmentation support

### I/O Optimization
- [ ] Zero-copy paths where possible
- [ ] Batch multiple writes
- [ ] io_uring backend interface
- [ ] Direct I/O option (O_DIRECT)
- [ ] Async I/O support

### Concurrency
- [ ] Fine-grained locking (per L2 slice)
- [ ] Range locks on virtual offsets
- [ ] Lock-free read paths where safe

---

## Phase 6: go-ublk Integration

### Block Device Interface
- [ ] Define block device interface
- [ ] Request queue handling
- [ ] Scatter-gather support
- [ ] Discard/TRIM support

### ublk Integration
- [ ] Create ublk target backend
- [ ] Handle ublk commands
- [ ] io_uring SQE submission for I/O
- [ ] Performance testing vs qemu-nbd

---

## E2E Test Suite ✅ COMPLETE

> **Comprehensive test plan**: See [todo_test_suite.md](todo_test_suite.md) for the full test suite design including fuzzing strategies, crash recovery testing, CI/CD integration, and implementation roadmap.

### QEMU Interoperability Testing ✅
- [x] Create images with `qemu-img` (various cluster sizes, versions)
- [x] Write patterns with `qemu-io`
- [x] Read QEMU images, verify checksums
- [x] Write with our lib, verify with `qemu-img check`
- [x] Round-trip: QEMU -> us -> QEMU
- [ ] Docker/script to pull multiple QEMU versions (2.x, 5.x, 8.x)

### Fuzz Testing ✅
- [x] Fuzz header parsing
- [x] Fuzz L2 table entries
- [x] Fuzz random offset read/write
- [x] Fuzz refcount entry read/write
- [x] Fuzz full image opening
- [ ] Fuzz concurrent access

### Crash Recovery Testing
- [ ] Simulate crash during L2 allocation
- [ ] Simulate crash during data write
- [x] Verify `qemu-img check -r` repairs our images (lazy refcounts test)
- [x] Verify we handle QEMU dirty images

### Test Infrastructure
- [ ] Add GitHub Actions CI workflow
- [ ] Parallelize unit tests (t.Parallel())
- [ ] Add benchmarks

---

## Code Quality (From Review)

### Refactoring
- [ ] Extract common L2 allocation code from getClusterForWrite/setZeroCluster
- [ ] Split qcow2.go (1072 lines) into smaller modules
- [ ] Remove unused `tableLen` field from l2Cache
- [ ] Consider RWMutex for l2Cache.get() to improve read performance

### Documentation
- [ ] Document compressed L2 entry bit math in compress.go
- [ ] Add spec references to format.go constants
- [ ] Document Validate() limitations

### Testing
- [ ] Fix TestFreeClusterReuse to fail on no reuse
- [ ] Fix TestCheckAfterWriteZero leak detection
- [ ] Add cross-L1-boundary write tests
- [ ] Add large image tests (>2GB)

---

## Feature Summary Table

| Feature | Priority | Complexity | Status |
|---------|----------|------------|--------|
| Header parsing v2/v3 | Essential | Simple | ✅ Done |
| L1/L2 translation | Essential | Simple | ✅ Done |
| io.ReaderAt/WriterAt | Essential | Simple | ✅ Done |
| L2 cache (LRU) | Essential | Simple | ✅ Done |
| Image creation | Essential | Simple | ✅ Done |
| Dirty/corrupt tracking | High | Simple | ✅ Done |
| Safety mechanisms | High | Medium | ✅ Done |
| Zero cluster read | Medium | Simple | ✅ Done |
| Zero cluster write | Medium | Simple | ✅ Done |
| Refcount reading | Essential | Medium | ✅ Done |
| Backing files (read) | High | Medium | ✅ Done |
| Backing files (COW write) | High | Medium | ✅ Done |
| Refcount updates | Essential | Hard | ⚠️ Works, needs optimization |
| Lazy refcounts | High | Hard | ✅ Done |
| Compression (read) | Medium | Medium | ⚠️ Deflate only |
| Internal snapshots | Medium | Hard | ⏳ Pending |
| Header extensions | Medium | Simple | ⚠️ V3 only |
| Compression (write) | Low | Medium | ⏳ Pending |
| Encryption | Low | Very Hard | ⏳ Pending |
| Extended L2 | Low | Hard | ⏳ Pending |
| Bitmaps/CBT | Low | Medium | ⏳ Pending |
| External data files | Low | Medium | ⏳ Pending |
| **E2E Test Suite** | High | Medium | ✅ Done |
| **Critical Bug Fixes** | **Critical** | **Easy** | ⚠️ **Pending** |
| **Performance (P0)** | **Critical** | **Medium** | ⚠️ **10-20% of qemu-img** |

---

## Notes

### Design Decisions
1. **Zero-struct L2 tables**: Keep as `[]byte`, access via `binary.BigEndian.Uint64`
2. **LRU cache**: Simple doubly-linked list, returns copies to avoid races
3. **Cluster allocation**: File-end growth with free cluster reuse via refcount scanning
4. **Backing store interface**: `BackingStore` interface supports both qcow2 and raw backing files
5. **Lazy refcounts**: Skip refcount updates during writes, rebuild from L1/L2 on dirty open; always grow file (no free cluster reuse) in lazy mode
6. **Write ordering barriers**: Configurable via WriteBarrierMode (None/Metadata/Full); default is BarrierMetadata which syncs after metadata updates

### Review Findings (2025-11-28)
1. **Phase 2 should not be marked COMPLETE** - Several issues remain
2. **itoa() helper is broken** - Critical test bug
3. **No backing chain limit** - Security vulnerability
4. **Refcount blocks not cached** - Performance issue
5. **Dead code exists** - refcountTable struct, tableLen field
6. See `REVIEW.md` for full analysis

### Performance Findings (2025-11-28)
1. **L2 cache copies 64KB on every hit** - Biggest performance issue
2. **No refcount block cache** - Disk I/O per refcount lookup
3. **4 fsyncs per cluster allocation** - Excessive with default barrier mode
4. **O(n) free cluster search** - Unusable for large images
5. **12+ syscalls per new cluster** - vs optimal 2-3
6. **Estimated throughput: 10-20% of qemu-img**
7. See `REVIEW.md` "Performance Analysis" section for detailed profiling

### Open Questions
1. Should we support mmap for large images?
2. io_uring: goroutine-per-request vs event loop?
3. How aggressive should lazy refcount flushing be?

### Reference Implementation Notes
- QEMU's `block/qcow2.c` is ~5000 lines
- Key complexity: metadata update ordering
- L2 cache slicing (QEMU optimization) may be needed for large images
- Refcount management is the hardest part to get right
