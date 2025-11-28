# TODO - go-qcow2 Roadmap

## Phase 1: Core Functionality (Current)

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

### In Progress
- [ ] E2E test suite with QEMU interoperability

---

## Phase 2: Production-Ready

### Refcount Management (Hard - Critical)
- [ ] Two-level refcount structure (table -> blocks)
- [ ] Variable refcount width (1, 2, 4, 8, 16, 32, 64 bits)
- [ ] Read refcount for cluster
- [ ] Update refcount on allocation
- [ ] Update refcount on deallocation
- [ ] Free-space tracking via refcounts

### Backing Files (Medium - Very Common)
- [x] Parse backing file path from header
- [ ] Backing file format extension (0xe2792aca)
- [x] Open backing file chain recursively
- [x] Fall through to backing on unallocated read
- [ ] **Copy-on-write: copy from backing on partial cluster write** (BUG: currently zeroes cluster on write)
- [x] Path resolution relative to child image
- [x] CreateOverlay helper function

### Lazy Refcounts (Hard - Common)
- [ ] Detect `lazy_refcounts` compatible feature
- [ ] Defer refcount updates during writes
- [ ] Mark image dirty on write
- [ ] Refcount rebuild on open if dirty
- [ ] Scan L1/L2 tables to repair refcounts

### Safety Mechanisms (Medium - Essential)
- [ ] Mark image dirty (incompatible bit 0) on RW open
- [ ] Clear dirty bit on clean close
- [ ] Detect corrupt bit (incompatible bit 1)
- [ ] Refuse writes to corrupt images
- [ ] Overlap checks (prevent metadata corruption)
- [ ] Write ordering barriers

### Repair Capabilities (Hard - Important)
- [ ] Scan L1/L2 tables for consistency
- [ ] Rebuild refcounts from L1/L2
- [ ] Check command equivalent
- [ ] Detect and report inconsistencies

---

## Phase 3: Common Features

### Zero Clusters (Simple - Common)
- [ ] Detect zero flag in L2 entry (bit 0)
- [ ] Return zeros without disk read
- [ ] Write zero clusters (deallocate + set flag)
- [ ] QCOW2_CLUSTER_ZERO_PLAIN vs ZERO_ALLOC

### Compression - Reading (Medium - Common)
- [ ] Detect compressed L2 entries (bit 62)
- [ ] Parse compressed cluster offset/size
- [ ] Deflate (zlib) decompression
- [ ] Zstd decompression
- [ ] Handle compression type field (byte 104)

### Snapshots - Internal (Hard - Common)
- [ ] Parse snapshot table structure
- [ ] Snapshot directory entries
- [ ] Read from specific snapshot
- [ ] Create snapshot (copy L1 table, update refcounts)
- [ ] Delete snapshot
- [ ] Revert to snapshot
- [ ] Reconstruct COPIED flag from refcounts

### Header Extensions (Simple - Important)
- [ ] Parse extension headers
- [ ] Feature name table (0x6803f857)
- [ ] Backing file format (0xe2792aca)
- [ ] Ignore unknown compatible extensions
- [ ] Fail on unknown incompatible extensions

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

### L2 Cache Improvements
- [ ] Configurable cache size
- [ ] Sharded cache (reduce lock contention)
- [ ] Cache statistics/metrics
- [ ] Dirty tracking for write-back
- [ ] Cache preloading

### Allocation Optimization
- [ ] Cluster pre-allocation (slab allocator)
- [ ] Free cluster bitmap
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

## E2E Test Suite (Priority)

### QEMU Interoperability Testing
- [ ] Docker/script to pull multiple QEMU versions (2.x, 5.x, 8.x)
- [ ] Create images with `qemu-img` (various cluster sizes, versions)
- [ ] Write patterns with `qemu-io`
- [ ] Read QEMU images, verify checksums
- [ ] Write with our lib, verify with `qemu-img check`
- [ ] Round-trip: QEMU -> us -> QEMU

### Fuzz Testing
- [ ] Fuzz header parsing
- [ ] Fuzz L2 table entries
- [ ] Fuzz random offset read/write
- [ ] Fuzz concurrent access

### Crash Recovery Testing
- [ ] Simulate crash during L2 allocation
- [ ] Simulate crash during data write
- [ ] Verify `qemu-img check -r` repairs our images
- [ ] Verify we handle QEMU dirty images

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
| Zero cluster detection | Medium | Simple | ✅ Done |
| Refcount reading | Essential | Medium | ✅ Done |
| Backing files (read) | High | Medium | ✅ Done |
| Backing files (COW write) | High | Medium | ⚠️ Partial |
| Refcount updates | Essential | Hard | ⏳ Pending |
| Lazy refcounts | High | Hard | ⏳ Pending |
| Compression (read) | Medium | Medium | ⏳ Pending |
| Internal snapshots | Medium | Hard | ⏳ Pending |
| Header extensions | Medium | Simple | ⏳ Pending |
| Compression (write) | Low | Medium | ⏳ Pending |
| Encryption | Low | Very Hard | ⏳ Pending |
| Extended L2 | Low | Hard | ⏳ Pending |
| Bitmaps/CBT | Low | Medium | ⏳ Pending |
| External data files | Low | Medium | ⏳ Pending |

---

## Notes

### Design Decisions
1. **Zero-struct L2 tables**: Keep as `[]byte`, access via `binary.BigEndian.Uint64`
2. **LRU cache**: Simple doubly-linked list, returns copies to avoid races
3. **Simple allocation**: Currently appends to file end, no reuse

### Open Questions
1. Should we support mmap for large images?
2. io_uring: goroutine-per-request vs event loop?
3. How aggressive should lazy refcount flushing be?

### Reference Implementation Notes
- QEMU's `block/qcow2.c` is ~5000 lines
- Key complexity: metadata update ordering
- L2 cache slicing (QEMU optimization) may be needed for large images
- Refcount management is the hardest part to get right
