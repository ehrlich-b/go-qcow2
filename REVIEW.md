# go-qcow2 Code Review

**Date:** November 30, 2025
**Reviewer:** Gemini (AI Assistant)
**Scope:** Full Codebase Audit (Functionality, Concurrency, Style, QEMU Compatibility)

## Executive Summary

The `go-qcow2` project is a sophisticated and well-structured implementation of the QCOW2 file format in pure Go. It successfully implements complex features like backing chains, snapshots, LUKS encryption, and persistent bitmaps. The code style is idiomatic, clean, and well-documented.

However, there are **critical concurrency defects** that make the library unsafe for concurrent writers (even though it uses mutexes, they are too fine-grained). There is also a likely **data corruption bug** in the refcount block allocation logic, and the persistent bitmap implementation is incomplete (read-only), rendering bitmaps stale upon any write.

While the project is "feature complete" per the TODO, it is **not production-ready** due to these correctness issues.

---

## 1. Macro-Level Architecture & Design

### Strengths
*   **Modularity:** The `Image` struct acts as a clean facade, delegating to specialized components (`refcount.go`, `cache.go`, `snapshot.go`).
*   **Caching:** The sharded LRU cache for L2 tables and refcount blocks is a high-performance design choice.
*   **Zero-Copy Intent:** The use of direct slice references in caches (with strict warnings) shows a focus on performance.
*   **Interface Compliance:** implementing `io.ReaderAt` and `io.WriterAt` makes it a drop-in replacement for `os.File`.

### Weaknesses
*   **Concurrency Model:** The library uses fine-grained locking (`l1Mu`, `refcountTableLock`, cache shard locks) which protects individual data structures but **fails to protect logical operations**.
    *   **Critical Race:** In `WriteAt`, the sequence `getClusterForWrite` -> `allocateCluster` -> `write data` -> `update L2` is not atomic. Two concurrent writes to the same unallocated cluster will both allocate space, write data, and race to update the L2 table. The last writer wins the L2 update, but the first allocation is leaked (space used, refcount > 0, but not referenced).
    *   **Snapshot Race:** `CreateSnapshot` copies the L1 table. If a write occurs concurrently, the snapshot might capture a mix of pre- and post-write state, or worse, an inconsistent L1 table.
*   **External data file layout:** Metadata allocations (L2 tables, snapshot L1 copies, etc.) use `dataFile()` instead of the qcow2 metadata file. On images with `IncompatExternalData`, that writes metadata into the external data file, which QEMU will not interpret as metadata and which the refcount table in the qcow2 file does not cover.

---

## 2. Micro-Level Code Review

### `qcow2.go` - Core Logic
*   **`WriteAt` / `getClusterForWrite`:**
    *   As noted, lacks a high-level cluster lock. Needs a `sync.Mutex` per cluster (or a hashed lock map) to serialize allocations/COW for the *same* cluster.
    *   **COW Optimization:** The COW logic reads the old cluster, then writes it to the new location. `io.Copy` or similar could be cleaner, but the current buffer reuse is fine.
*   **`Open`:**
    *   Good use of options pattern.
    *   Correctly handles `lazy_refcounts` and dirty bit checks.

### `refcount.go` - Reference Counting
*   **Self-Referential Allocation Bug:**
    *   In `updateRefcount`, if a new refcount block is needed, `allocateRefcountBlock` is called. This function allocates space and updates the *refcount table*, but it **does not increment the refcount** for the new block itself.
    *   The comment says: `// The new refcount block itself needs a refcount of 1 ... This is handled by the caller`.
    *   **Bug:** The caller (`updateRefcount`) *does not handle this*. It proceeds to update the refcount for the *target* cluster (the one passed in `hostOffset`), not the *metadata* cluster (the new refcount block).
    *   **Impact:** New refcount blocks effectively have a refcount of 0. If `Check()` or `Repair()` runs, it will flag this as corruption or a leak.

### `bitmaps.go` - Persistent Bitmaps
*   **Staleness Issue:**
    *   The implementation supports **reading** bitmaps (`IsSet`, `GetDirtyRanges`).
    *   **Missing Feature:** `WriteAt` does **not** update the dirty bitmaps.
    *   **Consequence:** If you modify an image that has persistent bitmaps, those bitmaps immediately become invalid (stale). QEMU handles this by marking them "in use" or updating them. This library leaves them as "consistent" in the header but factually incorrect. This destroys the validity of incremental backups based on these bitmaps.

### `qcow2.go` / `compress.go` - Cluster flag handling
*   **Compressed cluster overwrite:** `getClusterForWrite` ignores `L2EntryCompressed`. Writes are directed to the compressed payload offset (using `L2EntryOffsetMask`), never decompressing or re-allocating a normal cluster, and they still consult refcounts for that byte offset. Any overwrite of a compressed cluster corrupts data and leaves the L2 entry in an impossible state (compressed flag + non-compressed payload). The compressed write path (`writeCompressedCluster`) also skips `dataBarrier` entirely, so `BarrierFull` does not order data vs. metadata for compressed writes.
*   **Zero-alloc writes stay zero:** If a cluster is marked `L2EntryZeroFlag` with `COPIED` already set (e.g., after `ZeroAlloc`), `getClusterForWrite` never clears the zero flag. The data is written to disk, but `translate` will continue returning `clusterZero`, so reads return zeros and silently discard user data.

### Extended L2 (bit 4) - write path
*   Extended L2 is treated as fully supported (`header.Validate` allows the feature), but `getClusterForWrite`/`setZeroCluster` treat entries as 64-bit standard ones. Subcluster allocation/zero bitmaps are ignored and never updated, so any write to an extended-L2 image will produce metadata that QEMU will interpret differently from this implementation.

### External data file handling
*   `allocateCluster` always targets `dataFile()` (external file if present). Callers use it for metadata (new L2 tables, snapshot L1 copies), so metadata migrates into the external data file. Refcount bookkeeping remains in the qcow2 file and no longer protects that metadata. Additionally, `buildFreeBitmap` sizes the bitmap using the qcow2 file, not the external data file, so free-space tracking cannot work for external-data images.

### `cache.go` - Caching
*   **Design:** Sharded LRU is excellent.
*   **Safety:** The comment `// Callers may read from it freely. Callers that modify the slice MUST call put()` relies on developer discipline. Given the race conditions elsewhere, this is a risky optimization, though standard for high-perf Go.

### `check.go` - Validation
*   **Completeness:** The check logic looks robust and correctly calculates expected refcounts from L1/L2 traversal. It should correctly identify the leaks/corruptions caused by the bugs mentioned above.

---

## 3. TODO vs. Reality Gap Analysis

| Feature | Status in TODO | Actual Status | Discrepancy |
| :--- | :--- | :--- | :--- |
| **Concurrency** | Implicitly "Production Ready" | **Broken** | Unsafe for concurrent writes. |
| **Refcounts** | "Complete" | **Buggy** | Refcount block allocation likely corrupts metadata. |
| **Bitmaps** | "Dirty tracking bitmap API" | **Read-Only** | Writes do not update bitmaps, invalidating them. |
| **AES Encrypt** | "Read-only" | **Read-only** | Accurate (by design). |
| **LUKS Encrypt**| "Write Support" | **Implemented** | Seems correct. |
| **Extended L2 entries** | "Complete" (subclusters) | **Writes ignore subcluster bitmaps** | Images with bit 4 set will be corrupted on write; should be read-only or fully implemented. |
| **External data files** | "Support read/write/compress operations with external data" | **Metadata allocated into external file; free-space map uses qcow2 file size** | Not compatible with QEMU’s external-data layout; refcounts don’t cover metadata placed in the external file. |
| **Compression – Writing** | Checked off | **Overwrites compressed clusters in place** | No decompress + reallocate path; metadata barriers missing for compressed writes. |

---

## 4. Recommendations for "Definitive Edition"

To make this the definitive Go implementation, you must address the following:

### P0: Critical Fixes
1.  **Fix Refcount Recursion:** Modify `allocateRefcountBlock` to recursively call `updateRefcount` for the new block's offset, OR handle it explicitly in `updateRefcount`. Be careful of infinite recursion (allocating a block to store the refcount of the new block...). QEMU handles this carefully; mimic their recursion termination logic.
2.  **Cluster Locking:** Implement a `clusterLock` system. Before `getClusterForWrite` does anything, it must lock the virtual cluster index. This prevents racing allocations/COW.
3.  **Bitmap Invalidation:** At minimum, `WriteAt` should verify if any persistent bitmaps exist. If they do, it must either:
    *   Update them (hard, requires implementing bitmap write logic).
    *   **OR** Mark them as "inconsistent" (clear `BitmapFlagInUse` or similar) so consumers know they are stale.
4.  **Compressed-cluster rewrite path:** Detect `L2EntryCompressed` in `getClusterForWrite`; decompress into a freshly allocated normal cluster, clear the compressed flag, and update refcounts accordingly. Add `dataBarrier` before metadata updates in `writeCompressedCluster`.
5.  **Zero flag clearing:** When `L2EntryZeroFlag` is set (especially with `COPIED` already set from `ZeroAlloc`), clear the flag on first write so subsequent reads return the written data.
6.  **External data layout:** Ensure all metadata (L1/L2 tables, refcount blocks, snapshot tables) are always allocated in the qcow2 file, not the external data file. Maintain a separate allocator for data clusters vs. metadata, and size/grow the free-space bitmap using the actual target file.
7.  **Extended L2 writes:** Until subcluster allocation is implemented, reject writes on images with `IncompatExtendedL2` (or implement full subcluster bitmap updates).

### P1: Polish & Confidence
1.  **Fuzzing:** The `fuzz_test.go` is present. Run it for at least 24 hours on a high-core machine.
2.  **Concurrency Tests:** Add a test in `qcow2_test.go` that spawns 100 goroutines all writing to random offsets (some overlapping) in the same image. Verify `Check()` returns 0 leaks/corruptions afterwards. **This will likely fail currently.**
3.  **IO_URING:** The TODO mentions `io_uring`. For a "definitive" high-performance implementation on Linux, this is key. The current `os.File` synchronous I/O is a bottleneck.
4.  **External-data integration test:** Add an interop test that creates an external-data qcow2 with QEMU, writes via this library, and re-opens with QEMU to confirm metadata placement and refcounts.
5.  **Snapshot/write serialization test:** Add a test that creates a snapshot while concurrent writers hammer the same image; assert snapshot sees a consistent L1/L2 view and `Check()` remains clean.
6.  **Regression harness:** Add explicit regression tests for compressed-cluster overwrite, ZeroAlloc overwrite, extended-L2 write rejection/handling, and run them in the docker/qemu image path. Gate CI on `make docker-test-verbose` (or `docker-test-race` once races are fixed) so format-accurate QEMU tooling exercises these cases.

### P2: Performance
1.  **Batch Refcount Updates:** Instead of updating the refcount block for every allocation, use a journal or strict `LazyRefcounts` adherence (which you already support) to minimize metadata I/O.
2.  **Vectorized I/O:** Use `preadv`/`pwritev` if possible to combine header updates with data writes, though strict ordering requirements (barriers) often prevent this in filesystems.

## Conclusion

The codebase is a fantastic foundation. It implements the hard parts of QCOW2 (crypto, compression, format details) very well. However, the core I/O path has subtle race conditions and several correctness gaps (refcount-block self-tracking, compressed-cluster rewrites, zero-flag clearing, external-data metadata placement) that prevent it from being "correct" in all cases. Fix those along with locking, and you have a production-grade library.
