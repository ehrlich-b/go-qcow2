# go-qcow2

> **⚠️ EXPERIMENTAL - DO NOT USE IN PRODUCTION**
>
> This is an early-stage project exploring a pure Go QCOW2 implementation. The code has not been
> thoroughly tested against real-world images or validated for data integrity. **Using this library
> may result in data loss or image corruption.** This disclaimer will be removed once the
> implementation has been validated with comprehensive testing and real-world usage.

A pure Go implementation of the QCOW2 (QEMU Copy-On-Write version 2) disk image format.

## Goals

- **Definitive Go Implementation**: Production-quality QCOW2 reader/writer
- **Pure Go**: No CGo dependencies, single static binary
- **Go-Idiomatic**: Implements `io.ReaderAt` and `io.WriterAt`
- **Performance**: Zero-allocation hot paths, efficient L2 caching
- **Future**: Integration with [go-ublk](https://github.com/bpf-examples/go-ublk) for qemu-nbd style experience

## Installation

```bash
go get github.com/ehrlich-b/go-qcow2
```

## Usage

```go
package main

import (
    "log"
    qcow2 "github.com/ehrlich-b/go-qcow2"
)

func main() {
    // Open existing image
    img, err := qcow2.Open("disk.qcow2")
    if err != nil {
        log.Fatal(err)
    }
    defer img.Close()

    // Read data (implements io.ReaderAt)
    buf := make([]byte, 4096)
    _, err = img.ReadAt(buf, 0)
    if err != nil {
        log.Fatal(err)
    }

    // Write data (implements io.WriterAt)
    _, err = img.WriteAt([]byte("Hello, QCOW2!"), 0)
    if err != nil {
        log.Fatal(err)
    }

    // Sync to disk
    img.Flush()
}
```

## Architecture

### QCOW2 Format Overview

QCOW2 is a virtual disk format that supports:
- **Copy-on-Write**: Only modified clusters are stored
- **Sparse Files**: Unallocated regions don't consume disk space
- **Backing Files**: Chain of images for snapshots/overlays
- **Compression**: Optional per-cluster compression
- **Encryption**: AES or LUKS encryption

### On-Disk Layout

```
+----------------+
|     Header     |  <- Offset 0
+----------------+
|   L1 Table     |  <- header.L1TableOffset
+----------------+
| Refcount Table |  <- header.RefcountTableOffset
+----------------+
|  L2 Tables     |  <- Allocated as needed
+----------------+
| Data Clusters  |  <- Allocated as needed
+----------------+
```

### Two-Level Page Table

QCOW2 uses a two-level lookup table (like x86 page tables):

```
Virtual Offset
      │
      ▼
┌─────────────────────────────────────────┐
│ L1 Index │ L2 Index │ Cluster Offset    │
└─────────────────────────────────────────┘
      │          │              │
      ▼          │              │
  ┌───────┐      │              │
  │  L1   │──────┼──────────────┤
  │ Table │      │              │
  └───────┘      ▼              │
              ┌───────┐         │
              │  L2   │─────────┤
              │ Table │         │
              └───────┘         │
                                ▼
                         Physical Offset
```

### Address Translation

For a virtual offset with 64KB clusters (cluster_bits=16):

```
l2_entries = cluster_size / 8 = 8192
l2_bits = 13

cluster_offset = virt_off & 0xFFFF           // bits 0-15
l2_index = (virt_off >> 16) & 0x1FFF         // bits 16-28
l1_index = virt_off >> 29                     // bits 29+
```

### L1/L2 Entry Format

64-bit big-endian entries:

```
┌──┬──┬───────────────────────────────┬─────────┐
│63│62│           61 - 9              │  8 - 0  │
├──┼──┼───────────────────────────────┼─────────┤
│C │Z │    Host Offset (aligned)      │Reserved │
└──┴──┴───────────────────────────────┴─────────┘

Bit 63 (C): COPIED flag - refcount is exactly 1
Bit 62 (Z): Compressed (L2 only) / Reserved (L1)
```

## Current Status

**Status: Feature Complete (Beta)**

Comprehensive QCOW2 support with extensive testing against qemu-img.

### Core Features
- [x] Header parsing and validation (v2 and v3)
- [x] L1/L2 table address translation
- [x] `io.ReaderAt` and `io.WriterAt` interfaces
- [x] LRU cache for L2 tables
- [x] Cluster allocation with free-space tracking
- [x] Image creation (`Create()`)

### Advanced Features
- [x] Backing file support (COW chains, raw + qcow2)
- [x] Refcount table management (variable width: 1-64 bits)
- [x] Lazy refcounts with automatic rebuild
- [x] Compression (deflate + zstd)
- [x] Encryption (legacy AES read-only, LUKS1/LUKS2 read/write)
- [x] Snapshots (create, delete, revert, read)
- [x] Extended L2 entries (32 subclusters)
- [x] External data files
- [x] Dirty tracking bitmaps (incremental backup support)
- [x] Zero clusters (space-efficient zeroing)
- [x] Write ordering barriers (configurable safety levels)

### Not Yet Implemented
- [ ] io_uring backend
- [ ] Direct I/O (O_DIRECT)
- [ ] go-ublk integration

## QCOW2 Header Fields

| Offset | Size | Name | Description |
|--------|------|------|-------------|
| 0 | 4 | magic | `0x514649fb` ("QFI\xfb") |
| 4 | 4 | version | 2 or 3 |
| 8 | 8 | backing_file_offset | Offset to backing file name |
| 16 | 4 | backing_file_size | Length of backing file name |
| 20 | 4 | cluster_bits | Log2 of cluster size (9-21) |
| 24 | 8 | size | Virtual disk size in bytes |
| 32 | 4 | crypt_method | 0=none, 1=AES, 2=LUKS |
| 36 | 4 | l1_size | Number of L1 table entries |
| 40 | 8 | l1_table_offset | Offset of L1 table |
| 48 | 8 | refcount_table_offset | Offset of refcount table |
| 56 | 4 | refcount_table_clusters | Size of refcount table |
| 60 | 4 | nb_snapshots | Number of snapshots |
| 64 | 8 | snapshots_offset | Offset of snapshot table |

### Version 3 Extensions (offset 72+)

| Offset | Size | Name | Description |
|--------|------|------|-------------|
| 72 | 8 | incompatible_features | Must understand to open |
| 80 | 8 | compatible_features | Can safely ignore |
| 88 | 8 | autoclear_features | Cleared on RW open |
| 96 | 4 | refcount_order | Log2 of refcount bits |
| 100 | 4 | header_length | Total header length |

## Feature Bits

### Incompatible Features
- Bit 0: Dirty - Image was not cleanly closed
- Bit 1: Corrupt - Image is corrupt
- Bit 2: External data file
- Bit 3: Compression type in header
- Bit 4: Extended L2 entries

### Compatible Features
- Bit 0: Lazy refcounts - Defer refcount updates

## Testing

Create test images with qemu-img:

```bash
# Create 1GB sparse image
qemu-img create -f qcow2 test.qcow2 1G

# Create with specific cluster size
qemu-img create -f qcow2 -o cluster_size=64K test.qcow2 1G

# Check image integrity
qemu-img check test.qcow2

# Get image info
qemu-img info test.qcow2
```

## References

- [QCOW2 Specification](https://github.com/qemu/qemu/blob/master/docs/interop/qcow2.txt)
- [QEMU Source](https://github.com/qemu/qemu/blob/master/block/qcow2.c)
- [KVM Forum 2017: Improving QCOW2 Performance](https://www.youtube.com/watch?v=JtqfRS1hLWQ)

## License

MIT
