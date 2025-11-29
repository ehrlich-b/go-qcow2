# Claude Code Guidance for go-qcow2

## Project Overview
A pure Go implementation of the QCOW2 (QEMU Copy-On-Write version 2) disk image format. The goal is to be the definitive Go library for QCOW2, providing idiomatic reader/writer abstractions.

## Key Files
- `format.go` - QCOW2 on-disk format constants, header struct, parsing
- `qcow2.go` - Main Image type with ReadAt/WriteAt implementation
- `cache.go` - LRU cache for L2 tables
- `create.go` - Image creation (TODO)

## Build & Test
Always use `make` commands when available:
```bash
make build            # Build the library
make test             # Run tests
make test-race        # Run tests with race detector
make test-cover       # Run tests with coverage
make check            # Run all checks (fmt, vet, build, test)
make fmt              # Format code
make help             # Show all available targets
```

## Architecture Decisions

### Zero-Struct Hot Path
L2 tables are kept as `[]byte` slices, not Go structs. Access via `binary.BigEndian.Uint64` with offset math. This avoids GC pressure on the read/write hot path.

### L2 Cache
Simple LRU cache holding recently-used L2 tables. Default 32 entries = 2MB with 64KB clusters. The cache returns copies to avoid races.

### Address Translation
```
Virtual Offset -> L1 Index -> L2 Table -> L2 Index -> Physical Offset

l2Index = (virtOff >> clusterBits) & (l2Entries - 1)
l1Index = virtOff >> (clusterBits + l2Bits)
```

### Cluster Allocation (Simple)
Currently uses file-end allocation - grows the file for each new cluster. Future: free-space tracking with refcount tables.

## QCOW2 Format Reference

### Header (offset 0)
| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | Magic (0x514649fb = "QFI\xfb") |
| 4 | 4 | Version (2 or 3) |
| 8 | 8 | Backing file offset |
| 16 | 4 | Backing file size |
| 20 | 4 | Cluster bits (9-21, typically 16 = 64KB) |
| 24 | 8 | Virtual size |
| 32 | 4 | Encryption method |
| 36 | 4 | L1 table size (entries) |
| 40 | 8 | L1 table offset |
| 48 | 8 | Refcount table offset |
| 56 | 4 | Refcount table clusters |
| 60 | 4 | Number of snapshots |
| 64 | 8 | Snapshots offset |
| 72+ | ... | V3 extension fields |

### L1/L2 Entry Format (64-bit, big-endian)
```
Bit 63: COPIED flag (refcount == 1)
Bit 62: Compressed flag (L2 only)
Bits 9-61: Host cluster offset (512-byte aligned)
Bits 0-8: Reserved
```

### Key Constants
- Magic: `0x514649fb`
- Default cluster: 64KB (1 << 16)
- L2 entries per table: cluster_size / 8
- Entry size: 8 bytes

### Address Translation Math
```go
clusterBits := header.ClusterBits        // e.g., 16
l2Bits := clusterBits - 3                 // 13 (8192 entries)
l2Entries := 1 << l2Bits                  // 8192
clusterSize := 1 << clusterBits           // 65536

// For virtual offset 'off':
clusterOffset := off & (clusterSize - 1)  // Offset within cluster
l2Index := (off >> clusterBits) & (l2Entries - 1)
l1Index := off >> (clusterBits + l2Bits)
```

## Code Style
- Error wrapping with `%w` for context
- Prefer `uint64` for offsets (QCOW2 uses 64-bit offsets)
- Big-endian byte order for all on-disk structures
- Use `sync.RWMutex` for concurrent access

## Testing Strategy
- Unit tests for header parsing with known-good images
- Create test images with `qemu-img create`
- Verify read/write round-trips
- Compare output with `qemu-img check`

## Dependencies
None - pure Go standard library only.

## Related Resources
- [QCOW2 Spec](https://github.com/qemu/qemu/blob/master/docs/interop/qcow2.txt)
- [lima-vm/go-qcow2reader](https://github.com/lima-vm/go-qcow2reader) - Read-only reference
- [dpeckett/qcow2](https://github.com/dpeckett/qcow2) - Another Go implementation (experimental)
