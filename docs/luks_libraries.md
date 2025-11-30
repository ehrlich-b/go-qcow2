# LUKS Library Options for go-qcow2

This document compares options for implementing LUKS encryption support in go-qcow2.

## Background: What LUKS Actually Is

LUKS (Linux Unified Key Setup) is **not** an encryption algorithm - it's a key management specification:

1. **Header format** - Stores encrypted master keys, salt, iteration counts
2. **Key derivation** - PBKDF2 (v1/v2) or Argon2i/Argon2id (v2 only)
3. **Anti-forensic splitting** - 4000 stripes to make secure key deletion possible
4. **Multiple key slots** - Up to 8 (v1) or 32 (v2) passwords can unlock the same volume

The actual data encryption uses standard ciphers like AES-XTS-PLAIN64.

## Option 1: containers/luksy

**Repository:** [github.com/containers/luksy](https://github.com/containers/luksy)

### Pros
- **Pure Go** - No cgo dependencies
- **Designed for our use case** - Built for offline encryption without device mapper
- **Full cipher support**: AES, Twofish, Serpent, CAST5
- **Full mode support**: XTS-PLAIN64, XTS-PLAIN, CBC-PLAIN64, CBC-PLAIN, CBC-ESSIV:SHA256
- **Both LUKS versions** - v1 and v2 supported
- **Argon2 support** - Argon2i and Argon2id for LUKS2
- **Active maintenance** - containers org, 205 commits
- **Apache 2.0 license** - Compatible with our MIT license

### Cons
- **More dependencies** - Pulls in serpent, uuid, logrus, cobra, testify, x/crypto
- **Heavier than needed** - Includes CLI tooling we don't need
- **205 stars/8 forks** - Relatively small user base
- **README warns** "use cryptsetup instead" when possible

### Dependencies
```
github.com/aead/serpent      # Serpent cipher
github.com/google/uuid       # UUID generation
github.com/sirupsen/logrus   # Logging (CLI only)
github.com/spf13/cobra       # CLI framework (CLI only)
golang.org/x/crypto          # PBKDF2, Argon2, twofish, cast5, xts
golang.org/x/term            # Terminal (CLI only)
```

### API for QCOW2 Integration
```go
// Read LUKS header from our QCOW2 file offset
v1hdr, v2hdr, v2json, err := luksy.ReadHeaders(sectionReader)

// Decrypt to get cipher function
cipherFn, blockSize, payloadOffset, payloadSize, err := v1hdr.Decrypt(password, reader)

// Use cipherFn to decrypt sectors
plaintext, err := cipherFn(ciphertext)
```

---

## Option 2: anatol/luks.go

**Repository:** [github.com/anatol/luks.go](https://github.com/anatol/luks.go)

### Pros
- **Pure Go** - No cgo dependencies
- **Well-tested** - Includes VM tests against real cryptsetup
- **Full LUKS2 support** - Including Argon2
- **Cleaner API** - Focused on key management

### Cons
- **Device path only** - `Open(path string)` doesn't accept io.Reader
- **Designed for device mapper** - Returns `Volume` for setting up dm-crypt
- **Can't read from QCOW2 offset** - Would need fork/modification
- **More dependencies** - devmapper, losetup, camellia, whirlpool

### Dependencies
```
github.com/anatol/devmapper.go   # Device mapper (Linux only)
github.com/anatol/go-losetup     # Loop devices (Linux only)
go.tmthrgd.dev/go-camellia       # Camellia cipher
github.com/jzelinskie/whirlpool  # Whirlpool hash
golang.org/x/crypto              # Core crypto
golang.org/x/sys                 # System calls
```

### Would Require
To use with QCOW2, we'd need to either:
1. **Fork and modify** to accept `io.ReaderAt` instead of file path
2. **Extract LUKS to temp file** - Ugly and slow

---

## Option 3: Roll Our Own

Implement LUKS parsing and key derivation from scratch.

### Required Components

| Component | Complexity | Stdlib? |
|-----------|------------|---------|
| LUKS1 header parsing | Low | Yes (encoding/binary) |
| LUKS2 header parsing | Medium | Yes + encoding/json |
| PBKDF2 key derivation | Low | golang.org/x/crypto/pbkdf2 |
| Argon2i/id key derivation | Low | golang.org/x/crypto/argon2 |
| Anti-forensic merge | Medium | Need to implement |
| AES-XTS sector decryption | Low | golang.org/x/crypto/xts |
| CBC-PLAIN64 decryption | Low | crypto/cipher |
| CBC-ESSIV decryption | Medium | Need ESSIV IV generation |

### Lines of Code Estimate
- Header parsing: ~300 LOC
- Key derivation: ~100 LOC (mostly stdlib wrappers)
- AF merge: ~50 LOC
- Cipher modes: ~200 LOC
- **Total: ~650-800 LOC**

### Pros
- **Zero external dependencies** - Only stdlib + x/crypto
- **Exactly what we need** - No CLI bloat, no device mapper
- **Full control** - Can optimize for our io.ReaderAt use case
- **Educational** - Deep understanding of the format

### Cons
- **More initial work** - 650+ lines to write and test
- **Security risk** - Crypto code needs careful review
- **Maintenance burden** - Must track LUKS spec changes
- **Testing complexity** - Need extensive test vectors

### Key Implementation Details

**LUKS1 Header (592 bytes)**
```go
type LUKS1Header struct {
    Magic         [6]byte   // "LUKS\xba\xbe"
    Version       uint16    // 1
    CipherName    [32]byte  // e.g., "aes"
    CipherMode    [32]byte  // e.g., "xts-plain64"
    HashSpec      [32]byte  // e.g., "sha256"
    PayloadOffset uint32    // Encrypted data start (512-byte sectors)
    KeyBytes      uint32    // Master key length
    MkDigest      [20]byte  // Master key verification digest
    MkDigestSalt  [32]byte  // Salt for digest
    MkDigestIter  uint32    // PBKDF2 iterations for digest
    UUID          [40]byte
    KeySlots      [8]LUKS1KeySlot
}
```

**Anti-Forensic Merge** (4000 stripes)
```go
func afMerge(data []byte, keySize int, stripes int, hash func() hash.Hash) []byte {
    // XOR and diffuse stripes to recover original key
    // Diffusion uses hash(index || block) iteratively
}
```

---

## Option 4: Minimal Hybrid

Use **x/crypto** primitives + minimal custom code for header parsing.

### Dependencies
```
golang.org/x/crypto/pbkdf2   # Already have via zstd
golang.org/x/crypto/argon2   # Small addition
golang.org/x/crypto/xts      # Small addition
```

### Implementation
- Write LUKS1/2 header parsing (~300 LOC)
- Write AF merge (~50 LOC)
- Use stdlib for everything else

### Pros
- Minimal dependencies (x/crypto only, which we may already have)
- Full control over io.ReaderAt integration
- No CLI/logging bloat

### Cons
- Still need to write/test crypto-adjacent code
- Less battle-tested than luksy

---

## Recommendation

**Short term:** Use **containers/luksy**
- Already handles all the complexity
- Well-maintained by containers org
- Pure Go with reasonable dependencies
- Supports our io.Reader use case with `ReadHeaders()`

**Long term consideration:** Evaluate after LUKS support ships
- If luksy dependencies become problematic, extract minimal implementation
- The core logic is ~650 LOC, not insurmountable

### Integration Approach with luksy

```go
// In Image struct
type Image struct {
    // ...existing fields...
    luksDecrypt func([]byte) ([]byte, error)  // Sector decryption function
    luksSectorSize int
    luksPayloadOffset uint64
}

// SetPasswordLUKS unlocks a LUKS-encrypted image
func (img *Image) SetPasswordLUKS(password string) error {
    if img.header.EncryptMethod != EncryptionLUKS {
        return errors.New("not a LUKS encrypted image")
    }

    // Get LUKS header location from extension
    ext := img.extensions.EncryptionHeader
    if ext == nil {
        return errors.New("missing encryption header extension")
    }

    // Create section reader at LUKS header offset
    reader := io.NewSectionReader(img.file, int64(ext.Offset), int64(ext.Length))

    // Parse LUKS headers
    v1hdr, v2hdr, v2json, err := luksy.ReadHeaders(reader)
    // ... decrypt and store cipher function
}
```

---

## Critical Discovery: luksy Doesn't Support Random Access

**Date:** 2024-11-29

### The Problem

After implementing and testing with luksy, we discovered a **fundamental incompatibility**:

```go
// luksy's Decrypt() returns this:
func([]byte) ([]byte, error)  // Decrypts sectors IN SUCCESSION
```

The decrypt function **auto-increments an internal sector counter**. It's designed for **streaming** decryption of sequential sectors, NOT random access.

### Why This Breaks QCOW2

QCOW2 stores data in **scattered clusters** at arbitrary physical offsets:

```
QCOW2 Layout:
┌─────────────────┐
│ Header          │ offset 0
├─────────────────┤
│ LUKS Header     │ offset 0x10000  (from extension)
├─────────────────┤
│ L1/L2 Tables    │ various offsets
├─────────────────┤
│ Data Cluster A  │ offset 0x50000  ← need sector 0x50000/512 = 640
├─────────────────┤
│ Data Cluster B  │ offset 0x80000  ← need sector 0x80000/512 = 1024
└─────────────────┘
```

When reading cluster at physical offset X, we need to decrypt with **sector number X/512** as the IV tweak. luksy can't do this - it always starts at sector 0 and increments.

### What luksy Doesn't Expose

The ideal solution would be to use luksy for key derivation, then use `x/crypto/xts` directly:

```go
// What we WANT to do:
masterKey := luksy.DeriveMasterKey(password, header)  // ❌ NOT EXPOSED
cipher, _ := xts.NewCipher(aes.NewCipher, masterKey)
cipher.Decrypt(plaintext, ciphertext, physicalSector)  // Random access!
```

But luksy's `Decrypt()` does PBKDF2 + AF merge internally and **only returns the closure**, not the master key.

### Test Results

```
=== RUN   TestLUKSEncryptedImage
    luks_test.go:77: Data mismatch: first bytes got 726f10e745d5b9843efae3bec5973934, want 0xAB
--- FAIL: TestLUKSEncryptedImage (9.17s)
=== RUN   TestLUKSWrongPassword
--- PASS: TestLUKSWrongPassword (6.80s)  ← Password verification works!
```

The wrong password test passes (luksy correctly rejects bad passwords), but data decryption fails because we're decrypting with wrong sector numbers.

---

## Revised Decision: Hybrid Approach

**Date:** 2024-11-29 (updated)

### New Plan

Use **luksy for header parsing only**, implement key derivation + XTS ourselves:

```
luksy                          Our code
┌─────────────────┐            ┌─────────────────────────────┐
│ ReadHeaders()   │ ────────►  │ LUKS1/2 header structs      │
│ CipherName()    │            │ CipherMode, KeyBytes, etc   │
│ CipherMode()    │            └─────────────────────────────┘
│ HashSpec()      │                        │
│ KeySlot()       │                        ▼
└─────────────────┘            ┌─────────────────────────────┐
                               │ Our PBKDF2 + AF Merge       │
                               │ (using x/crypto primitives) │
                               └─────────────────────────────┘
                                           │
                                           ▼
                               ┌─────────────────────────────┐
                               │ x/crypto/xts.Cipher         │
                               │ .Decrypt(plain, cipher, N)  │
                               │ Random access by sector!    │
                               └─────────────────────────────┘
```

### Components to Implement

| Component | Source | LOC |
|-----------|--------|-----|
| Header parsing | luksy (reuse) | 0 |
| Key slot reading | luksy V1KeySlot/V2JSON | 0 |
| PBKDF2 derivation | x/crypto/pbkdf2 | ~20 |
| Argon2 derivation | x/crypto/argon2 | ~20 |
| AF merge | Implement (spec is simple) | ~50 |
| XTS decryption | x/crypto/xts | ~30 |
| **Total new code** | | **~120 LOC** |

### Why This Works

1. **luksy still useful** - Handles complex header parsing, JSON for LUKS2
2. **We control the cipher** - Can call `xts.Decrypt(plain, cipher, sectorNum)` with any sector
3. **Minimal new code** - Only ~120 LOC for key derivation + AF merge
4. **Standard primitives** - x/crypto is well-audited

### Anti-Forensic Merge Algorithm

The one piece we need to implement (from LUKS spec):

```go
func afMerge(splitKey []byte, keyLen, stripes int, h func() hash.Hash) []byte {
    // splitKey is stripes * keyLen bytes
    // XOR and diffuse each stripe to recover original key
    d := make([]byte, keyLen)
    for i := 0; i < stripes-1; i++ {
        stripe := splitKey[i*keyLen : (i+1)*keyLen]
        for j := range d {
            d[j] ^= stripe[j]
        }
        d = afDiffuse(d, h)  // Hash-based diffusion
    }
    // XOR with final stripe
    lastStripe := splitKey[(stripes-1)*keyLen:]
    for j := range d {
        d[j] ^= lastStripe[j]
    }
    return d
}
```

### Trade-offs

**Pros:**
- Random access decryption works
- Still leverage luksy for header parsing
- Only ~120 LOC of new crypto code
- Uses well-audited x/crypto primitives

**Cons:**
- Must implement AF merge ourselves
- Must carefully match LUKS spec for key derivation
- Two "sources of truth" for LUKS (luksy headers + our key derivation)

### Alternative: Fork luksy

Could fork luksy and add `ExportMasterKey()` method. But:
- Maintenance burden of fork
- Would need to upstream or maintain indefinitely
- Our hybrid approach is cleaner

---

## Implementation Status

- [x] Header extension parsing (0x0537be77)
- [x] luksy integration for header reading
- [ ] PBKDF2 key derivation for key slots
- [ ] Argon2 key derivation for LUKS2
- [ ] AF merge implementation
- [ ] XTS random-access decryption
- [ ] Integration tests with qemu-img images

---

## References

- [QCOW2 Encryption Spec](https://www.qemu.org/docs/master/interop/qcow2.html)
- [LUKS1 On-Disk Format](https://gitlab.com/cryptsetup/cryptsetup/-/wikis/LUKS-standard/on-disk-format.pdf)
- [LUKS2 On-Disk Format](https://gitlab.com/cryptsetup/LUKS2-docs)
- [containers/luksy](https://github.com/containers/luksy)
- [anatol/luks.go](https://github.com/anatol/luks.go)
- [golang.org/x/crypto/xts](https://pkg.go.dev/golang.org/x/crypto/xts)
- [LUKS Wikipedia](https://en.wikipedia.org/wiki/Linux_Unified_Key_Setup)
- [Baeldung LUKS1 vs LUKS2](https://www.baeldung.com/linux/luks1-vs-luks2)
