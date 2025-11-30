# LUKS Decryption Design for go-qcow2

## Overview

This document describes the design for LUKS encryption support in go-qcow2. LUKS (Linux Unified Key Setup) is the modern encryption method for QCOW2 images (crypt_method=2).

**Key insight**: Standard LUKS libraries are designed for streaming decryption of block devices. QCOW2 requires **random-access** decryption of scattered clusters. This requires a hybrid approach.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         QCOW2 Image                                 │
├─────────────────────────────────────────────────────────────────────┤
│  Header (crypt_method=2)                                            │
│  Extension 0x0537be77 → offset=0x10000, length=0x200000             │
├─────────────────────────────────────────────────────────────────────┤
│  @ 0x10000: LUKS Header                                             │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  Magic: LUKS\xba\xbe                                          │  │
│  │  Version: 1 or 2                                              │  │
│  │  Cipher: aes-xts-plain64                                      │  │
│  │  Hash: sha256                                                 │  │
│  │  Key Slots [0-7]: encrypted master key material               │  │
│  │  Master Key Digest: for password verification                 │  │
│  └───────────────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────────┤
│  @ various offsets: Encrypted Data Clusters                         │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                             │
│  │Cluster A│  │Cluster B│  │Cluster C│  ...                        │
│  │@ 0x50000│  │@ 0x80000│  │@ 0xA0000│                             │
│  └─────────┘  └─────────┘  └─────────┘                             │
└─────────────────────────────────────────────────────────────────────┘
```

## The Random Access Problem

### Standard LUKS Usage (dm-crypt)
```
Sector 0 → Sector 1 → Sector 2 → Sector 3 → ...
   ↓          ↓          ↓          ↓
Decrypt   Decrypt   Decrypt   Decrypt
(IV=0)    (IV=1)    (IV=2)    (IV=3)
```

Sequential. IV increments automatically.

### QCOW2 Usage (scattered clusters)
```
Read cluster @ physical offset 0x50000:
  → Need to decrypt with IV = 0x50000 / 512 = 640

Read cluster @ physical offset 0x80000:
  → Need to decrypt with IV = 0x80000 / 512 = 1024

Read cluster @ physical offset 0x50000 again:
  → Need IV = 640 again (random access!)
```

Non-sequential. Must specify IV for each decryption.

### Why luksy Doesn't Work

luksy's `Decrypt()` returns a closure that auto-increments an internal sector counter:

```go
// Internal to luksy
ivTweak := uint64(0)
return func(ciphertext []byte) ([]byte, error) {
    plaintext := decrypt(ciphertext, ivTweak)
    ivTweak++  // ← Can't control this!
    return plaintext, nil
}
```

We need:
```go
cipher.Decrypt(plaintext, ciphertext, sectorNumber)  // Specify sector!
```

## Solution: Hybrid Approach

### Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    SetPasswordLUKS(password)                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 1: Parse LUKS Header (using luksy)                        │
│  ─────────────────────────────────────────                      │
│  v1hdr, v2hdr, _, v2json := luksy.ReadHeaders(reader)           │
│                                                                 │
│  Extract:                                                       │
│  - CipherName: "aes"                                            │
│  - CipherMode: "xts-plain64"                                    │
│  - HashSpec: "sha256"                                           │
│  - KeyBytes: 64 (512-bit for AES-256-XTS)                       │
│  - KeySlots[N]: salt, iterations, key material offset           │
│  - MkDigest, MkDigestSalt, MkDigestIter: for verification       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 2: Key Derivation (our implementation)                    │
│  ───────────────────────────────────────────                    │
│  For each active key slot:                                      │
│                                                                 │
│  LUKS1:                                                         │
│    afKey = PBKDF2(password, slot.Salt, slot.Iterations, keyLen) │
│                                                                 │
│  LUKS2:                                                         │
│    afKey = Argon2id(password, slot.Salt, time, mem, threads)    │
│         or PBKDF2 (depending on keyslot config)                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 3: Decrypt Key Material                                   │
│  ────────────────────────────                                   │
│  Read encrypted key material from slot.KeyMaterialOffset        │
│  Decrypt using XTS with afKey (sector 0, 1, 2, ...)             │
│  Result: splitKey (stripes * keyLen bytes)                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 4: Anti-Forensic Merge                                    │
│  ───────────────────────────                                    │
│  masterKey = afMerge(splitKey, keyLen, stripes=4000, sha256)    │
│                                                                 │
│  Algorithm:                                                     │
│    d = zeros(keyLen)                                            │
│    for i in 0..stripes-2:                                       │
│      d ^= splitKey[i]                                           │
│      d = diffuse(d)  // hash-based diffusion                    │
│    d ^= splitKey[stripes-1]                                     │
│    return d                                                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 5: Verify Master Key                                      │
│  ─────────────────────────                                      │
│  digest = PBKDF2(masterKey, MkDigestSalt, MkDigestIter, 20)     │
│  if digest != MkDigest:                                         │
│      return ErrWrongPassword                                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 6: Create XTS Cipher                                      │
│  ─────────────────────────                                      │
│  cipher = xts.NewCipher(aes.NewCipher, masterKey)               │
│  Store cipher in Image struct for later use                     │
└─────────────────────────────────────────────────────────────────┘


┌─────────────────────────────────────────────────────────────────┐
│                    ReadAt(buf, offset)                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Translate virtual offset → physical cluster offset             │
│  Read encrypted cluster from disk                               │
│                                                                 │
│  For each 512-byte sector in cluster:                           │
│    sectorNum = physicalOffset / 512                             │
│    cipher.Decrypt(plaintext, ciphertext, sectorNum)             │
│                                                                 │
│  Return decrypted data                                          │
└─────────────────────────────────────────────────────────────────┘
```

## Data Structures

### LUKSDecryptor

```go
type LUKSDecryptor struct {
    cipher     *xts.Cipher  // AES-XTS cipher with master key
    sectorSize int          // 512 bytes typically
}

func (d *LUKSDecryptor) DecryptSector(ciphertext []byte, sectorNum uint64) ([]byte, error) {
    plaintext := make([]byte, d.sectorSize)
    d.cipher.Decrypt(plaintext, ciphertext, sectorNum)
    return plaintext, nil
}

func (d *LUKSDecryptor) DecryptCluster(ciphertext []byte, physOffset uint64) ([]byte, error) {
    plaintext := make([]byte, len(ciphertext))
    startSector := physOffset / uint64(d.sectorSize)

    for i := 0; i < len(ciphertext); i += d.sectorSize {
        sectorNum := startSector + uint64(i/d.sectorSize)
        d.cipher.Decrypt(plaintext[i:i+d.sectorSize], ciphertext[i:i+d.sectorSize], sectorNum)
    }
    return plaintext, nil
}
```

### Image Integration

```go
type Image struct {
    // ... existing fields ...

    // LUKS decryptor for modern encrypted images (method=2)
    luksDecryptor *LUKSDecryptor
}

func (img *Image) SetPasswordLUKS(password string) error {
    // 1. Get LUKS header location from extension
    // 2. Parse LUKS header using luksy
    // 3. Derive master key (PBKDF2/Argon2 + AF merge)
    // 4. Verify master key against digest
    // 5. Create XTS cipher and store in luksDecryptor
}
```

## Anti-Forensic Splitting

LUKS uses anti-forensic splitting to make secure key deletion possible. The master key is "bloated" across 4000 stripes so that destroying even one bit makes recovery impossible.

### AFSplit (for encryption - not needed for read-only)
```
masterKey (32 bytes) → splitKey (32 * 4000 = 128000 bytes)
```

### AFMerge (for decryption - we need this)
```
splitKey (128000 bytes) → masterKey (32 bytes)

Algorithm:
1. Start with d = zeros(keyLen)
2. For each stripe except the last:
   a. XOR stripe into d
   b. Apply diffusion function to d
3. XOR final stripe into d
4. Result is the master key
```

### Diffusion Function

```go
func afDiffuse(data []byte, h func() hash.Hash) []byte {
    result := make([]byte, len(data))
    hashSize := h().Size()

    // Process in hash-sized blocks
    for i := 0; i < len(data); i += hashSize {
        hasher := h()
        // Write block index as big-endian uint32
        binary.Write(hasher, binary.BigEndian, uint32(i/hashSize))
        // Write the data block
        end := i + hashSize
        if end > len(data) {
            end = len(data)
        }
        hasher.Write(data[i:end])
        copy(result[i:], hasher.Sum(nil))
    }
    return result
}
```

## Cipher Mode Details

### AES-XTS-PLAIN64 (default for QEMU LUKS)

- **AES-256**: 256-bit key (but XTS needs double, so 512-bit total)
- **XTS mode**: IEEE P1619 disk encryption mode
- **PLAIN64**: IV = sector number as 64-bit little-endian

```go
// Key is 64 bytes (512 bits) for AES-256-XTS
// First 32 bytes: encryption key
// Second 32 bytes: tweak key
cipher, _ := xts.NewCipher(aes.NewCipher, masterKey)

// Decrypt sector with sector number as tweak
cipher.Decrypt(plaintext, ciphertext, sectorNum)
```

### IV Generation for QCOW2

QEMU's QCOW2 LUKS uses **physical host sector** for IV:

```
Physical offset 0x50000, sector size 512:
IV = 0x50000 / 512 = 0x280 = 640

Physical offset 0x50200 (next sector in same cluster):
IV = 0x50200 / 512 = 0x281 = 641
```

This differs from legacy AES encryption which uses **virtual** sector numbers.

## LUKS1 vs LUKS2

### LUKS1
- Fixed header structure (592 bytes)
- PBKDF2 only for key derivation
- 8 key slots max
- Simpler to parse

### LUKS2
- JSON metadata (variable size, up to 4MB)
- Argon2i/Argon2id support (memory-hard KDF)
- 32 key slots max
- More complex but more secure

We'll support both, prioritizing LUKS1 initially since it's simpler and LUKS2 falls back to PBKDF2 for GRUB compatibility anyway.

## Security Considerations

1. **Master key in memory**: The decrypted master key must stay in memory while the image is open. Consider using `mlock()` to prevent swapping.

2. **Key derivation cost**: PBKDF2 iterations are intentionally high (>100k). First unlock will be slow (~1-5 seconds).

3. **No write support initially**: Writing encrypted data requires careful IV handling. Read-only is safer to implement first.

4. **Audit requirement**: The AF merge and key derivation code handles sensitive cryptographic material. Should be reviewed.

## Implementation Plan

### Phase 1: LUKS1 Read-Only
1. [ ] Implement PBKDF2 key derivation wrapper
2. [ ] Implement AF merge algorithm
3. [ ] Implement master key verification
4. [ ] Integrate with x/crypto/xts for decryption
5. [ ] Test with qemu-img LUKS1 images

### Phase 2: LUKS2 Read-Only
1. [ ] Add Argon2 key derivation
2. [ ] Parse LUKS2 JSON metadata via luksy
3. [ ] Handle LUKS2-specific key slot format
4. [ ] Test with qemu-img LUKS2 images

### Phase 3: Write Support (Future)
1. [ ] Implement encrypted cluster writes
2. [ ] Handle IV generation for new clusters
3. [ ] Ensure write barriers for encrypted data

## Dependencies

```
golang.org/x/crypto/pbkdf2   # Key derivation
golang.org/x/crypto/argon2   # LUKS2 key derivation (optional)
golang.org/x/crypto/xts      # Disk encryption mode
crypto/aes                   # AES block cipher (stdlib)
crypto/sha256               # Hash for PBKDF2/AF (stdlib)
github.com/containers/luksy  # Header parsing only
```

## Test Plan

1. **Unit tests**: AF merge with known test vectors
2. **Integration tests**:
   - Create LUKS1 image with qemu-img, write pattern, verify read
   - Create LUKS2 image with qemu-img, write pattern, verify read
   - Wrong password rejection
   - Multiple key slots
3. **Fuzz testing**: Malformed LUKS headers

## References

- [LUKS1 On-Disk Format Spec](https://gitlab.com/cryptsetup/cryptsetup/-/wikis/LUKS-standard/on-disk-format.pdf)
- [LUKS2 On-Disk Format](https://gitlab.com/cryptsetup/LUKS2-docs)
- [QEMU QCOW2 Encryption](https://www.qemu.org/docs/master/interop/qcow2.html)
- [IEEE P1619 XTS-AES](https://en.wikipedia.org/wiki/Disk_encryption_theory#XEX-based_tweaked-codebook_mode_with_ciphertext_stealing_(XTS))
- [Anti-Forensic Information Splitter](https://clemens.endorphin.org/LUKS-on-disk-format.pdf)
