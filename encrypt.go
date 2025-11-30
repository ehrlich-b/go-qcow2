package qcow2

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
)

// AESDecryptor handles legacy AES-128-CBC decryption for QCOW2 images.
// This implements the deprecated encryption method 1, which is insecure
// and should only be used for reading legacy encrypted images.
//
// Security warnings:
//   - Password is used directly as key (no PBKDF)
//   - Predictable IVs based on sector number
//   - Vulnerable to chosen plaintext attacks
//
// This implementation exists only to allow data recovery from legacy images.
type AESDecryptor struct {
	cipher cipher.Block
}

// NewAESDecryptor creates a decryptor from a password.
// The password is truncated or zero-padded to 16 bytes (AES-128 key).
func NewAESDecryptor(password string) (*AESDecryptor, error) {
	// Key derivation: password directly copied to 16-byte buffer
	// This is intentionally weak - matches QEMU's broken implementation
	key := make([]byte, 16)
	copy(key, password)

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("qcow2: failed to create AES cipher: %w", err)
	}

	return &AESDecryptor{cipher: block}, nil
}

// DecryptSector decrypts a 512-byte sector at the given sector number.
// The IV is generated using PLAIN64: sector number as little-endian uint64, zero-padded.
func (d *AESDecryptor) DecryptSector(ciphertext []byte, sectorNum uint64) ([]byte, error) {
	if len(ciphertext) != 512 {
		return nil, fmt.Errorf("qcow2: AES decrypt requires 512-byte sector, got %d", len(ciphertext))
	}

	// Generate IV using PLAIN64: sector number as little-endian uint64, zero-padded
	iv := make([]byte, aes.BlockSize)
	binary.LittleEndian.PutUint64(iv, sectorNum)

	// Create CBC decrypter
	mode := cipher.NewCBCDecrypter(d.cipher, iv)

	// Decrypt in place
	plaintext := make([]byte, len(ciphertext))
	mode.CryptBlocks(plaintext, ciphertext)

	return plaintext, nil
}

// DecryptCluster decrypts a full cluster of encrypted data.
// The cluster is divided into 512-byte sectors, each decrypted separately.
// clusterOffset is the byte offset of the cluster in the virtual disk.
func (d *AESDecryptor) DecryptCluster(ciphertext []byte, clusterOffset uint64) ([]byte, error) {
	if len(ciphertext)%512 != 0 {
		return nil, fmt.Errorf("qcow2: cluster size must be multiple of 512, got %d", len(ciphertext))
	}

	plaintext := make([]byte, len(ciphertext))
	startSector := clusterOffset / 512

	for i := 0; i < len(ciphertext); i += 512 {
		sectorNum := startSector + uint64(i/512)
		decrypted, err := d.DecryptSector(ciphertext[i:i+512], sectorNum)
		if err != nil {
			return nil, err
		}
		copy(plaintext[i:], decrypted)
	}

	return plaintext, nil
}

// SetPassword sets the password for an encrypted image.
// Must be called before reading from an encrypted image.
// Returns an error if the image is not encrypted or if key setup fails.
func (img *Image) SetPassword(password string) error {
	if img.header.EncryptMethod != EncryptionAES {
		return fmt.Errorf("qcow2: SetPassword called on non-AES encrypted image (method=%d)", img.header.EncryptMethod)
	}

	decryptor, err := NewAESDecryptor(password)
	if err != nil {
		return err
	}

	img.aesDecryptor = decryptor
	return nil
}

// readEncrypted reads and decrypts data from an encrypted cluster.
func (img *Image) readEncrypted(buf []byte, physOff, virtOff uint64) (int, error) {
	if img.aesDecryptor == nil {
		return 0, fmt.Errorf("qcow2: encrypted image requires password (call SetPassword)")
	}

	// Read the encrypted cluster data
	clusterStart := physOff & ^img.offsetMask
	clusterOff := physOff - clusterStart
	clusterVirtStart := virtOff & ^img.offsetMask

	// Read full cluster
	encrypted := make([]byte, img.clusterSize)
	_, err := img.dataFile().ReadAt(encrypted, int64(clusterStart))
	if err != nil {
		return 0, fmt.Errorf("qcow2: failed to read encrypted cluster: %w", err)
	}

	// Decrypt the cluster
	decrypted, err := img.aesDecryptor.DecryptCluster(encrypted, clusterVirtStart)
	if err != nil {
		return 0, fmt.Errorf("qcow2: decryption failed: %w", err)
	}

	// Copy requested portion to buffer
	n := copy(buf, decrypted[clusterOff:])
	return n, nil
}
