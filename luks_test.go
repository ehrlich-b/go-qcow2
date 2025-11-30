package qcow2

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestLUKSEncryptedImage(t *testing.T) {
	// Skip if qemu-img not available
	if _, err := exec.LookPath("qemu-img"); err != nil {
		t.Skip("qemu-img not available")
	}

	dir := t.TempDir()
	imgPath := filepath.Join(dir, "luks.qcow2")
	password := "testpassword"

	// Create LUKS encrypted image
	cmd := exec.Command("qemu-img", "create", "-f", "qcow2",
		"-o", "encrypt.format=luks,encrypt.key-secret=sec0",
		"--object", "secret,id=sec0,data="+password,
		imgPath, "10M")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to create LUKS image: %v\n%s", err, out)
	}

	// Write test pattern using qemu-io
	cmd = exec.Command("qemu-io",
		"-c", "write -P 0xAB 0 4096",
		"--object", "secret,id=sec0,data="+password,
		"--image-opts", "driver=qcow2,file.driver=file,file.filename="+imgPath+",encrypt.key-secret=sec0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to write to LUKS image: %v\n%s", err, out)
	}

	// Open with our library
	img, err := OpenFile(imgPath, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Failed to open LUKS image: %v", err)
	}
	defer img.Close()

	// Verify it's detected as encrypted
	hdr := img.Header()
	if !hdr.IsEncrypted() {
		t.Error("Image should be detected as encrypted")
	}
	if hdr.EncryptionMethod() != EncryptionLUKS {
		t.Errorf("Expected LUKS encryption method, got %d", hdr.EncryptionMethod())
	}

	// Reading without password should fail
	buf := make([]byte, 4096)
	_, err = img.ReadAt(buf, 0)
	if err == nil {
		t.Error("Read should fail without password")
	}

	// Set password
	err = img.SetPasswordLUKS(password)
	if err != nil {
		t.Fatalf("SetPasswordLUKS failed: %v", err)
	}

	// Now read should succeed
	_, err = img.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("Read after SetPasswordLUKS failed: %v", err)
	}

	// Verify data matches pattern (0xAB)
	expected := bytes.Repeat([]byte{0xAB}, 4096)
	if !bytes.Equal(buf, expected) {
		t.Errorf("Data mismatch: first bytes got %x, want 0xAB", buf[:16])
	}
}

func TestLUKSWrongPassword(t *testing.T) {
	// Skip if qemu-img not available
	if _, err := exec.LookPath("qemu-img"); err != nil {
		t.Skip("qemu-img not available")
	}

	dir := t.TempDir()
	imgPath := filepath.Join(dir, "luks.qcow2")
	password := "correctpassword"

	// Create LUKS encrypted image
	cmd := exec.Command("qemu-img", "create", "-f", "qcow2",
		"-o", "encrypt.format=luks,encrypt.key-secret=sec0",
		"--object", "secret,id=sec0,data="+password,
		imgPath, "10M")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to create LUKS image: %v\n%s", err, out)
	}

	// Open with our library
	img, err := OpenFile(imgPath, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Failed to open LUKS image: %v", err)
	}
	defer img.Close()

	// Try wrong password
	err = img.SetPasswordLUKS("wrongpassword")
	if err == nil {
		t.Error("SetPasswordLUKS should fail with wrong password")
	}
}

func TestLUKS2EncryptedImage(t *testing.T) {
	// Skip if qemu-img not available
	if _, err := exec.LookPath("qemu-img"); err != nil {
		t.Skip("qemu-img not available")
	}

	dir := t.TempDir()
	imgPath := filepath.Join(dir, "luks2.qcow2")
	password := "testpassword"

	// Try to create LUKS2 encrypted image (requires qemu >= 5.1)
	cmd := exec.Command("qemu-img", "create", "-f", "qcow2",
		"-o", "encrypt.format=luks,encrypt.key-secret=sec0,encrypt.luks-version=2",
		"--object", "secret,id=sec0,data="+password,
		imgPath, "10M")
	out, err := cmd.CombinedOutput()
	if err != nil {
		// LUKS2 not supported by this qemu-img version
		t.Skipf("qemu-img does not support LUKS2: %s", out)
	}

	// Write test pattern using qemu-io
	cmd = exec.Command("qemu-io",
		"-c", "write -P 0xCD 0 4096",
		"--object", "secret,id=sec0,data="+password,
		"--image-opts", "driver=qcow2,file.driver=file,file.filename="+imgPath+",encrypt.key-secret=sec0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to write to LUKS2 image: %v\n%s", err, out)
	}

	// Open with our library
	img, err := OpenFile(imgPath, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Failed to open LUKS2 image: %v", err)
	}
	defer img.Close()

	// Verify it's detected as encrypted
	hdr := img.Header()
	if !hdr.IsEncrypted() {
		t.Error("Image should be detected as encrypted")
	}

	// Set password
	err = img.SetPasswordLUKS(password)
	if err != nil {
		t.Fatalf("SetPasswordLUKS failed: %v", err)
	}

	// Read and verify data
	buf := make([]byte, 4096)
	_, err = img.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("Read after SetPasswordLUKS failed: %v", err)
	}

	// Verify data matches pattern (0xCD)
	expected := bytes.Repeat([]byte{0xCD}, 4096)
	if !bytes.Equal(buf, expected) {
		t.Errorf("Data mismatch: first bytes got %x, want 0xCD", buf[:16])
	}
}

func TestLUKSEncryptedWrite(t *testing.T) {
	// Skip if qemu-img not available
	if _, err := exec.LookPath("qemu-img"); err != nil {
		t.Skip("qemu-img not available")
	}

	dir := t.TempDir()
	imgPath := filepath.Join(dir, "luks_write.qcow2")
	password := "testpassword"

	// Create LUKS encrypted image
	cmd := exec.Command("qemu-img", "create", "-f", "qcow2",
		"-o", "encrypt.format=luks,encrypt.key-secret=sec0",
		"--object", "secret,id=sec0,data="+password,
		imgPath, "10M")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to create LUKS image: %v\n%s", err, out)
	}

	// Open with our library for read/write
	img, err := OpenFile(imgPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("Failed to open LUKS image: %v", err)
	}

	// Set password
	err = img.SetPasswordLUKS(password)
	if err != nil {
		t.Fatalf("SetPasswordLUKS failed: %v", err)
	}

	// Write test pattern using our library
	testData := bytes.Repeat([]byte{0xEF}, 4096)
	_, err = img.WriteAt(testData, 0)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Flush and close
	img.Flush()
	img.Close()

	// Verify with qemu-img check
	cmd = exec.Command("qemu-img", "check",
		"--object", "secret,id=sec0,data="+password,
		"--image-opts", "driver=qcow2,file.driver=file,file.filename="+imgPath+",encrypt.key-secret=sec0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("qemu-img check failed: %v\n%s", err, out)
	}

	// Verify data with qemu-io read
	cmd = exec.Command("qemu-io",
		"-c", "read -P 0xEF 0 4096",
		"--object", "secret,id=sec0,data="+password,
		"--image-opts", "driver=qcow2,file.driver=file,file.filename="+imgPath+",encrypt.key-secret=sec0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("qemu-io read verification failed: %v\n%s", err, out)
	}
}

func TestLUKSEncryptedWritePartialCluster(t *testing.T) {
	// Skip if qemu-img not available
	if _, err := exec.LookPath("qemu-img"); err != nil {
		t.Skip("qemu-img not available")
	}

	dir := t.TempDir()
	imgPath := filepath.Join(dir, "luks_partial.qcow2")
	password := "testpassword"

	// Create LUKS encrypted image
	cmd := exec.Command("qemu-img", "create", "-f", "qcow2",
		"-o", "encrypt.format=luks,encrypt.key-secret=sec0",
		"--object", "secret,id=sec0,data="+password,
		imgPath, "10M")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to create LUKS image: %v\n%s", err, out)
	}

	// Open with our library for read/write
	img, err := OpenFile(imgPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("Failed to open LUKS image: %v", err)
	}

	// Set password
	err = img.SetPasswordLUKS(password)
	if err != nil {
		t.Fatalf("SetPasswordLUKS failed: %v", err)
	}

	// Write at non-zero offset within a cluster (partial cluster write)
	testData := []byte("Hello, LUKS encrypted world!")
	offset := int64(512) // Start at offset 512 (not cluster-aligned)
	_, err = img.WriteAt(testData, offset)
	if err != nil {
		t.Fatalf("WriteAt (partial) failed: %v", err)
	}

	// Flush and close
	img.Flush()
	img.Close()

	// Verify with qemu-img check
	cmd = exec.Command("qemu-img", "check",
		"--object", "secret,id=sec0,data="+password,
		"--image-opts", "driver=qcow2,file.driver=file,file.filename="+imgPath+",encrypt.key-secret=sec0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("qemu-img check failed: %v\n%s", err, out)
	}

	// Reopen and verify the data
	img, err = OpenFile(imgPath, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Failed to reopen LUKS image: %v", err)
	}
	defer img.Close()

	err = img.SetPasswordLUKS(password)
	if err != nil {
		t.Fatalf("SetPasswordLUKS on reopen failed: %v", err)
	}

	// Read back and verify
	buf := make([]byte, len(testData))
	_, err = img.ReadAt(buf, offset)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	if !bytes.Equal(buf, testData) {
		t.Errorf("Data mismatch: got %q, want %q", buf, testData)
	}

	// Also verify that the start of the cluster (before our write) is zeros
	zeroBuf := make([]byte, 512)
	_, err = img.ReadAt(zeroBuf, 0)
	if err != nil {
		t.Fatalf("ReadAt (zeros) failed: %v", err)
	}

	expectedZeros := make([]byte, 512)
	if !bytes.Equal(zeroBuf, expectedZeros) {
		t.Errorf("Expected zeros at start of cluster, got %x", zeroBuf[:16])
	}
}

func TestLUKSEncryptedWriteReadRoundtrip(t *testing.T) {
	// Skip if qemu-img not available
	if _, err := exec.LookPath("qemu-img"); err != nil {
		t.Skip("qemu-img not available")
	}

	dir := t.TempDir()
	imgPath := filepath.Join(dir, "luks_roundtrip.qcow2")
	password := "testpassword"

	// Create LUKS encrypted image
	cmd := exec.Command("qemu-img", "create", "-f", "qcow2",
		"-o", "encrypt.format=luks,encrypt.key-secret=sec0",
		"--object", "secret,id=sec0,data="+password,
		imgPath, "10M")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to create LUKS image: %v\n%s", err, out)
	}

	// Open with our library for read/write
	img, err := OpenFile(imgPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("Failed to open LUKS image: %v", err)
	}
	defer img.Close()

	// Set password
	err = img.SetPasswordLUKS(password)
	if err != nil {
		t.Fatalf("SetPasswordLUKS failed: %v", err)
	}

	// Write multiple patterns at different offsets
	patterns := []struct {
		offset int64
		data   []byte
	}{
		{0, bytes.Repeat([]byte{0xAA}, 4096)},
		{4096, bytes.Repeat([]byte{0xBB}, 4096)},
		{65536, bytes.Repeat([]byte{0xCC}, 4096)}, // Second cluster
		{100000, []byte("Test data in the middle")},
	}

	for _, p := range patterns {
		_, err := img.WriteAt(p.data, p.offset)
		if err != nil {
			t.Fatalf("WriteAt offset=%d failed: %v", p.offset, err)
		}
	}

	// Read back and verify each pattern
	for _, p := range patterns {
		buf := make([]byte, len(p.data))
		_, err := img.ReadAt(buf, p.offset)
		if err != nil {
			t.Fatalf("ReadAt offset=%d failed: %v", p.offset, err)
		}
		if !bytes.Equal(buf, p.data) {
			t.Errorf("Data mismatch at offset %d: got first bytes %x, want %x",
				p.offset, buf[:min(16, len(buf))], p.data[:min(16, len(p.data))])
		}
	}
}

func TestLUKSWriteWithoutPassword(t *testing.T) {
	// Skip if qemu-img not available
	if _, err := exec.LookPath("qemu-img"); err != nil {
		t.Skip("qemu-img not available")
	}

	dir := t.TempDir()
	imgPath := filepath.Join(dir, "luks_nopass.qcow2")
	password := "testpassword"

	// Create LUKS encrypted image
	cmd := exec.Command("qemu-img", "create", "-f", "qcow2",
		"-o", "encrypt.format=luks,encrypt.key-secret=sec0",
		"--object", "secret,id=sec0,data="+password,
		imgPath, "10M")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to create LUKS image: %v\n%s", err, out)
	}

	// Open with our library for read/write WITHOUT setting password
	img, err := OpenFile(imgPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("Failed to open LUKS image: %v", err)
	}
	defer img.Close()

	// Try to write without password - should fail
	testData := []byte("test")
	_, err = img.WriteAt(testData, 0)
	if err == nil {
		t.Error("WriteAt should fail without password set")
	}
}
