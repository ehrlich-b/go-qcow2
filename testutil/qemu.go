// Package testutil provides test helpers for QCOW2 testing.
package testutil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// QemuResult holds the result of a QEMU command.
type QemuResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
}

// IsSuccess returns true if the command succeeded (exit code 0).
func (r QemuResult) IsSuccess() bool {
	return r.ExitCode == 0
}

// QemuCheckResult holds parsed output from qemu-img check.
type QemuCheckResult struct {
	QemuResult
	ImageEndOffset     int64 `json:"image-end-offset"`
	TotalClusters      int64 `json:"total-clusters"`
	AllocatedClusters  int64 `json:"allocated-clusters"`
	FragmentedClusters int64 `json:"fragmented-clusters"`
	CompressedClusters int64 `json:"compressed-clusters"`
	Corruptions        int   `json:"corruptions"`
	Leaks              int   `json:"leaks"`
	LeaksClusters      int64 `json:"leaks-fixed"`
	IsClean            bool
}

// QemuInfoResult holds parsed output from qemu-img info.
type QemuInfoResult struct {
	QemuResult
	VirtualSize    int64  `json:"virtual-size"`
	Filename       string `json:"filename"`
	ClusterSize    int    `json:"cluster-size"`
	Format         string `json:"format"`
	FormatSpecific struct {
		Type string `json:"type"`
		Data struct {
			Compat        string `json:"compat"`
			LazyRefcounts bool   `json:"lazy-refcounts"`
			RefcountBits  int    `json:"refcount-bits"`
			Corrupt       bool   `json:"corrupt"`
		} `json:"data"`
	} `json:"format-specific"`
	BackingFilename string `json:"backing-filename"`
	BackingFormat   string `json:"backing-filename-format"`
}

// RequireQemu skips the test if qemu-img is not available.
func RequireQemu(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("qemu-img"); err != nil {
		t.Skip("qemu-img not available, skipping QEMU interop test")
	}
}

// RequireQemuIO skips the test if qemu-io is not available.
func RequireQemuIO(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("qemu-io"); err != nil {
		t.Skip("qemu-io not available, skipping QEMU I/O test")
	}
}

// RunQemuImg runs a qemu-img command and returns the result.
func RunQemuImg(t *testing.T, args ...string) QemuResult {
	t.Helper()
	return runCommand(t, "qemu-img", args...)
}

// RunQemuIO runs a qemu-io command and returns the result.
func RunQemuIO(t *testing.T, args ...string) QemuResult {
	t.Helper()
	return runCommand(t, "qemu-io", args...)
}

func runCommand(t *testing.T, name string, args ...string) QemuResult {
	t.Helper()

	cmd := exec.Command(name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()

	result := QemuResult{
		Stdout: stdout.String(),
		Stderr: stderr.String(),
	}

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
		} else {
			t.Logf("%s error: %v", name, err)
			result.ExitCode = -1
		}
	}

	return result
}

// QemuCheck runs qemu-img check on an image file.
func QemuCheck(t *testing.T, path string) QemuCheckResult {
	t.Helper()
	RequireQemu(t)

	result := RunQemuImg(t, "check", "--output=json", path)

	checkResult := QemuCheckResult{
		QemuResult: result,
		IsClean:    result.ExitCode == 0,
	}

	// Parse JSON output if available
	if result.Stdout != "" {
		// qemu-img check outputs JSON to stdout
		if err := json.Unmarshal([]byte(result.Stdout), &checkResult); err != nil {
			t.Logf("Failed to parse qemu-img check JSON: %v", err)
		}
	}

	return checkResult
}

// QemuCheckRepair runs qemu-img check -r all on an image file.
func QemuCheckRepair(t *testing.T, path string) QemuCheckResult {
	t.Helper()
	RequireQemu(t)

	result := RunQemuImg(t, "check", "-r", "all", "--output=json", path)

	checkResult := QemuCheckResult{
		QemuResult: result,
		IsClean:    result.ExitCode == 0,
	}

	if result.Stdout != "" {
		if err := json.Unmarshal([]byte(result.Stdout), &checkResult); err != nil {
			t.Logf("Failed to parse qemu-img check JSON: %v", err)
		}
	}

	return checkResult
}

// QemuInfo runs qemu-img info on an image file.
func QemuInfo(t *testing.T, path string) QemuInfoResult {
	t.Helper()
	RequireQemu(t)

	result := RunQemuImg(t, "info", "--output=json", path)

	infoResult := QemuInfoResult{
		QemuResult: result,
	}

	if result.Stdout != "" {
		if err := json.Unmarshal([]byte(result.Stdout), &infoResult); err != nil {
			t.Logf("Failed to parse qemu-img info JSON: %v", err)
		}
	}

	return infoResult
}

// QemuCreate creates a QCOW2 image using qemu-img.
func QemuCreate(t *testing.T, path string, size string, opts ...string) {
	t.Helper()
	RequireQemu(t)

	args := []string{"create", "-f", "qcow2"}
	args = append(args, opts...)
	args = append(args, path, size)

	result := RunQemuImg(t, args...)
	if result.ExitCode != 0 {
		t.Fatalf("qemu-img create failed: %s", result.Stderr)
	}
}

// QemuCreateWithBacking creates a QCOW2 overlay image.
func QemuCreateWithBacking(t *testing.T, path, backingPath, backingFormat string) {
	t.Helper()
	RequireQemu(t)

	args := []string{"create", "-f", "qcow2", "-b", backingPath}
	if backingFormat != "" {
		args = append(args, "-F", backingFormat)
	}
	args = append(args, path)

	result := RunQemuImg(t, args...)
	if result.ExitCode != 0 {
		t.Fatalf("qemu-img create overlay failed: %s", result.Stderr)
	}
}

// QemuWrite writes a pattern to an image using qemu-io.
func QemuWrite(t *testing.T, path string, pattern byte, offset, length int64) {
	t.Helper()
	RequireQemuIO(t)

	cmd := fmt.Sprintf("write -P 0x%02x %d %d", pattern, offset, length)
	result := RunQemuIO(t, "-c", cmd, path)
	if result.ExitCode != 0 {
		t.Fatalf("qemu-io write failed: %s", result.Stderr)
	}
}

// QemuRead reads and verifies a pattern from an image using qemu-io.
// Returns true if pattern matches, false otherwise.
func QemuRead(t *testing.T, path string, pattern byte, offset, length int64) bool {
	t.Helper()
	RequireQemuIO(t)

	cmd := fmt.Sprintf("read -P 0x%02x %d %d", pattern, offset, length)
	result := RunQemuIO(t, "-c", cmd, path)
	if result.ExitCode != 0 {
		return false
	}
	// qemu-io returns success if pattern matches
	return true
}

// QemuReadData reads raw data from an image using qemu-io.
func QemuReadData(t *testing.T, path string, offset, length int64) ([]byte, error) {
	t.Helper()
	RequireQemuIO(t)

	// Create temp file for output
	tmpFile := filepath.Join(t.TempDir(), "qemu-read-output")

	// Use dump command to write data to file
	cmd := fmt.Sprintf("read -P 0x00 %d %d", offset, length)
	result := RunQemuIO(t, "-c", cmd, path)

	// Parse the output to extract data
	// This is a simplified approach - for exact data, we'd need different method
	_ = tmpFile
	_ = result

	return nil, fmt.Errorf("QemuReadData not fully implemented")
}

// QemuMap runs qemu-img map and returns allocation info.
func QemuMap(t *testing.T, path string) QemuResult {
	t.Helper()
	RequireQemu(t)

	return RunQemuImg(t, "map", "--output=json", path)
}

// QemuConvert converts an image, optionally with compression.
func QemuConvert(t *testing.T, srcPath, dstPath string, compress bool) {
	t.Helper()
	RequireQemu(t)

	args := []string{"convert", "-f", "qcow2", "-O", "qcow2"}
	if compress {
		args = append(args, "-c")
	}
	args = append(args, srcPath, dstPath)

	result := RunQemuImg(t, args...)
	if result.ExitCode != 0 {
		t.Fatalf("qemu-img convert failed: %s", result.Stderr)
	}
}

// QemuVersion returns the QEMU version string.
func QemuVersion(t *testing.T) string {
	t.Helper()

	if _, err := exec.LookPath("qemu-img"); err != nil {
		return ""
	}

	result := RunQemuImg(t, "--version")
	if result.ExitCode != 0 {
		return ""
	}

	// Parse "qemu-img version X.Y.Z"
	lines := strings.Split(result.Stdout, "\n")
	if len(lines) > 0 {
		return lines[0]
	}
	return ""
}

// RandomBytes generates deterministic random bytes from a seed.
func RandomBytes(seed int64, size int) []byte {
	data := make([]byte, size)
	// Simple LCG for reproducible "random" data
	state := uint64(seed)
	for i := range data {
		state = state*6364136223846793005 + 1442695040888963407
		data[i] = byte(state >> 56)
	}
	return data
}

// TempImage creates a temporary file path for a QCOW2 image.
func TempImage(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join(t.TempDir(), name)
}

// ParseSize parses a size string like "1G", "512M", "64K".
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return 0, fmt.Errorf("empty size string")
	}

	multiplier := int64(1)
	suffix := s[len(s)-1]

	switch suffix {
	case 'K', 'k':
		multiplier = 1024
		s = s[:len(s)-1]
	case 'M', 'm':
		multiplier = 1024 * 1024
		s = s[:len(s)-1]
	case 'G', 'g':
		multiplier = 1024 * 1024 * 1024
		s = s[:len(s)-1]
	case 'T', 't':
		multiplier = 1024 * 1024 * 1024 * 1024
		s = s[:len(s)-1]
	}

	value, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}

	return value * multiplier, nil
}

// FileExists returns true if the file exists.
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// FileSize returns the size of a file.
func FileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Failed to stat file %s: %v", path, err)
	}
	return info.Size()
}

// QemuSnapshot creates a snapshot in the image.
func QemuSnapshot(t *testing.T, path string, name string) {
	t.Helper()
	result := RunQemuImg(t, "snapshot", "-c", name, path)
	if !result.IsSuccess() {
		t.Fatalf("qemu-img snapshot -c failed: %s", result.Stderr)
	}
}

// QemuListSnapshots lists snapshots in JSON format.
func QemuListSnapshots(t *testing.T, path string) []map[string]interface{} {
	t.Helper()
	result := RunQemuImg(t, "info", "--output=json", path)
	if !result.IsSuccess() {
		t.Fatalf("qemu-img info failed: %s", result.Stderr)
	}

	var info struct {
		Snapshots []map[string]interface{} `json:"snapshots"`
	}
	if err := json.Unmarshal([]byte(result.Stdout), &info); err != nil {
		t.Fatalf("Failed to parse qemu-img info output: %v", err)
	}
	return info.Snapshots
}
