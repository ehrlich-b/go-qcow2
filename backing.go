package qcow2

import (
	"fmt"
	"os"
	"path/filepath"
)

// openBackingFile opens the backing file if one is specified.
func (img *Image) openBackingFile() error {
	if img.header.BackingFileOffset == 0 || img.header.BackingFileSize == 0 {
		return nil // No backing file
	}

	// Read backing file path from header
	pathBuf := make([]byte, img.header.BackingFileSize)
	_, err := img.file.ReadAt(pathBuf, int64(img.header.BackingFileOffset))
	if err != nil {
		return fmt.Errorf("qcow2: failed to read backing file path: %w", err)
	}

	backingPath := string(pathBuf)

	// Resolve relative paths relative to the image file
	if !filepath.IsAbs(backingPath) {
		imgPath := img.file.Name()
		imgDir := filepath.Dir(imgPath)
		backingPath = filepath.Join(imgDir, backingPath)
	}

	// Open backing file (read-only)
	backing, err := OpenFile(backingPath, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("qcow2: failed to open backing file %q: %w", backingPath, err)
	}

	img.backing = backing
	return nil
}

// BackingFile returns the path to the backing file, or empty string if none.
func (img *Image) BackingFile() string {
	if img.header.BackingFileOffset == 0 || img.header.BackingFileSize == 0 {
		return ""
	}

	pathBuf := make([]byte, img.header.BackingFileSize)
	_, err := img.file.ReadAt(pathBuf, int64(img.header.BackingFileOffset))
	if err != nil {
		return ""
	}

	return string(pathBuf)
}

// HasBackingFile returns true if this image has a backing file.
func (img *Image) HasBackingFile() bool {
	return img.header.BackingFileOffset != 0 && img.header.BackingFileSize != 0
}

// BackingChainDepth returns the depth of the backing file chain.
// Returns 0 if there is no backing file.
func (img *Image) BackingChainDepth() int {
	depth := 0
	current := img.backing
	for current != nil {
		depth++
		current = current.backing
	}
	return depth
}

// CreateOptions for images with backing files
// SetBackingFile sets the backing file for image creation.
func (opts *CreateOptions) SetBackingFile(path string) {
	opts.BackingFile = path
}
