package qcow2

import (
	"fmt"
	"os"
	"path/filepath"
)

// RawImage wraps an *os.File to implement BackingStore for raw backing files.
type RawImage struct {
	file *os.File
}

// ReadAt implements io.ReaderAt for raw backing files.
func (r *RawImage) ReadAt(p []byte, off int64) (int, error) {
	return r.file.ReadAt(p, off)
}

// Close implements io.Closer for raw backing files.
func (r *RawImage) Close() error {
	return r.file.Close()
}

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

	// Check backing format from header extension
	backingFormat := ""
	if img.extensions != nil {
		backingFormat = img.extensions.BackingFormat
	}

	// Open backing file based on format
	switch backingFormat {
	case "raw":
		// Open as raw image
		f, err := os.OpenFile(backingPath, os.O_RDONLY, 0)
		if err != nil {
			return fmt.Errorf("qcow2: failed to open raw backing file %q: %w", backingPath, err)
		}
		img.backing = &RawImage{file: f}

	case "qcow2", "":
		// Open as qcow2 (default if format not specified)
		backing, err := OpenFile(backingPath, os.O_RDONLY, 0)
		if err != nil {
			return fmt.Errorf("qcow2: failed to open backing file %q: %w", backingPath, err)
		}
		img.backing = backing

	default:
		return fmt.Errorf("qcow2: unsupported backing file format %q", backingFormat)
	}

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
		// Only qcow2 images can have further backing files
		if qcow2Img, ok := current.(*Image); ok {
			current = qcow2Img.backing
		} else {
			// Raw images don't have backing files
			break
		}
	}
	return depth
}

// CreateOptions for images with backing files
// SetBackingFile sets the backing file for image creation.
func (opts *CreateOptions) SetBackingFile(path string) {
	opts.BackingFile = path
}
