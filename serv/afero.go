package serv

import (
	"os"
	"path/filepath"

	"github.com/spf13/afero"

	"github.com/dosco/graphjin/core/v3"
)

type aferoFS struct {
	fs afero.Fs
}

// newAferoFS creates a new aferoFS instance
func newAferoFS(fs afero.Fs, basePath string) *aferoFS {
	return &aferoFS{fs: afero.NewBasePathFs(fs, basePath)}
}

// Get reads a file from the file system
func (f *aferoFS) Get(path string) ([]byte, error) {
	return afero.ReadFile(f.fs, path)
}

// Put writes a file to the file system
func (f *aferoFS) Put(path string, data []byte) (err error) {
	dir := filepath.Dir(path)
	ok, err := f.Exists(dir)
	if !ok {
		err = f.fs.MkdirAll(dir, os.ModePerm)
	}
	if err != nil {
		return
	}

	return afero.WriteFile(f.fs, path, data, os.ModePerm)
}

// Exists checks if a file exists in the file system
func (f *aferoFS) Exists(path string) (exists bool, err error) {
	return afero.Exists(f.fs, path)
}

// List returns all files in the directory
func (f *aferoFS) List(path string) ([]string, error) {
	dirEntries, err := afero.ReadDir(f.fs, path)
	if err != nil {
		return nil, err
	}
	return core.FilterFileNames(dirEntries), nil
}
