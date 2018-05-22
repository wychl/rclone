// Package mmap implements a large block memory allocator using
// anonymous memory maps.

// +build !plan9,!solaris

package mmap

import (
	"log"

	mmap "github.com/edsrzf/mmap-go"
)

// Alloc allocates size bytes and returns a slice containing them.  If
// the allocation fails it will exit with a fatal error.  This is best
// used for allocations which are a multiple of the page size.
func Alloc(size int) []byte {
	mem, err := mmap.MapRegion(nil, size, mmap.RDWR, mmap.ANON, 0)
	if err != nil {
		log.Fatalf("Failed to allocate memory for buffer: %v", err)
	}
	return mem
}

// Free frees buffers allocated by Alloc.  Note it should be passed
// the same slice (not a derived slice) that Alloc returned.  If the
// free fails it will exit with a fatal error.
func Free(mem []byte) {
	m := mmap.MMap(mem)
	err := m.Unmap()
	if err != nil {
		log.Fatalf("Failed to unmap memory: %v", err)
	}
}
