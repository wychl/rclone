// Fallback Alloc and Free for unsupported OSes

// +build plan9 solaris

package mmap

// Alloc allocates size bytes and returns a slice containing them.  If
// the allocation fails it will exit with a fatal error.  This is best
// used for allocations which are a multiple of the page size.
func Alloc(size int) []byte {
	return make([]byte, size)
}

// Free frees buffers allocated by Alloc.  Note it should be passed
// the same slice (not a derived slice) that Alloc returned.  If the
// free fails it will exit with a fatal error.
func Free(mem []byte) {
}
