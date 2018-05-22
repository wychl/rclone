package mmap

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAllocFree(t *testing.T) {
	const Size = 4096

	b := Alloc(Size)
	assert.Equal(t, Size, len(b))

	// check we can write to all the memory
	for i := range b {
		b[i] = byte(i)
	}

	// Now free the memory
	Free(b)
}

func BenchmarkAllocFree(b *testing.B) {
	for _, dirty := range []bool{false, true} {
		for size := 4096; size <= 1024*1024*1024; size *= 2 {
			b.Run(fmt.Sprintf("%dk,dirty=%v", size>>10, dirty), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					mem := Alloc(size)
					if dirty {
						mem[0] ^= 0xFF
					}
					Free(mem)
				}
			})
		}
	}
}

// benchmark the time alloc/free takes with lots of allocations already
func BenchmarkAllocFreeWithLotsOfAllocations(b *testing.B) {
	const size = 4096
	alloc := func(n int) (allocs [][]byte) {
		for i := 0; i < n; i++ {
			mem := Alloc(size)
			mem[0] ^= 0xFF
			allocs = append(allocs, mem)
		}
		return allocs
	}
	free := func(allocs [][]byte) {
		for _, mem := range allocs {
			Free(mem)
		}
	}
	for preAllocs := 1; preAllocs <= 16*1024; preAllocs *= 2 {
		allocs := alloc(preAllocs)
		b.Run(fmt.Sprintf("%d", preAllocs), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				mem := Alloc(size)
				mem[0] ^= 0xFF
				Free(mem)
			}
		})
		free(allocs)
	}
}
