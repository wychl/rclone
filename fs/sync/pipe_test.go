package sync

import (
	"testing"

	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/fstest/mockobject"
	"github.com/stretchr/testify/assert"
)

func TestPipe(t *testing.T) {
	var queueLength int
	var queueSize int64
	stats := func(n int, size int64) {
		queueLength, queueSize = n, size
	}

	// Make a new pipe
	p := newPipe(stats)

	checkStats := func(expectedN int, expectedSize int64) {
		n, size := p.Stats()
		assert.Equal(t, expectedN, n)
		assert.Equal(t, expectedSize, size)
		assert.Equal(t, expectedN, queueLength)
		assert.Equal(t, expectedSize, queueSize)
	}

	checkStats(0, 0)

	// Show that reading from an empty pipe panics
	assert.Panics(t, func() { p.Get() })

	obj1 := mockobject.New("potato").WithContent([]byte("hello"), mockobject.SeekModeNone)

	pair1 := fs.ObjectPair{Src: obj1, Dst: nil}

	// Put an object
	p.Put(pair1)
	checkStats(1, 5)

	// Close the pipe showing reading on closed pipe is OK
	p.Close()

	// Read from pipe
	<-p.C
	pair2 := p.Get()
	assert.Equal(t, pair1, pair2)
	checkStats(0, 0)

	// Check panic on write to closed pipe
	assert.Panics(t, func() { p.Put(pair1) })
}
