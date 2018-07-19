package sync

import (
	"sync"

	"github.com/ncw/rclone/fs"
)

// pipe provides an unbounded channel like experience
type pipe struct {
	mu        sync.Mutex
	C         chan struct{}
	queue     []fs.ObjectPair
	closed    bool
	totalSize int64
	stats     func(items int, totalSize int64)
}

func newPipe(stats func(items int, totalSize int64)) *pipe {
	return &pipe{
		C:     make(chan struct{}, 1<<31-1),
		stats: stats,
	}
}

// Put an pair into the pipe
func (p *pipe) Put(pair fs.ObjectPair) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		panic("sync: pipe closed")
	}
	p.queue = append(p.queue, pair)
	p.C <- struct{}{}
	size := pair.Src.Size()
	if size > 0 {
		p.totalSize += size
	}
	p.stats(len(p.queue), p.totalSize)
}

// Get a pair from the pipe
//
// Note that you **must* read from <-pipe.C before calling this.
func (p *pipe) Get() (pair fs.ObjectPair) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.queue) == 0 {
		panic("sync: pipe empty")
	}
	pair, p.queue = p.queue[0], p.queue[1:]
	size := pair.Src.Size()
	if size > 0 {
		p.totalSize -= size
	}
	if p.totalSize < 0 {
		p.totalSize = 0
	}
	p.stats(len(p.queue), p.totalSize)
	return pair
}

// Stats reads the number of items in the queue and the totalSize
func (p *pipe) Stats() (items int, totalSize int64) {
	p.mu.Lock()
	items, totalSize = len(p.queue), p.totalSize
	p.mu.Unlock()
	return items, totalSize
}

// Close the pipe
//
// Writes to a closed pipe will panic as will double closing a pipe
func (p *pipe) Close() {
	p.mu.Lock()
	close(p.C)
	p.closed = true
	p.mu.Unlock()
}
