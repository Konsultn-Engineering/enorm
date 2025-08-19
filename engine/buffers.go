package engine

import "sync"

type ScanBuffers struct {
	vals []any
	ptrs []any
}

// Reset clears the buffers for reuse
func (sb *ScanBuffers) Reset() {
	// Clear slices but keep capacity
	sb.vals = sb.vals[:0]
	sb.ptrs = sb.ptrs[:0]
}

// EnsureCapacity grows buffers if needed
func (sb *ScanBuffers) EnsureCapacity(size int) {
	if cap(sb.vals) < size {
		sb.vals = make([]any, 0, size)
		sb.ptrs = make([]any, 0, size)
	}
}

// Prepare sets up buffers for scanning
func (sb *ScanBuffers) Prepare(size int) {
	sb.Reset()
	sb.EnsureCapacity(size)

	// Extend slices to required size
	for len(sb.vals) < size {
		sb.vals = append(sb.vals, nil)
		sb.ptrs = append(sb.ptrs, nil)
	}

	// Set up pointers
	for i := range sb.vals {
		sb.ptrs[i] = &sb.vals[i]
	}
}

var scanPool = sync.Pool{
	New: func() interface{} {
		return &ScanBuffers{
			vals: make([]any, 0, 20), // Pre-allocate capacity
			ptrs: make([]any, 0, 20),
		}
	},
}
