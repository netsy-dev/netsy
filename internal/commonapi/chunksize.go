// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package commonapi

const (
	// The first chunk is small so data reaches the client quickly, before
	// value sizes are known
	rangeStreamInitialChunkKeys int64 = 10

	// Bounds a chunk made of very small values
	rangeStreamMaxChunkKeys int64 = 10_000

	// Chunk size (key+value bytes) the sizer converges towards. Mirrors etcd's
	// default max request size
	rangeStreamTargetChunkBytes int64 = 1536 * 1024
)

// chunkSizer adapts how many keys are requested per chunk to the size of the
// values seen so far
type chunkSizer struct {
	keys        int64
	minKeys     int64
	maxKeys     int64
	targetBytes int64
}

// newChunkSizer returns a chunkSizer configured with RangeStream's defaults
func newChunkSizer() *chunkSizer {
	return &chunkSizer{
		keys:        rangeStreamInitialChunkKeys,
		minKeys:     1,
		maxKeys:     rangeStreamMaxChunkKeys,
		targetBytes: rangeStreamTargetChunkBytes,
	}
}

// Keys is the number of keys to request for the next chunk
func (s *chunkSizer) Keys() int64 { return s.keys }

// Observe records the size of the chunk just produced and adjusts the next one
func (s *chunkSizer) Observe(chunkBytes int64) {
	switch {
	case chunkBytes < s.targetBytes/2:
		s.keys = min(s.keys*2, s.maxKeys)
	case chunkBytes > s.targetBytes*2:
		s.keys = max(s.keys/2, s.minKeys)
	}
}
