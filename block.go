package lsm

import (
	"bytes"
	"encoding/binary"
)

const restartKeyInterval = 10

type block struct {
	data []byte
}

// iter iterates over the block calling fn on each key-value pair.
// The key bytes are owned by iter and are only valid until fn
// returns. The value is a slice of the block's bytes. Keeping
// a reference to it will prevent GC-ing of the block.
func (b *block) iter(fn func(key, val []byte)) error {
	data := b.data
	var key []byte
	for len(data) > 0 {
		shared, keyDelta, val, remaining, err := decodeEntry(data)
		if err != nil {
			return err
		}

		// reset key to only the shared
		key = key[:shared]
		key = append(key, keyDelta...)
		fn(key, val)
		data = remaining
	}
	return nil
}

func decodeEntry(entry []byte) (shared uint64, keyDelta, value, rest []byte, err error) {
	r := bytes.NewBuffer(entry)
	shared, err = binary.ReadUvarint(r)
	if err != nil {
		return 0, nil, nil, nil, err
	}
	nonshared, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, nil, nil, nil, err
	}
	valueLen, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, nil, nil, nil, err
	}
	rest = r.Bytes()
	keyDelta = rest[:nonshared]
	value = rest[nonshared : nonshared+valueLen]
	return shared, keyDelta, value, rest[nonshared+valueLen:], nil
}

// blockBuilder generates blocks with prefix-compressed keys.
type blockBuilder struct {
	buf     bytes.Buffer
	lastKey []byte
	counter int
	tmp     [binary.MaxVarintLen64]byte // varint scratch space
}

// Reset resets the builder to be empty,
// but it retains the underlying storage for use by future writes.
func (bb *blockBuilder) reset() {
	bb.buf.Reset()
	bb.lastKey = nil
	bb.counter = 0
}

func (bb *blockBuilder) size() int {
	return bb.buf.Len()
}

func (bb *blockBuilder) finish() block {
	return block{data: bb.buf.Bytes()}
}

func (bb *blockBuilder) add(k, v []byte) {
	shared := 0
	if bb.counter%restartKeyInterval != 0 {
		// Count how many characters are shared between k and lastKey
		minLen := len(bb.lastKey)
		if len(k) < minLen {
			minLen = len(k)
		}
		for shared < minLen && bb.lastKey[shared] == k[shared] {
			shared++
		}
	}
	nonshared := len(k) - shared

	// Write the lengths: shared key, unshared key, value
	bb.putUvarint(shared)
	bb.putUvarint(nonshared)
	bb.putUvarint(len(v))

	// Write the unshared key bytes and the value
	bb.buf.Write(k[shared:])
	bb.buf.Write(v)

	bb.lastKey = k
	bb.counter++
}

func (bb *blockBuilder) putUvarint(v int) {
	b := binary.PutUvarint(bb.tmp[:], uint64(v))
	bb.buf.Write(bb.tmp[:b])
}
