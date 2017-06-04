package lsm

import (
	"bytes"
	"encoding/binary"
	"io"
)

const restartKeyPrefixInterval = 10

type block struct {
	data []byte
}

// iter iterates over the block calling fn on each key-value pair.
// The key bytes are owned by iter and are only valid until fn
// returns. The value is a slice of the block's bytes. Keeping
// a reference to it will prevent GC-ing of the block.
func (b block) iter(fn func(key, val []byte)) error {
	it := blockIterator{block: b}
	for it.hasNext() {
		err := it.next()
		if err != nil {
			return err
		}

		fn(it.key, it.value)
	}
	return nil
}

type blockIterator struct {
	block block
	off   int

	entryOff   int
	key, value []byte
}

func (bi blockIterator) hasNext() bool {
	return len(bi.block.data) > 0
}

func (bi *blockIterator) ReadByte() (byte, error) {
	if bi.off >= len(bi.block.data) {
		return 0, io.ErrUnexpectedEOF
	}
	b := bi.block.data[bi.off]
	bi.off++
	return b, nil
}

func (bi *blockIterator) next() error {
	startOff := bi.off
	shared, err := binary.ReadUvarint(bi)
	if err != nil {
		return err
	}
	nonshared, err := binary.ReadUvarint(bi)
	if err != nil {
		return err
	}
	valueLen, err := binary.ReadUvarint(bi)
	if err != nil {
		return err
	}

	rest := bi.block.data[bi.off:]

	// set bi.key, bi.value for the next value
	keyDelta := rest[:nonshared]
	bi.key = bi.key[:shared]
	bi.key = append(bi.key, keyDelta...)
	bi.value = rest[nonshared : nonshared+valueLen]
	bi.off += int(nonshared + valueLen)
	bi.entryOff = startOff
	return nil
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
	if bb.counter%restartKeyPrefixInterval != 0 {
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
