package store

import (
	"encoding/binary"
	"strconv"
)

func segmentIntToU32(value int, field string) (uint32, error) {
	if value < 0 {
		return 0, E(CodeCodec, "%s %d is negative", field, value)
	}
	if strconv.IntSize > 32 && value > int(^uint32(0)) {
		return 0, E(CodeCodec, "%s %d exceeds uint32", field, value)
	}
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], uint64(value))
	return binary.BigEndian.Uint32(raw[4:]), nil
}

func segmentU32ToInt(value uint32, field string) (int, error) {
	if strconv.IntSize == 32 && value > 1<<31-1 {
		return 0, E(CodeCodec, "%s %d exceeds int", field, value)
	}
	return int(value), nil
}
