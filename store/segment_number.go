package store

import (
	"fmt"
	"strconv"
)

func segmentIntToU32(value int, field string) (uint32, error) {
	if value < 0 {
		return 0, E(CodeCodec, "%s %d is negative", field, value)
	}
	var out uint32
	if _, err := fmt.Sscan(strconv.Itoa(value), &out); err != nil {
		return 0, E(CodeCodec, "%s %d exceeds uint32", field, value)
	}
	return out, nil
}

func segmentU32ToInt(value uint32, field string) (int, error) {
	out, err := strconv.Atoi(strconv.FormatUint(uint64(value), 10))
	if err != nil {
		return 0, E(CodeCodec, "%s %d exceeds int", field, value)
	}
	return out, nil
}
