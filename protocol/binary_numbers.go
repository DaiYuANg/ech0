package protocol

import (
	"github.com/samber/oops"
)

func checkedUint16(value int, label string) (uint16, error) {
	if value < 0 {
		return 0, binaryNegativeValue(label, value)
	}
	if uint64(value) > uint64(^uint16(0)) {
		return 0, binaryValueTooLarge(label, value)
	}
	return uint16(value), nil
}

func checkedUint32(value int, label string) (uint32, error) {
	if value < 0 {
		return 0, binaryNegativeValue(label, value)
	}
	if uint64(value) > uint64(^uint32(0)) {
		return 0, binaryValueTooLarge(label, value)
	}
	return uint32(value), nil
}

func binaryValueTooLarge(label string, value int) error {
	return oops.In("protocol").Code("binary_value_too_large").With("field", label, "value", value).New("value exceeds binary integer")
}

func binaryNegativeValue(label string, value int) error {
	return oops.In("protocol").Code("binary_value_too_large").With("field", label, "value", value).New("value exceeds binary integer")
}

func intFromUint32(value uint32) (int, error) {
	maxInt := int(^uint(0) >> 1)
	if value > uint32(maxInt) {
		return 0, oops.In("protocol").Code("binary_value_too_large").With("value", value, "max", maxInt).New("u32 value exceeds int")
	}
	return int(value), nil
}
