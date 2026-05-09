package protocol

import (
	"fmt"
	"strconv"

	"github.com/samber/oops"
)

func checkedUint16(value int, label string) (uint16, error) {
	if value < 0 {
		return 0, binaryNegativeValue(label, value)
	}
	var out uint16
	if _, err := fmt.Sscan(strconv.Itoa(value), &out); err != nil {
		return 0, binaryValueTooLarge(label, value, err)
	}
	return out, nil
}

func checkedUint32(value int, label string) (uint32, error) {
	if value < 0 {
		return 0, binaryNegativeValue(label, value)
	}
	var out uint32
	if _, err := fmt.Sscan(strconv.Itoa(value), &out); err != nil {
		return 0, binaryValueTooLarge(label, value, err)
	}
	return out, nil
}

func binaryValueTooLarge(label string, value int, err error) error {
	return oops.In("protocol").Code("binary_value_too_large").With("field", label, "value", value).Wrapf(err, "value exceeds binary integer")
}

func binaryNegativeValue(label string, value int) error {
	return oops.In("protocol").Code("binary_value_too_large").With("field", label, "value", value).New("value exceeds binary integer")
}

func intFromUint32(value uint32) (int, error) {
	out, err := strconv.Atoi(strconv.FormatUint(uint64(value), 10))
	if err != nil {
		return 0, oops.In("protocol").Code("binary_value_too_large").Wrapf(err, "convert u32 to int")
	}
	return out, nil
}
