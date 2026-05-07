package protocol

import "github.com/samber/oops"

func decodeWrap(err error, label string) error {
	return oops.In("protocol").Code("binary_decode_failed").Wrapf(err, "%s", label)
}
