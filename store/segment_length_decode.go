package store

func finishSegmentInt(length, digits int) (int, error) {
	if digits == 0 {
		return 0, E(CodeCodec, "empty segment length")
	}
	return length, nil
}

func segmentLengthDigit(digit byte) (int, bool) {
	if digit < '0' || digit > '9' {
		return 0, false
	}
	return int(digit - '0'), true
}

func appendSegmentLengthDigit(length, digit int) (int, error) {
	if length > (maxSegmentInt()-digit)/10 {
		return 0, E(CodeCodec, "segment length overflows int")
	}
	return length*10 + digit, nil
}

func maxSegmentInt() int {
	return int(^uint(0) >> 1)
}
