package store

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

var (
	segmentZstdDecoderOnce sync.Once
	segmentZstdReader      *zstd.Decoder
	segmentZstdDecoderErr  error
)

type segmentFrameCompression struct {
	kind    SegmentCompression
	encoder *zstd.Encoder
}

func newSegmentFrameCompression(kind SegmentCompression) (segmentFrameCompression, error) {
	normalized, err := normalizeSegmentCompression(kind)
	if err != nil {
		return segmentFrameCompression{}, err
	}
	compression := segmentFrameCompression{kind: normalized}
	if normalized != SegmentCompressionZstd {
		return compression, nil
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return segmentFrameCompression{}, wrapExternal(err, "create zstd segment encoder")
	}
	compression.encoder = encoder
	return compression, nil
}

func (c segmentFrameCompression) encode(body []byte) (uint32, []byte, error) {
	return c.encodeWithMagic(body, segmentFrameMagic, segmentFrameZstdMagic)
}

func (c segmentFrameCompression) encodeBatch(body []byte) (uint32, []byte, error) {
	return c.encodeWithMagic(body, segmentBatchMagic, segmentBatchZstdMagic)
}

func (c segmentFrameCompression) encodeWithMagic(body []byte, plainMagic, zstdMagic uint32) (uint32, []byte, error) {
	switch c.kind {
	case SegmentCompressionNone:
		return plainMagic, body, nil
	case SegmentCompressionZstd:
		if c.encoder == nil {
			return 0, nil, E(CodeInvalidArgument, "zstd segment encoder is not initialized")
		}
		return zstdMagic, c.encoder.EncodeAll(body, nil), nil
	default:
		return 0, nil, E(CodeInvalidArgument, "unsupported segment compression %q", c.kind)
	}
}

func (c *segmentFrameCompression) close() error {
	if c == nil || c.encoder == nil {
		return nil
	}
	encoder := c.encoder
	c.encoder = nil
	return wrapExternal(encoder.Close(), "close zstd segment encoder")
}

func decompressSegmentFrameZstd(body []byte) ([]byte, error) {
	decoder, err := segmentZstdDecoder()
	if err != nil {
		return nil, err
	}
	decoded, err := decoder.DecodeAll(body, nil)
	if err != nil {
		return nil, wrapExternal(err, "decode zstd segment frame")
	}
	return decoded, nil
}

func segmentZstdDecoder() (*zstd.Decoder, error) {
	segmentZstdDecoderOnce.Do(func() {
		segmentZstdReader, segmentZstdDecoderErr = zstd.NewReader(nil)
		if segmentZstdDecoderErr != nil {
			segmentZstdDecoderErr = wrapExternal(segmentZstdDecoderErr, "create zstd segment decoder")
		}
	})
	return segmentZstdReader, segmentZstdDecoderErr
}
