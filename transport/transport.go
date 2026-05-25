// Package transport implements frame encoding and IO.
package transport

import (
	"encoding/binary"
	"io"

	"github.com/lyonbrown4d/ech0/internal/bufferpool"
	"github.com/samber/oops"
)

const (
	Magic     uint32 = 0x45434830
	HeaderLen uint8  = 28

	StatusOK    uint16 = 0
	StatusError uint16 = 1
)

type FrameHeader struct {
	Magic     uint32
	Version   uint8
	HeaderLen uint8
	Flags     uint16
	Command   uint16
	Status    uint16
	RequestID uint64
	BodyLen   uint32
	Reserved  uint32
}

func NewFrameHeader(version uint8, command uint16, bodyLen uint32) FrameHeader {
	return FrameHeader{
		Magic:     Magic,
		Version:   version,
		HeaderLen: HeaderLen,
		Command:   command,
		Status:    StatusOK,
		BodyLen:   bodyLen,
	}
}

func (h FrameHeader) Encode() [HeaderLen]byte {
	var out [HeaderLen]byte
	binary.BigEndian.PutUint32(out[0:4], h.Magic)
	out[4] = h.Version
	out[5] = h.HeaderLen
	binary.BigEndian.PutUint16(out[6:8], h.Flags)
	binary.BigEndian.PutUint16(out[8:10], h.Command)
	binary.BigEndian.PutUint16(out[10:12], h.Status)
	binary.BigEndian.PutUint64(out[12:20], h.RequestID)
	binary.BigEndian.PutUint32(out[20:24], h.BodyLen)
	binary.BigEndian.PutUint32(out[24:28], h.Reserved)
	return out
}

func DecodeHeader(data [HeaderLen]byte) FrameHeader {
	return FrameHeader{
		Magic:     binary.BigEndian.Uint32(data[0:4]),
		Version:   data[4],
		HeaderLen: data[5],
		Flags:     binary.BigEndian.Uint16(data[6:8]),
		Command:   binary.BigEndian.Uint16(data[8:10]),
		Status:    binary.BigEndian.Uint16(data[10:12]),
		RequestID: binary.BigEndian.Uint64(data[12:20]),
		BodyLen:   binary.BigEndian.Uint32(data[20:24]),
		Reserved:  binary.BigEndian.Uint32(data[24:28]),
	}
}

type Frame struct {
	Header FrameHeader
	Body   []byte
}

type PooledFrame struct {
	Frame
	release func()
}

func (f *PooledFrame) Release() {
	if f == nil || f.release == nil {
		return
	}
	release := f.release
	f.release = nil
	release()
}

func NewFrame(version uint8, command uint16, body []byte) (Frame, error) {
	bodyLen, err := frameBodyLen(body)
	if err != nil {
		return Frame{}, err
	}
	return Frame{
		Header: NewFrameHeader(version, command, bodyLen),
		Body:   body,
	}, nil
}

func frameBodyLen(body []byte) (uint32, error) {
	bodyLen := len(body)
	if bodyLen > int(^uint32(0)) {
		return 0, oops.In("transport").Code("frame_body_too_large").With("body_len", len(body)).New("frame body too large")
	}
	return uint32(bodyLen), nil
}

func ReadFrame(r io.Reader) (Frame, error) {
	return ReadFrameWithLimit(r, 0)
}

func ReadFrameWithLimit(r io.Reader, maxBodyBytes uint32) (Frame, error) {
	out, err := ReadFrameWithLimitPooled(r, maxBodyBytes)
	if err != nil {
		return Frame{}, err
	}
	frame := out.Frame
	out.Release()
	return frame, nil
}

func ReadFrameWithLimitPooled(r io.Reader, maxBodyBytes uint32) (PooledFrame, error) {
	var headerBytes [HeaderLen]byte
	if _, err := io.ReadFull(r, headerBytes[:]); err != nil {
		return PooledFrame{}, oops.In("transport").Code("frame_header_read_failed").Wrapf(err, "read frame header")
	}
	header := DecodeHeader(headerBytes)
	if err := validateHeader(header); err != nil {
		return PooledFrame{}, err
	}
	if maxBodyBytes > 0 && header.BodyLen > maxBodyBytes {
		return PooledFrame{}, oops.In("transport").Code("frame_body_too_large").With("body_len", header.BodyLen, "max_body_bytes", maxBodyBytes).New("frame body exceeds limit")
	}
	maxBodyLen := int64(^uint(0) >> 1)
	if int64(header.BodyLen) > maxBodyLen {
		return PooledFrame{}, oops.In("transport").Code("frame_body_too_large").With("body_len", header.BodyLen).New("frame body exceeds in-memory limit")
	}

	if header.BodyLen == 0 {
		return PooledFrame{Frame: Frame{Header: header}}, nil
	}
	buffer := bufferpool.Get()
	if cap(buffer.B) < int(header.BodyLen) {
		buffer.B = make([]byte, int(header.BodyLen))
	} else {
		buffer.B = buffer.B[:int(header.BodyLen)]
	}
	if _, err := io.ReadFull(r, buffer.B); err != nil {
		bufferpool.Put(buffer)
		return PooledFrame{}, oops.In("transport").Code("frame_body_read_failed").Wrapf(err, "read frame body")
	}
	return PooledFrame{
		Frame: Frame{
			Header: header,
			Body:   buffer.B,
		},
		release: func() {
			bufferpool.Put(buffer)
		},
	}, nil
}

func validateHeader(header FrameHeader) error {
	if header.Magic != Magic {
		return oops.In("transport").Code("frame_bad_magic").With("magic", header.Magic).New("invalid frame magic")
	}
	if header.HeaderLen != HeaderLen {
		return oops.In("transport").Code("frame_bad_header_len").With("header_len", header.HeaderLen).New("unsupported frame header length")
	}
	return nil
}

func WriteFrame(w io.Writer, frame Frame) error {
	header := frame.Header.Encode()
	if _, err := w.Write(header[:]); err != nil {
		return oops.In("transport").Code("frame_header_write_failed").Wrapf(err, "write frame header")
	}
	if len(frame.Body) == 0 {
		return nil
	}
	if _, err := w.Write(frame.Body); err != nil {
		return oops.In("transport").Code("frame_body_write_failed").Wrapf(err, "write frame body")
	}
	return nil
}
