// Package transport implements frame encoding and IO.
package transport

import (
	"encoding/binary"
	"fmt"
	"io"
	"strconv"

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

func NewFrame(version uint8, command uint16, body []byte) (Frame, error) {
	bodyLen, err := frameBodyLen(body)
	if err != nil {
		return Frame{}, err
	}
	return Frame{
		Header: NewFrameHeader(version, command, bodyLen),
		Body:   append([]byte(nil), body...),
	}, nil
}

func frameBodyLen(body []byte) (uint32, error) {
	var out uint32
	if _, err := fmt.Sscan(strconv.Itoa(len(body)), &out); err != nil {
		return 0, oops.In("transport").Code("frame_body_too_large").With("body_len", len(body)).New("frame body too large")
	}
	return out, nil
}

func ReadFrame(r io.Reader) (Frame, error) {
	return ReadFrameWithLimit(r, 0)
}

func ReadFrameWithLimit(r io.Reader, maxBodyBytes uint32) (Frame, error) {
	var headerBytes [HeaderLen]byte
	if _, err := io.ReadFull(r, headerBytes[:]); err != nil {
		return Frame{}, oops.In("transport").Code("frame_header_read_failed").Wrapf(err, "read frame header")
	}
	header := DecodeHeader(headerBytes)
	if err := validateHeader(header); err != nil {
		return Frame{}, err
	}
	if maxBodyBytes > 0 && header.BodyLen > maxBodyBytes {
		return Frame{}, oops.In("transport").Code("frame_body_too_large").With("body_len", header.BodyLen, "max_body_bytes", maxBodyBytes).New("frame body exceeds limit")
	}
	body := make([]byte, header.BodyLen)
	if header.BodyLen > 0 {
		if _, err := io.ReadFull(r, body); err != nil {
			return Frame{}, oops.In("transport").Code("frame_body_read_failed").Wrapf(err, "read frame body")
		}
	}
	return Frame{Header: header, Body: body}, nil
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
	_, err := w.Write(frame.Body)
	return oops.In("transport").Code("frame_body_write_failed").Wrapf(err, "write frame body")
}
