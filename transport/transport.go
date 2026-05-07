// Package transport implements frame encoding and IO.
package transport

import (
	"encoding/binary"
	"io"

	"github.com/samber/oops"
)

const HeaderLen = 7

type FrameHeader struct {
	Version uint8
	Command uint16
	BodyLen uint32
}

func NewFrameHeader(version uint8, command uint16, bodyLen uint32) FrameHeader {
	return FrameHeader{Version: version, Command: command, BodyLen: bodyLen}
}

func (h FrameHeader) Encode() [HeaderLen]byte {
	var out [HeaderLen]byte
	out[0] = h.Version
	binary.BigEndian.PutUint16(out[1:3], h.Command)
	binary.BigEndian.PutUint32(out[3:7], h.BodyLen)
	return out
}

func DecodeHeader(data [HeaderLen]byte) FrameHeader {
	return FrameHeader{
		Version: data[0],
		Command: binary.BigEndian.Uint16(data[1:3]),
		BodyLen: binary.BigEndian.Uint32(data[3:7]),
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
	const maxUint32 = ^uint32(0)
	if uint64(len(body)) > uint64(maxUint32) {
		return 0, oops.In("transport").Code("frame_body_too_large").With("body_len", len(body)).New("frame body too large")
	}
	return uint32(len(body)), nil // #nosec G115 -- body length is checked against max uint32 before conversion.
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
