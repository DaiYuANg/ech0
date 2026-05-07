package transport

import (
	"encoding/binary"
	"fmt"
	"io"
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
	if uint64(len(body)) > uint64(^uint32(0)) {
		return Frame{}, fmt.Errorf("frame body too large: %d", len(body))
	}
	return Frame{
		Header: NewFrameHeader(version, command, uint32(len(body))),
		Body:   append([]byte(nil), body...),
	}, nil
}

func ReadFrame(r io.Reader) (Frame, error) {
	return ReadFrameWithLimit(r, 0)
}

func ReadFrameWithLimit(r io.Reader, maxBodyBytes uint32) (Frame, error) {
	var headerBytes [HeaderLen]byte
	if _, err := io.ReadFull(r, headerBytes[:]); err != nil {
		return Frame{}, err
	}
	header := DecodeHeader(headerBytes)
	if maxBodyBytes > 0 && header.BodyLen > maxBodyBytes {
		return Frame{}, fmt.Errorf("frame body length %d exceeds limit %d", header.BodyLen, maxBodyBytes)
	}
	body := make([]byte, header.BodyLen)
	if header.BodyLen > 0 {
		if _, err := io.ReadFull(r, body); err != nil {
			return Frame{}, err
		}
	}
	return Frame{Header: header, Body: body}, nil
}

func WriteFrame(w io.Writer, frame Frame) error {
	header := frame.Header.Encode()
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if len(frame.Body) == 0 {
		return nil
	}
	_, err := w.Write(frame.Body)
	return err
}
