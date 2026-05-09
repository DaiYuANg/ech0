// Package bufferpool wraps the selected third-party byte buffer pool.
package bufferpool

import "github.com/valyala/bytebufferpool"

type Buffer = bytebufferpool.ByteBuffer

func Get() *Buffer {
	buf := bytebufferpool.Get()
	buf.Reset()
	return buf
}

func Put(buf *Buffer) {
	if buf == nil {
		return
	}
	bytebufferpool.Put(buf)
}

func CopyAndPut(buf *Buffer) []byte {
	if buf == nil || len(buf.B) == 0 {
		Put(buf)
		return nil
	}
	out := append([]byte(nil), buf.B...)
	Put(buf)
	return out
}
