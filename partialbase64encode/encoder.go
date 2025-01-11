package partialbase64encode

import (
	"bytes"
	"context"
	"encoding/base64"
	"github.com/zerofox-oss/go-msg"
	"sync"
)

const ENCODING_ATTRIBUTE_VALUE = "partially-base64"

// Encoder wraps a topic with another which base64-encodes a Message.
func Encoder(next msg.Topic) msg.Topic {
	return msg.TopicFunc(func(ctx context.Context) msg.MessageWriter {
		return &encodeWriter{
			Next: next.NewWriter(ctx),
		}
	})
}

type encodeWriter struct {
	Next msg.MessageWriter

	buf    bytes.Buffer
	closed bool
	mux    sync.Mutex
}

// Attributes returns the attributes associated with the MessageWriter.
func (w *encodeWriter) Attributes() *msg.Attributes {
	return w.Next.Attributes()
}

// Close base64-encodes the contents of the buffer before
// writing them to the next MessageWriter.
func (w *encodeWriter) Close() error {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed {
		return msg.ErrClosedMessageWriter
	}
	w.closed = true

	attrs := *w.Attributes()
	attrs["Content-Transfer-Encoding"] = []string{ENCODING_ATTRIBUTE_VALUE}

	src := w.buf.String()
	src = Encode(src)
	buf := make([]byte, len(src))
	base64.StdEncoding.Encode(buf, []byte(src))

	if _, err := w.Next.Write(buf); err != nil {
		return err
	}
	return w.Next.Close()
}

// Write writes bytes to an internal buffer.
func (w *encodeWriter) Write(b []byte) (int, error) {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed {
		return 0, msg.ErrClosedMessageWriter
	}
	return w.buf.Write(b)
}
