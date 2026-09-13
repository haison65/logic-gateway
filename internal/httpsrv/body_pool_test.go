package httpsrv

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestReadBodyPooledExact4K(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte("x"), 4096)
	got, bp, err := readBodyPooled(bytes.NewReader(payload), maxBody+1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseBodyBuf(bp) })
	if len(got) != 4096 || !bytes.Equal(got, payload) {
		t.Fatalf("len=%d equal=%v", len(got), bytes.Equal(got, payload))
	}
}

func TestReadBodyPooledChunkedReader(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte("ab"), 3000) // 6000 bytes → grow past 4096
	r := &chunkReader{data: payload, chunk: 100}
	got, bp, err := readBodyPooled(r, maxBody+1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseBodyBuf(bp) })
	if !bytes.Equal(got, payload) {
		t.Fatalf("mismatch len=%d want=%d", len(got), len(payload))
	}
}

func TestReadBodyPooledLimit(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte("z"), 100)
	got, bp, err := readBodyPooled(bytes.NewReader(payload), 50)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseBodyBuf(bp) })
	if len(got) != 50 {
		t.Fatalf("len=%d want 50", len(got))
	}
}

func TestReadBodyPooledEmpty(t *testing.T) {
	t.Parallel()
	got, bp, err := readBodyPooled(strings.NewReader(""), maxBody+1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseBodyBuf(bp) })
	if len(got) != 0 {
		t.Fatalf("len=%d", len(got))
	}
}

// chunkReader returns at most chunk bytes per Read (exercises grow path).
type chunkReader struct {
	data  []byte
	off   int
	chunk int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.off >= len(c.data) {
		return 0, io.EOF
	}
	n := c.chunk
	if n > len(p) {
		n = len(p)
	}
	if c.off+n > len(c.data) {
		n = len(c.data) - c.off
	}
	copy(p, c.data[c.off:c.off+n])
	c.off += n
	return n, nil
}
