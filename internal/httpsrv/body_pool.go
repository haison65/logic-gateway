package httpsrv

import (
	"io"
	"sync"
)

// Body buffer pool (P1): tránh io.ReadAll alloc mới mỗi request trên hot path.
var bodyPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 4096)
		return &b
	},
}

const maxPooledBodyCap = maxBody

func acquireBodyBuf() *[]byte {
	return bodyPool.Get().(*[]byte)
}

func releaseBodyBuf(bp *[]byte) {
	if bp == nil {
		return
	}
	if cap(*bp) == 0 || cap(*bp) > maxPooledBodyCap {
		return
	}
	*bp = (*bp)[:0]
	bodyPool.Put(bp)
}

// readBodyPooled đọc tối đa limit bytes vào buffer pooled.
// Đọc thẳng vào spare capacity (không qua tmp[8192]+append) — bớt memmove trên hot path 4KB.
// Slice trả về alias *bp — chỉ hợp lệ đến khi releaseBodyBuf (sau khi Request xong).
func readBodyPooled(r io.Reader, limit int64) ([]byte, *[]byte, error) {
	bp := acquireBodyBuf()
	buf := (*bp)[:0]
	if limit < 0 {
		limit = 0
	}
	lr := io.LimitReader(r, limit)
	for {
		if int64(len(buf)) >= limit {
			break
		}
		if len(buf) == cap(buf) {
			newCap := cap(buf) * 2
			if newCap < 4096 {
				newCap = 4096
			}
			if int64(newCap) > limit {
				newCap = int(limit)
			}
			if newCap <= cap(buf) {
				break
			}
			nb := make([]byte, len(buf), newCap)
			copy(nb, buf)
			buf = nb
		}
		n, err := lr.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if err == io.EOF {
			break
		}
		if err != nil {
			releaseBodyBuf(bp)
			return nil, nil, err
		}
		if n == 0 {
			break
		}
	}
	*bp = buf
	return buf, bp, nil
}
