//go:build !linux

package udp

// forceSocketBuffers: non-Linux — no-op (SetReadBuffer/SetWriteBuffer only).
func (c *Conn) forceSocketBuffers(rcv, snd int) error {
	_ = rcv
	_ = snd
	return nil
}

// SocketBufferSizes: non-Linux không đọc getsockopt — trả 0, nil.
func (c *Conn) SocketBufferSizes() (rcv, snd int, err error) {
	if c == nil || c.conn == nil {
		return 0, 0, ErrTransportClosed
	}
	return 0, 0, nil
}
