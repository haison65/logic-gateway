//go:build linux

package udp

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// forceSocketBuffers dùng SO_RCVBUFFORCE/SNDBUFFORCE (cần CAP_NET_ADMIN) khi
// SetReadBuffer bị clamp — Docker Desktop WSL2 không có net.core.rmem_max.
func (c *Conn) forceSocketBuffers(rcv, snd int) error {
	if c == nil || c.conn == nil {
		return ErrTransportClosed
	}
	raw, err := c.conn.SyscallConn()
	if err != nil {
		return err
	}
	var setErr error
	cerr := raw.Control(func(fd uintptr) {
		if rcv > 0 {
			if err := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_RCVBUFFORCE, rcv); err != nil {
				setErr = fmt.Errorf("SO_RCVBUFFORCE: %w", err)
				return
			}
		}
		if snd > 0 {
			if err := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUFFORCE, snd); err != nil {
				setErr = fmt.Errorf("SO_SNDBUFFORCE: %w", err)
				return
			}
		}
	})
	if cerr != nil {
		return cerr
	}
	return setErr
}

// SocketBufferSizes trả về SO_RCVBUF / SO_SNDBUF hiệu dụng (kernel có thể ×2 so với Set*).
func (c *Conn) SocketBufferSizes() (rcv, snd int, err error) {
	if c == nil || c.conn == nil {
		return 0, 0, ErrTransportClosed
	}
	raw, err := c.conn.SyscallConn()
	if err != nil {
		return 0, 0, err
	}
	var rErr, sErr error
	cerr := raw.Control(func(fd uintptr) {
		rcv, rErr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_RCVBUF)
		snd, sErr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF)
	})
	if cerr != nil {
		return 0, 0, cerr
	}
	if rErr != nil {
		return 0, 0, fmt.Errorf("SO_RCVBUF: %w", rErr)
	}
	if sErr != nil {
		return 0, 0, fmt.Errorf("SO_SNDBUF: %w", sErr)
	}
	return rcv, snd, nil
}
