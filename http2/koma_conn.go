package http2

/*
#include <stdint.h>
*/
import "C"

import (
	"fmt"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"encoding/binary"
	"golang.org/x/sys/unix"
)

const SO_MARK = 36

type KomaConn struct {
	fd      int
	file    *os.File
	rawConn syscall.RawConn
	from    unix.Sockaddr
	m       *sync.Map
	mark    uint32 // for keeping track of skb->mark
	oobBuf  []byte // persistent control message buffer
}

type KomaAddr struct {
	fd   int
	from unix.Sockaddr
}

func (a *KomaAddr) Network() string {
	return "koma" // name your protocol/network here
}

func (a *KomaAddr) String() string {
	switch v := a.from.(type) {
	case *unix.SockaddrInet4:
		ip := net.IP(v.Addr[:])
		return string(fmt.Sprintf("%s:%d", ip.String(), v.Port))
	case *unix.SockaddrInet6:
		return "Not supported!"
	default:
		return ""
	}
}

func (f *KomaFramer) GetMark() uint32 {
	if kc, ok := f.KomaSocket.(*KomaConn); ok {
		return kc.mark
	}
	return 0
}

func NewKomaConn(fd int, m *sync.Map) (*KomaConn, error) {
	file := os.NewFile(uintptr(fd), "koma-socket")
	rawConn, err := file.SyscallConn()
	if err != nil || rawConn == nil {
		return nil, fmt.Errorf("error getting rawconn for koma sile descriptor: %w", err)
	}

	return &KomaConn{
		fd:      fd,
		file:    file,
		rawConn: rawConn,
		m:       m,
		oobBuf:  make([]byte, 64), // allocate a persistent buffer for control messages
	}, nil
}

func (k *KomaConn) GetFd() int {
	return k.fd
}

func (k *KomaConn) GetMap() *sync.Map {
	return k.m
}

func (k *KomaConn) Read(b []byte) (int, error) {
	var n, oobn int
	var err error
	readErr := k.rawConn.Read(func(fd uintptr) bool {
		n, oobn, _, k.from, err = unix.Recvmsg(int(fd), b, k.oobBuf, 0)
		// fmt.Printf("KomaConn.Read: %s\n", k.from)
		if err == unix.EAGAIN || err == unix.EWOULDBLOCK { // --> I think returning false is necesary.
			// If we dont get data, we say the poller to again wait until the fd is available. This matches grpc expected behavior
			fmt.Printf("KomaConn.Read: EAGAIN returned\n")
			return false
		}
		if err == nil && oobn > 0 {
			k.parseMarkFromCmsgs(k.oobBuf[:oobn])
		}
		return true
	})
	if readErr != nil {
		return 0, readErr
	}

	return n, err
}

func (k *KomaConn) parseMarkFromCmsgs(oob []byte) {
	msgs, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return
	}
	for _, msg := range msgs {
		if msg.Header.Level == unix.SOL_SOCKET && msg.Header.Type == SO_MARK {
			if len(msg.Data) >= 4 {
				k.mark = uint32(binary.LittleEndian.Uint32(msg.Data[:4]))
			}
		}
	}
}

func (k *KomaConn) Write(b []byte) (int, error) {
	var n int
	var err error

	// TODO: change to sendmsgN in the future to prevent n always being 0.
	// fmt.Printf("start KomaConn.Write()\n")
	writeErr := k.rawConn.Write(func(fd uintptr) bool {
		n, err = unix.SendmsgN(int(fd), b, nil, k.from, 0)
		// fmt.Printf("KomaConn.Write: to %s, sendmsgN returns %d %d\n", k.from, n, err)
		if err == unix.EAGAIN {
			fmt.Printf("KomaConn.Write: EAGAIN returned\n")
			return false
		}
		return true
	})
	// fmt.Printf("koma.rawConn.Write() returned %d\n", writeErr)
	if writeErr != nil {
		return 0, writeErr
	}

	return n, err
}

// functions required to implement net.conn interface
// the following are heavily copy-pasted from the net.conn implementation in src/net/net.go
func (k *KomaConn) ok() bool { return k != nil && k.fd != 0 }

// Close closes the connection.
func (k *KomaConn) Close() error {
	if !k.ok() {
		return syscall.EINVAL
	}
	err := k.file.Close()
	return err
}

// LocalAddr returns the local network address.
// The Addr returned is shared by all invocations of LocalAddr, so
// do not modify it.
func (k *KomaConn) LocalAddr() net.Addr {
	return &KomaAddr{fd: k.fd}
}

// RemoteAddr returns the remote network address stored in from.
// The Addr returned is shared by all invocations of RemoteAddr, so
// do not modify it.
func (k *KomaConn) RemoteAddr() net.Addr {
	if !k.ok() {
		return nil
	}
	return &KomaAddr{from: k.from}
}

// SetDeadline is a stub. Not implemented yet.
// TODO: check if its necessary.
func (k *KomaConn) SetDeadline(t time.Time) error {
	return nil
}

// SetReadDeadline is a stub.
func (k *KomaConn) SetReadDeadline(t time.Time) error {
	return nil
}

// SetWriteDeadline is a stub.
func (k *KomaConn) SetWriteDeadline(t time.Time) error {
	return nil
}

// SyscallConn exposes the underlying syscall.RawConn.
func (k *KomaConn) SyscallConn() (syscall.RawConn, error) {
	return k.rawConn, nil
}
