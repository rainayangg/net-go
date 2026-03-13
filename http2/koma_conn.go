package http2

/*
#include <stdint.h>
*/
import "C"

import (
	"fmt"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"syscall"
	"time"
)

const SO_MARK = 36

type KomaConn struct {
	fd      int
	file    *os.File
	rawConn syscall.RawConn
	// from    unix.Sockaddr
	mark uint32 // for keeping track of skb->mark
	// oobBuf  []byte // persistent control message buffer
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

// func (f *KomaFramer) GetMark() uint32 {
// 	if kc, ok := f.KomaSocket.(*KomaConn); ok {
// 		return kc.mark
// 	}
// 	return 0
// }

func NewKomaConn(fd int) (*KomaConn, error) {
	file := os.NewFile(uintptr(fd), "koma-socket")
	rawConn, err := file.SyscallConn()
	if err != nil || rawConn == nil {
		return nil, fmt.Errorf("error getting rawconn for koma sile descriptor: %w", err)
	}

	return &KomaConn{
		fd:      fd,
		file:    file,
		rawConn: rawConn,
		mark:    0,
		// oobBuf:  make([]byte, 64), // allocate a persistent buffer for control messages
	}, nil
}

func (k *KomaConn) GetFd() int {
	return k.fd
}

func (k *KomaConn) Read(b []byte) (int, error) {
	n, _, _, _, err := unix.Recvmsg(k.fd, b, nil, 0)
	return n, err
}

func (k *KomaConn) Write(b []byte) (int, error) {
	n, err := unix.SendmsgN(k.fd, b, nil, nil, 0)
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
	return nil
	// if !k.ok() {
	// 	return nil
	// }
	// return &KomaAddr{from: k.from}
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
