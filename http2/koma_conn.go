package http2

/*
#include <stdint.h>
*/
import "C"

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

const SO_MARK = 36
const (
	SOL_KOMA                      = 288
	KomaCmsgReplyCookie           = 1
	KomaReplyCookieFlagFinal      = 1 << 0
	KomaSockoptRequireReplyCookie = 1
	komaReplyCookieSize           = 16
)

type KomaReplyCookie struct {
	Handle uint64
	Flags  uint32
}

type KomaConn struct {
	fd      int
	file    *os.File
	rawConn syscall.RawConn
	// from    unix.Sockaddr
	mark uint32 // for keeping track of skb->mark
	// oobBuf  []byte // persistent control message buffer
	lastReplyHandle  atomic.Uint64
	lastReplyFlags   atomic.Uint32
	lastRecvmsgFlags atomic.Uint32
	lastRecvmsgSize  atomic.Uint32
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

// NewKomaConn wraps an AF_KOMA file descriptor in a net.Conn-like object.
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

// GetFd returns the underlying Koma socket file descriptor.
func (k *KomaConn) GetFd() int {
	return k.fd
}

// SetRequireReplyCookie configures whether sends on this Koma socket must
// include reply-routing metadata.
func (k *KomaConn) SetRequireReplyCookie(enabled bool) error {
	value := 0
	if enabled {
		value = 1
	}
	return unix.SetsockoptInt(k.fd, SOL_KOMA, KomaSockoptRequireReplyCookie, value)
}

// Read receives one Koma message and caches any reply cookie from ancillary data.
func (k *KomaConn) Read(b []byte) (int, error) {
	oob := make([]byte, unix.CmsgSpace(komaReplyCookieSize))
	n, oobn, recvmsgFlags, _, err := unix.Recvmsg(k.fd, b, oob, 0)
	k.lastRecvmsgFlags.Store(uint32(recvmsgFlags))
	k.lastRecvmsgSize.Store(uint32(n))
	if err == nil {
		cookie := parseKomaReplyCookie(oob[:oobn])
		k.lastReplyHandle.Store(cookie.Handle)
		k.lastReplyFlags.Store(cookie.Flags)
	}
	return n, err
}

// Write sends one Koma message without attaching reply-routing metadata.
func (k *KomaConn) Write(b []byte) (int, error) {
	n, err := unix.SendmsgN(k.fd, b, nil, nil, 0)
	return n, err
}

// WriteWithReplyCookie sends one Koma message and attaches reply-routing metadata.
func (k *KomaConn) WriteWithReplyCookie(b []byte, handle uint64, flags uint32) (int, error) {
	if handle == 0 {
		return k.Write(b)
	}
	oob := makeKomaReplyCookieOOB(KomaReplyCookie{
		Handle: handle,
		Flags:  flags,
	})
	return unix.SendmsgN(k.fd, b, oob, nil, 0)
}

// LastReplyCookie returns the most recently observed cookie from Read.
func (k *KomaConn) LastReplyCookie() KomaReplyCookie {
	return KomaReplyCookie{
		Handle: k.lastReplyHandle.Load(),
		Flags:  k.lastReplyFlags.Load(),
	}
}

// LastRecvmsgFlags returns the recvmsg flags from the most recent read.
func (k *KomaConn) LastRecvmsgFlags() int {
	return int(k.lastRecvmsgFlags.Load())
}

// LastRecvmsgSize returns the payload size from the most recent read.
func (k *KomaConn) LastRecvmsgSize() int {
	return int(k.lastRecvmsgSize.Load())
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

// parseKomaReplyCookie extracts a Koma reply cookie from socket control data.
func parseKomaReplyCookie(oob []byte) KomaReplyCookie {
	msgs, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return KomaReplyCookie{}
	}
	for _, msg := range msgs {
		if msg.Header.Level != SOL_KOMA || msg.Header.Type != KomaCmsgReplyCookie {
			continue
		}
		if len(msg.Data) < komaReplyCookieSize {
			continue
		}
		cookie := KomaReplyCookie{
			Handle: binary.NativeEndian.Uint64(msg.Data[:8]),
			Flags:  binary.NativeEndian.Uint32(msg.Data[8:12]),
		}
		return cookie
	}
	return KomaReplyCookie{}
}

// makeKomaReplyCookieOOB encodes a Koma reply cookie as socket control data (OOB: out-of-band).
func makeKomaReplyCookieOOB(cookie KomaReplyCookie) []byte {
	oob := make([]byte, unix.CmsgSpace(komaReplyCookieSize))
	hdr := (*unix.Cmsghdr)(unsafe.Pointer(&oob[0]))
	hdr.Level = SOL_KOMA
	hdr.Type = KomaCmsgReplyCookie
	hdr.SetLen(unix.CmsgLen(komaReplyCookieSize))

	dataStart := unix.CmsgLen(0)
	binary.NativeEndian.PutUint64(oob[dataStart:dataStart+8], cookie.Handle)
	binary.NativeEndian.PutUint32(oob[dataStart+8:dataStart+12], cookie.Flags)
	binary.NativeEndian.PutUint32(oob[dataStart+12:dataStart+16], 0)
	return oob
}
