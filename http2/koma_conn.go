package http2

/*
#include <stdint.h>
*/
import "C"

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

type KomaConn struct {
	fd      C.int
	file    *os.File
	rawConn syscall.RawConn
	from unix.Sockaddr
}

func NewKomaConn(fd C.int) (*KomaConn, error) {
	file := os.NewFile(uintptr(fd), "koma-socket")
	rawConn, err := file.SyscallConn()
	if err != nil || rawConn == nil {
		return nil, fmt.Errorf("error getting rawconn for koma sile descriptor: %w", err)
	}

	return &KomaConn{
		fd:      fd,
		file:    file,
		rawConn: rawConn,
	}, nil
}

func (k *KomaConn) Read(b []byte) (int, error) {
	var n int
	var err error
	readErr := k.rawConn.Read(func(fd uintptr) bool {
		n, _, _, k.from, err = unix.Recvmsg(int(fd), b, nil, 0)
		if err == unix.EAGAIN || err == unix.EWOULDBLOCK { // --> I think returning false is necesary. 
								// If we dont get data, we say the poller to again wait until the fd is available. This matches grpc expected behavior
			return false
		}
		return true
	})
	if readErr != nil {
		return 0, readErr
	}
	
	return n, err
}

func (k *KomaConn) Write(b []byte) (int, error) {
	var n int
	var err error

	writeErr := k.rawConn.Write(func(fd uintptr) bool {
		err = unix.Sendmsg(int(fd), b, nil, k.from, 0)
		if err == unix.EAGAIN {
			return false
		}
		return true
	})

	if writeErr != nil {
		return 0, writeErr
	}

	return n, err
}
