package http2

import (
	"net"
	"testing"
	"time"
)

type recordingConn struct {
	writes [][]byte
}

func (c *recordingConn) Read(_ []byte) (int, error) { return 0, nil }
func (c *recordingConn) Write(p []byte) (int, error) {
	c.writes = append(c.writes, append([]byte(nil), p...))
	return len(p), nil
}
func (c *recordingConn) Close() error                       { return nil }
func (c *recordingConn) LocalAddr() net.Addr                { return nil }
func (c *recordingConn) RemoteAddr() net.Addr               { return nil }
func (c *recordingConn) SetDeadline(_ time.Time) error      { return nil }
func (c *recordingConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *recordingConn) SetWriteDeadline(_ time.Time) error { return nil }

func TestKomaReplyCookieOOBRoundTrip(t *testing.T) {
	cookie := KomaReplyCookie{
		Handle: 0x1122334455667788,
		Flags:  KomaReplyCookieFlagFinal,
	}

	got := parseKomaReplyCookie(makeKomaReplyCookieOOB(cookie))
	if got != cookie {
		t.Fatalf("parseKomaReplyCookie(roundtrip) = %+v, want %+v", got, cookie)
	}
}

func TestParseKomaReplyCookieIgnoresMissingCookie(t *testing.T) {
	if got := parseKomaReplyCookie(nil); got != (KomaReplyCookie{}) {
		t.Fatalf("parseKomaReplyCookie(nil) = %+v, want zero cookie", got)
	}
}

func TestKomaFramerFlushBatchClearsReplyCookie(t *testing.T) {
	conn := &recordingConn{}
	fr := NewKomaFramer(conn)
	fr.replyCookie = KomaReplyCookie{
		Handle: 9,
		Flags:  KomaReplyCookieFlagFinal,
	}
	fr.wbuf = []byte{1, 2, 3}

	if err := fr.FlushBatch(); err != nil {
		t.Fatalf("FlushBatch() error = %v", err)
	}
	if len(conn.writes) != 1 {
		t.Fatalf("FlushBatch() wrote %d buffers, want 1", len(conn.writes))
	}
	if fr.replyCookie != (KomaReplyCookie{}) {
		t.Fatalf("FlushBatch() left reply cookie = %+v, want zero value", fr.replyCookie)
	}
}
