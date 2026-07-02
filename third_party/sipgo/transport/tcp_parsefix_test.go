package transport

import (
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	sipgo "github.com/emiago/sipgo/sip"
)

// TestTCPClosesOnUnrecoverableParse verifies that a TCP connection whose
// stream has lost framing (Content-Length mismatch) is closed rather than
// left open and poisoned. Without the fix, the de-synced parser drops every
// later message on the (pooled, reused) connection forever.
func TestTCPClosesOnUnrecoverableParse(t *testing.T) {
	par := sipgo.NewParser()
	tr := NewTCPTransport(slog.Default(), par, nil)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() { _ = tr.Serve(ln, func(sipgo.Message) {}) }()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	body := "v=0\r\no=- 1 1 IN IP4 1.2.3.4\r\ns=s\r\nc=IN IP4 1.2.3.4\r\nt=0 0\r\n"
	mk := func(start, hdr, b string) string {
		return start + "\r\n" +
			"Via: SIP/2.0/TCP 1.2.3.4:5060;branch=z9hG4bK.x\r\n" +
			"From: <sip:a@1.2.3.4>;tag=A\r\nTo: <sip:b@5.6.7.8>;tag=B\r\n" +
			"Call-ID: c\r\nCSeq: 1 INVITE\r\n" + hdr + "\r\n" + b
	}
	// good message, then one whose Content-Length is shorter than the body
	// (leftover bytes mis-parse -> hard error), then a valid message the buggy
	// parser would drop forever.
	stream := mk("SIP/2.0 100 Trying", "Content-Length: 0\r\n", "") +
		mk("SIP/2.0 200 OK", "Content-Type: application/sdp\r\nContent-Length: 5\r\n", body) +
		mk("SIP/2.0 180 Ringing", "Content-Length: 0\r\n", "")
	if _, err := c.Write([]byte(stream)); err != nil {
		t.Fatal(err)
	}

	// With the fix, the server closes the connection -> read returns EOF fast.
	// Without it, the connection stays open (poisoned) and this read times out.
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, err = c.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("expected connection closed after unrecoverable parse error")
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("connection stayed open (poisoned) — fix not effective")
	}
	_ = io.EOF // err should be io.EOF / connection reset
}

// TestTCPKeepAliveDoesNotEatMessageTerminator guards against the true
// root-cause bug: TCP does not preserve message boundaries, so a carrier can
// legally deliver a SIP message's terminating blank-line CRLF in its own tiny
// segment. The transport's keep-alive heuristic used to discard ANY <=4-byte
// all-CRLF read as a keep-alive ping — including such a terminator —
// leaving the parser stuck mid-message. The next message's start line then
// parsed as a header and the stream de-synced.
//
// The test scripts a connection that returns: (1) all of message-1's headers
// up to the last header's CRLF, (2) the message-terminating "\r\n" in its
// own read, (3) a complete second message. Both messages must be delivered
// to the handler.
func TestTCPKeepAliveDoesNotEatMessageTerminator(t *testing.T) {
	par := sipgo.NewParser()
	tr := NewTCPTransport(slog.Default(), par, nil)

	mk := func(start string) string {
		return start + "\r\n" +
			"Via: SIP/2.0/TCP 1.2.3.4:5060;branch=z9hG4bK.x\r\n" +
			"From: <sip:a@1.2.3.4>;tag=A\r\n" +
			"To: <sip:b@5.6.7.8>;tag=B\r\n" +
			"Call-ID: c\r\nCSeq: 1 INVITE\r\nContent-Length: 0\r\n\r\n"
	}
	msg1 := mk("SIP/2.0 100 Trying")
	msg2 := mk("SIP/2.0 180 Ringing")

	// Split msg1 so its terminating "\r\n" (the blank line after the last
	// header) lands in its own read. Index of the headers/body boundary:
	idx := strings.LastIndex(msg1, "\r\n\r\n")
	if idx < 0 {
		t.Fatalf("msg1 has no header terminator")
	}
	// chunk1 = headers + last header's CRLF (no blank line yet)
	chunk1 := []byte(msg1[:idx+2])
	// chunk2 = the blank line ("\r\n") — the message terminator.
	// This is the chunk the buggy keep-alive heuristic would eat.
	chunk2 := []byte(msg1[idx+2 : idx+4])
	// chunk3 = an entire second message.
	chunk3 := []byte(msg2)

	conn := &TCPConnection{
		Conn:     &scriptedConn{chunks: [][]byte{chunk1, chunk2, chunk3}},
		refcount: 1,
	}

	var (
		mu   sync.Mutex
		got  []sipgo.Message
		done = make(chan struct{})
	)
	handler := func(m sipgo.Message) {
		mu.Lock()
		got = append(got, m)
		mu.Unlock()
	}

	go func() {
		defer close(done)
		tr.readConnection(conn, "127.0.0.1:1234", handler)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("readConnection did not return — likely de-synced and stuck")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("expected 2 messages, got %d (the keep-alive heuristic likely ate the terminator and de-synced the stream)", len(got))
	}
}

// scriptedConn is a net.Conn whose Read returns one pre-defined byte slice
// per call, then EOF. It lets tests drive the TCP transport with controlled
// chunking, which is essential for reproducing TCP framing bugs.
type scriptedConn struct {
	chunks [][]byte
	idx    int
}

func (c *scriptedConn) Read(p []byte) (int, error) {
	if c.idx >= len(c.chunks) {
		return 0, io.EOF
	}
	chunk := c.chunks[c.idx]
	n := copy(p, chunk)
	if n < len(chunk) {
		c.chunks[c.idx] = chunk[n:]
		return n, nil
	}
	c.idx++
	return n, nil
}

func (c *scriptedConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *scriptedConn) Close() error                     { return nil }
func (c *scriptedConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *scriptedConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *scriptedConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }
