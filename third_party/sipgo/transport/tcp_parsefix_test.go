package transport

import (
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	sipgo "github.com/emiago/sipgo/sip"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Regression test for the TCP stream-parser poisoning fix.
//
// Before the fix: a single message whose framing the parser cannot follow
// (here, a Content-Length larger than the body) de-synced the per-connection
// parser, and the read loop kept the connection open, silently dropping every
// later message. After the fix: the transport closes the connection on an
// unrecoverable parse error, so the peer observes EOF instead of a hung,
// poisoned connection.
func TestTCPClosesOnUnrecoverableParse(t *testing.T) {
	before := testutil.ToFloat64(parseErrors.WithLabelValues("tcp", "connection_closed"))
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
	// good message, then one whose Content-Length is shorter than the real body
	// (the leftover body bytes mis-parse as a bogus next message → hard parse
	// error), then a perfectly valid message the buggy parser would drop forever.
	stream := mk("SIP/2.0 100 Trying", "Content-Length: 0\r\n", "") +
		mk("SIP/2.0 200 OK", "Content-Type: application/sdp\r\nContent-Length: 5\r\n", body) +
		mk("SIP/2.0 180 Ringing", "Content-Length: 0\r\n", "")
	if _, err := c.Write([]byte(stream)); err != nil {
		t.Fatal(err)
	}

	// With the fix, the server closes the connection after the parse error, so
	// a read returns io.EOF promptly. Without the fix, the connection stays
	// open and this read blocks until the deadline (timeout).
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 1)
	_, err = c.Read(buf)
	if err == nil {
		t.Fatal("expected connection to be closed after unrecoverable parse error, got data")
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("connection stayed open after parse error (poisoned) — fix not effective")
	}
	if err != io.EOF {
		t.Logf("connection closed with: %v (acceptable — not a timeout)", err)
	}

	// The parse-error metric must have incremented for the closed connection.
	if got := testutil.ToFloat64(parseErrors.WithLabelValues("tcp", "connection_closed")); got <= before {
		t.Fatalf("expected sipgo_transport_parse_errors_total{transport=tcp,outcome=connection_closed} to increment; before=%v after=%v", before, got)
	}
}
