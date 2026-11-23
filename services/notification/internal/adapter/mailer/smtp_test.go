package mailer_test

import (
	"bufio"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/notification/internal/adapter/mailer"
)

// fakeSMTP is a minimal SMTP server (no auth, no TLS) that records one
// message's envelope and data.
type fakeSMTP struct {
	addr string
	mu   sync.Mutex
	from string
	to   []string
	data string
}

func startSMTP(t *testing.T) *fakeSMTP {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })
	f := &fakeSMTP{addr: lis.Addr().String()}
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			go f.session(c)
		}
	}()
	return f
}

func (f *fakeSMTP) session(c net.Conn) {
	defer func() { _ = c.Close() }()
	r := bufio.NewReader(c)
	write := func(s string) { _, _ = c.Write([]byte(s + "\r\n")) }
	write("220 fake ESMTP")
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		cmd := strings.ToUpper(strings.TrimSpace(line))
		switch {
		case strings.HasPrefix(cmd, "EHLO"), strings.HasPrefix(cmd, "HELO"):
			write("250 fake")
		case strings.HasPrefix(cmd, "MAIL FROM:"):
			f.mu.Lock()
			f.from = strings.TrimSpace(line[len("MAIL FROM:"):])
			f.mu.Unlock()
			write("250 ok")
		case strings.HasPrefix(cmd, "RCPT TO:"):
			f.mu.Lock()
			f.to = append(f.to, strings.TrimSpace(line[len("RCPT TO:"):]))
			f.mu.Unlock()
			write("250 ok")
		case cmd == "DATA":
			write("354 go ahead")
			var b strings.Builder
			for {
				l, err := r.ReadString('\n')
				if err != nil || l == ".\r\n" {
					break
				}
				b.WriteString(l)
			}
			f.mu.Lock()
			f.data = b.String()
			f.mu.Unlock()
			write("250 queued")
		case cmd == "QUIT":
			write("221 bye")
			return
		default:
			write("250 ok")
		}
	}
}

func TestSMTP_SendsMultipartMessage(t *testing.T) {
	srv := startSMTP(t)
	m := mailer.NewSMTP(srv.addr, "SmartHire <no-reply@smarthire.local>", 5*time.Second)
	err := m.Send(context.Background(), "cand@x.local", "Hồ sơ của bạn: Mời phỏng vấn", "<p>Xin chào</p>", "Xin chào")
	require.NoError(t, err)

	srv.mu.Lock()
	defer srv.mu.Unlock()
	assert.Equal(t, "<no-reply@smarthire.local>", srv.from)
	assert.Equal(t, []string{"<cand@x.local>"}, srv.to)
	assert.Contains(t, srv.data, "To: cand@x.local")
	assert.Contains(t, srv.data, "From: SmartHire <no-reply@smarthire.local>")
	assert.Contains(t, srv.data, "Subject: =?UTF-8?", "non-ASCII subject is encoded")
	assert.Contains(t, srv.data, "multipart/alternative")
	assert.Contains(t, srv.data, "text/plain; charset=UTF-8")
	assert.Contains(t, srv.data, "text/html; charset=UTF-8")
}

func TestSMTP_UnreachableServerFails(t *testing.T) {
	m := mailer.NewSMTP("127.0.0.1:1", "a@b.c", 500*time.Millisecond)
	assert.Error(t, m.Send(context.Background(), "x@y.z", "s", "<p>h</p>", "t"))
}

func TestSMTP_RejectsHeaderInjection(t *testing.T) {
	m := mailer.NewSMTP("127.0.0.1:1", "a@b.c", time.Second)
	assert.Error(t, m.Send(context.Background(), "x@y.z\r\nBcc: evil@z", "s", "h", "t"))
}
