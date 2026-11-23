// Package mailer sends emails over plain SMTP without auth (Mailpit in
// compose; ADR 0012).
package mailer

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"mime"
	"mime/quotedprintable"
	"net"
	"net/mail"
	"net/smtp"
	"strings"
	"time"
)

// SMTP implements app.Mailer.
type SMTP struct {
	addr    string
	from    *mail.Address
	fromRaw string
	timeout time.Duration
}

// NewSMTP returns a mailer for the server at addr; from is an RFC 5322
// address ("Name <a@b>") and timeout bounds one whole send.
func NewSMTP(addr, from string, timeout time.Duration) *SMTP {
	a, err := mail.ParseAddress(from)
	if err != nil {
		a = &mail.Address{Address: from}
	}
	return &SMTP{addr: addr, from: a, fromRaw: from, timeout: timeout}
}

var errHeaderInjection = errors.New("address contains a line break")

// Send implements app.Mailer. Error texts carry no address or content.
func (s *SMTP) Send(ctx context.Context, to, subject, html, text string) error {
	if strings.ContainsAny(to, "\r\n") || strings.ContainsAny(subject, "\r\n") {
		return errHeaderInjection
	}
	rcpt, err := mail.ParseAddress(to)
	if err != nil {
		return errors.New("invalid recipient address")
	}
	deadline := time.Now().Add(s.timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	d := net.Dialer{Deadline: deadline}
	conn, err := d.DialContext(ctx, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("smtp dial: %w", err)
	}
	_ = conn.SetDeadline(deadline)
	host, _, _ := net.SplitHostPort(s.addr)
	c, err := smtp.NewClient(conn, host)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("smtp hello: %w", err)
	}
	defer func() { _ = c.Close() }()
	if err := c.Mail(s.from.Address); err != nil {
		return fmt.Errorf("smtp mail from: %w", err)
	}
	if err := c.Rcpt(rcpt.Address); err != nil {
		return fmt.Errorf("smtp rcpt: %w", err)
	}
	w, err := c.Data()
	if err != nil {
		return fmt.Errorf("smtp data: %w", err)
	}
	if _, err := w.Write(s.message(rcpt.Address, subject, html, text)); err != nil {
		return fmt.Errorf("smtp write: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("smtp end data: %w", err)
	}
	return c.Quit()
}

// message builds a multipart/alternative (text + HTML) UTF-8 message.
func (s *SMTP) message(to, subject, html, text string) []byte {
	boundary := randomHex(12)
	var b bytes.Buffer
	header := func(k, v string) { fmt.Fprintf(&b, "%s: %s\r\n", k, v) }
	header("From", s.fromRaw)
	header("To", to)
	header("Subject", mime.QEncoding.Encode("UTF-8", subject))
	header("Date", time.Now().UTC().Format(time.RFC1123Z))
	header("Message-ID", "<"+randomHex(16)+"@smarthire.local>")
	header("MIME-Version", "1.0")
	header("Content-Type", `multipart/alternative; boundary="`+boundary+`"`)
	b.WriteString("\r\n")
	part := func(contentType, body string) {
		fmt.Fprintf(&b, "--%s\r\nContent-Type: %s; charset=UTF-8\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\n", boundary, contentType)
		qp := quotedprintable.NewWriter(&b)
		_, _ = qp.Write([]byte(body))
		_ = qp.Close()
		b.WriteString("\r\n")
	}
	part("text/plain", text)
	part("text/html", html)
	fmt.Fprintf(&b, "--%s--\r\n", boundary)
	return b.Bytes()
}

func randomHex(n int) string {
	buf := make([]byte, n)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}
