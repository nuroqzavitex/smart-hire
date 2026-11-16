package main

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func TestMinimalPDF_Structure(t *testing.T) {
	b := minimalPDF([]string{"Smoke (Candidate)", `back\slash`})
	s := string(b)
	if !bytes.HasPrefix(b, []byte("%PDF-1.4\n")) || !bytes.HasSuffix(b, []byte("%%EOF\n")) {
		t.Fatalf("bad header/trailer")
	}
	for _, want := range []string{`(Smoke \(Candidate\)) Tj`, `(back\\slash) Tj`} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %q", want)
		}
	}

	m := regexp.MustCompile(`startxref\n(\d+)\n`).FindStringSubmatch(s)
	if m == nil {
		t.Fatal("no startxref")
	}
	xref, _ := strconv.Atoi(m[1])
	if !strings.HasPrefix(s[xref:], "xref\n") {
		t.Fatalf("startxref %d does not point at xref", xref)
	}
	entries := regexp.MustCompile(`(\d{10}) 00000 n \n`).FindAllStringSubmatch(s[xref:], -1)
	if len(entries) != 5 {
		t.Fatalf("got %d xref entries, want 5", len(entries))
	}
	for i, e := range entries {
		off, _ := strconv.Atoi(e[1])
		if want := fmt.Sprintf("%d 0 obj\n", i+1); !strings.HasPrefix(s[off:], want) {
			t.Errorf("entry %d offset %d does not point at %q", i+1, off, want)
		}
	}
}
