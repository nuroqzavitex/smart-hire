// Command keygen writes an RSA 2048 private key as a PKCS#8 PEM file for the
// auth service. An existing file is never overwritten.
package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

const keyBits = 2048

func main() {
	out := flag.String("out", "deploy/docker/secrets/jwt.pem", "output path of the private key")
	flag.Parse()
	if err := run(*out); err != nil {
		fmt.Fprintln(os.Stderr, "keygen:", err)
		os.Exit(1)
	}
}

func run(out string) error {
	if _, err := os.Stat(out); err == nil {
		fmt.Printf("keygen: %s already exists, keeping it\n", out)
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	key, err := rsa.GenerateKey(rand.Reader, keyBits)
	if err != nil {
		return err
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o700); err != nil {
		return err
	}
	// The file is bind-mounted into the distroless auth container, which runs
	// as a different uid (nonroot) than the host user, so it must be readable
	// by others; the 0700 directory keeps other host users out.
	// O_EXCL guards against a file created between the Stat and here.
	//nolint:gosec // see above: read access is restricted by the directory
	f, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if err := pem.Encode(f, &pem.Block{Type: "PRIVATE KEY", Bytes: der}); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	fmt.Printf("keygen: wrote %s\n", out)
	return nil
}
