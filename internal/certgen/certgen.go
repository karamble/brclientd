// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package certgen produces the mTLS cert triplet brclientd uses for its
// pre-setup endpoint and the clientrpc surface. File naming matches the
// brclient / dcrlnd convention: rpc-ca.cert, rpc-ca.key, rpc.cert, rpc.key,
// rpc-client.cert, rpc-client.key.
package certgen

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// Triplet groups the on-disk paths for the CA + server + client cert pairs.
type Triplet struct {
	Dir            string
	CACertPath     string
	CAKeyPath      string
	ServerCertPath string
	ServerKeyPath  string
	ClientCertPath string
	ClientKeyPath  string
}

// PathsIn returns the standard triplet rooted at dir.
func PathsIn(dir string) Triplet {
	return Triplet{
		Dir:            dir,
		CACertPath:     filepath.Join(dir, "rpc-ca.cert"),
		CAKeyPath:      filepath.Join(dir, "rpc-ca.key"),
		ServerCertPath: filepath.Join(dir, "rpc.cert"),
		ServerKeyPath:  filepath.Join(dir, "rpc.key"),
		ClientCertPath: filepath.Join(dir, "rpc-client.cert"),
		ClientKeyPath:  filepath.Join(dir, "rpc-client.key"),
	}
}

// AllPresent reports whether every file in the triplet exists on disk.
func (t Triplet) AllPresent() (bool, error) {
	for _, p := range []string{t.CACertPath, t.CAKeyPath, t.ServerCertPath, t.ServerKeyPath, t.ClientCertPath, t.ClientKeyPath} {
		if _, err := os.Stat(p); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return false, nil
			}
			return false, fmt.Errorf("stat %s: %w", p, err)
		}
	}
	return true, nil
}

// Generate writes a fresh cert triplet to t.Dir. The server cert's Subject
// Alternative Names list is populated from hosts; pass every name (and IP)
// consumers will dial.
func (t Triplet) Generate(hosts []string) error {
	if err := os.MkdirAll(t.Dir, 0o700); err != nil {
		return fmt.Errorf("create cert dir %s: %w", t.Dir, err)
	}

	caPub, caPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return fmt.Errorf("generate CA key: %w", err)
	}
	caSerial, err := randomSerial()
	if err != nil {
		return err
	}
	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber:          caSerial,
		Subject:               pkix.Name{CommonName: "brclientd CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caPub, caPriv)
	if err != nil {
		return fmt.Errorf("create CA cert: %w", err)
	}
	if err := writePEM(t.CACertPath, "CERTIFICATE", caDER, 0o644); err != nil {
		return err
	}
	caKeyDER, err := x509.MarshalPKCS8PrivateKey(caPriv)
	if err != nil {
		return err
	}
	if err := writePEM(t.CAKeyPath, "PRIVATE KEY", caKeyDER, 0o600); err != nil {
		return err
	}

	if err := signLeaf(caTemplate, caPriv, "brclientd", hosts, x509.ExtKeyUsageServerAuth, t.ServerCertPath, t.ServerKeyPath); err != nil {
		return fmt.Errorf("server cert: %w", err)
	}
	if err := signLeaf(caTemplate, caPriv, "brclientd-client", nil, x509.ExtKeyUsageClientAuth, t.ClientCertPath, t.ClientKeyPath); err != nil {
		return fmt.Errorf("client cert: %w", err)
	}
	return nil
}

func signLeaf(caCert *x509.Certificate, caPriv ed25519.PrivateKey, commonName string, hosts []string, eku x509.ExtKeyUsage, certPath, keyPath string) error {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	serial, err := randomSerial()
	if err != nil {
		return err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.AddDate(1, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{eku},
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, pub, caPriv)
	if err != nil {
		return err
	}
	if err := writePEM(certPath, "CERTIFICATE", der, 0o644); err != nil {
		return err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return err
	}
	return writePEM(keyPath, "PRIVATE KEY", keyDER, 0o600)
}

func writePEM(path, blockType string, der []byte, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer f.Close()
	return pem.Encode(f, &pem.Block{Type: blockType, Bytes: der})
}

func randomSerial() (*big.Int, error) {
	max := new(big.Int).Lsh(big.NewInt(1), 128)
	return rand.Int(rand.Reader, max)
}
