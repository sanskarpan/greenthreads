package web

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	goruntime "github.com/sanskarpan/greenthreads/internal/runtime"
	"github.com/sanskarpan/greenthreads/internal/scheduler"
)

// writeSelfSignedCert generates a throwaway self-signed certificate valid for
// 127.0.0.1 and writes cert.pem/key.pem into a temp dir, returning their paths.
func writeSelfSignedCert(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	// NotBefore is intentionally in the past so validity does not depend on
	// exact clock alignment during the test.
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "greenthreads-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")

	certOut, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("create cert file: %v", err)
	}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		t.Fatalf("encode cert: %v", err)
	}
	_ = certOut.Close()

	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyOut, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("create key file: %v", err)
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}); err != nil {
		t.Fatalf("encode key: %v", err)
	}
	_ = keyOut.Close()
	return certPath, keyPath
}

// TestServerServesHTTPSWithModernTLSAndHSTS covers the previously-untested TLS
// transport: the server must complete an HTTPS handshake at TLS >= 1.2 and set
// the HSTS header only when TLS is enabled.
func TestServerServesHTTPSWithModernTLSAndHSTS(t *testing.T) {
	certPath, keyPath := writeSelfSignedCert(t)

	cfg := DefaultConfig()
	cfg.TLSCertFile = certPath
	cfg.TLSKeyFile = keyPath

	rt := goruntime.NewRuntime(scheduler.TypeFIFO, 1)
	server := NewServerWithConfig(rt, cfg)
	addr := randomAddr(t)

	errCh := make(chan error, 1)
	go func() { errCh <- server.Start(addr) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	client := &http.Client{
		Transport: &http.Transport{
			// The cert is self-signed; trust it for the test only.
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}, //nolint:gosec // test-only self-signed cert
		},
		Timeout: 3 * time.Second,
	}

	url := "https://" + addr + "/healthz"
	var resp *http.Response
	deadline := time.Now().Add(3 * time.Second)
	for {
		var err error
		resp, err = client.Get(url)
		if err == nil {
			break
		}
		select {
		case startErr := <-errCh:
			t.Fatalf("server.Start returned early: %v", startErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("HTTPS GET never succeeded: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz over TLS status = %d, want 200", resp.StatusCode)
	}
	if resp.TLS == nil {
		t.Fatal("response has no TLS connection state")
	}
	if resp.TLS.Version < tls.VersionTLS12 {
		t.Fatalf("negotiated TLS version 0x%04x, want >= TLS 1.2 (0x%04x)", resp.TLS.Version, tls.VersionTLS12)
	}
	if got := resp.Header.Get("Strict-Transport-Security"); got == "" {
		t.Fatal("HSTS header missing on a TLS server")
	}
}

// TestServerFailFastOnMissingTLSCert verifies the startup config validation:
// a configured but missing cert file must fail Start immediately, not on the
// first connection.
func TestServerFailFastOnMissingTLSCert(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TLSCertFile = filepath.Join(t.TempDir(), "does-not-exist.pem")
	cfg.TLSKeyFile = filepath.Join(t.TempDir(), "missing-key.pem")

	rt := goruntime.NewRuntime(scheduler.TypeFIFO, 1)
	server := NewServerWithConfig(rt, cfg)

	err := server.Start(randomAddr(t))
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		t.Fatal("Start succeeded with a missing TLS cert; want a fail-fast error")
	}
}

// TestPlaintextServerHasNoHSTS confirms HSTS is not emitted without TLS, so a
// plaintext dev server cannot poison the browser HSTS cache for the host.
func TestPlaintextServerHasNoHSTS(t *testing.T) {
	t.Parallel()
	server := NewServer(nil)
	handler := server.Handler()

	req, _ := http.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if got := rec.Header().Get("Strict-Transport-Security"); got != "" {
		t.Fatalf("plaintext server set HSTS header: %q", got)
	}
}
