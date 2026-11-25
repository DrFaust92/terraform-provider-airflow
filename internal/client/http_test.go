package client

import (
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
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

type testCA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM string
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testCA{cert: cert, key: key, certPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))}
}

func (ca *testCA) issue(t *testing.T, usage x509.ExtKeyUsage) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}

func okHandler(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }

// newTLSServer starts an HTTPS server with a certificate from ca; with
// clientCA set it requires a client certificate signed by it.
func newTLSServer(t *testing.T, ca, clientCA *testCA) *httptest.Server {
	t.Helper()
	certPEM, keyPEM := ca.issue(t, x509.ExtKeyUsageServerAuth)
	pair, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(okHandler))
	srv.Config.ErrorLog = nil
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{pair}}
	if clientCA != nil {
		pool := x509.NewCertPool()
		pool.AddCert(clientCA.cert)
		srv.TLS.ClientCAs = pool
		srv.TLS.ClientAuth = tls.RequireAndVerifyClientCert
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, disableSSL bool, opts HTTPOptions, url string) error {
	t.Helper()
	c, err := newHTTPClient(disableSSL, opts)
	if err != nil {
		t.Fatalf("newHTTPClient: %v", err)
	}
	resp, err := c.Get(url)
	if err == nil {
		resp.Body.Close()
	}
	return err
}

func TestHTTPClient_caCertificate(t *testing.T) {
	ca := newTestCA(t)
	srv := newTLSServer(t, ca, nil)

	if err := get(t, false, HTTPOptions{}, srv.URL); err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("without ca_certificate: expected a certificate error, got %v", err)
	}
	if err := get(t, false, HTTPOptions{CACertificate: ca.certPEM}, srv.URL); err != nil {
		t.Fatalf("with ca_certificate: %v", err)
	}
	if err := get(t, true, HTTPOptions{}, srv.URL); err != nil {
		t.Fatalf("with disable_ssl_verification: %v", err)
	}
}

func TestHTTPClient_clientCertificate(t *testing.T) {
	ca, clientCA := newTestCA(t), newTestCA(t)
	srv := newTLSServer(t, ca, clientCA)

	if err := get(t, false, HTTPOptions{CACertificate: ca.certPEM}, srv.URL); err == nil {
		t.Fatal("without a client certificate: expected the server to reject the connection")
	}
	certPEM, keyPEM := clientCA.issue(t, x509.ExtKeyUsageClientAuth)
	if err := get(t, false, HTTPOptions{CACertificate: ca.certPEM, ClientCertificate: certPEM, ClientKey: keyPEM}, srv.URL); err != nil {
		t.Fatalf("with a client certificate: %v", err)
	}
	otherCert, otherKey := newTestCA(t).issue(t, x509.ExtKeyUsageClientAuth)
	if err := get(t, false, HTTPOptions{CACertificate: ca.certPEM, ClientCertificate: otherCert, ClientKey: otherKey}, srv.URL); err == nil {
		t.Fatal("with an untrusted client certificate: expected the server to reject the connection")
	}
}

func TestHTTPClient_timeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	start := time.Now()
	if err := get(t, false, HTTPOptions{Timeout: 100 * time.Millisecond}, srv.URL); err == nil {
		t.Fatal("expected a timeout error")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("timeout took %s", elapsed)
	}

	c, err := newHTTPClient(false, HTTPOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if c.Timeout != DefaultTimeout {
		t.Fatalf("default timeout = %s, want %s", c.Timeout, DefaultTimeout)
	}
}

// TestHTTPClient_proxy checks that requests go through HTTP_PROXY, including
// with disable_ssl_verification, which used to build a transport without a
// proxy. Go reads the proxy environment once per process, so the requests run
// in a child process started with the proxy set.
func TestHTTPClient_proxy(t *testing.T) {
	if os.Getenv("AIRFLOW_TEST_PROXY_CHILD") == "1" {
		for _, disableSSL := range []bool{false, true} {
			// The host does not resolve: only the proxy can answer.
			if err := get(t, disableSSL, HTTPOptions{}, "http://airflow.invalid:8080/api/v2/version"); err != nil {
				t.Fatalf("disable_ssl_verification=%v: %v", disableSSL, err)
			}
		}
		return
	}

	var (
		mu    sync.Mutex
		hosts []string
	)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hosts = append(hosts, r.Host)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(proxy.Close)

	cmd := exec.Command(os.Args[0], "-test.run=^TestHTTPClient_proxy$")
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		switch strings.ToUpper(name) {
		case "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "REQUEST_METHOD":
			continue
		}
		cmd.Env = append(cmd.Env, kv)
	}
	cmd.Env = append(cmd.Env, "AIRFLOW_TEST_PROXY_CHILD=1", "HTTP_PROXY="+proxy.URL)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child process: %v\n%s", err, out)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(hosts) != 2 || hosts[0] != "airflow.invalid:8080" || hosts[1] != "airflow.invalid:8080" {
		t.Fatalf("proxy saw %v, want two requests for airflow.invalid:8080", hosts)
	}
}

func TestHTTPClient_errors(t *testing.T) {
	ca := newTestCA(t)
	certPEM, keyPEM := ca.issue(t, x509.ExtKeyUsageClientAuth)
	_, otherKey := ca.issue(t, x509.ExtKeyUsageClientAuth)

	cases := map[string]struct {
		opts HTTPOptions
		want string
	}{
		"bad CA":           {HTTPOptions{CACertificate: "not a certificate"}, "ca_certificate"},
		"cert without key": {HTTPOptions{ClientCertificate: certPEM}, "must be set together"},
		"key without cert": {HTTPOptions{ClientKey: keyPEM}, "must be set together"},
		"mismatched key":   {HTTPOptions{ClientCertificate: certPEM, ClientKey: otherKey}, "client_certificate and client_key"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := newHTTPClient(false, tc.opts); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected an error containing %q, got %v", tc.want, err)
			}
		})
	}
}
