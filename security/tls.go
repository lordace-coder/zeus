// Self-signed TLS certificate generator.
//
// In production you'd use a cert from Let's Encrypt or your CA.
// For local development and quick setups, Zeus can generate its own.
package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"time"
)

// ensureSelfSignedCert generates a self-signed ECDSA certificate and writes
// it to certFile and keyFile — but ONLY if those files don't already exist.
// This prevents overwriting a real cert on restarts.
func ensureSelfSignedCert(certFile, keyFile string) error {
	// If both files already exist, nothing to do
	_, certErr := os.Stat(certFile)
	_, keyErr := os.Stat(keyFile)
	if certErr == nil && keyErr == nil {
		return nil // already have a cert
	}

	// Generate ECDSA P-256 key (smaller and faster than RSA for the same security)
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}

	// Build the certificate template
	template := x509.Certificate{
		// Large random serial number to avoid collisions in certificate stores
		SerialNumber: newSerialNumber(),
		Subject: pkix.Name{
			Organization: []string{"Zeus Self-Signed"},
			CommonName:   "zeus-server",
		},
		NotBefore:             time.Now().Add(-time.Minute), // small backdate for clock skew
		NotAfter:              time.Now().Add(365 * 24 * time.Hour), // valid 1 year
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// Self-sign: the cert is signed by its own key (issuer == subject)
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privKey.PublicKey, privKey)
	if err != nil {
		return err
	}

	// Write certificate PEM
	certOut, err := os.Create(certFile)
	if err != nil {
		return err
	}
	defer certOut.Close()
	if err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		return err
	}

	// Write private key PEM
	keyOut, err := os.OpenFile(keyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer keyOut.Close()
	privDER, err := x509.MarshalECPrivateKey(privKey)
	if err != nil {
		return err
	}
	return pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER})
}

// newSerialNumber generates a random 128-bit serial number for the cert.
func newSerialNumber() *big.Int {
	n, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	return n
}
