//go:build unit
// +build unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"
)

// generateCertificate creates a test certificate
func generateCertificate(isCA bool, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, err
	}

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Test Org"},
			CommonName:   "Test Certificate",
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  isCA,
	}

	if isCA {
		template.KeyUsage |= x509.KeyUsageCertSign
	}

	// If parent is nil, this is a self-signed certificate
	signingCert := template
	signingKey := privateKey
	if parent != nil {
		signingCert = parent
		signingKey = parentKey
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, signingCert, &privateKey.PublicKey, signingKey)
	if err != nil {
		return nil, nil, err
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, nil, err
	}

	return cert, privateKey, nil
}

func TestStrictVerifyPeerCertificate(t *testing.T) {
	t.Parallel()

	// Generate a valid certificate chain: Root CA -> Intermediate CA -> Leaf
	rootCA, rootKey, err := generateCertificate(true, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate root CA: %v", err)
	}

	intermediateCA, intermediateKey, err := generateCertificate(true, rootCA, rootKey)
	if err != nil {
		t.Fatalf("failed to generate intermediate CA: %v", err)
	}

	leafCert, _, err := generateCertificate(false, intermediateCA, intermediateKey)
	if err != nil {
		t.Fatalf("failed to generate leaf certificate: %v", err)
	}

	// Create a trusted root pool with the root CA
	rootPool := x509.NewCertPool()
	rootPool.AddCert(rootCA)

	t.Run("valid certificate chain", func(t *testing.T) {
		verifyFunc := strictVerifyPeerCertificate(rootPool)

		// Prepare the certificate chain as it would be presented during TLS handshake
		rawCerts := [][]byte{
			leafCert.Raw,
			intermediateCA.Raw,
		}

		err := verifyFunc(rawCerts, nil)
		if err != nil {
			t.Errorf("expected valid chain to pass verification, got error: %v", err)
		}
	})

	t.Run("empty certificate chain", func(t *testing.T) {
		verifyFunc := strictVerifyPeerCertificate(rootPool)

		err := verifyFunc([][]byte{}, nil)
		if err == nil {
			t.Error("expected error for empty certificate chain")
		}
	})

	t.Run("chain with only intermediate CA in root pool", func(t *testing.T) {
		// Create a pool with only the intermediate CA (not the root)
		intermediatePool := x509.NewCertPool()
		intermediatePool.AddCert(intermediateCA)

		verifyFunc := strictVerifyPeerCertificate(intermediatePool)

		rawCerts := [][]byte{
			leafCert.Raw,
			intermediateCA.Raw,
		}

		// This should fail because the intermediate CA is not self-signed
		// and thus doesn't qualify as a trusted root
		err := verifyFunc(rawCerts, nil)
		if err == nil {
			t.Error("expected error when intermediate CA is in root pool but chain doesn't terminate at self-signed root")
		}
	})

	t.Run("self-signed CA certificate", func(t *testing.T) {
		// Generate a self-signed CA certificate
		selfSigned, _, err := generateCertificate(true, nil, nil)
		if err != nil {
			t.Fatalf("failed to generate self-signed certificate: %v", err)
		}

		// Add it to the root pool
		selfSignedPool := x509.NewCertPool()
		selfSignedPool.AddCert(selfSigned)

		verifyFunc := strictVerifyPeerCertificate(selfSignedPool)

		rawCerts := [][]byte{
			selfSigned.Raw,
		}

		err = verifyFunc(rawCerts, nil)
		if err != nil {
			t.Errorf("expected self-signed CA certificate to pass verification when in root pool, got error: %v", err)
		}
	})

	t.Run("untrusted root", func(t *testing.T) {
		// Generate a different root CA that's not in our trust pool
		untrustedRoot, untrustedRootKey, err := generateCertificate(true, nil, nil)
		if err != nil {
			t.Fatalf("failed to generate untrusted root CA: %v", err)
		}

		untrustedIntermediate, untrustedIntermediateKey, err := generateCertificate(true, untrustedRoot, untrustedRootKey)
		if err != nil {
			t.Fatalf("failed to generate untrusted intermediate CA: %v", err)
		}

		untrustedLeaf, _, err := generateCertificate(false, untrustedIntermediate, untrustedIntermediateKey)
		if err != nil {
			t.Fatalf("failed to generate untrusted leaf certificate: %v", err)
		}

		verifyFunc := strictVerifyPeerCertificate(rootPool)

		rawCerts := [][]byte{
			untrustedLeaf.Raw,
			untrustedIntermediate.Raw,
		}

		err = verifyFunc(rawCerts, nil)
		if err == nil {
			t.Error("expected error for certificate chain with untrusted root")
		}
	})
}

func TestSetupTLSConfigStrictValidation(t *testing.T) {
	t.Parallel()

	t.Run("VerifyPeerCertificate set when verification enabled", func(t *testing.T) {
		opts := &SslOptions{
			EnableHostVerification: true,
		}

		tlsConfig, err := setupTLSConfig(opts)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if tlsConfig.InsecureSkipVerify {
			t.Error("expected InsecureSkipVerify to be false")
		}

		if tlsConfig.VerifyPeerCertificate == nil {
			t.Error("expected VerifyPeerCertificate to be set when verification is enabled")
		}
	})

	t.Run("VerifyPeerCertificate not set when verification disabled", func(t *testing.T) {
		opts := &SslOptions{
			EnableHostVerification: false,
		}

		tlsConfig, err := setupTLSConfig(opts)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !tlsConfig.InsecureSkipVerify {
			t.Error("expected InsecureSkipVerify to be true")
		}

		if tlsConfig.VerifyPeerCertificate != nil {
			t.Error("expected VerifyPeerCertificate to not be set when verification is disabled")
		}
	})

	t.Run("VerifyPeerCertificate set with custom tls.Config", func(t *testing.T) {
		opts := &SslOptions{
			Config: &tls.Config{
				InsecureSkipVerify: false,
			},
		}

		tlsConfig, err := setupTLSConfig(opts)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if tlsConfig.InsecureSkipVerify {
			t.Error("expected InsecureSkipVerify to be false")
		}

		if tlsConfig.VerifyPeerCertificate == nil {
			t.Error("expected VerifyPeerCertificate to be set")
		}
	})
}

func pemEncodeCert(cert *x509.Certificate) []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert.Raw,
	})
}

func pemEncodeKey(key *ecdsa.PrivateKey) ([]byte, error) {
	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: keyBytes,
	}), nil
}
