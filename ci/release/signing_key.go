package main

import _ "embed"

var (
	//go:embed release-signing-key.asc
	trustedPublicKey []byte

	//go:embed release-signing-key.fingerprint
	trustedFingerprint []byte
)
