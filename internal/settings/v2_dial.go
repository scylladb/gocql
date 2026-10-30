// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package settings

import (
	"crypto/tls"
	"crypto/x509"
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"

	"path/filepath"
	"slices"

	"strings"
)

// Parsed keys, callbacks, session caches and provider objects remain shared by
// their documented concurrency contracts. Mutable certificate/trust containers
// and wire bytes are copied before any connection uses them.
func InternalCloneV2TLSConfig(source *tls.Config) *tls.Config {
	if source == nil {
		return nil
	}
	c := source.Clone()
	if source.RootCAs != nil {
		c.RootCAs = source.RootCAs.Clone()
	}
	if source.ClientCAs != nil {
		c.ClientCAs = source.ClientCAs.Clone()
	}
	c.NextProtos = slices.Clone(source.NextProtos)
	c.CipherSuites = slices.Clone(source.CipherSuites)
	c.CurvePreferences = slices.Clone(source.CurvePreferences)
	cloneCert := func(v tls.Certificate) tls.Certificate {
		v.Certificate = slices.Clone(v.Certificate)
		for i, b := range v.Certificate {
			v.Certificate[i] = slices.Clone(b)
		}
		v.OCSPStaple = slices.Clone(v.OCSPStaple)
		v.SignedCertificateTimestamps = slices.Clone(v.SignedCertificateTimestamps)
		for i, b := range v.SignedCertificateTimestamps {
			v.SignedCertificateTimestamps[i] = slices.Clone(b)
		}
		v.SupportedSignatureAlgorithms = slices.Clone(v.SupportedSignatureAlgorithms)
		if v.Leaf != nil && len(v.Leaf.Raw) > 0 {
			if leaf, err := x509.ParseCertificate(slices.Clone(v.Leaf.Raw)); err == nil {
				v.Leaf = leaf
			}
		}
		return v
	}
	c.Certificates = slices.Clone(source.Certificates)
	for i, v := range c.Certificates {
		c.Certificates[i] = cloneCert(v)
	}
	if source.NameToCertificate != nil {
		c.NameToCertificate = make(map[string]*tls.Certificate, len(source.NameToCertificate))
		for n, v := range source.NameToCertificate {
			if v != nil {
				copy := cloneCert(*v)
				c.NameToCertificate[n] = &copy
			}
		}
	}
	return c
}

// UnsupportedSettingError identifies a requested setting without support on
// the selected transport. It never indicates a silently changed setting.
type UnsupportedSettingError = runtimeerrors.UnsupportedSettingError

func InternalUnixContact(config Config) (string, error) {
	path := ""
	for _, point := range config.ContactPoints {
		if strings.HasPrefix(point, "unix:") || filepath.IsAbs(point) {
			if len(config.ContactPoints) != 1 {
				return "", typeValidation("contact points", "Unix and TCP contacts cannot be mixed")
			}
			path = strings.TrimPrefix(point, "unix:")
			if !filepath.IsAbs(path) {
				return "", typeValidation("Unix contact point", "socket path must be absolute")
			}
		}
	}
	if path != "" {
		if config.Metadata.InitialDiscovery {
			return "", &UnsupportedSettingError{Setting: "Unix discovery", Reason: "one local socket requires InitialDiscovery=false"}
		}
		if config.Protocol != 3 {
			return "", &UnsupportedSettingError{Setting: "Unix protocol", Reason: "explicit native protocol 3 is required"}
		}
		if config.TLS != nil || config.TLSForHost != nil {
			return "", &UnsupportedSettingError{Setting: "Unix TLS", Reason: "maintenance sockets carry plaintext native protocol"}
		}
		if config.LocalAddress.IsValid() {
			return "", &UnsupportedSettingError{Setting: "Unix local address", Reason: "source address/port selection requires TCP"}
		}
	}
	return path, nil
}
