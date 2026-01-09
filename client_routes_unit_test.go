//go:build unit
// +build unit

package gocql

import (
	"net"
	"testing"
)

type dnsResolverFunc func(string) ([]net.IP, error)

// LookupIP implements DNSResolver for dnsResolverFunc.
func (f dnsResolverFunc) LookupIP(host string) ([]net.IP, error) { return f(host) }

func TestDNSEndpointResolver(t *testing.T) {
}
