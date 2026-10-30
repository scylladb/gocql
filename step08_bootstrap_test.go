//go:build unit

package gocql

import (
	"context"
	"errors"
	"net"
	"testing"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

func TestStep08BootstrapPreservesCause(t *testing.T) {
	for _, cause := range []error{errors.New("fixture dial rejected"), &frm.ErrorFrame{Code: ErrCodeCredentials, Message: "rejected"}} {
		config := DefaultConfig("127.0.20.1")
		config.Protocol = 4
		config.Logger = nopLogger{}
		config.Dialer = DialerFunc(func(context.Context, DialTarget) (net.Conn, error) { return nil, cause })
		session, err := Connect(context.Background(), config)
		if session != nil {
			session.Close()
		}
		if !errors.Is(err, cause) {
			t.Fatalf("bootstrap discarded dial cause: %v", err)
		}
		if request, ok := cause.(RequestError); ok {
			var got RequestError
			if !errors.As(err, &got) || got.GetCode() != request.GetCode() {
				t.Fatal("bootstrap discarded server error", err)
			}
		}
	}
}
