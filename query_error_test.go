//go:build unit
// +build unit

package gocql

import (
	"errors"
	"testing"
	"time"
)

func TestQueryError_PotentiallyExecuted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		potentiallyExecuted bool
		expected            bool
	}{
		{
			name:                "potentially executed true",
			potentiallyExecuted: true,
			expected:            true,
		},
		{
			name:                "potentially executed false",
			potentiallyExecuted: false,
			expected:            false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qErr := &QueryError{
				err:                 errors.New("test error"),
				potentiallyExecuted: tt.potentiallyExecuted,
			}

			got := qErr.PotentiallyExecuted()
			if got != tt.expected {
				t.Fatalf("QueryError.PotentiallyExecuted() = %v, expected %v", got, tt.expected)
			}
		})
	}
}

func TestQueryError_IsIdempotent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		isIdempotent bool
		expected     bool
	}{
		{
			name:         "idempotent true",
			isIdempotent: true,
			expected:     true,
		},
		{
			name:         "idempotent false",
			isIdempotent: false,
			expected:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qErr := &QueryError{
				err:          errors.New("test error"),
				isIdempotent: tt.isIdempotent,
			}

			got := qErr.IsIdempotent()
			if got != tt.expected {
				t.Errorf("QueryError.IsIdempotent() = %v, expected %v", got, tt.expected)
			}
		})
	}
}

func TestQueryError_Error(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		err                 error
		potentiallyExecuted bool
		timeout             time.Duration
		inFlight            int
		expected            string
	}{
		{
			name:                "with potentially executed true",
			err:                 errors.New("connection error"),
			potentiallyExecuted: true,
			expected:            "connection error (potentially executed: true)",
		},
		{
			name:                "with potentially executed false",
			err:                 errors.New("syntax error"),
			potentiallyExecuted: false,
			expected:            "syntax error (potentially executed: false)",
		},
		{
			name:                "with timeout",
			err:                 ErrTimeoutNoResponse,
			potentiallyExecuted: true,
			timeout:             11 * time.Second,
			inFlight:            42,
			expected:            "gocql: no response received from cassandra within timeout period (timeout: 11s, in-flight: 42) (potentially executed: true)",
		},
		{
			name:                "with zero timeout omits timeout",
			err:                 errors.New("some error"),
			potentiallyExecuted: false,
			timeout:             0,
			expected:            "some error (potentially executed: false)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qErr := &QueryError{
				err:                 tt.err,
				potentiallyExecuted: tt.potentiallyExecuted,
				timeout:             tt.timeout,
				inFlight:            tt.inFlight,
			}

			got := qErr.Error()
			if got != tt.expected {
				t.Errorf("QueryError.Error() = %v, expected %v", got, tt.expected)
			}
		})
	}
}

func TestQueryError_Timeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		timeout  time.Duration
		expected time.Duration
	}{
		{
			name:     "with timeout set",
			timeout:  11 * time.Second,
			expected: 11 * time.Second,
		},
		{
			name:     "with zero timeout",
			timeout:  0,
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qErr := &QueryError{
				err:     errors.New("test error"),
				timeout: tt.timeout,
			}

			got := qErr.Timeout()
			if got != tt.expected {
				t.Errorf("QueryError.Timeout() = %v, expected %v", got, tt.expected)
			}
		})
	}
}

func TestQueryError_InFlight(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		inFlight int
		expected int
	}{
		{
			name:     "with in-flight requests",
			inFlight: 42,
			expected: 42,
		},
		{
			name:     "with zero in-flight",
			inFlight: 0,
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qErr := &QueryError{
				err:      errors.New("test error"),
				inFlight: tt.inFlight,
			}

			got := qErr.InFlight()
			if got != tt.expected {
				t.Errorf("QueryError.InFlight() = %v, expected %v", got, tt.expected)
			}
		})
	}
}
