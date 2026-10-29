// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"io"
	"strings"
	"unicode/utf8"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// Bytes borrows immutable bytes, which may cross receive segments. A page view
// is valid only while its page or a retained page lease remains alive.
type Bytes struct {
	chain wire.Chain
	lease *wire.Lease
}

// Text is a validated text view. String explicitly materializes an owned string.
type Text struct{ bytes Bytes }

// BorrowBytes borrows b without copying. The caller must keep it immutable until
// all operations using the resulting view have completed.
func BorrowBytes(b []byte) Bytes { c, _ := wire.NewChain(wire.Borrow(b)); return Bytes{chain: c} }

// BorrowText borrows a valid UTF-8 string. Invalid UTF-8 panics with ValidationError.
func BorrowText(s string) Text {
	if !utf8.ValidString(s) {
		panic(typeValidation("text", "invalid UTF-8"))
	}
	c, _ := wire.NewChain(wire.BorrowString(s))
	return Text{bytes: Bytes{chain: c}}
}
func (b Bytes) Len() int64 { return b.chain.Len() }
func (b Bytes) checkAlive() error {
	if b.lease != nil && !b.lease.Alive() {
		return ErrReleased
	}
	return nil
}
func (b Bytes) each(fn func(wire.Span) error) error {
	if err := b.checkAlive(); err != nil {
		return err
	}
	return b.chain.Each(fn)
}
func (b Bytes) At(offset int64) (byte, error) {
	if err := b.checkAlive(); err != nil {
		return 0, err
	}
	if offset < 0 || offset >= b.Len() {
		return 0, wire.ErrBounds
	}
	c, err := b.chain.Slice(offset, 1)
	if err != nil {
		return 0, err
	}
	return wire.NewCursor(c).Uint8()
}
func (b Bytes) Slice(offset, length int64) (Bytes, error) {
	if err := b.checkAlive(); err != nil {
		return Bytes{}, err
	}
	c, err := b.chain.Slice(offset, length)
	if err != nil {
		return Bytes{}, err
	}
	return Bytes{chain: c, lease: b.lease}, nil
}
func (b Bytes) WriteTo(w io.Writer) (int64, error) {
	var total int64
	err := b.each(func(s wire.Span) error {
		n, err := w.Write(s.Bytes())
		if n < 0 || n > s.Len() {
			return io.ErrShortWrite
		}
		total += int64(n)
		if err != nil {
			return err
		}
		if n != s.Len() {
			return io.ErrShortWrite
		}
		return nil
	})
	return total, err
}
func (b Bytes) CopyTo(dst []byte) (int, error) {
	if err := b.checkAlive(); err != nil {
		return 0, err
	}
	return b.chain.CopyTo(dst)
}
func (b Bytes) Clone() Bytes { return BorrowBytes(b.OwnedBytes()) }
func (b Bytes) OwnedBytes() []byte {
	if err := b.checkAlive(); err != nil {
		panic(err)
	}
	if b.Len() > int64(int(^uint(0)>>1)) {
		panic(typeValidation("bytes", "value exceeds maximum allocation size"))
	}
	dst := make([]byte, int(b.Len()))
	if _, err := b.chain.CopyTo(dst); err != nil {
		panic(err)
	}
	return dst
}
func (t Text) Bytes() Bytes { return t.bytes }
func (t Text) Clone() Text  { return Text{bytes: t.bytes.Clone()} }
func (t Text) String() string {
	if err := t.bytes.checkAlive(); err != nil {
		panic(err)
	}
	if t.bytes.Len() > int64(int(^uint(0)>>1)) {
		panic(typeValidation("text", "value exceeds maximum allocation size"))
	}
	var b strings.Builder
	b.Grow(int(t.bytes.Len()))
	if err := t.bytes.each(func(s wire.Span) error { _, err := b.Write(s.Bytes()); return err }); err != nil {
		panic(err)
	}
	return b.String()
}
