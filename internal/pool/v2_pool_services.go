// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package pool

import (
	"context"
	"net"
)

type InternalPoolServices struct {
	Context      context.Context
	Logger       StdLogger
	Config       *Config
	Conviction   ConvictionPolicy
	Reconnection ReconnectionPolicy
	Worker       func(func()) bool
	Dial         func(context.Context, *HostInfo, ConnErrorHandler, int, int) (*Conn, error)
	Connected    func(*InternalHostConnPool)
	Down         func(net.IP, int)
}

func (p *InternalHostConnPool) typedConfig() (*Config, bool) {
	return p.InternalServices.Config, p.InternalServices.Config != nil
}
