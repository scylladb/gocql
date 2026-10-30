// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package pool

func (p *InternalHostConnPool) InternalNotifyReservationChanged() { p.signalReservation() }
