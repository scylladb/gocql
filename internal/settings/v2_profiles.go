// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"time"
)

type Profile struct {
	Consistency          Consistency
	SerialConsistency    Option[Consistency]
	Timeout, PageTimeout time.Duration
	PageSize             int32
	Prefetch             int
	Retry                RetryPolicy
	Routing              RoutingPolicy
	Speculation          SpeculationPolicy
}
type ProfileHandle struct{ privateStorage ProfileHandleStorage }

func NewProfile(profile Profile) (*ProfileHandle, error) {
	p := new(ProfileHandle)
	if err := p.Replace(profile); err != nil {
		return nil, err
	}
	return p, nil
}
func (p *ProfileHandle) Snapshot() Profile {
	if p == nil || p.privateStorage.current.Load() == nil {
		return InternalDefaultProfile()
	}
	return *p.privateStorage.current.Load()
}
func (p *ProfileHandle) Replace(profile Profile) error {
	if p == nil {
		return typeValidation("profile", "nil handle")
	}
	if err := InternalValidateProfile(profile); err != nil {
		return err
	}
	p.privateStorage.current.Store(&profile)
	return nil
}
func InternalDefaultProfile() Profile {
	return Profile{Consistency: LocalQuorum, SerialConsistency: Option[Consistency]{Value: LocalSerial, Set: true}, Timeout: 30 * time.Second, PageSize: 5000, Prefetch: 1, Retry: SafeRetryPolicy{MaxAttempts: 3}}
}
func InternalValidateProfile(p Profile) error {
	if !InternalValidConsistency(p.Consistency) {
		return typeValidation("consistency", "invalid consistency")
	}
	if p.SerialConsistency.Set && p.SerialConsistency.Value != Serial && p.SerialConsistency.Value != LocalSerial {
		return typeValidation("serial consistency", "must be SERIAL or LOCAL_SERIAL")
	}
	if p.Timeout < 0 || p.PageTimeout < 0 {
		return typeValidation("timeout", "must not be negative")
	}
	if p.PageSize <= 0 {
		return typeValidation("page size", "must be positive")
	}
	if p.Prefetch < 0 || p.Prefetch > 1 {
		return typeValidation("prefetch", "must be zero or one")
	}
	return nil
}
func InternalValidConsistency(c Consistency) bool { return c >= Any && c <= LocalOne }

type RetryPolicy interface {
	Decide(AttemptError) RetryDecision
}
type SpeculationPolicy interface {
	Delay(attempt int, metrics MetricsSnapshot) (time.Duration, bool)
}

type Observer interface {
	ObserveRequest(RequestEvent)
	ObserveAttempt(AttemptEvent)
}
type RequestEvent struct {
	ID              uint64
	Statement       string
	BatchStatements []string
	Parameters      Columns
	Routing         RoutingInfo
	Result          Result
	Err             error
}
