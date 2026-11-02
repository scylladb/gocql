package main

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
)

// Heap snapshots retain both allocation and live-object samples. Subtract the
// before snapshot with pprof -base to isolate the measured phase's allocations.
// The forced collections and profile writes happen outside phase's counters.
type measurementProfiles struct {
	prefix string
	cpu    *os.File
}

func heapProfile(path string) error {
	runtime.GC()
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	return errors.Join(pprof.WriteHeapProfile(f), f.Close())
}

func startProfiles(prefix string) (*measurementProfiles, error) {
	p := &measurementProfiles{prefix: prefix}
	if prefix == "" {
		return p, nil
	}
	if err := os.MkdirAll(filepath.Dir(prefix), 0755); err != nil {
		return nil, err
	}
	if err := heapProfile(prefix + ".heap.before.pprof"); err != nil {
		return nil, err
	}
	f, err := os.Create(prefix + ".cpu.pprof")
	if err != nil {
		return nil, err
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	p.cpu = f
	return p, nil
}

func (p *measurementProfiles) stop() error {
	if p.cpu == nil {
		return nil
	}
	pprof.StopCPUProfile()
	return errors.Join(p.cpu.Close(), heapProfile(p.prefix+".heap.after.pprof"))
}
