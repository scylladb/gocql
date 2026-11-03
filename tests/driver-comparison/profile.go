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
	prefix                string
	cpu                   *os.File
	synchronization       bool
	previousMutexFraction int
}

func heapProfile(path string) error {
	runtime.GC()
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	return errors.Join(pprof.WriteHeapProfile(f), f.Close())
}

func startProfiles(prefix string, synchronization ...bool) (*measurementProfiles, error) {
	p := &measurementProfiles{prefix: prefix}
	p.synchronization = len(synchronization) > 0 && synchronization[0]
	if p.synchronization && prefix == "" {
		return nil, errors.New("synchronization profiles require a diagnostic profile prefix")
	}
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
	if p.synchronization {
		runtime.SetBlockProfileRate(1)
		p.previousMutexFraction = runtime.SetMutexProfileFraction(1)
	}
	return p, nil
}

func writeNamedProfile(name, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	return errors.Join(pprof.Lookup(name).WriteTo(f, 0), f.Close())
}

func (p *measurementProfiles) stop() error {
	if p.cpu == nil {
		return nil
	}
	pprof.StopCPUProfile()
	var synchronizationErr error
	if p.synchronization {
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(p.previousMutexFraction)
		synchronizationErr = errors.Join(writeNamedProfile("block", p.prefix+".block.pprof"),
			writeNamedProfile("mutex", p.prefix+".mutex.pprof"))
	}
	return errors.Join(p.cpu.Close(), synchronizationErr, heapProfile(p.prefix+".heap.after.pprof"))
}
