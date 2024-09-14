package funcs

// Funcs is a set of functions that are used in serialization.NewCase, gen.Group and gen.Set.
// Marshal, Unmarshal functions should be provided.
type Funcs struct {
	Marshal   func(interface{}) ([]byte, error)
	Unmarshal func([]byte, interface{}) error

	EqualData func(in1, in2 []byte) bool
	NewVar    func(interface{}) interface{}
	EqualVals func(in1, in2 interface{}) bool
}

func (f *Funcs) Copy() *Funcs {
	if f == nil {
		return nil
	}
	out := &Funcs{
		EqualData: f.EqualData,
		NewVar:    f.NewVar,
		EqualVals: f.EqualVals,
	}
	if f.Marshal != nil {
		out.Marshal = f.Marshal
	}
	if f.Unmarshal != nil {
		out.Unmarshal = f.Unmarshal
	}
	return out
}

func (f *Funcs) ExcludeMarshal() *Funcs {
	cp := f.Copy()
	cp.Marshal = excludedMarshal
	return cp
}

func (f *Funcs) ExcludeUnmarshal() *Funcs {
	cp := f.Copy()
	cp.Unmarshal = excludedUnmarshal
	return cp
}

func (f *Funcs) Valid() bool {
	if f.EqualData == nil {
		return false
	}
	if f.NewVar == nil {
		return false
	}
	if f.EqualVals == nil {
		return false
	}
	if f.Unmarshal == nil {
		return false
	}
	if f.Marshal == nil {
		return false
	}
	return true
}

func (f *Funcs) PutDefaults(d *Funcs) {
	if f.Marshal == nil {
		f.Marshal = d.Marshal
	}
	if f.Unmarshal == nil {
		f.Unmarshal = d.Unmarshal
	}
	if f.EqualData == nil {
		f.EqualData = d.EqualData
	}
	if f.NewVar == nil {
		f.NewVar = d.NewVar
	}
	if f.EqualVals == nil {
		f.EqualVals = d.EqualVals
	}
}
