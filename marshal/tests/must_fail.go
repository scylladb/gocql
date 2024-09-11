package tests

import "testing"

// MustFail is a tool for testing `marshalling` and `unmarshalling` functions in cases where there should be any error or specific error.
type MustFail struct {
	Marshal   *MarshalCases
	Unmarshal *UnmarshalCases
	prepared  bool
}

func (l *MustFail) GetModified() *MustFail {
	return &MustFail{
		Marshal:   l.Marshal.GetModified(),
		Unmarshal: l.Unmarshal.GetModified(),
		prepared:  true,
	}
}

func (l *MustFail) Copy() *MustFail {
	return &MustFail{
		Marshal:   l.Marshal.Copy(),
		Unmarshal: l.Unmarshal.Copy(),
	}
}

func (l *MustFail) Run(t *testing.T) {
	if !l.prepared {
		t.Fatal("run GetModified fist")
	}
	if l.Marshal == nil && l.Unmarshal == nil {
		t.Fatal("nothing to run")
	}
	if l.Marshal != nil {
		l.Marshal.Run(t)
	}
	if l.Unmarshal != nil {
		l.Unmarshal.Run(t)
	}
}
