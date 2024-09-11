package mods

type asIs struct{}

func (m asIs) Suffix() string {
	return ""
}

func (m asIs) Apply(vals []interface{}) []interface{} { return vals }
