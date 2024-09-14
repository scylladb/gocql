package mod

type asIs struct{}

func (m asIs) Name() string {
	return "GO"
}

func (m asIs) Apply(vals []interface{}) []interface{} { return vals }
