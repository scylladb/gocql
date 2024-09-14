package mod

type intoCustomRef struct{}

func (m intoCustomRef) Name() string {
	return "ref_custom"
}

func (m intoCustomRef) Apply(vals []interface{}) []interface{} {
	custom := intoCustom{}.Apply(vals)
	return intoRef{}.Apply(custom)
}
