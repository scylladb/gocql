package run

func NewGroup(name string) Group {
	if name == "" {
		panic("name should be provided")
	}
	return &group{
		name:  name,
		elems: make([]Test, 0),
	}
}
