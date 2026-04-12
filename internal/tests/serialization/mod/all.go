package mod

var All = []Mod{CustomType, Reference, CustomTypeRef}

// Mod - value modifiers.
type Mod func(vals ...any) []any

type Values []any

func (v Values) AddVariants(mods ...Mod) Values {
	out := append(make([]any, 0), v...)
	for _, mod := range mods {
		out = append(out, mod(v...)...)
	}
	return out
}
