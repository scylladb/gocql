package mod

var CustomTypeRef Mod = func(vals ...any) []any {
	return Reference(CustomType(vals...)...)
}
