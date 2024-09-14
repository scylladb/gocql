package mod

type Mods []Mod

// Mod - value modifiers.
// Designed for test case generators, such as gen.Group, marshal.Group and unmarshal.Group.
type Mod interface {
	Name() string
	Apply([]interface{}) []interface{}
}

var (
	AsIs          asIs
	IntoCustom    intoCustom
	IntoRef       intoRef
	IntoCustomRef intoCustomRef

	Non     = Mods{AsIs}
	Custom  = Mods{AsIs, IntoCustom}
	Ref     = Mods{AsIs, IntoRef}
	Default = Mods{AsIs, IntoCustom, IntoRef, IntoCustomRef}
)
