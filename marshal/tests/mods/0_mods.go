package mods

type Mods []Mod

// Mod the modifier for are values.
type Mod interface {
	Suffix() string
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
