// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"maps"
)

func (InternalV2MetadataText) CheckType(t DataType) error {
	if t.Kind() == TypeAscii {
		return ASCIICodec.CheckType(t)
	}
	return TextCodec.CheckType(t)
}
func (InternalV2MetadataText) Decode(r ValueReader) (string, error) {
	var text Text
	var err error
	if r.Type().Kind() == TypeAscii {
		text, err = ASCIICodec.Decode(r)
	} else {
		text, err = TextCodec.Decode(r)
	}
	if err != nil {
		return "", err
	}
	return text.String(), nil
}
func (InternalV2MetadataText) Clone(v string) string { return v }

func (InternalV2MetadataUUIDString) CheckType(t DataType) error { return UUIDCodec.CheckType(t) }
func (InternalV2MetadataUUIDString) Decode(r ValueReader) (string, error) {
	v, e := UUIDCodec.Decode(r)
	if e != nil {
		return "", e
	}
	return v.String(), nil
}
func (InternalV2MetadataUUIDString) Clone(v string) string { return v }

func (InternalV2MetadataInt) CheckType(t DataType) error { return Int32.CheckType(t) }
func (InternalV2MetadataInt) Decode(r ValueReader) (int, error) {
	v, e := Int32.Decode(r)
	return int(v), e
}
func (InternalV2MetadataInt) Clone(v int) int { return v }

func (InternalV2MetadataKind) CheckType(t DataType) error {
	return (InternalV2MetadataText{}).CheckType(t)
}
func (InternalV2MetadataKind) Decode(r ValueReader) (ColumnKind, error) {
	v, e := (InternalV2MetadataText{}).Decode(r)
	if e != nil {
		return 0, e
	}
	return InternalColumnKindFromSchema(v)
}
func (InternalV2MetadataKind) Clone(v ColumnKind) ColumnKind { return v }

func (InternalV2MetadataStrings) CheckType(t DataType) error {
	if t.Kind() == TypeSet {
		return SetDecoder[Text](TextCodec).CheckType(t)
	}
	return ListDecoder[Text](TextCodec).CheckType(t)
}
func (InternalV2MetadataStrings) Decode(r ValueReader) ([]string, error) {
	decoder := ListDecoder[Text](TextCodec)
	if r.Type().Kind() == TypeSet {
		decoder = SetDecoder[Text](TextCodec)
	}
	v, err := decoder.Decode(r)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, v.Len())
	it := v.Iterator()
	for it.Next() {
		out = append(out, it.Value().String())
	}
	return out, it.Err()
}
func (InternalV2MetadataStrings) Clone(v []string) []string { return append([]string(nil), v...) }

func (InternalV2MetadataStringMap) CheckType(t DataType) error {
	return MapDecoder[Text, Text](TextCodec, TextCodec).CheckType(t)
}
func (InternalV2MetadataStringMap) Decode(r ValueReader) (map[string]string, error) {
	v, err := MapDecoder[Text, Text](TextCodec, TextCodec).Decode(r)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, v.Len())
	it := v.Iterator()
	for it.Next() {
		entry := it.Value()
		key := entry.Key.String()
		if _, ok := out[key]; ok {
			return nil, typeValidation("schema map", "duplicate key %q", key)
		}
		out[key] = entry.Value.String()
	}
	return out, it.Err()
}
func (InternalV2MetadataStringMap) Clone(v map[string]string) map[string]string { return maps.Clone(v) }

func (InternalV2MetadataExtensions) CheckType(t DataType) error {
	return MapDecoder[Text, Bytes](TextCodec, BlobCodec).CheckType(t)
}
func (InternalV2MetadataExtensions) Decode(r ValueReader) (map[string]any, error) {
	v, err := MapDecoder[Text, Bytes](TextCodec, BlobCodec).Decode(r)
	if err != nil {
		return nil, err
	}
	out := make(map[string]any, v.Len())
	it := v.Iterator()
	for it.Next() {
		entry := it.Value()
		key := entry.Key.String()
		if _, ok := out[key]; ok {
			return nil, typeValidation("schema extensions", "duplicate key %q", key)
		}
		out[key] = entry.Value.OwnedBytes()
	}
	return out, it.Err()
}
func (InternalV2MetadataExtensions) Clone(v map[string]any) map[string]any {
	return InternalCloneV2AnyMap(v)
}
