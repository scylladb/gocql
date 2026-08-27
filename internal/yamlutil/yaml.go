//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

// Package yamlutil marshals YAML using JSON struct tags and JSON-compatible
// scalar representations.
package yamlutil

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// Marshal converts a value to YAML while preserving encoding/json semantics,
// including JSON field names and base64 encoding for byte slices.
func Marshal(value any) ([]byte, error) {
	jsonData, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal JSON: %w", err)
	}

	var document yaml.Node
	if err := yaml.Unmarshal(jsonData, &document); err != nil {
		return nil, fmt.Errorf("convert JSON to YAML: %w", err)
	}
	useDefaultStyle(&document)

	yamlData, err := yaml.Marshal(&document)
	if err != nil {
		return nil, fmt.Errorf("marshal YAML: %w", err)
	}

	return yamlData, nil
}

func useDefaultStyle(node *yaml.Node) {
	node.Style = 0
	for _, child := range node.Content {
		useDefaultStyle(child)
	}
}

// Unmarshal converts YAML to JSON before decoding it into value. This keeps
// decoding aligned with encoding/json struct tags and byte-slice handling.
func Unmarshal(data []byte, value any) error {
	var yamlValue any
	if err := yaml.Unmarshal(data, &yamlValue); err != nil {
		return fmt.Errorf("unmarshal YAML: %w", err)
	}
	yamlValue = convertStringTargets(yamlValue, reflect.TypeOf(value))

	jsonData, err := json.Marshal(yamlValue)
	if err != nil {
		return fmt.Errorf("convert YAML to JSON: %w", err)
	}

	if err := json.Unmarshal(jsonData, value); err != nil {
		return fmt.Errorf("unmarshal JSON: %w", err)
	}

	return nil
}

var (
	jsonUnmarshalerType = reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()
	textUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
)

func convertStringTargets(value any, target reflect.Type) any {
	target = indirectTarget(target)

	switch typedValue := value.(type) {
	case map[string]any:
		converted := make(map[string]any, len(typedValue))
		for key, item := range typedValue {
			converted[key] = convertStringTargets(item, jsonValueTarget(target, key))
		}
		return converted
	case map[any]any:
		converted := make(map[string]any, len(typedValue))
		for key, item := range typedValue {
			stringKey, ok := yamlMapKeyString(key)
			if !ok {
				return value
			}
			converted[stringKey] = convertStringTargets(item, jsonValueTarget(target, stringKey))
		}
		return converted
	case []any:
		var itemTarget reflect.Type
		if target != nil && (target.Kind() == reflect.Slice || target.Kind() == reflect.Array) {
			itemTarget = target.Elem()
		}
		converted := make([]any, len(typedValue))
		for i, item := range typedValue {
			converted[i] = convertStringTargets(item, itemTarget)
		}
		return converted
	default:
		if target == nil || target.Kind() != reflect.String {
			return value
		}

		switch scalar := typedValue.(type) {
		case int:
			return strconv.FormatInt(int64(scalar), 10)
		case int64:
			return strconv.FormatInt(scalar, 10)
		case uint64:
			return strconv.FormatUint(scalar, 10)
		case float64:
			return strconv.FormatFloat(scalar, 'g', -1, 32)
		case bool:
			return strconv.FormatBool(scalar)
		default:
			return value
		}
	}
}

func jsonValueTarget(target reflect.Type, key string) reflect.Type {
	if target == nil {
		return nil
	}
	switch target.Kind() {
	case reflect.Struct:
		return jsonFieldType(target, key)
	case reflect.Map:
		return target.Elem()
	default:
		return nil
	}
}

func yamlMapKeyString(key any) (string, bool) {
	switch typedKey := key.(type) {
	case string:
		return typedKey, true
	case int:
		return strconv.FormatInt(int64(typedKey), 10), true
	case int64:
		return strconv.FormatInt(typedKey, 10), true
	case uint64:
		return strconv.FormatUint(typedKey, 10), true
	case float64:
		formatted := strconv.FormatFloat(typedKey, 'g', -1, 32)
		switch formatted {
		case "+Inf":
			formatted = ".inf"
		case "-Inf":
			formatted = "-.inf"
		case "NaN":
			formatted = ".nan"
		}
		return formatted, true
	case bool:
		return strconv.FormatBool(typedKey), true
	default:
		return "", false
	}
}

func indirectTarget(target reflect.Type) reflect.Type {
	for target != nil && target.Kind() == reflect.Pointer {
		if target.Implements(jsonUnmarshalerType) || target.Implements(textUnmarshalerType) {
			return nil
		}
		target = target.Elem()
	}
	if target != nil && (target.Implements(jsonUnmarshalerType) || target.Implements(textUnmarshalerType) ||
		reflect.PointerTo(target).Implements(jsonUnmarshalerType) || reflect.PointerTo(target).Implements(textUnmarshalerType)) {
		return nil
	}
	return target
}

func jsonFieldType(target reflect.Type, key string) reflect.Type {
	fields := dominantJSONFields(target)
	for _, field := range fields {
		if field.name == key {
			return field.typ
		}
	}
	for _, field := range fields {
		if strings.EqualFold(field.name, key) {
			return field.typ
		}
	}
	return nil
}

type jsonField struct {
	typ    reflect.Type
	name   string
	depth  int
	tagged bool
}

func dominantJSONFields(target reflect.Type) []jsonField {
	var candidates []jsonField
	collectJSONFields(target, 0, make(map[reflect.Type]bool), &candidates)

	grouped := make(map[string][]jsonField, len(candidates))
	order := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if _, ok := grouped[candidate.name]; !ok {
			order = append(order, candidate.name)
		}
		grouped[candidate.name] = append(grouped[candidate.name], candidate)
	}

	fields := make([]jsonField, 0, len(grouped))
	for _, name := range order {
		group := grouped[name]
		minDepth := group[0].depth
		for _, field := range group[1:] {
			if field.depth < minDepth {
				minDepth = field.depth
			}
		}

		var dominant []jsonField
		for _, field := range group {
			if field.depth == minDepth {
				dominant = append(dominant, field)
			}
		}
		var tagged []jsonField
		for _, field := range dominant {
			if field.tagged {
				tagged = append(tagged, field)
			}
		}
		if len(tagged) > 0 {
			dominant = tagged
		}
		if len(dominant) == 1 {
			fields = append(fields, dominant[0])
		}
	}
	return fields
}

func collectJSONFields(target reflect.Type, depth int, visiting map[reflect.Type]bool, fields *[]jsonField) {
	if visiting[target] {
		return
	}
	visiting[target] = true
	defer delete(visiting, target)

	for i := 0; i < target.NumField(); i++ {
		field := target.Field(i)
		fieldType := field.Type
		if fieldType.Kind() == reflect.Pointer {
			fieldType = fieldType.Elem()
		}
		if !field.IsExported() && (!field.Anonymous || fieldType.Kind() != reflect.Struct) {
			continue
		}

		name, tagged := field.Name, false
		if tag, ok := field.Tag.Lookup("json"); ok {
			name = strings.Split(tag, ",")[0]
			if name == "-" {
				continue
			}
			tagged = name != ""
			if name == "" {
				name = field.Name
			}
		}

		if field.Anonymous && !tagged && fieldType.Kind() == reflect.Struct {
			collectJSONFields(fieldType, depth+1, visiting, fields)
			continue
		}
		*fields = append(*fields, jsonField{name: name, typ: field.Type, depth: depth, tagged: tagged})
	}
}
