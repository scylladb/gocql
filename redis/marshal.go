package redis

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"
)

// maxTTLSeconds is the largest TTL CQL accepts (20 years).
const maxTTLSeconds = 630720000

// maxNameBytes is the largest value CQL accepts for a component of a primary
// key, which is what keys, hash fields, set members and list positions become.
const maxNameBytes = 1<<16 - 1

type kvPair struct {
	key   string
	value interface{}
}

type hashPair struct {
	field string
	value interface{}
}

func normalizeMSetValues(values ...interface{}) ([]kvPair, error) {
	flat := flattenMSetValues(values)
	if len(flat) == 0 {
		return nil, errors.New("rediscompat: MSet requires at least one key/value pair")
	}
	if len(flat)%2 != 0 {
		return nil, errors.New("rediscompat: MSet expects even number of arguments")
	}

	pairs := make([]kvPair, 0, len(flat)/2)
	for i := 0; i < len(flat); i += 2 {
		key, ok := flat[i].(string)
		if !ok || key == "" {
			return nil, errors.New("rediscompat: MSet key must be non-empty string")
		}
		pairs = append(pairs, kvPair{
			key:   key,
			value: flat[i+1],
		})
	}
	return pairs, nil
}

func normalizeHashSetValues(values ...interface{}) ([]hashPair, error) {
	flat := flattenMSetValues(values)
	if len(flat) == 0 {
		return nil, errors.New("rediscompat: HSet requires at least one field/value pair")
	}
	if len(flat)%2 != 0 {
		return nil, errors.New("rediscompat: HSet expects even number of arguments")
	}

	pairs := make([]hashPair, 0, len(flat)/2)
	for i := 0; i < len(flat); i += 2 {
		field, ok := flat[i].(string)
		if !ok || field == "" {
			return nil, errors.New("rediscompat: HSet field must be non-empty string")
		}
		pairs = append(pairs, hashPair{
			field: field,
			value: flat[i+1],
		})
	}
	return pairs, nil
}

func flattenMSetValues(values []interface{}) []interface{} {
	if len(values) != 1 {
		return values
	}

	switch typed := values[0].(type) {
	case []interface{}:
		return typed
	case []string:
		out := make([]interface{}, len(typed))
		for i := range typed {
			out[i] = typed[i]
		}
		return out
	case map[string]interface{}:
		out := make([]interface{}, 0, len(typed)*2)
		for key, value := range typed {
			out = append(out, key, value)
		}
		return out
	case map[string]string:
		out := make([]interface{}, 0, len(typed)*2)
		for key, value := range typed {
			out = append(out, key, value)
		}
		return out
	default:
		v := reflect.ValueOf(values[0])
		if !v.IsValid() {
			return values
		}
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return nil
			}
			v = v.Elem()
		}
		if v.Kind() != reflect.Struct {
			return values
		}
		t := v.Type()
		out := make([]interface{}, 0, t.NumField()*2)
		for i := 0; i < t.NumField(); i++ {
			tag := t.Field(i).Tag.Get("redis")
			if tag == "" || tag == "-" {
				continue
			}
			name := strings.TrimSpace(strings.Split(tag, ",")[0])
			if name == "" {
				continue
			}
			field := v.Field(i)
			if !field.CanInterface() {
				continue
			}
			out = append(out, name, field.Interface())
		}
		return out
	}
}

func marshalValue(value any) ([]byte, error) {
	switch v := value.(type) {
	case nil:
		return nil, errors.New("rediscompat: nil values are not supported by Set")
	case []byte:
		cp := make([]byte, len(v))
		copy(cp, v)
		return cp, nil
	case string:
		return []byte(v), nil
	case fmt.Stringer:
		return []byte(v.String()), nil
	default:
		return []byte(fmt.Sprint(v)), nil
	}
}

// validateKey checks a key name. The key is a text column, so the server
// rejects invalid UTF-8 and oversized components; catching it here says which
// argument was wrong instead of failing as an opaque server error.
func validateKey(key string) error {
	if key == "" {
		return Error("rediscompat: key must not be empty")
	}
	if len(key) > maxNameBytes {
		return fmt.Errorf("rediscompat: key is %d bytes, limit is %d", len(key), maxNameBytes)
	}
	if !utf8.ValidString(key) {
		return Error("rediscompat: key must be valid UTF-8; keys are stored as text")
	}
	return nil
}

// validateElement checks a hash field or set member. Both are blob clustering
// columns, so unlike the key they are binary safe and only the length matters.
func validateElement(kind, name string) error {
	if name == "" {
		return Error("rediscompat: " + kind + " must not be empty")
	}
	if len(name) > maxNameBytes {
		return fmt.Errorf("rediscompat: %s is %d bytes, limit is %d", kind, len(name), maxNameBytes)
	}
	return nil
}

func valueTooLarge(size, limit int) error {
	return fmt.Errorf("rediscompat: value is %d bytes, limit is %d: %w", size, limit, ErrValueTooLarge)
}

// marshalBounded serializes a value and refuses it when it is too large for a
// single CQL cell. Rejecting it here reports which command and value was at
// fault, instead of surfacing a frame size failure from deep in the driver or
// writing a cell large enough to destabilize a replica.
func (c *Client) marshalBounded(value any) ([]byte, error) {
	payload, err := marshalValue(value)
	if err != nil {
		return nil, err
	}
	if c.core.maxValueSize > 0 && len(payload) > c.core.maxValueSize {
		return nil, valueTooLarge(len(payload), c.core.maxValueSize)
	}
	return payload, nil
}
