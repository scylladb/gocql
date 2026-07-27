package redis

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"
)

// maxTTLSeconds is the largest TTL CQL accepts (20 years).
const maxTTLSeconds = 630720000

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

// ttlSecondsFromDuration converts a Go duration to a CQL TTL, clamped to the
// range the server accepts so an absurd duration cannot overflow the wire type.
func ttlSecondsFromDuration(d time.Duration) int {
	seconds := math.Ceil(d.Seconds())
	if seconds < 1 {
		return 1
	}
	if seconds > maxTTLSeconds {
		return maxTTLSeconds
	}
	return int(seconds)
}
