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

package yamlutil

import (
	"bytes"
	"testing"
)

type testConfig struct {
	CurrentContext string `json:"currentContext"`
	Data           []byte `json:"data"`
}

type embeddedConfig struct {
	Promoted string `json:"promoted"`
}

type testEmbeddedConfig struct {
	embeddedConfig
}

type testMapConfig struct {
	Values map[string]string `json:"values"`
}

func TestMarshalUsesJSONSemantics(t *testing.T) {
	data, err := Marshal(testConfig{
		CurrentContext: "default",
		Data:           []byte("certificate data"),
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, expected := range [][]byte{
		[]byte("currentContext: default"),
		[]byte("data: Y2VydGlmaWNhdGUgZGF0YQ=="),
	} {
		if !bytes.Contains(data, expected) {
			t.Fatalf("expected output to contain %q, got:\n%s", expected, data)
		}
	}
}

func TestUnmarshalUsesJSONSemantics(t *testing.T) {
	data := []byte("currentContext: default\ndata: Y2VydGlmaWNhdGUgZGF0YQ==\n")

	var config testConfig
	if err := Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}

	if config.CurrentContext != "default" {
		t.Fatalf("expected current context %q, got %q", "default", config.CurrentContext)
	}
	if !bytes.Equal(config.Data, []byte("certificate data")) {
		t.Fatalf("expected decoded certificate data, got %q", config.Data)
	}
}

func TestUnmarshalConvertsScalarsForStringTargets(t *testing.T) {
	for _, test := range []struct {
		name  string
		input string
		want  string
	}{
		{name: "number", input: "currentContext: 123\n", want: "123"},
		{name: "boolean", input: "currentContext: true\n", want: "true"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var config testConfig
			if err := Unmarshal([]byte(test.input), &config); err != nil {
				t.Fatal(err)
			}
			if config.CurrentContext != test.want {
				t.Fatalf("expected current context %q, got %q", test.want, config.CurrentContext)
			}
		})
	}
}

func TestUnmarshalConvertsScalarForPromotedStringTarget(t *testing.T) {
	var config testEmbeddedConfig
	if err := Unmarshal([]byte("promoted: true\n"), &config); err != nil {
		t.Fatal(err)
	}
	if config.Promoted != "true" {
		t.Fatalf("expected promoted value %q, got %q", "true", config.Promoted)
	}
}

func TestUnmarshalConvertsScalarMappingKeys(t *testing.T) {
	for _, test := range []struct {
		name  string
		input string
		key   string
	}{
		{name: "number", input: "values: {1: value}\n", key: "1"},
		{name: "boolean", input: "values: {true: value}\n", key: "true"},
		{name: "float", input: "values: {1.5: value}\n", key: "1.5"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var config testMapConfig
			if err := Unmarshal([]byte(test.input), &config); err != nil {
				t.Fatal(err)
			}
			if config.Values[test.key] != "value" {
				t.Fatalf("expected key %q to map to %q, got %#v", test.key, "value", config.Values)
			}
		})
	}
}
