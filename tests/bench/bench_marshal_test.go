package bench_test

import (
	"encoding/json"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/gocql/gocql"
)

func generateRandomBinaryData(size int) []byte {
	rnd := rand.New(rand.NewSource(100))
	randomBuffer := make([]byte, size)
	io.ReadAtLeast(rnd, randomBuffer, size)
	return randomBuffer
}

type RandomData struct {
	ID        string `json:"id"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Email     string `json:"email"`
	City      string `json:"city"`
	State     string `json:"state"`
	Zip       string `json:"zip"`
	Phone     string `json:"phone"`
}

func generateRandomJSON(size int) string {
	gofakeit.Seed(100)
	var jsonData []byte
	var randomData []RandomData
	currentLength := 0

	for currentLength < size {
		data := RandomData{
			ID:        gofakeit.UUID(),
			FirstName: gofakeit.FirstName(),
			LastName:  gofakeit.LastName(),
			Email:     gofakeit.Email(),
			City:      gofakeit.City(),
			State:     gofakeit.State(),
			Zip:       gofakeit.Zip(),
			Phone:     gofakeit.Phone(),
		}
		randomData = append(randomData, data)

		tempData, _ := json.Marshal(randomData)
		currentLength = len(tempData)
		jsonData = tempData
	}

	return string(jsonData)
}

func BenchmarkSerialization(b *testing.B) {
	b.Run("SimpleTypes", func(b *testing.B) {
		b.Run("Int", func(b *testing.B) {
			tType := gocql.NewNativeType(4, gocql.TypeInt, "")
			var val int = 42
			b.Run("Marshal", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, err := gocql.Marshal(tType, val)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			marshaled, err := gocql.Marshal(tType, val)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("Unmarshal", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					var unmarshaled int
					err = gocql.Unmarshal(tType, marshaled, &unmarshaled)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})

		cases := []struct {
			name string
			size int
		}{
			{"Small-100b", 100},
			{"Medium-1kb", 1024},
			{"Big-1M", 1024 * 1024},
		}

		for _, c := range cases {
			b.Run("Blob"+c.name, func(b *testing.B) {
				tType := gocql.NewNativeType(4, gocql.TypeBlob, "")
				val := generateRandomBinaryData(c.size)
				b.Run("Marshal", func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						_, err := gocql.Marshal(tType, val)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
				marshaled, err := gocql.Marshal(tType, val)
				if err != nil {
					b.Fatal(err)
				}
				b.Run("Unmarshal", func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						var unmarshaled []byte
						err = gocql.Unmarshal(tType, marshaled, &unmarshaled)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			})
		}

		for _, c := range cases {
			b.Run("Text"+c.name, func(b *testing.B) {
				tType := gocql.NewNativeType(4, gocql.TypeText, "")
				val := generateRandomJSON(c.size)
				b.Run("Marshal", func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						_, err := gocql.Marshal(tType, val)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
				marshaled, err := gocql.Marshal(tType, val)
				if err != nil {
					b.Fatal(err)
				}
				b.Run("Unmarshal", func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						var unmarshaled string
						err = gocql.Unmarshal(tType, marshaled, &unmarshaled)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			})
		}

		b.Run("UUID", func(b *testing.B) {
			tType := gocql.NewNativeType(4, gocql.TypeUUID, "")
			val := gocql.UUID{}
			b.Run("Marshal", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, err := gocql.Marshal(tType, val)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			marshaled, err := gocql.Marshal(tType, val)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("Unmarshal", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					var unmarshaled gocql.UUID
					err = gocql.Unmarshal(tType, marshaled, &unmarshaled)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})

		b.Run("Duration", func(b *testing.B) {
			tType := gocql.NewNativeType(4, gocql.TypeDuration, "")
			val := gocql.Duration{Nanoseconds: 300000000000}
			b.Run("Marshal", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, err := gocql.Marshal(tType, val)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			marshaled, err := gocql.Marshal(tType, val)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("Unmarshal", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					var unmarshaled gocql.Duration
					err = gocql.Unmarshal(tType, marshaled, &unmarshaled)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})

		b.Run("Timestamp", func(b *testing.B) {
			tType := gocql.NewNativeType(4, gocql.TypeTimestamp, "")
			val := time.Now()
			b.Run("Marshal", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, err := gocql.Marshal(tType, val)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			marshaled, err := gocql.Marshal(tType, val)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("Unmarshal", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					var unmarshaled time.Time
					err = gocql.Unmarshal(tType, marshaled, &unmarshaled)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	})

	b.Run("ComplexTypes", func(b *testing.B) {
		b.Run("List", func(b *testing.B) {
			tType := gocql.CollectionType{
				NativeType: gocql.NewNativeType(4, gocql.TypeList, ""),
				Elem:       gocql.NewNativeType(4, gocql.TypeText, ""),
			}
			val := []string{"foo", "bar", "baz"}
			b.Run("Marshal", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, err := gocql.Marshal(tType, val)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			marshaled, err := gocql.Marshal(tType, val)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("Unmarshal", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					var unmarshaled []string
					err = gocql.Unmarshal(tType, marshaled, &unmarshaled)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})

		b.Run("Map", func(b *testing.B) {
			tType := gocql.CollectionType{
				NativeType: gocql.NewNativeType(4, gocql.TypeMap, ""),
				Key:        gocql.NewNativeType(4, gocql.TypeVarchar, ""),
				Elem:       gocql.NewNativeType(4, gocql.TypeInt, ""),
			}
			val := map[string]int{"a": 1, "b": 2}
			b.Run("Marshal", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, err := gocql.Marshal(tType, val)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			marshaled, err := gocql.Marshal(tType, val)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("Unmarshal", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					var unmarshaled map[string]int
					err = gocql.Unmarshal(tType, marshaled, &unmarshaled)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})

		b.Run("Set", func(b *testing.B) {
			tType := gocql.CollectionType{
				NativeType: gocql.NewNativeType(4, gocql.TypeSet, ""),
				Elem:       gocql.NewNativeType(4, gocql.TypeInt, ""),
			}
			val := map[int]struct{}{1: {}, 2: {}}
			b.Run("Marshal", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, err := gocql.Marshal(tType, val)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			marshaled, err := gocql.Marshal(tType, val)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("Unmarshal", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					var unmarshaled []int
					err = gocql.Unmarshal(tType, marshaled, &unmarshaled)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})

		b.Run("UDT", func(b *testing.B) {
			type MyUDT struct {
				ID    gocql.UUID
				Name  string
				Value int
			}

			val := MyUDT{
				ID:    gocql.UUID{},
				Name:  "test udt",
				Value: 123,
			}

			tType := gocql.UDTTypeInfo{
				NativeType: gocql.NewNativeType(4, gocql.TypeUDT, ""),
				Name:       "myudt",
				KeySpace:   "myks",
				Elements: []gocql.UDTField{
					{
						Name: "id",
						Type: gocql.NewNativeType(4, gocql.TypeUUID, ""),
					},
					{
						Name: "name",
						Type: gocql.NewNativeType(4, gocql.TypeText, ""),
					},
					{
						Name: "value",
						Type: gocql.NewNativeType(4, gocql.TypeInt, ""),
					},
				},
			}

			b.Run("Marshal", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, err := gocql.Marshal(tType, val)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			marshaled, err := gocql.Marshal(tType, val)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("Unmarshal", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					var unmarshaled MyUDT
					err = gocql.Unmarshal(tType, marshaled, &unmarshaled)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})

		b.Run("Tuple", func(b *testing.B) {
			val := struct {
				Field1 int
				Field2 string
			}{
				Field1: 1,
				Field2: "test tuple",
			}

			tType := gocql.TupleTypeInfo{
				NativeType: gocql.NewNativeType(4, gocql.TypeTuple, ""),
				Elems: []gocql.TypeInfo{
					gocql.NewNativeType(4, gocql.TypeInt, ""),
					gocql.NewNativeType(4, gocql.TypeText, ""),
				},
			}
			b.Run("Marshal", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, err := gocql.Marshal(tType, val)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			marshaled, err := gocql.Marshal(tType, val)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("Unmarshal", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					var unmarshaled struct {
						Field1 int
						Field2 string
					}
					err = gocql.Unmarshal(tType, marshaled, &unmarshaled)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	})

	b.Run("NestedTypes", func(b *testing.B) {
		b.Run("3-lvl", func(b *testing.B) {
			type MyUDT struct {
				ID    gocql.UUID
				Name  string
				Value int
			}

			val := []map[string]MyUDT{
				{
					"key1": {ID: gocql.UUID{}, Name: "name1", Value: 123},
					"key2": {ID: gocql.UUID{}, Name: "name2", Value: 456},
				},
				{
					"key3": {ID: gocql.UUID{}, Name: "name3", Value: 789},
				},
			}

			tType := gocql.CollectionType{
				NativeType: gocql.NewNativeType(4, gocql.TypeList, ""),
				Elem: gocql.CollectionType{
					NativeType: gocql.NewNativeType(4, gocql.TypeMap, ""),
					Key:        gocql.NewNativeType(4, gocql.TypeText, ""),
					Elem: gocql.UDTTypeInfo{
						NativeType: gocql.NewNativeType(4, gocql.TypeUDT, ""),
						Name:       "myudt",
						KeySpace:   "myks",
						Elements: []gocql.UDTField{
							{
								Name: "id",
								Type: gocql.NewNativeType(4, gocql.TypeUUID, ""),
							},
							{
								Name: "name",
								Type: gocql.NewNativeType(4, gocql.TypeText, ""),
							},
							{
								Name: "value",
								Type: gocql.NewNativeType(4, gocql.TypeInt, ""),
							},
						},
					},
				},
			}

			b.Run("Marshal", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, err := gocql.Marshal(tType, val)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			marshaled, err := gocql.Marshal(tType, val)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("Unmarshal", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					var unmarshaled []map[string]MyUDT
					err = gocql.Unmarshal(tType, marshaled, &unmarshaled)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	})
}
