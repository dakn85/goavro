// Copyright [2019] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

import (
	"fmt"
	"math/big"
	"testing"
	"time"
)

func TestFloatToBigScaled(t *testing.T) {
	tests := []struct {
		decimal   float64
		scale     int
		expected  string
		expectErr bool
	}{
		{50.113, 4, "501130", false},
		{50.1113, 4, "501113", false},
		{50.13, 2, "5013", false},
		// Add more test cases as needed
	}

	for _, test := range tests {
		t.Run("test", func(t *testing.T) {
			result, err := floatToBigScaled(test.decimal, test.scale)
	
			if test.expectErr && err == nil {
					t.Errorf("Expected an error but got nil")
			}
	
			if !test.expectErr && err != nil {
					t.Errorf("Unexpected error: %v", err)
			}
	
			if !test.expectErr {
					expected, success := new(big.Int).SetString(test.expected, 10)
					if !success {
							t.Errorf("Invalid expected result: %s", test.expected)
					}
	
					if result.Cmp(expected) != 0 {
							t.Errorf("Expected %s, but got %s", expected, result)
					}
	
					fmt.Println("Actual Result:", result)
			}
	})
	}
}

const (
	precision = "precision"
	scale     = "scale"
)

func TestSchemaLogicalType(t *testing.T) {
	testSchemaValid(t, `{"type": "long", "logicalType": "timestamp-millis"}`)
	testSchemaInvalid(t, `{"type": "bytes", "logicalType": "decimal"}`, "precision")
	testSchemaInvalid(t, `{"type": "fixed", "size": 16, "logicalType": "decimal"}`, "precision")
}

func TestStringLogicalTypeFallback(t *testing.T) {
	schema := `{"type": "string", "logicalType": "this_logical_type_does_not_exist"}`
	testSchemaValid(t, schema)
	testBinaryCodecPass(t, schema, "test string", []byte("\x16\x74\x65\x73\x74\x20\x73\x74\x72\x69\x6e\x67"))
}

func TestLongLogicalTypeFallback(t *testing.T) {
	schema := `{"type": "long", "logicalType": "this_logical_type_does_not_exist"}`
	testSchemaValid(t, schema)
	testBinaryCodecPass(t, schema, 12345, []byte("\xf2\xc0\x01"))
}

func TestTimeStampMillisLogicalTypeEncode(t *testing.T) {
	schema := `{"type": "long", "logicalType": "timestamp-millis"}`
	testBinaryDecodeFail(t, schema, []byte(""), "short buffer")
	testBinaryEncodeFail(t, schema, "test", "cannot transform to binary timestamp-millis, expected time.Time or Go numeric")
	testBinaryCodecPass(t, schema, time.Date(2006, 1, 2, 15, 04, 05, 565000000, time.UTC), []byte("\xfa\x82\xac\xba\x91\x42"))
}

func TestTimeStampMillisLogicalTypeUnionEncode(t *testing.T) {
	schema := `{"type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]}`
	testBinaryEncodeFail(t, schema, Union("string", "test"), "cannot encode binary union: no member schema types support datum: allowed types: [null long.timestamp-millis]")
	testBinaryCodecPass(t, schema, Union("long.timestamp-millis", time.Date(2006, 1, 2, 15, 04, 05, 565000000, time.UTC)), []byte("\x02\xfa\x82\xac\xba\x91\x42"))
}

func TestTimeStampMicrosLogicalTypeEncode(t *testing.T) {
	schema := `{"type": "long", "logicalType": "timestamp-micros"}`
	testBinaryDecodeFail(t, schema, []byte(""), "short buffer")
	testBinaryEncodeFail(t, schema, "test", "cannot transform to binary timestamp-micros, expected time.Time or Go numeric")
	testBinaryCodecPass(t, schema, time.Date(2006, 1, 2, 15, 04, 05, 565283000, time.UTC), []byte("\xc6\x8d\xf7\xe7\xaf\xd8\x84\x04"))
}

func TestTimeStampMicrosLogicalTypeUnionEncode(t *testing.T) {
	schema := `{"type": ["null", {"type": "long", "logicalType": "timestamp-micros"}]}`
	testBinaryEncodeFail(t, schema, Union("string", "test"), "cannot encode binary union: no member schema types support datum: allowed types: [null long.timestamp-micros]")
	testBinaryCodecPass(t, schema, Union("long.timestamp-micros", time.Date(2006, 1, 2, 15, 04, 05, 565283000, time.UTC)), []byte("\x02\xc6\x8d\xf7\xe7\xaf\xd8\x84\x04"))
}

func TestTimeMillisLogicalTypeEncode(t *testing.T) {
	schema := `{"type": "int", "logicalType": "time-millis"}`
	testBinaryDecodeFail(t, schema, []byte(""), "short buffer")
	testBinaryEncodeFail(t, schema, "test", "cannot transform to binary time-millis, expected time.Duration")
	testBinaryCodecPass(t, schema, 66904022*time.Millisecond, []byte("\xac\xff\xe6\x3f"))
}

func TestTimeMillisLogicalTypeUnionEncode(t *testing.T) {
	schema := `{"type": ["null", {"type": "int", "logicalType": "time-millis"}]}`
	testBinaryEncodeFail(t, schema, Union("string", "test"), "cannot encode binary union: no member schema types support datum: allowed types: [null int.time-millis]")
	testBinaryCodecPass(t, schema, Union("int.time-millis", 66904022*time.Millisecond), []byte("\x02\xac\xff\xe6\x3f"))
}

func TestTimeMicrosLogicalTypeEncode(t *testing.T) {
	schema := `{"type": "long", "logicalType": "time-micros"}`
	testBinaryDecodeFail(t, schema, []byte(""), "short buffer")
	testBinaryEncodeFail(t, schema, "test", "cannot transform to binary time-micros, expected time.Duration")
	testBinaryCodecPass(t, schema, 66904022566*time.Microsecond, []byte("\xcc\xf8\xd2\xbc\xf2\x03"))
}

func TestTimeMicrosLogicalTypeUnionEncode(t *testing.T) {
	schema := `{"type": ["null", {"type": "long", "logicalType": "time-micros"}]}`
	testBinaryEncodeFail(t, schema, Union("string", "test"), "cannot encode binary union: no member schema types support datum: allowed types: [null long.time-micros]")
	testBinaryCodecPass(t, schema, Union("long.time-micros", 66904022566*time.Microsecond), []byte("\x02\xcc\xf8\xd2\xbc\xf2\x03"))
}
func TestDateLogicalTypeEncode(t *testing.T) {
	schema := `{"type": "int", "logicalType": "date"}`
	testBinaryDecodeFail(t, schema, []byte(""), "short buffer")
	testBinaryEncodeFail(t, schema, "test", "cannot transform to binary date, expected time.Time or Go numeric, received string")
	testBinaryCodecPass(t, schema, time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC), []byte("\xbc\xcd\x01"))
}

func testGoZeroTime(t *testing.T, schema string, expected []byte) {
	t.Helper()
	testBinaryEncodePass(t, schema, time.Time{}, expected)

	codec, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}

	value, remaining, err := codec.NativeFromBinary(expected)
	if err != nil {
		t.Fatalf("schema: %s; %s", schema, err)
	}

	// remaining ought to be empty because there is nothing remaining to be
	// decoded
	if actual, expected := len(remaining), 0; actual != expected {
		t.Errorf("schema: %s; Remaining; Actual: %#v; Expected: %#v", schema, actual, expected)
	}

	zeroTime, ok := value.(time.Time)
	if !ok {
		t.Fatalf("schema: %s, NativeFromBinary: expected time.Time, got %T", schema, value)
	}

	if !zeroTime.IsZero() {
		t.Fatalf("schema: %s, Check: time.Time{}.IsZero(), Actual: %t, Expected: true", schema, zeroTime.IsZero())
	}
}

func TestDateGoZero(t *testing.T) {
	testGoZeroTime(t, `{"type": "int", "logicalType": "date"}`, []byte{0xf3, 0xe4, 0x57})
}

func TestTimeStampMillisGoZero(t *testing.T) {
	testGoZeroTime(t, `{"type": "long", "logicalType": "timestamp-millis"}`, []byte{0xff, 0xdf, 0xe6, 0xa2, 0xe2, 0xa0, 0x1c})
}

func TestTimeStampMicrosGoZero(t *testing.T) {
	testGoZeroTime(t, `{"type": "long", "logicalType": "timestamp-micros"}`, []byte{0xff, 0xff, 0xdd, 0xf2, 0xdf, 0xff, 0xdf, 0xdc, 0x1})
}

func TestValidatedStringLogicalTypeInRecordEncode(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "myrecord",
		"fields": [
			{
				"name": "number",
				"doc": "Phone number inside the national network. Length between 4-14",
				"type": {
					  "type": "string",
					  "logicalType": "validatedString",
					  "pattern": "^[\\d]{4,14}$"
				}
			}
		]
	  }`

	codec, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}

	// NOTE: May omit fields when using default value
	textual := []byte(`{"number": "667777777"}`)

	// Convert textual Avro data (in Avro JSON format) to native Go form
	native, _, err := codec.NativeFromTextual(textual)
	if err != nil {
		t.Fatal(err)
	}

	// Convert native Go form to binary Avro data
	binary, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		t.Fatal(err)
	}

	testSchemaValid(t, schema)
	testBinaryCodecPass(t, schema, map[string]interface{}{"number": "667777777"}, binary)

	// Convert binary Avro data back to native Go form
	native, _, err = codec.NativeFromBinary(binary)
	if err != nil {
		t.Fatal(err)
	}

	// Convert native Go form to textual Avro data
	textual, err = codec.TextualFromNative(nil, native)
	if err != nil {
		t.Fatal(err)
	}

	// NOTE: Textual encoding will show all fields, even those with values that
	// match their default values
	if got, want := string(textual), "{\"number\":\"667777777\"}"; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func ExampleUnion_logicalType() {
	// Supported logical types and their native go types:
	// * timestamp-millis - time.Time
	// * timestamp-micros - time.Time
	// * time-millis      - time.Duration
	// * time-micros      - time.Duration
	// * date             - int
	// * decimal          - big.Rat
	codec, err := NewCodec(`["null", {"type": "long", "logicalType": "timestamp-millis"}]`)
	if err != nil {
		fmt.Println(err)
	}

	// Note the usage of type.logicalType i.e. `long.timestamp-millis` to denote the type in a union. This is due to the single string naming format
	// used by goavro. Decimal can be both bytes.decimal or fixed.decimal
	bytes, err := codec.BinaryFromNative(nil, map[string]interface{}{"long.timestamp-millis": time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC)})
	if err != nil {
		fmt.Println(err)
	}

	decoded, _, err := codec.NativeFromBinary(bytes)
	if err != nil {
		fmt.Println(err)
	}
	out := decoded.(map[string]interface{})
	fmt.Printf("%#v\n", out["long.timestamp-millis"].(time.Time).String())
	// Output: "2006-01-02 15:04:05 +0000 UTC"
}

func TestPrecisionAndScaleFromSchemaMapValidation(t *testing.T) {
	testCasesInvalid := []struct {
		schemaMap map[string]interface{}
		errMsg    string
	}{
		{map[string]interface{}{}, "cannot create decimal logical type without precision"},
		{map[string]interface{}{
			precision: true,
		}, "wrong precision type"},
		{map[string]interface{}{
			precision: float64(0),
		}, "precision is less than one"},
		{map[string]interface{}{
			precision: float64(2),
			scale:     true,
		}, "wrong scale type"},
		{map[string]interface{}{
			precision: float64(2),
			scale:     float64(-1),
		}, "scale is less than zero"},
		{map[string]interface{}{
			precision: float64(2),
			scale:     float64(3),
		}, "scale is larger than precision"},
	}
	for _, tc := range testCasesInvalid {
		_, _, err := precisionAndScaleFromSchemaMap(tc.schemaMap)
		ensureError(t, err, tc.errMsg)
	}

	// validation passes
	p, s, err := precisionAndScaleFromSchemaMap(map[string]interface{}{
		precision: float64(1),
		scale:     float64(1),
	})
	if p != 1 || s != 1 || err != nil {
		t.Errorf("GOT: %v %v %v; WANT: 1 1 nil", p, s, err)
	}
}
