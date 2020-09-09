package main

import (
	// "encoding/json"
	// "encoding/hex"
	"log"
	// "math"
	// "fmt"

	"github.com/dataform-co/dataform/protobufts/tests"
	"github.com/golang/protobuf/proto"
)

func main() {
	bytes, err := proto.Marshal(&testprotos.TestMessage{
		// BytesField: []byte{0x5, 0xFF},
		// BytesField: []byte{},
		// Uint32Field: 4294967295,
		// Sfixed64Field: 9223372036854775807,
		// Sint64Field: 9223372036854775807,
		// DoubleField: 35.6,
		// Oneof: &testprotos.TestMessage_OneofInt32Field{0},
		Sint32Field: -132,
		// Int32Field: 263,
	})
	// f1, f2 := proto.DecodeVarint([]byte{1, 135, 2})
	// panic(fmt.Sprintf("hello %v %v", f1, f2))
	// bytes, err := proto.Marshal(&testprotos.TestRepeatedMessage{
	// 	// UnpackedDoubleField: []float64{35.6, 12.8, -8.9},
	// 	UnpackedFloatField: []float32{2.7, -9876.549},
	// })
	if err != nil {
		log.Fatalf("Failed to marshal protobuf: %v", err)
	}
	log.Printf("Encoded protobuf %v", bytes)
}
