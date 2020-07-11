package main

import (
	// "encoding/json"
	"log"
	// "math"

	"github.com/dataform-co/dataform/protobufts/tests"
	"github.com/golang/protobuf/proto"
)

func main() {
	// bytes, err := proto.Marshal(&testprotos.TestMessage{
	// 	// BytesField: []byte{0x5, 0xFF},
	// 	// BytesField: []byte{},
	// 	// Uint32Field: 4294967295,
	// 	// Sfixed64Field: 9223372036854775807,
	// 	// Sint64Field: 9223372036854775807,
	// 	DoubleField: 35.6,
	// })
	bytes, err := proto.Marshal(&testprotos.TestRepeatedMessage{
		// UnpackedDoubleField: []float64{35.6, 12.8, -8.9},
		UnpackedFloatField: []float32{2.7, -9876.549},
	})
	if err != nil {
		log.Fatalf("Failed to marshal protobuf: %v", err)
	}
	log.Printf("Encoded protobuf %v", bytes)
}
