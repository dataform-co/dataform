package main

import (
	"log"
	// "math"

	"github.com/dataform-co/dataform/protobufts/tests"
	"github.com/golang/protobuf/proto"
)

func main() {
	bytes, err := proto.Marshal(&testprotos.TestMessage{
		// BytesField: []byte{0x5, 0xFF},
		// BytesField: []byte{},
		Uint32Field: 4294967295,
	})
	if err != nil {
		log.Fatalf("Failed to marshal protobuf: %v", err)
	}
	log.Printf("Encoded protobuf %v", bytes)
}
