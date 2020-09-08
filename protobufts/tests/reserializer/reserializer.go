package main

import (
	"encoding/base64"
	"flag"
	"fmt"

	"github.com/dataform-co/dataform/protobufts/tests"
	"github.com/golang/protobuf/proto"
)

var (
	protoType          = flag.String("proto_type", "", "Name of the type of protobuf to be reserialized.")
	base64EncodedProto = flag.String("base64_proto_value", "", "Base 64 encoded value of the protobuf to be reserialized.")
)

func main() {
	flag.Parse()
	decodedProto, err := base64.StdEncoding.DecodeString(*base64EncodedProto)
	if err != nil {
		panic(err)
	}
	var unmarshalledProto proto.Message
	switch {
	case *protoType == "TestMessage":
		unmarshalledProto = &testprotos.TestMessage{}
	case *protoType == "TestRepeatedMessage":
		unmarshalledProto = &testprotos.TestRepeatedMessage{}
	default:
		panic(fmt.Sprintf("Unrecognized protobuf type: %v", *protoType))
	}
	if err = proto.Unmarshal(decodedProto, unmarshalledProto); err != nil {
		panic(err)
	}
	marshalledBytes, err := proto.Marshal(unmarshalledProto)
	if err != nil {
		panic(err)
	}
	if _, err = fmt.Print(base64.StdEncoding.EncodeToString(marshalledBytes)); err != nil {
		panic(err)
	}
}
