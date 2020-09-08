package main

import (
	"flag"
)

var (
	protoType          = flag.String("proto_type", "", "Name of the type of protobuf to be reserialized.")
	base64EncodedProto = flag.String("base64_proto_value", "", "Base 64 encoded value of the protobuf to be reserialized.")
)

func main() {

}
