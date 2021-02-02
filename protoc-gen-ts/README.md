# protoc-gen-ts

`protoc-gen-ts` is a protobuf compiler which generates TypeScript class definitions. It was written with the following goals in mind:

- it is fully natively written in TypeScript
- it uses the standard 'plugin' mechanism to hook into the general-purpose `protoc` protobuf compiler, meaning that it does not have to re-implement protobuf file parsing and/or compilation checks
- it ships with support for a standard `ts_proto_library` Bazel BUILD rule to generate TypeScript definitions for any proto3 protobuf file(s); these can depend on the output of other `ts_proto_library` rules in other source trees
- it generates clean, correct type definitions, with all fields containing the correct proto3 default values

## TODO

- Cleanup:
  - Transpiler code determines whether a field is a Map in several places; this should be consolidated
  - `DECODERS_SINGLETON` members should have correct typing (instead of `any`)
- Features:
  - `toJson()` is supported, but `fromJson()` is not
  - `toJson()` support for "special" protobuf types e.g. `Any`
- Correctness/validation additions:
  - the transpiler should reject any non-proto3 protobufs (until support for proto2 transpilation is added)
  - the transpiler should reject any field that uses `proto3_optional`, aka "synthetic" optional fields (until support for `proto3_optional` is added)
- More tests need to be added:
  - deserialization of unknown fields / unknown enum values
  - messages with more than one field set
  - compatibility tests - `optional` vs `repeated ... [packed=false]`; `repeated ... [packed=false]` vs `repeated ... [packed=true]`; other types that are safe to change to another type
  - `JSON` compatibility with a reference spec (Go?)
