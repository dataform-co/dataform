load("@npm//:ts-proto/package_json.bzl", _ts_proto_bin_factories = "bin")
load("@rules_proto_grpc//:defs.bzl", "proto_plugin")

exports_files(["ts_proto_library.bzl"])

_ts_proto_bin_factories.protoc_gen_ts_proto_binary(
    name = "protoc-gen-ts-proto",
)

proto_plugin(
    name = "ts_proto_compile",
    outputs = ["{protopath}.ts"],
    protoc_plugin_name = "ts_proto",
    tool = ":protoc-gen-ts-proto",
    use_built_in_shell_environment = False,
    visibility = ["//visibility:public"],
)
