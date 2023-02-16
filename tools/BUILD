# load("//:node_modules/ts-proto/package_json.bzl", _ts_proto_bin_factories = "bin")
load("@npm//:ts-proto/package_json.bzl", _ts_proto_bin_factories = "bin")
load("@rules_proto_grpc//:defs.bzl", "proto_plugin")

package(default_visibility = ["//visibility:public"])

_ts_proto_bin_factories.protoc_gen_ts_proto_binary(
    name = "protoc-gen-ts-proto",
)

proto_plugin(
    name = "ts_proto_compile",
    outputs = ["{protopath}.ts"],
    protoc_plugin_name = "ts_proto",
    tool = ":protoc-gen-ts-proto",
    use_built_in_shell_environment = False,
)

# load("@rules_proto_grpc//:defs.bzl", "proto_plugin")
# load("@npm//:@protobuf-ts/plugin/package_json.bzl", protobuf_ts_plugin = "bin")

# package(default_visibility = ["//visibility:public"])

# exports_files(["ts_proto_library.bzl"])

# protobuf_ts_plugin.protoc_gen_ts_binary(
#     name = "protoc_gen_ts",
# )

# proto_plugin(
#     name = "ts_proto_compile",
#     outputs = [
#         "{protopath}.ts",
#     ],
#     tool = ":protoc_gen_ts",
#     use_built_in_shell_environment = False,
# )

# proto_plugin(
#     name = "ts_grpc_proto_compile",
#     outputs = [
#         "{protopath}.client.ts",
#     ],
#     tool = ":protoc_gen_ts",
#     use_built_in_shell_environment = False,
# )
