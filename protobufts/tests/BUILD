proto_library(
    name = "testprotos_proto",
    srcs = ["test1.proto"],
    visibility = ["//visibility:public"],
)

load("//tools:ts_proto_library.bzl", protobufjs_proto_library = "ts_proto_library")

protobufjs_proto_library(
    name = "protobufjs_testprotos_ts_proto",
    deps = [
        ":testprotos_proto",
    ],
)

load("//protobufts:ts_proto_library.bzl", "ts_proto_library")

ts_proto_library(
    name = "testprotos_ts_proto",
    protos = [
        ":testprotos_proto",
    ],
    deps = [
        "@npm//@types/long",
    ],
)

load("//testing:index.bzl", "ts_test_suite")

ts_test_suite(
    name = "tests",
    srcs = glob(["*.spec.ts"]),
    deps = [
        ":protobufjs_testprotos_ts_proto",
        ":testprotos_ts_proto",
        "//testing",
        "@npm//@types/chai",
        "@npm//chai",
    ],
)
