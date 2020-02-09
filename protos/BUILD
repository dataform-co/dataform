load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "files",
    srcs = glob(["**/*.proto"]),
)

proto_library(
    name = "dataform_proto",
    srcs = [
        "core.proto",
        "profiles.proto",
    ],
)

proto_library(
    name = "schedules_proto",
    srcs = ["schedules.proto"],
)

proto_library(
    name = "server_proto",
    srcs = ["server.proto"],
)

java_proto_library(
    name = "dataform_java_proto",
    deps = [":dataform_proto"],
)

java_proto_library(
    name = "schedules_java_proto",
    deps = [":schedules_proto"],
)

go_proto_library(
    name = "dataform_go_proto",
    importpath = "github.com/dataform-co/dataform/protos/dataform",
    proto = ":dataform_proto",
)

go_proto_library(
    name = "schedules_go_proto",
    importpath = "github.com/dataform-co/dataform/protos/dataform/schedules",
    proto = ":schedules_proto",
)

load("//tools:ts_proto_library.bzl", "ts_proto_library")

ts_proto_library(
    name = "protobufjs_lib",
    deps = [
        ":dataform_proto",
        ":schedules_proto",
        ":server_proto",
    ],
)

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "protos",
    srcs = glob(["index.ts"]),
    data = [
        "//protos:protobufjs_lib",
    ],
    module_name = "@dataform/protos",
    deps = [
        "//protos:protobufjs_lib",
        "@npm//@types/node",
        "@npm//protobufjs",
    ],
)

load("//tools/npm:package.bzl", "dataform_npm_package")

dataform_npm_package(
    name = "package",
    srcs = glob(["**/*.proto"]),
    package_layers = [
        "//:common.package.json",
        "protos.package.json",
    ],
    deps = [":protos"],
)
