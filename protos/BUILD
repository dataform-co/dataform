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
    deps = [
        ":dataform_proto",
    ],
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

go_proto_library(
    name = "server_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/dataform-co/dataform/protos",
    proto = ":server_proto",
    deps = [":dataform_go_proto"],
)

load("//tools:ts_proto_library.bzl", "ts_proto_library")

ts_proto_library(
    name = "ts",
    deps = [
        ":dataform_proto",
        ":schedules_proto",
        ":server_proto",
    ],
)

load("//protobufts:ts_proto_library.bzl", new_ts_proto_library = "ts_proto_library")

new_ts_proto_library(
    name = "dataform_ts_proto",
    protos = [
        ":dataform_proto",
    ],
)
