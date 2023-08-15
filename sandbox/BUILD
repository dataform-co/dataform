package(default_visibility = ["//visibility:public"])

load("@com_google_sandboxed_api//sandboxed_api/bazel:build_defs.bzl", "sapi_platform_copts")

cc_binary(
    name = "compile_executor",
    srcs = ["compile_executor.cc"],
    args = [
        "$(location @nodejs_linux_amd64//:node_bin)",
        "$(location //sandbox/worker:worker_bundle.js)",
    ],
    copts = sapi_platform_copts(),
    data = [
        "@nodejs_linux_amd64//:node_bin",
        "//sandbox/worker:worker_bundle.js",
        "//sandbox/worker:node_modules",
    ],
    deps = [
        "@bazel_tools//tools/cpp/runfiles",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_sandboxed_api//sandboxed_api/sandbox2",
        "@com_google_sandboxed_api//sandboxed_api/sandbox2:comms",
        "@com_google_sandboxed_api//sandboxed_api/sandbox2/util:bpf_helper",
        "@com_google_sandboxed_api//sandboxed_api/util:runfiles",
    ],
)

load("//testing:index.bzl", "ts_test_suite")

ts_test_suite(
    name = "test",
    srcs = glob(["*.spec.ts"]),
    args = ["$(execpath :compile_executor)"],
    data = [":compile_executor"],
    deps = [
        "//common/errors",
        "//protos:ts",
        "//testing",
        "@npm//@types/chai",
        "@npm//@types/node",
        "@npm//chai",
    ],
)
