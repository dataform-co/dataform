package(default_visibility = ["//visibility:public"])

load("@com_google_sandboxed_api//sandboxed_api/bazel:build_defs.bzl", "sapi_platform_copts")

cc_binary(
    name = "compile_executor",
    srcs = ["compile_executor.cc"],
    args = [
        "$(execpath @nodejs_linux_amd64//:node_bin)",
        "$(execpath :copy_worker_bundle)",
    ],
    copts = sapi_platform_copts(),
    data = [
        ":copy_worker_bundle",
        "@nodejs_linux_amd64//:node_bin",
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

# Worker bundle needs to be copied over into packages, as deps files are only
# included in the root directory of package folders.
genrule(
    name = "copy_worker_bundle",
    srcs = ["//sandbox/vm:worker_bundle"],
    outs = ["worker_bundle.js"],
    cmd = "cp $< $@",
    output_to_bindir = 1,
)
