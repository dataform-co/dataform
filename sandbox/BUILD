load("@com_google_sandboxed_api//sandboxed_api/bazel:build_defs.bzl", "sapi_platform_copts")

cc_binary(
    name = "executor",
    srcs = ["executor.cc"],
    args = [
        "$(execpath @nodejs_linux_amd64//:node_bin)",
        "$(locations //api:vm/compile_loader.js)",
    ],
    copts = sapi_platform_copts(),
    data = [
        "@nodejs_linux_amd64//:node_bin",
        "//api:vm/compile_loader.js",
    ],
    deps = [
        "@com_google_sandboxed_api//sandboxed_api/sandbox2",
        "@com_google_sandboxed_api//sandboxed_api/sandbox2:comms",
        "@com_google_sandboxed_api//sandboxed_api/sandbox2/util:bpf_helper",
        "@com_google_sandboxed_api//sandboxed_api/util:runfiles",
        "@com_google_absl//absl/base:core_headers",
        "@bazel_tools//tools/cpp/runfiles",
    ],
)
