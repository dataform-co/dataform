load("@com_google_sandboxed_api//sandboxed_api/bazel:build_defs.bzl", "sapi_platform_copts")

cc_binary(
    name = "executor",
    srcs = ["executor.cc",],
    copts = sapi_platform_copts(),
    data = [],
    deps = [
        "@com_google_sandboxed_api//sandboxed_api/sandbox2",
        "@com_google_sandboxed_api//sandboxed_api/sandbox2:comms",
        "@com_google_sandboxed_api//sandboxed_api/sandbox2/util:bpf_helper",
        "@com_google_sandboxed_api//sandboxed_api/sandbox2/util:runfiles",
        "@com_google_absl//absl/base:core_headers",
    ],
)
