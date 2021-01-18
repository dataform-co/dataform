load("@com_google_sandboxed_api//sandboxed_api/bazel:build_defs.bzl", "sapi_platform_copts")

cc_binary(
    name = "executor",
    srcs = ["executor.cc"],
    args = [
        "$(execpath @nodejs_linux_amd64//:node_bin)",
        "$(locations //api:vm/compile)",
        # "$(locations //packages/@dataform/cli:worker_bundle)",
    ],
    copts = sapi_platform_copts(),
    data = [
        "@nodejs_linux_amd64//:node_bin",
        "//api:vm/compile",
        # "//packages/@dataform/cli:worker_bundle",
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
