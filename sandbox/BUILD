load("@com_google_sandboxed_api//sandboxed_api/bazel:build_defs.bzl", "sapi_platform_copts")

# Executor
cc_binary(
    name = "sandbox_executor",
    srcs = ["sandbox_executor.cc"],
    copts = sapi_platform_copts(),
    data = [":sandboxee_bin"],
    deps = [
        "@com_google_sandboxed_api//sandboxed_api/sandbox2",
        "@com_google_sandboxed_api//sandboxed_api/sandbox2:comms",
        "@com_google_sandboxed_api//sandboxed_api/sandbox2/util:bpf_helper",
        "@com_google_sandboxed_api//sandboxed_api/sandbox2/util:fileops",
        "@com_google_sandboxed_api//sandboxed_api/sandbox2/util:runfiles",
        "@com_google_sandboxed_api//sandboxed_api/util:flags",
        "@com_google_absl//absl/base:core_headers",
    ],
)

# This should run vm2 with @dataform/core inside, which takes data being piped in, then compiles the project.
cc_binary(
    name = "compile_project_bin",
    srcs = ["compile_project.cc"],
    copts = sapi_platform_copts(),
    deps = [
        "@com_google_sandboxed_api//sandboxed_api/sandbox2:client",
        "@com_google_sandboxed_api//sandboxed_api/sandbox2:comms",
        "@com_google_sandboxed_api//sandboxed_api/sandbox2:util",
        "@com_google_absl//absl/strings:str_format",
    ],
)