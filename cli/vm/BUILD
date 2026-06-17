load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "vm",
    srcs = [
        "compile.ts",
        "jit_worker.ts",
    ],
    deps = [
        "//:node_modules/@types/glob",
        "//:node_modules/@types/node",
        "//:node_modules/@types/semver",
        "//:node_modules/glob",
        "//:node_modules/semver",
        "//:node_modules/vm2",
        "//common/protos",
        "//core",
        "//protos:ts",
    ],
)

# This is a bit nuts. As we fork a process when compiling projects, we
# start a new node instance but within bazel, modules will fail to load in the
# forked process as we don't have a loader script available.
# To fix this, we generate the bazel loader scripts
# and then execute these scripts instead, so module resulution works.

genrule(
    name = "bazel_compile_loader",
    srcs = ["//testing:resolver-patch.js", "compile.js"],
    outs = ["bazel_compile_loader.js"],
    cmd = "cat $(SRCS) > $@",
    testonly = True,
)

genrule(
    name = "bazel_jit_worker_loader",
    srcs = ["//testing:resolver-patch.js", "jit_worker.js"],
    outs = ["bazel_jit_worker_loader.js"],
    cmd = "cat $(SRCS) > $@",
    testonly = True,
)
