load("@aspect_rules_js//js:defs.bzl", "js_library")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "vm",
    srcs = [
        "compile.ts",
        "jit_worker.ts",
        "worker.ts",
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

# When compiling a project, the CLI forks a worker process running worker.ts. The published
# CLI forks worker_bundle.js; in dev and tests there is no bundle, so the worker is started
# through this loader, which installs the "df/..." resolver patch before loading worker.js.
# //cli/api depends on this target, so anything that runs the CLI API gets a working loader.
js_library(
    name = "worker_loader",
    srcs = ["worker_loader.js"],
    deps = [
        ":vm",
        "//testing:resolver-patch",
    ],
)
