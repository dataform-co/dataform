load("@aspect_rules_js//js:defs.bzl", "js_library")
load("//tools:node_modules.bzl", "node_modules")

package(default_visibility = ["//tests:__subpackages__"])

js_library(
    name = "files",
    srcs = glob([
        "**/*.*",
    ]),
)

node_modules(
    name = "node_modules",
    deps = [
        "//packages/@dataform/core:package_tar",
    ],
)
