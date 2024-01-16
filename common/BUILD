load("//tools:ts_library.bzl", "ts_library")
load("//tools:expand_template.bzl", "expand_template")
load("//:version.bzl", "DF_VERSION")

package(default_visibility = ["//visibility:public"])

expand_template(
    name = "version_tmpl_expansion",
    out = "version.ts",
    substitutions = {
        "$DF_VERSION": DF_VERSION,
    },
    template = "version.ts.tmpl",
)

ts_library(
    name = "version",
    srcs = [
        ":version.ts",
    ]
)
