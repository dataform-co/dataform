load("@aspect_bazel_lib//lib:copy_to_bin.bzl", "copy_to_bin")
load("@bazel_gazelle//:def.bzl", "gazelle")
load("@npm//:defs.bzl", "npm_link_all_packages")
load("@npm//:protobufjs-cli/package_json.bzl", "bin")
load("@npm//:tslint/package_json.bzl", tslint_bin = "bin")

package(default_visibility = ["//visibility:public"])

npm_link_all_packages(name = "node_modules")

copy_to_bin(
    name = "tsconfig",
    srcs = ["tsconfig.json"],
    visibility = ["//visibility:public"],
)

copy_to_bin(
    name = "tsconfig_esm",
    srcs = ["tsconfig.esm.json"],
    visibility = ["//visibility:public"],
)

copy_to_bin(
    name = "package_json",
    srcs = ["package.json"],
    visibility = ["//visibility:public"],
)

exports_files([
    "tsconfig.json",
    "tsconfig.esm.json",
    "package.json",
    "readme.md",
    "version.bzl",
])

bin.pbjs_binary(
    name = "pbjs",
    chdir = ".",
    visibility = ["//visibility:public"],
)

bin.pbts_binary(
    name = "pbts",
    chdir = ".",
    visibility = ["//visibility:public"],
)

tslint_bin.tslint_binary(
    name = "tslint",
    data = [
        "//:node_modules/tslint-config-prettier",
        "//:node_modules/tslint-config-security",
    ],
    visibility = ["//visibility:public"],
)

# gazelle:prefix github.com/dataform-co/dataform
# gazelle:proto package
# gazelle:proto_group go_package
gazelle(name = "gazelle")
