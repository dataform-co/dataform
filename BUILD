load("@aspect_bazel_lib//lib:copy_to_bin.bzl", "copy_to_bin")
load("@bazel_gazelle//:def.bzl", "gazelle")
load("@npm//:defs.bzl", "npm_link_all_packages")
load("@npm//:eslint/package_json.bzl", eslint_bin = "bin")
load("@npm//:prettier/package_json.bzl", prettier_bin = "bin")
load("@npm//:protobufjs-cli/package_json.bzl", "bin")

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

eslint_bin.eslint_binary(
    name = "eslint",
    data = [
        "eslint.config.js",
        "//:node_modules/@typescript-eslint/parser",
        "//:node_modules/eslint",
    ] + glob(["eslint-rules/**"]),
    visibility = ["//visibility:public"],
)

prettier_bin.prettier_binary(
    name = "prettier",
    data = [
        ".prettierignore",
        "//:node_modules/prettier",
    ],
    visibility = ["//visibility:public"],
)

# gazelle:prefix github.com/dataform-co/dataform
# gazelle:proto package
# gazelle:proto_group go_package
gazelle(name = "gazelle")
