load("@aspect_bazel_lib//lib:copy_to_bin.bzl", "copy_to_bin")
load("@bazel_gazelle//:def.bzl", "gazelle")
load("@npm//:defs.bzl", "npm_link_all_packages")
load("@npm//:eslint/package_json.bzl", eslint_bin = "bin")
load("@npm//:prettier/package_json.bzl", prettier_bin = "bin")

package(default_visibility = ["//visibility:public"])

npm_link_all_packages(name = "node_modules")

copy_to_bin(
    name = "tsconfig",
    srcs = ["tsconfig.json"],
    visibility = ["//visibility:public"],
)

exports_files([
    "tsconfig.json",
    "package.json",
    "readme.md",
    "version.bzl",
])

eslint_bin.eslint_binary(
    name = "eslint",
    chdir = "$$BUILD_WORKSPACE_DIRECTORY",
    data = [
        "//testing:resolver-patch",
        "//:node_modules/typescript-eslint",
    ],
    node_options = [
        "--require=./testing/resolver-patch.js",
    ],
    visibility = ["//visibility:public"],
)

prettier_bin.prettier_binary(
    name = "prettier",
    chdir = "$$BUILD_WORKSPACE_DIRECTORY",
    visibility = ["//visibility:public"],
)

# gazelle:prefix github.com/dataform-co/dataform
# gazelle:proto package
# gazelle:proto_group go_package
gazelle(name = "gazelle")
