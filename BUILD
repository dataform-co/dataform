load("@aspect_rules_js//js:defs.bzl", "js_binary", "js_library")
load("@bazel_gazelle//:def.bzl", "gazelle")
load("@npm//:defs.bzl", "npm_link_all_packages")
load("@aspect_rules_ts//ts:defs.bzl", "ts_config")
load("@build_bazel_rules_nodejs//:index.bzl", "nodejs_binary")

package(default_visibility = ["//visibility:public"])

npm_link_all_packages(name = "node_modules")

exports_files([
    "package.json",
    "readme.md",
    "version.bzl",
    "jsconfig.json"
])

PROTOBUF_DEPS = [
    "//:node_modules/protobufjs",
    "//:node_modules/protobufjs-cli",
    # these deps are needed even though they are not automatic transitive deps of
    # protobufjs since if they are not in the runfiles then protobufjs attempts to
    # run `npm install` at runtime to get thhem which fails as it tries to access
    # the npm cache outside of the sandbox
    "//:node_modules/semver",
    "//:node_modules/chalk",
    "//:node_modules/glob",
    "//:node_modules/jsdoc",
    "//:node_modules/minimist",
    "//:node_modules/tmp",
    "//:node_modules/uglify-js",
    "//:node_modules/uglify-es",
    "//:node_modules/espree",
    "//:node_modules/escodegen",
    "//:node_modules/estraverse",
]

nodejs_binary(
    name = "pbjs",
    data = PROTOBUF_DEPS,
    entry_point = "//:node_modules/protobufjs-cli/bin/pbjs",
)

nodejs_binary(
    name = "pbts",
    data = PROTOBUF_DEPS,
    entry_point = "//:node_modules/protobufjs-cli/bin/pbts",
)

js_library(
    name = "package_json",
    srcs = ["package.json"],
    visibility = ["//visibility:public"],
)

ts_config(
    name = "tsconfig",
    src = "tsconfig.json",
)

js_binary(
    name = "run_tslint",
    entry_point = "//:node_modules/tslint/bin/tslint",
    node_options = ["--preserve-symlinks"],
    # install_source_map_support = False,
)

# gazelle:prefix github.com/dataform-co/dataform
# gazelle:proto package
# gazelle:proto_group go_package
gazelle(name = "gazelle")
