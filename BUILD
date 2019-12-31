package(default_visibility = ["//visibility:public"])

load("@build_bazel_rules_nodejs//:defs.bzl", "nodejs_binary")

exports_files([
    "tsconfig.json",
    "package.json",
    "common.package.json",
    "readme.md",
    "version.bzl",
])

PROTOBUF_DEPS = [
    "@npm//protobufjs",
    # these deps are needed even though they are not automatic transitive deps of
    # protobufjs since if they are not in the runfiles then protobufjs attempts to
    # run `npm install` at runtime to get thhem which fails as it tries to access
    # the npm cache outside of the sandbox
    "@npm//semver",
    "@npm//chalk",
    "@npm//glob",
    "@npm//jsdoc",
    "@npm//minimist",
    "@npm//tmp",
    "@npm//uglify-js",
    "@npm//uglify-es",
    "@npm//espree",
    "@npm//escodegen",
    "@npm//estraverse",
]

nodejs_binary(
    name = "pbjs",
    data = PROTOBUF_DEPS,
    entry_point = "@npm//:node_modules/protobufjs/bin/pbjs",
    install_source_map_support = False,
)

nodejs_binary(
    name = "pbts",
    data = PROTOBUF_DEPS,
    entry_point = "@npm//:node_modules/protobufjs/bin/pbts",
    install_source_map_support = False,
)

nodejs_binary(
    name = "tslint",
    data = [
        "@npm//tslint",
    ],
    entry_point = "@npm//:node_modules/tslint/bin/tslint",
    install_source_map_support = False,
    templated_args = ["--node_options=--preserve-symlinks"],
)

load("@bazel_gazelle//:def.bzl", "gazelle")

# gazelle:prefix github.com/dataform-co/dataform
# gazelle:proto package
# gazelle:proto_group go_package
gazelle(name = "gazelle")
