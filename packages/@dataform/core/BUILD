load("@aspect_rules_webpack//webpack:defs.bzl", "webpack_bundle")
load("//:version.bzl", "DF_VERSION")
load("//packages:index.bzl", "add_license_header_to_file", "pkg_bundle_dts", "pkg_json", "pkg_npm_tar")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "core",
    srcs = glob(["*.ts"]),
    deps = [
        "//core",
    ],
)

webpack_bundle(
    name = "bundle_no_license",
    args = [
        "--mode=production",
    ],
    data = [
        ":webpack.config.js",
        "//:node_modules/webpack-cli",
    ],
    entry_point = "index.js",
    node_modules = "//:node_modules",
    webpack_config = ":webpack.config.js",
    deps = [
        ":core",
        "//common/errors",
        "//common/protos",
        "//common/strings",
        "//core",
        "//core/compilation_sql",
        "//protos:ts",
        "//sqlx:lexer",
    ],
)

pkg_json(
    name = "json",
    package_name = "@dataform/core",
    description = "Dataform core API.",
    external_deps = [],
    layers = [
        "//:package_json",
        "//packages/@dataform:package_layer",
    ],
    main = "bundle.js",
    version = DF_VERSION,
)

add_license_header_to_file(
    name = "core_proto_with_license",
    from_file = "//protos:core.proto",
    to_file = "core.proto",
)

add_license_header_to_file(
    name = "configs_proto_with_license",
    from_file = "//protos:configs.proto",
    to_file = "configs.proto",
)

add_license_header_to_file(
    name = "db_adapter_proto_with_license",
    from_file = "//protos:db_adapter.proto",
    to_file = "db_adapter.proto",
)

add_license_header_to_file(
    name = "jit_proto_with_license",
    from_file = "//protos:jit.proto",
    to_file = "jit.proto",
)

add_license_header_to_file(
    name = "extension_proto_with_license",
    from_file = "//protos:extension.proto",
    to_file = "extension.proto",
)

add_license_header_to_file(
    name = "bundle_with_license",
    from_file = ":bundle_no_license",
    to_file = "bundle.js",
)

pkg_bundle_dts(
    name = "bundle.d",
    entry_point = "index.d.ts",
    externals = [
        "protobufjs",
        "tarjan-graph",
        "semver",
        "moo",
        "long",
    ],
    deps = [
        ":core",
    ],
)

pkg_npm_tar(
    name = "package",
    deps = [
        ":bundle.d",
        ":bundle_with_license",
        ":configs_proto_with_license",
        ":core_proto_with_license",
        ":db_adapter_proto_with_license",
        ":extension_proto_with_license",
        ":jit_proto_with_license",
        ":package.json",
        "//:version.bzl",
    ],
)
