load("@aspect_rules_js//js:defs.bzl", "js_binary")
load("//:version.bzl", "DF_VERSION")
load("//packages:index.bzl", "add_license_header_to_file", "pkg_bundle", "pkg_json", "pkg_npm_tar")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

genrule(
    name = "register_loader_gen",
    srcs = ["//testing:resolver-patch.js"],
    outs = ["register_loader.js"],
    cmd = "cat $(SRCS) > $@",
)

ts_library(
    name = "cli",
    srcs = glob(["*.ts"]),
    deps = [
        "//cli",
        "//cli/vm:compile_loader",
    ],
)

js_binary(
    name = "bin",
    chdir = "../../..",
    data = [
        "bin_loader.js",
        ":register_loader.js",
        ":cli",
        "//:node_modules/source-map-support",
    ],
    entry_point = "bin_loader.js",
)

externals = [
    "@google-cloud/bigquery",
    "chokidar",
    "deepmerge",
    "fs-extra",
    "glob",
    "google-sql-syntax-ts",
    "js-beautify",
    "js-yaml",
    "moo",
    "object-sizeof",
    "parse-duration",
    "promise-pool-executor",
    "protobufjs",
    "readline-sync",
    "semver",
    "tarjan-graph",
    "tmp",
    "typeid-js",
    "untildify",
    "vm2",
    "yargs",
]

pkg_json(
    name = "json",
    package_name = "@dataform/cli",
    description = "Dataform command line interface.",
    external_deps = externals,
    layers = [
        "//:package_json",
        "//packages/@dataform:package_layer",
        "//packages/@dataform/cli:package.layer.json",
    ],
    main = "bundle.js",
    version = DF_VERSION,
)

pkg_bundle(
    name = "bundle_no_license",
    allow_node_builtins = True,
    entry_point = "esm/index.js",
    externals = externals,
    deps = [
        ":cli",
    ],
)

add_license_header_to_file(
    name = "bundle",
    from_file = ":bundle_no_license",
    to_file = "bundle.js",
    use_shebang = True,
)

pkg_bundle(
    name = "worker_bundle_no_license",
    allow_node_builtins = True,
    entry_point = "esm/worker.js",
    externals = externals,
    deps = [
        ":cli",
    ],
)

add_license_header_to_file(
    name = "worker_bundle",
    from_file = ":worker_bundle_no_license",
    to_file = "worker_bundle.js",
    use_shebang = True,
)

pkg_npm_tar(
    name = "package",
    deps = [
        ":bundle",
        ":package.json",
        ":worker_bundle",
    ],
)
