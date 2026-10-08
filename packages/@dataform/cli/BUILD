load("@aspect_rules_js//js:defs.bzl", "js_binary")
load("//:version.bzl", "DF_VERSION")
load("//packages:index.bzl", "add_license_header_to_file", "pkg_bundle", "pkg_json", "pkg_npm_tar")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "cli",
    srcs = glob(["*.ts"]),
    deps = [
        "//cli",
        "//cli/api",
        "//protos:ts",
    ],
)

js_binary(
    name = "bin",
    chdir = "$$BUILD_WORKING_DIRECTORY",
    data = [
        "bin_loader.js",
        ":cli",
        "//testing:resolver-patch",
    ],
    entry_point = "bin_loader.js",
    testonly = True,
)

externals = [
    "@google-cloud/bigquery",
    "@google-cloud/lineage",
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
    entry_point = "index.js",
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
    # Compiled cli/vm/worker.ts, the single worker entry point (also used in dev/tests via
    # cli/vm/worker_loader.js).
    entry_point = "//cli/vm:worker.js",
    externals = externals,
    deps = [
        "//cli/vm",
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
