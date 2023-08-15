load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "worker",
    srcs = glob(["*.ts"]),
    deps = [
        "//sandbox/vm",
    ],
)

load("@npm//webpack:index.bzl", "webpack")

webpack(
    name = "bundler",
    data = [
        ":webpack.config.js",
        ":worker",
        "@npm//webpack-cli",
    ],
)

load("@build_bazel_rules_nodejs//:index.bzl", "npm_package_bin")

npm_package_bin(
    name = "bundle",
    outs = [
        "worker_bundle.js",
    ],
    args = [
        "--config=sandbox/worker/webpack.config.js",
        "--output=$(location worker_bundle.js)",
    ],
    data = [
        ":webpack.config.js",
    ],
    tool = ":bundler",
)

load("//tools/common:copy.bzl", "copy_files")

copy_files(
    name = "node_modules",
    map = {
        # vm2
        "@npm//:node_modules/vm2/index.js": "node_modules/vm2/index.js",
        "@npm//:node_modules/vm2/lib/bridge.js": "node_modules/vm2/lib/bridge.js",
        "@npm//:node_modules/vm2/lib/builtin.js": "node_modules/vm2/lib/builtin.js",
        "@npm//:node_modules/vm2/lib/cli.js": "node_modules/vm2/lib/cli.js",
        "@npm//:node_modules/vm2/lib/compiler.js": "node_modules/vm2/lib/compiler.js",
        "@npm//:node_modules/vm2/lib/events.js": "node_modules/vm2/lib/events.js",
        "@npm//:node_modules/vm2/lib/filesystem.js": "node_modules/vm2/lib/filesystem.js",
        "@npm//:node_modules/vm2/lib/main.js": "node_modules/vm2/lib/main.js",
        "@npm//:node_modules/vm2/lib/nodevm.js": "node_modules/vm2/lib/nodevm.js",
        "@npm//:node_modules/vm2/lib/resolver-compat.js": "node_modules/vm2/lib/resolver-compat.js",
        "@npm//:node_modules/vm2/lib/resolver.js": "node_modules/vm2/lib/resolver.js",
        "@npm//:node_modules/vm2/lib/script.js": "node_modules/vm2/lib/script.js",
        "@npm//:node_modules/vm2/lib/setup-node-sandbox.js": "node_modules/vm2/lib/setup-node-sandbox.js",
        "@npm//:node_modules/vm2/lib/setup-sandbox.js": "node_modules/vm2/lib/setup-sandbox.js",
        "@npm//:node_modules/vm2/lib/transformer.js": "node_modules/vm2/lib/transformer.js",
        "@npm//:node_modules/vm2/lib/vm.js": "node_modules/vm2/lib/vm.js",
        "@npm//:node_modules/vm2/node_modules/.bin/acorn": "node_modules/vm2/node_modules/.bin/acorn",
        "@npm//:node_modules/vm2/node_modules/acorn/bin/acorn": "node_modules/vm2/node_modules/acorn/bin/acorn",
        "@npm//:node_modules/vm2/node_modules/acorn/dist/acorn.js": "node_modules/vm2/node_modules/acorn/dist/acorn.js",
        "@npm//:node_modules/vm2/node_modules/acorn/dist/acorn.mjs": "node_modules/vm2/node_modules/acorn/dist/acorn.mjs",
        "@npm//:node_modules/vm2/node_modules/acorn/dist/bin.js": "node_modules/vm2/node_modules/acorn/dist/bin.js",
        "@npm//:node_modules/vm2/node_modules/acorn/package.json": "node_modules/vm2/node_modules/acorn/package.json",
        "@npm//:node_modules/vm2/package.json": "node_modules/vm2/package.json",
        # acorn-walk
        "@npm//:node_modules/acorn-walk/dist/walk.js": "node_modules/acorn-walk/dist/walk.js",
        "@npm//:node_modules/acorn-walk/dist/walk.mjs": "node_modules/acorn-walk/dist/walk.mjs",
        "@npm//:node_modules/acorn-walk/package.json": "node_modules/acorn-walk/package.json",
    },
)
