load("@aspect_rules_js//js:defs.bzl", "js_binary")

package(default_visibility = ["//visibility:public"])

exports_files([
    "ts_proto_library.bzl",
    "compile_protos.js",
])

js_binary(
    name = "compile_protos",
    data = [
        "//:node_modules/minimist",
        "//:node_modules/protobufjs-cli",
    ],
    entry_point = "compile_protos.js",
)
