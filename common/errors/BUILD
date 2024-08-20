load("@aspect_rules_jasmine//jasmine:defs.bzl", "jasmine_test")
load("@aspect_rules_js//js:defs.bzl", "js_binary", "js_test")
load("@aspect_rules_ts//ts:defs.bzl", "ts_project")
load("@df//testing:index.bzl", "ts_test_suite")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_project(
    name = "errors",
    srcs = ["errors.ts"],
    validate = False,
    tsconfig = "//:tsconfig",
    declaration = True,
    transpiler = "tsc",
)

ts_project(
    name = "errors_test",
    testonly = 1,
    srcs = ["errors.spec.ts"],
    transpiler = "tsc",
    tsconfig = "//:tsconfig",
    validate = False,
    deps = [
        ":errors",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "@df//testing",
    ],
)

# jasmine_test(
#     name = "test",
#     args = ["errors.spec.ts"],
#     chdir = package_name(),
#     data = [
#         ":errors_test",
#     ],
#     node_modules = "//:node_modules",
# )

js_test(
    name = "errors_test_js",
    # chdir = package_name(),
    # data = [
    #     ":errors_test",
    #     # ":package.json"
    # ],
    # tsconfig = "//:tsconfig",
    entry_point = "errors.spec.js",
    data = [
        ":errors.js",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "@df//testing",
    ],
)
