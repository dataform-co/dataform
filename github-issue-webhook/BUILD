package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "github-issue-webhook",
    srcs = glob(["**/*.ts"]),
    module_name = "@dataform/github-issue-webhook",
    deps = [
        "@npm//@octokit/rest",
        "@npm//@types/express",
        "@npm//@types/node",
        "@npm//@types/request",
        "@npm//express",
        "@npm//request",
    ],
)

load("//tools/npm:package.bzl", "dataform_npm_package")

dataform_npm_package(
    name = "package",
    package_layers = [
        "github-issue-webhook.package.json",
    ],
    deps = [],
)

load(":deploy_function.bzl", "deploy_gcloud_function")

deploy_gcloud_function(
    name = "foo",
    function_name = "handleStackdriverEvent",
    source_path = "bazel-bin/github-issue-webhook",
    deps = [
        ":github-issue-webhook",
        ":package",
    ],
)
