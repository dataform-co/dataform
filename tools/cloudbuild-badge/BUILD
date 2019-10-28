package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "cloudbuild-badge",
    srcs = glob(["**/*.ts"]),
    deps = [
        "@npm//@google-cloud/storage",
        "@npm//@types/node",
    ],
)

load("@build_bazel_rules_nodejs//:defs.bzl", "npm_package")

npm_package(
    name = "package",
    srcs = ["package.json"],
    deps = [":cloudbuild-badge"],
)

load("//tools/gcloud:gcloud_function.bzl", "deploy_pubsub_nodejs_gcloud_function")

deploy_pubsub_nodejs_gcloud_function(
    name = "deploy",
    function_name = "updateCloudBuildStatusBadge",
    npm_package = ":package",
    topic_name = "cloud-builds",
)
