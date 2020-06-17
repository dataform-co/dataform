package(default_visibility = ["//visibility:public"])

load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "stackdriver-github-bridge",
    srcs = glob(["**/*.ts"]),
    deps = [
        "@npm//@octokit/rest",
        "@npm//@types/express",
        "@npm//@types/node",
        "@npm//@types/request",
        "@npm//express",
        "@npm//request",
    ],
)

filegroup(
    name = "index.js",
    srcs = ["stackdriver-github-bridge"],
    output_group = "es5_sources",
)

load("//tools/gcloud:secrets.bzl", "gcloud_secret")

gcloud_secret(
    name = "env.yaml",
    ciphertext_file = ":env.yaml.enc",
    key = "dataform-co-build",
    keyring = "dataform-co-build",
)

load("//tools/gcloud:gcloud_function.bzl", "deploy_http_nodejs_gcloud_function")

deploy_http_nodejs_gcloud_function(
    name = "deploy",
    srcs = [
        ":index.js",
        ":package.json",
    ],
    env_vars_file = ":env.yaml",
    function_name = "handleStackdriverEvent",
)
