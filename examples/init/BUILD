package(default_visibility = ["//tests:__subpackages__"])

load("//tools:node_modules.bzl", "node_modules")

node_modules(
    name = "node_modules",
    deps = [
        "//core:package_tar",
        "//crossdb:package_tar",
        "//protos:package_tar",
    ],
)

# TODO: The output of this is used by the "init" tests in api.spec.ts.
# We should instead just invoke the init API directly in that test.
genrule(
    name = "init",
    outs = [
        "package.json",
        "dataform.json",
    ],
    cmd = "$(location //cli:bin) init bigquery $$(dirname $(location dataform.json)) --gcloud-project-id=12345 --skip-install",
    tools = ["//cli:bin"],
)
