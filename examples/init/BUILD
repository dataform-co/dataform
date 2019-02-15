package(default_visibility=["//tests:__subpackages__"])

load("//tools:node_modules.bzl", "node_modules")

node_modules(
  name = "node_modules",
  deps = [
    "//core:package_tar",
    "//crossdb:package_tar",
    "//protos:package_tar"
  ]
)

genrule(
  name = "init",
  outs = ["package.json", "dataform.json"],
  cmd = "$(location //cli:bin) init $$(dirname $(location dataform.json)) --skip-install",
  tools = ["//cli:bin"]
)
