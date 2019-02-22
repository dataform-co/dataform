package(default_visibility = ["//visibility:public"])

load("//tools:css_typings.bzl", "css_typings")

filegroup(
  name = "files",
  srcs = glob(["**/*"])
)

css_typings(
  name = "css",
  srcs = glob(["**/*.css"]),
  path = "fe"
)
