load("//tools:css_typings.bzl", "css_typings")

filegroup(
  name = "files",
  srcs = glob(["**/*"]) + [":css"]
)

css_typings(
  name = "css",
  srcs = glob(["**/*.css"]),
  path = "fe"
)
