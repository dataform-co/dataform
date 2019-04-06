package(default_visibility = ["//visibility:public"])

load("//tools:css_typings.bzl", "css_typings")

EXCLUDE_PATTERNS = [
    ".next/**",
    "out/**",
]

load("//tools:next_site.bzl", "next_site")

next_site(
    name = "docs",
    srcs = glob(["**/*.*"], exclude = EXCLUDE_PATTERNS),
    data = [
        "//fe",
        "//fe:css",
        "//static:files",
        "//:package.json",
        "//:tsconfig.json",
        "@npm//@blueprintjs/core",
        "@npm//@zeit/next-css",
        "@npm//@zeit/next-mdx",
        "@npm//@zeit/next-typescript",
        "@npm//css-loader",
        "@npm//extracted-loader",
        "@npm//next-images",
        "@npm//next-server",
        "@npm//react-is",
        "@npm//remark-highlight.js",
        "@npm//remark-slug",
        "@npm//umd-compat-loader",
        "@npm//url-loader",
    ],
    site_path = "docs",
)

# load("@io_bazel_rules_docker//container:container.bzl", "container_image")

# container_image(
#     name = "image",
#     base = "@nginx_base//image",
#     directory = "/usr/share/nginx/html",
#     tars = [":site_pkg"],
# )

# load("//rules:containers_push.bzl", "containers_push")

# containers_push(
#     name = "push",
#     image = ":image",
#     repository = "tada-analytics/static-landing",
# )
