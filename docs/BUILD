package(default_visibility = ["//visibility:public"])

load("//tools:css_typings.bzl", "css_typings")

EXCLUDE_PATTERNS = [
    ".next/**",
    "out/**",
]

load("//tools:next_site.bzl", "next_site")

next_site(
    name = "docs",
    srcs = glob(
        ["**/*.*"],
        exclude = EXCLUDE_PATTERNS,
    ),
    data = [
        "//:package.json",
        "//:tsconfig.json",
        "//fe",
        "//static:files",
        "@npm//@blueprintjs/core",
        "@npm//@zeit/next-css",
        "@npm//@zeit/next-mdx",
        "@npm//@zeit/next-typescript",
        "@npm//css-loader",
        "@npm//extracted-loader",
        "@npm//next-images",
        "@npm//next-server",
        "@npm//react-is",
        "@npm//react-media",
        "@npm//remark",
        "@npm//remark-highlight.js",
        "@npm//remark-react",
        "@npm//remark-slug",
        "@npm//tsconfig-paths-webpack-plugin",
        "@npm//umd-compat-loader",
        "@npm//url-loader",
    ],
    site_path = "docs",
)