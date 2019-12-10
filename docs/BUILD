package(default_visibility = ["//visibility:public"])

EXCLUDE_PATTERNS = [
    ".next/**",
    "out/**",
    "assets/**",
]

load("//tools:css_typings.bzl", "css_typings")

filegroup(
    name = "css",
    srcs = glob(["**/*.css"]),
)

css_typings(
    name = "css_typings",
    srcs = [":css"],
)

filegroup(
    name = "sources",
    srcs = glob(
        ["**/*.*"],
        exclude = EXCLUDE_PATTERNS,
    )
)
filegroup(
    name = "assets",
    srcs = glob(["static/**/*.*"])
)

load("//tools:next_site.bzl", "next_site2")

next_site2(
    name = "docs",
    srcs = [":sources"],
    data = [
        ":css_typings",
        "//:package.json",
        "//:tsconfig.json",
        "//content",
        "//fe:css",
        "//static:files",
        "@npm//typescript",
        "@npm//@types/react",
        "@npm//@types/node",
        "@npm//@blueprintjs/core",
        "@npm//@blueprintjs/select",
        "@npm//@mapbox/rehype-prism",
        "@npm//@zeit/next-css",
        "@npm//@zeit/next-mdx",
        "@npm//@zeit/next-typescript",
        "@npm//algoliasearch",
        "@npm//css-loader",
        "@npm//front-matter",
        "@npm//remark-rehype",
        "@npm//rehype-raw",
        "@npm//rehype-react",
        "@npm//@octokit/rest",
        "@npm//extracted-loader",
        "@npm//next-images",
        "@npm//react-instantsearch-dom",
        "@npm//react-is",
        "@npm//react-media",
        "@npm//remark",
        "@npm//rehype-slug",
        "@npm//tsconfig-paths-webpack-plugin",
        "@npm//umd-compat-loader",
        "@npm//url-loader",
    ],
    port = 3001,
    site_path = "docs",
)

load("@npm//next:index.bzl", "next")

# npm_package_bin(
#     name = ".next",
#     output_dir = True,
#     args = [
#         "--src-dir=docs",
#         "--dest-dir=$(@D)",
#     ],
#     tool = "//tools/next:build-bin",

DEPS = [
        ":css_typings",
        "//:package.json",
        "//:tsconfig.json",
        "//content",
        "//static:files",
        "@npm//typescript",
        "@npm//@types/react",
        "@npm//@types/node",
        "@npm//@blueprintjs/core",
        "@npm//@blueprintjs/select",
        "@npm//@mapbox/rehype-prism",
        "@npm//@zeit/next-css",
        "@npm//@zeit/next-mdx",
        "@npm//@zeit/next-typescript",
        "@npm//algoliasearch",
        "@npm//css-loader",
        "@npm//front-matter",
        "@npm//remark-rehype",
        "@npm//rehype-raw",
        "@npm//rehype-react",
        "@npm//@octokit/rest",
        "@npm//extracted-loader",
        "@npm//next-images",
        "@npm//react-instantsearch-dom",
        "@npm//react-is",
        "@npm//react-media",
        "@npm//remark",
        "@npm//rehype-slug",
        "@npm//tsconfig-paths-webpack-plugin",
        "@npm//umd-compat-loader",
        "@npm//url-loader",
    ]

# This is the output build directory for the next project. It must be called .next because next is very opinionated on this.
next(
    name = ".next",
    output_dir = True,
    args = [
        "build",
        "docs",
        # This resolves to the output_dir, which is the name of this build rule.
        "$(@D)",
    ],
    data = DEPS + glob(
        ["**/*.*"],
        exclude = EXCLUDE_PATTERNS,
    ),
)

next(
    name = "dev",
    args = [
        "dev",
        "docs",
        "-p",
        "3001"
    ],
    data = DEPS + [
        ":assets"
    ] + glob(
        ["**/*.*"],
        exclude = EXCLUDE_PATTERNS,
    )
)

next(
    name = "bin",
    args = [
        "start",
        "docs",
    ],
    data = DEPS + [
        ":assets",
        ":.next",
    ]
)

load("@io_bazel_rules_docker//container:container.bzl", "container_image")

container_image(
    name = "image",
    base = "//tools/nginx/static",
    directory = "/usr/share/nginx/html",
    tars = [":docs_pkg"],
)

load("@io_bazel_rules_docker//container:container.bzl", "container_push")

container_push(
    name = "push",
    format = "Docker",
    image = ":image",
    registry = "gcr.io",
    repository = "tada-analytics/static-docs",
    tag = "latest",
)
