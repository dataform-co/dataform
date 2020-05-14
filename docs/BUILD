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
    ),
)

genrule(
    name = "blueprint",
    srcs = [
        "//components/blueprint:sass",
        "//components/code:sass",
    ],
    outs = ["public/blueprint.css"],
    cmd = "cat $(SRCS) > $(OUTS)",
)

filegroup(
    name = "assets",
    srcs = glob(["static/**/*.*"]) + [":public/blueprint.css"],
)

load("@npm//typedoc:index.bzl", "typedoc")

typedoc(
    name = "typedoc",
    outs = ["core.typedoc.json"],
    args = [
        "--json",
        "$(location :core.typedoc.json)",
        "--excludePrivate",
        "--excludeNotExported",
        "--ignoreCompilerErrors",
        "--entryPoint",
        "\"table\"",
        "--excludeExternals",
        "core/table.ts",
        "core/assertion.ts",
        "core/declaration.ts",
        "core/operation.ts",
        "core/test.ts",
        "core/common.ts",
    ],
    data = [
        "//:tsconfig.json",
        "//core:files",
    ],
)

load("@npm//next:index.bzl", "next")

DEPS = [
    ":core.typedoc.json",
    ":css_typings",
    "//:package.json",
    "//:tsconfig.json",
    "//content",
    "//static:files",
    "//components",
    "//components/code",
    "//components:sass",
    "//components/blueprint:sass",
    "//tools/markdown-cms",
    "@npm//axios",
    "@npm//node-cache",
    "@npm//typescript",
    "@npm//@types/react",
    "@npm//@types/node-cache",
    "@npm//@types/swagger-schema-official",
    "@npm//@types/node",
    "@npm//@blueprintjs/core",
    "@npm//@blueprintjs/select",
    "@npm//@mapbox/rehype-prism",
    "@npm//@zeit/next-css",
    "@npm//@zeit/next-mdx",
    "@npm//@zeit/next-typescript",
    "@npm//algoliasearch",
    "@npm//css-loader",
    "@npm//remark-rehype",
    "@npm//rehype-raw",
    "@npm//rehype-react",
    "@npm//extracted-loader",
    "@npm//next-images",
    "@npm//react-instantsearch-dom",
    "@npm//remark",
    "@npm//rehype-slug",
    "@npm//umd-compat-loader",
    "@npm//url-loader",
]

next(
    name = "docs",
    args = [
        "dev",
        "docs",
        "-p",
        "3001",
    ],
    data = DEPS + [
        ":assets",
        ":sources",
    ],
)

load("@build_bazel_rules_nodejs//:index.bzl", "npm_package_bin")

# This is the output build directory for the next project. It must be called .next because next is very opinionated on this.
npm_package_bin(
    name = ".next",
    args = [
        "build",
        "docs",
        # This resolves to the output_dir, which is the name of this build rule.
        "$(@D)",
    ],
    data = DEPS + [
        ":sources",
        ":assets",
    ],
    output_dir = True,
    # The number of input files to this rule exceeds the cap of 50K.
    tags = ["no-remote-exec"],
    tool = ":docs",
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
    ],
)

load("@io_bazel_rules_docker//nodejs:image.bzl", "nodejs_image")

nodejs_image(
    name = "image",
    args = [
        "start",
        "docs",
    ],
    data = DEPS + [
        ":.next",
        ":assets",
        "@npm//next",
    ],
    entry_point = "@npm//:node_modules/next/dist/bin/next",
    install_source_map_support = False,
)

load("@io_bazel_rules_docker//container:container.bzl", "container_push")

container_push(
    name = "push",
    format = "Docker",
    image = ":image",
    registry = "gcr.io",
    repository = "tada-analytics/dataform-docs",
    tag = "latest",
)
