workspace(name = "df")

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "bazel_skylib",
    sha256 = "b5f6abe419da897b7901f90cbab08af958b97a8f3575b0d3dd062ac7ce78541f",
    strip_prefix = "bazel-skylib-0.5.0",
    urls = ["https://github.com/bazelbuild/bazel-skylib/archive/0.5.0.tar.gz"],
)

load("@bazel_skylib//:lib.bzl", "versions")

versions.check(minimum_bazel_version = "0.20.0")

# proto_library rules implicitly depend on @com_google_protobuf//:protoc,
# which is the proto-compiler.
# This statement defines the @com_google_protobuf repo.
http_archive(
    name = "com_google_protobuf",
    sha256 = "983975ab66113cbaabea4b8ec9f3a73406d89ed74db9ae75c74888e685f956f8",
    strip_prefix = "protobuf-66dc42d891a4fc8e9190c524fd67961688a37bbe",
    url = "https://github.com/google/protobuf/archive/66dc42d891a4fc8e9190c524fd67961688a37bbe.tar.gz",
)

http_archive(
    name = "build_bazel_rules_nodejs",
    sha256 = "23987a5cf549146742aa6a0d2536e4906906e63a608d5b9b32dd9fe5523ef51c",
    urls = ["https://github.com/bazelbuild/rules_nodejs/releases/download/0.18.4/rules_nodejs-0.18.4.tar.gz"],
)

# load("@build_bazel_rules_nodejs//:package.bzl", "rules_nodejs_dependencies")
# rules_nodejs_dependencies()

# Setup the NodeJS toolchain
load("@build_bazel_rules_nodejs//:defs.bzl", "node_repositories", "npm_install")

node_repositories(package_json = ["//:package.json"])

# Setup Bazel managed npm dependencies with the `npm_install` rule.
npm_install(
    name = "npm",
    package_json = "//:package.json",
    package_lock_json = "//:package-lock.json",
)

# Install all Bazel dependencies needed for npm packages that supply Bazel rules
load("@npm//:install_bazel_dependencies.bzl", "install_bazel_dependencies")

install_bazel_dependencies()

# Setup TypeScript toolchain
load("@build_bazel_rules_typescript//:defs.bzl", "ts_setup_workspace")

ts_setup_workspace()

# buildifier is written in Go and hence needs rules_go to be built.
# See https://github.com/bazelbuild/rules_go for the up to date setup instructions.
http_archive(
    name = "io_bazel_rules_go",
    sha256 = "f87fa87475ea107b3c69196f39c82b7bbf58fe27c62a338684c20ca17d1d8613",
    url = "https://github.com/bazelbuild/rules_go/releases/download/0.16.2/rules_go-0.16.2.tar.gz",
)

http_archive(
    name = "com_github_bazelbuild_buildtools",
    strip_prefix = "buildtools-609ca6e8a79750cf4c6ce37bb92ae8d54876f9e1",
    url = "https://github.com/bazelbuild/buildtools/archive/609ca6e8a79750cf4c6ce37bb92ae8d54876f9e1.zip",
)

load("@io_bazel_rules_go//go:def.bzl", "go_register_toolchains", "go_rules_dependencies")
load("@com_github_bazelbuild_buildtools//buildifier:deps.bzl", "buildifier_dependencies")

go_rules_dependencies()

go_register_toolchains()

buildifier_dependencies()
