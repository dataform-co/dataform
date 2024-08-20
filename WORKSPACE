workspace(name = "df")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

http_archive(
    name = "bazel_skylib",
    sha256 = "97e70364e9249702246c0e9444bccdc4b847bed1eb03c5a3ece4f83dfe6abc44",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.0.2/bazel-skylib-1.0.2.tar.gz",
        "https://github.com/bazelbuild/bazel-skylib/releases/download/1.0.2/bazel-skylib-1.0.2.tar.gz",
    ],
)

load("@bazel_skylib//:workspace.bzl", "bazel_skylib_workspace")

bazel_skylib_workspace()

load("@bazel_skylib//lib:versions.bzl", "versions")

versions.check(minimum_bazel_version = "0.26.0")

# proto_library rules implicitly depend on @com_google_protobuf//:protoc,
# which is the proto-compiler.
# This statement defines the @com_google_protobuf repo.
http_archive(
    name = "com_google_protobuf",
    sha256 = "f7042d540c969b00db92e8e1066a9b8099c8379c33f40f360eb9e1d98a36ca26",
    strip_prefix = "protobuf-3.21.12",
    urls = ["https://github.com/protocolbuffers/protobuf/archive/v3.21.12.zip"],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()


########

# TODO(ekrekr): We need `base_env` added (used in //tools/ts_proto_library), so have to pull from a
# recent commit sha. When avaialble, update to rules_proto_grpc 4.4.0 when available, and undo
# custom version.
git_repository(
    name = "rules_proto_grpc",
    commit = "e9b7119d712c8e1f4159a10893b2752b38d1b664",
    remote = "https://github.com/rules-proto-grpc/rules_proto_grpc.git",
)

load("@rules_proto_grpc//:repositories.bzl", "rules_proto_grpc_repos", "rules_proto_grpc_toolchains")

rules_proto_grpc_toolchains()

rules_proto_grpc_repos()

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")

rules_proto_dependencies()

rules_proto_toolchains()

############ rules-js

http_archive(
    name = "aspect_rules_js",
    sha256 = "d7500d59712accca9622bce723b8ea596b92fb9b4ff4a4dbd77ed353b9d29f34",
    strip_prefix = "rules_js-1.40.1",
    url = "https://github.com/aspect-build/rules_js/releases/download/v1.40.1/rules_js-v1.40.1.tar.gz",
)

load("@aspect_rules_js//js:repositories.bzl", "rules_js_dependencies")

rules_js_dependencies()

load("@rules_nodejs//nodejs:repositories.bzl", "DEFAULT_NODE_VERSION", "nodejs_register_toolchains")

nodejs_register_toolchains(
    name = "nodejs",
    node_version = DEFAULT_NODE_VERSION,
)

# For convenience, npm_translate_lock does this call automatically.
# Uncomment if you don't call npm_translate_lock at all.
#load("@bazel_features//:deps.bzl", "bazel_features_deps")
#bazel_features_deps()

load("@aspect_rules_js//npm:repositories.bzl", "npm_translate_lock")

npm_translate_lock(
    name = "npm",
    pnpm_lock = "//:pnpm-lock.yaml",
    verify_node_modules_ignored = "//:.bazelignore",
)

load("@npm//:repositories.bzl", "npm_repositories")

npm_repositories()

######### rules-ts

http_archive(
    name = "aspect_rules_ts",
    sha256 = "f69a6452b129d39d9b05f3dff8b1057185bb195b4daf0cff419988de757c6c31",
    strip_prefix = "rules_ts-2.4.2",
    url = "https://github.com/aspect-build/rules_ts/releases/download/v2.4.2/rules_ts-v2.4.2.tar.gz",
)

load("@aspect_rules_ts//ts:repositories.bzl", "rules_ts_dependencies")

rules_ts_dependencies(
    ts_version_from = "//:package.json",
)

########## gazelle

# Go/Gazelle requirements/dependencies.
http_archive(
    name = "io_bazel_rules_go",
    sha256 = "56d8c5a5c91e1af73eca71a6fab2ced959b67c86d12ba37feedb0a2dfea441a6",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.37.0/rules_go-v0.37.0.zip",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.37.0/rules_go-v0.37.0.zip",
    ],
)

http_archive(
    name = "bazel_gazelle",
    sha256 = "ecba0f04f96b4960a5b250c8e8eeec42281035970aa8852dda73098274d14a1d",
    urls = [
        "https://storage.googleapis.com/bazel-mirror/github.com/bazelbuild/bazel-gazelle/releases/download/v0.29.0/bazel-gazelle-v0.29.0.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.29.0/bazel-gazelle-v0.29.0.tar.gz",
    ],
)

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_register_toolchains(version = "1.19.3")

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")

gazelle_dependencies()

#################

# Gcloud SDK binaries.
load("//tools/gcloud:repository_rules.bzl", "gcloud_sdk")

gcloud_sdk(
    name = "gcloud_sdk",
)

# Go dependencies.

go_repository(
    name = "org_golang_google_grpc",
    build_file_proto_mode = "disable",
    importpath = "google.golang.org/grpc",
    sum = "h1:rRYRFMVgRv6E0D70Skyfsr28tDXIuuPZyWGMPdMcnXg=",
    version = "v1.27.0",
)

go_repository(
    name = "org_golang_x_net",
    importpath = "golang.org/x/net",
    sum = "h1:2mqDk8w/o6UmeUCu5Qiq2y7iMf6anbx+YA8d1JFoFrs=",
    version = "v0.0.0-20191002035440-2ec189313ef0",
)

go_repository(
    name = "org_golang_x_text",
    importpath = "golang.org/x/text",
    sum = "h1:tW2bmiBqwgJj/UpqtC8EpXEZVYOwU0yG4iWbprSVAcs=",
    version = "v0.3.2",
)
