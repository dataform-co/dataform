workspace(name = "df")

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

skylib_version = "0.8.0"

http_archive(
    name = "bazel_skylib",
    sha256 = "2ef429f5d7ce7111263289644d233707dba35e39696377ebab8b0bc701f7818e",
    type = "tar.gz",
    url = "https://github.com/bazelbuild/bazel-skylib/releases/download/{}/bazel-skylib.{}.tar.gz".format(skylib_version, skylib_version),
)

load("@bazel_skylib//:lib.bzl", "versions")

versions.check(minimum_bazel_version = "0.26.0")

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
    sha256 = "d14076339deb08e5460c221fae5c5e9605d2ef4848eee1f0c81c9ffdc1ab31c1",
    urls = ["https://github.com/bazelbuild/rules_nodejs/releases/download/1.6.1/rules_nodejs-1.6.1.tar.gz"],
)

load("@build_bazel_rules_nodejs//:index.bzl", "node_repositories", "yarn_install")

node_repositories(
    node_version = "12.13.0",
    package_json = ["//:package.json"],
    yarn_version = "1.13.0",
)

yarn_install(
    name = "npm",
    package_json = "//:package.json",
    symlink_node_modules = False,
    yarn_lock = "//:yarn.lock",
)

load("@npm//:install_bazel_dependencies.bzl", "install_bazel_dependencies")

install_bazel_dependencies()

load("@npm_bazel_typescript//:index.bzl", "ts_setup_workspace")

ts_setup_workspace()

# Go/Gazelle requirements.
http_archive(
    name = "io_bazel_rules_go",
    sha256 = "e88471aea3a3a4f19ec1310a55ba94772d087e9ce46e41ae38ecebe17935de7b",
    urls = [
        "https://storage.googleapis.com/bazel-mirror/github.com/bazelbuild/rules_go/releases/download/v0.20.3/rules_go-v0.20.3.tar.gz",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.20.3/rules_go-v0.20.3.tar.gz",
    ],
)

http_archive(
    name = "bazel_gazelle",
    sha256 = "86c6d481b3f7aedc1d60c1c211c6f76da282ae197c3b3160f54bd3a8f847896f",
    urls = [
        "https://storage.googleapis.com/bazel-mirror/github.com/bazelbuild/bazel-gazelle/releases/download/v0.19.1/bazel-gazelle-v0.19.1.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.19.1/bazel-gazelle-v0.19.1.tar.gz",
    ],
)

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_register_toolchains()

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")

gazelle_dependencies()

# Docker base images.

http_archive(
    name = "io_bazel_rules_docker",
    sha256 = "dc97fccceacd4c6be14e800b2a00693d5e8d07f69ee187babfd04a80a9f8e250",
    strip_prefix = "rules_docker-0.14.1",
    urls = ["https://github.com/bazelbuild/rules_docker/releases/download/v0.14.1/rules_docker-v0.14.1.tar.gz"],
)

load(
    "@io_bazel_rules_docker//repositories:repositories.bzl",
    container_repositories = "repositories",
)

container_repositories()

load(
    "@io_bazel_rules_docker//container:container.bzl",
    "container_pull",
)

container_pull(
    name = "nodejs_slim_base",
    # This digest is for tag "12.13.0-buster-slim".
    digest = "sha256:b63713d51d3a655e476d33b72b1f13d61edfd2b51e7dea1124c4cc1f604c0f69",
    registry = "index.docker.io",
    repository = "library/node",
)

container_pull(
    name = "nginx_base",
    digest = "sha256:8c3cdb5acd050a5a46be0bb5637e23d192f4ef010b4fb6c5af40e45c5b7a0a71",
    registry = "index.docker.io",
    repository = "library/nginx",
)

load(
    "@io_bazel_rules_docker//nodejs:image.bzl",
    _nodejs_image_repos = "repositories",
)

_nodejs_image_repos()

# Sass requirements.
http_archive(
    name = "io_bazel_rules_sass",
    sha256 = "77e241148f26d5dbb98f96fe0029d8f221c6cb75edbb83e781e08ac7f5322c5f",
    strip_prefix = "rules_sass-1.24.0",
    url = "https://github.com/bazelbuild/rules_sass/archive/1.24.0.zip",
)

# Fetch required transitive dependencies. This is an optional step because you
# can always fetch the required NodeJS transitive dependency on your own.
load("@io_bazel_rules_sass//:package.bzl", "rules_sass_dependencies")

rules_sass_dependencies()

# Setup repositories which are needed for the Sass rules.
load("@io_bazel_rules_sass//:defs.bzl", "sass_repositories")

sass_repositories()

# Gcloud SDK binaries.
load("//tools/gcloud:repository_rules.bzl", "gcloud_sdk")

gcloud_sdk(
    name = "gcloud_sdk",
)

# Go dependencies.

go_repository(
    name = "com_github_go_stack_stack",
    importpath = "github.com/go-stack/stack",
    sum = "h1:5SgMzNM5HxrEjV0ww2lTmX6E2Izsfxas4+YHWRs3Lsk=",
    version = "v1.8.0",
)

go_repository(
    name = "com_github_golang_protobuf",
    importpath = "github.com/golang/protobuf",
    sum = "h1:6nsPYzhq5kReh6QImI3k5qWzO4PEbvbIW2cwSfR/6xs=",
    version = "v1.3.2",
)

go_repository(
    name = "org_mongodb_go_mongo_driver",
    importpath = "go.mongodb.org/mongo-driver",
    sum = "h1:6fhXjXSzzXRQdqtFKOI1CDw6Gw5x6VflovRpfbrlVi0=",
    version = "v1.2.0",
)
