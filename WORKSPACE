workspace(name = "df")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

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
    sha256 = "1c744a6a1f2c901e68c5521bc275e22bdc66256eeb605c2781923365b7087e5f",
    strip_prefix = "protobuf-3.13.0",
    urls = ["https://github.com/protocolbuffers/protobuf/archive/v3.13.0.zip"],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

http_archive(
    name = "build_bazel_rules_nodejs",
    sha256 = "d14076339deb08e5460c221fae5c5e9605d2ef4848eee1f0c81c9ffdc1ab31c1",
    urls = ["https://github.com/bazelbuild/rules_nodejs/releases/download/1.6.1/rules_nodejs-1.6.1.tar.gz"],
)

load("@build_bazel_rules_nodejs//:index.bzl", "node_repositories", "yarn_install")

node_repositories(
    node_repositories = {
        "16.16.0-darwin_amd64": ("node-v16.16.0-darwin-x64.tar.gz", "node-v16.16.0-darwin-x64", "982edd0fad364ad6e2d72161671544ab9399bd0ca8c726bde3cd07775c4c709a"),
        "16.16.0-linux_amd64": ("node-v16.16.0-linux-x64.tar.xz", "node-v16.16.0-linux-x64", "edcb6e9bb049ae365611aa209fc03c4bfc7e0295dbcc5b2f1e710ac70384a8ec"),
        "16.16.0-windows_amd64": ("node-v16.16.0-win-x64.zip", "node-v16.16.0-win-x64", "c657acc98af55018c8fd6113c7e08d67c8083af75ba0306f9561b0117abc39d4"),
    },
    node_version = "16.16.0",
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

# Go/Gazelle requirements/dependencies.
http_archive(
    name = "io_bazel_rules_go",
    sha256 = "7b9bbe3ea1fccb46dcfa6c3f3e29ba7ec740d8733370e21cdc8937467b4a4349",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.22.4/rules_go-v0.22.4.tar.gz",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.22.4/rules_go-v0.22.4.tar.gz",
    ],
)

http_archive(
    name = "bazel_gazelle",
    sha256 = "d8c45ee70ec39a57e7a05e5027c32b1576cc7f16d9dd37135b0eddde45cf1b10",
    urls = [
        "https://storage.googleapis.com/bazel-mirror/github.com/bazelbuild/bazel-gazelle/releases/download/v0.20.0/bazel-gazelle-v0.20.0.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.20.0/bazel-gazelle-v0.20.0.tar.gz",
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
    sha256 = "4521794f0fba2e20f3bf15846ab5e01d5332e587e9ce81629c7f96c793bb7036",
    strip_prefix = "rules_docker-0.14.4",
    urls = ["https://github.com/bazelbuild/rules_docker/releases/download/v0.14.4/rules_docker-v0.14.4.tar.gz"],
)

load("@io_bazel_rules_docker//toolchains/docker:toolchain.bzl",
    docker_toolchain_configure="toolchain_configure"
)

# Force Docker toolchain to use 'which' to find Docker binary.
docker_toolchain_configure(
  name = "docker_config",
)

load(
    "@io_bazel_rules_docker//repositories:repositories.bzl",
    container_repositories = "repositories",
)

container_repositories()

load("@io_bazel_rules_docker//repositories:deps.bzl", container_deps = "deps")

container_deps()

load("@io_bazel_rules_docker//repositories:pip_repositories.bzl", "pip_deps")

pip_deps()

load(
    "@io_bazel_rules_docker//container:container.bzl",
    "container_pull",
)

container_pull(
    name = "nodejs_base",
    # This digest is for tag "12.18.4".
    digest = "sha256:7f35eaf7c26a25056a43777fff187fd590662fa5564b3cbb665ee253c4da7604",
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

# Postgres

container_pull(
    name = "postgres",
    # This digest is for tag "12.4".
    digest = "sha256:b0cfe264cb1143c7c660ddfd5c482464997d62d6bc9f97f8fdf3deefce881a8c",
    registry = "index.docker.io",
    repository = "library/postgres",
)

# Trino (used to be called Presto)

container_pull(
    name = "trino",
    # This digest is for tag "351".
    digest = "sha256:e97be2f4193b30cad176307665a7e334988059461f677d542ad8a62f68511133",
    registry = "index.docker.io",
    repository = "trinodb/trino",
)

# Gcloud SDK binaries.
load("//tools/gcloud:repository_rules.bzl", "gcloud_sdk")

gcloud_sdk(
    name = "gcloud_sdk",
)

# Go dependencies.

go_repository(
    name = "org_golang_x_text",
    importpath = "golang.org/x/text",
    sum = "h1:tW2bmiBqwgJj/UpqtC8EpXEZVYOwU0yG4iWbprSVAcs=",
    version = "v0.3.2",
)

container_pull(
    name = "debian_base",
    # docker manifest inspect index.docker.io/debian:bullseye
    digest = "sha256:c11d2593cb741ae8a36d0de9cd240d13518e95f50bccfa8d00a668c006db181e",
    registry = "index.docker.io",
    repository = "library/debian",
)
