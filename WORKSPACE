workspace(name = "df")

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

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

git_repository(
    name = "com_google_sandboxed_api",
    commit = "19fd11b91e6678db2fcfe69dd4037605730f5317",
    remote = "https://github.com/google/sandboxed-api.git",
    shallow_since = "1610629882 -0800",
)

http_archive(
    name = "enum34_archive",
    build_file = "@com_google_sandboxed_api//sandboxed_api/bazel/external:enum34.BUILD",
    sha256 = "8ad8c4783bf61ded74527bffb48ed9b54166685e4230386a9ed9b1279e2df5b1",
    strip_prefix = "enum34-1.1.6",
    urls = ["https://files.pythonhosted.org/packages/bf/3e/31d502c25302814a7c2f1d3959d2a3b3f78e509002ba91aea64993936876/enum34-1.1.6.tar.gz"],
)

load("@com_google_sandboxed_api//sandboxed_api/bazel:sapi_deps.bzl", "sapi_deps")

sapi_deps()

http_archive(
    name = "net_zlib",
    build_file = "@com_google_sandboxed_api//sandboxed_api:bazel/external/zlib.BUILD",
    patch_args = ["-p1"],
    # This is a patch that removes the "OF" macro that is used in zlib function
    # definitions. It is necessary, because libclang, the library used by the
    # interface generator to parse C/C++ files contains a bug that manifests
    # itself with macros like this.
    # We are investigating better ways to avoid this issue. For most "normal"
    # C and C++ headers, parsing just works.
    patches = ["@com_google_sandboxed_api//sandboxed_api:bazel/external/zlib.patch"],
    sha256 = "c3e5e9fdd5004dcb542feda5ee4f0ff0744628baf8ed2dd5d66f8ca1197cb1a1",
    strip_prefix = "zlib-1.2.11",
    urls = [
        "https://mirror.bazel.build/zlib.net/zlib-1.2.11.tar.gz",
        "https://www.zlib.net/zlib-1.2.11.tar.gz",
    ],
)

maybe(
    http_archive,
    name = "com_google_googletest",
    sha256 = "a6ab7c7d6fd4dd727f6012b5d85d71a73d3aa1274f529ecd4ad84eb9ec4ff767",
    strip_prefix = "googletest-dcc92d0ab6c4ce022162a23566d44f673251eee4",
    urls = ["https://github.com/google/googletest/archive/dcc92d0ab6c4ce022162a23566d44f673251eee4.zip"],
)

maybe(
    http_archive,
    name = "com_google_benchmark",
    sha256 = "7f45be0bff07d787d75c3864212e9ea5ebba57593b2e487c783d11da70ef6857",
    strip_prefix = "benchmark-56898e9a92fba537671d5462df9c5ef2ea6a823a",
    urls = ["https://github.com/google/benchmark/archive/56898e9a92fba537671d5462df9c5ef2ea6a823a.zip"],
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

# Presto

container_pull(
    name = "presto",
    # This digest is for tag "340".
    digest = "sha256:21f9f92ddb232848ef56ef09a83d04f3d1c9a03539c20b0cb786e346797988bb",
    registry = "index.docker.io",
    repository = "prestosql/presto",
)

# Fetch Redis source (binaries are not distributed).
# Urls and hashes come from here: https://github.com/redis/redis-hashes/blob/master/README.
http_file(
    name = "redis-source",
    sha256 = "12ad49b163af5ef39466e8d2f7d212a58172116e5b441eebecb4e6ca22363d94",
    urls = ["http://download.redis.io/releases/redis-6.0.6.tar.gz"],
)

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
