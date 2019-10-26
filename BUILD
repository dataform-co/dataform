package(default_visibility = ["//visibility:public"])

load("@build_bazel_rules_nodejs//:defs.bzl", "nodejs_binary")

exports_files([
    "tsconfig.json",
    "package.json",
    "common.package.json",
    "readme.md",
])

PROTOBUF_DEPS = [
    "@npm//protobufjs",
    # these deps are needed even though they are not automatic transitive deps of
    # protobufjs since if they are not in the runfiles then protobufjs attempts to
    # run `npm install` at runtime to get thhem which fails as it tries to access
    # the npm cache outside of the sandbox
    "@npm//semver",
    "@npm//chalk",
    "@npm//glob",
    "@npm//jsdoc",
    "@npm//minimist",
    "@npm//tmp",
    "@npm//uglify-js",
    "@npm//uglify-es",
    "@npm//espree",
    "@npm//escodegen",
    "@npm//estraverse",
]

nodejs_binary(
    name = "pbjs",
    data = PROTOBUF_DEPS,
    entry_point = "@npm//:node_modules/protobufjs/bin/pbjs",
    install_source_map_support = False,
)

nodejs_binary(
    name = "pbts",
    data = PROTOBUF_DEPS,
    entry_point = "@npm//:node_modules/protobufjs/bin/pbts",
    install_source_map_support = False,
)

nodejs_binary(
    name = "tslint",
    data = [
        "@npm//tslint",
    ],
    entry_point = "@npm//:node_modules/tslint/bin/tslint",
    install_source_map_support = False,
    templated_args = ["--node_options=--preserve-symlinks"],
)

load("@com_github_bazelbuild_buildtools//buildifier:def.bzl", "buildifier")

buildifier(
    name = "buildifier",
)

load("@io_bazel_rules_docker//docker/util:run.bzl", "container_run_and_commit")

load("@io_bazel_rules_docker//container:container.bzl", "container_layer", "container_image", "container_bundle", "container_push")

# The following defines our base builder image for the dataform repo.
# It creates a docker image with all dependencies, and then runs
# bazel fetch ...
# on a version of the repository specified in the WORKSPACE file to pre cache
# bazel artifacts so they don't have to be downloaded and processed on every build.
# This also effectively populates the yarn global cache to speed up builds.

BUILDER_BAZEL_VERSION = "0.27.0"

# The base builder image with bazel and other deps installed.
container_run_and_commit(
    name = "builder_base",
    image = "@debian_base//image",
    commands = [
        "apt update",
        "apt install -yq --no-install-recommends libasound2 libgtk-3-0 libxss1 pkg-config zip g++ zlib1g-dev unzip python python3 libstdc++6 git wget ca-certificates patch",
        "wget https://github.com/bazelbuild/bazel/releases/download/{bazel_version}/bazel-{bazel_version}-installer-linux-x86_64.sh".format(bazel_version = BUILDER_BAZEL_VERSION),
        "chmod +x bazel-{bazel_version}-installer-linux-x86_64.sh".format(bazel_version = BUILDER_BAZEL_VERSION),
        "./bazel-{bazel_version}-installer-linux-x86_64.sh".format(bazel_version = BUILDER_BAZEL_VERSION)
    ]
)

# The builder image with the locked repo archive pulled in.
container_image(
    name = "builder_with_repo",
    base = ":builder_base",
    files = ["@builder_checkpoint_repo//file"],
    directory = "/workspace-checkpoint-archive"
)

# The final image, where we warm up bazel on the extracted archive.
container_run_and_commit(
    name = "builder",
    image = ":builder_with_repo.tar",
    commands = [
        # Cloud build uses the root /workspace for actual builds.
        "mkdir /workspace-checkpoint",
        "tar xzf /workspace-checkpoint-archive/downloaded -C /workspace-checkpoint",
        # There is an archive prefix (subfolder), which we can cd straight into.
        "cd /workspace-checkpoint/*/",
        # TODO: Can remove this once we lock to a commit after this flag is set.
        "bazel fetch ... --incompatible_disable_deprecated_attr_params=false"
    ]
)

container_push(
    name = "builder.push",
    format = "Docker",
    image = ":builder_final",
    registry = "gcr.io",
    repository = "tada-analytics/builder-dataform",
    tag = "latest",
)
