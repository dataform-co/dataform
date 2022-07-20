load("@io_bazel_rules_docker//docker/util:run.bzl", "container_run_and_commit")
load("@io_bazel_rules_docker//container:container.bzl", "container_image", "container_push")

# The following defines our base builder image for the dataform repo.

# The base builder image with bazel and other deps installed.
container_run_and_commit(
    name = "builder",
    commands = [
        "apt update",
        "apt install -yq --no-install-recommends build-essential ca-certificates apt-transport-https wget",
        "update-ca-certificates",
        "wget https://github.com/bazelbuild/bazelisk/releases/download/v1.2.1/bazelisk-linux-amd64 -O /usr/local/bin/bazel",
        "chmod +x /usr/local/bin/bazel"
    ],
    image = "@debian_base//image",
    tags = [
        "no-remote",
    ],
)

container_image(
    name = "builder_wrapper",
    base = ":builder_commit.tar",
    tags = [
        "no-remote",
    ],
)

container_push(
    name = "builder.push",
    format = "Docker",
    image = ":builder_wrapper",
    registry = "gcr.io",
    repository = "dataform-public/dataform-builder",
    tag = "latest",
)
