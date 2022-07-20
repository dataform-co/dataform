load("@io_bazel_rules_docker//docker/util:run.bzl", "container_run_and_commit")
load("@io_bazel_rules_docker//container:container.bzl", "container_push")

# The following defines our base builder image for the dataform repo.
container_run_and_commit(
    name = "builder",
    commands = [
        # Install apt packages.
        "apt update",
        "apt install -yq --no-install-recommends build-essential cmake git ca-certificates apt-transport-https gnupg2 lsb-release wget curl python2 python3.9 default-jre default-jdk",
        # Update certificates.
        "update-ca-certificates",
        # Install docker.
        "mkdir -p /etc/apt/keyrings",
        "curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg",
        """echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null""",
        "apt update",
        "apt install -yq --no-install-recommends docker-ce docker-ce-cli containerd.io docker-compose-plugin",
        # Install bazelisk.
        "wget https://github.com/bazelbuild/bazelisk/releases/download/v1.2.1/bazelisk-linux-amd64 -O /usr/local/bin/bazel",
        "chmod +x /usr/local/bin/bazel",
        # Set python versions.
        "ln -sf /usr/bin/python2.7 /usr/bin/python",
        "ln -sf /usr/bin/python3.9 /usr/bin/python3"
    ],
    image = "@debian_base//image",
    tags = [
        "no-remote",
    ],
)

container_push(
    name = "builder.push",
    format = "Docker",
    image = ":builder_commit.tar",
    registry = "gcr.io",
    repository = "dataform-public/dataform-builder",
    tag = "latest",
)
