# TODO(ekrekr): update to image `gcr.io/cloud-builders/bazel` once we're at Bazel version 7+.
FROM gcr.io/cloud-builders/bazel:5.4.0

# Install apt packages.
RUN apt update
RUN apt install -yq build-essential cmake git ca-certificates apt-transport-https gnupg2 lsb-release wget curl python3 default-jre default-jdk

# Update certificates.
RUN update-ca-certificates

# Install bazelisk.
RUN wget https://github.com/bazelbuild/bazelisk/releases/download/v1.2.1/bazelisk-linux-amd64 -O /usr/bin/bazel
RUN chmod +x /usr/bin/bazel
