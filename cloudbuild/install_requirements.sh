#!/bin/bash
set -e

apt update
apt install -yq build-essential cmake git ca-certificates apt-transport-https gnupg2 lsb-release wget curl python3 default-jre default-jdk

# Update certificates.
update-ca-certificates

# Install bazelisk.
wget https://github.com/bazelbuild/bazelisk/releases/download/v1.2.1/bazelisk-linux-amd64 -O /usr/bin/bazel
chmod +x /usr/bin/bazel
