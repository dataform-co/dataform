package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_docker//container:container.bzl", "container_image")

container_image(
    name = "spa",
    base = "@nginx_base//image",
    data_path = "/tools/nginx/spa",
    directory = "/etc/nginx/",
    files = [":nginx.conf"],
)
