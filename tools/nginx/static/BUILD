package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_docker//container:container.bzl", "container_image")

container_image(
    name = "static",
    base = "@nginx_base//image",
    data_path = "./",
    directory = "/etc/nginx/",
    files = [":nginx.conf"],
)
