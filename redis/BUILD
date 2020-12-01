package(default_visibility = ["//visibility:public"])

load("@df//testing:index.bzl", "ts_test_suite")
load("@df//tools:ts_library.bzl", "ts_library")

genrule(
    name = "redis",
    srcs = ["@redis-source//file"],
    outs = [
        "redis-server",
    ],
    cmd = """
        tmpdir="redis.tmp"
        source_file="$(SRCS)"
        parallel="$$(getconf _NPROCESSORS_ONLN || echo 1)"
        output_location="$(location redis-server)"

        mkdir $${tmpdir}
        tar -xzf $${source_file} -C $${tmpdir} --strip-components 1 --no-same-owner
        chmod +x $${tmpdir}/deps/jemalloc/configure
        make -s -C $${tmpdir} -j $${parallel} V=0 CFLAGS="$${CFLAGS-} -DLUA_USE_MKSTEMP -Wno-pragmas -Wno-empty-body"
        mv $${tmpdir}/src/redis-server $${output_location}
        chmod +x $${output_location}
        rm -r -f -- $${tmpdir} $${source_file}
    """,
    visibility = ["//visibility:public"],
)

ts_library(
    name = "cache",
    srcs = glob(
        ["*.ts"],
        exclude = ["*.spec.ts"],
    ),
    deps = [
        "//common/promises",
        "//common/protos",
        "//common/strings",
        "@npm//@types/bunyan",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//@types/redis",
        "@npm//bunyan",
        "@npm//handy-redis",
        "@npm//protobufjs",
        "@npm//redis",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = glob(["**/*.spec.ts"]),
    data = [
        ":redis",
        "@npm//source-map-support",
    ],
    templated_args = ["--node_options=--require=source-map-support/register"],
    deps = [
        ":cache",
        "//common/promises",
        "//common/protos",
        "//protos:ts",
        "//testing",
        "@npm//@types/chai",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//@types/redis",
        "@npm//chai",
        "@npm//handy-redis",
        "@npm//protobufjs",
        "@npm//redis",
    ],
)
