package(default_visibility = ["//visibility:public"])

load("@npm//typedoc:index.bzl", "typedoc")

typedoc(
    name = "typedoc",
    outs = ["core.typedoc.json"],
    args = [
        "--json",
        "$(location :core.typedoc.json)",
        "--excludePrivate",
        "--excludeNotExported",
        "--ignoreCompilerErrors",
        "--entryPoint",
        "\"table\"",
        "--excludeExternals",
        "$(location //core:table.ts)",
        "$(location //core:assertion.ts)",
        "$(location //core:declaration.ts)",
        "$(location //core:operation.ts)",
        "$(location //core:test.ts)",
        "$(location //core:common.ts)",
    ],
    data = [
        "//:tsconfig.json",
        "//core:assertion.ts",
        "//core:common.ts",
        "//core:declaration.ts",
        "//core:files",
        "//core:operation.ts",
        "//core:table.ts",
        "//core:test.ts",
    ],
)
