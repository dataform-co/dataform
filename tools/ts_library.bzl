load("@aspect_rules_ts//ts:defs.bzl", native_ts_library = "ts_project")

def ts_library(**kwargs):
    native_ts_library(
        tsconfig = "//:tsconfig",
        declaration = True,
        source_map = True,
        **kwargs
    )
