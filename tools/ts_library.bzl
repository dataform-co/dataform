load("@aspect_rules_ts//ts:defs.bzl", "ts_project")

def ts_library(**kwargs):
    ts_project(
        tsconfig = "//:tsconfig",
        declaration = True,
        source_map = True,
        **kwargs
    )
