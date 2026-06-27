load("@npm//@bazel/typescript:index.bzl", native_ts_library = "ts_library")

def ts_library(**kwargs):
    if "module_name" not in kwargs:
        package = native.package_name()
        kwargs["module_name"] = "df/" + package if package else "df"
    native_ts_library(
        devmode_target = "es2017",
        prodmode_target = "es2017",
        devmode_module = "commonjs",
        prodmode_module = "esnext",
        **kwargs
    )
