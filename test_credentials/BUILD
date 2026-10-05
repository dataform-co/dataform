package(default_visibility = ["//visibility:public"])

# Default integration test configuration using Application Default Credentials (ADC).
# No secret key is stored here. Before running integration tests locally, authenticate with:
#   gcloud auth application-default login
# To override the target GCP project or location locally, create a git-ignored
# `test_credentials/bigquery.local.json` file or set the `DATAFORM_TEST_CREDENTIALS`
# environment variable. See contributing.md for details.
exports_files(["bigquery.json"])

filegroup(
    name = "bigquery_credentials",
    testonly = 1,
    srcs = [
        "bigquery.json",
    ] + glob(
        ["bigquery.local.json"],
        allow_empty = True,
    ),
)
