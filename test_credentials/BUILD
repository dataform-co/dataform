package(default_visibility = ["//visibility:public"])

# Integration tests authenticate with Application Default Credentials (ADC); no secret key is
# stored in the repository. Before running integration tests locally, authenticate with:
#   gcloud auth application-default login
# bigquery.default.json (committed) targets the shared dataform-open-source project. To target
# your own GCP project or location, create a git-ignored bigquery.json next to it; it takes
# precedence when present. See contributing.md for details.
filegroup(
    name = "bigquery_credentials",
    testonly = 1,
    srcs = [
        "bigquery.default.json",
    ] + glob(
        ["bigquery.json"],
        allow_empty = True,
    ),
)
