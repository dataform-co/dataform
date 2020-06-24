load("//tools/gcloud:secrets.bzl", "gcloud_secret")

package(default_visibility = ["//visibility:public"])

gcloud_secret(
    name = "bigquery.json",
    testonly = 1,
    ciphertext_file = ":bigquery.json.enc",
    project = "tada-analytics",
    key = "dataform-co-build",
    keyring = "dataform-co-build",
)

gcloud_secret(
    name = "redshift.json",
    testonly = 1,
    ciphertext_file = ":redshift.json.enc",
    project = "tada-analytics",
    key = "dataform-co-build",
    keyring = "dataform-co-build",
)

gcloud_secret(
    name = "snowflake.json",
    testonly = 1,
    ciphertext_file = ":snowflake.json.enc",
    project = "tada-analytics",
    key = "dataform-co-build",
    keyring = "dataform-co-build",
)

gcloud_secret(
    name = "sqldatawarehouse.json",
    testonly = 1,
    ciphertext_file = ":sqldatawarehouse.json.enc",
    project = "tada-analytics",
    key = "dataform-co-build",
    keyring = "dataform-co-build",
)
