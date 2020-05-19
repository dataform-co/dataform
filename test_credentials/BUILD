package(default_visibility = [
    "//visibility:public",
])

package(default_visibility = ["//tests:__subpackages__"])

load("//tools/gcloud:secrets.bzl", "gcloud_secret")

gcloud_secret(
    name = "bigquery.json",
    ciphertext_file = ":bigquery.json.enc",
    key = "dataform-co-build",
    keyring = "dataform-co-build",
)

gcloud_secret(
    name = "redshift.json",
    ciphertext_file = ":redshift.json.enc",
    key = "dataform-co-build",
    keyring = "dataform-co-build",
)

gcloud_secret(
    name = "snowflake.json",
    ciphertext_file = ":snowflake.json.enc",
    key = "dataform-co-build",
    keyring = "dataform-co-build",
)

gcloud_secret(
    name = "sqldatawarehouse.json",
    ciphertext_file = ":sqldatawarehouse.json.enc",
    key = "dataform-co-build",
    keyring = "dataform-co-build",
)
