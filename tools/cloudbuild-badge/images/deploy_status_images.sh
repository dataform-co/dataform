#!/bin/bash

gsutil mb -p tada-analytics gs://dataform-cloud-build-badges/

gsutil cp -r tools/cloudbuild-badge/images gs://dataform-cloud-build-badges/

gsutil acl set public-read gs://dataform-cloud-build-badges/build/status.svg
