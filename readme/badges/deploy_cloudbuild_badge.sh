#!/bin/bash

gsutil mb -p tada-analytics gs://dataform-cloud-build-badges/

gsutil cp -r readme/badges/build gs://dataform-cloud-build-badges/

gsutil acl set public-read gs://dataform-cloud-build-badges/build/status.svg

gcloud functions deploy status --source=readme/badges/cloudfunction --runtime nodejs6 --trigger-resource cloud-builds --trigger-event google.pubsub.topic.publish
