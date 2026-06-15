PROJECT_ID=swalk-composer-1
AR_REPOSITORY=op-dataform-dev

bazel run @nodejs//:npm config set //us-central1-npm.pkg.dev/$PROJECT_ID/$AR_REPOSITORY/:_authToken "$(gcloud auth print-access-token)"

# Core
gcloud artifacts tags delete latest --package @dataform/core --repository=$AR_REPOSITORY --location=us-central1 --project=$PROJECT_ID --quiet
gcloud artifacts versions delete 3.0.59 --package @dataform/core --repository=$AR_REPOSITORY --location=us-central1 --project=$PROJECT_ID --quiet
bazel run //packages/@dataform/core:package.publish -- --registry=https://us-central1-npm.pkg.dev/$PROJECT_ID/$AR_REPOSITORY/

# Extension
gcloud artifacts tags delete latest --package @dataform/op-to-dataform --repository=$AR_REPOSITORY --location=us-central1 --project=$PROJECT_ID --quiet
gcloud artifacts versions delete 3.0.59 --package @dataform/op-to-dataform --repository=$AR_REPOSITORY --location=us-central1 --project=$PROJECT_ID --quiet
bazel run //packages/op-to-dataform:package.publish -- --registry=https://us-central1-npm.pkg.dev/$PROJECT_ID/$AR_REPOSITORY/
