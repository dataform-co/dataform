# Image Annotate

Use BigQuery ML to annotate images from a GCS bucket.

Images where annotation failed are filtered out, and retried in subsequent runs.

![Image Annotation DAG](./image-annotation-dag.png?raw=true "Image Annotation DAG")

<!-- TODO(ekrekr): add more info on setting up BigQuery connections. -->
