# Pub/Sub JSON data source for DLT

A Pub/sub subscription source implementation for [data load tool](https://dlthub.com/).
[All DLT destinations should be supported.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

✅ Automatically infer schema
✅ Dynamic table name based on a JSON property
✅ Configurable batch size and window in seconds
✅ Easily supports 200 messages/second throughput per worker on a GCP n1-standard-1 machine

## Configuration

Each configuration can be specified using a dlt config file, or the equivalent
environment var.

- `destination_name`: The output destination. [See here](https://dlthub.com/docs/dlt-ecosystem/destinations/)
- `dataset_name`: The output dataset name. Actual result on the database itself; i.e.
  with Postgres this is the schema.
- `max_bundle_size`: If the number of messages reaches this within one window_size_secs,
  will flush early. Keep in mind that this implementation can't support more than about
  500 messages per second. Therefore, If window_size_secs = 5, max_bundle_size should be
  about 5 \* 500 = 2500. This will avoid one worker hogging many messages without
  acking.
- `pubsub_input_subscription`: The input Pub/Sub subscription path
- `table_name_data_key`: The JSON data can contain a specific key `table_name_data_key`
  that will define the output table name. I.e. if `table_name_data_key=eventName`, the
  JSON data should be in the format of `{ "eventName": "something", ... }`. Such an
  event would be stored in a table named `something`.
- `table_name_prefix`: Prefix all table names with this string. I.e. if
  `table_name_prefix=raw_events`, the previous example would yield a table named
  `raw_events_something`
- window_size_secs: Flush all received messages every X seconds.

To configure your destination and its credentials, refer to DLT documentation.

## Example deployment as a GCP instance group

Here is a brief summary on how to deploy this streaming worker to a GCP instance group.

Replace values in {brackets} with your own!

### 1. Build and push the docker image

```bash

gcloud auth configure-docker {REGION}-docker.pkg.dev
docker build -t {REGION}-docker.pkg.dev/{PROJECT}/{REPOSITORY}/pubsub-dlt-stream:latest .
docker push {REGION}-docker.pkg.dev/{PROJECT}/{REPOSITORY}/pubsub-dlt-stream:latest

```

### 2. Create a private env file containing your config and secrets

{% note %}

**Note:** If you want to access one of your Cloud SQL database, the easiest way is to
enable the private IP. With the default network, your VMs will have authorization to
connect to your Cloud SQL instance.

{% endnote %}

Name this file `.pubsub-dlt-stream.env`:

```env

DESTINATION__POSTGRES__CREDENTIALS=postgresql://user:password@10.109.144.1:5432/db
DATASET_NAME=analytics
MAX_BUNDLE_SIZE=5000
PUBSUB_INPUT_SUBSCRIPTION=projects/{PROJECT}/subscriptions/{SUBSCRIPTION_NAME}
TABLE_NAME_DATA_KEY=eventName
TABLE_NAME_PREFIX=raw_events_
WINDOW_SIZE_SECS=5
```

### 3. Create an instance template

```bash

gcloud --project {PROJECT} compute instance-templates \
    create-with-container pubsub-dlt-stream \
    --container-image {REGION}-docker.pkg.dev/{PROJECT}/{REPOSITORY}/pubsub-dlt-stream \
    --container-env-file .pubsub-dlt-stream.env

```

### 4. Deploy a new instance group

```bash

gcloud --project {PROJECT} compute instance-groups \
    managed create pubsub-dlt-stream \
    --template pubsub-dlt-stream  \
    --size 2 \
    --region us-central1

```

You can play around the instance-group options to configure auto-scaling if you want to!
