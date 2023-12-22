# Pub/Sub JSON data source for DLT

A Pub/sub subscription source implementation for [data load tool](https://dlthub.com/).
[All DLT destinations should be supported.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

- ✅ Automatically infer schema
- ✅ Dynamic table name based on a JSON property
- ✅ Configurable batch size and window in seconds
- ✅ Easily supports 250 messages/second throughput per worker on a GCP n1-standard-1 machine

## Example usage

We are using this project in production to stream analytics events from our server into
our application database in near-realtime. Here is an example of the server-side code
you can use to publish messages to Pub/Sub.

### Events generation code (Typescript)

```typescript
import { PubSub } from "@google-cloud/pubsub";

import {
  GOOGLE_CLOUD_PROJECT,
  PUBSUB_EVENTS_TOPIC_NAME,
  GCP_KEY_FILENAME,
} from "../../middleware/env";

const pubsub = new PubSub({
  keyFilename: GCP_KEY_FILENAME,
  projectId: GOOGLE_CLOUD_PROJECT,
});
const eventsTopic = pubsub.topic(
  `projects/${GOOGLE_CLOUD_PROJECT}/topics/${PUBSUB_EVENTS_TOPIC_NAME}`
);

const sendEvent = async (data: object & { eventName: string }) => {
  try {
    await eventsTopic.publishMessage({
      json: { timestamp: Date.now(), ...data },
    });
  } catch (e: unknown) {
    console.log(`Error while sending event: ${e}`);
  }
};
```

### Output table format

Say you are using the above code and a running worker streaming data to your PostgreSQL database. If you input:

```typescript
await sendEvent({
  id: uuidv4(),
  publishedAt: new Date(),
  eventName: "tasks",
  tookMs: 1322,
  userId: "abcdefg",
});
```

Using

- `dataset_name='analytics'`,
- `table_name_data_key='eventName'`,
- `primary_key_column_name='id'`,
- and `table_name_prefix='raw_events_'`,

this will create a postgres `analytics.raw_events_tasks` table with the following
schema:

```
         Column         |           Type           | Collation | Nullable | Default
------------------------+--------------------------+-----------+----------+---------
 published_at           | timestamp with time zone |           |          |
 id                     | character varying        |           | not null |
 event_name             | character varying        |           |          |
 took_ms                | double precision         |           |          |
 user_id                | character varying        |           |          |
 _dlt_load_id           | character varying        |           | not null |
 _dlt_id                | character varying        |           | not null |
Indexes:
    "raw_events_tasks__dlt_id_key" UNIQUE CONSTRAINT, btree (_dlt_id)
    "raw_events_tasks_id_key" UNIQUE CONSTRAINT, btree (id)

```

And it will insert the incoming messages with that `eventName` in that table!

## Configuration

Each configuration can be specified using a dlt config file, or the equivalent
environment var.

- `destination_name`: The output destination. [See DLT-supported destinations here](https://dlthub.com/docs/dlt-ecosystem/destinations/)
- `dataset_name`: The output dataset name. Actual result on the database itself; i.e.
  with Postgres this is the schema.
- `max_bundle_size`: If the number of messages reaches this within one window_size_secs,
  will flush early. Keep in mind that this implementation can't support more than about
  300 messages per second. Therefore, If window_size_secs = 5, max_bundle_size should be
  about 5 \* 300 = 1500. This will avoid one worker hogging many messages without
  acking.
- `primary_key_column_name`: The primary key column name to deduplicate incoming events
- `pubsub_input_subscription`: The input Pub/Sub subscription path
- `table_name_data_key`: The JSON data can contain a specific key `table_name_data_key`
  that will define the output table name. I.e. if `table_name_data_key=eventName`, the
  JSON data should be in the format of `{ "eventName": "something", ... }`. Such an
  event would be stored in a table named `something`.
- `table_name_prefix`: Prefix all table names with this string. I.e. if
  `table_name_prefix=raw_events_`, the previous example would yield a table named
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

> **Note:** If you want to access one of your Cloud SQL database, the easiest way is to
> enable the private IP. With the default network, your VMs will have authorization to
> connect to your Cloud SQL instance.

Name this file `.pubsub-dlt-stream.env`:

```env

DESTINATION_NAME=postgres
DESTINATION__POSTGRES__CREDENTIALS=postgresql://user:password@10.109.144.1:5432/db
DATASET_NAME=analytics
WINDOW_SIZE_SECS=5
MAX_BUNDLE_SIZE=5000
PUBSUB_INPUT_SUBSCRIPTION=projects/{PROJECT}/subscriptions/{SUBSCRIPTION_NAME}
PRIMARY_KEY_COLUMN_NAME=id
TABLE_NAME_DATA_KEY=eventName
TABLE_NAME_PREFIX=raw_events_
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

You can play around the instance-group options to configure auto-scaling if you want to.
