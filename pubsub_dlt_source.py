try:
    import dotenv

    dotenv.load_dotenv()
except ImportError:
    pass

import requests
import threading
import time
import datetime
from typing import Any, Dict, Iterable, Optional, TypedDict
from collections import defaultdict, deque
from concurrent.futures import TimeoutError

import dlt
from dlt.common.schema import TTableSchema
from dlt.common.typing import TDataItems
from dlt.common.json import json
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import FlowControl
from google.cloud.pubsub_v1.subscriber.message import Message
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture


DataT = dict[str, Any]
DATASET_NAME = dlt.config["dataset_name"]
DESTINATION_NAME = dlt.config["destination_name"]
MAX_BUNDLE_SIZE = dlt.config["max_bundle_size"]
PUBSUB_INPUT_SUBCRIPTION = dlt.config.get("pubsub_input_subscription", None)
PUBSUB_INPUT_TOPIC = dlt.config.get("pubsub_input_topic", None)

TABLE_NAME_DATA_KEY = dlt.config["table_name_data_key"] or None
TABLE_NAME_PREFIX = dlt.config["table_name_prefix"]
WINDOW_SIZE_SECS = float(dlt.config["window_size_secs"])


class StreamingPull(threading.Thread):
    def __init__(self, input_subscription: Optional[str] = None, input_topic: Optional[str] = None):
        super().__init__()
        self.input_subscription = input_subscription
        self.input_topic = input_topic

        self.messages: deque[Message] = deque()
        self.is_running = False
        self.pull_future: Optional[StreamingPullFuture] = None

    def _callback(self, message: Message):
        self.messages.append(message)

    def run(self):
        self.is_running = True
        subscriber = pubsub_v1.SubscriberClient()

        if not self.input_subscription and not self.input_topic:
            raise ValueError("Either input_subscription or input_topic must be provided")

        if not self.input_subscription:
            # Create a temporary subscription from the input topic
            subscriber_path = subscriber.subscription_path(
                self.input_topic.split("/")[1],  # project_id 
                f"temp-sub-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}"
            )
            self.input_subscription = subscriber_path

            # Try to create the topic if it doesn't exist
            try:
                publisher = pubsub_v1.PublisherClient()
                publisher.create_topic(request={"name": self.input_topic})
            except Exception as e:
                # Topic may already exist or we don't have permission - continue silently
                print(f"Error creating topic: {e}")
            
            subscription = subscriber.create_subscription(
                request={
                    "name": subscriber_path,
                    "topic": self.input_topic,
                    "expiration_policy": {"ttl": {"seconds": 3600}},  # auto-delete after 1h
                }
            )
            self.input_subscription = subscription.name
        
        print(f"Subscribing to {self.input_subscription}")
        self.pull_future = subscriber.subscribe(
            self.input_subscription,
            callback=self._callback,
            flow_control=FlowControl(max_messages=MAX_BUNDLE_SIZE * 2),
        )
        with subscriber:
            while self.is_running:
                try:
                    self.pull_future.result()
                except TimeoutError:
                    self.stop()
                except BaseException:
                    self.stop()
                    raise

    def stop(self):
        if self.pull_future:
            self.pull_future.cancel()
            self.pull_future.result()  # Block until the shutdown is complete.
            self.is_running = False

    def bundle(self, timeout: float):
        return MessageBundle(self.consume(timeout=timeout, max_size=MAX_BUNDLE_SIZE))

    def consume(self, timeout: float, max_size: int):
        started_at = time.monotonic()
        size = 0
        while time.monotonic() - started_at < timeout and size < max_size:
            try:
                message = self.messages.popleft()
                yield message
                size += 1
            except IndexError:
                time.sleep(timeout / 10)


class MessageBundle:
    def __init__(self, messages: Iterable[Message]):
        self.messages_by_table_name: dict[str, deque[DataT]] = defaultdict(deque)
        self.messages_to_ack: deque[Message] = deque()
        self.min_ts: Optional[int] = None
        self.max_ts: Optional[int] = None

        for msg in messages:
            parsed_msg = parse_message(msg)
            self.messages_by_table_name[parsed_msg["table_name"]].append(
                parsed_msg["data"]
            )
            self.messages_to_ack.append(msg)

    def __len__(self):
        return len(self.messages_to_ack)


    def ack_bundle(self):
        print(f"Acking {len(self.messages_to_ack)} messages...")

        for msg in self.messages_to_ack:
            msg.ack()

@dlt.source
def bundle_source(bundle: MessageBundle):
    for table_name, msgs in bundle.messages_by_table_name.items():
        print(f"Loading {len(msgs)} for table {table_name}")
        yield dlt.resource(
            msgs,
            write_disposition="append",
            table_name=TABLE_NAME_PREFIX + table_name,
            name=table_name,
        )

class ParsedMessage(TypedDict):
    msg: Message
    data: DataT
    table_name: str


def parse_message(message: Message) -> ParsedMessage:
    data: DataT = json.loadb(message.data)
    return {"msg": message, "data": data, "table_name": pop_event_key(data)}

def pop_event_key(data: DataT) -> str:
    if TABLE_NAME_DATA_KEY:
        return data.pop(TABLE_NAME_DATA_KEY, None) or "unknown"
    else:
        return "default"


def main():
    print("Starting pubsub_dlt_source.py")

    destination = CUSTOM_DESTINATIONS[DESTINATION_NAME]() if DESTINATION_NAME in CUSTOM_DESTINATIONS else DESTINATION_NAME

    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="pubsub_dlt",
        destination=destination,
        dataset_name=DATASET_NAME,
    )

    pull = StreamingPull(input_subscription=PUBSUB_INPUT_SUBCRIPTION, input_topic=PUBSUB_INPUT_TOPIC)
    pull.start()

    try:
        no_messages_count = 0
        while pull.is_running:
            bundle = pull.bundle(timeout=WINDOW_SIZE_SECS)
            if len(bundle):
                no_messages_count = 0
                load_info = pipeline.run(bundle_source(bundle))
                bundle.ack_bundle()
            else:
                no_messages_count += 1

                if no_messages_count * WINDOW_SIZE_SECS > 120:
                    print(f"No messages received in the last 2 minutes")
                    no_messages_count = 0

    finally:
        print("Exiting pubsub_dlt_source.py")
        pull.stop()


DATASET_SCHEMAS: Dict[str, Dict[str, str]] = {}

def get_shaped_schema(dataset_name: str, api_key: str) -> str:
    if dataset_name in DATASET_SCHEMAS:
        return DATASET_SCHEMAS[dataset_name]

    print(f"Getting schema for dataset {dataset_name}")
    resp = requests.get(f"https://api.shaped.ai/v1/datasets/{dataset_name}", headers={"x-api-key": api_key})
    DATASET_SCHEMAS[dataset_name] = resp.json()['dataset_schema']
    return DATASET_SCHEMAS[dataset_name]

@dlt.destination(batch_size=MAX_BUNDLE_SIZE, loader_file_format="typed-jsonl", name="shaped_custom_dataset")
def shaped_custom_dataset(items: TDataItems, table: TTableSchema, api_key: str = dlt.secrets.value) -> None:
    table_name = table.get('name')
    schema = get_shaped_schema(table_name, api_key)

    # Only include columns that are in the schema, else shaped will throw an error
    data = json.dumpb({
        "data": [{k: v for k, v in i.items() if k in schema} for i in items],
    })

    resp = requests.post(f"https://api.shaped.ai/v1/datasets/{table_name}/insert", headers={"x-api-key": api_key, 'Content-Type': 'application/json',}, data=data)

    if (resp.status_code != 200):
        raise Exception(f"Failed to insert data into Shaped dataset {table_name}: {resp.text}")


CUSTOM_DESTINATIONS = {
    "shaped_custom_dataset": lambda: shaped_custom_dataset(api_key=dlt.config["shaped_api_key"]),
}

if __name__ == "__main__":
    main()
