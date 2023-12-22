try:
    import dotenv

    dotenv.load_dotenv()
except ImportError:
    pass

import threading
import time
import datetime
from typing import Any, Iterable, Optional, TypedDict
from collections import defaultdict, deque
from concurrent.futures import TimeoutError

import dlt
import orjson
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import FlowControl
from google.cloud.pubsub_v1.subscriber.message import Message
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture


DataT = dict[str, Any]
DATASET_NAME = dlt.config["dataset_name"]
DESTINATION_NAME = dlt.config["destination_name"]
MAX_BUNDLE_SIZE = dlt.config["max_bundle_size"]
PRIMARY_KEY_COLUMN_NAME = dlt.config["primary_key_column_name"]
PUBSUB_INPUT_SUBCRIPTION = dlt.config["pubsub_input_subscription"]
TABLE_NAME_DATA_KEY = dlt.config["table_name_data_key"] or None
TABLE_NAME_PREFIX = dlt.config["table_name_prefix"]
WINDOW_SIZE_SECS = dlt.config["window_size_secs"]


class StreamingPull(threading.Thread):
    def __init__(self, input_subscription: StreamingPullFuture):
        super().__init__()
        self.input_subscription = input_subscription
        self.messages: deque[Message] = deque()
        self.is_running = False
        self.pull_future: Optional[StreamingPullFuture]

    def _callback(self, message: Message):
        self.messages.append(message)

    def run(self):
        self.is_running = True
        subscriber = pubsub_v1.SubscriberClient()
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
        while time.monotonic() - started_at < timeout and len(self.messages) < max_size:
            try:
                message = self.messages.popleft()
                yield message
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

    @dlt.source
    def dlt_source(self):
        for table_name, msgs in self.messages_by_table_name.items():
            print(f"Loading {len(msgs)} for table {table_name}")
            print()
            yield dlt.resource(
                msgs,
                primary_key=PRIMARY_KEY_COLUMN_NAME,
                write_disposition="merge" if PRIMARY_KEY_COLUMN_NAME else "append",
                table_name=TABLE_NAME_PREFIX + table_name,
                name=table_name,
                columns={PRIMARY_KEY_COLUMN_NAME: dict(unique=True)}
                if PRIMARY_KEY_COLUMN_NAME
                else {},
            )

    def ack_bundle(self):
        print(f"Acking {len(self.messages_to_ack)} messages...")

        for msg in self.messages_to_ack:
            msg.ack()


class ParsedMessage(TypedDict):
    msg: Message
    data: DataT
    table_name: str


def parse_message(message: Message) -> ParsedMessage:
    data: DataT = orjson.loads(message.data)
    return {"msg": message, "data": data, "table_name": get_event_key(data)}


def get_event_key(data: DataT) -> str:
    if TABLE_NAME_DATA_KEY:
        return data.get(TABLE_NAME_DATA_KEY) or "unknown"
    else:
        return "default"


if __name__ == "__main__":
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="pubsub_dlt",
        destination=DESTINATION_NAME,
        dataset_name=DATASET_NAME,
    )

    pull = StreamingPull(PUBSUB_INPUT_SUBCRIPTION)
    pull.start()

    try:
        while True:
            bundle = pull.bundle(timeout=WINDOW_SIZE_SECS)
            load_info = pipeline.run(bundle.dlt_source())
            bundle.ack_bundle()

            # pretty print the information on data that was loaded
            print(load_info)
    finally:
        pull.stop()
