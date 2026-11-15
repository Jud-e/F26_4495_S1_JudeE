"""
Device isolation: which devices the gateway must refuse to accept data from.

The dashboard publishes isolate / restore commands to the Kafka topic
`iomt.actions`, keyed by device_id:

    {"device_id": "infusion-pump-001", "isolated": true, "by": "admin", "at": "..."}

The topic is compacted, so Kafka keeps the latest command per device forever.
On startup the gateway replays it from the beginning to rebuild the set of
isolated devices, then keeps following it for new commands.

Safety choice: until the replay has caught up, `registry.ready` is False and
the gateway answers 503 instead of accepting data. Accepting during that
window could let an isolated device through right after a restart.
"""
import json
import os
import threading
import time
from collections import Counter

TOPIC_ACTIONS = os.getenv("TOPIC_ACTIONS", "iomt.actions")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


class IsolationRegistry:
    """Thread-safe set of isolated device IDs, updated by the Kafka follower thread."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._isolated: set[str] = set()
        self._ready = False
        self.blocked: Counter = Counter()   # messages refused, per device (since start)

    @property
    def ready(self) -> bool:
        return self._ready

    def mark_ready(self) -> None:
        self._ready = True

    def apply(self, event: dict) -> None:
        """Apply one command. Malformed commands are ignored rather than crashing the follower."""
        device_id = event.get("device_id") if isinstance(event, dict) else None
        isolated = event.get("isolated") if isinstance(event, dict) else None
        if not isinstance(device_id, str) or not device_id or not isinstance(isolated, bool):
            return
        with self._lock:
            if isolated:
                self._isolated.add(device_id)
            else:
                self._isolated.discard(device_id)

    def is_isolated(self, device_id: str) -> bool:
        with self._lock:
            return device_id in self._isolated

    def count(self) -> int:
        with self._lock:
            return len(self._isolated)


registry = IsolationRegistry()


def follow(consumer, reg: IsolationRegistry, topic: str = TOPIC_ACTIONS, stop: threading.Event | None = None) -> None:
    """Replay `topic` from the beginning into `reg`, mark it ready once caught up, then keep following.

    `consumer` is a kafka-python KafkaConsumer created with no subscription; it is
    passed in so tests can supply a fake.
    """
    from kafka import TopicPartition

    stop = stop or threading.Event()
    partitions = None
    while partitions is None and not stop.is_set():
        partitions = consumer.partitions_for_topic(topic)
        if partitions is None:
            time.sleep(1)            # topic does not exist yet
    if stop.is_set():
        return

    tps = [TopicPartition(topic, p) for p in sorted(partitions)]
    consumer.assign(tps)
    consumer.seek_to_beginning(*tps)
    end = consumer.end_offsets(tps)  # where "caught up" is

    def caught_up() -> bool:
        return all(consumer.position(tp) >= end[tp] for tp in tps)

    while not stop.is_set():
        batches = consumer.poll(timeout_ms=1000)
        for records in batches.values():
            for record in records:
                reg.apply(record.value)
        if not reg.ready and caught_up():
            reg.mark_ready()
            print(f"isolation: caught up with '{topic}', {reg.count()} device(s) isolated")


def run_forever(reg: IsolationRegistry = registry) -> None:
    """Background thread entry point: connect to Kafka (retrying) and follow the actions topic."""
    from kafka import KafkaConsumer

    while True:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                enable_auto_commit=False,
                group_id=None,
            )
            follow(consumer, reg)
        except Exception as exc:     # Kafka unavailable, deserialisation error, etc.
            print(f"isolation: follower error ({exc}); retrying in 3s")
            time.sleep(3)