import os
import json
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from elastic_transport import TransportError


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_IN = os.getenv("TOPIC_IN", "frequency.ml-alerts")
ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
ES_INDEX = os.getenv("ES_INDEX", "frequency-ml-alerts")

GROUP_ID = os.getenv("KAFKA_GROUP_ID", "frequency-ml-indexer")
OFFSET_RESET = os.getenv("OFFSET_RESET", "latest")
BATCH_REFRESH_EVERY = int(os.getenv("REFRESH_EVERY", "200"))
POLL_MS = int(os.getenv("POLL_MS", "1000"))
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "200"))


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def make_es():
    return Elasticsearch(ES_URL)


def make_consumer():
    return KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset=OFFSET_RESET,
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        api_version_auto_timeout_ms=30000,
    )


def ensure_index(es: Elasticsearch):
    if not es.indices.exists(index=ES_INDEX):
        es.indices.create(
            index=ES_INDEX,
            mappings={
                "properties": {
                    "@timestamp": {"type": "date"},
                    "alert_generated_at": {"type": "date"},
                    "event_type": {"type": "keyword"},
                    "detector": {"type": "keyword"},
                    "alert_type": {"type": "keyword"},
                    "reason": {"type": "text"},
                    "severity": {"type": "integer"},
                    "grid": {"type": "keyword"},
                    "station": {"type": "keyword"},
                    "source": {"type": "keyword"},
                    "raw": {"type": "object", "enabled": True},
                }
            },
        )


def main():
    es = make_es()

    while True:
        try:
            es.info()
            break
        except Exception:
            time.sleep(1)

    ensure_index(es)

    consumer = make_consumer()

    print(
        f"[ml-indexer] boot {utc_now_iso()} topic={TOPIC_IN} index={ES_INDEX} "
        f"es={ES_URL} group={GROUP_ID} offset_reset={OFFSET_RESET}",
        flush=True,
    )
    print("[ml-indexer] consuming...", flush=True)

    indexed = 0
    last_heartbeat = time.time()

    while True:
        records = consumer.poll(timeout_ms=POLL_MS, max_records=MAX_RECORDS)

        if not records:
            if time.time() - last_heartbeat > 30:
                print("[ml-indexer] alive (no new messages)", flush=True)
                last_heartbeat = time.time()
            continue

        for tp, msgs in records.items():
            for msg in msgs:
                doc = msg.value
                doc_id = f"{msg.topic}-{msg.partition}-{msg.offset}"

                try:
                    es.index(index=ES_INDEX, id=doc_id, document=doc)
                    indexed += 1

                    if indexed % 20 == 0:
                        print(f"[ml-indexer] indexed={indexed} last_id={doc_id}", flush=True)

                    if BATCH_REFRESH_EVERY > 0 and indexed % BATCH_REFRESH_EVERY == 0:
                        es.indices.refresh(index=ES_INDEX)

                except TransportError as e:
                    print(f"[ml-indexer] ES error id={doc_id}: {e}", flush=True)
                    time.sleep(1)
                except Exception as e:
                    print(f"[ml-indexer] error id={doc_id}: {e}", flush=True)
                    time.sleep(1)

if __name__ == "__main__":
    main()
