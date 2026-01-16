import os
import json
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from elasticsearch import Elasticsearch

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_IN = os.getenv("TOPIC_IN", "frequency.telemetry")
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "frequency.alerts")
ES_URL = os.getenv("ES_URL", "http://localhost:9200")
ES_INDEX = os.getenv("ES_INDEX", "frequency-alerts")

NOMINAL_HZ = float(os.getenv("NOMINAL_HZ", "50.0"))
FREQ_LOW = float(os.getenv("FREQ_LOW", "49.90"))
FREQ_HIGH = float(os.getenv("FREQ_HIGH", "50.10"))
DEV_WARN = float(os.getenv("DEV_WARN", "0.02"))
DEV_CRIT = float(os.getenv("DEV_CRIT", "0.05"))
HARD_MIN = float(os.getenv("HARD_MIN", "45.0"))
HARD_MAX = float(os.getenv("HARD_MAX", "55.0"))
STALE_SEC = int(os.getenv("STALE_SEC", "60"))
COOLDOWN_SEC = int(os.getenv("COOLDOWN_SEC", "120"))

es = Elasticsearch(ES_URL)

producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    except NoBrokersAvailable:
        print("Kafka non pronto (producer), riprovo tra 3s...")
        time.sleep(3)

consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            TOPIC_IN,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id=os.getenv("GROUP_ID", "frequency-alert-engine"),
        )
    except NoBrokersAvailable:
        print("Kafka non pronto (consumer), riprovo tra 3s...")
        time.sleep(3)

last_sent: dict[tuple[str, str], float] = {}

def parse_iso(ts: str | None) -> datetime | None:
    if not ts or not isinstance(ts, str):
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def clamp(n: int, lo: int = 1, hi: int = 5) -> int:
    return max(lo, min(hi, n))

def severity_from_absdev(abs_dev: float) -> int:
    if abs_dev >= DEV_CRIT:
        return 4
    if abs_dev >= DEV_WARN:
        return 2
    return 1

def detect(ev: dict) -> list[tuple[str, str, int, dict]]:
    alerts = []
    grid = ev.get("grid", "unknown")
    station = ev.get("n", ev.get("station", "unknown"))

    freq_raw = ev.get("frequency_hz")
    if freq_raw is None:
        return [("DATA_MISSING_FIELD", "Manca frequency_hz", 4, {"grid": grid, "station": station})]

    try:
        freq = float(freq_raw)
    except Exception:
        return [("DATA_BAD_TYPE", f"frequency_hz non numerico: {freq_raw}", 4, {"grid": grid, "station": station})]

    dev_raw = ev.get("deviation_hz")
    try:
        dev = float(dev_raw) if dev_raw is not None else (freq - NOMINAL_HZ)
    except Exception:
        dev = freq - NOMINAL_HZ

    abs_dev = abs(dev)

    if freq < HARD_MIN or freq > HARD_MAX:
        alerts.append(("FREQ_OUTLIER_HARD", f"Frequenza impossibile: {freq} Hz", 5,
                       {"grid": grid, "station": station, "frequency_hz": freq, "deviation_hz": dev, "abs_deviation_hz": abs_dev}))
        return alerts

    if freq < FREQ_LOW or freq > FREQ_HIGH:
        sev = max(3, severity_from_absdev(abs_dev))
        alerts.append(("FREQ_OUT_OF_BAND", f"Fuori banda [{FREQ_LOW}, {FREQ_HIGH}] Hz: {freq}", sev,
                       {"grid": grid, "station": station, "frequency_hz": freq, "deviation_hz": dev, "abs_deviation_hz": abs_dev}))

    if abs_dev >= DEV_CRIT:
        alerts.append(("FREQ_DEVIATION_CRIT", f"Deviazione critica: {dev} Hz", 4,
                       {"grid": grid, "station": station, "frequency_hz": freq, "deviation_hz": dev, "abs_deviation_hz": abs_dev}))
    elif abs_dev >= DEV_WARN:
        alerts.append(("FREQ_DEVIATION_WARN", f"Deviazione alta: {dev} Hz", 2,
                       {"grid": grid, "station": station, "frequency_hz": freq, "deviation_hz": dev, "abs_deviation_hz": abs_dev}))

    ev_ts = ev.get("@timestamp") or ev.get("ts")
    dt = parse_iso(ev_ts)
    if dt:
        age = (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds()
        if age > STALE_SEC:
            alerts.append(("STALE_EVENT", f"Evento vecchio di {int(age)}s (>{STALE_SEC}s)", 3,
                           {"grid": grid, "station": station, "event_timestamp": ev_ts, "age_sec": age}))

    return alerts

print("Frequency alert engine in ascolto... (CTRL+C per uscire)")
for msg in consumer:
    ev = msg.value
    grid = ev.get("grid", "unknown")
    station = ev.get("n", ev.get("station", "unknown"))

    now_iso = datetime.now(timezone.utc).isoformat()
    ev_ts_iso = ev.get("@timestamp") or now_iso

    for atype, reason, sev, extra in detect(ev):
        key = (str(station), atype)
        epoch = time.time()
        if key in last_sent and (epoch - last_sent[key]) < COOLDOWN_SEC:
            continue
        last_sent[key] = epoch

        alert = {
            "@timestamp": ev_ts_iso,
            "alert_generated_at": now_iso,
            "event_type": "grid_alert",
            "detector": "python_rules",
            "alert_type": atype,
            "reason": reason,
            "severity": clamp(int(sev)),
            "grid": grid,
            "station": station,
            "source": ev.get("source"),
            "raw": ev,
            **extra,
        }

        producer.send(TOPIC_ALERTS, alert)
        try:
            es.index(index=ES_INDEX, document=alert)
        except Exception as e:
            print("Elastic error:", e)

        print(f"ALERT sev={alert['severity']} {atype} {grid}/{station} - {reason}")
