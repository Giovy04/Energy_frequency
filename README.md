# Energy Frequency

Pipeline di **streaming in tempo reale** per il monitoraggio della frequenza della rete elettrica europea (nominale 50 Hz). I dati vengono raccolti da una sorgente pubblica, trasportati su Kafka, analizzati con regole e con un modello di Machine Learning, quindi indicizzati su Elasticsearch e visualizzati con Kibana.

## Architettura

```
 netzfrequenzmessung.de
          │  (polling ogni 10 s)
          ▼
      Logstash ──────────────► Elasticsearch ──► Kibana
          │
          ▼
  Kafka: frequency.telemetry
          │
          ├──► alert_engine.py (Kubernetes) ──► frequency.alerts ──► Elasticsearch
          ├──► Spark · alerts ────────► frequency.spark-alerts
          ├──► Spark · parquet ───────► data/telemetry_parquet/ (storico)
          └──► Spark · ML inference ──► frequency.ml-alerts ──► ml-indexer ──► Elasticsearch
```

## Tecnologie

Kafka 7.6.1 (Confluent) · Logstash · Spark 3.5.1 (PySpark, Structured Streaming, MLlib) · Elasticsearch / Kibana 8.12.2 · Python 3.11 · Java 17 · docker-compose · Kubernetes

## Requisiti

- docker-compose
- Un cluster Kubernetes (es. k3s) e `kubectl`, per l'alert engine
- Almeno 4 GB di RAM liberi

## Avvio rapido

**1. Clona il repository**

```bash
git clone https://github.com/Giovy04/Energy_frequency.git
cd Energy_frequency
```

**2. Avvia lo stack**

```bash
docker-compose up -d --build
```

I topic Kafka vengono creati dal servizio `kafka-init`.

**3. Avvia l'alert engine su Kubernetes**

```bash
kubectl create namespace frequency        # solo la prima volta
kubectl apply -f k8s/
kubectl -n frequency scale deploy/frequency-alert-engine --replicas=1
kubectl -n frequency get pods
```

**4. Verifica**

| Servizio | URL |
|---|---|
| Kafka UI | <http://localhost:8080> |
| Kibana | <http://localhost:5601> |
| Elasticsearch | <http://localhost:9200> |

In Kibana crea i data view per gli indici `frequency-alerts` e `frequency-ml-alerts`.

**5. Arresto**

```bash
kubectl -n frequency scale deploy/frequency-alert-engine --replicas=0
docker-compose down        # aggiungi -v per eliminare anche i dati
```

> Il servizio `spark-ml-inference` richiede un modello in `data/models/frequency_anomaly_clf`.

## Rilevamento anomalie

Tre rilevatori lavorano in parallelo sul topic `frequency.telemetry`:

| Rilevatore | Topic di output | Logica |
|---|---|---|
| `alert_engine.py` | `frequency.alerts` | Regole Python (tabella sotto) |
| `spark-alerts` | `frequency.spark-alerts` | Deviazione ≥ `WARN_HZ` (0,02 Hz, severità 2) o ≥ `CRIT_HZ` (0,05 Hz, severità 3) |
| `spark-ml-inference` | `frequency.ml-alerts` | Random Forest: alert se la misura è classificata anomala, severità 3 se la probabilità è ≥ `PROB_CRIT` (0,85), altrimenti 2 |

Inoltre `spark-parquet` salva la telemetria in `data/telemetry_parquet/frequency` (partizionata per `event_date`), usata per il training.

**Regole di `alert_engine.py`**

| Alert | Condizione | Severità |
|---|---|---|
| `FREQ_OUTLIER_HARD` | Frequenza fuori da 45–55 Hz | 5 |
| `FREQ_OUT_OF_BAND` | Frequenza fuori da 49,90–50,10 Hz | ≥ 3 |
| `FREQ_DEVIATION_CRIT` | Deviazione ≥ 0,05 Hz | 4 |
| `FREQ_DEVIATION_WARN` | Deviazione ≥ 0,02 Hz | 2 |
| `STALE_EVENT` | Evento più vecchio di 60 s | 3 |
| `DATA_MISSING_FIELD` / `DATA_BAD_TYPE` | `frequency_hz` assente o non numerico | 4 |

Lo stesso alert per la stessa stazione non viene ripetuto prima del cooldown (120 s).

## Addestramento del modello ML

Il modello è un Random Forest (Spark MLlib, 100 alberi) addestrato sullo storico Parquet. Le etichette sono *weak labels* generate da regole: una misura è anomala se la deviazione assoluta è ≥ `WARN_HZ` oppure se `|dt|` è ≥ `DT_ABS_STALE`. Lo script stampa l'AUC sul test set e salva il modello in `MODEL_OUT`.

**Prerequisiti**: lo stack deve aver accumulato abbastanza dati, con entrambe le classi (normale e anomala), altrimenti lo script termina con errore.

```bash
docker-compose run --rm --no-deps spark-parquet spark-submit /app/train_frequency_mllib.py
docker-compose restart spark-ml-inference
```

| Variabile | Default | Descrizione |
|---|---|---|
| `PARQUET_PATH` | `/data/telemetry_parquet/frequency` | Storico di input |
| `MODEL_OUT` | `/data/models/frequency_anomaly_clf` | Modello salvato |
| `WARN_HZ` | `0.02` | Soglia di deviazione per l'etichetta anomala |
| `DT_ABS_STALE` | `120` | Soglia di `\|dt\|` per l'etichetta anomala |
| `TEST_FRACTION` | `0.2` | Frazione di test |
| `SEED` | `42` | Seed |

## Configurazione dell'alert engine

Le variabili sono lette dal ConfigMap `frequency-config` (`k8s/`); quelle assenti usano il default del codice.

| Variabile | Default | Descrizione |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Broker Kafka |
| `TOPIC_IN` / `TOPIC_ALERTS` | `frequency.telemetry` / `frequency.alerts` | Topic di input e output |
| `ES_URL` / `ES_INDEX` | `http://localhost:9200` / `frequency-alerts` | Elasticsearch |
| `NOMINAL_HZ` | `50.0` | Frequenza nominale |
| `FREQ_LOW` / `FREQ_HIGH` | `49.90` / `50.10` | Banda ammessa |
| `DEV_WARN` / `DEV_CRIT` | `0.02` / `0.05` | Soglie di deviazione |
| `HARD_MIN` / `HARD_MAX` | `45.0` / `55.0` | Limiti fisici plausibili |
| `STALE_SEC` | `60` | Età massima di un evento |
| `COOLDOWN_SEC` | `120` | Pausa tra alert identici |

Dopo una modifica al ConfigMap:

```bash
kubectl apply -f k8s/
kubectl -n frequency rollout restart deploy/frequency-alert-engine
```

I parametri di Logstash (`POLL_INTERVAL`, `GRIDFREQ_URL`) e dei job Spark sono nel `docker-compose.yml`.

## Deploy dell'alert engine su Kubernetes

`alert_engine.py` gira come Deployment `frequency-alert-engine` nel namespace `frequency`. Il pod usa `hostNetwork: true`, quindi raggiunge Kafka ed Elasticsearch (esposti da docker-compose) tramite `localhost`.

```bash
kubectl -n frequency get pods                                           # stato
kubectl -n frequency logs -f deploy/frequency-alert-engine              # log
kubectl -n frequency scale deploy/frequency-alert-engine --replicas=0   # arresto
kubectl -n frequency scale deploy/frequency-alert-engine --replicas=1   # avvio
```

**Immagine**: il Deployment usa `frequency_alert-engine:latest` con `imagePullPolicy: IfNotPresent`. L'immagine non è inclusa nel repository e non viene scaricata da un registry: deve essere già presente sul nodo del cluster.

## Autore

**Giovy04** · [github.com/Giovy04](https://github.com/Giovy04)
