# Energy Frequency

Pipeline di **streaming in tempo reale** per il monitoraggio della frequenza della rete elettrica europea (nominale **50 Hz**). I dati vengono raccolti da una sorgente pubblica, trasportati su Kafka, analizzati con regole, Spark Structured Streaming e un modello di Machine Learning, quindi indicizzati su Elasticsearch e visualizzati con Kibana.


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
          ├──► alert_engine.py (k3s) ──► frequency.alerts ──► Elasticsearch
          │
          ├──► Spark · alerts ────────► frequency.spark-alerts
          │
          ├──► Spark · parquet ───────► data/telemetry_parquet/ (storico)
          │
          └──► Spark · ML inference ──► frequency.ml-alerts ──► ml-indexer ──► Elasticsearch
```

1. **Ingestione**: Logstash interroga la sorgente `dat.netzfrequenzmessung.de` e pubblica le misure su Kafka.
2. **Rilevamento anomalie** su tre livelli paralleli:
   - regole a soglia in Python (`alert_engine.py`);
   - regole a soglia con Spark Structured Streaming;
   - classificatore di anomalie addestrato (Spark ML).
3. **Archiviazione**: gli alert vengono indicizzati su Elasticsearch, la telemetria grezza viene salvata in formato Parquet.
4. **Visualizzazione**: dashboard Kibana e Kafka UI per l'ispezione dei topic.

## Tecnologie

| Ambito | Strumenti |
|---|---|
| Messaging | Apache Kafka 7.6.1 (Confluent), Zookeeper |
| Ingestione | Logstash |
| Stream processing | Apache Spark 3.5.1 (Structured Streaming, MLlib) |
| Storage e ricerca | Elasticsearch 8.12.2, Parquet |
| Visualizzazione | Kibana 8.12.2, Kafka UI |
| Linguaggio | Python 3.11 (`kafka-python`, `elasticsearch`) |
| Orchestrazione | Docker Compose, Kubernetes |

## Requisiti

- [docker-compose](https://docs.docker.com/compose/install/)
- [k3s](https://k3s.io/) (Kubernetes leggero) per eseguire `alert_engine.py`
- Almeno **4 GB di RAM** liberi per lo stack (Kafka, Elasticsearch, Logstash, Spark)
- Python 3.11+ (solo per provare `alert_engine.py` in locale, senza k3s)

## Avvio rapido

**1. Clona il repository**

```bash
git clone https://github.com/Giovy04/Energy_frequency.git
cd Energy_frequency
```

**2. Avvia lo stack con docker-compose**

```bash
docker-compose up -d --build
```

I topic Kafka vengono creati automaticamente dal servizio `kafka-init`.

**3. Avvia l'alert engine su k3s**

```bash
kubectl -n frequency get pods
kubectl -n frequency get deploy
kubectl -n frequency scale deploy/frequency-alert-engine --replicas=1
```

**4. Verifica**

- Apri **Kafka UI** su <http://localhost:8080> per vedere i messaggi nei topic.
- Apri **Kibana** su <http://localhost:5601> e crea i data view per gli indici `frequency-alerts` e `frequency-ml-alerts`.

**5. Arresto**

```bash
kubectl -n frequency scale deploy/frequency-alert-engine --replicas=0   # ferma l'alert engine
docker-compose down                                                     # ferma lo stack, mantiene i volumi
docker-compose down -v                                                  # ferma lo stack ed elimina anche i dati
```

> Il servizio `spark-ml-inference` richiede un modello addestrato in `data/models/frequency_anomaly_clf`.
> <!-- TODO: indicare come generare/addestrare il modello (script e comando) -->

## Servizi e porte

| Servizio | Porta | Descrizione |
|---|---|---|
| Kafka (host) | `9092` | Broker per client esterni |
| Kafka UI | `8080` | Interfaccia web per i topic |
| Elasticsearch | `9200` | API REST |
| Kibana | `5601` | Dashboard |

## Topic Kafka

| Topic | Contenuto |
|---|---|
| `frequency.telemetry` | Misure grezze di frequenza |
| `frequency.alerts` | Alert generati da `alert_engine.py` |
| `frequency.spark-alerts` | Alert generati dai job Spark a soglia |
| `frequency.ml-alerts` | Alert generati dal modello ML |

## Logica di alerting

### Regole Python (`alert_engine.py`)

| Tipo di alert | Condizione | Severità |
|---|---|---|
| `FREQ_OUTLIER_HARD` | Frequenza fuori da 45–55 Hz (valore impossibile) | 5 |
| `FREQ_OUT_OF_BAND` | Frequenza fuori dalla banda 49,90–50,10 Hz | ≥ 3 |
| `FREQ_DEVIATION_CRIT` | Deviazione dal nominale ≥ 0,05 Hz | 4 |
| `FREQ_DEVIATION_WARN` | Deviazione dal nominale ≥ 0,02 Hz | 2 |
| `STALE_EVENT` | Evento più vecchio di 60 s | 3 |
| `DATA_MISSING_FIELD` / `DATA_BAD_TYPE` | Campo `frequency_hz` assente o non numerico | 4 |

Per evitare spam, lo stesso alert per la stessa stazione non viene ripetuto prima del periodo di **cooldown** (120 s di default).

### Spark e Machine Learning

- **`spark-alerts`**: soglie di deviazione `WARN_HZ=0.02` e `CRIT_HZ=0.05`.
- **`spark-ml-inference`**: classificatore con soglie di probabilità `PROB_WARN=0.60` e `PROB_CRIT=0.85`.
- **`spark-parquet`**: salva la telemetria in `data/telemetry_parquet/frequency` per analisi e training successivi.

## Configurazione

Tutti i parametri si impostano tramite variabili d'ambiente. Per l'alert engine su k3s vanno definite nella sezione `env` del manifest in `k8s/`.

**`alert_engine.py`**

| Variabile | Default | Descrizione |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Indirizzo del broker |
| `TOPIC_IN` | `frequency.telemetry` | Topic di input |
| `TOPIC_ALERTS` | `frequency.alerts` | Topic di output |
| `ES_URL` | `http://localhost:9200` | Endpoint Elasticsearch |
| `ES_INDEX` | `frequency-alerts` | Indice degli alert |
| `NOMINAL_HZ` | `50.0` | Frequenza nominale |
| `FREQ_LOW` / `FREQ_HIGH` | `49.90` / `50.10` | Limiti della banda |
| `DEV_WARN` / `DEV_CRIT` | `0.02` / `0.05` | Soglie di deviazione |
| `HARD_MIN` / `HARD_MAX` | `45.0` / `55.0` | Limiti fisici plausibili |
| `STALE_SEC` | `60` | Età massima di un evento |
| `COOLDOWN_SEC` | `120` | Pausa tra alert identici |

**Logstash**: `POLL_INTERVAL` (default `10` secondi) e `GRIDFREQ_URL` (sorgente dati).

## Deploy dell'alert engine su k3s

Lo stack principale gira con docker-compose, mentre `alert_engine.py` viene eseguito come workload su **k3s**, nel namespace `frequency`. I manifest sono nella cartella `k8s/`.

```bash
# Stato di pod e deployment
kubectl -n frequency get pods
kubectl -n frequency get deploy

# Avvio (1 replica)
kubectl -n frequency scale deploy/frequency-alert-engine --replicas=1

# Arresto
kubectl -n frequency scale deploy/frequency-alert-engine --replicas=0

# Log in tempo reale
kubectl -n frequency logs -f deploy/frequency-alert-engine
```

Il pod deve raggiungere Kafka ed Elasticsearch, che girano sull'host tramite docker-compose. Nel manifest imposta gli endpoint di conseguenza:

```yaml
env:
  - name: KAFKA_BOOTSTRAP
    value: "<IP_HOST>:9092"
  - name: ES_URL
    value: "http://<IP_HOST>:9200"
```

> Kafka pubblicizza il listener esterno come `localhost:9092`, quindi un pod che si collega a `<IP_HOST>:9092` può ricevere dai metadati un indirizzo non raggiungibile. Se succede, esegui il pod con `hostNetwork: true` e usa `localhost` come endpoint.

## Autore

**Giovy04** · [github.com/Giovy04](https://github.com/Giovy04)
