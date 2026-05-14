# NewsLake — Big Data Platform for Press Article Analytics

> **End-to-end Big Data platform** that automatically scrapes news articles from Moroccan and international media, stores them in a medallion-architecture data lake, transforms them into analytical tables, loads them into a data warehouse and exposes BI dashboards — fully containerised, orchestrated by Apache Airflow, observable via Prometheus + Grafana, and deployable on Kubernetes.

---

## 1. Project description

Media outlets publish thousands of articles every day. This platform exploits that stream to:

- identify **trending topics** (*tendances d'actualité*),
- analyse **dominant themes**,
- follow **events in near real-time**,
- give analysts a foundation for **fake-news detection**.

It implements every functional layer required by the project brief:

| Brief requirement | NewsLake component |
|---|---|
| Web scraping (BS4 / Scrapy) | `scraper/` (Python + BeautifulSoup + RSS) |
| Distributed architecture | Kafka + MinIO + Postgres + Airflow |
| Data Lake | MinIO (S3-compatible) — `bronze/`, `silver/`, `gold/` buckets |
| Medallion architecture | Bronze → Silver → Gold (Python transforms) |
| ETL / ELT | `transformer/` modules + Airflow DAG |
| Batch **and** streaming | Hourly DAG **+** continuous Kafka producer/consumer |
| Data quality & governance | DQ gates, rejection bucket, lineage timestamps |
| Visualisation | Metabase (BI) + Grafana (ops) |
| Containerised deployment | Single `docker-compose.yml` |
| K8s deployment (optional) | `k8s/` manifests, six namespaces |
| Monitoring (optional) | `docker-compose.monitoring.yml` |

---

## 2. Architecture overview

```
                 ┌──────────────────────────────────────────────────────────────┐
                 │                       INGESTION                              │
                 │  scraper (Python + BS4) ──► Kafka topic `raw_articles`       │
                 └────────────────────────────────┬─────────────────────────────┘
                                                  │ JSON events
                                                  ▼
              ┌──────────────────────────  DATA LAKE (MinIO)  ─────────────────────────────┐
              │                                                                            │
              │   bronze/source=…/dt=YYYY-MM-DD/<sha1>.json    (raw, untouched)            │
              │            │                                                               │
              │            │  process_bronze_silver.py                                     │
              │            │   - DQ gates (title / date / content length)                  │
              │            │   - HTML strip, NFKC normalize, language detect               │
              │            ▼                                                               │
              │   silver/source=…/dt=…/<sha1>.json             (clean, enriched)           │
              │            │                                                               │
              │            │  process_gold_dwh.py / build_gold()                           │
              │            ▼                                                               │
              │   gold/<table>.parquet                         (analytical tables)         │
              └──────────────────────────────────┬─────────────────────────────────────────┘
                                                 │ load_warehouse()
                                                 ▼
                ┌──────────────────────  DATA WAREHOUSE (Postgres)  ──────────────────────┐
                │  schema gold:                                                           │
                │   articles, articles_per_day, articles_per_source,                      │
                │   articles_per_theme, articles_per_country, top_keywords                │
                └────────────────────────────────────┬────────────────────────────────────┘
                                                     │
                              ┌──────────────────────┴──────────────────────┐
                              ▼                                             ▼
                    ┌──────────────────┐                          ┌────────────────────┐
                    │   Metabase BI    │                          │ Grafana (ops view) │
                    │  trends, themes, │                          │ Prometheus metrics │
                    │  keywords, KPIs  │                          │ container health   │
                    └──────────────────┘                          └────────────────────┘

                Cross-cutting:  Airflow (hourly DAG)   ·   Prometheus + Grafana monitoring
```

### Data flow

1. **Scraper** (continuous container) crawls Hespress (MA) and BBC News (intl.) every hour, publishing one JSON event per article to **Kafka** topic `raw_articles`.
2. **Bronze/Silver consumer** (continuous container) drains Kafka, lands raw events in `minio://newslake/bronze/`, applies **data-quality gates** (missing title / missing date / content too short → moved to `bronze/_rejected/`), then cleans (HTML strip, Unicode NFKC, URL strip, language detect) and writes to `minio://newslake/silver/`.
3. **Airflow** runs an `@hourly` DAG that triggers the same code paths in batch mode, then aggregates Silver into Gold parquet files and loads them into the **Postgres warehouse** (`gold` schema).
4. **Metabase** queries the warehouse to render dashboards: trending topics, articles per source, most frequent keywords, articles per theme/country.
5. **Prometheus + Grafana** scrape every container (Kafka, Postgres, MinIO, node, cAdvisor) and surface pipeline-health dashboards.

---

## 3. Technology stack

| Layer | Technology | Version |
|---|---|---|
| Language | Python | 3.11 |
| Web scraping | `requests` + `beautifulsoup4` + `lxml` | 4.12 / 5.2 |
| Message broker | Apache Kafka (Confluent Platform) + Zookeeper | 7.6 |
| Object storage / Data Lake | MinIO (S3-compatible) | 2024-10 |
| Storage format | JSON (Bronze/Silver) + Parquet (Gold) | — |
| Transformation | `pandas` + `pyarrow` | 2.2 / 16.1 |
| Language detection | `langdetect` | 1.0.9 |
| Orchestration | Apache Airflow (LocalExecutor) | 2.9.3 |
| Data Warehouse | PostgreSQL | 16-alpine |
| Warehouse loading | SQLAlchemy + psycopg2 | 2.0 / 2.9 |
| BI / Dashboards | Metabase | 0.50.20 |
| Monitoring | Prometheus + Grafana | 2.54 / 11.2 |
| Exporters | node-exporter, cAdvisor, kafka-exporter, postgres-exporter | latest |
| Containerisation | Docker + Docker Compose v2 | — |
| Orchestration (optional) | Kubernetes (plain manifests) | 1.28+ |

---

## 4. Repository layout

```
project_archi/
├── docker-compose.yml                  Core stack (Kafka, MinIO, Postgres, Airflow, Metabase)
├── docker-compose.monitoring.yml       Optional Prometheus + Grafana overlay
├── .env                                Centralised configuration
├── README.md                           This file
│
├── scraper/                            BS4 scraper -> Kafka producer
│   ├── scraper.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── transformer/                        Bronze/Silver consumer + Gold/DWH builder
│   ├── process_bronze_silver.py
│   ├── process_gold_dwh.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── airflow/
│   ├── dags/news_pipeline_dag.py       Hourly orchestration DAG
│   └── jobs/                           Source code mounted into Airflow
│
├── init/
│   └── init_warehouse.sql              Bootstrap of gold.* tables
│
├── dashboards/
│   └── queries.sql                     Ready-to-paste SQL for Metabase / Grafana
│
├── monitoring/
│   ├── prometheus/prometheus.yml
│   └── grafana/                        Datasource + dashboard provisioning
│
└── k8s/                                Optional Kubernetes manifests (6 namespaces)
    ├── 00-namespaces.yaml
    ├── 01-storage.yaml
    ├── 02-streaming.yaml
    ├── 03-processing.yaml
    ├── 04-orchestration.yaml
    ├── 05-bi.yaml
    ├── 06-monitoring.yaml
    └── README.md
```

---

## 5. Installation & execution

### 5.1. Prerequisites

- **Docker Desktop** (Linux / macOS / Windows) with at least **6 GB RAM** and **4 vCPU** allocated.
- **Docker Compose v2** (`docker compose version` ≥ 2.20).
- Internet access (to pull images and scrape RSS feeds).
- *(optional)* `kubectl` and a local cluster (`kind`, `minikube`, Docker Desktop K8s) for the Kubernetes deploy.

### 5.2. Clone & start the core platform

```bash
git clone <your-repo-url> project_archi
cd project_archi

# Build images and bring everything up in detached mode
docker compose up -d --build

# Watch the bootstrap (MinIO buckets + Airflow init)
docker compose logs -f minio-init airflow-init

# All running services
docker compose ps
```

First-time startup takes 2–4 minutes (image pulls + Airflow `_PIP_ADDITIONAL_REQUIREMENTS` install).

### 5.3. Add the optional monitoring overlay

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.monitoring.yml \
  up -d
```

### 5.4. Stop / clean

```bash
docker compose down                       # stop, keep data
docker compose down -v                    # stop and wipe all volumes
```

### 5.5. Deploy on Kubernetes (optional)

```bash
kubectl apply -f k8s/00-namespaces.yaml
kubectl apply -f k8s/01-storage.yaml
kubectl apply -f k8s/02-streaming.yaml
kubectl apply -f k8s/03-processing.yaml
kubectl apply -f k8s/04-orchestration.yaml
kubectl apply -f k8s/05-bi.yaml
kubectl apply -f k8s/06-monitoring.yaml

kubectl get pods -A | grep newslake
```

See `k8s/README.md` for image-publishing details and access ports.

---

## 6. Access the platform

Once `docker compose up -d` is healthy:

| Service | URL | Credentials |
|---|---|---|
| **MinIO console** (Data Lake) | http://localhost:9001 | `minio` / `minio12345` |
| **MinIO S3 API** | http://localhost:9000 | same |
| **Kafka** (host port) | `localhost:29092` | — |
| **Postgres warehouse** | `localhost:5432`, db `warehouse` | `warehouse` / `warehouse` |
| **Airflow UI** | http://localhost:8080 | `admin` / `admin` |
| **Metabase** (BI) | http://localhost:3000 | set on first launch |
| **Prometheus** *(monitoring overlay)* | http://localhost:9090 | — |
| **Grafana** *(monitoring overlay)* | http://localhost:3001 | `admin` / `admin` |

### 6.1. Airflow UI

1. Open http://localhost:8080 → log in with `admin` / `admin`.
2. The DAG **`news_pipeline`** is unpaused and runs on `@hourly`. Trigger it manually with the *Play* icon.
3. View task logs: click the run → click a task square → *Logs*.
4. Pipeline order: `start → scrape_articles → bronze_silver_process → build_gold → load_warehouse → end`.

### 6.2. Postgres warehouse

```bash
# psql inside the container
docker exec -it warehouse psql -U warehouse -d warehouse

# Quick inspection
\dt gold.*
SELECT keyword, SUM(n_occurrences) AS total
FROM gold.top_keywords
WHERE day >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY keyword ORDER BY total DESC LIMIT 10;
```

From any external SQL client (DBeaver, DataGrip, pgAdmin):
- Host `localhost`, Port `5432`, DB `warehouse`, User `warehouse`, Password `warehouse`.

### 6.3. MinIO data lake

- Web console http://localhost:9001 → bucket **`newslake`** → folders `bronze/`, `silver/`, `gold/`, `bronze/_rejected/`.
- CLI through the running container:

```bash
docker exec -it minio mc alias set local http://localhost:9000 minio minio12345
docker exec -it minio mc ls --recursive local/newslake/silver | head
```

### 6.4. Metabase dashboards

1. Open http://localhost:3000 → finish the welcome wizard (any admin email/password).
2. *Add database* → **PostgreSQL**:
   - Host `warehouse` *(Docker service name)*
   - Port `5432`, DB `warehouse`, User `warehouse`, Password `warehouse`.
3. Create a new dashboard **"NewsLake"** and add cards by pasting the queries from `dashboards/queries.sql`:
   - **Tendances d'actualité** (block 1, 1.bis)
   - **Nombre articles par source** (block 2, 2.bis)
   - **Mots clés les plus fréquents** (block 3)
   - **KPI tiles, theme donut, country map** (blocks 4–8)

### 6.5. Grafana (monitoring + business overlay)

1. Open http://localhost:3001 → `admin` / `admin`.
2. Datasources **Prometheus** and **NewsWarehouse** are already provisioned.
3. *Dashboards → NewsLake → "NewsLake — Pipeline Health"* — pre-built dashboard with throughput, CPU/RAM and business KPIs.

### 6.6. Inspect the Kafka topic

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic raw_articles --from-beginning --max-messages 3
```

### 6.7. Run a one-shot pipeline manually

```bash
# Scrape one cycle
docker compose exec scraper python -c "import scraper; print(scraper.run_one_cycle())"

# Drain Kafka into Bronze + Silver
docker compose exec bronze-silver python -c "import process_bronze_silver as bs; print(bs.run_batch())"

# Build Gold + load warehouse
docker compose exec bronze-silver python process_gold_dwh.py
```

---

## 7. Data quality & governance

- **DQ gates** (`transformer/process_bronze_silver.py:dq_check`)
  - `missing_title`, `missing_date` / `invalid_date`, `content_too_short` (configurable threshold via `DQ_MIN_CONTENT_CHARS`).
- **Rejected records are preserved** in `minio://newslake/bronze/_rejected/reason=<x>/...` — nothing is silently dropped.
- **Lineage timestamps** on every layer:
  - `scraped_at` (source ingestion)
  - `silver_processed_at` (cleaning)
  - `ingested_at` (warehouse insert).
- **Idempotent keys** — every article keyed by `sha1(url)`, used as MinIO object name **and** Postgres primary key, so re-runs do not duplicate.
- **Schema contract** documented in `scraper/scraper.py` (the `Article` dataclass) and reflected by warehouse DDL in `init/init_warehouse.sql` and `process_gold_dwh.py:DDL`.

---

## 8. Configuration reference

All settings live in `.env` (loaded by Compose) — override per environment.

| Variable | Default | Description |
|---|---|---|
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | `minio` / `minio12345` | Lake credentials |
| `MINIO_BUCKET` | `newslake` | Lake bucket |
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka address (cluster-internal) |
| `KAFKA_TOPIC_ARTICLES` | `raw_articles` | Topic name |
| `POSTGRES_USER`/`POSTGRES_PASSWORD`/`POSTGRES_DB` | `warehouse` (×3) | Warehouse credentials |
| `SCRAPE_INTERVAL_SECONDS` | `3600` | Scraper loop period |
| `SCRAPE_MAX_ARTICLES_PER_FEED` | `15` | Articles fetched per RSS feed per cycle |
| `SCRAPE_FETCH_CONTENT` | `true` | Fetch full HTML body or rely on RSS summary |
| `DQ_MIN_CONTENT_CHARS` | `100` | Minimum content length to reach Silver |

---

## 9. Troubleshooting

| Symptom | Action |
|---|---|
| `airflow-init` exits with `db migrate` error | `docker compose down -v` then `up -d` (clean Airflow metadata DB) |
| Kafka container restarts on boot | Wait — Zookeeper takes ~20 s to be ready; healthcheck handles this |
| Scraper logs `NoBrokersAvailable` | Normal during the first ~30 s; producer auto-retries 20× |
| MinIO buckets missing | Re-run `docker compose up -d minio-init` |
| Empty Metabase dashboards | Trigger the Airflow DAG once, or run the manual one-shot from §6.7 |
| Grafana shows "No data" on Kafka panels | Wait one scrape interval (15 s) after the topic receives traffic |

---

## 10. Roadmap / next steps

- Migrate Airflow to **CeleryExecutor + Redis** (or the official Helm chart) for HA.
- Replace the bespoke MinIO/Kafka/Postgres K8s manifests with **MinIO Operator + Strimzi + CloudNativePG**.
- Add **Great Expectations** suite on Silver and surface results via the existing `gold.dq_results` table.
- Add an **OpenLineage** emitter to Airflow tasks, push to **Marquez** for visual data-lineage.
- Enrich the trends pipeline with **NER + topic modelling** (spaCy / BERTopic) instead of raw frequency.

---

## 11. Authors & licence

Project realised as a **Senior Big Data Engineering** capstone — *Architecture de données pour l'analyse d'articles de presse*.

Source code released under the **MIT License**.
