<div align="center">

# 🛒 E-Commerce Data Engineering Pipeline

**A production-style end-to-end data pipeline built on the Bronze → Silver → Gold medallion architecture**

[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Astronomer](https://img.shields.io/badge/Astronomer-Astro%20CLI-7352FF?style=flat-square)](https://www.astronomer.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=flat-square&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?style=flat-square&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-22c55e?style=flat-square)](LICENSE)

[Overview](#-overview) · [Architecture](#-architecture) · [Quick Start](#-quick-start) · [Pipeline Layers](#-pipeline-layers) · [Data Model](#-data-model) · [Project Structure](#-project-structure) · [Commands](#-commands-reference)

</div>

---

## 📌 Overview

This project implements a full data engineering pipeline for the [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) public dataset. Raw CSV files are ingested, cleaned, and modelled into a star schema warehouse that feeds a Power BI dashboard.

### What's built

- **4 Airflow DAGs** orchestrating the full ETL lifecycle on a daily schedule
- **Bronze layer** — raw data ingested as-is with `_loaded_at` audit timestamps
- **Silver layer** — deduplicated, typed, null-safe, and standardised tables
- **Gold layer** — star schema with dimensions, facts, ML features, and Power BI views
- **Quality gate** — automated row count, null rate, and business rule validation

### Coming next

- 🔲 FastAPI backend — `/api/metrics`, `/api/trends`, `/api/pipeline/status`
- 🔲 React + Recharts dashboard — KPI cards, time-series charts, pipeline monitor

---

## 🏗 Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                       DATA SOURCES                           │
│          Olist Brazilian E-Commerce  (9 CSV files)           │
└─────────────────────────────┬────────────────────────────────┘
                              │
                              ▼  DAG 1 · daily 01:00 UTC
┌──────────────────────────────────────────────────────────────┐
│  🥉 BRONZE  ·  schema: bronze                                │
│                                                              │
│  Raw ingestion — all columns stored as TEXT                  │
│  _loaded_at audit timestamp appended to every table          │
│  No transformations, no type casts, no filtering             │
│                                                              │
│  bronze.orders        bronze.customers    bronze.products    │
│  bronze.order_items   bronze.payments     bronze.sellers     │
│  bronze.reviews       bronze.geolocation                     │
└─────────────────────────────┬────────────────────────────────┘
                              │
                              ▼  DAG 2 · daily 03:00 UTC
┌──────────────────────────────────────────────────────────────┐
│  🥈 SILVER  ·  schema: silver                                │
│                                                              │
│  Deduplication    ROW_NUMBER() OVER (PARTITION BY pk         │
│                                      ORDER BY loaded_at DESC)│
│  Null handling    NULLIF(col, '') before every type cast      │
│  Type casting     ::TIMESTAMP  ::NUMERIC(12,2)  ::INT        │
│  Standardisation  UPPER(TRIM()) city/state                   │
│                   LOWER(TRIM()) category names               │
│  Imputation       Per-category median for product dimensions │
│  Ordering         ORDER BY natural key ASC                   │
└─────────────────────────────┬────────────────────────────────┘
                              │
                              ▼  DAG 3 · daily 05:00 UTC
┌──────────────────────────────────────────────────────────────┐
│  🥇 GOLD  ·  schema: gold                                    │
│                                                              │
│  Dimensions   dim_customers   dim_products                   │
│               dim_sellers     dim_dates                      │
│                                                              │
│  Facts        fact_orders   fact_order_payments              │
│               fact_seller_performance                        │
│                                                              │
│  ML features  fulfilment_days   approval_hours               │
│               delivery_delta_days   is_high_instalment       │
│                                                              │
│  Public views  public.*  ──▶  Power BI navigator ready       │
└─────────────────────────────┬────────────────────────────────┘
                              │
                              ▼  DAG 4 · daily 07:00 UTC
┌──────────────────────────────────────────────────────────────┐
│  ✅ QUALITY CHECKS                                           │
│                                                              │
│  Row count minimums per gold table                           │
│  Null rate thresholds on key columns                         │
│  Business rules  no negative revenue · no orphan FK keys     │
│                  no duplicate order IDs · valid review scores│
└─────────────────────────────┬────────────────────────────────┘
                              │
              ┌───────────────┴────────────────┐
              ▼                                ▼  (next phase)
        Power BI Desktop              FastAPI + React Dashboard
    (public.* views via PostgreSQL)
```

---

## 🚀 Quick Start

### Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) | Latest | Runs the PostgreSQL warehouse container |
| [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) | Latest | Manages local Airflow environment |
| [Python](https://www.python.org/downloads/) | 3.12 | Local test execution |

---

### Step 1 — Install the Astro CLI

```powershell
# Run PowerShell as Administrator
winget install -e --id Astronomer.Astro

# Confirm the install
astro version
```

> **Already using Docker Desktop?** Keep it by skipping the Podman dependency:
> ```powershell
> winget install -e --id Astronomer.Astro --skip-dependencies
> ```

---

### Step 2 — Clone and configure

```bash
git clone https://github.com/your-username/ecommerce-pipeline.git
cd ecommerce-pipeline
```

Create a `.env` file in the project root:

```env
WAREHOUSE_HOST=host.docker.internal
WAREHOUSE_PORT=5433
WAREHOUSE_DB=ecommerce_dw
WAREHOUSE_USER=warehouse
WAREHOUSE_PASSWORD=warehouse
DATA_DIR=/usr/local/airflow/include/data
```

---

### Step 3 — Add the dataset

Download the dataset from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and place all CSV files into `include/data/`:

```
include/data/
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
├── olist_customers_dataset.csv
├── olist_products_dataset.csv
├── olist_sellers_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_geolocation_dataset.csv
└── product_category_name_translation.csv
```

> Add `include/data/` to your `.gitignore` — do not commit raw data files.

---

### Step 4 — Start the warehouse

```bash
docker compose -f docker-compose.warehouse.yml up -d
```

---

### Step 5 — Start Airflow

```bash
# First time only — initialises the Astro project files
astro dev init

# Start the Airflow environment
astro dev start
```

Open **http://localhost:8080** · credentials: `admin` / `admin`

---

### Step 6 — Add the warehouse connection

In the Airflow UI navigate to **Admin → Connections → +** and enter:

| Field | Value |
|---|---|
| Connection ID | `postgres_warehouse` |
| Connection Type | `Postgres` |
| Host | `host.docker.internal` |
| Database | `ecommerce_dw` |
| Login | `warehouse` |
| Password | `warehouse` |
| Port | `5433` |

---

### Step 7 — Run the pipeline

```bash
astro dev run dags trigger 01_bronze_ingest
astro dev run dags trigger 02_silver_clean
astro dev run dags trigger 03_gold_model
astro dev run dags trigger 04_quality_checks
```

> DAGs also run automatically on their daily schedule — see [DAG Schedules](#dag-schedules).

---

## 🔧 Pipeline Layers

### Silver — cleaning operations

Every bronze table passes through the same cleaning pipeline before data reaches gold:

| Operation | Method | Reason |
|---|---|---|
| **Deduplication** | `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY loaded_at DESC)` | CSV re-exports introduce duplicate rows per entity |
| **Empty string → NULL** | `NULLIF(col, '')` before every `::TYPE` cast | Ingest stores everything as TEXT; empty strings crash type casts |
| **NULL imputation** | `COALESCE(value, category_median, 0)` | Product dimensions use per-category medians so ML features are always populated |
| **Type casting** | `::TIMESTAMP`, `::NUMERIC(12,2)`, `::INT` | Applied after the `NULLIF` guard to prevent runtime cast errors |
| **Standardisation** | `UPPER(TRIM(...))` on city/state · `LOWER(TRIM(...))` on categories | Prevents broken `GROUP BY` from mixed-case data (`"SP"` vs `"sp"`) |
| **Ordering** | `ORDER BY` on natural key ascending | Reduces sort overhead in downstream window functions and joins |

### Gold — ML-ready features

The gold fact tables include derived feature columns designed for ML workloads:

| Feature | Table | Description |
|---|---|---|
| `fulfilment_days` | `fact_orders` | Days from purchase to delivery |
| `approval_hours` | `fact_orders` | Hours from purchase to payment approval |
| `delivery_delta_days` | `fact_orders` | Estimated minus actual delivery (negative = late) |
| `delivered_on_time` | `fact_orders` | Boolean — delivered before estimated date |
| `is_high_instalment` | `fact_order_payments` | Flag for orders with > 6 payment instalments |
| `avg_review_score` | `fact_seller_performance` | Monthly rolling review average per seller |
| `on_time_pct` | `fact_seller_performance` | Monthly on-time delivery rate per seller |

---

## 📊 Data Model

### DAG schedules

| DAG | ID | Schedule | Runs after |
|---|---|---|---|
| Bronze ingest | `01_bronze_ingest` | `0 1 * * *` | — |
| Silver clean | `02_silver_clean` | `0 3 * * *` | Bronze |
| Gold model | `03_gold_model` | `0 5 * * *` | Silver |
| Quality checks | `04_quality_checks` | `0 7 * * *` | Gold |

### Dimension tables

| Table | Grain | Primary key | Notable columns |
|---|---|---|---|
| `gold.dim_customers` | 1 row per unique customer | `customer_key` | `city`, `state`, `order_account_count` |
| `gold.dim_products` | 1 row per product | `product_key` | `category`, `weight_g`, `volumetric_weight_kg` |
| `gold.dim_sellers` | 1 row per seller | `seller_key` | `city`, `state` |
| `gold.dim_dates` | 1 row per calendar day | `date_key` | `year`, `quarter`, `month_name`, `is_weekend`, `is_public_holiday`, `fiscal_period` |

### Fact tables

| Table | Grain | Key measures |
|---|---|---|
| `gold.fact_orders` | 1 row per order | `revenue`, `freight_revenue`, `gross_revenue`, `fulfilment_days`, `delivered_on_time`, `review_score` |
| `gold.fact_order_payments` | 1 row per order × payment type × instalments | `total_amount`, `installments`, `avg_instalment_amount`, `is_high_instalment` |
| `gold.fact_seller_performance` | 1 row per seller × month | `total_revenue`, `total_orders`, `avg_review_score`, `on_time_pct`, `avg_fulfilment_days` |

### Power BI views

The `public` schema exposes thin views over every gold table. Power BI's PostgreSQL navigator surfaces `public` by default — no schema configuration required.

```sql
public.dim_customers            →  gold.dim_customers
public.dim_products             →  gold.dim_products
public.dim_sellers              →  gold.dim_sellers
public.dim_dates                →  gold.dim_dates
public.fact_orders              →  gold.fact_orders
public.fact_order_payments      →  gold.fact_order_payments
public.fact_seller_performance  →  gold.fact_seller_performance
```

---

## 📁 Project Structure

```
ecommerce-pipeline/
│
├── 📄 Dockerfile                         Astro Runtime image (generated by astro dev init)
├── 📄 .env                               Warehouse credentials — gitignored
├── 📄 requirements.txt                   Python dependencies
├── 📄 docker-compose.warehouse.yml       PostgreSQL 15 warehouse on port 5433
│
├── 📁 .astro/                            Astro CLI project config
│
├── 📁 dags/
│   ├── 01_bronze_ingest.py               Load CSVs → bronze via psycopg2 COPY
│   ├── 02_silver_clean.py                Execute sql/silver/*.sql scripts
│   ├── 03_gold_model.py                  Execute sql/gold/*.sql scripts
│   └── 04_quality_checks.py             Validate gold layer — fails loudly on breach
│
├── 📁 sql/
│   ├── silver/
│   │   ├── 00_create_schemas.sql         CREATE SCHEMA bronze / silver / gold
│   │   ├── 01_orders.sql                 Dedup + NULLIF casts + derived fulfilment cols
│   │   ├── 02_order_items.sql            Dedup + price cast + line_total
│   │   ├── 03_customers.sql              Dedup + UPPER(TRIM()) geography
│   │   ├── 04_products.sql               Dedup + per-category median imputation
│   │   └── 05_sellers_payments_reviews.sql
│   └── gold/
│       ├── 01_dimensions.sql             dim_* tables + PKs + indexes
│       ├── 02_facts.sql                  fact_* tables + ML features
│       └── 03_powerbi_views.sql          public.* views for Power BI
│
├── 📁 include/
│   └── data/                             Olist CSVs — gitignored
│
├── 📁 plugins/                           Custom Airflow plugins
├── 📁 config/                            Airflow connections / variables
├── 📁 tests/                             DAG integrity and unit tests
│
├── 📁 backend/                           FastAPI REST API  ← next phase
└── 📁 frontend/                          React + Recharts dashboard  ← next phase
```

---

## 💻 Commands Reference

### Astro CLI — daily workflow

| Command | Description |
|---|---|
| `astro dev start` | Start Airflow (webserver, scheduler, triggerer) |
| `astro dev stop` | Stop all Airflow containers |
| `astro dev restart` | Rebuild Docker image and restart — required after editing `requirements.txt` |
| `astro dev logs` | Stream live scheduler and webserver logs |
| `astro dev parse` | Validate all DAG files for import errors without starting containers |
| `astro dev run dags trigger <dag_id>` | Manually trigger a DAG by its ID |

### Running tests

```bash
pip install pytest
pytest tests/
```

---

## 🧰 Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.8 · Astronomer Astro CLI |
| Warehouse | PostgreSQL 15 |
| Ingestion | Python · pandas 2.1 · psycopg2 COPY |
| Transformation | SQL executed via `PostgresHook.get_conn()` |
| BI | Power BI Desktop |
| Containerisation | Docker · Astro Runtime (based on Python 3.12) |

---

## 🗺 Roadmap

- [x] Bronze ingest — raw CSV loading with audit timestamps
- [x] Silver clean — deduplication, type casting, null handling, standardisation
- [x] Gold model — star schema with ML-ready feature columns
- [x] Quality checks — automated validation gate before BI refresh
- [x] Power BI integration — public schema views
- [ ] FastAPI backend — metrics, trends, and pipeline status endpoints
- [ ] React dashboard — KPI cards, charts, pipeline run monitor
- [ ] dbt integration — replace raw SQL files with versioned dbt models
- [ ] CI/CD — GitHub Actions for DAG linting, parsing, and test runs

---

## 🤝 Contributing

Pull requests are welcome. For major changes please open an issue first to discuss what you would like to change. Please make sure to update tests as appropriate.

---

## 📄 License

[MIT](LICENSE)

---

<div align="center">

Built with the [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) public dataset

</div>