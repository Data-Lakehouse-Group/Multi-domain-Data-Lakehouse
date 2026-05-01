# Multi-Domain Data Lakehouse

A single-node, open-source data lakehouse built on the Medallion Architecture (Bronze → Silver → Gold) using lightweight, cost-free tooling. The pipeline ingests NYC TLC Yellow Taxi Trip records and NOAA Global Surface Summary of the Day (GSOD) weather data programmatically, processes them through a fully validated and orchestrated ETL pipeline, and surfaces Gold-layer aggregations through interactive Apache Superset dashboards backed by Trino. The system demonstrates that production-grade lakehouse capabilities — ACID transactions, time travel, schema evolution, data quality gating, and query optimisation — are achievable on a single node without distributed infrastructure or paid cloud services.

---

## Group Members

| Name              | Student ID  |
| ----------------- | ----------- |
| [Arvesh Gosine]   | [816039536] |
| [Nie-l Constance] | [816041818] |

---

## Tech Stack

| Component          | Tool                      |
| ------------------ | ------------------------- |
| Object Storage     | MinIO (S3-compatible)     |
| Table Format       | Delta Lake via delta-rs   |
| In-Memory Format   | PyArrow                   |
| Query Engine       | DuckDB                    |
| Data Quality       | Great Expectations        |
| Gold Aggregations  | dbt (exposed via FastAPI) |
| Orchestration      | Apache Airflow            |
| Analytical Serving | Trino + Apache Superset   |

---

## Repository Structure

```
multi-domain-data-lakehouse/
├── dags/                        # Airflow DAG definitions
├── dashboard/
│   ├── superset/                # Superset config and dashboard exports
│   └── trino/                   # Trino catalog and config
├── docs/                        # Project documentation
├── ingestion/
│   ├── taxi/
│   │   ├── download.py          # Downloads TLC raw parquet files
│   │   ├── bronze_ingestion.py  # Ingests TLC data to Bronze Delta table
│   │   └── gold_ingestion.py    # Ingests dbt output to Gold Delta table
│   └── weather/
│       ├── download.py          # Downloads NOAA GSOD tar.gz files
│       ├── bronze_ingestion.py  # Ingests NOAA data to Bronze Delta table
│       └── gold_ingestion.py    # Ingests dbt output to Gold Delta table
├── notebooks/
│   ├── query_optimization.ipynb # Query performance comparison (before/after)
│   └── time_travel_demo.ipynb   # Delta Lake time travel demonstration
├── quality/
│   ├── taxi/                    # Great Expectations suites for TLC data
│   └── weather/                 # Great Expectations suites for NOAA data
├── transformations/
│   ├── dbt/                     # dbt project for Gold aggregations + FastAPI
│   └── silver/                  # DuckDB Silver transformation scripts
├── .env                         # Environment variables (see setup below)
├── docker-compose.yml           # Full stack definition
├── Dockerfile.airflow
├── Dockerfile.dbt
├── Dockerfile.superset
└── requirements.txt
```

---

## Prerequisites

- **Python** `3.12+`
- **Docker** and **Docker Compose** (Docker Desktop or Docker Engine)
- At least **16 GB RAM** recommended for full pipeline runs
- At least **50 GB free disk space** for raw and processed data

---

## Setup Instructions

### 1. Clone the repository

```bash
git clone <repository-url>
cd multi-domain-data-lakehouse
```

### 2. Install Python dependencies

It is recommended to use a virtual environment:

```bash
python -m venv venv
source venv/bin/activate        # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Key dependencies installed:

```
duckdb==1.4.4
pyarrow==23.0.0
deltalake==1.5.0
great_expectations==1.15.2
dbt-core==1.11.8
dbt-duckdb==1.10.1
apache-airflow==3.2.1
fastapi>=0.110.0
boto3==1.42.95
trino==0.337.0
```

### 3. Configure environment variables

Copy the `.env.sample` file and verify the default values are correct for local development. The defaults below match the Docker Compose configuration out of the box:

### 4. Start all services

```bash
docker compose up --build
```

This will start and initialise the following services in order:

| Service       | URL                   |
| ------------- | --------------------- |
| MinIO Console | http://localhost:9001 |
| Airflow UI    | http://localhost:8080 |
| Superset      | http://localhost:8088 |
| Trino         | http://localhost:8082 |
| dbt API       | http://localhost:8001 |

MinIO will automatically create the following buckets on first start: `raw`, `bronze`, `silver`, `gold`, `artifacts`.

---

## Running the Pipeline End to End

The pipeline is orchestrated through Apache Airflow. After all containers are healthy, trigger the DAGs in the following order via the Airflow UI at http://localhost:8080:

### TLC Yellow Taxi Pipeline

| Step | DAG                      | Description                                                       |
| ---- | ------------------------ | ----------------------------------------------------------------- |
| 1    | `taxi_download`          | Downloads TLC parquet files to the MinIO raw bucket               |
| 2    | `taxi_bronze_ingest`     | Ingests raw files to Bronze Delta table with audit columns        |
| 3    | `taxi_bronze_validation` | Runs Great Expectations Bronze suite — halts on failure           |
| 4    | `taxi_silver_transform`  | Cleans and enriches data via DuckDB, writes to Silver Delta table |
| 5    | `taxi_silver_validation` | Runs Great Expectations Silver suite — halts on failure           |
| 6    | `taxi_gold_transform`    | Triggers dbt aggregations via FastAPI and writes to staging area  |
| 7    | `taxi_gold_ingest`       | Writes gold aggregated data to Gold Delta table                   |

### NOAA GSOD Weather Pipeline

| Step | DAG                         | Description                                                      |
| ---- | --------------------------- | ---------------------------------------------------------------- |
| 1    | `weather_download`          | Downloads NOAA GSOD tar.gz files to the MinIO raw bucket         |
| 2    | `weather_bronze_ingest`     | Extracts CSVs, merges, and ingests to Bronze Delta table         |
| 3    | `weather_bronze_validation` | Runs Great Expectations Bronze suite — halts on failure          |
| 4    | `weather_silver_transform`  | Cleans, removes sentinel values, enriches via DuckDB             |
| 5    | `weather_silver_validation` | Runs Great Expectations Silver suite — halts on failure          |
| 6    | `weather_gold_transform`    | Triggers dbt aggregations via FastAPI and writes to staging area |
| 7    | `weather_gold_ingest`       | Writes gold aggregated data to Gold Delta table                  |

> **Note:** Each DAG can be configured with a year and month range via Airflow variables or DAG parameters before triggering.

---

## Reproducing Main Results

### Row Counts by Layer

After running the full pipeline, the approximate row counts at each layer are:

| Dataset         | Bronze | Silver | Gold  |
| --------------- | ------ | ------ | ----- |
| TLC Yellow Taxi | 600M+  | 500M+  | 250K+ |
| NOAA GSOD       | 32M+   | 20M+   | 12M+  |

### Data Quality Reports

Validation reports for every Bronze and Silver run are saved automatically to the MinIO `artifacts` bucket under:

```
artifacts/great_expectations/reports/taxi/bronze/
artifacts/great_expectations/reports/taxi/silver/
artifacts/great_expectations/reports/weather/bronze/
artifacts/great_expectations/reports/weather/silver/
```

These can be accessed via the MinIO Console at http://localhost:9001.

### Query Performance Comparison

Open and run the query optimisation notebook:

```bash
jupyter notebook notebooks/query_optimization.ipynb
```

This notebook demonstrates before and after query performance for partition pruning and predicate pushdown on the Delta tables.

### Time Travel Demonstration

Open and run the time travel notebook:

```bash
jupyter notebook notebooks/time_travel_demo.ipynb
```

This notebook demonstrates querying prior versions of a Delta table by version number and timestamp using delta-rs.

### Dashboards

After the Gold layer has been populated, navigate to Superset at http://localhost:8088. Two pre-built dashboards are available:

- **TLC Yellow Taxi Dashboard** — trip volume, fare trends, pickup heatmaps by zone and hour
- **NOAA Weather Dashboard** — temperature trends, precipitation, wind speed, and weather event flags

---

## Data Sources

Data is downloaded programmatically by the pipeline and does not need to be manually placed anywhere.

| Dataset                                       | Source                                                                              | License              |
| --------------------------------------------- | ----------------------------------------------------------------------------------- | -------------------- |
| NYC TLC Yellow Taxi Trip Records              | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page                        | Public Domain        |
| NOAA Global Surface Summary of the Day (GSOD) | https://www.ncei.noaa.gov/products/land-based-station/global-surface-summary-of-day | Public Domain (NOAA) |

Both datasets are publicly released by their respective government agencies and are free to use for research and educational purposes.

---

## Demo (Note the MinIO store is not shown)

https://www.youtube.com/watch?v=NlbHWzP-cms&feature=youtu.be

---

## AI Tools Used

The following AI tools were used during development of this project:

- **Claude (Anthropic)** — used for (i) tutorials and articles on setting up the development environment, (ii) syntax for achieving the desired outputs of our solutions, (iii) architectural guidance in what technologies work the best and the pros and cons of each, (iv) formatting guidance in generating this report and (v) explanations on various Lakehouse concepts required to develop this project.

All architectural decisions, feature engineering choices, validation rule design, and analytical conclusions were made and verified by the project authors.

---

## Notes

- The Airflow LocalExecutor is used for simplicity. For larger workloads, the CeleryExecutor can be configured in `docker-compose.yml`.
- DuckDB memory and thread limits are set to `4GB` and `4 threads` respectively in the transformation scripts. Adjust these in `transformations/silver/taxi.py` and `transformations/silver/weather.py` if your machine has different resources.
