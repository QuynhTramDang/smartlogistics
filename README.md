
# Smart Logistics
## Table of Contents

- [A. Project Objective](#a-project-objective)  
  - [Key Outcomes](#key-outcomes)  
    - [1. Route and Network Optimization](#1-route-and-network-optimization)  
    - [2. Logistics Performance Analysis (KPI & Reporting)](#2-logistics-performance-analysis-kpi--reporting)  

- [B. Dataset Selection](#b-dataset-selection)  
  - [1. SAP APIs (SalesOrder, OutboundDelivery, WarehouseOrder)](#1-sap-apis-salesorder-outbounddelivery-warehouseorder)  
  - [2. OSRM Dataset](#2-osrm-dataset)  

- [C. System Architecture](#c-system-architecture)  
  - [1. System Overview](#1-system-overview)  
  - [2. Data Processing (Bronze / Silver / Gold)](#2-data-processing-bronze--silver--gold)  

- [D. Deployment](#d-deployment)  
  - [1. System Requirements](#1-system-requirements)  
  - [2. Deployment Steps](#2-deployment-steps)  
  - [3. Connections & User Interfaces](#3-connections--user-interfaces)  
  - [4. Visualization & Dashboards](#4-visualization--dashboards)  
  - [5. Monitoring & Alerting](#5-monitoring--alerting)  

- [E. Results](#e-results)  
  - [Dashboard with Metabase](#dashboard-with-metabase)  
  - [Monitoring with Grafana](#monitoring-with-grafana)  
  - [Monitoring Airflow](#monitoring-airflow)  
  - [Monitoring Spark](#monitoring-spark)  
  - [Monitoring Node-Noporter](#monitoring-node-noporter)  

- [Author](#author)  

## A. Project Objective

The **SmartLogistics** project aims to build an intelligent logistics platform that leverages both real-time and historical data to optimize routing, reduce transportation costs, and deliver actionable KPI dashboards with drill-down capabilities across regions, carriers, and warehouses.

### Key Outcomes

### 1. Route and Network Optimization

* **Business Requirement (BR):**
  Reduce transportation costs, shorten delivery lead times, and improve on-time performance.

* **Technical Execution (TE):**

  * Map the logistics network of hubs and transportation routes using GIS or graph databases.
  * Calculate cost, duration, distance, and load capacity for each route.
  * Develop an optimization module leveraging shortest path algorithms (e.g., Dijkstra, A\*) or routing engines (e.g., OSRM).
  * Integrate real-time traffic data (e.g., via mapping/traffic APIs) to dynamically adjust route planning.
  * Analyze historical delivery data to assess route reliability by time of day and day of week.

* **Output:**
  `routes_summary` table containing: route details, actual average travel time, ETA deviation, cost per km, delay ratio, and successful delivery frequency.

### 2. Logistics Performance Analysis (KPI & Reporting)**

* **Business Requirement (BR):**
  Provide weekly and monthly KPIs to evaluate carrier performance and support continuous operational improvement.

* **Technical Execution (TE):**

  * Aggregate data from TMS, WMS, orders, and GPS telemetry.
  * Compute KPIs across key logistics dimensions.
  * Enable drill-down reporting by region, carrier, warehouse, time window, and order typology.

* **Output:**
  `kpi_logistics_monthly(region, carrier, metric, value, YoY_change)` — a monthly KPI summary with year-over-year comparison capabilities.

## B. Dataset Selection

The project leverages the following primary data sources. Each dataset is briefly described in terms of its content and its role within the pipeline.

### 1. SAP APIs (3 main datasets)

* **Source:** SAP REST/OData APIs

* **SalesOrder — API\_SALES\_ORDER\_SRV**

  * **Content:** Sales order information including order headers, items, values, customers, creation dates, and status.
  * **Role:** Serves as the primary source for linking orders to delivery routes.

* **OutboundDelivery — API\_OUTBOUND\_DELIVERY\_SRV\_0002**

  * **Content:** Delivery document details such as dispatch records, delivery status, pick/dispatch times, and actual delivered quantities.
  * **Role:** Provides the ground truth for monitoring actual delivery performance, enabling calculations of On-Time Delivery (OTD) and ETA deviation.

* **WarehouseOrder — API\_WAREHOUSEORDER\_0001**

  * **Content:** Warehouse execution orders (putaway, picking, packing) with associated warehouse, location, and processing time details.
  * **Role:** Used to compute warehouse processing lead times, identify bottlenecks, and map orders into the fulfillment flow.

### 2. OSRM Dataset (OpenStreetMap Routing Data)

* **Content:** `.osm.pbf` files and prebuilt `.osrm` artifacts (CH or MLD) containing the road network graph, travel times, and distances.
* **Role:** Enables route calculation, ETA estimation, and construction of route profiles.

## C. System Architecture

The Data Lakehouse architecture in this project is carefully designed to support batch data processing and seamlessly integrate multiple data sources into a unified analytics platform. The design follows the **Medallion Architecture** paradigm, organizing data into **Bronze, Silver, and Gold layers**, each serving distinct purposes in the data lifecycle.

### 1. System Overview

The architecture consists of the following key components:

<img width="1753" height="1105" alt="image" src="https://github.com/user-attachments/assets/7cdacdab-2706-4d91-9b7d-231c1446825e" />



#### **Data Ingestion Layer**

* **HTTP / OData Connectors (SAP APIs):** Collects data from *SalesOrder*, *OutboundDelivery*, and *WarehouseOrder* via OData/REST connectors with built-in retry, pagination, and rate-limit handling.
* **Airflow Ingestion DAGs / Custom Ingest Scripts:** DAGs located under `dags/` are responsible for scheduling, retries, authentication, and writing raw payloads into the Bronze layer (MinIO).

#### **Data Collection & Staging Layer**

* **PostgreSQL (Staging):** Stores metadata, staging records, and enables validation/reconciliation steps before moving data into Delta.
* **MinIO (Object Storage):** Hosts Bronze raw data (JSON/Parquet), intermediate Delta files, and artifacts, partitioned by date and source.

#### **Processing Layer**

* **Apache Spark (Master & Worker):** Main batch processing engine for cleansing, deduplication, enrichment (route mapping via OSRM, distance/ETA calculation), producing Silver datasets, and materialized Gold aggregates.
* **Apache Airflow (Webserver / Scheduler / Worker / Flower):** Orchestrates ETL pipelines: ingestion → transformation → validation → publishing.
* **Redis / Celery (supporting Airflow CeleryExecutor):** Provides task execution support and lightweight caching.

#### **Storage Layer (Lakehouse + Metastore)**

* **Delta Lake (local FS under `/mnt/delta`):** Stores Silver datasets in Delta format, supporting ACID transactions, time travel, and schema evolution.
* **Hive Metastore:** Serves as catalog for Spark and Trino, connected via `delta-metastore` (Thrift URI).
* **PostgreSQL:** Hosts metadata databases including `airflow-db`, `metastore-db`, `postgres-staging`, and `db-metabase`.

#### **Serving Layer**

* **ClickHouse:** OLAP database optimized for aggregated Gold tables, enabling fast interactive queries (KPIs, time series, drill-downs).
* **Trino (Coordinator):** Distributed SQL query engine federating Delta, ClickHouse, and Hive, providing a unified query point for analysts.
* **OSRM Service (osrm + osrm-prep):** Routing engine to compute paths, ETAs, and prebuild offline route profiles from `.osm.pbf`.

#### **Visualization & BI Layer**

* **Metabase:** Business intelligence and dashboarding tool for stakeholders, offering quick insights and ad-hoc analytics.
* **Grafana:** Visualization for Prometheus system and pipeline metrics.

#### **Monitoring & Metrics**

* **Prometheus & StatsD Exporter:** Collects metrics from Airflow, Spark jobs, OSRM health checks, and custom indicators (e.g., ETA error, job duration).
* **Grafana:** Provides monitoring dashboards for SLA tracking, job success/failure rates, throughput, and latency.

### 2. Data Processing (Bronze / Silver / Gold)

The system implements the **Medallion Architecture** with a structured processing flow:

* **Bronze (Raw Ingested Data):**

  * Stores unprocessed payloads from SAP APIs and external sources in JSON/Parquet format within MinIO.
  * Purpose: ensure auditability, replayability, and preservation of raw payloads for debugging and lineage tracking.

* **Silver (Cleansed & Enriched):**

  * Spark jobs perform parsing, validation, normalization of field names and types, deduplication, and enrichment (e.g., route mapping from OSRM, distance/ETA calculation, mapping warehouses to regions).
  * Silver tables are stored in Delta format, partitioned by date/source to accelerate queries.

* **Gold (Aggregated & Serving):**

  * Pre-aggregated tables and materialized views (e.g., `routes_summary`, `kpi_logistics_monthly`) are generated to power BI and analytics.
  * Gold data is optimized for fast queries, including regional, carrier, and monthly roll-ups.

Here’s a professional English rewrite of your **D. Deployment** section, tailored for your SmartLogistics project:


## D. Deployment

### 1. System Requirements

**Hardware (recommended minimum for PoC / development)**

* **CPU:** 8 vCPUs (≥12 cores recommended when running multiple services simultaneously)
* **RAM:** 32 GB
* **Storage:** 200 GB SSD (with priority allocation for `osrm-data`, ClickHouse, and Delta files)

**Software**

* **Operating System:** Linux (Ubuntu/Debian/CentOS). WSL2 may be used for development only.
* **Docker Engine:** Compatible version supporting Docker Compose v2.
* **Docker Compose:** Using the `docker compose` CLI.
* *(Optional)* **DBeaver** / `clickhouse-client` for querying.

**Default Ports (from `docker-compose.yml`)**

* Airflow Web: **8082 (host)**
* MinIO Console / API: **9001 / 9000**
* OSRM: **5000**
* ClickHouse: **8123 (HTTP) / 9009 (native)**
* Trino: **8080**
* Metabase: **3000**
* Prometheus: **9090**
* Grafana: **3001**


### 2. Deployment Steps

1. **Clone the Repository**

   ```bash
   git clone https://github.com/QuynhTramDang/smartlogistics.git
   cd smartlogistics
   ```

2. **Prepare Environment Variables & Script Permissions**

   ```bash
   cp .env.example .env   # update variables as needed
   chmod -R +x ./*
   ```

3. **Prepare OSRM Data Files (.osm.pbf)**

   * If you already have a regional `.osm.pbf` file, copy it into `./osrm-data/`.
   * To download automatically, use `download-osrm-pbf.sh` (if provided) or fetch the appropriate PBF file manually.

   **Build OSRM files (run `osrm-prep`):**

   ```bash
   docker compose run --rm osrm-prep
   ```

   This executes `osrm-extract` and `osrm-contract` (or MLD flow), outputting `.osrm` files into `./osrm-data/`.

4. **Start the Full Stack**

   ```bash
   docker compose up -d
   ```

5. **Initialize Airflow Database and Create Admin User**

   ```bash
   docker compose exec airflow-webserver airflow db upgrade

   docker compose exec airflow-webserver airflow users create \
     --username admin \
     --firstname Admin \
     --lastname User \
     --role Admin \
     --email admin@example.com
   ```

6. **Check Service Status & Logs**

   ```bash
   docker compose ps
   docker compose logs -f airflow-webserver
   docker compose logs -f osrm
   docker compose logs -f clickhouse-server
   ```

7. **Trigger DAGs / Test Ingestion**

   * Open **Airflow UI**: [http://localhost:8082](http://localhost:8082)
   * Trigger a sample ingestion DAG.
   * Verify output in MinIO (Bronze bucket) and check Spark logs for Silver/Delta dataset creation.


### 3. Connections & User Interfaces (Quick Reference)

* **Airflow Web:** [http://localhost:8082](http://localhost:8082)
* **MinIO Console:** [http://localhost:9001](http://localhost:9001) (login with `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY`)
* **OSRM API:** [http://localhost:5000](http://localhost:5000)
* **ClickHouse HTTP:** [http://localhost:8123](http://localhost:8123)
* **Trino Coordinator:** [http://localhost:8080](http://localhost:8080)
* **Metabase:** [http://localhost:3000](http://localhost:3000)
* **Grafana:** [http://localhost:3001](http://localhost:3001)

**DBeaver / ClickHouse Client**

* Host: `localhost`
* Port: `9009` (native) or `8123` (HTTP)
* Database: `default` (or configured name in repo)
* User/Password: per `clickhouse/config/users.d` (or default if unchanged)

**Metabase → Trino**

* Host: `localhost`
* Port: `8080`
* Catalog: `hive`

### 4. Visualization & Dashboards

* Build and publish core dashboards:

  * **`routes_summary`**
  * **`kpi_logistics_monthly`**

### 5. Monitoring & Alerting

* **Prometheus configuration** is located in `./prometheus`, scraping exporters such as:

  * `statsd-exporter`
  * `node-exporter`
  * Airflow exporter (if enabled)

* **Grafana:** Import sample dashboards from `grafana/Dashboard`.

* **Prometheus Alert Rules** (recommended):

  * Airflow task failures / overdue DAGs
  * Disk usage > 80%
  * Service downtime alerts for Spark, OSRM, or ClickHouse


## E. Results

### Dashboard with Metabase
  A set of interactive dashboards was built in **Metabase**, enabling stakeholders to explore KPIs, time-series trends, and drill-down insights across sales, delivery, and warehouse operations.

<img width="1561" height="1333" alt="image" src="https://github.com/user-attachments/assets/bc5dac4c-1b27-499b-abe7-f7a44b9ff122" />


<img width="1448" height="1276" alt="image" src="https://github.com/user-attachments/assets/5a51d06d-fc4d-4db2-9861-0eab5658d15f" />


### Monitoring with Grafana
  **Grafana** was integrated with Prometheus to provide real-time monitoring of system and pipeline metrics, including SLA adherence, job success/failure rates, throughput, and latency.

* **Monitoring Airflow**
  A dedicated **Airflow monitoring dashboard** allows visualization and tracking of DAG execution, task retries, and workflow health, ensuring orchestration reliability.
<img width="2480" height="1327" alt="image" src="https://github.com/user-attachments/assets/96a5f906-10a8-4b40-9869-2c370a0fba3e" />


* **Monitoring Spark**
  **Apache Spark job monitoring** was implemented to track performance, resource utilization, and system health, supporting efficient batch and transformation processing.
<img width="2473" height="1101" alt="image" src="https://github.com/user-attachments/assets/49d774a3-820e-42b4-ae27-6352d9ec47e9" />


* **Monitoring Node-Noporter**
  A monitoring module was designed for **Node-Noporter** to capture service availability, request throughput, and latency, ensuring the reliability of API-based integrations.
<img width="2473" height="1335" alt="image" src="https://github.com/user-attachments/assets/11d5da83-476a-4125-befd-00e975a2cff2" />

---

## Author  

**Dang Nguyen Quynh Tram**  

📧 Email: [quynhtramdang.ueh@gmail.com](mailto:quynhtramdang.ueh@gmail.com)  
🔗 LinkedIn: [linkedin.com/in/tramdang311](https://www.linkedin.com/in/tramdang311)  
💻 GitHub: [github.com/QuynhTramDang](https://github.com/QuynhTramDang)  

