# ✈️ **Flight Data Engineering Platform**

### **An End-to-End Modern Data Pipeline (Airflow + Snowflake + dbt + Streamlit)**

![Architecture Banner](https://dummyimage.com/1200x280/001122/ffffff\&text=Flight+Data+Pipeline+-+Airflow+Snowflake+dbt+Streamlit)

---
## 🌟 **Streamlite Dashboard** 

https://flight-data-performance-airflow-dbt-mw6pkagwhhqyxmtplqjvqe.streamlit.app/


## 🌟 **Overview**

This repository contains a complete **end-to-end data engineering project**, built with a modern data stack:

| Layer              | Technology            | Purpose                                                            |
| ------------------ | --------------------- | ------------------------------------------------------------------ |
| **Ingestion**      | Python, Airflow, APIs | Fetch flight, airport, BTS, & weather datasets from public sources |
| **Storage**        | AWS S3                | Raw zone storage for ingestion                                     |
| **Warehouse**      | Snowflake             | RAW → STAGING → MARTS transformation                               |
| **Transformation** | dbt Core              | Modular SQL modeling for analytics                                 |
| **Orchestration**  | Apache Airflow        | Full DAG automation (ingest → stage → load → transform)            |
| **Visualization**  | Streamlit             | Interactive dashboard connected to live Snowflake data             |

This project is built exactly like a **real enterprise pipeline** and is designed for learning, demos, and production inspiration.

---

# 🏗️ **Architecture**

```
                ┌──────────────────────┐
                │ Public Data Sources  │
                │  (GitHub, BTS, NOAA) │
                └──────────┬───────────┘
                           │
                           ▼
                 ┌──────────────────┐
                 │  Airflow DAG     │
                 │ fetch → S3 upload│
                 └───────┬──────────┘
                         │
                         ▼
            ┌──────────────────────────┐
            │        AWS S3            │
            │  (Raw Landing Bucket)    │
            └──────────┬───────────────┘
                       │ Stage in Snowflake
                       ▼
            ┌──────────────────────────┐
            │       Snowflake RAW      │
            └──────────┬───────────────┘
                       │ dbt models
                       ▼
            ┌──────────────────────────┐
            │   STAGING_MARTS / JFK    │
            │  Analytics-ready tables  │
            └──────────┬──────────────┘
                       │
                       ▼
           ┌────────────────────────────┐
           │        Streamlit App       │
           │  (Auto-refresh Snowflake)  │
           └────────────────────────────┘
```

---

# 🚀 **Features**

### ✅ Automated Ingestion Pipeline

* Downloads CSV + ZIP files
* Extracts, uploads to S3
* Snowflake COPY INTO staging tables

### ✅ dbt Transformation

* RAW → STAGING
* STAGING → MARTS/MARTS_JFK
* Includes:

  * Airline On-Time Ranking
  * Airport Daily Performance
  * Hourly Delay Distribution
  * Weather Delay Impact (JFK AIRPORT)

### ✅ Streamlit Dashboard

* Schema & table selector
* Pagination (250 rows per batch)
* Interactive charts using Altair
* Supports STAGING_MARTS and STAGING_MARTS_JFK

---

# 📁 **Repository Structure**

```
flight_project/
│
├── dags/
│   ├── flight_pipeline.py
│   ├── upload_to_s3.py
│   └── dbt_runner.py
│
├── dbt/
│   ├── logs/
│   └── flight_project/
│       ├── analyses/
│       ├── models/
│       ├── macros/
│       ├── seeds/
│       ├── snapshots/
│       ├── logs/  (NOT in Git)
│       ├── target/ 
│       ├── tests/
│       ├── dbt_project.yml
│       └── packages.yml
│
├── logs/ (NOT in Git)
├── plugins/ (NOT in Git)
│
├── streamlit/
│   ├── dashboard.py
│   └── .streamlit/secrets.toml (NOT in Git)
│
├── .env (NOT in Git)
├── .gitignore
├── 01_db_setup.sql
├── 02_create_load_tables.sql
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

# ⚙️ **Setup Instructions**

## **1️⃣ Clone this repository**

```bash
git clone https://github.com/YOUR_USERNAME/flight-data-performance-airflow-dbt.git
cd flight-data-performance-airflow-dbt
```

---

# **2️⃣ Airflow Setup (Docker)**

### Start containers:

```bash
cd flight_project
docker-compose up --build -d
```

### Verify Airflow UI

```
http://localhost:8081
```

Login:

```
username: airflow
password: airflow
```

---

# **3️⃣ Configure Connections in Airflow UI**

### Create `aws_default`

Fill:

* Access Key
* Secret Key

### Create `snowflake_default`

Fill:

* Account
* Username & Password
* Role
* Warehouse
* Database

---

# **4️⃣ Run the DAG**

Enable:

```
flight_daily_pipeline
```

This executes:

1. Download + upload to S3
2. Create Snowflake stages
3. COPY INTO tables
4. dbt run
5. Dashboard auto-refreshes

---

# **5️⃣ Streamlit Setup (Local or Cloud)**

### Install dependencies

```bash
pip install -r requirements.txt
```

### Create secrets file

`streamlit/.streamlit/secrets.toml`

```
[snowflake]
account = "xxxx"
user = "xxxx"
password = "xxxx"
database = "FLIGHT"
warehouse = "FLIGHT_WH"
role = "ACCOUNTADMIN"
```

### Run Streamlit

```bash
cd streamlit
streamlit run dashboard.py
```

---

# 🌐 **Deploy Streamlit Online (Public Access)**

You can deploy using:

* **Streamlit Cloud** (recommended)
* **Railway**
* **Render**
* **AWS ECS**

Just upload your repo → add secrets → deploy.

---

# 📊 **Dashboard Preview**

* Pagination (250 rows at a time)
* Automatic chart builder
* STAGING_MARTS + JFK schema selector
* Auto-refresh every 5 minutes

---


---

# 🎉 **Conclusion**

This repository demonstrates a **full production-grade data pipeline**:

✔ Airflow orchestration
✔ Snowflake ELT
✔ dbt transformations
✔ Automated staging & ingestion
✔ Interactive Streamlit dashboard
✔ Real-world architecture


