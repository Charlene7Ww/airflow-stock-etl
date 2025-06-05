# Stock Market ETL with Airflow

This project is a complete end-to-end stock data pipeline built with **Apache Airflow**, designed to automatically fetch, transform, and load stock market data into a PostgreSQL database for visualization in Metabase.

---

## Why this project?

As someone who enjoys investing and has made my best returns on NVDA, I wanted to build a pipeline that could help me **track daily stock performance** with a scalable, production-style workflow — while also practicing **real-world Data Engineering skills**.

This project is part of my learning journey to transition from analytics into Data Engineering.

---

## Tech Stack

- **Apache Airflow (Astro Platform)** – DAG scheduling and orchestration
- **MinIO** – S3-compatible object storage for intermediate files
- **Docker** – For isolated data transformation tasks
- **PostgreSQL** – Data warehouse to store final results
- **Metabase** – Dashboard to visualize daily stock trends
- **Yahoo Finance API** – Free source for historical stock data
- **Astro SDK** – Used for simplified data loading

---

## Workflow Overview

1. **Sensor** checks the availability of the Yahoo Finance API
2. **PythonOperator** pulls raw data for `$NVDA`
3. Raw JSON is saved to **MinIO**
4. **DockerOperator** transforms the data using a Python script
5. Transformed CSV is loaded into **PostgreSQL**
6. **Metabase** visualizes daily prices, volumes, and trends

---

## Example: NVDA Dashboard

Includes:
- **Close Price (Line Chart)** — NVDA daily closing price
- **Volume (Bar Chart)** — daily trading volume
- **Two KPI Blocks** — show latest average price and average volume

> I plan to expand this to QQQ and VOO for broader ETF tracking and performance comparison.

---

## Project Structure
```
dags/
└── stock_market.py              # Airflow DAG definition

include/
└── stock_market/
    ├── tasks.py                # Python logic for API call & file save
    └── format_prices.py        # Docker-run transformation script
```


