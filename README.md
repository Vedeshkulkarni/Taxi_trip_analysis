# Taxi_trip_analysis


# 🚖 Taxi Trip Analysis using Databricks Delta Live Tables

## 📌 Project Overview

This project implements an end-to-end data engineering pipeline for taxi trip data using the Medallion Architecture (Bronze → Silver → Gold) in Databricks. The pipeline ingests raw CSV files, performs data cleaning and transformations, creates business-ready analytical tables, and visualizes key insights using Databricks Dashboards.

---

## 🚀 Technologies Used

* Databricks Community Edition
* PySpark
* Delta Live Tables (DLT)
* Delta Lake
* Unity Catalog
* SQL
* Databricks Dashboards

---

## 📂 Project Architecture

Raw CSV Files
↓
Bronze Layer (Raw Data)
↓
Silver Layer (Cleaned & Validated Data)
↓
Gold Layer (Business Aggregations)
↓
Databricks Dashboard

---

## 🥉 Bronze Layer

* Ingests raw taxi trip CSV files using Auto Loader.
* Stores data without transformations.
* Acts as the landing layer for incoming data.

---

## 🥈 Silver Layer

* Cleans and standardizes the raw data.
* Renames columns.
* Converts data types.
* Removes invalid or null records.
* Calculates trip duration.

---

## 🥇 Gold Layer

The Gold layer contains business-ready analytical tables, including:

* Revenue Analysis
* Distance Analysis
* Passenger Analysis
* Pickup Location Analysis
* Dropoff Location Analysis
* Time Analysis

These tables are used for dashboard visualizations.

---

## 📊 Dashboard

The dashboard provides insights such as:

* Total Trips
* Total Revenue
* Average Fare
* Average Trip Duration
* Trips by Distance Range
* Revenue by Pickup Location
* Passenger Distribution
* Peak Pickup Hours

---

## 📁 Project Structure

```text
Taxi_trip_analysis/
│── bronze/
│── silver/
│── gold/
│── notebooks/
│── dashboard/
│── README.md
```

---

## ▶️ How to Run

1. Upload the CSV dataset to a Databricks Volume.
2. Create the Bronze DLT table.
3. Execute the Silver transformations.
4. Build the Gold aggregation tables.
5. Refresh the DLT pipeline.
6. Open the Databricks Dashboard to view the latest analytics.

---

## 📈 Features

* Incremental data ingestion
* Automated DLT pipeline
* Medallion Architecture
* Business-ready Gold tables
* Interactive dashboards
* Scalable Delta Lake storage

---

##
