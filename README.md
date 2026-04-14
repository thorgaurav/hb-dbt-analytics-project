# Analytics Engineer dbt Project

## 📊 Overview

This project models rental listing data to analyze marketplace performance, pricing trends, and availability constraints using dbt and DuckDB.

The goal is to transform raw listing, calendar, and amenities data into a clean, analytics-ready data model that answers key business questions.

---

## 🧱 Data Model

The project follows a layered dbt architecture:

* **Staging**

  * Cleans raw source data
  * Handles nulls and deduplication
  * Standardizes column names and types

* **Intermediate**

  * Enriches calendar data
  * Implements **SCD Type 2** for amenities

* **Mart**

  * `fct_listing_day` (grain: listing × day)
  * Derived marts for business analysis

---

## 🔑 Key Features

* ✅ SCD Type 2 modeling (amenities history)
* ✅ Incremental fact table (`fct_listing_day`)
* ✅ Data quality tests using `dbt_utils` and `dbt_expectations`
* ✅ Exposure for downstream dashboard
* ✅ Business-ready marts
* ✅ Advanced SQL (window functions, gaps & islands)

---

## 📈 Business Questions Answered

### 1. Revenue by Air Conditioning

* Calculated total revenue and % contribution by month
* Segmented by listings with/without AC
* (SQL in /analyses/q1_amenity_revenue.sql)

### 2. Neighborhood Pricing Trends

* Measured average price increase per neighborhood
* Based on point-in-time comparison
* (SQL in /analyses/q2_neighborhood_pricing.sql)

### 3. Longest Possible Stay

* Identified longest consecutive availability windows
* Applied constraints using `maximum_nights`
* Used gaps-and-islands technique
* (SQL in /analyses/q3_long_stay.sql)

---

## ⚙️ Tech Stack

* dbt
* DuckDB
* Python

---

## 🚀 How to Run

```bash
pip install -r requirements.txt
dbt deps
dbt seed
dbt run
dbt test
dbt docs generate
```

---

## 🧠 Key Design Decisions

* **Listing-day grain** enables flexible analysis of availability and revenue
* **SCD Type 2** used to track changing amenities over time
* **Staging layer** handles data quality issues (nulls, duplicates)
* **Marts** provide business-focused outputs on top of reusable fact table
* **Seeds** are used to load CSV data into DuckDB for reproducibility

---

## 📂 Project Structure

```
models/
  staging/
  intermediate/
  marts/
analyses/
macros/
snapshots/
seeds/
```

---

## 📌 Author

Gaurav Thorat
