# StayNest — Delta Lake & Lakehouse Engineering

An end-to-end Lakehouse engineering project built with **Databricks, PySpark and Delta Lake**, demonstrating Medallion Architecture, transactional data management, historical versioning, optimization and incremental data processing.

The project transforms raw StayNest hotel booking data into progressively refined **Bronze, Silver and Gold datasets**, while also demonstrating core Delta Lake capabilities such as `UPDATE`, `DELETE`, Time Travel, `RESTORE`, `OPTIMIZE`, `ZORDER` and `MERGE`.

---

## Project Overview

StayNest is a hotel-booking data platform used to demonstrate practical Lakehouse engineering patterns.

The project covers two complementary workflows:

### 1. Medallion Architecture

```text
Raw CSV Sources
      │
      ▼
┌──────────────┐
│    Bronze    │
│ Raw bookings │
└──────┬───────┘
       │
       ▼
┌─────────────────────┐
│       Silver        │
│ Cleaned & Enriched  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│        Gold         │
│ City Revenue Metrics│
└─────────────────────┘
```

### 2. Delta Lake Engineering Operations

The project also uses a dedicated Delta table to demonstrate:

```text
CREATE
  │
  ├── UPDATE
  ├── DELETE
  ├── Time Travel
  ├── RESTORE
  ├── OPTIMIZE
  ├── ZORDER
  └── MERGE
```

Together, these workflows demonstrate how transactional Lakehouse capabilities can be combined with a layered data architecture.

---

## Project Objectives

The main engineering objectives are to:

* Build a Bronze → Silver → Gold Lakehouse pipeline
* Create and manage Delta Lake tables
* Demonstrate transactional `UPDATE` and `DELETE` operations
* Use Delta Lake Time Travel and `RESTORE`
* Apply `OPTIMIZE` and `ZORDER`
* Clean and enrich data using PySpark
* Perform dimension enrichment through joins
* Identify referential-integrity issues
* Create business-ready revenue metrics
* Implement incremental ingestion using `MERGE`
* Validate transformations using row counts and data-quality checks

---

## Technology Stack

| Technology   | Purpose                                                  |
| ------------ | -------------------------------------------------------- |
| Databricks   | Lakehouse development and execution                      |
| PySpark      | Data transformation and processing                       |
| Apache Spark | Distributed data processing                              |
| Delta Lake   | ACID transactions, versioning and incremental processing |
| SQL          | Delta table operations and validation                    |
| GitHub       | Source control and portfolio management                  |

---

## Dataset

The project uses three source files stored in a Databricks Volume.

### Source files

| File                   | Description                     | Records |
| ---------------------- | ------------------------------- | ------: |
| `bookings.csv`         | Base hotel booking transactions |  12,000 |
| `hotels.csv`           | Hotel reference/dimension data  |     200 |
| `bookings_updates.csv` | Incremental update batch        |     200 |

The incremental update dataset contains:

* **150 existing booking IDs** requiring updates
* **50 new booking IDs** requiring inserts

---

# Medallion Architecture

## Bronze Layer

The Bronze layer stores the raw booking data with minimal transformation.

A technical ingestion timestamp is added:

```text
ingested_at
```

### Result

**12,000 records**

The Bronze layer provides the raw, traceable foundation for downstream processing.

---

## Silver Layer

The Silver layer applies business and data-quality transformations.

Processing includes:

* Filtering for completed bookings
* Joining bookings with hotel reference data
* Enriching booking records with:

  * Hotel name
  * Category
  * Star rating

### Result

**9,660 records**

The reduction from 10,563 completed bookings to 9,660 Silver records revealed:

**903 completed bookings without a matching hotel record.**

This demonstrates a practical referential-integrity issue that would need to be investigated in a production pipeline.

---

## Gold Layer

The Gold layer provides business-ready aggregated metrics.

The project calculates:

* Booking count
* Revenue
* Revenue by city

### Result

**10 city-level records**

The Gold dataset is designed for downstream analytics, reporting and BI consumption.

---

# Delta Lake Engineering

## Delta Table

The project creates a Delta Lake table from the source booking dataset.

Initial dataset:

**12,000 records**

The table is then used to demonstrate transactional Delta Lake capabilities.

---

## UPDATE and DELETE

The project demonstrates transactional modifications including:

* Updating booking statuses
* Deleting cancelled bookings

Delta Lake records these operations in its transaction history rather than treating the dataset as a simple static file.

---

## Time Travel

Delta Lake Time Travel is used to access a previous version of the table.

Example:

```python
version0_df = (
    spark.read
    .option("versionAsOf", 0)
    .table(bookings_table)
)
```

The historical version contained:

**12,000 records**

This demonstrates the ability to query previous states of a Delta table without maintaining separate physical copies of the dataset.

---

## RESTORE

The project also demonstrates restoring a Delta table to a previous version:

```sql
RESTORE TABLE workspace.default.bookings_delta
TO VERSION AS OF 0
```

After restoration:

**12,000 records**

This demonstrates how Delta Lake can recover a table to a known historical state.

---

## OPTIMIZE and ZORDER

The project applies Delta Lake optimization techniques:

```sql
OPTIMIZE workspace.default.bookings_delta;
```

`OPTIMIZE` compacts Delta data files to improve storage layout and query performance.

The project also demonstrates Z-Ordering:

```sql
OPTIMIZE workspace.default.bookings_delta
ZORDER BY (city);
```

`ZORDER` organizes related data within files to improve data skipping for frequently filtered columns.

For this small development dataset, the performance impact is limited, but the exercise demonstrates optimization techniques commonly used in larger Delta Lake workloads.

---

## Incremental Data Processing with MERGE

The project demonstrates incremental ingestion using Delta Lake `MERGE`.

The update batch contains:

* **150 existing booking IDs** — records requiring updates
* **50 new booking IDs** — records requiring inserts
* **200 total incremental records**

The MERGE operation updates matching records and inserts new records without creating duplicate booking IDs.

```sql
MERGE INTO workspace.default.bookings_delta AS target
USING bookings_updates_batch AS source
ON target.booking_id = source.booking_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

### MERGE Validation

| Metric                           | Result |
| -------------------------------- | -----: |
| Rows before MERGE                | 12,000 |
| Rows after MERGE                 | 12,050 |
| New rows inserted                |     50 |
| Existing rows updated            |    150 |
| Duplicate booking IDs introduced |      0 |

This demonstrates a common Lakehouse pattern for processing incremental source data.

---

## Data Validation Results

| Layer / Component                      | Result |
| -------------------------------------- | -----: |
| Source bookings                        | 12,000 |
| Source hotels                          |    200 |
| Incremental updates                    |    200 |
| Bronze bookings                        | 12,000 |
| Silver bookings                        |  9,660 |
| Completed bookings without hotel match |    903 |
| Gold city records                      |     10 |
| Delta table before MERGE               | 12,000 |
| Delta table after MERGE                | 12,050 |

The reduction from Bronze to Silver is caused by the Silver transformation requiring completed bookings with a matching hotel dimension record.

---

## Engineering Concepts Demonstrated

This project demonstrates practical implementation of:

* Lakehouse architecture
* Medallion Architecture
* PySpark DataFrame transformations
* Spark join optimization
* Broadcast joins
* Delta Lake table creation
* ACID transactional operations
* `UPDATE` and `DELETE` operations
* Delta Lake Time Travel
* `RESTORE` operations
* `OPTIMIZE`
* `ZORDER`
* Data cleaning and enrichment
* Dimensional enrichment using joins
* Business aggregation
* Incremental data processing
* Delta Lake `MERGE`
* Row-count and data-quality validation

---

## Repository Structure

```text
staynest-delta-lake-lakehouse/
│
├── README.md
│
└── notebooks/
    └── StayNest_Delta_Lake_Lakehouse_Engineering.ipynb
```

The notebook contains the complete implementation and validation steps.

---

## Running the Project

The project was developed and executed in **Databricks** using PySpark and Delta Lake.

### Prerequisites

* Databricks workspace
* PySpark
* Delta Lake
* Access to the StayNest source CSV files

### Source Data Location

The original notebook expects the source files under:

```text
/Volumes/workspace/default/staynest/
```

Expected files:

```text
bookings.csv
hotels.csv
bookings_updates.csv
```

The notebook creates the required Delta tables in:

```text
workspace.default
```

### Execution Flow

Run the notebook sections in order:

1. Project Setup & Data Validation
2. Broadcast Join & Query Plan Optimization
3. Delta Lake Table & Transaction History
4. Delta Lake Time Travel & RESTORE
5. Delta Lake Optimization
6. Bronze Layer
7. Silver Layer
8. Gold Layer
9. Incremental MERGE
10. Final Validation & Project Summary

---

## Project Outcome

This project demonstrates how raw transactional booking data can be transformed into a structured Lakehouse architecture using Databricks, PySpark and Delta Lake.

The implementation covers the complete progression from:

**Raw CSV data → Bronze → Silver → Gold**

while also demonstrating Delta Lake capabilities for:

**Transactions → Historical versioning → Recovery → Optimization → Incremental processing**

The result is a practical example of Lakehouse engineering that combines data transformation, data quality validation, performance optimization and reliable incremental ingestion.

---

## Author

**Kalyan Raj Dakuri**

MSc International Business | Data Engineering & BI

**Focus areas**

* Data Engineering
* SQL
* PySpark
* Databricks
* Delta Lake
* Power BI
* Lakehouse Architecture
