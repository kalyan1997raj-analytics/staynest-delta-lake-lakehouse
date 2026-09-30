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

The project applies:

```sql
OPTIMIZE workspace.default.bookings_delta
```

and:

```sql
OPTIMIZE workspace.default.bookings_delta
ZORDER BY (city)
```

### OPTIMIZE

Compacts Delta data fil

