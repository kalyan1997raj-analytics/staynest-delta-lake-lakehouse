# StayNest — Lakehouse Architecture

## 1. Architecture Overview

StayNest implements a layered Lakehouse architecture using **Databricks, PySpark and Delta Lake**.

The pipeline transforms raw hotel-booking data through three progressively refined layers:

```text
                         StayNest Lakehouse

┌─────────────────────────────────────────────────────────────┐
│                        Source Data                           │
│                                                             │
│  bookings.csv     hotels.csv     bookings_updates.csv       │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                         BRONZE                              │
│                                                             │
│  Raw booking data                                           │
│  + ingestion timestamp                                      │
│                                                             │
│  12,000 records                                             │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                         SILVER                              │
│                                                             │
│  Completed bookings                                         │
│  + hotel enrichment                                         │
│  + data-quality filtering                                   │
│                                                             │
│  9,660 records                                              │
│                                                             │
│  903 completed bookings without matching hotel             │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                          GOLD                               │
│                                                             │
│  City-level business metrics                                │
│  • Booking count                                            │
│  • Revenue                                                  │
│                                                             │
│  10 city-level records                                      │
└─────────────────────────────────────────────────────────────┘
```

A separate Delta Lake engineering workflow is used to demonstrate transactional and operational capabilities such as:

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

---

## 2. Source Data

The project uses three CSV datasets stored in a Databricks Volume.

| Source                 | Purpose                         | Records |
| ---------------------- | ------------------------------- | ------: |
| `bookings.csv`         | Base hotel booking transactions |  12,000 |
| `hotels.csv`           | Hotel reference/dimension data  |     200 |
| `bookings_updates.csv` | Incremental booking changes     |     200 |

### Incremental Dataset

The incremental batch contains:

* 150 existing booking IDs requiring updates
* 50 new booking IDs requiring inserts

This allows the project to demonstrate an incremental ingestion pattern using Delta Lake `MERGE`.

---

## 3. Bronze Layer

### Purpose

The Bronze layer acts as the raw ingestion layer.

The source booking dataset is loaded with minimal transformation and written as a Delta table.

A technical ingestion timestamp is added:

```python
from pyspark.sql.functions import current_timestamp

bronze_df = bookings_df.withColumn(
    "ingested_at",
    current_timestamp()
)
```

### Characteristics

* Preserves the source booking records
* Adds ingestion metadata
* Uses Delta Lake as the storage format
* Provides the foundation for downstream transformations

### Result

**12,000 records**

---

## 4. Silver Layer

### Purpose

The Silver layer applies data-quality and enrichment logic to the Bronze dataset.

The transformation performs two main operations:

1. Filters bookings to completed transactions.
2. Enriches bookings using the hotel reference dataset.

Conceptually:

```text
Bronze bookings
      │
      ├── Filter status = completed
      │
      ▼
Completed bookings
      │
      ├── Join with hotel dimension
      │
      ▼
Enriched Silver dataset
```

Hotel attributes added include:

* Hotel name
* Category
* Star rating

### Data Quality Finding

The Bronze dataset contains **10,563 completed bookings**.

After joining with the hotel reference data, the Silver layer contains:

**9,660 records**

This identified:

**903 completed bookings without a matching hotel record.**

The issue is retained as a documented data-quality finding rather than being hidden.

In a production pipeline, these unmatched records could be routed to a data-quality exception process for investigation.

---

## 5. Gold Layer

### Purpose

The Gold layer provides business-ready aggregated data.

The Silver dataset is grouped by city and aggregated into:

* Booking count
* Revenue

Conceptually:

```text
Silver bookings
      │
      ▼
Group by city
      │
      ├── COUNT(booking_id)
      └── SUM(amount)
      │
      ▼
Gold city revenue dataset
```

### Result

**10 city-level records**

The Gold dataset is suitable for downstream analytics, reporting and BI consumption.

---

## 6. Delta Lake Transaction Management

The project uses a dedicated Delta table to demonstrate transactional data management.

The initial Delta table contains:

**12,000 records**

The project then demonstrates:

### UPDATE

Booking statuses are updated transactionally.

### DELETE

Cancelled bookings are removed through a Delta transaction.

### Transaction History

Delta Lake history is inspected using:

```sql
DESCRIBE HISTORY workspace.default.bookings_delta;
```

This provides visibility into table operations and transaction history.

---

## 7. Time Travel

Delta Lake Time Travel allows previous versions of the table to be queried.

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

This demonstrates how previous table states can be accessed without creating separate physical copies of the dataset.

---

## 8. RESTORE

The project restores the Delta table to a previous version:

```sql
RESTORE TABLE workspace.default.bookings_delta
TO VERSION AS OF 0;
```

After restoration, the table contains:

**12,000 records**

RESTORE demonstrates how Delta Lake can recover a table to a known historical state.

---

## 9. OPTIMIZE and ZORDER

The project demonstrates Delta Lake storage optimization.

### OPTIMIZE

```sql
OPTIMIZE workspace.default.bookings_delta;
```

`OPTIMIZE` compacts Delta data files to improve storage layout and query performance.

### ZORDER

```sql
OPTIMIZE workspace.default.bookings_delta
ZORDER BY (city);
```

`ZORDER` organizes related data within files to improve data skipping for frequently filtered columns.

Because the development dataset is relatively small, measurable performance improvement is limited. The implementation demonstrates the optimization techniques used for larger Delta workloads.

---

## 10. Incremental MERGE

The project implements incremental processing using Delta Lake `MERGE`.

The update batch contains:

* 150 existing records
* 50 new records

The MERGE logic matches records using `booking_id`.

```sql
MERGE INTO workspace.default.bookings_delta AS target
USING bookings_updates_batch AS source
ON target.booking_id = source.booking_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

### Result

| Metric                           | Result |
| -------------------------------- | -----: |
| Rows before MERGE                | 12,000 |
| Rows after MERGE                 | 12,050 |
| Existing records updated         |    150 |
| New records inserted             |     50 |
| Duplicate booking IDs introduced |      0 |

This demonstrates a common incremental ingestion pattern in Lakehouse architectures.

---

## 11. Spark Join Optimization

The project also evaluates join strategy using Spark execution plans.

The booking dataset is joined with the smaller hotel dataset.

The hotel dimension is small enough for Spark/Photon to select a broadcast join strategy.

The project also tests an explicit broadcast:

```python
from pyspark.sql.functions import broadcast

broadcast_join = bookings_df.join(
    broadcast(hotels_df.drop("city")),
    "hotel_id"
)
```

The execution plan confirms a `BroadcastHashJoin`.

This demonstrates how broadcasting a small dimension can avoid shuffling the larger booking dataset across the join key.

---

## 12. Data Quality and Validation

The pipeline validates the transformation at multiple stages.

| Component                    | Result |
| ---------------------------- | -----: |
| Source bookings              | 12,000 |
| Source hotels                |    200 |
| Incremental updates          |    200 |
| Bronze bookings              | 12,000 |
| Silver bookings              |  9,660 |
| Unmatched completed bookings |    903 |
| Gold city records            |     10 |
| Delta table before MERGE     | 12,000 |
| Delta table after MERGE      | 12,050 |

These checks provide evidence that the transformations and incremental processing behaved as expected.

---

## 13. Design Principles

The StayNest implementation follows several Lakehouse engineering principles:

### Layered Processing

Each layer has a distinct responsibility:

```text
Bronze → ingestion
Silver → cleaning and enrichment
Gold   → business aggregation
```

### Transactional Storage

Delta Lake provides transactional table operations and version history.

### Incremental Processing

`MERGE` enables existing records to be updated while new records are inserted.

### Data Quality Visibility

Unmatched hotel records are explicitly identified rather than silently discarded without measurement.

### Separation of Engineering and Business Layers

The project separates:

* Raw ingestion
* Transformation and enrichment
* Business aggregation
* Delta operational demonstrations

This makes the pipeline easier to understand and extend.

---

## 14. Technology Architecture

```text
┌──────────────────────┐
│     CSV Sources      │
│ Bookings / Hotels /  │
│ Incremental Updates  │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│      Databricks      │
│      + PySpark       │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│      Delta Lake      │
│                      │
│ Bronze → Silver → Gold
│                      │
│ Time Travel          │
│ RESTORE              │
│ UPDATE / DELETE      │
│ OPTIMIZE / ZORDER     │
│ MERGE                │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ Analytics / BI       │
│ Business Metrics     │
└──────────────────────┘
```

---

## 15. Project Outcome

The StayNest project demonstrates an end-to-end Lakehouse engineering workflow using Databricks, PySpark and Delta Lake.

It combines:

* Layered data architecture
* Distributed data processing
* Data enrichment
* Data-quality validation
* Transactional Delta operations
* Historical versioning
* Table recovery
* Storage optimization
* Incremental ingestion

The project therefore demonstrates both **data pipeline development** and **Lakehouse engineering concepts** in a single practical implementation.
