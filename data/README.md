# StayNest — Input Data

## Overview

The StayNest Lakehouse project uses three CSV datasets as input sources.

The datasets are loaded into a Databricks Volume and processed using **PySpark and Delta Lake** through the Bronze, Silver and Gold layers.

The raw CSV files are intentionally **not committed to this GitHub repository**. This keeps the repository focused on the engineering implementation while avoiding unnecessary duplication of source data.

---

## Input Datasets

| File                   | Purpose                         | Records |
| ---------------------- | ------------------------------- | ------: |
| `bookings.csv`         | Base hotel booking transactions |  12,000 |
| `hotels.csv`           | Hotel reference/dimension data  |     200 |
| `bookings_updates.csv` | Incremental booking changes     |     200 |

---

## 1. `bookings.csv`

The main transactional dataset containing hotel booking records.

Key fields include:

* `booking_id` — Unique booking identifier
* `customer_id` — Customer identifier
* `hotel_id` — Hotel identifier
* `booking_date` — Booking date
* `city` — Booking city
* `nights` — Number of nights
* `amount` — Booking amount
* `status` — Booking status

**Record count:** 12,000

This dataset forms the primary input to the Bronze layer.

---

## 2. `hotels.csv`

A reference dataset containing hotel information used to enrich booking transactions.

Key fields include:

* `hotel_id` — Hotel identifier
* `hotel_name` — Hotel name
* `city` — Hotel city
* `category` — Hotel category
* `star_rating` — Hotel star rating

**Record count:** 200

The dataset is used during the Silver transformation to add hotel attributes to completed bookings.

---

## 3. `bookings_updates.csv`

An incremental dataset used to demonstrate Delta Lake `MERGE`.

The batch contains:

* **150 existing booking IDs** requiring updates
* **50 new booking IDs** requiring inserts

**Record count:** 200

The incremental batch demonstrates how a Lakehouse pipeline can process changes without replacing the complete target dataset.

---

## Databricks Storage Location

The datasets are stored in a Databricks Volume:

```text
/Volumes/workspace/default/staynest
```

The notebook reads the files using:

```python
BASE = "/Volumes/workspace/default/staynest"
```

---

## Data Flow

```text
                    Input CSV Files

        bookings.csv        hotels.csv
              │                  │
              │                  │
              ▼                  ▼
          ┌─────────────────────────┐
          │      Bronze / Silver    │
          │   Booking Transformation│
          └────────────┬────────────┘
                       │
                       ▼
                  Gold Metrics


        bookings_updates.csv
                 │
                 ▼
          Delta MERGE
                 │
                 ▼
       Existing + New Records
```

---

## Data Availability

The raw CSV files are provided as the input dataset for the Databricks implementation.

They are **not stored in this GitHub repository**.

To reproduce the project, the expected files should be available in:

```text
/Volumes/workspace/default/staynest/
```

with the following names:

```text
bookings.csv
hotels.csv
bookings_updates.csv
```

The notebook contains the required loading logic and validation checks.

---

## Data Quality Considerations

During the Silver-layer transformation, the project identified:

* **10,563** completed bookings in the Bronze dataset
* **9,660** completed bookings successfully enriched with hotel information
* **903** completed bookings without a matching hotel reference

The unmatched records are explicitly measured and documented as a data-quality finding.

In a production implementation, these records could be routed to a data-quality exception workflow for investigation.

---

## Dataset Role in the Lakehouse

| Layer            | Dataset / Output    | Purpose                                            |
| ---------------- | ------------------- | -------------------------------------------------- |
| Source           | CSV files           | Raw input                                          |
| Bronze           | `bronze_bookings`   | Raw booking ingestion                              |
| Silver           | `silver_bookings`   | Cleaning and enrichment                            |
| Gold             | `gold_city_revenue` | Business-ready city revenue metrics                |
| Delta Operations | `bookings_delta`    | Transaction, versioning and incremental processing |

---

## Reproducibility

The project is designed so that another engineer can reproduce the pipeline by providing the expected input datasets in the documented Databricks Volume location and executing the notebook.

The repository therefore contains the **engineering implementation and documentation**, while the source CSV files remain external to Git.
