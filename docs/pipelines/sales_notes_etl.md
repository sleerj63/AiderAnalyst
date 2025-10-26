# Sales Notes ETL Pipeline

## Overview

The Sales Notes ETL pipeline extracts sales note interactions, resolves them to customer records, and prepares them for loading into the analytics warehouse. The implementation lives in [`pipelines/sales_notes_etl.py`](../../pipelines/sales_notes_etl.py).

## Configuration

Configuration values are supplied via environment variables or an optional JSON/YAML config file. The loader follows this precedence:

1. Environment variables
2. Values defined in the config file provided with `--config`

Required keys:

| Variable | Description |
| --- | --- |
| `SALES_NOTES_SOURCE` | Path to the sales notes CSV export. |
| `CRM_CUSTOMERS_SOURCE` | Path to the CRM customers CSV export. |
| `DIM_CUSTOMERS_SOURCE` | Path to the `dim_customer` CSV snapshot. |

Optional keys:

| Variable | Description |
| --- | --- |
| `SALES_NOTES_NOTIFICATION_CHANNEL` | Destination for failure notifications (e.g., Slack webhook name). |
| `quarantine_dir` (config file only) | Directory where invalid records are written as JSON for manual review. |

Each CSV file must contain headers. The expected columns are documented via `SalesNotesConfig.schema_mapping` within the pipeline module and can be overridden if a different schema mapping is required.

## Running Locally

```bash
export SALES_NOTES_SOURCE="/path/to/sales_notes.csv"
export CRM_CUSTOMERS_SOURCE="/path/to/crm_customers.csv"
export DIM_CUSTOMERS_SOURCE="/path/to/dim_customers.csv"
python -m pipelines.sales_notes_etl --log-level INFO
```

To supply a config file (JSON or YAML):

```bash
python -m pipelines.sales_notes_etl --config configs/sales_notes.json
```

## Tests

Validation tests live in [`tests/test_sales_notes_etl.py`](../../tests/test_sales_notes_etl.py) and can be executed with:

```bash
pytest
```

The tests cover:

- Row counts on transformed data
- Null checks on required columns
- Referential integrity for customer IDs
- Quarantine behavior for invalid records

## Monitoring and Notifications

The pipeline logs progress via the standard Python `logging` package. Any exception raised during pipeline execution triggers `notify_failure`, which logs the error and includes the configured notification channel when available. Quarantined records are stored as JSON in the configured directory to assist with troubleshooting.
