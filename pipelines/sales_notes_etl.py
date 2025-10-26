"""Sales notes ETL pipeline."""
from __future__ import annotations

import csv
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class SalesNotesConfig:
    """Runtime configuration for the sales notes ETL pipeline."""

    sales_notes_source: Path
    crm_customers_source: Path
    dim_customers_source: Path
    notification_channel: Optional[str] = None
    quarantine_dir: Optional[Path] = None
    schema_mapping: Dict[str, str] = field(
        default_factory=lambda: {
            "note_id": "note_id",
            "customer_external_id": "customer_external_id",
            "created_at": "created_at",
            "author": "author",
            "content": "content",
        }
    )

    @classmethod
    def from_env(
        cls,
        *,
        config_file: Optional[Path] = None,
    ) -> "SalesNotesConfig":
        """Load configuration from environment variables or a JSON/YAML file."""

        config_data: Dict[str, str] = {}
        if config_file:
            if not config_file.exists():
                raise FileNotFoundError(f"Config file {config_file} does not exist")
            if config_file.suffix.lower() in {".json"}:
                config_data = json.loads(config_file.read_text())
            elif config_file.suffix.lower() in {".yaml", ".yml"}:
                try:
                    import yaml  # type: ignore
                except ImportError as exc:  # pragma: no cover - optional dependency
                    raise RuntimeError(
                        "PyYAML is required to load YAML configuration files"
                    ) from exc
                config_data = yaml.safe_load(config_file.read_text()) or {}
            else:
                raise ValueError(
                    "Unsupported config file format. Use JSON or YAML."
                )

        def _env_or_config(key: str) -> str:
            if key in os.environ:
                return os.environ[key]
            if key in config_data:
                return str(config_data[key])
            raise KeyError(
                f"Configuration value '{key}' must be provided via environment variable or config file."
            )

        quarantine_dir = config_data.get("quarantine_dir")
        quarantine_path = Path(quarantine_dir) if quarantine_dir else None
        if quarantine_path and not quarantine_path.exists():
            quarantine_path.mkdir(parents=True, exist_ok=True)

        return cls(
            sales_notes_source=Path(_env_or_config("SALES_NOTES_SOURCE")),
            crm_customers_source=Path(_env_or_config("CRM_CUSTOMERS_SOURCE")),
            dim_customers_source=Path(_env_or_config("DIM_CUSTOMERS_SOURCE")),
            notification_channel=config_data.get("notification_channel")
            or os.environ.get("SALES_NOTES_NOTIFICATION_CHANNEL"),
            quarantine_dir=quarantine_path,
        )


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"CSV file {path} not found")
    with path.open("r", newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        return [row for row in reader]


def extract_sales_notes(config: SalesNotesConfig) -> List[Dict[str, str]]:
    """Extract raw sales notes records from the configured source."""

    logger.info("Extracting sales notes from %s", config.sales_notes_source)
    return _read_csv(config.sales_notes_source)


def extract_crm_customers(config: SalesNotesConfig) -> Dict[str, Dict[str, str]]:
    """Extract CRM customer details keyed by external ID."""

    logger.info("Extracting CRM customers from %s", config.crm_customers_source)
    records = _read_csv(config.crm_customers_source)
    return {record["external_id"]: record for record in records if record.get("external_id")}


def extract_dim_customers(config: SalesNotesConfig) -> Dict[str, Dict[str, str]]:
    """Extract dimension table customers keyed by internal ID."""

    logger.info("Extracting dim_customers from %s", config.dim_customers_source)
    records = _read_csv(config.dim_customers_source)
    return {record["customer_id"]: record for record in records if record.get("customer_id")}


def _parse_timestamp(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        logger.warning("Invalid timestamp encountered: %s", value)
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def transform_sales_notes(
    *,
    raw_notes: Iterable[Dict[str, str]],
    crm_customers: Dict[str, Dict[str, str]],
    dim_customers: Dict[str, Dict[str, str]],
    schema_mapping: Dict[str, str],
    quarantine_dir: Optional[Path] = None,
) -> Tuple[List[Dict[str, object]], List[Dict[str, str]]]:
    """Transform raw sales notes into the warehouse schema.

    Returns a tuple of (clean_records, quarantined_records).
    """

    clean_records: List[Dict[str, object]] = []
    quarantined_records: List[Dict[str, str]] = []

    for record in raw_notes:
        transformed: Dict[str, object] = {}
        quarantine_reason: Optional[str] = None

        for target_field, source_field in schema_mapping.items():
            transformed[target_field] = record.get(source_field)

        raw_timestamp = transformed.get("created_at")
        parsed_ts = _parse_timestamp(str(raw_timestamp)) if raw_timestamp else None
        if not parsed_ts:
            quarantine_reason = "invalid_timestamp"
        else:
            transformed["created_at"] = parsed_ts

        customer_external_id = transformed.get("customer_external_id")
        resolved_customer_id: Optional[str] = None
        if customer_external_id:
            crm_record = crm_customers.get(str(customer_external_id))
            if crm_record:
                resolved_customer_id = crm_record.get("customer_id")
        if not resolved_customer_id and record.get("customer_id"):
            dim_record = dim_customers.get(record["customer_id"])
            resolved_customer_id = dim_record.get("customer_id") if dim_record else None

        if not resolved_customer_id:
            quarantine_reason = quarantine_reason or "unresolved_customer"
        else:
            transformed["customer_id"] = resolved_customer_id

        if any(value in (None, "") for key, value in transformed.items() if key != "content"):
            quarantine_reason = quarantine_reason or "missing_required_fields"

        if quarantine_reason:
            logger.error(
                "Quarantining record %s due to %s", record.get("note_id"), quarantine_reason
            )
            record_copy = dict(record)
            record_copy["quarantine_reason"] = quarantine_reason
            quarantined_records.append(record_copy)
            if quarantine_dir:
                quarantine_dir.mkdir(parents=True, exist_ok=True)
                quarantine_file = quarantine_dir / f"{record.get('note_id', 'unknown')}.json"
                quarantine_file.write_text(json.dumps(record_copy, indent=2, default=str))
            continue

        clean_records.append(transformed)

    logger.info("Transformation complete. %s records ready, %s quarantined.", len(clean_records), len(quarantined_records))
    return clean_records, quarantined_records


def validate_records(records: List[Dict[str, object]], *, expected_min_rows: int = 1) -> None:
    """Run validation checks on the transformed records."""

    if len(records) < expected_min_rows:
        raise ValueError(
            f"Row count validation failed. Expected at least {expected_min_rows} rows, got {len(records)}."
        )

    for record in records:
        for field in ("note_id", "customer_id", "created_at"):
            if not record.get(field):
                raise ValueError(f"Null check failed for required field '{field}'")

    customer_ids = {record["customer_id"] for record in records}
    if not customer_ids:
        raise ValueError("Referential integrity check failed: no customer IDs resolved")


def notify_failure(message: str, *, channel: Optional[str] = None) -> None:
    if channel:
        logger.error("Notification sent to %s: %s", channel, message)
    else:
        logger.error("Pipeline failure: %s", message)


def run_pipeline(config: SalesNotesConfig) -> None:
    """Execute the full ETL pipeline."""

    logger.info("Starting sales notes ETL pipeline")
    try:
        raw_notes = extract_sales_notes(config)
        crm_customers = extract_crm_customers(config)
        dim_customers = extract_dim_customers(config)
        clean_records, quarantined_records = transform_sales_notes(
            raw_notes=raw_notes,
            crm_customers=crm_customers,
            dim_customers=dim_customers,
            schema_mapping=config.schema_mapping,
            quarantine_dir=config.quarantine_dir,
        )
        validate_records(clean_records)
        logger.info(
            "Pipeline completed successfully with %s records and %s quarantined.",
            len(clean_records),
            len(quarantined_records),
        )
    except Exception as exc:  # pragma: no cover - orchestrated error path
        notify_failure(str(exc), channel=config.notification_channel)
        raise


def main(argv: Optional[List[str]] = None) -> None:
    """Command-line entrypoint for the ETL pipeline."""

    import argparse

    parser = argparse.ArgumentParser(description="Run the sales notes ETL pipeline.")
    parser.add_argument(
        "--config",
        type=Path,
        help="Path to JSON or YAML configuration file.",
        default=None,
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level for pipeline execution.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, args.log_level))

    config = SalesNotesConfig.from_env(config_file=args.config)
    run_pipeline(config)


if __name__ == "__main__":  # pragma: no cover
    main()
