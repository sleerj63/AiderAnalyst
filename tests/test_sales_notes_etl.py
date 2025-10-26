import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipelines.sales_notes_etl import (
    SalesNotesConfig,
    extract_crm_customers,
    extract_dim_customers,
    extract_sales_notes,
    transform_sales_notes,
    validate_records,
    run_pipeline,
)


def write_csv(path: Path, rows):
    if not rows:
        raise ValueError("Rows are required")
    headers = rows[0].keys()
    with path.open("w", encoding="utf-8", newline="") as csvfile:
        csvfile.write(",".join(headers) + "\n")
        for row in rows:
            csvfile.write(",".join(str(row[h]) for h in headers) + "\n")


def build_sample_sources(tmp_path):
    notes_path = tmp_path / "notes.csv"
    crm_path = tmp_path / "crm.csv"
    dim_path = tmp_path / "dim.csv"

    write_csv(
        notes_path,
        [
            {
                "note_id": "n1",
                "customer_external_id": "ext-1",
                "created_at": "2023-01-01T12:30:00+00:00",
                "author": "rep-1",
                "content": "Follow up",
            }
        ],
    )
    write_csv(
        crm_path,
        [
            {
                "external_id": "ext-1",
                "customer_id": "cust-1",
                "name": "Acme",
            }
        ],
    )
    write_csv(
        dim_path,
        [
            {
                "customer_id": "cust-1",
                "segment": "enterprise",
            }
        ],
    )
    return notes_path, crm_path, dim_path


def test_transform_sales_notes_success(tmp_path):
    notes_path, crm_path, dim_path = build_sample_sources(tmp_path)

    config = SalesNotesConfig(
        sales_notes_source=notes_path,
        crm_customers_source=crm_path,
        dim_customers_source=dim_path,
    )

    raw_notes = extract_sales_notes(config)
    crm_customers = extract_crm_customers(config)
    dim_customers = extract_dim_customers(config)

    transformed, quarantined = transform_sales_notes(
        raw_notes=raw_notes,
        crm_customers=crm_customers,
        dim_customers=dim_customers,
        schema_mapping=config.schema_mapping,
    )

    assert len(transformed) == 1
    assert not quarantined
    record = transformed[0]
    assert record["note_id"] == "n1"
    assert record["customer_id"] == "cust-1"
    assert isinstance(record["created_at"], datetime)
    assert record["created_at"].tzinfo == timezone.utc


def test_transform_sales_notes_quarantines_invalid(tmp_path):
    notes_path = tmp_path / "notes.csv"
    crm_path = tmp_path / "crm.csv"
    dim_path = tmp_path / "dim.csv"

    write_csv(
        notes_path,
        [
            {
                "note_id": "n2",
                "customer_external_id": "missing",
                "created_at": "not-a-date",
                "author": "rep-1",
                "content": "Bad note",
            }
        ],
    )
    write_csv(crm_path, [{"external_id": "ext-1", "customer_id": "cust-1"}])
    write_csv(dim_path, [{"customer_id": "cust-1", "segment": "enterprise"}])

    config = SalesNotesConfig(
        sales_notes_source=notes_path,
        crm_customers_source=crm_path,
        dim_customers_source=dim_path,
        quarantine_dir=tmp_path / "quarantine",
    )

    raw_notes = extract_sales_notes(config)
    transformed, quarantined = transform_sales_notes(
        raw_notes=raw_notes,
        crm_customers={},
        dim_customers={},
        schema_mapping=config.schema_mapping,
        quarantine_dir=config.quarantine_dir,
    )

    assert not transformed
    assert len(quarantined) == 1
    quarantine_file = config.quarantine_dir / "n2.json"
    assert quarantine_file.exists()
    data = json.loads(quarantine_file.read_text())
    assert data["quarantine_reason"]


def test_validate_records_enforces_constraints():
    valid_record = {
        "note_id": "n1",
        "customer_id": "cust-1",
        "created_at": datetime.now(timezone.utc),
    }
    validate_records([valid_record], expected_min_rows=1)

    with pytest.raises(ValueError):
        validate_records([], expected_min_rows=1)

    with pytest.raises(ValueError):
        invalid_record = {
            "note_id": "n2",
            "customer_id": "",
            "created_at": datetime.now(timezone.utc),
        }
        validate_records([invalid_record], expected_min_rows=1)


def test_run_pipeline_end_to_end(tmp_path, monkeypatch, caplog):
    notes_path, crm_path, dim_path = build_sample_sources(tmp_path)
    caplog.set_level("INFO")

    monkeypatch.setenv("SALES_NOTES_SOURCE", str(notes_path))
    monkeypatch.setenv("CRM_CUSTOMERS_SOURCE", str(crm_path))
    monkeypatch.setenv("DIM_CUSTOMERS_SOURCE", str(dim_path))

    config = SalesNotesConfig.from_env()
    run_pipeline(config)

    assert any("Pipeline completed successfully" in message for message in caplog.messages)
