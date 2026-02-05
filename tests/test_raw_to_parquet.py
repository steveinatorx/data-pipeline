"""Tests for jobs/raw_to_parquet.py"""

import json
import sys
import tempfile
from pathlib import Path

import pyarrow as pa
import pytest

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from jobs.raw_to_parquet import (
    dedup_by_event_id,
    list_ingest_dates,
    normalize_rows,
    read_ndjson_files,
)


class TestListIngestDates:
    """Test listing ingest dates from directory structure"""

    def test_list_ingest_dates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir) / "events"
            base.mkdir(parents=True)

            # Create some date partitions
            (base / "ingest_date=2026-02-05").mkdir()
            (base / "ingest_date=2026-02-06").mkdir()
            (base / "ingest_date=2026-02-07").mkdir()

            dates = list_ingest_dates(Path(tmpdir))
            assert "2026-02-05" in dates
            assert "2026-02-06" in dates
            assert "2026-02-07" in dates
            assert len(dates) == 3

    def test_list_ingest_dates_sorted(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir) / "events"
            base.mkdir(parents=True)

            (base / "ingest_date=2026-02-07").mkdir()
            (base / "ingest_date=2026-02-05").mkdir()
            (base / "ingest_date=2026-02-06").mkdir()

            dates = list_ingest_dates(Path(tmpdir))
            assert dates == ["2026-02-05", "2026-02-06", "2026-02-07"]

    def test_list_ingest_dates_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dates = list_ingest_dates(Path(tmpdir))
            assert dates == []


class TestReadNdjsonFiles:
    """Test reading NDJSON files"""

    def test_read_single_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test.ndjson"
            file_path.write_text('{"event_id": "1"}\n{"event_id": "2"}\n')

            rows = read_ndjson_files([str(file_path)])
            assert len(rows) == 2
            assert rows[0]["event_id"] == "1"
            assert rows[1]["event_id"] == "2"

    def test_read_multiple_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            file1 = Path(tmpdir) / "file1.ndjson"
            file1.write_text('{"event_id": "1"}\n')
            file2 = Path(tmpdir) / "file2.ndjson"
            file2.write_text('{"event_id": "2"}\n')

            rows = read_ndjson_files([str(file1), str(file2)])
            assert len(rows) == 2

    def test_read_skips_empty_lines(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test.ndjson"
            file_path.write_text('{"event_id": "1"}\n\n{"event_id": "2"}\n')

            rows = read_ndjson_files([str(file_path)])
            assert len(rows) == 2

    def test_read_skips_malformed_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test.ndjson"
            file_path.write_text('{"event_id": "1"}\n{invalid json}\n{"event_id": "2"}\n')

            rows = read_ndjson_files([str(file_path)])
            assert len(rows) == 2  # Should skip the malformed line


class TestNormalizeRows:
    """Test normalizing JSON rows to Arrow table"""

    def test_normalize_basic_event(self):
        rows = [
            {
                "event_id": "evt-1",
                "event_type": "order_created",
                "schema_version": 1,
                "event_time": "2026-02-05T10:00:00Z",
                "ingest_time": "2026-02-05T10:01:00Z",
                "tenant_id": "tenant-1",
                "user_id": "user-1",
                "payload": {"order_id": "123"},
            }
        ]

        table = normalize_rows(rows, "2026-02-05")

        assert table.num_rows == 1
        assert table.num_columns == 14  # All envelope fields + ingest_date

        # Check some key fields
        event_ids = table["event_id"].to_pylist()
        assert event_ids[0] == "evt-1"

        event_types = table["event_type"].to_pylist()
        assert event_types[0] == "order_created"

        # Check payload is JSON string
        payloads = table["payload"].to_pylist()
        assert payloads[0] is not None
        parsed_payload = json.loads(payloads[0])
        assert parsed_payload["order_id"] == "123"

    def test_normalize_with_missing_fields(self):
        rows = [
            {
                "event_id": "evt-1",
                "event_type": "order_created",
                # Missing some optional fields
            }
        ]

        table = normalize_rows(rows, "2026-02-05")
        assert table.num_rows == 1

        # Missing fields should be None
        tenant_ids = table["tenant_id"].to_pylist()
        assert tenant_ids[0] is None

    def test_normalize_sets_ingest_date(self):
        rows = [{"event_id": "evt-1", "event_type": "test"}]
        table = normalize_rows(rows, "2026-02-05")

        ingest_dates = table["ingest_date"].to_pylist()
        # ingest_date is converted to date32, so we check the string representation
        assert str(ingest_dates[0]) == "2026-02-05"

    def test_normalize_multiple_rows(self):
        rows = [
            {"event_id": "evt-1", "event_type": "test"},
            {"event_id": "evt-2", "event_type": "test"},
            {"event_id": "evt-3", "event_type": "test"},
        ]

        table = normalize_rows(rows, "2026-02-05")
        assert table.num_rows == 3


class TestDedupByEventId:
    """Test deduplication by event_id"""

    def test_dedup_removes_duplicates(self):
        table = pa.table(
            {
                "event_id": ["evt-1", "evt-2", "evt-1", "evt-3", "evt-2"],
                "event_type": ["a", "b", "c", "d", "e"],
            }
        )

        result = dedup_by_event_id(table)
        assert result.num_rows == 3

        event_ids = result["event_id"].to_pylist()
        assert "evt-1" in event_ids
        assert "evt-2" in event_ids
        assert "evt-3" in event_ids

    def test_dedup_keeps_first_occurrence(self):
        table = pa.table(
            {
                "event_id": ["evt-1", "evt-1", "evt-1"],
                "event_type": ["first", "second", "third"],
            }
        )

        result = dedup_by_event_id(table)
        assert result.num_rows == 1

        event_types = result["event_type"].to_pylist()
        assert event_types[0] == "first"  # Should keep first occurrence

    def test_dedup_no_duplicates(self):
        table = pa.table(
            {
                "event_id": ["evt-1", "evt-2", "evt-3"],
                "event_type": ["a", "b", "c"],
            }
        )

        result = dedup_by_event_id(table)
        assert result.num_rows == 3

    def test_dedup_filters_null_event_ids(self):
        table = pa.table(
            {
                "event_id": ["evt-1", None, "evt-2", None],
                "event_type": ["a", "b", "c", "d"],
            }
        )

        result = dedup_by_event_id(table)
        assert result.num_rows == 2

        event_ids = result["event_id"].to_pylist()
        assert None not in event_ids
        assert "evt-1" in event_ids
        assert "evt-2" in event_ids
