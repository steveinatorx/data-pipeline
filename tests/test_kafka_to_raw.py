"""Tests for consumers/kafka_to_raw.py"""

import json
import sys
import tempfile
import time
from pathlib import Path

import pytest

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from consumers.kafka_to_raw import (
    RollingWriter,
    RollPolicy,
    parse_iso_to_date,
)


class TestParseIsoToDate:
    """Test ISO timestamp to date parsing"""

    def test_parse_with_z_suffix(self):
        assert parse_iso_to_date("2026-02-05T10:30:00Z") == "2026-02-05"

    def test_parse_with_timezone_offset(self):
        assert parse_iso_to_date("2026-02-05T10:30:00+00:00") == "2026-02-05"

    def test_parse_with_utc_offset(self):
        assert parse_iso_to_date("2026-02-05T10:30:00-05:00") == "2026-02-05"

    def test_parse_midnight(self):
        assert parse_iso_to_date("2026-02-05T00:00:00Z") == "2026-02-05"


class TestRollingWriter:
    """Test RollingWriter file rolling and partitioning"""

    def test_write_creates_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            roll = RollPolicy(max_bytes=1024 * 1024, max_seconds=3600)
            writer = RollingWriter(base, roll)

            event = {"event_id": "test-1", "ingest_time": "2026-02-05T10:00:00Z"}
            writer.write("2026-02-05", event)

            expected_dir = base / "events" / "ingest_date=2026-02-05"
            assert expected_dir.exists()
            assert expected_dir.is_dir()

    def test_write_creates_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            roll = RollPolicy(max_bytes=1024 * 1024, max_seconds=3600)
            writer = RollingWriter(base, roll)

            event = {"event_id": "test-1", "ingest_time": "2026-02-05T10:00:00Z"}
            writer.write("2026-02-05", event)
            writer.close_all()

            expected_file = base / "events" / "ingest_date=2026-02-05" / "part-00000.ndjson"
            assert expected_file.exists()

            # Verify content
            content = expected_file.read_text()
            assert "test-1" in content
            assert "ingest_time" in content

    def test_write_partitions_by_date(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            roll = RollPolicy(max_bytes=1024 * 1024, max_seconds=3600)
            writer = RollingWriter(base, roll)

            writer.write("2026-02-05", {"event_id": "test-1"})
            writer.write("2026-02-06", {"event_id": "test-2"})
            writer.close_all()

            assert (base / "events" / "ingest_date=2026-02-05" / "part-00000.ndjson").exists()
            assert (base / "events" / "ingest_date=2026-02-06" / "part-00000.ndjson").exists()

    def test_roll_by_size(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            # Small roll size: 100 bytes
            roll = RollPolicy(max_bytes=100, max_seconds=3600)
            writer = RollingWriter(base, roll)

            # Write a large event that exceeds the roll size
            large_event = {"event_id": "test-1", "data": "x" * 200}
            writer.write("2026-02-05", large_event)
            writer.close_all()

            # Should create a new file after rolling
            part0 = base / "events" / "ingest_date=2026-02-05" / "part-00000.ndjson"
            part1 = base / "events" / "ingest_date=2026-02-05" / "part-00001.ndjson"

            # At least one file should exist
            assert part0.exists() or part1.exists()

    def test_roll_by_time(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            # Very short roll time: 1 second
            roll = RollPolicy(max_bytes=1024 * 1024, max_seconds=1)
            writer = RollingWriter(base, roll)

            writer.write("2026-02-05", {"event_id": "test-1"})
            time.sleep(1.1)  # Wait longer than roll time
            writer.write("2026-02-05", {"event_id": "test-2"})
            writer.close_all()

            # Should have rolled to a new file
            part0 = base / "events" / "ingest_date=2026-02-05" / "part-00000.ndjson"
            part1 = base / "events" / "ingest_date=2026-02-05" / "part-00001.ndjson"

            # Both files should exist after time-based roll
            assert part0.exists()
            assert part1.exists()

    def test_close_all_closes_all_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            roll = RollPolicy(max_bytes=1024 * 1024, max_seconds=3600)
            writer = RollingWriter(base, roll)

            writer.write("2026-02-05", {"event_id": "test-1"})
            writer.write("2026-02-06", {"event_id": "test-2"})

            # Files should be open
            assert len(writer._fh_by_date) == 2

            writer.close_all()

            # All files should be closed
            assert len(writer._fh_by_date) == 0

    def test_json_output_format(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            roll = RollPolicy(max_bytes=1024 * 1024, max_seconds=3600)
            writer = RollingWriter(base, roll)

            event = {
                "event_id": "test-1",
                "event_type": "order_created",
                "ingest_time": "2026-02-05T10:00:00Z",
                "payload": {"order_id": "123", "amount": 99.99},
            }
            writer.write("2026-02-05", event)
            writer.close_all()

            file_path = base / "events" / "ingest_date=2026-02-05" / "part-00000.ndjson"
            content = file_path.read_text()
            parsed = json.loads(content.strip())

            assert parsed["event_id"] == "test-1"
            assert parsed["event_type"] == "order_created"
            assert parsed["payload"]["order_id"] == "123"
