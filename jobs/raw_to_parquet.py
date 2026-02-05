#!/usr/bin/env python3
"""
Raw NDJSON -> Parquet (partitioned by ingest_date), with dedup by event_id.

Reads:
  <raw_dir>/events/ingest_date=YYYY-MM-DD/part-*.ndjson

Writes:
  <out_dir>/events/ingest_date=YYYY-MM-DD/part-*.parquet

Dedup:
- Drops duplicate event_id within each ingest_date batch.

Schema:
- Flattens stable envelope fields to columns.
- Stores payload as JSON string (portable).
- Parses event_time/ingest_time to timestamps.

Run:
  python jobs/raw_to_parquet.py \
    --raw-dir ./data/raw \
    --out-dir ./data/parquet \
    --date 2026-02-01

  # or process all available ingest_date partitions:
  python jobs/raw_to_parquet.py --raw-dir ./data/raw --out-dir ./data/parquet --all
"""

from __future__ import annotations

import argparse
import glob
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq


def parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def list_ingest_dates(raw_dir: Path) -> List[str]:
    base = raw_dir / "events"
    dates = []
    for p in base.glob("ingest_date=*"):
        if p.is_dir():
            dates.append(p.name.split("=", 1)[1])
    return sorted(dates)


def read_ndjson_files(paths: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for path in paths:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    # skip malformed
                    continue
    return rows


def normalize_rows(rows: List[Dict[str, Any]], ingest_date: str) -> pa.Table:
    """
    Normalize JSON envelope rows into an Arrow Table with stable columns.
    payload is stored as JSON string to keep schema evolution painless.
    """
    cols = {
        "event_id": [],
        "event_type": [],
        "schema_version": [],
        "event_time": [],
        "ingest_time": [],
        "tenant_id": [],
        "user_id": [],
        "session_id": [],
        "source_system": [],
        "environment": [],
        "record_source": [],
        "checksum": [],
        "payload": [],
        "ingest_date": [],
    }

    for r in rows:
        payload_obj = r.get("payload")
        payload_str = None
        if payload_obj is not None:
            try:
                payload_str = json.dumps(payload_obj, separators=(",", ":"), ensure_ascii=False)
            except Exception:
                payload_str = None

        cols["event_id"].append(r.get("event_id"))
        cols["event_type"].append(r.get("event_type"))
        cols["schema_version"].append(r.get("schema_version"))
        cols["event_time"].append(r.get("event_time"))
        cols["ingest_time"].append(r.get("ingest_time"))
        cols["tenant_id"].append(r.get("tenant_id"))
        cols["user_id"].append(r.get("user_id"))
        cols["session_id"].append(r.get("session_id"))
        cols["source_system"].append(r.get("source_system"))
        cols["environment"].append(r.get("environment"))
        cols["record_source"].append(r.get("record_source"))
        cols["checksum"].append(r.get("checksum"))
        cols["payload"].append(payload_str)
        cols["ingest_date"].append(ingest_date)

    # Build Arrow arrays
    table = pa.table(
        {
            "event_id": pa.array(cols["event_id"], type=pa.string()),
            "event_type": pa.array(cols["event_type"], type=pa.string()),
            "schema_version": pa.array(cols["schema_version"], type=pa.int32()),
            "event_time": pa.array(cols["event_time"], type=pa.string()),
            "ingest_time": pa.array(cols["ingest_time"], type=pa.string()),
            "tenant_id": pa.array(cols["tenant_id"], type=pa.string()),
            "user_id": pa.array(cols["user_id"], type=pa.string()),
            "session_id": pa.array(cols["session_id"], type=pa.string()),
            "source_system": pa.array(cols["source_system"], type=pa.string()),
            "environment": pa.array(cols["environment"], type=pa.string()),
            "record_source": pa.array(cols["record_source"], type=pa.string()),
            "checksum": pa.array(cols["checksum"], type=pa.string()),
            "payload": pa.array(cols["payload"], type=pa.string()),
            "ingest_date": pa.array(cols["ingest_date"], type=pa.string()),
        }
    )

    # Cast timestamps (keep timezone by parsing ISO -> timestamp[us, tz=UTC] if possible)
    # Arrow's timestamp parsing works well if we parse to timestamps via compute.strptime
    # We'll keep as UTC naive for portability: timestamp('us') after parsing.
    # (DuckDB/Spark can interpret; Iceberg stage can enforce TZ.)
    event_ts = pc.strptime(table["event_time"], format="%Y-%m-%dT%H:%M:%S%z", unit="us", error_is_null=True)
    ingest_ts = pc.strptime(table["ingest_time"], format="%Y-%m-%dT%H:%M:%S%z", unit="us", error_is_null=True)

    table = table.set_column(table.schema.get_field_index("event_time"), "event_time", event_ts)
    table = table.set_column(table.schema.get_field_index("ingest_time"), "ingest_time", ingest_ts)

    # Cast ingest_date to date32
    ingest_date_arr = pc.strptime(table["ingest_date"], format="%Y-%m-%d", unit="s", error_is_null=True)
    # ingest_date_arr is timestamp; cast to date
    table = table.set_column(
        table.schema.get_field_index("ingest_date"),
        "ingest_date",
        pc.cast(ingest_date_arr, pa.date32()),
    )

    return table


def dedup_by_event_id(table: pa.Table) -> pa.Table:
    """
    Deduplicate rows by event_id, keeping the first occurrence.
    """
    # Filter out null event_id rows first (optional)
    not_null = pc.is_valid(table["event_id"])
    table = table.filter(not_null)

    # Find first occurrence indices per key
    # Approach: use dictionary encode + unique + group_by? simplest:
    # Use pyarrow.compute.unique on event_id then take first index via match_substring? not ideal.
    # We'll do a stable dedup by building a boolean mask in Python for simplicity at local scales.
    seen = set()
    keep = []
    event_ids = table["event_id"].to_pylist()
    for eid in event_ids:
        if eid in seen:
            keep.append(False)
        else:
            seen.add(eid)
            keep.append(True)

    return table.filter(pa.array(keep))


def write_parquet_partition(out_dir: Path, ingest_date: str, table: pa.Table, rows_per_file: int) -> None:
    dest = out_dir / "events" / f"ingest_date={ingest_date}"
    dest.mkdir(parents=True, exist_ok=True)

    n = table.num_rows
    if n == 0:
        return

    # chunk into multiple parquet files
    file_ix = 0
    for start in range(0, n, rows_per_file):
        chunk = table.slice(start, min(rows_per_file, n - start))
        path = dest / f"part-{file_ix:05d}.parquet"
        pq.write_table(
            chunk,
            path,
            compression="zstd",
            use_dictionary=True,
            write_statistics=True,
        )
        file_ix += 1


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Convert raw NDJSON partitions to Parquet.")
    p.add_argument("--raw-dir", required=True, help="Base raw dir, e.g. ./data/raw")
    p.add_argument("--out-dir", required=True, help="Base parquet dir, e.g. ./data/parquet")
    p.add_argument("--date", default=None, help="Ingest date to process, YYYY-MM-DD")
    p.add_argument("--all", action="store_true", help="Process all available ingest_date partitions")
    p.add_argument("--rows-per-file", type=int, default=250_000, help="Max rows per parquet output file")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    raw_dir = Path(args.raw_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if (args.date is None) == (args.all is False):
        print("ERROR: Provide --date YYYY-MM-DD or --all", file=sys.stderr)
        raise SystemExit(2)

    dates = [args.date] if args.date else list_ingest_dates(raw_dir)

    for ingest_date in dates:
        part_dir = raw_dir / "events" / f"ingest_date={ingest_date}"
        paths = sorted(glob.glob(str(part_dir / "part-*.ndjson")))
        if not paths:
            print(f"[raw_to_parquet] no files for ingest_date={ingest_date}, skipping")
            continue

        print(f"[raw_to_parquet] ingest_date={ingest_date} files={len(paths)} ...")
        rows = read_ndjson_files(paths)
        table = normalize_rows(rows, ingest_date)
        before = table.num_rows
        table = dedup_by_event_id(table)
        after = table.num_rows

        write_parquet_partition(out_dir, ingest_date, table, rows_per_file=args.rows_per_file)
        print(f"[raw_to_parquet] wrote ingest_date={ingest_date} rows={after:,} (deduped {before-after:,})")

    print("[raw_to_parquet] done")


if __name__ == "__main__":
    import sys
    main()
