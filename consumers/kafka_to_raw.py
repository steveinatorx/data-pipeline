#!/usr/bin/env python3
"""
Kafka -> Raw Landing (NDJSON), partitioned by ingest_date.

Writes:
  <out_dir>/events/ingest_date=YYYY-MM-DD/part-00000.ndjson

Features:
- file rolling by max_bytes or max_seconds
- partitioning by ingest_time date (ingest_date)
- optional consumer group + offset commit
- optional local checkpoint file (best-effort)

Assumes each Kafka message is a JSON object with:
  ingest_time (ISO string, e.g. "2026-02-01T10:00:00Z")
  event_id (string)

Run example:
  python consumers/kafka_to_raw.py \
    --bootstrap localhost:9092 --topic events \
    --out-dir ./data/raw \
    --group raw-sink \
    --roll-max-mb 64 --roll-max-seconds 60
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

# kafka-python
from kafka import KafkaConsumer  # type: ignore


def parse_iso_to_date(s: str) -> str:
    # Accepts "...Z" or "+00:00"
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    return dt.date().isoformat()


@dataclass
class RollPolicy:
    max_bytes: int
    max_seconds: int


class RollingWriter:
    """
    Maintains one open file per partition (ingest_date), rolling by size/time.
    """
    def __init__(self, base_dir: Path, roll: RollPolicy):
        self.base_dir = base_dir
        self.roll = roll
        self._fh_by_date: Dict[str, Any] = {}
        self._bytes_by_date: Dict[str, int] = {}
        self._opened_at_by_date: Dict[str, float] = {}
        self._partnum_by_date: Dict[str, int] = {}

    def _should_roll(self, ingest_date: str) -> bool:
        b = self._bytes_by_date.get(ingest_date, 0)
        t0 = self._opened_at_by_date.get(ingest_date, time.time())
        age = time.time() - t0
        return (b >= self.roll.max_bytes) or (age >= self.roll.max_seconds)

    def _open_new(self, ingest_date: str) -> None:
        out_dir = self.base_dir / "events" / f"ingest_date={ingest_date}"
        out_dir.mkdir(parents=True, exist_ok=True)

        part = self._partnum_by_date.get(ingest_date, 0)
        path = out_dir / f"part-{part:05d}.ndjson"
        fh = open(path, "a", encoding="utf-8")

        self._fh_by_date[ingest_date] = fh
        self._bytes_by_date[ingest_date] = 0
        self._opened_at_by_date[ingest_date] = time.time()
        self._partnum_by_date[ingest_date] = part + 1

    def write(self, ingest_date: str, obj: Dict[str, Any]) -> None:
        if ingest_date not in self._fh_by_date:
            self._open_new(ingest_date)
        elif self._should_roll(ingest_date):
            self.close(ingest_date)
            self._open_new(ingest_date)

        line = json.dumps(obj, separators=(",", ":"), ensure_ascii=False) + "\n"
        fh = self._fh_by_date[ingest_date]
        fh.write(line)
        fh.flush()

        self._bytes_by_date[ingest_date] += len(line.encode("utf-8"))

    def close(self, ingest_date: str) -> None:
        fh = self._fh_by_date.pop(ingest_date, None)
        if fh:
            fh.flush()
            fh.close()

    def close_all(self) -> None:
        for d in list(self._fh_by_date.keys()):
            self.close(d)


def load_checkpoint(path: Path) -> Optional[int]:
    if not path.exists():
        return None
    try:
        return int(path.read_text().strip())
    except Exception:
        return None


def save_checkpoint(path: Path, offset: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(offset), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Kafka -> raw NDJSON sink (partitioned by ingest_date)")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="events")
    p.add_argument("--group", default="raw-sink")

    p.add_argument("--out-dir", required=True, help="Base output dir, e.g. ./data/raw")
    p.add_argument("--checkpoint-file", default=None, help="Optional: store last seen offset (single partition only best-effort)")

    p.add_argument("--auto-offset-reset", choices=["earliest", "latest"], default="earliest")
    p.add_argument("--enable-auto-commit", action="store_true", help="Enable auto commit (default off; we commit manually).")

    p.add_argument("--poll-timeout-ms", type=int, default=1000)

    p.add_argument("--roll-max-mb", type=int, default=64)
    p.add_argument("--roll-max-seconds", type=int, default=60)

    return p.parse_args()


def main() -> None:
    args = parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    checkpoint_path = Path(args.checkpoint_file) if args.checkpoint_file else None

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        enable_auto_commit=args.enable_auto_commit,
        auto_offset_reset=args.auto_offset_reset,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=0,  # keep running
        max_poll_records=500,
    )

    # NOTE: kafka-python doesn't support "seek by timestamp" easily;
    # checkpoint_file here is best-effort and most useful if your topic has 1 partition.
    if checkpoint_path:
        last_offset = load_checkpoint(checkpoint_path)
        if last_offset is not None:
            # seek requires assigned partitions; force a poll to get assignment.
            consumer.poll(timeout_ms=1000)
            for tp in consumer.assignment():
                consumer.seek(tp, last_offset)

    roll = RollPolicy(max_bytes=args.roll_max_mb * 1024 * 1024, max_seconds=args.roll_max_seconds)
    writer = RollingWriter(out_dir, roll)

    stopping = False

    def handle_sig(_sig, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    processed = 0
    last_commit = time.time()

    print(f"[raw-sink] consuming topic={args.topic} bootstrap={args.bootstrap} group={args.group}")
    print(f"[raw-sink] writing under: {out_dir}")

    try:
        while not stopping:
            records = consumer.poll(timeout_ms=args.poll_timeout_ms)
            wrote_any = False

            for tp, msgs in records.items():
                for msg in msgs:
                    obj = msg.value  # already JSON dict
                    ingest_time = obj.get("ingest_time")
                    if not ingest_time:
                        # Skip malformed record
                        continue

                    ingest_date = parse_iso_to_date(ingest_time)
                    writer.write(ingest_date, obj)
                    processed += 1
                    wrote_any = True

                    # checkpoint best-effort
                    if checkpoint_path:
                        save_checkpoint(checkpoint_path, msg.offset + 1)

            # Manual commit periodically (if auto-commit disabled)
            if not args.enable_auto_commit and wrote_any:
                # commit offsets for all partitions polled
                if time.time() - last_commit >= 2.0:
                    consumer.commit()
                    last_commit = time.time()

            if processed and processed % 5000 == 0:
                print(f"[raw-sink] processed={processed:,}")

    finally:
        print("[raw-sink] shutting down...")
        try:
            if not args.enable_auto_commit:
                consumer.commit()
        except Exception:
            pass
        writer.close_all()
        consumer.close()
        print(f"[raw-sink] done. processed={processed:,}")


if __name__ == "__main__":
    main()
