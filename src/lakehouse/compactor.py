"""Batch compaction worker for Lakehouse Parquet partitions."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config.config import settings

logger = logging.getLogger(__name__)


@dataclass
class CompactionStats:
    """Summary statistics from a partition compaction run."""

    partitions_processed: int
    files_merged: int
    records_compacted: int
    bytes_before: int
    bytes_after: int

    @property
    def compression_ratio(self) -> float:
        if self.bytes_before == 0:
            return 1.0
        return round(self.bytes_after / self.bytes_before, 3)

    @property
    def savings_percent(self) -> float:
        if self.bytes_before == 0:
            return 0.0
        return round((1.0 - (self.bytes_after / self.bytes_before)) * 100.0, 2)


class LakehouseCompactor:
    """Solves the small-file problem by merging streaming micro-batches into consolidated partitions."""

    def __init__(self, base_dir: Path | None = None) -> None:
        self.base_dir = base_dir or settings.local_lakehouse_dir
        self.silver_dir = self.base_dir / "silver" / "aircraft_telemetry"

    def compact_partition(self, partition_dir: Path) -> tuple[int, int, int, int]:
        """Compact all non-compacted parquet files in a single leaf partition directory.

        Returns (files_merged, records, bytes_before, bytes_after).
        """
        # Find all uncompacted batch files
        source_files = [
            f
            for f in partition_dir.glob("telemetry_*.parquet")
            if not f.name.startswith("compacted_")
        ]

        if len(source_files) <= 1:
            return (0, 0, 0, 0)

        bytes_before = sum(f.stat().st_size for f in source_files)
        dfs = [pq.ParquetFile(f).read().to_pandas() for f in source_files]
        merged_df = pd.concat(dfs, ignore_index=True)
        merged_table = pa.Table.from_pandas(merged_df, preserve_index=False)
        total_records = len(merged_df)

        # Write consolidated file
        compacted_ts = int(datetime.now(UTC).timestamp())
        compacted_path = partition_dir / f"compacted_{compacted_ts}.parquet"
        pq.write_table(merged_table, compacted_path, compression="zstd")

        bytes_after = compacted_path.stat().st_size

        # Remove source small files after verifying write
        for f in source_files:
            try:
                f.unlink()
            except OSError as e:
                logger.warning("Could not delete uncompacted file %s: %s", f, e)

        logger.info(
            "Compacted %d files (%d rows) in %s from %d to %d bytes (%.1f%% reduction)",
            len(source_files),
            total_records,
            partition_dir.name,
            bytes_before,
            bytes_after,
            (1 - bytes_after / bytes_before) * 100 if bytes_before > 0 else 0,
        )

        return (len(source_files), total_records, bytes_before, bytes_after)

    def run_compaction(self) -> CompactionStats:
        """Scan all silver partitions and compact where needed."""
        total_parts = 0
        total_files = 0
        total_records = 0
        total_before = 0
        total_after = 0

        if not self.silver_dir.exists():
            return CompactionStats(0, 0, 0, 0, 0)

        # Leaf directories containing parquet files
        leaf_dirs = {p.parent for p in self.silver_dir.glob("**/*.parquet")}

        for p_dir in leaf_dirs:
            f_count, r_count, b_before, b_after = self.compact_partition(p_dir)
            if f_count > 0:
                total_parts += 1
                total_files += f_count
                total_records += r_count
                total_before += b_before
                total_after += b_after

        return CompactionStats(
            partitions_processed=total_parts,
            files_merged=total_files,
            records_compacted=total_records,
            bytes_before=total_before,
            bytes_after=total_after,
        )
