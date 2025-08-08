"""
WHAT: Ingest raw CSV files into standardized bronze-layer files under `data/bronze`.
WHEN: First step of the ETL (bronze stage); runs before normalization and enrichment.

INPUTS → OUTPUTS:
- INPUTS: CSV files matching `CSV_GLOB` in `data/sources/`.
- OUTPUTS: Cleaned CSV/Parquet files written to `data/bronze/`.

Key functions to implement next:
- discover_csv_files()
- read_csv_files()
- standardize_schema()
- write_bronze_dataset()
- main()
"""

# TODO: Implement the bronze CSV ingestion as described above.

from pathlib import Path
import pandas as pd, hashlib
from loguru import logger

"""
Reads all CSVs in data/sources -> raw Parquet in data/bronze (adds __source_file/__source_sha).
Run early to snapshot inputs.
"""

SRC = Path("data/sources")
OUT = Path("data/bronze"); OUT.mkdir(parents=True, exist_ok=True)

def _sha16(p: Path):
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1<<20), b""): h.update(chunk)
    return h.hexdigest()[:16]

def run():
    files = sorted(SRC.glob("*.csv"))
    if not files:
        logger.error("No CSVs in data/sources")
        return
    total = 0
    for fp in files:
        df = pd.read_csv(fp)
        df["__source_file"] = fp.name
        df["__source_sha"] = _sha16(fp)
        out = OUT / f"{fp.stem}.parquet"
        df.to_parquet(out, index=False)
        logger.info(f"Wrote {out.name}: {len(df):,} rows, cols={list(df.columns)}")
        total += len(df)
    logger.success(f"Bronze CSV complete. Total rows: {total:,}")

if __name__ == "__main__":
    run()

