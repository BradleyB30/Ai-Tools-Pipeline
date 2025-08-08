"""
WHAT: Deduplicate, score, and curate silver records into high-quality gold data.
WHEN: After silver normalization; before embedding and database load.

INPUTS → OUTPUTS:
- INPUTS: Normalized silver records from `data/silver/`.
- OUTPUTS: Curated gold records written to `data/gold/`.

Key functions to implement next:
- load_silver()
- compute_content_hashes()
- deduplicate_records()
- select_best_record_per_group()
- write_gold()
- main()
"""

# TODO: Implement deduplication strategy and curation logic.

from pathlib import Path
import pandas as pd
from loguru import logger
from itertools import chain

"""
Merges all silver files into one deduped dataset.
Primary key: canonical URL. Fallback: (domain, name) when URL missing.
Keeps longest description; unions tags/categories; bools OR'ed.
Output: data/gold/tools.parquet
"""

SILVER = Path("data/silver")
GOLD_DIR = Path("data/gold"); GOLD_DIR.mkdir(parents=True, exist_ok=True)

def _flatten_unique(series_of_lists):
    s = list(chain.from_iterable(series_of_lists.dropna().tolist()))
    return sorted(set([x for x in s if str(x).strip()]))

def _longest_desc(g):
    s = g["description"].fillna("")
    return s.loc[s.str.len().idxmax()] if not s.empty else None

def _agg_group(g):
    return pd.Series({
        "name": g["name"].iloc[0],
        "url": g["url"].iloc[0] if g["url"].notna().any() else None,
        "description": _longest_desc(g),
        "tags": _flatten_unique(g["tags"]),
        "categories": _flatten_unique(g["categories"]),
        "has_api": bool(g["has_api"].any()),
        "has_free": bool(g["has_free"].any()),
        "domain": g["domain"].iloc[0]
    })

def run():
    files = list(SILVER.glob("*.parquet"))
    if not files:
        logger.error("No silver files.")
        return
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)

    with_url = df[df["url"].notna()].copy()
    no_url = df[df["url"].isna()].copy()

    g1 = with_url.groupby("url", as_index=False).apply(_agg_group).reset_index(drop=True)
    g2 = pd.DataFrame()
    if not no_url.empty:
        g2 = no_url.groupby(["domain","name"], as_index=False).apply(_agg_group).reset_index(drop=True)

    out = pd.concat([g1, g2], ignore_index=True)
    out_path = GOLD_DIR / "tools.parquet"
    out.to_parquet(out_path, index=False)
    logger.success(f"Gold written: {len(out):,} rows -> {out_path}")

if __name__ == "__main__":
    run()

