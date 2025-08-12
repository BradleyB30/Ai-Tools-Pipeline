"""
WHAT: Deduplicate, score, and curate silver records into high-quality gold data.
WHEN: After silver normalization; before embedding and database load.

INPUTS → OUTPUTS:
- INPUTS: Normalized silver records from `data/silver/`.
- OUTPUTS: Curated gold records written to `data/gold/`.

Strategy:
- Group by canonical URL when present; otherwise by (domain, name).
- Keep the longest non-empty name/description within each group.
- Union tags and categories so we never drop signal.
- Booleans are OR'ed (any()).
"""

from pathlib import Path
from itertools import chain
import pandas as pd
from loguru import logger

SILVER = Path("data/silver")
GOLD_DIR = Path("data/gold"); GOLD_DIR.mkdir(parents=True, exist_ok=True)

def _flatten_unique(series_of_lists):
    """Union list[str] across a group; drop empties; return sorted list."""
    items = []
    for v in series_of_lists:
        if isinstance(v, list):
            items.extend([s for s in v if isinstance(s, str) and s.strip()])
    return sorted(set(items))

def _pick_longest_str(series: pd.Series):
    """Choose the longest non-empty string; fall back to first non-null or None."""
    candidates = [s for s in series if isinstance(s, str) and s.strip()]
    if candidates:
        return max(candidates, key=len)
    # fallback: first non-null if any
    non_null = series.dropna()
    return non_null.iloc[0] if len(non_null) else None

def _first_non_null(series: pd.Series):
    non_null = series.dropna()
    return non_null.iloc[0] if len(non_null) else None

def _agg_group(g: pd.DataFrame) -> pd.Series:
    """Aggregate one duplicate group into a single curated record."""
    return pd.Series({
        "name":        _pick_longest_str(g["name"]),
        "url":         _first_non_null(g["url"]),
        "description": _pick_longest_str(g["description"]),
        "tags":        _flatten_unique(g["tags"]),
        "categories":  _flatten_unique(g["categories"]),
        "has_api":     bool(g["has_api"].any()),
        "has_free":    bool(g["has_free"].any()),
        "domain":      _first_non_null(g["domain"]),
    })

def run():
    files = list(SILVER.glob("*.parquet"))
    if not files:
        logger.error("No silver files.")
        return

    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)

    # Split by presence of URL (strongest dedupe key)
    with_url = df[df["url"].notna()].copy()
    no_url   = df[df["url"].isna()].copy()

    g1 = with_url.groupby("url", as_index=False).apply(_agg_group).reset_index(drop=True)

    # Fallback key for rows with missing URL
    if not no_url.empty:
        g2 = no_url.groupby(["domain", "name"], as_index=False).apply(_agg_group).reset_index(drop=True)
        out = pd.concat([g1, g2], ignore_index=True)
    else:
        out = g1

    out_path = GOLD_DIR / "tools.parquet"
    out.to_parquet(out_path, index=False)
    logger.success(f"Gold written: {len(out):,} rows -> {out_path}")

if __name__ == "__main__":
    run()
