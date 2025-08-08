"""
WHAT: Normalize bronze-layer data into a clean, unified silver schema.
WHEN: After bronze ingestion completes; before gold curation and embedding.

INPUTS → OUTPUTS:
- INPUTS: Files from `data/bronze/` (CSV, Markdown-derived JSON, etc.).
- OUTPUTS: Normalized records written to `data/silver/`.

Key functions to implement next:
- load_bronze()
- normalize_records()
- validate_schema()
- write_silver()
- main()
"""

# TODO: Implement normalization transformations and validations.

from pathlib import Path
import pandas as pd
import re, tldextract
from loguru import logger

"""
Maps messy headers to canonical schema and cleans values.
Input: data/bronze/*.parquet  ->  Output: data/silver/*.parquet
Columns: name,url,description,tags(list),categories(list),has_api(bool),has_free(bool),domain,source
"""

BRONZE = Path("data/bronze")
SILVER = Path("data/silver"); SILVER.mkdir(parents=True, exist_ok=True)

ALIASES = {
    "name": ["name","tool","title","tool_name"],
    "url": ["url","website","link","homepage"],
    "description": ["description","desc","summary","about","bio"],
    "tags": ["tags","keywords","labels"],
    "categories": ["category","categories","group","section"],
    "has_api": ["api","has_api","provides_api"],
    "has_free": ["free","has_free","freemium"]
}

def pick(colnames, keys):
    s = {c.lower(): c for c in colnames}
    for k in keys:
        if k in s: return s[k]
    for k in keys:
        for c in s:
            if k in c: return s[c]
    return None

def canonical_url(u):
    if not isinstance(u,str) or not u.strip(): return None
    u = u.strip().replace("http://","https://")
    u = re.sub(r"#.*$","",u)
    u = re.sub(r"\?.*$","",u)
    u = u.rstrip("/")
    return u

def listify(x):
    if x is None or (isinstance(x,float) and pd.isna(x)): return []
    if isinstance(x,list): return [str(i).strip() for i in x if str(i).strip()]
    return [t.strip() for t in str(x).split(",") if t.strip()]

def normalize_one(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    cols = list(df.columns)
    m = {k: pick(cols, [a.lower() for a in v]) for k, v in ALIASES.items()}
    out = pd.DataFrame()
    out["name"] = df[m["name"]] if m["name"] else None
    out["url"] = df[m["url"]] if m["url"] else None
    out["description"] = df[m["description"]] if m["description"] else None
    out["tags"] = df[m["tags"]] if m["tags"] else None
    out["categories"] = df[m["categories"]] if m["categories"] else None
    out["has_api"] = df[m["has_api"]] if m["has_api"] else False
    out["has_free"] = df[m["has_free"]] if m["has_free"] else False

    out["name"] = out["name"].astype(str).str.strip()
    out["url"] = out["url"].apply(canonical_url)
    out["description"] = out["description"].astype(str).str.strip()
    out["tags"] = out["tags"].apply(listify)
    out["categories"] = out["categories"].apply(listify)
    out["has_api"] = out["has_api"].astype(str).str.lower().isin(["true","1","yes","y"])
    out["has_free"] = out["has_free"].astype(str).str.lower().isin(["true","1","yes","y"])

    out["domain"] = out["url"].apply(lambda u: tldextract.extract(u).registered_domain if u else None)
    out["source"] = source_name
    out = out.dropna(subset=["name"]).reset_index(drop=True)
    return out

def run():
    total = 0
    for fp in BRONZE.glob("*.parquet"):
        df = pd.read_parquet(fp)
        clean = normalize_one(df, source_name=fp.stem)
        clean.to_parquet(SILVER / fp.name, index=False)
        logger.info(f"Normalized {fp.name}: {len(clean):,} rows -> {list(clean.columns)}")
        total += len(clean)
    logger.success(f"Silver complete. Total rows: {total:,}")

if __name__ == "__main__":
    run()

