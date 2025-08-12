"""
WHAT: Normalize bronze-layer data into a clean, unified silver schema.
WHEN: After bronze ingestion completes; before gold curation and embedding.

INPUTS → OUTPUTS:
- INPUTS: Files from `data/bronze/` (CSV, Markdown-derived JSON, etc.).
- OUTPUTS: Normalized records written to `data/silver/`.

Schema (silver):
  name, url, description, tags(list[str]), categories(list[str]),
  has_api(bool), has_free(bool), domain, source
"""

from pathlib import Path
import re
import pandas as pd
import tldextract
from loguru import logger

# ------------------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------------------
BRONZE = Path("data/bronze")
SILVER = Path("data/silver"); SILVER.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------------------------
# Column alias map: maps messy vendor headers -> canonical field names
# We use `pick()` below to choose the first matching column present.
# ------------------------------------------------------------------------------
ALIASES = {
    "name":        ["name", "tool", "title", "tool_name"],
    "url":         ["url", "website", "link", "homepage"],
    "description": ["description", "desc", "summary", "about", "bio"],
    "tags":        ["tags", "keywords", "labels"],
    "categories":  ["category", "categories", "group", "section"],
    "has_api":     ["api", "has_api", "provides_api"],
    "has_free":    ["free", "has_free", "freemium"],
}

# ------------------------------------------------------------------------------
# Category normalization (Option A):
# - Map many messy strings to a small, canonical category set.
# - If no category is recognized, force ["uncategorized"].
# - Keep unknown tokens searchable by pushing them into tags.
# ------------------------------------------------------------------------------
_CANON_MAP = {
    # image
    "image generation": {"image", "image tool", "image generator", "text-to-image", "t2i", "graphics", "art"},
    "image editing":    {"photo", "image edit", "upscaler", "image enhancer", "removal", "background remover"},
    # text
    "writing":          {"copywriting", "content writing", "blog", "seo writing", "story"},
    "chatbot":          {"chat", "assistant", "conversational ai"},
    "summarization":    {"summary", "summarizer"},
    # code
    "code":             {"developer", "programming", "coding", "code generation"},
    "code assistant":   {"code assistant", "pair programmer", "copilot"},
    # audio/video
    "audio":            {"music", "voice", "speech", "tts", "stt"},
    "video":            {"video editing", "video generator"},
    # data/agent
    "search":           {"research", "web search"},
    "agents":           {"agent", "autonomous agent", "workflow", "automation"},
}
# Reverse lookup: synonym -> canonical
_REVERSE = {syn: canon for canon, syns in _CANON_MAP.items() for syn in syns}
# Splitter for list-like strings
_SPLIT = re.compile(r"[,/|;]| and |·|•|\||\u2022", re.I)

def _clean_token(s: str) -> str:
    """Lowercase, collapse whitespace, trim punctuation — keeps comparisons sane."""
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s.strip(" -_")

def normalize_categories_for_row(candidates: list[str], also_from_tags: list[str]) -> tuple[list[str], list[str]]:
    """
    Given raw category strings (plus tag hints), return:
      - cats_norm: canonical categories (may be empty before fallback)
      - unknown:   unrecognized tokens we’ll keep in tags for searchability
    """
    seen = {}
    unknown = []

    def _feed(raw):
        if not raw:
            return
        parts = _SPLIT.split(raw) if isinstance(raw, str) else raw
        for p in parts:
            tok = _clean_token(p)
            if not tok:
                continue
            # Direct synonym?
            canon = _REVERSE.get(tok)
            if canon:
                seen[canon] = True
                continue
            # Exact canonical name typed by source?
            if tok in _CANON_MAP:
                seen[tok] = True
            else:
                unknown.append(tok)

    # consider both the source categories and tags as hints
    _feed(", ".join(candidates or []))
    _feed(", ".join(also_from_tags or []))

    cats_norm = sorted(seen.keys())
    unknown = sorted(set(unknown))
    return cats_norm, unknown

# ------------------------------------------------------------------------------
# Helpers for general normalization
# ------------------------------------------------------------------------------
def pick(colnames, keys):
    """Pick the first column in `colnames` whose lowercase matches (or contains) any of `keys`."""
    s = {c.lower(): c for c in colnames}
    # exact match pass
    for k in keys:
        if k in s:
            return s[k]
    # contains pass
    for k in keys:
        for c in s:
            if k in c:
                return s[c]
    return None

def canonical_url(u):
    """Normalize URLs so identical sites dedupe: https, strip fragments/queries, trim trailing slash."""
    if not isinstance(u, str) or not u.strip():
        return None
    u = u.strip().replace("http://", "https://")
    u = re.sub(r"#.*$", "", u)
    u = re.sub(r"\?.*$", "", u)
    u = u.rstrip("/")
    return u

def listify(x):
    """
    Ensure a value is a list[str].
    - None/NaN -> []
    - list     -> strip each item
    - str      -> split on commas
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return []
    if isinstance(x, list):
        return [s for s in (str(i).strip() for i in x) if s]
    return [t.strip() for t in str(x).split(",") if t and t.strip()]

# ------------------------------------------------------------------------------
# Core normalization per source file
# ------------------------------------------------------------------------------
def normalize_one(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """
    Map messy vendor columns to the canonical schema, clean values, normalize categories,
    and guarantee each row has at least one category (fallback: ['uncategorized']).
    """
    cols = list(df.columns)
    m = {k: pick(cols, [a.lower() for a in v]) for k, v in ALIASES.items()}

    out = pd.DataFrame()
    out["name"]        = df[m["name"]]        if m["name"]        else None
    out["url"]         = df[m["url"]]         if m["url"]         else None
    out["description"] = df[m["description"]] if m["description"] else None
    out["tags"]        = df[m["tags"]]        if m["tags"]        else None
    out["categories"]  = df[m["categories"]]  if m["categories"]  else None
    out["has_api"]     = df[m["has_api"]]     if m["has_api"]     else False
    out["has_free"]    = df[m["has_free"]]    if m["has_free"]    else False

    # Basic cleaning
    out["name"]        = out["name"].astype(str).str.strip()
    out["url"]         = out["url"].apply(canonical_url)
    out["description"] = out["description"].astype(str).str.strip()
    out["tags"]        = out["tags"].apply(listify)
    out["categories"]  = out["categories"].apply(listify)
    out["has_api"]     = out["has_api"].astype(str).str.lower().isin(["true", "1", "yes", "y"])
    out["has_free"]    = out["has_free"].astype(str).str.lower().isin(["true", "1", "yes", "y"])

    # Category normalization (Option A)
    def _norm_row(row):
        cats_raw = row.get("categories", []) or []
        tags_raw = row.get("tags", []) or []

        cats_norm, unknown = normalize_categories_for_row(cats_raw, tags_raw)

        # Fallback: if nothing mapped, force into a single bucket
        if not cats_norm:
            cats_norm = ["uncategorized"]

        # Keep unknown tokens searchable via tags (without duplicating)
        row["categories"] = cats_norm
        row["tags"] = sorted(set(tags_raw + unknown))
        return row

    out = out.apply(_norm_row, axis=1)

    # Derived fields
    out["domain"] = out["url"].apply(lambda u: tldextract.extract(u).registered_domain if u else None)
    out["source"] = source_name

    # Drop invalid rows (no name)
    out = out.dropna(subset=["name"]).reset_index(drop=True)
    return out

# ------------------------------------------------------------------------------
# Entrypoint: read all bronze parquet files, normalize, write to silver
# ------------------------------------------------------------------------------
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
