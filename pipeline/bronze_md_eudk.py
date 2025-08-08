"""
WHAT: Fetch and store raw Markdown documents from the EUDK source into the bronze layer.
WHEN: Bronze stage; runs alongside CSV ingestion before normalization.

INPUTS → OUTPUTS:
- INPUTS: Remote index/content from `EUDK_RAW_URL`.
- OUTPUTS: Raw Markdown files (or JSON) stored under `data/bronze/eudk/`.

Key functions to implement next:
- fetch_remote_index()
- download_documents()
- sanitize_markdown()
- persist_bronze_md()
- main()
"""

# TODO: Implement EUDK Markdown retrieval and persistence to bronze.

import os, re
from pathlib import Path
import httpx, pandas as pd
from loguru import logger

"""
Fetches eudk README raw -> parses [Name](url) — desc, captures H2 section as category.
Writes data/bronze/eudk.parquet
"""

RAW_URL = os.getenv("EUDK_RAW_URL", "https://raw.githubusercontent.com/eudk/awesome-ai-tools/main/README.md")
OUT = Path("data/bronze/eudk.parquet"); OUT.parent.mkdir(parents=True, exist_ok=True)

def run():
    r = httpx.get(RAW_URL, timeout=30)
    r.raise_for_status()
    lines = r.text.splitlines()
    curr_cat = None
    H2 = re.compile(r"^##\s+(.+)")
    LINK = re.compile(r"\[([^\]]+)\]\((https?://[^\)]+)\)")
    rows = []
    for line in lines:
        h = H2.match(line)
        if h:
            curr_cat = h.group(1).strip()
            continue
        m = LINK.search(line)
        if not m:
            continue
        name, url = m.group(1).strip(), m.group(2).strip()
        desc = line[m.end():].strip(" –—:- ") or None
        rows.append({"name": name, "url": url, "description": desc, "categories": curr_cat, "source": "eudk"})
    df = pd.DataFrame(rows)
    df.to_parquet(OUT, index=False)
    logger.success(f"eudk bronze: {len(df):,} rows -> {OUT}")

if __name__ == "__main__":
    run()

