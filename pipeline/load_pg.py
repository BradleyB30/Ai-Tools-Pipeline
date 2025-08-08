# pipeline/load_pg.py
"""
Loads data/gold/tools.parquet into Postgres using a staging table and UPSERT logic.

- Reads GOLD parquet
- Normalizes list columns to Postgres text[] literals
- COPY -> stage_tools (regular table, truncated each run)
- Upsert into tools (by url), then update/insert rows with null url
- Logs counts so you can see what landed
"""

import os
from pathlib import Path

import pandas as pd
import psycopg
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

GOLD = Path("data/gold/tools.parquet")


def to_pg_array(xs):
    """Convert Python list to Postgres text[] literal like {a,b}."""
    if not isinstance(xs, list) or not xs:
        return "{}"
    clean = [
        str(x)
        .replace("{", " ")
        .replace("}", " ")
        .replace('"', "")
        .replace(",", " ")
        .strip()
        for x in xs
        if str(x).strip()
    ]
    return "{" + ",".join(clean) + "}"


def run():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL not set")

    # psycopg v3 native connect prefers postgresql:// (not the SQLAlchemy +psycopg hint)
    if db_url.startswith("postgresql+psycopg://"):
        db_url = db_url.replace("postgresql+psycopg://", "postgresql://", 1)

    if not GOLD.exists():
        raise SystemExit(f"Gold parquet not found at {GOLD}")

    df = pd.read_parquet(GOLD)
    logger.info(f"Gold rows: {len(df):,}")

    # Ensure Postgres-friendly types/format
    df = df.assign(
        tags=df["tags"].apply(to_pg_array),
        categories=df["categories"].apply(to_pg_array),
        has_api=df["has_api"].astype(bool),
        has_free=df["has_free"].astype(bool),
    )

    cols = ["name", "url", "description", "tags", "categories", "has_api", "has_free", "domain"]
    csv_data = df[cols].to_csv(index=False)

    with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
        # Regular staging table (not TEMP) so it survives autocommit; truncate each run
        cur.execute(
            """
            create table if not exists stage_tools (
                name text,
                url text,
                description text,
                tags text[],
                categories text[],
                has_api boolean,
                has_free boolean,
                domain text
            );
            truncate table stage_tools;
            """
        )

        # COPY CSV string into stage table
        with cur.copy(
            """
            copy stage_tools (name,url,description,tags,categories,has_api,has_free,domain)
            from stdin with (format csv, header true)
            """
        ) as cp:
            cp.write(csv_data)

        stage_cnt = cur.execute("select count(*) from stage_tools").fetchone()[0]
        logger.info(f"Staged rows: {stage_cnt:,}")

        # 1) Upsert rows with a URL (unique constraint on tools.url)
        cur.execute(
            """
            insert into tools (id, name, url, description, tags, categories, has_api, has_free, domain, updated_at)
            select gen_random_uuid(), name, url, description, tags, categories, has_api, has_free, domain, now()
            from stage_tools
            where url is not null
            on conflict (url) do update
            set name         = excluded.name,
                description  = coalesce(excluded.description, tools.description),
                tags         = coalesce(excluded.tags, tools.tags),
                categories   = coalesce(excluded.categories, tools.categories),
                has_api      = tools.has_api or excluded.has_api,
                has_free     = tools.has_free or excluded.has_free,
                domain       = coalesce(excluded.domain, tools.domain),
                updated_at   = now();
            """
        )

        # 2) Rows without URL: update by (name, domain) else insert
        cur.execute(
            """
            update tools t
               set description = coalesce(s.description, t.description),
                   tags        = coalesce(s.tags, t.tags),
                   categories  = coalesce(s.categories, t.categories),
                   has_api     = t.has_api or s.has_api,
                   has_free    = t.has_free or s.has_free,
                   updated_at  = now()
              from stage_tools s
             where s.url is null
               and t.name = s.name
               and (t.domain is not distinct from s.domain);

            insert into tools (id, name, url, description, tags, categories, has_api, has_free, domain)
            select gen_random_uuid(), name, null, description, tags, categories, has_api, has_free, domain
              from stage_tools s
             where s.url is null
               and not exists (
                   select 1 from tools t
                    where t.name = s.name
                      and (t.domain is not distinct from s.domain)
               );
            """
        )

        total = cur.execute("select count(*) from tools").fetchone()[0]
        logger.success(f"Load complete. tools rows now: {total:,}")


if __name__ == "__main__":
    run()
