"""
WHAT: API for health checks and search over loaded data.
WHEN: Start after ETL has populated the database.

INPUTS → OUTPUTS:
- INPUTS: DATABASE_URL (Postgres), ALLOWED_ORIGINS (CORS)
- OUTPUTS: /health, /search, /tool/{id}, /stats, /categories
"""

from typing import Optional, List
import os

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, text

# --- Env & DB URL fix ---------------------------------------------------------
load_dotenv()  # Loads .env during local dev so DATABASE_URL/ALLOWED_ORIGINS are available.

db_url = os.getenv("DATABASE_URL", "")
if not db_url:
    raise SystemExit("DATABASE_URL not set")

# Use psycopg (v3) driver explicitly so we don't need psycopg2.
if db_url.startswith("postgresql://"):
    db_url = db_url.replace("postgresql://", "postgresql+psycopg://", 1)

# pool_pre_ping=True avoids "stale connection" errors by pinging before use.
engine = create_engine(db_url, pool_pre_ping=True, future=True)

# --- App & CORS ---------------------------------------------------------------
# Parse allowed origins from env. You can set "*" to allow all.
origins = [
    o.strip() for o in os.getenv(
        "ALLOWED_ORIGINS",
        "http://localhost:5173,http://localhost:8000"
    ).split(",") if o.strip()
]

app = FastAPI(title="AI Tools Catalog API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if "*" in origins else origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Models -------------------------------------------------------------------
class ToolOut(BaseModel):
    """
    Pydantic model that documents the shape of a tool record we return.
    It doesn't affect the SQL—just the response schema and docs.
    """
    id: str
    name: str
    url: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = []
    categories: Optional[List[str]] = []

# --- Routes -------------------------------------------------------------------
@app.get("/health")
def health():
    """
    Quick DB heartbeat. If this returns {"ok": true}, DB creds/connection are good.
    """
    with engine.begin() as conn:
        conn.execute(text("select 1"))
    return {"ok": True}


@app.get("/search")
def search(
    q: str = "",
    category: Optional[str] = None,
    has_api: Optional[bool] = None,
    has_free: Optional[bool] = None,
    domain: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
):
    """
    Search endpoint with optional filters.
    Adds `has_more` by fetching limit+1 and trimming.
    """
    # bounds
    limit = max(1, min(limit, 100))
    offset = max(0, offset)
    q = (q or "").strip()

    # dynamic WHERE
    where = []
    params = {"limit": limit + 1, "offset": offset}  # NOTE: ask DB for one extra row
    if q:
        where.append("(tsv @@ plainto_tsquery('english', :q) OR name ILIKE :like OR description ILIKE :like)")
        params["q"] = q
        params["like"] = f"%{q}%"
    if category:
        where.append(":category = ANY(categories)")
        params["category"] = category
    if has_api is not None:
        where.append("has_api = :has_api")
        params["has_api"] = has_api
    if has_free is not None:
        where.append("has_free = :has_free")
        params["has_free"] = has_free
    if domain:
        where.append("domain = :domain")
        params["domain"] = domain

    # base SQL
    base_sql = """
        SELECT id, name, url, description, tags, categories
        FROM tools
    """
    if where:
        base_sql += " WHERE " + " AND ".join(where)

    if q:
        base_sql += """
            ORDER BY ts_rank_cd(tsv, plainto_tsquery('english', :q)) DESC,
                     updated_at DESC
        """
    else:
        base_sql += " ORDER BY updated_at DESC"

    base_sql += " LIMIT :limit OFFSET :offset"

    # query
    with engine.begin() as conn:
        rows = conn.execute(text(base_sql), params).mappings().all()

    # trim to requested page size, compute has_more
    items = [dict(r) for r in rows[:limit]]
    has_more = len(rows) > limit

    return {
        "items": items,
        "q": q,
        "category": category,
        "has_api": has_api,
        "has_free": has_free,
        "domain": domain,
        "limit": limit,
        "offset": offset,
        "has_more": has_more,
    }


@app.get("/tool/{tool_id}")
def tool(tool_id: str):
    """
    Fetch a single tool by UUID. Returns 404-style payload if not found.
    """
    with engine.begin() as conn:
        r = conn.execute(text("""
            SELECT id, name, url, description, tags, categories, domain, first_seen, updated_at
            FROM tools
            WHERE id = :id
        """), {"id": tool_id}).mappings().first()
    return dict(r) if r else {"error": "not found"}


@app.get("/stats")
def stats():
    with engine.begin() as conn:
        total = conn.execute(text("SELECT count(*) FROM tools")).scalar_one()
        by_cat = conn.execute(text("""
            SELECT cat, COUNT(*) AS n
            FROM (
              SELECT unnest(
                CASE
                  WHEN coalesce(cardinality(categories), 0) = 0
                    THEN ARRAY['uncategorized']::text[]
                  ELSE categories
                END
              ) AS cat
              FROM tools
            ) s
            GROUP BY cat
            ORDER BY n DESC
            LIMIT 20
        """)).all()
    return {"total": int(total), "top_categories": [(c, int(n)) for c, n in by_cat]}


@app.get("/categories")
def categories():
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT cat
            FROM (
              SELECT unnest(
                CASE
                  WHEN coalesce(cardinality(categories), 0) = 0
                    THEN ARRAY['uncategorized']::text[]
                  ELSE categories
                END
              ) AS cat
              FROM tools
            ) s
            ORDER BY cat
        """)).all()
    return {"categories": [r[0] for r in rows]}
