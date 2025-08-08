"""
WHAT: API for health checks and search over loaded data.
WHEN: Start after ETL has populated the database.

INPUTS → OUTPUTS:
- INPUTS: DATABASE_URL (Postgres), ALLOWED_ORIGINS (CORS)
- OUTPUTS: /health, /search, /tool/{id}, /stats
"""

from typing import Optional, List
import os

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, text

# --- Env & DB URL fix ---------------------------------------------------------
load_dotenv()  # read .env in dev

db_url = os.getenv("DATABASE_URL", "")
if not db_url:
    raise SystemExit("DATABASE_URL not set")

# Force SQLAlchemy to use psycopg v3 instead of psycopg2
if db_url.startswith("postgresql://"):
    db_url = db_url.replace("postgresql://", "postgresql+psycopg://", 1)

engine = create_engine(db_url, pool_pre_ping=True, future=True)

# --- App & CORS ---------------------------------------------------------------
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
    id: str
    name: str
    url: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = []
    categories: Optional[List[str]] = []

# --- Routes -------------------------------------------------------------------
@app.get("/health")
def health():
    # Simple DB ping
    with engine.begin() as conn:
        conn.execute(text("select 1"))
    return {"ok": True}

@app.get("/search")
def search(q: str = "", limit: int = 20, offset: int = 0):
    q = (q or "").strip()
    with engine.begin() as conn:
        if q:
            rows = conn.execute(text("""
                select id, name, url, description, tags, categories
                from tools
                where tsv @@ plainto_tsquery('english', :q)
                   or name ilike :like
                   or description ilike :like
                order by ts_rank_cd(tsv, plainto_tsquery('english', :q)) desc, updated_at desc
                limit :limit offset :offset
            """), {"q": q, "like": f"%{q}%", "limit": limit, "offset": offset}).mappings().all()
        else:
            rows = conn.execute(text("""
                select id, name, url, description, tags, categories
                from tools
                order by updated_at desc
                limit :limit offset :offset
            """), {"limit": limit, "offset": offset}).mappings().all()

    return {"items": [dict(r) for r in rows], "q": q, "limit": limit, "offset": offset}

@app.get("/tool/{tool_id}")
def tool(tool_id: str):
    with engine.begin() as conn:
        r = conn.execute(text("""
            select id, name, url, description, tags, categories, domain, first_seen, updated_at
            from tools
            where id = :id
        """), {"id": tool_id}).mappings().first()
    return dict(r) if r else {"error": "not found"}

@app.get("/stats")
def stats():
    with engine.begin() as conn:
        total = conn.execute(text("select count(*) from tools")).scalar_one()
        by_cat = conn.execute(text("""
            with cats as (
              select u.cat
              from tools t
              left join lateral unnest(
                case
                  when t.categories is null or array_length(t.categories, 1) = 0
                    then array['uncategorized']::text[]
                  else t.categories
                end
              ) as u(cat) on true
            )
            select cat, count(*) as n
            from cats
            group by cat
            order by n desc
            limit 20
        """)).all()
    return {"total": int(total), "top_categories": [(c, int(n)) for c, n in by_cat]}
