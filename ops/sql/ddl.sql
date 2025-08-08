create extension if not exists pg_trgm;
create extension if not exists vector;

create table if not exists tools (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  url text unique,
  description text,
  tags text[],
  categories text[],
  has_api boolean,
  has_free boolean,
  domain text,
  first_seen timestamptz default now(),
  updated_at timestamptz default now(),
  tsv tsvector generated always as (
    to_tsvector('english',
      coalesce(name,'') || ' ' || coalesce(description,'') || ' ' ||
      array_to_string(coalesce(tags,'{}'::text[]),' ')
    )
  ) stored
);

create table if not exists embeddings (
  tool_id uuid primary key references tools(id) on delete cascade,
  embedding vector(384)
);

create index if not exists tools_tsv_idx on tools using gin(tsv);
create index if not exists tools_name_trgm on tools using gin (name gin_trgm_ops);
create index if not exists tools_url_idx on tools(url);
