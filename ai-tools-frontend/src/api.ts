import type { SearchResponse } from "./types";

const BASE = import.meta.env.VITE_API_URL;

export async function searchTools(params: {
  q?: string;
  limit?: number;
  offset?: number;
}) {
  const url = new URL(`${BASE}/search`);
  if (params.q) url.searchParams.set("q", params.q);
  url.searchParams.set("limit", String(params.limit ?? 24));
  url.searchParams.set("offset", String(params.offset ?? 0));

  const res = await fetch(url.toString());
  if (!res.ok) throw new Error(`Search failed: ${res.status}`);
  return (await res.json()) as SearchResponse;
}

export async function getStats() {
  const res = await fetch(`${BASE}/stats`);
  if (!res.ok) throw new Error(`Stats failed: ${res.status}`);
  return res.json() as Promise<{ total: number; top_categories: [string, number][] }>;
}
