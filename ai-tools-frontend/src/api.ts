import type { SearchResponse } from "./types";

async function resolveApiBase(): Promise<string> {
  // Prefer explicit env var when provided
  const envBase = (import.meta as any)?.env?.VITE_API_URL as
    | string
    | undefined;
  const candidates: string[] = [];
  if (envBase && envBase.trim().length > 0) candidates.push(envBase.trim());

  // Local dev fallback
  const { protocol, hostname } = window.location;
  if (hostname === "localhost" || hostname.startsWith("127.")) {
    candidates.push("http://localhost:8000");
  }

  // Same-origin path-based proxy (if deployed behind a reverse proxy)
  candidates.push(`${protocol}//${hostname}/api`);

  // api.<host> convention (common when FE on root domain and BE on api subdomain)
  if (!hostname.startsWith("api.")) {
    candidates.push(`${protocol}//api.${hostname}`);
  }

  // Try candidates by probing /health; cache the first that works in-memory
  for (const base of candidates) {
    try {
      const u = new URL("/health", base);
      const res = await fetch(u.toString(), { cache: "no-store" });
      if (res.ok) return base.replace(/\/$/, "");
    } catch {
      // ignore and try the next candidate
    }
  }

  throw new Error(
    "API base URL could not be resolved. Set VITE_API_URL to your backend URL."
  );
}

export async function searchTools(params: {
  q?: string;
  limit?: number;
  offset?: number;
}) {
  const BASE = await resolveApiBase();
  const url = new URL(`${BASE}/search`);
  if (params.q) url.searchParams.set("q", params.q);
  url.searchParams.set("limit", String(params.limit ?? 24));
  url.searchParams.set("offset", String(params.offset ?? 0));

  const res = await fetch(url.toString());
  if (!res.ok) throw new Error(`Search failed: ${res.status}`);
  return (await res.json()) as SearchResponse;
}

export async function getStats() {
  const BASE = await resolveApiBase();
  const res = await fetch(`${BASE}/stats`);
  if (!res.ok) throw new Error(`Stats failed: ${res.status}`);
  return res.json() as Promise<{ total: number; top_categories: [string, number][] }>;
}
