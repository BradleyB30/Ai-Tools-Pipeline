import { useEffect, useMemo, useState } from "react";
import { searchTools, getStats } from "./api";
import type { Tool } from "./types";
import "./index.css"; // tailwind is imported here

function ToolCard({ tool }: { tool: Tool }) {
  const domain = useMemo(() => {
    try {
      return tool.url ? new URL(tool.url).hostname : "";
    } catch {
      return "";
    }
  }, [tool.url]);

  return (
    <a
      href={tool.url ?? "#"}
      target="_blank"
      rel="noreferrer"
      className="block rounded-2xl border border-zinc-800 bg-zinc-900/70 p-4 shadow-sm ring-1 ring-black/5 transition hover:border-zinc-700 hover:shadow-md"
    >
      <div className="mb-1 text-sm text-zinc-400">{domain}</div>
      <h3 className="line-clamp-1 text-lg font-semibold text-zinc-100">{tool.name}</h3>
      <p className="mt-2 line-clamp-3 text-sm text-zinc-300/80">
        {tool.description || "—"}
      </p>

      {(tool.categories?.length ?? 0) > 0 && (
        <div className="mt-3 flex flex-wrap gap-2">
          {tool.categories!.slice(0, 3).map((c) => (
            <span key={c} className="rounded-full bg-zinc-800 px-2 py-0.5 text-xs text-zinc-300">
              {c}
            </span>
          ))}
        </div>
      )}
    </a>
  );
}

function buildPageList(currentPage: number, totalPages: number) {
  const windowSize = 2;
  const pagesSet = new Set<number>();
  pagesSet.add(1);
  pagesSet.add(totalPages);
  for (let p = currentPage - windowSize; p <= currentPage + windowSize; p += 1) {
    if (p > 1 && p < totalPages) pagesSet.add(p);
  }
  const numericPages = Array.from(pagesSet).sort((a, b) => a - b);
  const result: (number | "ellipsis")[] = [];
  for (let i = 0; i < numericPages.length; i += 1) {
    if (i > 0 && numericPages[i] - numericPages[i - 1] > 1) {
      result.push("ellipsis");
    }
    result.push(numericPages[i]);
  }
  return result;
}

function Pagination({
  page,
  totalPages,
  onPage,
}: {
  page: number;
  totalPages: number;
  onPage: (p: number) => void;
}) {
  const pages = buildPageList(page, totalPages);
  const btnBase =
    "h-9 min-w-9 rounded-lg border transition text-sm flex items-center justify-center";

  return (
    <nav className="mt-8 flex flex-wrap items-center justify-center gap-2">
      <button
        className={`${btnBase} border-zinc-800 bg-zinc-900 text-zinc-300 hover:bg-zinc-800 disabled:opacity-40`}
        disabled={page === 1}
        onClick={() => onPage(1)}
      >
        « First
      </button>
      <button
        className={`${btnBase} border-zinc-800 bg-zinc-900 text-zinc-300 hover:bg-zinc-800 disabled:opacity-40`}
        disabled={page === 1}
        onClick={() => onPage(Math.max(1, page - 1))}
      >
        ← Prev
      </button>

      {pages.map((p, i) =>
        p === "ellipsis" ? (
          <span key={`e-${i}`} className="px-1 text-zinc-500">
            …
          </span>
        ) : (
          <button
            key={p}
            className={`${btnBase} ${
              p === page
                ? "border-indigo-500 bg-indigo-500 text-white"
                : "border-zinc-800 bg-zinc-900 text-zinc-300 hover:bg-zinc-800"
            }`}
            onClick={() => onPage(p)}
          >
            {p}
          </button>
        )
      )}

      <button
        className={`${btnBase} border-zinc-800 bg-zinc-900 text-zinc-300 hover:bg-zinc-800 disabled:opacity-40`}
        disabled={page === totalPages}
        onClick={() => onPage(Math.min(totalPages, page + 1))}
      >
        Next →
      </button>
      <button
        className={`${btnBase} border-zinc-800 bg-zinc-900 text-zinc-300 hover:bg-zinc-800 disabled:opacity-40`}
        disabled={page === totalPages}
        onClick={() => onPage(totalPages)}
      >
        Last »
      </button>
    </nav>
  );
}

export default function App() {
  const [q, setQ] = useState("");
  const [pendingQ, setPendingQ] = useState("");
  const [items, setItems] = useState<Tool[]>([]);
  const [total, setTotal] = useState<number | null>(null);
  const [limit] = useState(24);
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const page = Math.floor(offset / limit) + 1;

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        setLoading(true);
        setErr(null);
        const [search, stats] = await Promise.all([
          searchTools({ q, limit, offset }),
          getStats(),
        ]);
        if (cancelled) return;
        setItems(search.items);
        setTotal(stats.total);
      } catch (e: any) {
        if (!cancelled) setErr(e?.message ?? "Request failed");
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [q, limit, offset]);

  // pagination is handled by the numbered component below

  const totalPages = total ? Math.max(1, Math.ceil(total / limit)) : 1;

  return (
    <div className="min-h-screen bg-gradient-to-b from-zinc-950 to-zinc-900 text-zinc-100">
      <header className="sticky top-0 z-10 border-b border-zinc-800 bg-zinc-950/70 backdrop-blur">
        <div className="mx-auto max-w-6xl px-4 py-8">
          <h1 className="text-center text-3xl font-extrabold tracking-tight">
            <span className="bg-gradient-to-r from-indigo-400 to-fuchsia-400 bg-clip-text text-transparent">
              Need to find an AI to complete a specific task?
            </span>
          </h1>
          <form
            className="mx-auto mt-5 flex w-full max-w-3xl items-center gap-2"
            onSubmit={(e) => {
              e.preventDefault();
              setOffset(0);
              setQ(pendingQ.trim());
            }}
          >
            <input
              className="w-full rounded-xl border border-zinc-700 bg-zinc-900 px-4 py-3 text-zinc-100 placeholder-zinc-500 outline-none ring-0 focus:border-zinc-600"
              placeholder="Search all tools (e.g., 'image upscaler', 'speech to text', 'Figma plugin')"
              value={pendingQ}
              onChange={(e) => setPendingQ(e.target.value)}
            />
            <button
              type="submit"
              className="rounded-xl bg-gradient-to-r from-indigo-500 to-fuchsia-500 px-5 py-3 font-medium text-white shadow hover:from-indigo-400 hover:to-fuchsia-400"
            >
              Search
            </button>
          </form>

          <div className="mt-3 text-center text-sm text-zinc-400">
            {total != null && (
              <>
                Showing <span className="font-medium text-zinc-200">{items.length}</span> of{" "}
                <span className="font-medium text-zinc-200">{total.toLocaleString()}</span>{" "}
                tools {q ? <>for “{q}”</> : null}
              </>
            )}
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-6xl px-4 py-8">
        {err && (
          <div className="mb-4 rounded-xl border border-red-900/50 bg-red-950/50 p-3 text-red-300">
            {err}
          </div>
        )}

        {loading ? (
          <div className="py-24 text-center text-zinc-400">Loading…</div>
        ) : items.length === 0 ? (
          <div className="py-24 text-center text-zinc-400">No results.</div>
        ) : (
          <>
            <div className="grid grid-cols-1 gap-5 sm:grid-cols-2 md:grid-cols-3">
              {items.map((t) => (
                <ToolCard key={t.id} tool={t} />
              ))}
            </div>

            <Pagination
              page={page}
              totalPages={totalPages}
              onPage={(p) => setOffset((p - 1) * limit)}
            />
          </>
        )}
      </main>

      <footer className="mt-8 border-t border-zinc-800/80 bg-zinc-950/60">
        <div className="mx-auto max-w-6xl px-4 py-6 text-center text-sm text-zinc-500">
          Built on your AI Tools Catalog API.
        </div>
      </footer>
    </div>
  );
}
