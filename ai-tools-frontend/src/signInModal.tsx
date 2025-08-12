import { useState } from "react";
import { useAuth } from "./auth";

export default function SignInModal() {
  const { authOpen, closeAuthModal, signIn, signUp } = useAuth();
  const [mode, setMode] = useState<"signin"|"signup">("signin");
  const [email, setEmail] = useState("");
  const [pw, setPw] = useState("");
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  if (!authOpen) return null;

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setErr(null); setBusy(true);
    try {
      if (mode === "signin") await signIn(email, pw);
      else await signUp(email, pw);
    } catch (e: any) {
      setErr(e.message ?? "Failed");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 grid place-items-center bg-black/60">
      <div className="w-full max-w-md rounded-2xl bg-zinc-900 p-6 shadow-xl">
        <div className="flex items-center justify-between">
          <h2 className="text-lg font-semibold">
            {mode === "signin" ? "Sign in" : "Create account"}
          </h2>
          <button className="text-zinc-400 hover:text-white" onClick={closeAuthModal}>✕</button>
        </div>

        <form onSubmit={submit} className="mt-4 space-y-3">
          <input
            type="email"
            required
            placeholder="you@email.com"
            className="w-full rounded-lg bg-zinc-800 p-3 outline-none ring-1 ring-zinc-700 focus:ring-indigo-500"
            value={email} onChange={e=>setEmail(e.target.value)}
          />
          <input
            type="password"
            required
            placeholder="password"
            className="w-full rounded-lg bg-zinc-800 p-3 outline-none ring-1 ring-zinc-700 focus:ring-indigo-500"
            value={pw} onChange={e=>setPw(e.target.value)}
          />
          {err && <div className="text-sm text-red-400">{err}</div>}
          <button
            disabled={busy}
            className="w-full rounded-lg bg-indigo-600 p-3 font-medium hover:bg-indigo-500 disabled:opacity-60"
          >
            {busy ? "Please wait…" : (mode === "signin" ? "Sign in" : "Sign up")}
          </button>
        </form>

        <div className="mt-3 text-center text-sm text-zinc-400">
          {mode === "signin" ? (
            <>
              No account?{" "}
              <button className="text-indigo-400 hover:underline" onClick={()=>setMode("signup")}>
                Sign up
              </button>
            </>
          ) : (
            <>
              Already have an account?{" "}
              <button className="text-indigo-400 hover:underline" onClick={()=>setMode("signin")}>
                Sign in
              </button>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
