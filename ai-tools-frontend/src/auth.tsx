import { createContext, useContext, useEffect, useState } from "react";
import type { User, Session, AuthChangeEvent } from "@supabase/supabase-js";
import { supabase } from "./supabase";

type AuthCtx = {
  user: User | null;
  loading: boolean;

  openAuthModal: () => void;
  closeAuthModal: () => void;
  authOpen: boolean;

  signIn: (email: string, password: string) => Promise<void>;
  signUp: (email: string, password: string) => Promise<void>;
  signOut: () => Promise<void>;
  requireAuth: (fn: () => void) => void;
};

const Ctx = createContext<AuthCtx | null>(null);
export const useAuth = () => useContext(Ctx)!;

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [authOpen, setAuthOpen] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let mounted = true;

    // Initial session hydrate
    supabase.auth.getSession().then(({ data }: { data: { session: Session | null } }) => {
      if (!mounted) return;
      setUser(data.session?.user ?? null);
      setLoading(false);
    });

    // Subscribe to auth changes
    const { data: sub } = supabase.auth.onAuthStateChange(
      (_evt: AuthChangeEvent, sess: Session | null) => {
        setUser(sess?.user ?? null);
      }
    );

    return () => {
      mounted = false;
      sub.subscription.unsubscribe();
    };
  }, []);

  const signIn = async (email: string, password: string) => {
    const { error } = await supabase.auth.signInWithPassword({ email, password });
    if (error) throw error;
    setAuthOpen(false);
  };

  const signUp = async (email: string, password: string) => {
    const { error } = await supabase.auth.signUp({ email, password });
    if (error) throw error;
    setAuthOpen(false);
  };

  const signOut = async () => {
    await supabase.auth.signOut();
  };

  const requireAuth = (fn: () => void) => {
    if (!user) setAuthOpen(true);
    else fn();
  };

  return (
    <Ctx.Provider
      value={{
        user,
        loading,
        authOpen,
        openAuthModal: () => setAuthOpen(true),
        closeAuthModal: () => setAuthOpen(false),
        signIn,
        signUp,
        signOut,
        requireAuth,
      }}
    >
      {children}
    </Ctx.Provider>
  );
}
