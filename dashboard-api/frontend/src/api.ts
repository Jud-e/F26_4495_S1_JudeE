// Mocked auth: the token returned by /api/login is kept in sessionStorage.
import type { User } from "./types";

const get = (k: string) => { try { return sessionStorage.getItem(k); } catch { return null; } };
const set = (k: string, v: string) => { try { sessionStorage.setItem(k, v); } catch { /* storage blocked */ } };
const del = (k: string) => { try { sessionStorage.removeItem(k); } catch { /* storage blocked */ } };

export const getToken = () => get("mg_token");
export const getUser = (): User | null => { try { return JSON.parse(get("mg_user") || "null"); } catch { return null; } };
export const saveSession = (token: string, user: User) => { set("mg_token", token); set("mg_user", JSON.stringify(user)); };
export const clearSession = () => { del("mg_token"); del("mg_user"); };
export const homeFor = (u: User | null) => (u?.role === "admin" ? "/admin" : "/nurse");

export async function api<T = unknown>(path: string, opts: { method?: string; body?: unknown } = {}): Promise<T> {
  const token = getToken();
  const res = await fetch(path, {
    method: opts.method || "GET",
    headers: { "Content-Type": "application/json", ...(token ? { Authorization: "Bearer " + token } : {}) },
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  if (res.status === 401 && path !== "/api/login") {
    clearSession();
    window.location.replace("/login");
    throw new Error("signed out");
  }
  if (!res.ok) {
    let detail = res.statusText;
    try { detail = (await res.json()).detail || detail; } catch { /* not json */ }
    throw new Error(detail);
  }
  return res.json() as Promise<T>;
}

export async function logout() {
  try { await api("/api/logout", { method: "POST" }); } catch { /* already signed out */ }
  clearSession();
  window.location.replace("/login");
}