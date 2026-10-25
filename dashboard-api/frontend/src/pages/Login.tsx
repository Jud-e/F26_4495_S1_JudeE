import { useEffect, useState, type FormEvent } from "react";
import { useNavigate } from "react-router-dom";
import { api, clearSession, getToken, homeFor, saveSession } from "../api";
import type { User } from "../types";

const DEMO = [
  { u: "admin", p: "admin123", label: "Admin (all devices, audit log)" },
  { u: "nurse.amara", p: "nurse123", label: "Nurse (infusion pumps, pulse oximeters)" },
  { u: "nurse.ben", p: "nurse123", label: "Nurse (cardiac monitors)" },
];

export default function Login() {
  const nav = useNavigate();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  // Already signed in? Go straight to your page.
  useEffect(() => {
    if (!getToken()) return;
    api<User>("/api/me").then((u) => nav(homeFor(u), { replace: true })).catch(() => clearSession());
  }, [nav]);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setError(""); setBusy(true);
    try {
      const r = await api<{ token: string; user: User }>("/api/login", { method: "POST", body: { username, password } });
      saveSession(r.token, r.user);
      nav(homeFor(r.user), { replace: true });
    } catch (ex) { setError((ex as Error).message); setBusy(false); }
  }

  return (
    <div className="login-wrap"><div className="login">
      <div><h1>MedGuard</h1><div className="note">Zero-trust monitoring for connected medical devices</div></div>
      <section className="panel">
        <form id="form" onSubmit={submit}>
          <label className="f">Username
            <input id="u" required autoFocus autoComplete="username" value={username} onChange={(e) => setUsername(e.target.value)} />
          </label>
          <label className="f">Password
            <input id="p" type="password" required autoComplete="current-password" value={password} onChange={(e) => setPassword(e.target.value)} />
          </label>
          {error && <div id="err" className="err" role="alert">{error}</div>}
          <button className="primary" id="go" type="submit" disabled={busy}>Sign in</button>
        </form>
      </section>
      <section className="panel demo">
        <h2>Demo accounts</h2>
        <div className="note">Authentication is mocked for this prototype. Click an account to fill the form.</div>
        {DEMO.map((d) => (
          <button key={d.u} type="button" data-u={d.u} onClick={() => { setUsername(d.u); setPassword(d.p); }}>
            <span>{d.label}</span><span className="mono">{d.u}</span>
          </button>
        ))}
      </section>
    </div></div>
  );
}