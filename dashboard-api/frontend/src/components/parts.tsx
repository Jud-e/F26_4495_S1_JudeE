import { CRIT, riskClass } from "../format";
import { useState } from "react";
import { download, logout } from "../api";
import { api } from "../api";
import { ago } from "../format";
import type { Alert, Device, User } from "../types";

export function Chips({ items }: { items: string[] }) {
  return <div className="chips">{items.map((f) => <span key={f} className={"chip" + (CRIT.has(f) ? " crit" : "")}>{f}</span>)}</div>;
}

export function RiskCell({ d }: { d: Device }) {
  return (
    <div className="riskcell">
      <div className="bar"><i className={riskClass(d.risk_score)} style={{ width: Math.min(100, d.risk_score) + "%" }} /></div>
      <span className="num">{d.risk_score}</span>
    </div>
  );
}

export function DeviceLabel({ d }: { d: Device }) {
  return (
    <div>
      <div className="id">{d.device_id}{d.isolated && <> <span className="pill crit">isolated</span></>}</div>
      <div className="sub">{[d.device_type, d.unit, d.patient_id].filter(Boolean).join(" · ")}</div>
    </div>
  );
}

export function TopBar({ user, title, connected }: { user: User; title: string; connected: boolean }) {
  return (
    <div className="top"><div>
      <div className="brand"><b>MedGuard</b><span>{title}</span></div>
      <div className="who">
        <span className={"pill " + (connected ? "ok" : "warn")}>{connected ? "live" : "reconnecting"}</span>
        <div><div className="id">{user.name}</div><div className="sub">{user.username} · {user.role}</div></div>
        <button className="small" onClick={logout}>Sign out</button>
      </div>
    </div></div>
  );
}

/** Download buttons for an export endpoint. `query` carries any filters, e.g. "action=login". */
export function ExportButtons({ path, query = "", label = "Export" }: { path: string; query?: string; label?: string }) {
  const [busy, setBusy] = useState(false);
  async function go(format: "csv" | "json") {
    setBusy(true);
    try { await download(`${path}${path.includes("?") ? "&" : "?"}format=${format}${query ? "&" + query : ""}`); }
    catch (e) { window.alert((e as Error).message); }
    setBusy(false);
  }
  return (
    <span className="actions">
      <button className="small" disabled={busy} onClick={() => go("csv")}>{label} CSV</button>
      <button className="small" disabled={busy} onClick={() => go("json")}>JSON</button>
    </span>
  );
}


export const statusOf = (a: Alert) => a.remediation?.status ?? "open";
export const isOpen = (a: Alert) => statusOf(a) === "open" || statusOf(a) === "reopened";

const STATUS_LABEL = { open: "open", reopened: "fix failed", fixed: "fixed, rechecking", verified: "verified" } as const;
const STATUS_CLASS = { open: "warn", reopened: "crit", fixed: "plain", verified: "ok" } as const;

/** Where an alert is in flagged -> fixed -> verified. */
export function StatusPill({ a }: { a: Alert }) {
  const s = statusOf(a);
  return <span className={"pill " + STATUS_CLASS[s]}>{STATUS_LABEL[s]}</span>;
}

/** The fix note, the recheck progress, and the outcome. */
export function FixInfo({ a }: { a: Alert }) {
  const r = a.remediation;
  if (!r || (!r.fixed && !r.reopens)) return null;
  return (
    <div className="sub fixinfo">
      {r.fixed && <div>Fixed by {r.fixed.by} {ago(r.fixed.at)}: {r.fixed.note}</div>}
      {r.status === "fixed" && <div>Rechecking: {r.clean} clean message{r.clean === 1 ? "" : "s"} since the fix. The device has to keep reporting without the problem.</div>}
      {r.status === "verified" && r.verified_at && <div>Verified {ago(r.verified_at)}: the problem has not come back.</div>}
      {r.reopens > 0 && r.status !== "verified" && <div>The problem came back after a fix ({r.reopens} time{r.reopens === 1 ? "" : "s"}).</div>}
    </div>
  );
}

/** "Mark fixed" button that opens a small form asking what was done. */
export function FixForm({ a }: { a: Alert }) {
  const [open, setOpen] = useState(false), [note, setNote] = useState(""), [busy, setBusy] = useState(false), [err, setErr] = useState("");
  if (!isOpen(a)) return null;
  async function submit() {
    setBusy(true); setErr("");
    try { await api(`/api/alerts/${a.id}/fix`, { method: "POST", body: { note } }); setOpen(false); setNote(""); }
    catch (e) { setErr((e as Error).message); }
    setBusy(false);
  }
  if (!open) return <div className="actions"><button className="small" onClick={() => setOpen(true)}>Mark fixed</button></div>;
  return (
    <div className="fixform">
      <label className="f">What did you do to fix it?
        <textarea rows={2} maxLength={500} value={note} onChange={(e) => setNote(e.target.value)} placeholder="e.g. Turned TLS back on and changed the default password" autoFocus />
      </label>
      {err && <div className="sub" style={{ color: "var(--crit)" }}>{err}</div>}
      <div className="actions">
        <button className="primary small" disabled={busy || note.trim().length < 3} onClick={submit}>Confirm fixed</button>
        <button className="small" disabled={busy} onClick={() => setOpen(false)}>Cancel</button>
      </div>
      <div className="sub">The system rechecks this device's next messages. If the problem is still there, the alert reopens.</div>
    </div>
  );
}