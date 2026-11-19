import { CRIT, riskClass } from "../format";
import { useState } from "react";
import { download, logout } from "../api";
import type { Device, User } from "../types";

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