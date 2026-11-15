import { CRIT, ago, explain, riskClass } from "../format";
import { logout } from "../api";
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

/** A device's highest recent risk scores, so a spike stays visible after quieter messages replace the current score. */
export function Peaks({ d }: { d: Device }) {
  const windowMs = (d.peak_window_s ?? 600) * 1000;
  const recent = (d.recent_peaks ?? []).filter((p) => Date.now() - new Date(p.at).getTime() <= windowMs);
  if (!recent.length) return null;
  return (
    <div className="peaks">
      <span className="sub">Recent peaks</span>
      {recent.map((p) => (
        <span key={p.at + p.score} className={"peak " + riskClass(p.score)} title={p.reasons.map(explain).join("; ")}>
          {p.score} · {ago(p.at)}
        </span>
      ))}
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