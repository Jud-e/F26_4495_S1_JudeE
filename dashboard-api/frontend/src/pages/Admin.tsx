import { useEffect, useMemo, useState } from "react";
import { api } from "../api";
import { Chips, DeviceLabel, ExportButtons, RiskCell, TopBar } from "../components/parts";
import RiskChart, { PeriodPicker } from "../components/riskChart";
import { ago, clock, explain, readings, riskClass } from "../format";
import { useLive, useTick } from "../hooks/useLive";
import type { Alert, AuditEntry, Device, User } from "../types";

const TABS = ["overview", "devices", "alerts", "audit", "access"] as const;
type Tab = (typeof TABS)[number];
const LABEL: Record<Tab, string> = { overview: "Overview", devices: "Devices", alerts: "Alerts", audit: "Audit log", access: "Access" };

function Tile({ label, n, cls = "" }: { label: string; n: number; cls?: string }) {
  return <div className={"tile " + cls}><div className="n">{n}</div><div className="l">{label}</div></div>;
}

function AlertRow({ a }: { a: Alert }) {
  return (
    <li className={a.severity + (a.ack ? " acked" : "")}>
      <div className="row">
        <div><span className={"pill " + (a.severity === "high" ? "crit" : "warn")}>{a.severity}</span> <strong>{a.device_id}</strong></div>
        <span className="sub">{ago(a.at)} · risk {a.risk_score}</span>
      </div>
      <Chips items={a.reasons} />
      {a.explanations?.map((e) => <div key={e} className="sub">Why: {explain(e)}</div>)}
      <div className="sub">{a.ack ? `Acknowledged by ${a.ack.by} ${ago(a.ack.at)}` : "Not yet acknowledged"}</div>
    </li>
  );
}

function DevicesGrid({ devices }: { devices: Device[] }) {
  const [busy, setBusy] = useState("");
  const [minutes, setMinutes] = useState(60);
  async function toggle(d: Device) {
    if (!d.isolated && !window.confirm(`Isolate ${d.device_id}? The gateway will refuse its messages until you restore it.`)) return;
    setBusy(d.device_id);
    try { await api(`/api/devices/${encodeURIComponent(d.device_id)}/${d.isolated ? "restore" : "isolate"}`, { method: "POST" }); }
    catch (e) { window.alert((e as Error).message); }
    setBusy("");
  }
  return (
    <>
      <div className="toolbar">
        <PeriodPicker minutes={minutes} onChange={setMinutes} />
        <span className="sub">Highest risk score per time slot. Dashed line: alert threshold (70).</span>
      </div>
      <div className="devgrid" id="devices">
        {devices.map((d) => (
          <div key={d.device_id} className={"devcard" + (d.isolated ? " isolated" : "")}>
            <div className="head">
              <DeviceLabel d={d} />
              <div className={"big " + riskClass(d.risk_score)}>{d.risk_score}<small>now</small></div>
            </div>
            <RiskChart deviceId={d.device_id} minutes={minutes} />
            {d.flags.length ? <Chips items={d.flags} /> : <span className="sub">No flags</span>}
            {d.risk_reasons?.length ? <div className="sub">Why: {d.risk_reasons.map(explain).join("; ")}</div> : null}
            <div className="sub">{readings(d.vitals)}</div>
            <div className="sub">Last seen {ago(d.last_seen)}{d.model_version && <> · <span className="mono">{d.model_version}</span></>}</div>
            <div className="actions">
              <button className={"small " + (d.isolated ? "" : "danger")} disabled={busy === d.device_id} onClick={() => toggle(d)}>{d.isolated ? "Restore" : "Isolate"}</button>
              <ExportButtons path={`/api/devices/${encodeURIComponent(d.device_id)}/history`} query={`minutes=${minutes}`} label="Risk" />
            </div>
          </div>
        ))}
      </div>
      {devices.length === 0 && <div className="empty">No devices yet.</div>}
    </>
  );
}

function AuditTable({ audit }: { audit: AuditEntry[] }) {
  const [action, setAction] = useState(""), [user, setUser] = useState(""), [text, setText] = useState("");
  const actions = useMemo(() => [...new Set(audit.map((e) => e.action))].sort(), [audit]);
  const users = useMemo(() => [...new Set(audit.map((e) => e.user))].sort(), [audit]);
  const t = text.toLowerCase();
  const rows = audit.filter((e) => (!action || e.action === action) && (!user || e.user === user) && (!t || (e.target + " " + e.detail + " " + e.user).toLowerCase().includes(t)));
  return (
    <>
      <div className="filters">
        <label className="f">Action<select id="f-action" value={action} onChange={(e) => setAction(e.target.value)}><option value="">All actions</option>{actions.map((a) => <option key={a}>{a}</option>)}</select></label>
        <label className="f">User<select id="f-user" value={user} onChange={(e) => setUser(e.target.value)}><option value="">All users</option>{users.map((u) => <option key={u}>{u}</option>)}</select></label>
        <label className="f">Search<input id="f-text" placeholder="device, detail..." value={text} onChange={(e) => setText(e.target.value)} /></label>
        <div className="f grow" style={{ alignSelf: "flex-end", textAlign: "right" }}>
          <ExportButtons path="/api/audit/export" label="Export log"
            query={[action && "action=" + encodeURIComponent(action), user && "user_filter=" + encodeURIComponent(user), text && "q=" + encodeURIComponent(text)].filter(Boolean).join("&")} />
        </div>
      </div>
      <div className="scroll"><table>
        <thead><tr><th>Time</th><th>User</th><th>Role</th><th>Action</th><th>Target</th><th>Detail</th></tr></thead>
        <tbody id="audit">
          {rows.slice(0, 200).map((e) => (
            <tr key={e.id}>
              <td className="mono">{clock(e.at)}</td><td>{e.user}</td><td className="sub">{e.role}</td>
              <td><span className={"pill " + (e.action === "login_failed" || e.action === "device_isolated" ? "crit" : "plain")}>{e.action}</span></td>
              <td>{e.target}</td><td className="sub">{e.detail}</td>
            </tr>
          ))}
        </tbody>
      </table></div>
      {rows.length === 0 && <div className="empty">No entries match.</div>}
    </>
  );
}

function AccessTable({ refreshKey }: { refreshKey: number }) {
  const [users, setUsers] = useState<User[]>([]);
  useEffect(() => { api<User[]>("/api/users").then(setUsers).catch(() => {}); }, [refreshKey]);
  return (
    <div className="scroll"><table>
      <thead><tr><th>User</th><th>Role</th><th>Device access</th><th>Status</th></tr></thead>
      <tbody>
        {users.map((u) => (
          <tr key={u.username}>
            <td><div className="id">{u.name}</div><div className="sub mono">{u.username}</div></td>
            <td>{u.role}</td>
            <td><Chips items={u.devices.map((x) => (x === "*" ? "all devices" : x))} /></td>
            <td><span className={"pill " + (u.online ? "ok" : "plain")}>{u.online ? "online" : "offline"}</span></td>
          </tr>
        ))}
      </tbody>
    </table></div>
  );
}

export default function Admin({ user }: { user: User }) {
  useTick();
  const { devices, alerts, audit, connected, loadAudit } = useLive();
  const [tab, setTab] = useState<Tab>(() => (TABS.find((t) => t === location.hash.slice(1)) ?? "overview"));
  useEffect(() => { history.replaceState(null, "", "#" + tab); }, [tab]);
  useEffect(() => { api<AuditEntry[]>("/api/audit").then(loadAudit).catch(() => {}); }, []);  // eslint-disable-line react-hooks/exhaustive-deps

  const open = alerts.filter((a) => !a.ack);

  return (
    <>
      <TopBar user={user} title="Admin console" connected={connected} />
      <main>
        <div className="tabs" role="tablist">
          {TABS.map((t) => (
            <button key={t} className="tab" role="tab" data-tab={t} aria-selected={tab === t} onClick={() => setTab(t)}>
              {LABEL[t]}{t === "alerts" && <span className="pill plain count">{open.length}</span>}
            </button>
          ))}
        </div>

        {tab === "overview" && (
          <section role="tabpanel">
            <div className="tiles" id="tiles">
              <Tile label="Devices" n={devices.length} />
              <Tile label="High risk (70+)" n={devices.filter((d) => d.risk_score >= 70).length} cls="crit" />
              <Tile label="Unacknowledged alerts" n={open.length} cls={open.length ? "warn" : ""} />
              <Tile label="Isolated" n={devices.filter((d) => d.isolated).length} />
            </div>
            <div className="cols" style={{ marginTop: 18 }}>
              <div className="panel">
                <h2>Highest risk devices</h2>
                <div className="scroll"><table>
                  <thead><tr><th>Device</th><th>Risk</th><th>Flags</th></tr></thead>
                  <tbody>{devices.slice(0, 5).map((d) => <tr key={d.device_id}><td><DeviceLabel d={d} /></td><td><RiskCell d={d} /></td><td><Chips items={d.flags} /></td></tr>)}</tbody>
                </table></div>
                {devices.length === 0 && <div className="empty">Waiting for scored messages. Run the simulator and the stream consumer.</div>}
              </div>
              <div className="panel">
                <h2>Latest alerts</h2>
                <ul className="alerts">{alerts.slice(0, 5).map((a) => <AlertRow key={a.id} a={a} />)}</ul>
                {alerts.length === 0 && <div className="empty">No alerts yet.</div>}
              </div>
            </div>
          </section>
        )}

        {tab === "devices" && (
          <section role="tabpanel" className="panel">
            <h2>Devices: risk over time</h2>
            <DevicesGrid devices={devices} />
            <p className="note">Isolating a device makes the gateway refuse its messages (HTTP 403) until you restore it. Every command is audited.</p>
          </section>
        )}

        {tab === "alerts" && (
          <section role="tabpanel" className="panel">
            <div className="toolbar"><h2 style={{ margin: 0 }}>All alerts</h2><ExportButtons path="/api/alerts/export" label="Export alerts" /></div>
            <ul className="alerts">{alerts.slice(0, 100).map((a) => <AlertRow key={a.id} a={a} />)}</ul>
            {alerts.length === 0 && <div className="empty">No alerts yet.</div>}
          </section>
        )}

        {tab === "audit" && <section role="tabpanel" className="panel"><h2>Audit log</h2><p className="note" style={{ marginTop: 0 }}>Export downloads the full stored log that matches the filters below, not only the rows shown. Each export is itself audited.</p><AuditTable audit={audit} /></section>}

        {tab === "access" && (
          <section role="tabpanel" className="panel">
            <h2>Accounts and device access</h2>
            <AccessTable refreshKey={audit.length} />
            <p className="note">Nurses only see, and are only notified about, devices that match their access list.</p>
          </section>
        )}
      </main>
    </>
  );
}