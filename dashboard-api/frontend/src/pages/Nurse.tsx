import { useState } from "react";
import { api } from "../api";
import { TopBar } from "../components/parts";
import { useToast } from "../components/Toasts";
import { ago, plain, readings } from "../format";
import { useLive, useTick } from "../hooks/useLive";
import type { Alert, Device, User } from "../types";

const canPush = typeof Notification !== "undefined";

function DeviceCard({ d, open }: { d: Device; open: Alert[] }) {
  const mine = open.filter((a) => a.device_id === d.device_id);
  const urgent = d.risk_score >= 70 || mine.some((a) => a.severity === "high");
  const state = urgent ? "attn" : mine.length || d.flags.length ? "watch" : "";
  return (
    <div className={"card " + state}>
      <div className="id">{d.device_id}</div>
      <div className="sub">{[d.device_type.replace(/_/g, " "), d.unit, d.patient_id && "Patient " + d.patient_id].filter(Boolean).join(" · ")}</div>
      <div className="status">{d.isolated ? "Disconnected by security" : urgent ? "Check this device now" : state ? "Needs a look" : "Operating normally"}</div>
      {d.isolated && <div className="sub">This device was isolated by an administrator. Use manual monitoring until it is restored.</div>}
      {d.flags.length > 0 && <ul>{d.flags.map((f) => <li key={f}>{plain(f)}</li>)}</ul>}
      <div className="sub">{readings(d.vitals)}</div>
      <div className="sub">Last update {ago(d.last_seen)}</div>
    </div>
  );
}

function AlertCard({ a }: { a: Alert }) {
  const [busy, setBusy] = useState(false);
  async function ack() {
    setBusy(true);
    try { await api(`/api/alerts/${a.id}/ack`, { method: "POST" }); }
    catch (e) { window.alert((e as Error).message); setBusy(false); }
  }
  return (
    <li className={a.severity + (a.ack ? " acked" : "")}>
      <div className="row">
        <div><span className={"pill " + (a.severity === "high" ? "crit" : "warn")}>{a.severity === "high" ? "urgent" : "check"}</span> <strong>{a.device_id}</strong></div>
        <span className="sub">{ago(a.at)}</span>
      </div>
      <ul style={{ margin: 0, paddingLeft: 18 }}>{a.reasons.map((r) => <li key={r}>{plain(r)}</li>)}</ul>
      {a.tips.map((t) => <div key={t} className="sub">What to do: {t}</div>)}
      {a.ack
        ? <div className="sub">Acknowledged by {a.ack.by} {ago(a.ack.at)}</div>
        : <button className="primary small" style={{ alignSelf: "flex-start" }} disabled={busy} onClick={ack}>Acknowledge</button>}
    </li>
  );
}

export default function Nurse({ user }: { user: User }) {
  useTick();
  const toast = useToast();
  const [perm, setPerm] = useState<string>(canPush ? Notification.permission : "unsupported");

  const { devices, alerts, connected } = useLive((a) => {
    const title = (a.severity === "high" ? "URGENT: " : "Alert: ") + a.device_id;
    const body = a.reasons.map(plain).join("; ");
    toast(title, body, a.severity);
    if (canPush && Notification.permission === "granted") {
      try {
        const n = new Notification(title, { body, tag: "alert-" + a.id, requireInteraction: a.severity === "high" });
        n.onclick = () => { window.focus(); n.close(); };
      } catch { /* some browsers block constructing notifications */ }
    }
  });

  const open = alerts.filter((a) => !a.ack);
  const done = alerts.filter((a) => a.ack);
  document.title = (open.length ? `(${open.length}) ` : "") + "MedGuard nurse";

  async function enable() { try { setPerm(await Notification.requestPermission()); } catch { /* ignore */ } }

  return (
    <>
      <TopBar user={user} title="Nurse station" connected={connected} />
      <main>
        {perm === "default" && (
          <div className="banner">
            <span>Turn on notifications to be alerted about your devices even when this tab is in the background.</span>
            <button className="primary small" onClick={enable}>Enable notifications</button>
          </div>
        )}
        <div className="note">
          {perm === "granted" && "Desktop notifications are on for your devices."}
          {perm === "denied" && "Notifications are blocked in this browser. Alerts still appear on this page."}
          {perm === "unsupported" && "This browser does not support desktop notifications. Alerts still appear on this page."}
        </div>

        <section>
          <h2>My devices {devices.length > 0 && <span className="note">({devices.length})</span>}</h2>
          <div className="cards">{devices.map((d) => <DeviceCard key={d.device_id} d={d} open={open} />)}</div>
          {devices.length === 0 && <div className="empty">No device data yet for your assigned devices.</div>}
        </section>

        <section className="panel">
          <h2>Needs attention <span className="pill plain">{open.length}</span></h2>
          <ul className="alerts" id="open">{open.map((a) => <AlertCard key={a.id} a={a} />)}</ul>
          {open.length === 0 && <div className="empty">Nothing needs your attention right now.</div>}
        </section>

        <section className="panel">
          <h2>Acknowledged</h2>
          <ul className="alerts" id="done">{done.slice(0, 20).map((a) => <AlertCard key={a.id} a={a} />)}</ul>
          {done.length === 0 && <div className="empty">No acknowledged alerts yet.</div>}
        </section>
      </main>
    </>
  );
}