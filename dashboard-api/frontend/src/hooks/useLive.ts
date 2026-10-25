import { useEffect, useReducer, useRef, useState } from "react";
import { api, getToken } from "../api";
import type { Alert, AuditEntry, Device, LiveEvent } from "../types";

interface State { devices: Record<string, Device>; alerts: Alert[]; audit: AuditEntry[] }
type Action = LiveEvent | { type: "audit_load"; rows: AuditEntry[] };

const initial: State = { devices: {}, alerts: [], audit: [] };

function reducer(s: State, a: Action): State {
  switch (a.type) {
    case "snapshot":
      return { ...s, devices: Object.fromEntries(a.devices.map((d) => [d.device_id, d])), alerts: a.alerts };
    case "device":
      return { ...s, devices: { ...s.devices, [a.device.device_id]: a.device } };
    case "alert":
      return { ...s, alerts: [a.alert, ...s.alerts] };
    case "alert_update":
      return { ...s, alerts: s.alerts.map((x) => (x.id === a.alert.id ? a.alert : x)) };
    case "audit":
      return { ...s, audit: [a.entry, ...s.audit] };
    case "audit_load": {
      const seen = new Set(s.audit.map((e) => e.id));
      return { ...s, audit: [...s.audit, ...a.rows.filter((e) => !seen.has(e.id))].sort((x, y) => y.id - x.id) };
    }
  }
}

/** Live device, alert and audit state from the /ws WebSocket, with automatic reconnect. */
export function useLive(onAlert?: (a: Alert) => void) {
  const [state, dispatch] = useReducer(reducer, initial);
  const [connected, setConnected] = useState(false);
  const alertRef = useRef(onAlert);
  alertRef.current = onAlert;

  useEffect(() => {
    let closed = false, opened = false, retry = 1000;
    let ws: WebSocket | undefined, timer: number | undefined;

    const connect = () => {
      const proto = location.protocol === "https:" ? "wss://" : "ws://";
      ws = new WebSocket(`${proto}${location.host}/ws?token=${encodeURIComponent(getToken() || "")}`);
      ws.onopen = () => { opened = true; retry = 1000; setConnected(true); };
      ws.onmessage = (m) => {
        const ev = JSON.parse(m.data) as LiveEvent;
        if (ev.type === "alert") alertRef.current?.(ev.alert);   // the snapshot's history never triggers this
        dispatch(ev);
      };
      ws.onclose = async () => {
        if (closed) return;
        setConnected(false);
        // A rejected handshake (bad or expired token) never opens: check the session before retrying.
        if (!opened) { try { await api("/api/me"); } catch { return; } }
        timer = window.setTimeout(connect, retry);
        retry = Math.min(retry * 2, 10000);
      };
    };
    connect();
    return () => { closed = true; clearTimeout(timer); ws?.close(); };
  }, []);

  const devices = Object.values(state.devices).sort((a, b) => b.risk_score - a.risk_score || a.device_id.localeCompare(b.device_id));
  return { devices, alerts: state.alerts, audit: state.audit, connected, loadAudit: (rows: AuditEntry[]) => dispatch({ type: "audit_load", rows }) };
}

/** Re-render every `ms` so relative times ("3 min ago") stay current. */
export function useTick(ms = 15000) {
  const [, set] = useState(0);
  useEffect(() => { const t = window.setInterval(() => set((n) => n + 1), ms); return () => clearInterval(t); }, [ms]);
}