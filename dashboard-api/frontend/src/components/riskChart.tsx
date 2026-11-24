import { useEffect, useRef, useState } from "react";
import { api } from "../api";
import type { History } from "../types";

export const PERIODS = [
  { minutes: 15, label: "15m" },
  { minutes: 60, label: "1h" },
  { minutes: 360, label: "6h" },
  { minutes: 1440, label: "24h" },
] as const;

const W = 320, H = 120, L = 24, R = 6, T = 8, B = 16;   // drawing area inside the viewBox
const ALERT = 70;

export function PeriodPicker({ minutes, onChange }: { minutes: number; onChange: (m: number) => void }) {
  return (
    <div className="period" role="group" aria-label="Time period">
      {PERIODS.map((p) => <button key={p.minutes} aria-pressed={minutes === p.minutes} onClick={() => onChange(p.minutes)}>{p.label}</button>)}
    </div>
  );
}

const hhmm = (epoch: number) => new Date(epoch * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });

/** Risk score over time for one device. Fixed 0 to 100 axis, with the alert threshold dashed. Refreshes itself. */
export default function RiskChart({ deviceId, minutes, refreshKey }: { deviceId: string; minutes: number; refreshKey?: string | number }) {
  const [h, setH] = useState<History | null>(null);
  const [failed, setFailed] = useState(false);
  const [hover, setHover] = useState<number | null>(null);
  const box = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let off = false;
    const load = () => api<History>(`/api/devices/${encodeURIComponent(deviceId)}/history?minutes=${minutes}`)
      .then((r) => { if (!off) { setH(r); setFailed(false); } })
      .catch(() => { if (!off) setFailed(true); });
    load();
    const t = window.setInterval(load, 10000);
    return () => { off = true; clearInterval(t); };
  }, [deviceId, minutes, refreshKey]);

  if (failed && !h) return <div className="chart"><div className="none">Could not load history.</div></div>;
  if (!h) return <div className="chart"><div className="none">Loading...</div></div>;
  if (!h.points.length) return <div className="chart"><div className="none">No scores in this period.</div></div>;

  const x = (t: number) => L + ((t - h.from) / (h.to - h.from)) * (W - L - R);
  const y = (s: number) => T + (1 - Math.min(100, Math.max(0, s)) / 100) * (H - T - B);
  const pts = h.points.map((p) => ({ ...p, px: x(p.t + h.bucket_s / 2), py: y(p.score) }));
  // Break the line where the device went quiet for a while, instead of drawing a false connection.
  const runs: typeof pts[] = [];
  pts.forEach((p, i) => { if (i === 0 || p.t - pts[i - 1].t > h.bucket_s * 3) runs.push([p]); else runs[runs.length - 1].push(p); });
  const base = y(0);
  // One dot per stretch at or above the alert level (its peak), plus any lone point that has no line to show it.
  const marks: typeof pts = [];
  let peak: (typeof pts)[number] | null = null;
  pts.forEach((p, i) => {
    if (p.score >= ALERT) { if (!peak || p.score > peak.score) peak = p; }
    if (peak && (p.score < ALERT || i === pts.length - 1 || p.t - pts[i + 1].t < -h.bucket_s * 3)) { marks.push(peak); peak = null; }
  });
  runs.forEach((r) => { if (r.length === 1 && !marks.includes(r[0])) marks.push(r[0]); });
  const hot = hover !== null ? pts[hover] : null;

  function move(e: React.PointerEvent) {
    const r = box.current!.getBoundingClientRect();
    const vx = ((e.clientX - r.left) / r.width) * W;
    let best = 0;
    pts.forEach((p, i) => { if (Math.abs(p.px - vx) < Math.abs(pts[best].px - vx)) best = i; });
    setHover(best);
  }

  const label = `Risk score over the last ${minutes} minutes. Highest ${Math.max(...pts.map((p) => p.score))} of 100.`;
  return (
    <div className="chart" ref={box} onPointerMove={move} onPointerLeave={() => setHover(null)}>
      <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label={label}>
        {[0, 50, 100].map((v) => <line key={v} className="grid" x1={L} x2={W - R} y1={y(v)} y2={y(v)} />)}
        <line className="alertline" x1={L} x2={W - R} y1={y(ALERT)} y2={y(ALERT)} />
        {[0, 70, 100].map((v) => <text key={v} className="axis" x={L - 4} y={y(v) + 3} textAnchor="end">{v}</text>)}
        <text className="axis" x={L} y={H - 3}>{hhmm(h.from)}</text>
        <text className="axis" x={W - R} y={H - 3} textAnchor="end">now</text>
        {runs.map((r, i) => r.length > 1 && (
          <g key={i}>
            <path className="area" d={`M${r[0].px},${base} ` + r.map((p) => `L${p.px},${p.py}`).join(" ") + ` L${r[r.length - 1].px},${base}Z`} />
            <polyline className="line" points={r.map((p) => `${p.px},${p.py}`).join(" ")} />
          </g>
        ))}
        {marks.map((p) => <circle key={p.t} className={"dot" + (p.score >= ALERT ? " high" : "")} cx={p.px} cy={p.py} r={3} />)}
        {hot && <><line className="cursor" x1={hot.px} x2={hot.px} y1={T} y2={base} /><circle className={"dot" + (hot.score >= ALERT ? " high" : "")} cx={hot.px} cy={hot.py} r={4} /></>}
      </svg>
      {hot && <div className="tip" style={{ left: Math.min(88, Math.max(12, (hot.px / W) * 100)) + "%" }}>{hhmm(hot.t)} · risk {hot.score}</div>}
    </div>
  );
}