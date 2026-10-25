export const CRIT = new Set(["infusion_rate_mismatch", "high_risk_score"]);

export const riskClass = (n: number) => (n >= 70 ? "high" : n >= 40 ? "mid" : "");

export function ago(iso?: string): string {
  if (!iso) return "-";
  const s = (Date.now() - new Date(iso).getTime()) / 1000;
  if (isNaN(s)) return iso;
  if (s < 5) return "just now";
  if (s < 60) return Math.round(s) + "s ago";
  if (s < 3600) return Math.round(s / 60) + " min ago";
  if (s < 86400) return Math.round(s / 3600) + " h ago";
  return new Date(iso).toLocaleDateString();
}

export function clock(iso: string): string {
  const d = new Date(iso);
  return isNaN(d.getTime()) ? iso : d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

export const readings = (v: Record<string, number> = {}) =>
  Object.entries(v).map(([k, x]) => k.replace(/_(ml_per_hr|mmHg|pct|bpm)$/, "") + " " + x).join(" · ");

// Plain-language wording for nurses.
const PLAIN: Record<string, string> = {
  auto_lock_disabled: "Screen auto-lock is turned off",
  default_credentials_unchanged: "Device still uses its factory password",
  firmware_outdated: "Device software is out of date",
  tls_disabled: "Device is sending data without encryption",
  infusion_rate_mismatch: "Infusion rate does not match the prescription",
  high_risk_score: "Device is behaving unusually",
};
export const plain = (r: string) => PLAIN[r] || r.replace(/_/g, " ");