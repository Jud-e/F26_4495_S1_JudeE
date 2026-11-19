export type Role = "admin" | "nurse";

export interface User { username: string; name: string; role: Role; devices: string[]; online?: boolean }

export interface Device {
  device_id: string; device_type: string; patient_id?: string; unit?: string;
  risk_score: number; risk_reasons?: string[]; model_version?: string; flags: string[]; tips: string[];
  last_seen?: string; vitals: Record<string, number>; isolated?: boolean;
}

export interface HistoryPoint { t: number; score: number }
export interface History { device_id: string; minutes: number; bucket_s: number; from: number; to: number; points: HistoryPoint[] }

export interface Alert {
  id: number; device_id: string; device_type?: string; unit?: string; reasons: string[];
  severity: "high" | "medium"; risk_score: number; explanations?: string[]; tips: string[]; at: string;
  ack: { by: string; at: string } | null;
}

export interface AuditEntry { id: number; at: string; user: string; role: string; action: string; target: string; detail: string }

export type LiveEvent =
  | { type: "snapshot"; devices: Device[]; alerts: Alert[] }
  | { type: "device"; device: Device }
  | { type: "alert"; alert: Alert }
  | { type: "alert_update"; alert: Alert }
  | { type: "audit"; entry: AuditEntry };