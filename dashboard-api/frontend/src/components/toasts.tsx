import { createContext, useCallback, useContext, useState, type ReactNode } from "react";

interface Toast { id: number; title: string; body: string; severity?: string }
const Ctx = createContext<(title: string, body: string, severity?: string) => void>(() => {});
export const useToast = () => useContext(Ctx);

export function ToastProvider({ children }: { children: ReactNode }) {
  const [items, setItems] = useState<Toast[]>([]);
  const push = useCallback((title: string, body: string, severity?: string) => {
    const id = Date.now() + Math.random();
    setItems((t) => [{ id, title, body, severity }, ...t]);
    window.setTimeout(() => setItems((t) => t.filter((x) => x.id !== id)), 9000);
  }, []);
  return (
    <Ctx.Provider value={push}>
      {children}
      <div id="toasts" role="status" aria-live="polite">
        {items.map((t) => (
          <div key={t.id} className={"toast " + (t.severity || "")}>
            <b>{t.title}</b><span className="sub">{t.body}</span>
          </div>
        ))}
      </div>
    </Ctx.Provider>
  );
}