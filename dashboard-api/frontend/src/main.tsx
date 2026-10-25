import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { getToken, getUser, homeFor } from "./api";
import { ToastProvider } from "./components/Toasts";
import Admin from "./pages/Admin";
import Login from "./pages/Login";
import Nurse from "./pages/Nurse";
import type { Role } from "./types";
import "./app.css";

/** Redirects to /login when signed out, or to your own page when your role does not match. */
function RequireRole({ role, children }: { role: Role; children: (user: NonNullable<ReturnType<typeof getUser>>) => React.ReactNode }) {
  const user = getUser();
  if (!getToken() || !user) return <Navigate to="/login" replace />;
  if (user.role !== role) return <Navigate to={homeFor(user)} replace />;
  return <>{children(user)}</>;
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <BrowserRouter>
      <ToastProvider>
        <Routes>
          <Route path="/login" element={<Login />} />
          <Route path="/admin" element={<RequireRole role="admin">{(u) => <Admin user={u} />}</RequireRole>} />
          <Route path="/nurse" element={<RequireRole role="nurse">{(u) => <Nurse user={u} />}</RequireRole>} />
          <Route path="*" element={<Navigate to="/login" replace />} />
        </Routes>
      </ToastProvider>
    </BrowserRouter>
  </React.StrictMode>
);
