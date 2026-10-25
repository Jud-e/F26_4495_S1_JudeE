import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// `npm run dev` serves the app on :5173 and forwards API and WebSocket calls to the
// dashboard-api container on :8100. `npm run build` writes ./dist, which the
// dashboard-api Docker image serves in production.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": "http://localhost:8100",
      "/ws": { target: "ws://localhost:8100", ws: true },
    },
  },
});
