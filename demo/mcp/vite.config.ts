import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  // Relative asset paths, so the UI works behind a path-prefix proxy.
  base: "./",
  plugins: [react()],
  build: { chunkSizeWarningLimit: 1500 },
  server: {
    port: 5173,
    host: "0.0.0.0",
    allowedHosts: true,
    proxy: { "/ws": { target: "ws://127.0.0.1:8017", ws: true } },
  },
});
