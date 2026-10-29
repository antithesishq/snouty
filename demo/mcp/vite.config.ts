import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  // Relative asset paths, so the UI works behind a path-prefix proxy.
  base: "./",
  plugins: [react()],
  build: { chunkSizeWarningLimit: 1500 },
  server: {
    port: 5173,
    // The page can call any tool with your credentials: loopback only.
    host: process.env.HOST ?? "127.0.0.1",
    proxy: { "/ws": { target: "ws://127.0.0.1:8017", ws: true } },
  },
});
