import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  // Relative base so the same build works both at the gateway root (`lucent dev`)
  // and under a subpath on GitHub Pages (`/lucent/`) — asset + bundle URLs
  // resolve against the current document, and replay uses hash routing so the
  // path stays constant.
  base: "./",
  server: { port: 5173 },
  build: { outDir: "dist", sourcemap: true },
});
