import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// Radar Segurança SP — dashboard React (Vite + Tailwind).
// DuckDB-WASM carrega os bundles via CDN (jsdelivr), então não precisamos
// de configuração especial de assets .wasm aqui.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  build: {
    outDir: "dist",
    sourcemap: false,
  },
});
