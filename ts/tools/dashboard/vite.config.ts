import tailwindcss from "@tailwindcss/vite"
import react from "@vitejs/plugin-react"
import { defineConfig } from "vite"

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    tsconfigPaths: true,
  },
  server: {
    port: 5183,
    host: "0.0.0.0",
    proxy: {
      "/api": {
        target: `http://localhost:${process.env.PORT ?? 3030}`,
        changeOrigin: true,
      },
    },
  },
})
