import { defineConfig } from "tsdown";

export default defineConfig({
  entry: { index: "src/index.ts", "react/index": "src/react/index.tsx" },
  format: "esm",
  dts: true,
  clean: true,
  treeshake: true,
  external: ["react", "react-dom", "jotai"],
});
