import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: "node",
          environment: "node",
          globalSetup: ["./__tests__/global-setup.ts"],
          testTimeout: 30_000,
          hookTimeout: 60_000,
          include: ["__tests__/**/*.test.ts"],
          forceExit: true,
        },
      },
      {
        test: {
          name: "react",
          environment: "jsdom",
          globalSetup: ["./__tests__/global-setup.ts"],
          testTimeout: 30_000,
          hookTimeout: 60_000,
          include: ["__tests__/**/*.test.tsx"],
          forceExit: true,
        },
      },
    ],
  },
});
