// Generates sdk/ts/src/types.ts from openapi/v1.json via openapi-typescript.
// Run via: mise run generate:types
import { execSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
const spec = resolve(root, "openapi/v1.json");
const out = resolve(root, "sdk/ts/src/types.ts");

execSync(`npx openapi-typescript ${spec} -o ${out}`, { stdio: "inherit" });
