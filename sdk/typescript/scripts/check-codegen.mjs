#!/usr/bin/env node
/**
 * Drift check (#246): Verify that committed TypeScript types match the current openapi.yaml.
 *
 * This script regenerates types from the current openapi.yaml using openapi-typescript
 * and compares the result against the committed generated/api.d.ts. It fails if they differ,
 * ensuring hand-edits to the YAML or schema changes never silently diverge from generated types.
 *
 * Usage (from sdk/typescript/):
 *   node scripts/check-codegen.mjs
 *
 * Run automatically in CI via `npm run codegen:check`.
 */
import { execSync } from "node:child_process";
import { readFileSync, writeFileSync, unlinkSync, existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { tmpdir } from "node:os";
import { randomBytes } from "node:crypto";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const openapi = join(root, "..", "..", "openapi.yaml");
const committed = join(root, "generated", "api.d.ts");
const tmp = join(tmpdir(), `lumenqraph-codegen-${randomBytes(6).toString("hex")}.d.ts`);

console.log(`ℹ️  Regenerating TypeScript types from ${openapi}...`);

// Generate fresh types into a temp file from the current openapi.yaml.
try {
  execSync(
    `npx openapi-typescript "${openapi}" -o "${tmp}"`,
    { cwd: root, stdio: "pipe" },
  );
} catch (err) {
  console.error("❌ Codegen failed:", err.message ?? err);
  process.exit(1);
}

console.log(`ℹ️  Comparing generated types against ${committed}...`);

// Compare regenerated types with the committed version.
const fresh = readFileSync(tmp, "utf8");
const current = existsSync(committed) ? readFileSync(committed, "utf8") : "";

if (fresh !== current) {
  console.error(
    "❌ Generated types are out of sync with openapi.yaml!\n" +
    "   The OpenAPI schema at the repo root was updated but the committed\n" +
    "   generated/api.d.ts does not match the current schema.\n\n" +
    "   To fix this, run:\n" +
    "   cd sdk/typescript && npm run codegen && git add generated/api.d.ts && git commit\n",
  );
  unlinkSync(tmp);
  process.exit(1);
}

unlinkSync(tmp);
console.log("✅ Generated types are in sync with openapi.yaml.");
