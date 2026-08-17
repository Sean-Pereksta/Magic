import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const source = readFileSync(new URL("./hearthmouse-character-models-polish.mjs", import.meta.url), "utf8");

test("Mabel keeps the original cat.glb skin while named alternate cats are recolored", () => {
  assert.doesNotMatch(source, /\bmabel\s*:\s*0x/i);
  assert.match(source, /\bbiscuit\s*:\s*0x/i);
  assert.match(source, /\bpepper\s*:\s*0x/i);
  assert.match(source, /const tint = CAT_COAT_COLORS\[catId\];/);
  assert.match(source, /if \(!Number\.isFinite\(tint\)\) \{/);
  assert.match(source, /__hearthmouseCatUsesOriginalSkin = true/);
  assert.match(source, /if \(solidBase && copy\.map\) copy\.map = null;/);
  assert.doesNotMatch(source, /CAT_COAT_COLORS\[catId\]\s*\?\?/);
});
