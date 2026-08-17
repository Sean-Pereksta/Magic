import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const entrySource = readFileSync(new URL("./hearthmouse-character-models.mjs", import.meta.url), "utf8");
const fixSource = readFileSync(new URL("./hearthmouse-rat-skin-fix.mjs", import.meta.url), "utf8");

test("rat.glb pink skin is restored after coat tinting", () => {
  assert.match(entrySource, /hearthmouse-rat-skin-fix\.mjs/);
  assert.match(fixSource, /RAT_SKIN_MATERIAL_PATTERN\s*=\s*\/\^\(pink\|skin\)\$\/i/);
  assert.match(fixSource, /0\.64000004529953/);
  assert.match(fixSource, /0\.368318110704422/);
  assert.match(fixSource, /0\.32935419678688/);
  assert.match(fixSource, /mouseTintApplied/);
  assert.match(fixSource, /__hearthmouseOriginalRatSkinRestored/);
});
