import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

const root = process.cwd();
const sourcePath = path.join(root, "scripts/patch-warrealms-heat-payoffs.mjs");
const fixedPath = path.join(root, "scripts/.patch-warrealms-heat-payoffs-fixed.mjs");
const wrapperPath = path.join(root, "scripts/run-warrealms-heat-patch.mjs");
const validatorTestPath = path.join(root, "game/warrealms/tests/card-validator.test.mjs");

let source = fs.readFileSync(sourcePath, "utf8");
source = source.replace(
  '      label: `${getCard(entry).name} · Heat ${entry.heat || 0}`,',
  '      label: getCard(entry).name + " · Heat " + (entry.heat || 0),'
);
source = source.replace(
  '      addLog(game, `${player.name} had no eligible Heat card to empower.`);',
  '      addLog(game, player.name + " had no eligible Heat card to empower.");'
);

if (source.includes('label: `${getCard(entry).name}')) throw new Error("Nested label template literal was not repaired");
if (source.includes('addLog(game, `${player.name}')) throw new Error("Nested log template literal was not repaired");

fs.writeFileSync(fixedPath, source);
await import(pathToFileURL(fixedPath).href + `?run=${Date.now()}`);

let validatorTest = fs.readFileSync(validatorTestPath, "utf8");
if (validatorTest.includes("assert.equal(pack.COLLECTIBLE_CARDS.length, 414);")) {
  validatorTest = validatorTest.replace(
    "assert.equal(pack.COLLECTIBLE_CARDS.length, 414);",
    "assert.equal(pack.COLLECTIBLE_CARDS.length, 417);"
  );
  fs.writeFileSync(validatorTestPath, validatorTest);
}

for (const file of [fixedPath, wrapperPath]) {
  if (fs.existsSync(file)) fs.rmSync(file);
}
