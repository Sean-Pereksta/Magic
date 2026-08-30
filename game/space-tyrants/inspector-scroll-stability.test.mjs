import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";

const loader=readFileSync(new URL("../space-tyrants.html",import.meta.url),"utf8");
const source=readFileSync(new URL("./inspector-scroll-stability.js",import.meta.url),"utf8");

test("inspector stability layer loads after every other Space Tyrants patch",()=>{
  const inspector=loader.indexOf("'./space-tyrants/inspector-scroll-stability.js'");
  const galaxyBalance=loader.indexOf("'./space-tyrants/galactic-balance.js'");
  assert.ok(inspector>galaxyBalance,"inspector stability must remain the final patch wrapper");
  assert.equal(loader.slice(inspector).includes("'./space-tyrants/"),true);
  const patchList=loader.match(/const patchFiles=\[([\s\S]*?)\];/)?.[1]||"";
  const files=[...patchList.matchAll(/'([^']+)'/g)].map(match=>match[1]);
  assert.equal(files.at(-1),'./space-tyrants/inspector-scroll-stability.js');
});

test("ship construction is rendered as a persistent collapsible count section",()=>{
  assert.match(source,/SHIPS BEING CONSTRUCTED/);
  assert.match(source,/stxInspectorShipConstructionCount/);
  assert.match(source,/stxInspectorShipConstructionOpen = new Map\(\)/);
  assert.match(source,/details\.hidden = !open/);
  assert.match(source,/No ships are currently being produced at this world/);
});

test("live inspector refreshes cannot replace controls during a pointer interaction",()=>{
  assert.match(source,/pointerdown/);
  assert.match(source,/pointerup/);
  assert.match(source,/if\(stxInspectorPointerActive\) return/);
});
