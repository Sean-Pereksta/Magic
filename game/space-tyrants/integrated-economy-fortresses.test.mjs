import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";

const source=readFileSync(new URL("./integrated-economy-fortresses.js",import.meta.url),"utf8");
const loader=readFileSync(new URL("../space-tyrants.html",import.meta.url),"utf8");

test("integrated economy layer parses and loads after its dependencies",()=>{
  assert.doesNotThrow(()=>new Function(source));
  const layer=loader.indexOf("'./space-tyrants/integrated-economy-fortresses.js'");
  assert.ok(layer>loader.indexOf("'./space-tyrants/deep-space-bases.js'"));
  assert.ok(layer>loader.indexOf("'./space-tyrants/resource-trade-economy.js'"));
  assert.ok(layer>loader.indexOf("'./space-tyrants/strategic-policies.js'"));
  assert.ok(layer<loader.indexOf("'./space-tyrants/inspector-scroll-stability.js'"));
  assert.equal(loader.match(/integrated-economy-fortresses\.js/g)?.length,1);
});

test("expansion imposes a reversible opportunity cost instead of free completion",()=>{
  assert.match(source,/m\.factory\*=1-\.30\*expansion/);
  assert.match(source,/m\.mine\*=1-\.18\*expansion/);
  assert.match(source,/m\.training\*=1-\.22\*expansion/);
  assert.match(source,/finally\{for\(const \[key,value\] of Object\.entries\(old\)\)infra\[key\]=value\}/);
  assert.doesNotMatch(source,/expansionProject\.progress\s*=\s*1/);
});

test("priority can mobilize real personnel and deeper reserves without inventing crew",()=>{
  assert.match(source,/priority>=100/);
  assert.match(source,/p\.pop-=crew/);
  assert.match(source,/p\.stock\.equipment=Math\.max\(0,stxIFN\(p\.stock\?\.equipment\)-crew\*700\)/);
  assert.match(source,/p\.stock\.trained=stxIFN\(p\.stock\?\.trained\)\+crew/);
  assert.match(source,/existing freight system ships it normally|freight system ships it normally/i);
});

test("world resource routing protects projects and physically redistributes export surplus",()=>{
  assert.match(source,/STX_IF_MODES=\["balanced","project","export"\]/);
  assert.match(source,/if\(mode==="project"\)return Math\.max\(base,local\+normal\*\.65\)/);
  assert.match(source,/if\(mode==="export"\)return Math\.max\(local,base\*\.55\)/);
  assert.match(source,/createShip\(type,source\.p,dest,0,\{cargo:\{\[r\]:amount\}/);
  assert.match(source,/ship\.stxIFRedistribution=true/);
});

test("fortress upgrades use physical cargo and three visually distinct tiers",()=>{
  assert.match(source,/mission:"fortress-upgrade"/);
  assert.match(source,/stxDeepMission==="fortress-upgrade"/);
  assert.match(source,/target<2\|\|target>3/);
  assert.match(source,/System Citadel/);
  assert.match(source,/Grand Commerce Ring/);
  assert.match(source,/Strategic Logistics Nexus/);
  assert.match(source,/Far-Reach Observatory/);
  assert.match(source,/ctx\.ellipse/);
  assert.match(source,/const arms=base\.type==="military"\?4\+tier\*2/);
});

test("station upgrades yield to an active player priority on the same resource",()=>{
  assert.match(source,/if\(priority&&stxIFNeed\(priority,r\)>\.2\)continue/);
  assert.match(source,/if\(o\?\.stxFortressUpgradeId\)\{const p=stxIFPriorityProject\(\);if\(p&&stxIFNeed\(p,o\.resource\)>\.2\)return\}/);
});
