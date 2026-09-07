import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";

const runtime=readFileSync(new URL("./fortress-command-runtime.txt",import.meta.url),"utf8");
const polish=readFileSync(new URL("./fortress-command-polish-runtime.txt",import.meta.url),"utf8");
const loader=readFileSync(new URL("./fortress-command-expansion.js",import.meta.url),"utf8");
const wrapper=readFileSync(new URL("../DwarfWorld.html",import.meta.url),"utf8");

test("fortress expansion loader and injected runtimes are valid JavaScript",()=>{
  assert.doesNotThrow(()=>new Function(loader));
  assert.doesNotThrow(()=>new Function(runtime));
  assert.doesNotThrow(()=>new Function(polish));
  assert.match(loader,/fortress-command-runtime\.txt/);
  assert.match(loader,/fortress-command-polish-runtime\.txt/);
  assert.match(wrapper,/DwarfWorldFortressExpansion\?\.prepareCore/);
  assert.match(wrapper,/frame\.srcdoc=await buildExpandedCore\(\)/);
});

test("auto equip is explicitly empty-slot only and uses existing item scoring",()=>{
  assert.match(runtime,/targets=order\.filter\(d=>!d\[cat\]\)/);
  assert.match(runtime,/itemScore\(b\)-itemScore\(a\)/);
  assert.match(runtime,/if\(ix<0\|\|d\[cat\]\)continue/);
  assert.match(runtime,/Auto Equip Empty Slots/);
});

test("expanded roster exposes identity, equipment needs, perks and health",()=>{
  assert.match(runtime,/dwx-avatar/);
  assert.match(runtime,/dwxActivity\(d\)/);
  assert.match(runtime,/dwxGear\("🛡",d\.armor\)/);
  assert.match(runtime,/dwxGear\("⚔",d\.weapon\)/);
  assert.match(runtime,/dwxGear\("⛏",d\.pickaxe\)/);
  assert.match(runtime,/dwx-perks/);
  assert.match(runtime,/dwx-hp/);
});

test("minimap respects explored fog and provides navigation targets",()=>{
  assert.match(runtime,/if\(tSeen\[i\]\)/);
  assert.match(runtime,/data-nav="fortress"/);
  assert.match(runtime,/data-nav="selected"/);
  assert.match(runtime,/data-nav="deepest"/);
  assert.match(runtime,/data-nav="latest"/);
  assert.match(runtime,/spawners/);
  assert.match(runtime,/TILE_DIAMOND/);
});

test("all requested civic building families and depth unlocks are represented",()=>{
  for(const name of ["great_hall","tavern","treasury","barracks","workshop","mushroom_farm","infirmary","survey_hall","lift_shaft","shrine","great_vault"]){
    assert.match(runtime,new RegExp(`${name}:\\{`));
  }
  assert.match(runtime,/depth:120/);
  assert.match(runtime,/depth:260/);
  assert.match(runtime,/depth:420/);
  assert.match(runtime,/depth:620/);
  assert.match(runtime,/depth:860/);
  assert.match(polish,/dwxUpgradeBuilding/);
  assert.match(polish,/b\.level>=3/);
  assert.match(polish,/Protect Legendary\+/);
  assert.match(polish,/DWX_BASE_BUILD_META\[BUILD_DEPOT\]/);
  assert.match(polish,/DWX_BASE_BUILD_META\[BUILD_GUARD\]/);
});

test("unique relics are one-per-world discoveries and are protected from selling",()=>{
  for(const id of ["worldsplitter","heart_of_mountain","fortunes_end","king_under_stone","spawnbreaker","deepward_bow","grudgeplate","last_kings_bulwark","armor_deep_road","lantern_first_delver","voidhook","ancestors_token"]){
    assert.match(runtime,new RegExp(`id:\"${id}\"`));
  }
  assert.match(runtime,/!dwx\.relicsFound\.includes\(r\.id\)/);
  assert.match(runtime,/dwxIsProtected/);
  assert.match(runtime,/✦ RELIC DISCOVERED ✦/);
});

test("expansion state is folded into the existing compressed world save",()=>{
  assert.match(runtime,/save\.fortressExpansion=dwx/);
  assert.match(runtime,/gzipStrToB64/);
  assert.match(runtime,/gunzipB64ToStr/);
  assert.match(runtime,/fortressExpansion\|\|null/);
  assert.doesNotMatch(runtime,/localStorage\.setItem\([^,]*fortress-command/i);
});
