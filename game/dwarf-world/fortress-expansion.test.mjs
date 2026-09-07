import assert from "node:assert/strict";
import fs from "node:fs";

const source = fs.readFileSync(new URL("./fortress-expansion.js", import.meta.url), "utf8");
const core = fs.readFileSync(new URL("../DwarfWorld-core.html", import.meta.url), "utf8");

assert.match(source, /Auto Equip Empty Slots/);
assert.match(source, /if \(d\[slot\]\) continue; \/\/ critical safety rule: never replace existing gear/);
assert.match(source, /itemScore\(b\) - itemScore\(a\)/);
assert.match(source, /fortressExpansion: dwfxSerializeState\(\)/);
assert.match(source, /v: DWFX_SAVE_VERSION/);
assert.match(source, /\[1, 2, 3\]\.includes\(ver\)/);
assert.match(source, /dwfxState\.civicStructures/);
assert.match(source, /DWFX_MAP_SIZE = 240/);
assert.match(source, /tSeen\[ii\]/);
assert.match(source, /protected \|\| it\.relic/);
assert.match(source, /dwfxPendingPlacement/);
assert.match(source, /dwfxToggleMapFullscreen/);

const buildingIds = [
  "greatHall", "tavern", "treasury", "barracks", "workshop", "mushroomFarm",
  "infirmary", "surveyHall", "liftShaft", "shrine", "greatVault"
];
for (const id of buildingIds) assert.match(source, new RegExp(`${id}: \\{`), `missing civic building ${id}`);

const relicIds = [
  "worldsplitter", "heart_of_mountain", "fortunes_end", "king_under_stone",
  "spawnbreaker", "deepward_bow", "grudgeplate", "last_kings_bulwark",
  "armor_deep_road", "lantern_first_delver", "voidhook", "ancestors_token"
];
for (const id of relicIds) assert.match(source, new RegExp(`id: "${id}"`), `missing relic ${id}`);

assert.match(core, /DWARF_WORLD_FORTRESS_EXPANSION_BEGIN/);
assert.match(core, /DWARF_WORLD_FORTRESS_EXPANSION_END/);
assert.match(core, /window\.__dwarfWorldFortressExpansionInstalled/);
assert.match(core, /fortressExpansion: dwfxSerializeState\(\)/);

console.log("Dwarf World fortress expansion static coverage passed.");
