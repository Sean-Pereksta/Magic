import fs from "node:fs";

const corePath = new URL("../DwarfWorld-core.html", import.meta.url);
const sourcePath = new URL("./fortress-expansion.js", import.meta.url);
const BEGIN = "  // DWARF_WORLD_FORTRESS_EXPANSION_BEGIN";
const END = "  // DWARF_WORLD_FORTRESS_EXPANSION_END";
const BOOT = `  // ============================================================
  // Boot
  // ============================================================
  generateWorld();`;

let core = fs.readFileSync(corePath, "utf8");
const source = fs.readFileSync(sourcePath, "utf8").trimEnd();
const injected = source.split("\n").map(line => `  ${line}`).join("\n");
const block = `${BEGIN}\n${injected}\n${END}`;

const beginAt = core.indexOf(BEGIN);
const endAt = core.indexOf(END);
if (beginAt >= 0 || endAt >= 0) {
  if (beginAt < 0 || endAt < beginAt) throw new Error("Dwarf World fortress expansion markers are malformed.");
  core = core.slice(0, beginAt) + block + core.slice(endAt + END.length);
} else {
  const bootAt = core.indexOf(BOOT);
  if (bootAt < 0) throw new Error("Could not find Dwarf World boot marker.");
  core = core.slice(0, bootAt) + `${block}\n\n` + core.slice(bootAt);
}

fs.writeFileSync(corePath, core);
console.log(`Synced fortress expansion into DwarfWorld-core.html (${source.split("\n").length} source lines).`);
