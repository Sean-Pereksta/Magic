import { REEF, LOOM, GLASS } from './deep-roster.mjs';
import { travelers } from './deep-journal.mjs';
const rules = {
  carilluma: [1500, 'ft', REEF, 'River Pearl', 'water'],
  bridgeback: [2000, 'ft', LOOM, 'Frost Crystal', 'ice'],
  kilnveil: [1500, 'ft', GLASS, 'Ember Stone', 'fire'],
  driftmantle: [3, 'basket', null, 'berry', 'berry'],
  keelknuckle: [2, 'camp', REEF, 'charm material', 'charmMaterial'],
  silkshear: [2, 'grove', null, 'Dusk Petal', 'shadow'],
  coppercoil: [3, 'essence', null, 'Storm Shard', 'storm'],
  mirrorkin: [2, 'camp', GLASS, 'barter cache', 'cache'],
  rimewhistle: [3, 'ruins', null, 'Frost Crystal and Storm Shard', 'rime'],
};
export function forageProfile(p) {
  const row = rules[p?.species];
  if (!row) return null;
  const [intervalFeet, unit, region, name] = row;
  return {
    name,
    unit,
    intervalFeet,
    description: `One ${name} every ${intervalFeet} ${unit}${region ? ' in ' + region.replaceAll('-', ' ') : ''}.`,
    progressFeet: p.deepForage || 0,
    rewards: [],
  };
}
export function forage(s, kind, amount = 1) {
  for (const p of travelers(s)) {
    const row = rules[p.species];
    if (p.hp <= 0 || !row || row[1] !== kind || (row[2] && row[2] !== s.world.activeLand)) continue;
    const [interval, , , name, item] = row,
      total = (p.deepForage || 0) + amount,
      count = Math.floor((total + 1e-7) / interval);
    p.deepForage = Math.max(0, total - count * interval);
    if (!count) continue;
    if (item === 'cache') {
      s.run.fruit += count * 3;
      s.run.items.charmMaterial += count;
    } else if (item === 'rime') {
      s.run.essence.ice += count;
      s.run.essence.storm += count;
    } else if (item in s.run.essence) s.run.essence[item] += count;
    else s.run.items[item] = (s.run.items[item] || 0) + count;
    s.log.push(`${p.nickname || p.species} found ${count} × ${name}.`);
  }
}
