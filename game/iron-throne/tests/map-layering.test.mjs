import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';

// Execute the real render method with a recording canvas and deterministic art.
const source = readFileSync(process.env.MAP_SOURCE || new URL('../map.mjs', import.meta.url), 'utf8')
  .replace(/^import .*;\n/gm, '').replace(/export /g, '');
for (const reduced of [false, true]) test(`map completes with ordered layers (reduced effects: ${reduced})`, () => {
  const calls = [];
  const ctx = new Proxy({}, { get(target, key) {
    if (key in target) return target[key];
    if (key === 'createLinearGradient' || key === 'createRadialGradient') return () => ({ addColorStop() {} });
    if (key === 'measureText') return () => ({ width: 30 });
    if (key === 'stroke') return () => { if (target.strokeStyle === '#fff0b4') calls.push('selection'); };
    if (key === 'fillText') return text => calls.push(typeof text === 'number' ? 'count' : text === 'Capital' ? 'label' : 'icon');
    return () => {};
  }});
  const tile = { id: '0,0', q: 0, r: 0, owner: 'ashen', building: 'city', terrain: 'plains', name: 'Capital' };
  const state = { seed: 1, turn: 1, tiles: { '0,0': tile }, kingdoms: [{ id: 'ashen', color: '#ff0000', sigil: 'A' }], armies: [{ id: 'army', tile: '0,0', owner: 'ashen', units: { levy: 12 }, path: [] }] };
  const scope = { performance: { now: () => 100 }, document: { hidden: true }, HEX_DIRECTIONS: [], BUILDINGS: {}, UNITS: { levy: {} }, ART: { structures: { city: { 1: 'city' } } }, buildingLevel: () => 1, sizeOf: () => 12, familyCount: () => 0 };
  vm.createContext(scope);
  vm.runInContext(source + '\nglobalThis.MapClass = WorldMap;', scope);
  const map = Object.create(scope.MapClass.prototype);
  Object.assign(map, { width: 400, height: 300, dpr: 1, zoom: 1, x: 0, y: 0, selected: '0,0', armyId: 'army', ctx, getState: () => state, motion: { matches: reduced }, assets: {}, geography: { get: () => new Map([['0,0', { ground: 'plains' }]]) }, geographyArt: { draw: () => calls.push('overlay') }, effects: { ingest() {}, drawWorld() {}, drawResults() { calls.push('complete'); }, results: [] }, groundArt: () => calls.push('tile'), structureArt: () => { calls.push('building'); return true; }, armyArt: () => calls.push('troop') });
  map.render();
  for (const [lower, upper] of [['tile','overlay'], ['overlay','building'], ['building','selection'], ['label','selection'], ['selection','troop'], ['troop','count'], ['count','complete']]) {
    assert.ok(calls.indexOf(lower) >= 0 && calls.lastIndexOf(lower) < calls.indexOf(upper), `${lower} must render before ${upper}: ${calls}`);
  }
  assert.equal(map.hits.at(-1).tile, '0,0');
});
