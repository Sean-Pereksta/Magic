import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createLevel,updateWorldStreaming} from '../js/world/generator.js';
import {fingerprintLegacy} from './legacy-layout-fingerprint.mjs';

// Captured from main at b7f82b07641b3d676a1b564e8af313b9c2b7a068,
// before introducing the exploration layout. Do not regenerate from new code.
const fixtures=JSON.parse(await readFile(new URL('./fixtures/legacy-layouts.json',import.meta.url),'utf8'));
for(const {biome,seed,layoutVersion,fingerprint} of fixtures) {
  const world=createLevel({biome,seed,layoutVersion,difficulty:4});
  assert.equal(fingerprintLegacy(world,updateWorldStreaming),fingerprint,`${biome}/${seed}/layout${layoutVersion}: saved geometry, item IDs and clocks must remain identical`);
}
console.log(`${fixtures.length} original layout fingerprints preserve every streamed collider, checkpoint, item and enemy identity.`);
