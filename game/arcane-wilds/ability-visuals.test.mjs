import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';

const source=readFileSync(new URL('./ability-visuals.js',import.meta.url),'utf8');
const html=readFileSync(new URL('../arcane-wilds.html',import.meta.url),'utf8');

test('Solar Beam uses a dedicated room-spanning beam cast',()=>{
  assert.match(source,/SPELLS\.solarLance\.name='Solar Beam'/);
  assert.match(source,/SPELLS\.solarLance\.cast='solarBeam'/);
  assert.match(source,/rayToRoomEdge/);
  assert.match(source,/lineDistance/);
  assert.match(source,/pushEffect\('solarBeam'/);
});

test('grand update abilities receive dedicated visual cast identities',()=>{
  for(const cast of ['stormSpear','emberComet','glassWinter','briarCrown','novaSwarm','cycloneWall','mirrorAegis','heavenCircuit','rotBloom','eclipseDisc','ancestorChoir']){
    assert.match(source,new RegExp(`cast='${cast}'`));
  }
});

test('ability visual layer loads after the grand update',()=>{
  const grandAt=html.indexOf('arcane-wilds/grand-update.js');
  const visualAt=html.indexOf('arcane-wilds/ability-visuals.js');
  const mobileAt=html.indexOf('arcane-wilds/mobile-interaction.js');
  assert.ok(grandAt>=0&&visualAt>grandAt,'ability visuals should load after grand update');
  assert.ok(mobileAt<0||visualAt<mobileAt,'ability visuals should load before mobile interaction layer');
});
