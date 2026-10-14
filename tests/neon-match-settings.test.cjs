'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),settings=require('../game/neon-arena/neon-match-settings.js');
test('mode rules preserve zero-bot practice, 1v1 Frag and bounded Siege assaults',()=>{
  assert.equal(settings.modeSettings('dm',{botCount:0}).botCount,0);assert.equal(settings.modeSettings('tdm',{botCount:16}).botCount,16);assert.equal(settings.modeSettings('frag',{botCount:16}).botCount,1);assert.equal(settings.modeSettings('frag',{botCount:0}).botCount,0);assert.equal(settings.modeSettings('siege',{botCount:0}).botCount,2);assert.equal(settings.modeSettings('survivor',{botCount:16}).botCount,0);
});
test('saved settings are sanitized instead of trusting malformed or out-of-range values',()=>{
  for(const input of [null,false,'invalid',[],{mode:'missing',maps:{dm:'../../bad'},modes:{dm:null},common:{volume:900,lookSensitivity:-3}}]){const p=settings.preferences(input);assert.equal(p.mode,'dm');assert.equal(p.maps.dm,'grid');assert(p.common.volume>=0&&p.common.volume<=100);assert(p.common.lookSensitivity>=.25&&p.common.lookSensitivity<=2.5)}
  const p=settings.modeSettings('dm',{botCount:500,matchLen:-10,scoreTarget:Infinity,respawnDelay:-9,difficulty:'godmode'});assert.equal(p.botCount,16);assert.equal(p.matchLen,60);assert.equal(p.scoreTarget,15);assert.equal(p.respawnDelay,.5);assert.equal(p.difficulty,'normal');
});
test('each mode retains independent arena and gameplay choices across a save round trip',()=>{
  const saved=settings.preferences({mode:'siege',maps:{dm:'terminal',tdm:'foundry',frag:'outpost',siege:'reactor',survivor:'derelict_station'},modes:{dm:{botCount:0,scoreTarget:5},tdm:{botCount:16,scoreTarget:100},frag:{botCount:1,scoreTarget:20,respawnDelay:.5}},common:{volume:35,lookSensitivity:1.5}}),roundTrip=settings.preferences(JSON.parse(JSON.stringify(saved)));
  assert.deepEqual(roundTrip,saved);assert.equal(roundTrip.modes.dm.botCount,0);assert.equal(roundTrip.modes.tdm.botCount,16);assert.equal(roundTrip.modes.frag.scoreTarget,20);assert.equal(roundTrip.common.volume,35);
});
