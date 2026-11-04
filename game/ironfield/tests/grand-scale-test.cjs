'use strict';
const assert = require('node:assert/strict');
const DT = 1 / 30;
function register(test) {
  const D = global.GameData, Engine = global.BattleEngine;
  const roster = () => ({ swordsman: 10000, pikeman: 5000, archer: 3000, cavalry: 2000 });
  const config = (width = 16000, height = 11000, regions = []) => ({ map: 'custom', seed: 430247,
    customMap: { width, height, biome: 'grassland', regions }, armies: [{ roster: roster() }, { roster: roster() }] });
  function separated(e) {
    const buckets = new Map(); let minimum = Infinity;
    for (const u of e.units) {
      assert(u.x >= u._radius && u.y >= u._radius && u.x <= e.width - u._radius && u.y <= e.height - u._radius);
      assert(!e._bodyBlocked(u.x, u.y, u._radius));
      const cx = Math.floor(u.x / 32), cy = Math.floor(u.y / 32);
      for (let y = cy - 1; y <= cy + 1; y++) for (let x = cx - 1; x <= cx + 1; x++) for (const v of buckets.get(y * e._bodyCols + x) || []) {
        const ratio = Math.hypot(u.x - v.x, u.y - v.y) / (u._radius + v._radius);
        minimum = Math.min(minimum, ratio); assert(ratio >= .96 - 1e-8, 'Default large armies must begin with separated real bodies');
      }
      const key = cy * e._bodyCols + cx;
      if (!buckets.has(key)) buckets.set(key, []); buckets.get(key).push(u);
    }
    return Math.round(minimum * 1000) / 1000;
  }
  test('Forty thousand soldiers deploy inside both Standard and Colossal fields without overlapping bodies', () => {
    assert.equal(D.MAX_UNITS, 40000);
    return [[6400,4400],[16000,11000]].map(([width,height]) => {
      const e = new Engine(config(width,height));
      assert.equal(e.units.length, 40000);
      assert.deepEqual(e.getStats().teams.map(t => t.initial), [20000,20000]);
      return { width, height, units: e.units.length, minimumBodyRatio: separated(e) };
    });
  });
  test('Army normalization enforces the new twenty-thousand-per-side limit', () => {
    const e = new Engine({ map: 'custom', customMap: { width: 16000, height: 11000, regions: [] },
      armies: [{ roster: { militia: 30000 } }, { roster: { militia: 30000 } }] });
    assert.equal(e.units.length, 40000);
    assert.deepEqual(e.config.armies.map(a => a.roster.militia), [20000,20000]);
  });
  test('Certified far marches move every real soldier and preserve geometry, caches, cooldowns and fresh orders', () => {
    const e = new Engine(config()); e.start();
    let peakFast = 0, checked = 0;
    for (let tick = 0; tick < 12; tick++) {
      const before = e.units.map(u => [u.x,u.y,u.anim,u._cooldown]); e.step(DT);
      peakFast = Math.max(peakFast,e.fastMarchUnits);
      for (const u of e.units) {
        const p = before[u.id];
        assert([u.x,u.y,u.hp].every(Number.isFinite));
        assert(Math.hypot(u.x-p[0],u.y-p[1]) <= D.TYPES[u.type].speed*DT*1.8+.25);
        assert(Math.abs(u.anim-p[2]-DT) < 1e-7);
        assert.equal(e._bodyX[u.id],u.x); assert.equal(e._bodyY[u.id],u.y);
        assert(Math.abs(u._cooldown-p[3]+DT) < 1e-7);
      }
      const r = e.regiments.find(r=>r.unitIds.length>30&&r.unitIds.every(id=>e.units[id]._bulkStepTick===e._tick));
      if (r) {
        const first=e.units[r.unitIds[0]],p=before[first.id],dx=first.x-p[0],dy=first.y-p[1];
        for(const id of r.unitIds){const u=e.units[id],p=before[id];assert(Math.abs(u.x-p[0]-dx)<1e-7&&Math.abs(u.y-p[1]-dy)<1e-7,'A certified translation must preserve internal distances');}
        checked++;
      }
    }
    assert(peakFast>30000&&checked>5,'Large coherent armies must actually use the certified path');
    const r=e.regiments.find(r=>r.type==='swordsman'&&e.units[r.unitIds[0]]._bulkStepTick===e._tick);
    assert(r);e.command([r.id],'hold');const held=r.unitIds.map(id=>[e.units[id].x,e.units[id].y]);e.step(DT);
    for(const [i,id]of r.unitIds.entries()){const u=e.units[id];assert(u._bulkStepTick!==e._tick);assert(Math.hypot(u.x-held[i][0],u.y-held[i][1])<.01,'A fresh Hold must stop the optimized march');}
    const victim=e.units[r.unitIds[0]],attacker=e.units.find(u=>u.team!==victim.team);
    e.impacts.weaponImpulse(attacker,victim,-1,0,80,.5,0);e.step(DT);
    assert.equal(victim.state,'knocked');assert(victim._bulkStepTick!==e._tick);
    assert.equal(e._bodyX[victim.id],victim.x);assert.equal(e._bodyY[victim.id],victim.y);
    return { peakCertifiedSoldiers:peakFast, uniformTranslationsChecked:checked, units:e.units.length };
  });
  test('New ground and structure metadata survive normalization and drive the real terrain model', () => {
    const ground=['road','mud','marsh','snow','trench'];
    const regions=ground.map((type,i)=>({type,x:500+i*350,y:500,w:250,h:250}));
    regions.push({type:'forest',x:500,y:1000,w:600,h:600},{type:'mud',x:500,y:1000,w:600,h:600},{type:'road',x:500,y:1000,w:600,h:600});
    regions.push({type:'water',x:1500,y:1000,w:500,h:500},{type:'road',x:1500,y:1000,w:500,h:500},{type:'bridge',x:1450,y:1150,w:600,h:180});
    const kinds=['stone','palisade','earthwork','house','watchtower','barricade','keep'];
    regions.push(...kinds.map((structureKind,i)=>({type:'wall',structureKind,name:'Fort '+structureKind,x:500+i*350,y:2000,w:150,h:150,team:1,hp:1200})));
    const e=new Engine({map:'custom',customMap:{regions},armies:[{roster:{militia:1}},{roster:{guardian:1}}]});
    for(const [i,id]of ground.entries()){const actual=e.terrainAt(600+i*350,600);assert.equal(actual.type,id);assert.equal(actual.speed,D.TERRAIN_TYPES[id].speed);assert.equal(actual.cover,D.TERRAIN_TYPES[id].cover);assert.equal(actual.blocked,false);}
    assert.equal(e.terrainAt(700,1200).type,'road','Latest passable paint must win');
    assert.equal(e.terrainAt(1700,1050).type,'water');assert.equal(e.terrainAt(1700,1200).type,'bridge');
    assert.deepEqual(e.structures.map(s=>s.structureKind),kinds);assert(e.structures.every(s=>s.structureHeight>0&&s.name==='Fort '+s.structureKind));
    const largest={map:'custom',customMap:{regions:Array.from({length:220},(_,i)=>({type:'road',x:40+i*22,y:3000,w:20,h:20}))},armies:[{roster:{militia:1}},{roster:{militia:1}}]};
    const normalized=new Engine(largest);assert.equal(normalized.config.customMap.regions.length,192);
  });
}
module.exports=register;
if(require.main===module){global.window=global;for(const f of ['data','tactics','impacts','siege','engine'])require('../src/'+f+'.js');register((name,fn)=>console.log('PASS '+name+' '+JSON.stringify(fn()||{})));}
