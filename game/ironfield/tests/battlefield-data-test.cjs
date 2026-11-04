'use strict';
const assert=require('node:assert/strict');
const NEW_FIELDS=['timberfort','fourwinds','reedmoat','starhaven','twincrown','kingsroad','whitepass','sablelines'];
function register(test) {
  const D=global.GameData,Engine=global.BattleEngine;
  test('Grand collection keeps old fields and adds eight larger authored encounters plus thirty sourced historical battles',()=>{
    assert.equal(D.MAX_UNITS,40000);assert.equal(D.MAX_TERRAIN_REGIONS,192);
    assert.deepEqual(D.EXPANSION_MAPS.map(m=>m.id),NEW_FIELDS);assert.equal(D.EXPANSION_PRESETS.length,8);assert.equal(D.HISTORICAL_BATTLES.length,30);
    assert.equal(D.MAPS.length,31);assert.equal(D.PRESETS.length,62);assert.equal(new Set(D.MAPS.map(m=>m.id)).size,D.MAPS.length);
    for(const map of D.EXPANSION_MAPS){assert(map.width*map.height>=9600*6600);assert(map.regions.length<=D.MAX_TERRAIN_REGIONS);for(const r of map.regions){assert(r.x>=0&&r.y>=0&&r.w>0&&r.h>0&&r.x+r.w<=map.width&&r.y+r.h<=map.height,map.id+' has terrain beyond its physical field');}assert(D.EXPANSION_PRESETS.some(p=>p.map===map.id));}
    for(const type of ['road','mud','marsh','snow','trench'])assert(D.EXPANSION_MAPS.some(m=>m.regions.some(r=>r.type===type)),type+' has no authored use');
    return{maximumSoldiers:D.MAX_UNITS,authoredFields:D.MAPS.length-1,historicalBattles:D.HISTORICAL_BATTLES.length,newFields:NEW_FIELDS};
  });
  test('Fortress and structure templates expose usable physical gates and safe material metadata',()=>{
    const kinds=new Set(['stone','palisade','earthwork','house','watchtower','barricade','keep']);
    for(const [kind,spec]of Object.entries(D.FORTRESS_TYPES)){
      const parts=D.createFortress(kind,1000,1000,spec.defaultWidth,spec.defaultHeight,{team:0,name:'Template trial'});
      assert(parts.length>0);assert(parts.filter(r=>r.type==='gate').length>=(kind==='classic'?1:kind==='concentric'?8:4));
      for(const r of parts){assert([r.x,r.y,r.w,r.h].every(Number.isFinite));assert(r.w>0&&r.h>0);if(r.type==='wall'||r.type==='gate'){assert(kinds.has(r.structureKind));assert.equal(r.team,0);assert(r.name);}if(r.type==='gate')assert(Math.max(r.w,r.h)>=160,'A template doorway is narrower than the navigation grid can safely use');}
      if(kind==='moated'){assert.equal(parts.filter(r=>r.type==='water').length,4);assert.equal(parts.filter(r=>r.type==='bridge').length,4);}
      if(kind==='star')assert(parts.some(r=>r.name.endsWith('bastion')));
      assert.deepEqual(D.createFortress(kind,1000,1000,spec.minSize-1,spec.minSize-1),[]);
    }
    for(const [kind,spec]of Object.entries(D.STRUCTURE_TYPES)){
      const parts=D.createBattlefieldStructure(kind,1000,1000,spec.defaultWidth,spec.defaultHeight,{team:0,name:'Named structure'});
      assert(parts.length>0);assert(parts.every(p=>p.team===0&&p.name.startsWith('Named structure')));
      if(kind==='keep')assert.equal(parts.filter(p=>p.type==='gate').length,2);else assert.equal(parts[0].structureKind,kind);
    }
    assert.deepEqual(D.createFortress('unknown',0,0,3000,3000),[]);assert.deepEqual(D.createBattlefieldStructure('unknown',0,0),[]);
    return{fortressKinds:Object.keys(D.FORTRESS_TYPES),structureKinds:Object.keys(D.STRUCTURE_TYPES)};
  });
  test('New grand encounters preserve intended deployment centers without bodies in walls or overlapping formations',()=>{
    const results=[];
    for(const p of D.EXPANSION_PRESETS){
      const e=new Engine(p),cells=new Map();let min=Infinity;
      assert(e.units.length>=3000);assert.equal(e.units.length,p.formations.reduce((n,f)=>n+f.count,0));assert.equal(e.regiments.length,p.formations.length);
      for(let i=0;i<e.regiments.length;i++)assert(Math.hypot(e.regiments[i].x-p.formations[i].x,e.regiments[i].y-p.formations[i].y)<8,p.name+': a formation had to move away from authored solid terrain');
      for(const u of e.units){
        assert([u.x,u.y,u.angle].every(Number.isFinite));assert(!e._bodyBlocked(u.x,u.y,u._radius),p.name+': unit inside a solid');assert(u.x>=u._radius&&u.y>=u._radius&&u.x<=e.width-u._radius&&u.y<=e.height-u._radius,p.name+': unit beyond map');
        const cx=Math.floor(u.x/80),cy=Math.floor(u.y/80);
        for(let yy=cy-1;yy<=cy+1;yy++)for(let xx=cx-1;xx<=cx+1;xx++)for(const v of cells.get(xx+':'+yy)||[]){const ratio=Math.hypot(u.x-v.x,u.y-v.y)/(u._radius+v._radius);min=Math.min(min,ratio);assert(ratio>=.9,p.name+': overlapping initial soldiers');}
        const key=cx+':'+cy;if(!cells.has(key))cells.set(key,[]);cells.get(key).push(u);
      }
      results.push({field:e.map.id,units:e.units.length,formations:e.regiments.length,minimumBodyRatio:Math.round(min*1000)/1000});
    }
    return results;
  });
}
module.exports=register;
if(require.main===module){global.window=global;for(const name of ['data','tactics','impacts','siege','engine'])require('../src/'+name+'.js');register((name,fn)=>{try{console.log('PASS '+name+' '+JSON.stringify(fn()));}catch(error){console.error('FAIL '+name+'\n'+error.stack);process.exitCode=1;}});}
