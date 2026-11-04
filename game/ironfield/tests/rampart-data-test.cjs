'use strict';
const assert=require('node:assert/strict');
const SIDES=['north','south','east','west'];
function register(test){
  const D=global.GameData,Engine=global.BattleEngine;
  const inside=(x,y,r)=>x>=r.x&&x<=r.x+r.w&&y>=r.y&&y<=r.y+r.h;
  function components(decks){
    const result=[],seen=new Set();
    for(let i=0;i<decks.length;i++){
      if(seen.has(i))continue;
      const group=[],queue=[i];seen.add(i);
      while(queue.length){const at=queue.pop(),a=decks[at];group.push(a);
        for(let j=0;j<decks.length;j++){const b=decks[j];if(seen.has(j)||a.deckHeight!==b.deckHeight)continue;
          if(a.x<=b.x+b.w&&a.x+a.w>=b.x&&a.y<=b.y+b.h&&a.y+a.h>=b.y){seen.add(j);queue.push(j);}
        }
      }
      result.push(group);
    }
    return result;
  }
  function inspect(parts,label,expectedComponents,height){
    const decks=parts.filter(p=>p.walkable),stairs=decks.flatMap(p=>p.stairs||[]);
    assert(decks.length>0,label+' has no walkable deck');assert.equal(stairs.length,expectedComponents*4,label+' needs four interior accesses per independent ring');
    const e=new Engine({map:'custom',customMap:{name:'Rampart access trial',width:16000,height:11000,biome:'grassland',regions:parts},armies:[{roster:{guardian:1}},{roster:{guardian:1}}]});
    assert.equal(new Set(stairs.map(s=>s.id)).size,stairs.length,label+' has duplicated stair ids');
    for(const wall of decks){
      assert(['wall','gate'].includes(wall.type));assert.equal(wall.deckHeight,height);assert(SIDES.includes(wall.rampartSide));
      assert([wall.courtyardX,wall.courtyardY].every(Number.isFinite));assert(!e._bodyBlocked(wall.courtyardX,wall.courtyardY,10),label+' has a court response point inside solid terrain');
      assert(Array.isArray(wall.stairs));
      for(const s of wall.stairs){
        for(const key of ['x','y','w','h','width','height','groundX','groundY','deckX','deckY'])assert(Number.isFinite(s[key]),label+' has invalid stair '+key);
        assert(['x','y'].includes(s.axis));assert([-1,1].includes(s.dir));assert.equal(s.width,64);assert.equal(s.height,height);assert(s.w>=s.width&&s.h>=s.width);
        assert(inside(s.deckX,s.deckY,wall),label+' stair deck endpoint is outside its owning wall');assert(inside(s.groundX,s.groundY,s)&&inside(s.deckX,s.deckY,s));
        assert(!e._bodyBlocked(s.groundX,s.groundY,10),label+' stair begins inside a wall, moat, or building');
        const dx=s.deckX-s.groundX,dy=s.deckY-s.groundY;
        assert(Math.hypot(dx,dy)>0);assert.equal(s.axis==='x'?dy:dx,0);assert.equal(Math.sign(s.axis==='x'?dx:dy),s.dir);
      }
    }
    const rings=components(decks);assert.equal(rings.length,expectedComponents,label+' contains a disconnected tower or wall deck');
    for(const ring of rings){const access=ring.flatMap(p=>p.stairs);assert.equal(access.length,4);assert.deepEqual([...new Set(access.map(s=>s.id.split('-')[1]))].sort(),SIDES.slice().sort());}
    return{label,decks:decks.length,stairs:stairs.length,rings:rings.length};
  }
  test('Every fortress ring has connected wall decks and four clear interior stair accesses at minimum and default sizes',()=>{
    const results=[];
    for(const[kind,spec]of Object.entries(D.FORTRESS_TYPES))for(const size of [{tag:'minimum',w:spec.minSize,h:spec.minSize},{tag:'default',w:spec.defaultWidth,h:spec.defaultHeight}]){
      const parts=D.createFortress(kind,2400,1600,size.w,size.h,{team:0});
      results.push(inspect(parts,kind+' '+size.tag,kind==='concentric'?2:1,kind==='palisade'?28:kind==='star'?14:36));
    }
    return results;
  });
  test('Legacy castle footprints and keep gates retain ground geometry while adding accessible decks',()=>{
    const args=[2400,1600,1200,1200],plain=D.castleWalls(...args,{walkable:false}),decks=D.castleWalls(...args);
    const footprint=parts=>parts.map(p=>[p.type,p.x,p.y,p.w,p.h,p.hp,p.team]);assert.deepEqual(footprint(plain),footprint(decks));assert.equal(decks.length,13);assert.equal(decks.filter(p=>p.type==='gate').length,1);
    const results=[];for(const size of [640,D.STRUCTURE_TYPES.keep.defaultWidth])results.push(inspect(D.createBattlefieldStructure('keep',2400,1600,size,size,{team:0}),'keep '+size,1,56));
    for(const kind of ['house','watchtower','barricade'])assert(D.createBattlefieldStructure(kind,2400,1600).every(p=>!p.walkable),'A solid ordinary building unexpectedly became a castle platform');
    return results;
  });
  test('Four Winds assigns ranged companies to physical stairs while preserving courtyard ground starts and the gate reserve',()=>{
    const p=D.EXPANSION_PRESETS.find(p=>p.map==='fourwinds'),assigned=p.formations.filter(f=>f.rampart);
    assert.equal(assigned.length,2);assert.deepEqual(assigned.map(f=>f.type).sort(),['archer','crossbow']);
    const e=new Engine(p);for(const f of assigned){assert.equal(f.team,1);assert.deepEqual(f.rampart,{ranks:2,spacing:'normal'});const r=e.regiments.find(r=>r.name===f.name);assert(r,'The wall garrison lost its formation identity');for(const id of r.unitIds){const u=e.units[id];assert(!u._onRampart&&!u._rampartZ,'A garrison body spawned on the wall instead of walking the stairs');assert(!e._bodyBlocked(u.x,u.y,u._radius));}}
    const reserve=p.formations.find(f=>f.name==='Gate reserve');assert(reserve&&!reserve.rampart&&reserve.type==='hoplite');
    const map=D.EXPANSION_MAPS.find(m=>m.id==='fourwinds'),stair=map.regions.flatMap(r=>r.stairs||[]).find(s=>s.id.startsWith('stair-west-'));assert.equal(stair.groundY,2090);assert.equal(stair.deckY,2090);
    for(const offset of [-12,12])for(let x=stair.groundX;x<=stair.groundX+1920;x+=24){assert(!e._bodyBlocked(x,stair.groundY+offset,6));for(const u of e.units)if(u.team===1&&u.type==='guardian')assert(Math.hypot(u.x-x,u.y-stair.groundY-offset)>u._radius+6,'The authored west stair queue intersects its stationary ground guard');}
    assert.equal(D.EXPANSION_MAPS.length,8);assert.equal(D.HISTORICAL_BATTLES.length,30);
    return{garrisonCompanies:assigned.length,garrisonSoldiers:assigned.reduce((n,f)=>n+f.count,0),groundReserve:reserve.count};
  });
  test('Standalone wall brush regions preserve their footprint and support either chosen courtyard side',()=>{
    const results=[];
    for(const side of SIDES){const horizontal=side==='north'||side==='south',w=horizontal?1200:120,h=horizontal?120:1200,r=D.createRampartWall(2400,1600,w,h,{team:0,hp:3333,courtyardSide:side,worldWidth:16000,worldHeight:11000});
      assert(r);assert.deepEqual([r.type,r.x,r.y,r.w,r.h,r.hp,r.team],['wall',2400,1600,w,h,3333,0]);assert(r.walkable);assert.equal(r.stairs.length,1);assert.equal(r.rampartSide,{north:'south',south:'north',east:'west',west:'east'}[side]);const stair=r.stairs[0];assert(inside(stair.deckX,stair.deckY,r));
      const e=new Engine({map:'custom',customMap:{width:16000,height:11000,regions:[r]},armies:[{roster:{guardian:1}},{roster:{guardian:1}}]});assert(!e._bodyBlocked(stair.groundX,stair.groundY,10));assert(!e._bodyBlocked(r.courtyardX,r.courtyardY,10));results.push({courtyardSide:side,exterior:r.rampartSide});
    }
    assert.equal(D.createRampartWall(0,0,23,120),null);assert.equal(D.createRampartWall(15600,1600,120,1200,{worldWidth:16000,worldHeight:11000}),null);
    const gate=D.createRampartWall(2400,1600,120,400,{type:'gate',team:0});assert(gate.type==='gate'&&gate.castleGate&&gate.walkable);assert.equal(D.createRampartWall(2400,1600,120,1200).rampartSide,'west');assert.equal(D.createRampartWall(2400,1600,1200,120).rampartSide,'north');return results;
  });
  test('Authored bare rampart lines and redoubt walls receive bounded clear interior stair access',()=>{
    const results=[];
    for(const[id,x,count]of [['moat',3520,5],['mountainfort',3720,5],['imperialwall',7000,7],['highcitadel',9200,7]]){
      const map=D.MAPS.find(m=>m.id===id),walls=map.regions.filter(r=>['wall','gate'].includes(r.type)&&r.x===x);assert.equal(walls.length,count);const e=new Engine({map:id});
      for(const wall of walls){assert(wall.walkable&&wall.rampartSide==='west');assert.equal(wall.stairs.length,1);const stair=wall.stairs[0];assert(stair.groundX>wall.x+wall.w);assert(inside(stair.deckX,stair.deckY,wall));assert(!e._bodyBlocked(stair.groundX,stair.groundY,10));assert(!e._bodyBlocked(wall.courtyardX,wall.courtyardY,10));assert(stair.groundX<map.width-20&&stair.groundY>=20&&stair.groundY<map.height-20);if(wall.type==='gate')assert(wall.castleGate);}
      results.push({map:id,wallAndGateAccesses:walls.length});
    }
    const borodino=D.HISTORICAL_BATTLES.find(p=>p.id==='historyBorodino'),redoubts=borodino.customMap.regions.filter(r=>r.structureKind==='earthwork');assert.equal(redoubts.length,3);assert(redoubts.every(r=>r.walkable&&r.deckHeight===14));
    for(const map of D.MAPS)for(const r of map.regions)if(['house','barricade','watchtower'].includes(r.structureKind)&&!r.castleTower)assert(!r.walkable,'A standalone solid building was converted into a wall deck');return results;
  });
}
module.exports=register;
if(require.main===module){global.window=global;for(const name of ['data','tactics','impacts','siege','ramparts','engine'])require('../src/'+name+'.js');register((name,fn)=>{try{console.log('PASS '+name+' '+JSON.stringify(fn()));}catch(error){console.error('FAIL '+name+'\n'+error.stack);process.exitCode=1;}});}
