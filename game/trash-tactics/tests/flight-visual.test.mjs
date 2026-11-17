import assert from 'node:assert/strict';
import test from 'node:test';
import {Game,MAPS,TYPES,layoutMap} from '../engine.js';
import {actorBounds} from '../actor-billboards.js';
import {actorLift,actorTerrainHeight} from '../flight-visual.js';
import {snapshotFor} from '../online-snapshot.mjs';
import {terrainHeight} from '../terrain.js';
import {Renderer} from '../render.js';

test('aircraft start climbing before cliffs and descend smoothly past their far edges',()=>{
 const map={plateaus:[{x:400,y:400,w:250,h:300,height:1}]};
 for(const type of ['cd','airplane']){
  const e={type,r:TYPES[type].r,y:550};
  const at=x=>actorTerrainHeight(map,{...e,x});
  assert.equal(at(100),0);
  assert(at(310)>0&&at(310)<1,'normal cruising height starts rising on approach');
  assert.equal(at(399),1,'the aircraft clears the top before its center reaches the cliff');
  assert.equal(at(500),1);
  assert(at(740)>0&&at(740)<1,'leaving the cliff starts a gradual descent');
  assert.equal(at(950),0);
 }
});

test('an intentional dive begins at the current hover height and tapers to zero',()=>{
 const e={id:1,type:'airplane'},dive={...e,crash:{diving:true,remaining:.65,total:.65}};
 assert.equal(actorLift(dive,0),actorLift(e,0));
 dive.crash.remaining=0;
 assert.equal(actorLift(dive,0),0);
});

test('remote aircraft get the same dive lift without exposing the enemy destination',()=>{
 const game=new Game({online:true});
 const e=game._spawn('airplane',1,1000,1100);
 e.crash={diving:true,remaining:.2,total:.65,x:1110,y:1200};
 game.isVisible=()=>true;
 const remote=snapshotFor(game,{team:0}).entities.find(entity=>entity.id===e.id);
 assert.deepEqual(remote.crash,{diving:true,remaining:.2,total:.65});
 assert.equal(actorLift(remote,game.time),actorLift(e,game.time));
});

test('flight height continuously clears raised land and every ramp across all skirmish layouts',()=>{
 for(const id of Object.keys(MAPS))for(const players of [2,3,4]){
  const map=layoutMap(id,players),e={type:'airplane',r:TYPES.airplane.r};
  for(const area of [...map.plateaus,...map.ramps])for(const vertical of [false,true]){
   const origin=vertical?area.y:area.x,end=origin+(vertical?area.h:area.w);
   const center=vertical?area.x+area.w/2:area.y+area.h/2;
   let previous;
   for(let along=origin-212;along<=end+212;along+=4){
    [e.x,e.y]=vertical?[center,along]:[along,center];
    const altitude=actorTerrainHeight(map,e);
    assert(altitude+1e-9>=terrainHeight(map,e.x,e.y),`${id}, ${players} players, ${area.id} clears the terrain`);
    if(previous!==undefined)assert(Math.abs(altitude-previous)<.035,`${id}, ${players} players, ${area.id} has no abrupt altitude change`);
    previous=altitude;
   }
  }
 }
});

test('Canvas and WebGL selection geometry follow the smoothed altitude over a cliff',()=>{
 const map={plateaus:[{x:400,y:400,w:250,h:300,height:1}]};
 const e={type:'cd',id:1,r:TYPES.cd.r,y:550};
 const renderer=Object.assign(Object.create(Renderer.prototype),{
  game:{map},width:1000,height:700,camera:{x:0,y:0,zoom:1,yaw:0,pitch:44},time:0
 });
 for(const bounds of [e=>renderer.actorBounds(e),e=>actorBounds(renderer,e)]){
  const flat=bounds({...e,x:100}),near=bounds({...e,x:399}),over=bounds({...e,x:401});
  assert(near.y<flat.y-30,'the hit area has already climbed on approach');
  assert(Math.abs(near.y-over.y)<1,'the hit area stays aligned when crossing the cliff edge');
 }
});
