import assert from 'node:assert/strict';
import test from 'node:test';
import {layoutMap,MAPS} from '../engine.js';
import {billboardBasis,BILLBOARD_RIGHT,BILLBOARD_UP,ELEVATION,getViewMatrix,pickGround,projectGround,projectWorld} from '../world-camera.js';
import {buildTerrainMesh} from '../world-mesh.js';

const dimensions=[1440,920];
const distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const clip=(m,x,y,z)=>[m[0]*x+m[4]*y+m[8]*z+m[12],m[1]*x+m[5]*y+m[9]*z+m[13]];

test('the same orthographic camera positions terrain, billboards and pointer hits',()=>{
 const map=layoutMap('cartonworks',2);
 const camp=map.camps[0],camera={x:camp.x+337,y:camp.y+181,zoom:.79};
 const matrix=getViewMatrix(camera,...dimensions);
 for(const [x,y,z] of [[camp.x,camp.y,ELEVATION],[2220,3450,0],[3180,2960,89]]){
  const hit=projectWorld(camera,...dimensions,x,y,z), [nx,ny]=clip(matrix,x,y,z);
  assert(Math.abs(hit.x-(1+nx)*dimensions[0]/2)<.001);
  assert(Math.abs(hit.y-(1-ny)*dimensions[1]/2)<.001);
  const right=projectWorld(camera,...dimensions,x+BILLBOARD_RIGHT[0],y+BILLBOARD_RIGHT[1],z+BILLBOARD_RIGHT[2]);
  const up=projectWorld(camera,...dimensions,x+BILLBOARD_UP[0],y+BILLBOARD_UP[1],z+BILLBOARD_UP[2]);
  assert(Math.abs(right.x-hit.x-camera.zoom)<1e-8&&Math.abs(right.y-hit.y)<1e-8);
  assert(Math.abs(up.y-hit.y+camera.zoom)<1e-8&&Math.abs(up.x-hit.x)<1e-8);
  assert(Math.abs(right.depth-hit.depth)<1e-8&&Math.abs(up.depth-hit.depth)<1e-8);
 }
 const platform=map.plateaus.find(p=>p.campTeam===0),ramp=platform.ramps.find(r=>r.side==='south');
 const points=[camp,{x:ramp.x+ramp.w/2,y:ramp.y+ramp.h*.82},{x:camp.x,y:camp.y+1300}];
 for(const p of points){
  const screen=projectGround(map,camera,...dimensions,p.x,p.y);
  const unprojected=pickGround(map,camera,...dimensions,screen.x,screen.y);
  assert(distance(p,unprojected)<.001,JSON.stringify({p,unprojected}));
 }
 const rotated={...camera,yaw:-33,pitch:35};
 const at=projectGround(map,rotated,...dimensions,camp.x,camp.y);
 const body=billboardBasis(rotated);
 const up=projectWorld(rotated,...dimensions,camp.x+body.up[0],camp.y+body.up[1],ELEVATION+body.up[2]);
 assert(Math.abs(up.y-at.y+rotated.zoom)<1e-8&&Math.abs(up.x-at.x)<1e-8);
 assert(distance(pickGround(map,rotated,...dimensions,at.x,at.y),camp)<.001);
});

test('all battlegrounds build bounded finite triangle data with solid and shadow passes',()=>{
 for(const id of Object.keys(MAPS))for(const players of [2,3,4]){
  const map=layoutMap(id,players);
  const terrain=buildTerrainMesh(map,id==='rainyard'||id==='cartonworks');
  assert(terrain.solids.length>3500*terrain.stride&&terrain.solids.length<90000*terrain.stride,id);
  for(const data of [terrain.solids,terrain.translucent]){
   assert.equal(data.length%(3*terrain.stride),0,id);
   assert(data.every(Number.isFinite),id);
   assert(data.length>0,id);
  }
  let upper=false,lower=false;
  for(let i=2;i<terrain.solids.length;i+=terrain.stride){
   upper||=terrain.solids[i]===ELEVATION;
   lower||=terrain.solids[i]===0;
  }
  assert(upper&&lower,id);
 }
});
