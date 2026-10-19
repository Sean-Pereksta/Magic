const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
class Vector3{
 constructor(x=0,y=0,z=0){this.set(x,y,z);}set(x,y,z){Object.assign(this,{x,y,z});return this;}
 clone(){return new Vector3(this.x,this.y,this.z);}copy(v){return this.set(v.x,v.y,v.z);}length(){return Math.hypot(this.x,this.y,this.z);}
}
function fixture(){
 const ctx=vm.createContext({THREE:{Vector3,MathUtils:{clamp:(v,a,b)=>Math.max(a,Math.min(b,v))}},HOOP_POS:new Vector3(0,3.28,0),
  shotCharging:false,chargingPlayer:null,gameOver:false,hasShotThisPossession:false,ballOwner:'p1',possession:'p1',ballState:'dribble',
  layupActive:false,layupOwner:null,dunkActive:false,dunkOwner:null,shotPower:0,chargeDir:0,keys:{},
  flatDistance:(a,b)=>Math.hypot(a.x-b.x,a.z-b.z),getNearestOpponent:()=>null});
 for(const file of ['rendering.js','input-gameplay.js'])vm.runInContext(fs.readFileSync(path.join(__dirname,'../js',file),'utf8'),ctx);
 ctx.canDunk=()=>false;ctx.canHookShot=()=>false;
 ctx.player={id:'p1',controllerSlot:'p1',hasBall:true,onGround:true,moveState:'normal',group:{position:new Vector3(3.4,0,0)},
  velocity:new Vector3(-3.5,0,0),moveMeta:{},profile:{height:.94,width:.9,layup:.85}};
 return ctx;
}
test('a close running shot keeps its layup intent as the feet settle during the same hold',()=>{
 const c=fixture(),p=c.player;assert.equal(c.chooseShotType(p),'layup');c.handleActionButton(p);
 assert.equal(p.moveMeta.shotGatherIntent,'layup');p.velocity.set(-.15,0,0);p.moveMeta.chargeHeld=.85;
 assert.equal(c.chooseShotType(p),'layup','the meter and released shot keep the same finish');
});
test('close intent cannot extend finish range or convert a fresh standing jumper',()=>{
 const c=fixture(),p=c.player;c.handleActionButton(p);p.velocity.set(0,0,0);p.group.position.x=3.7;
 assert.equal(c.chooseShotType(p),'setshot','the existing3.6m running-finish limit still applies');
 p.group.position.x=3.4;c.shotCharging=false;c.chargingPlayer=null;
 assert.equal(c.chooseShotType(p),'setshot','old intent is inactive once its charge ends');
 c.handleActionButton(p);assert.equal(p.moveMeta.shotGatherIntent,null);assert.equal(c.chooseShotType(p),'setshot');
});
test('a stopped dunk approach falls back to a layup instead of forcing dunk eligibility',()=>{
 const c=fixture(),p=c.player;c.canDunk=()=>true;c.handleActionButton(p);
 assert.equal(p.moveMeta.shotGatherIntent,'dunk');c.canDunk=()=>false;p.velocity.set(0,0,0);
 assert.equal(c.chooseShotType(p),'layup');
});
test('a held shot can earn a running finish after entering the paint, and loses it when leaving range',()=>{
 const c=fixture(),p=c.player;p.group.position.x=4.1;c.handleActionButton(p);
 assert.equal(p.moveMeta.shotGatherIntent,null);p.group.position.x=3.4;
 assert.equal(c.chooseShotType(p),'layup');p.velocity.set(0,0,0);
 assert.equal(c.chooseShotType(p),'layup','stopping after the legitimate entry keeps that gather');
 p.group.position.x=3.8;assert.equal(c.chooseShotType(p),'setshot');assert.equal(p.moveMeta.shotGatherIntent,null);
 p.group.position.x=3.4;assert.equal(c.chooseShotType(p),'setshot','returning while stationary cannot resurrect the old entry');
});
test('human modern and classic motors leave active gather positions and approach velocity to the finish solver',()=>{
 for(const kind of ['layup','dunk']){
  const c=fixture(),p=c.player;c[kind+'Active']=true;c[kind+'Owner']=p.id;
  const before=JSON.stringify({position:p.group.position,velocity:p.velocity});
  // Neither entrypoint should consult an input style or apply a second motor.
  c.updateModernPlayerControl(p,{moveX:1,sprint:true},1/60);
  c.updatePlayerControl(p,{state:{moveX:1,sprint:true}},1/60);
  assert.equal(JSON.stringify({position:p.group.position,velocity:p.velocity}),before);
  assert.equal(c.activeFinishOwnsMovement({id:'p2'}),false,'the defender still has normal controls');
 }
});
