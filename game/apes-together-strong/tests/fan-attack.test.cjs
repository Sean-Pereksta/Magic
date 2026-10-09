'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');

function arena(){
 const c=loadEngine(),g=new c.ATSGame('FAN-ATTACK');
 g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();
 g.world.ensure=()=>{};g.world.stream=()=>{};g.world.getSites=()=>[];
 g.world.terrain=()=>({biome:'forest',water:false});g.world.waterBlocked=()=>false;
 g.spawnSites=()=>{};g.nextDirectorAt=g.nextConvoyAt=g.heliTimer=1e9;
 g.king.x=g.king.y=0;return {c,g};
}
function ape(g,species='gibbon',state='follow'){
 const a=g.makeApe(0,0,state);a.species=species;g.siege.balance(a,true);a.attackCD=0;return a;
}
function grids(g){
 g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);g.humanGrid.rebuild(g.humans);
 g.vehicleGrid.rebuild(g.vehicles);g.navigation.beginFrame(g.time);g.performance.beginStep(g);
}
function step(g,n=1){for(let i=0;i<n;i++){g.time+=.05;grids(g);g.siege.tick(.05);for(const a of g.apes)g.updateApe(a,.05)}}
function width(list){return Math.max(...list.map(a=>a.y))-Math.min(...list.map(a=>a.y))}

test('the ordinary fan attack physically widens its front and still hits enemies',()=>{
 const {g}=arena(),units=Array.from({length:24},()=>ape(g)),resident=ape(g,'gibbon','settled');
 grids(g);assert.equal(g.command('spreadCharge',{x:1,y:0}),true);
 assert.ok(units.every(a=>a.state==='charge'&&a.commandStyle==='spreadCharge'));
 assert.equal(resident.state,'settled');
 step(g,20);assert.ok(width(units)>200,'the outside lanes move apart during the charge');
 assert.ok(units.every(a=>a.x>75),'every lane advances toward the attack');
 const wing=units[0],enemy=g.makeHuman(wing.x+20,wing.y,null),hp=enemy.hp;
 step(g,8);assert.ok(enemy.hp<hp,'a fanned attacker continues ordinary combat');
});

test('selected species fan across one shared arc, advance, and attack without recruiting other apes',()=>{
 const {g}=arena(),units=Array.from({length:24},(_,i)=>ape(g,i%2?'gorilla':'gibbon'));
 const outsider=ape(g,'orangutan'),resident=ape(g,'gibbon','settled');grids(g);
 g.siege.select(['gibbon','gorilla']);assert.equal(g.command('spreadCharge',{x:1,y:0}),true);
 const destinations=units.map(a=>a.armyOrder.fanTarget);
 assert.ok(width(destinations)>800,'selected species occupy the full fan');
 assert.equal(new Set(destinations.map(p=>p.y)).size,24,'species do not reuse lanes');
 assert.ok(destinations.every(p=>Math.abs(Math.hypot(p.x,p.y)-570)<1e-6));
 assert.equal(outsider.armyOrder,undefined);assert.equal(resident.armyOrder,undefined);
 step(g,20);assert.ok(width(units)>150,'the selected front physically fans out');
 assert.ok(units.every(a=>a.x>50));
 const wing=units[0],enemy=g.makeHuman(wing.x+20,wing.y,null),hp=enemy.hp;
 step(g,8);assert.ok(enemy.hp<hp,'selected fan charge keeps siege combat active');
});

test('an individual selection shares only its own fan and later charge clears it',()=>{
 const {g}=arena(),left=ape(g),right=ape(g),other=ape(g);grids(g);
 g.siege.selectUnits([left.id,right.id]);assert.equal(g.command('spreadCharge',{x:0,y:1}),true);
 assert.ok(left.armyOrder.fanTarget.x>0&&right.armyOrder.fanTarget.x<0);
 assert.ok(left.armyOrder.fanTarget.y>0&&right.armyOrder.fanTarget.y>0);
 assert.equal(other.armyOrder,undefined);
 assert.equal(g.command('charge',{x:1,y:0}),true);
 assert.equal(left.armyOrder.fanTarget,undefined);assert.equal(right.armyOrder.fanTarget,undefined);
 assert.equal(left.armySpread,undefined);assert.equal(other.armyOrder,undefined);
 const only=left;g.siege.selectUnits([only.id]);assert.equal(g.command('spreadCharge',{x:0,y:-1}),true);
 assert.ok(Math.abs(only.armyOrder.fanTarget.x)<1e-6);
 assert.ok(only.armyOrder.fanTarget.y<0,'a lone selected ape follows the aimed direction');
});

test('selected fan destinations survive a save and route independently after restore',()=>{
 const {c,g}=arena(),units=Array.from({length:10},()=>ape(g));grids(g);
 g.siege.select('gibbon');g.command('spreadCharge',{x:1,y:0});
 const expected=units.map(a=>({...a.armyOrder.fanTarget}));
 const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize())));
 loaded.world.ensure=()=>{};loaded.world.stream=()=>{};loaded.world.getSites=()=>[];
 loaded.world.terrain=()=>({biome:'forest',water:false});loaded.world.waterBlocked=()=>false;
 for(let i=0;i<units.length;i++)assert.deepEqual({...loaded.apes[i].armyOrder.fanTarget},expected[i]);
 step(loaded,20);assert.ok(width(loaded.apes)>200);
 assert.notEqual(loaded.apes[0].siegeRoute.key,loaded.apes.at(-1).siegeRoute.key,'cached routes preserve each lane destination');
});

test('fan routes snapshot the aim and never rewrite a shared species order',()=>{
 const {g}=arena(),units=Array.from({length:12},()=>ape(g)),aim={x:1,y:0};grids(g);
 g.siege.select('gibbon');g.command('spreadCharge',aim);
 const order=g.siege.groups.gibbon.orders[0],snapshot=JSON.stringify(order);
 const destinations=units.map(a=>({...a.armyOrder.fanTarget}));
 aim.x=0;aim.y=-1;g.aim={x:0,y:-1};step(g,20);
 assert.equal(JSON.stringify(order),snapshot,'individual fan destinations stay off the shared order');
 for(let i=0;i<units.length;i++)assert.deepEqual({...units[i].armyOrder.fanTarget},destinations[i]);
 assert.ok(width(units)>200);assert.ok(units.every(a=>a.x>75),'later pointer movement cannot rotate the issued fan');
});

test('global recall replaces both ordinary and selected fan attacks and prevents stale routes',()=>{
 for(const selected of [false,true]){
  const {g}=arena(),units=Array.from({length:10},()=>ape(g)),resident=g.makeApe(1200,0,'settled','home');grids(g);
  if(selected)g.siege.select('gibbon');g.command('spreadCharge',{x:1,y:0});step(g,8);
  assert.equal(g.command('recallAll'),true);
  for(const a of [...units,resident]){
   assert.equal(a.state,'follow');assert.equal(a.armyOrder,undefined);assert.equal(a.siegeRoute,undefined);
   assert.equal(a.commandStyle,undefined);assert.equal(a.armySpread,undefined);assert.equal(a.recallOrder.kind,'all');
  }
  g.time+=.4;grids(g);g.siege.tick(.05);assert.equal(g.siege.groups.gibbon,undefined);
 }
});

test('skipping a fan charge clears its lane before the next queued charge',()=>{
 const {g}=arena(),a=ape(g),b=ape(g);grids(g);g.siege.select('gibbon');
 g.command('spreadCharge',{x:1,y:0});
 assert.equal(g.siege.issue(['gibbon'],[{id:'next-charge',type:'charge',x:0,y:600}],true),true);
 step(g);assert.ok(a.armyOrder.fanTarget);assert.ok(a.siegeRoute);
 g.siege.skip();assert.equal(a.armyOrder.fanTarget,undefined);assert.equal(b.armyOrder.fanTarget,undefined);
 assert.equal(a.siegeRoute,undefined);assert.equal(a.armyOrder.heading,undefined);
 step(g,8);assert.ok(a.target.y>590,'the next charge routes to its own destination');
 assert.ok(a.siegeRoute.key.startsWith('next-charge:'));
});
