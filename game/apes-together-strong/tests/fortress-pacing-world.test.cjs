'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
function engine(){const c=vm.createContext({console,Math,Map,Set});c.window=c;vm.runInContext(fs.readFileSync(path.join(__dirname,'..','world.js'),'utf8'),c);return c}
const ranges={transport:[2,4],hunter:[4,7],research:[8,14],checkpoint:[8,16],prison:[18,30],detention:[42,72],experimental:[80,125],forwardBase:[32,60],armoredDepot:[60,110],regionalCommand:[160,260]};
function plans(w){const found=new Map();for(let cy=-24;cy<=24;cy++)for(let cx=-24;cx<=24;cx++){const plan=w._sitePlan(cx,cy);if(plan&&!found.has(plan.type))found.set(plan.type,{plan,cx,cy})}return found}
function openWorld(c){const w=new c.ATSWorld('facility-collision');w.ensure=()=>{};w._sitePlan=()=>null;w.terrain=()=>({biome:'forest',water:false,road:false});w.waterBlocked=()=>false;return w}

test('fresh cage progression starts small and preserves every procedural site type',()=>{
 const c=engine(),w=new c.ATSWorld('fortress-cage-progression'),found=plans(w);assert.equal(found.size,10);
 w.ensure(0,0,800);assert.equal(w.sites.get('opening-rescue').count,2);assert.equal(w.sites.get('opening-hunters').count,6);
 for(const [type,{plan,cx,cy}]of found){w._generateChunk(cx,cy);const site=w.sites.get(plan.id),[minimum,maximum]=ranges[type];if(!site.tutorial)assert.ok(site.count>=minimum&&site.count<=maximum,type+' stock');const cages=site.objects.map(id=>w.objects.get(id)).filter(o=>o.type==='cage');assert.equal(cages.reduce((sum,o)=>sum+o.prisoners,0),site.count,type+' physical stock');assert.equal(cages.reduce((sum,o)=>sum+o.count,0),site.count)}
});

test('larger tiered fortresses leave dry walls, main roads and hull-clear access lanes',()=>{
 const c=engine(),w=new c.ATSWorld('fortress-cage-progression'),found=plans(w),tiers={forwardBase:2,armoredDepot:3,regionalCommand:4},heights=[0,28,45,68,95],hp=[0,240,620,1400,2800];
 for(const type of Object.keys(tiers)){
  const {plan,cx,cy}=found.get(type);w._generateChunk(cx,cy);const site=w.sites.get(plan.id),objects=site.objects.map(id=>w.objects.get(id)),walls=objects.filter(o=>o.type==='wall'&&!o.barricade),gates=objects.filter(o=>o.type==='gate'),tier=tiers[type];
  assert.equal(site.radius,{forwardBase:420,armoredDepot:540,regionalCommand:690}[type]);assert.ok(site.extentX>site.extentY,'rectangular fortress occupies a broad dry shelf');
  if(type==='regionalCommand'){assert.ok(site.guards>=82&&site.guards<=120);assert.ok(site.extentX*site.extentY*4>700000,'late fortress has a much larger defended area')}
  if(type==='forwardBase')assert.ok(site.guards>=30&&site.guards<=50);
  assert.ok(walls.length>40);for(const wall of walls){assert.equal(wall.wallTier,tier);assert.equal(wall.height,heights[tier]);assert.equal(wall.visualHeight,heights[tier]);assert.equal(wall.maxHp,hp[tier]);assert.equal(wall.faction,'human');assert.equal(wall.climbable,tier<=2);assert.equal(w.terrain(wall.x,wall.y).water,false);assert.equal(w.terrain(wall.x,wall.y).road,false)}
  assert.ok(gates.length);for(const gate of gates){assert.equal(gate.climbable,false);assert.ok(gate.maxHp>hp[tier]);assert.ok(gate.height>heights[tier])}
  const [from,to]=site.approach;for(let i=0;i<=16;i++){const x=from.x+(to.x-from.x)*i/16,y=from.y+(to.y-from.y)*i/16;assert.equal(w.waterBlocked(x,y,31),false,'supply approach retains tank bank clearance')}
  for(const kind of ['truck','apc','tank']){const staging=w.vehicleStaging(site,kind,0);assert.ok(staging);assert.equal(w.vehicleBlocked(staging.x,staging.y,w.vehicleRadius(kind),kind),false)}
  const capacity=w.militaryCapacity(site);assert.ok(Object.values(capacity.inventory).every(Number.isFinite));assert.ok(site.campaignReserve.personnel>0&&Number.isFinite(site.campaignReserve.personnel));
 }
});

test('fresh fortress reservations keep neighboring compounds outside the perimeter in either streaming order',()=>{
 const c=engine(),layouts=[];
 for(const order of [[[-8,-6],[-9,-6]],[[-9,-6],[-8,-6]]]){
  const w=new c.ATSWorld('fortress-cage-progression');for(const [cx,cy]of order)w._generateChunk(cx,cy);
  const base=w.sites.get('site:-8,-6');assert.equal(base.type,'forwardBase');assert.equal(w.sites.has('site:-9,-6'),false,'research buildings no longer intersect this perimeter');
  for(let dy=-1;dy<=1;dy++)for(let dx=-1;dx<=1;dx++){
   const satellite=w._sitePlan(-8+dx,-6+dy);if(!satellite||satellite.id===base.id||satellite.military)continue;
   assert.ok(Math.abs(satellite.x-base.x)>=base.footprintX+satellite.radius+40||Math.abs(satellite.y-base.y)>=base.footprintY+satellite.radius+40,'adjacent compounds retain physical clearance');
  }
  layouts.push(base.objects.map(id=>{const o=w.objects.get(id);return[id,o.type,o.x,o.y,o.hp]}));
 }
 assert.deepEqual(layouts[0],layouts[1],'streaming a neighbor first cannot shift the fortress');
});

test('loading old compounds preserves captive stock, wounds, explored layout and access geometry',()=>{
 const c=engine(),w=new c.ATSWorld('legacy-fortress-layout'),site={id:'site:13,0',type:'armoredDepot',military:true,x:10340,y:590,radius:345,tier:4,count:17,guards:32,strength:11,known:true,radioDown:true,objects:['old-wall','old-gate','old-cage'],approach:[{x:10340,y:961},{x:9950,y:961}],staging:[{x:9950,y:961}],vehicleInventory:{tank:0},armorCapacity:0};
 const wall={id:'old-wall',siteId:site.id,type:'wall',x:10069,y:590,w:15,h:39,height:30,hp:71,maxHp:145,solid:true,collision:'rect'},gate={id:'old-gate',siteId:site.id,type:'gate',x:10340,y:861,w:93,h:19,height:44,hp:80,maxHp:190,solid:true,collision:'rect'},cage={id:'old-cage',siteId:site.id,type:'cage',x:10320,y:560,w:64,h:51,height:45,hp:31,maxHp:85,count:17,prisoners:17,solid:true};
 w.sites.set(site.id,site);for(const o of[wall,gate,cage])w.objects.set(o.id,o);w.discovered.add('13,0');w._coldChunks.set('13,0',{id:'13,0',cx:13,cy:0,sites:[site.id],changes:[]});
 const loaded=c.ATSWorld.fromJSON(JSON.parse(JSON.stringify(w.serialize()))),saved=loaded.sites.get(site.id);assert.equal(loaded._sitePlan(13,0),saved,'recorded site overrides the fresh procedural parcel');loaded._generateChunk(13,0);
 assert.equal(saved.x,10340);assert.equal(saved.y,590);assert.equal(saved.radius,345);assert.equal(saved.count,17);assert.equal(saved.strength,11);assert.equal(saved.radioDown,true);assert.equal(saved.approach[0].y,961);assert.equal(saved.vehicleInventory.tank,0);assert.ok(loaded.discovered.has('13,0'));
 const old=loaded.objects.get(wall.id);assert.equal(old.hp,71);assert.equal(old.maxHp,145);assert.equal(old.height,30);assert.equal(old.x,wall.x);assert.equal(old.wallTier,1);assert.equal(old.faction,'human');assert.equal(old.climbable,true);assert.equal(loaded.objects.get(gate.id).climbable,false);assert.equal(loaded.objects.get(gate.id).maxHp,190);assert.equal(loaded.objects.get(cage.id).count,17);assert.equal(loaded.objects.get(cage.id).hp,31);
 const again=c.ATSWorld.fromJSON(JSON.parse(JSON.stringify(loaded.serialize())));assert.equal(again.objects.get(wall.id).hp,71);assert.equal(again.objects.get(cage.id).prisoners,17);assert.equal(again._sitePlan(13,0).approach[1].x,9950);
});

test('completed commissioned footprints activate once, evacuate occupants and persist their damage',()=>{
 const c=engine(),w=openWorld(c),tower={id:'tower',kind:'spearTower',x:0,y:0,stage:4,hp:220,maxHp:220},training={id:'training',kind:'training',x:200,y:0,stage:3,hp:0,maxHp:180},s={id:'home',huts:[],structures:[tower],facilities:[tower,training]},king={id:'king',x:20,y:0,hp:160},builder={id:'ape-builder',x:0,y:0,hp:120},tank={id:'vehicle-tank',x:200,y:0,hp:1000,vehicleClass:'tank'},g={king,apes:[builder],humans:[],vehicles:[tank],navigation:{profile:a=>a.vehicleClass||'ape'}};
 w.syncSettlementBuildings(s,g);assert.equal(w.objects.size,1,'facility references are deduplicated');assert.equal(w.objects.get('tower:collision').w,42);assert.equal(w.objects.get('tower:collision').h,34);assert.equal(w.actorBlocked(king.x,king.y,12,'ape'),false);assert.equal(w.actorBlocked(builder.x,builder.y,10,'ape'),false);assert.equal(w.objects.has('training:collision'),false,'unfinished work has no completed-building collision');
 training.stage=4;training.hp=180;w.syncSettlementBuildings(s,g);const collider=w.objects.get('training:collision');assert.equal(collider.w,64);assert.equal(collider.h,46);assert.equal(collider.maxHp,180);assert.equal(collider.structureId,'training');assert.equal(w.vehicleBlocked(tank.x,tank.y,31,'tank'),false);assert.notEqual(tank.x,200,'a tank under a newly completed footprint moves to safe ground');
 const revision=w.navRevision;w.syncSettlementBuildings(s,g);assert.equal(w.navRevision,revision,'ordinary syncing never reactivates completed geometry');
 training.hp=0;w.syncSettlementBuildings(s,g);assert.equal(collider.dead,true);assert.equal(collider.solid,false);assert.equal(w.vehicleBlocked(200,0,31,'tank'),false);
 const loaded=c.ATSWorld.fromJSON(JSON.parse(JSON.stringify(w.serialize())));loaded.ensure=()=>{};loaded._sitePlan=()=>null;loaded.terrain=w.terrain;loaded.waterBlocked=()=>false;const restored=JSON.parse(JSON.stringify(s));king.x=builder.x=0;king.y=builder.y=0;loaded.syncSettlementBuildings(restored,g);assert.equal(loaded.objects.get('training:collision').hp,0);assert.equal(loaded.actorBlocked(builder.x,builder.y,10,'ape'),false,'restored occupied footprints receive one safe evacuation');assert.equal(loaded.actorBlocked(king.x,king.y,12,'ape'),false);
});
