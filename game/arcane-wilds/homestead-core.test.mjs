import test from 'node:test';
import assert from 'node:assert/strict';
import C from './homestead-core.js';
const START=1700000000000;
function harness(){let home=C.fresh('journey',START),wallet={gold:100000,materials:Object.fromEntries(['timber','stone','fiber','iron','hide','dust','frost','ember','bone','voidshard'].map(k=>[k,10000]))},serial=0;
 const ctx={atHome:true,unlocked:['verdant','meridian','gloam'],visited:['sunmere']};
 const h={get home(){return home;},get wallet(){return wallet;},setTime(t){h.time=t;},time:START,
 act(type,args={},extra={}){const r=C.apply(home,wallet,{type,requestId:String(++serial),...args},h.time,{...ctx,...extra});home=r.home;wallet=r.wallet;return r;},
 craft(kind,x=6,y=4){h.act('craft',{kind});const id=home.items.at(-1).id;h.act('place',{id,x,y});return id;},
 plot(crop='lanternberry',tree=false){h.act('bed',{x:tree?2:11,y:tree?1:2,tree});const id=home.plots.at(-1).id;home.seeds[crop]=1;h.act('plant',{id,crop});return id;}};
 h.act('claimExpedition',{node:'danger'},{earned:true,multiWave:true});h.act('buildHouse');return h;
}
test('starter deed is one-time and a failed purchase never mutates either input',()=>{
 const h=harness(),gold=h.wallet.gold,claims=h.home.claims.length;
 h.act('claimExpedition',{node:'danger'},{earned:true,multiWave:true});assert.equal(h.wallet.gold,gold);assert.equal(h.home.claims.length,claims);
 const state=JSON.stringify(h.home),wallet={gold:0,materials:{}};
 assert.throws(()=>C.apply(h.home,wallet,{type:'craft',kind:'bed',requestId:'fail'},START,{atHome:true}),/Need/);
 assert.equal(JSON.stringify(h.home),state);assert.deepEqual(wallet,{gold:0,materials:{}});
});
test('twelve-hour crop earns only eight watered hours after ten elapsed hours',()=>{
 const h=harness(),id=h.plot('stormgrape');h.act('water',{id});h.setTime(START+10*C.HOUR);C.settle(h.home,h.time);
 assert.equal(h.home.plots[0].plant.growthMs,8*C.HOUR);assert.equal(C.ready(h.home.plots[0]),false);
 h.act('water',{id});h.setTime(START+14*C.HOUR);C.settle(h.home,h.time);assert.equal(C.ready(h.home.plots[0]),true);
});
test('never-watered plants pause and dry time is never credited retroactively',()=>{
 const h=harness(),id=h.plot('stormgrape');h.setTime(START+20*C.HOUR);h.act('water',{id});assert.equal(h.home.plots[0].plant.growthMs,0);
 h.setTime(START+21*C.HOUR);C.settle(h.home,h.time);assert.equal(h.home.plots[0].plant.growthMs,C.HOUR);
});
test('watering early refills a fixed capacity instead of stacking unlimited water',()=>{
 const h=harness(),id=h.plot('stormgrape');h.act('water',{id});h.setTime(START+C.HOUR);h.act('water',{id});h.act('water',{id});assert.equal(h.home.plots[0].plant.wateredUntil,START+9*C.HOUR);
});
test('ready crops remain available without spoilage or additional growth',()=>{
 const h=harness(),id=h.plot();h.act('water',{id});h.setTime(START+90*C.DAY);C.settle(h.home,h.time);assert.ok(C.ready(h.home.plots[0]));assert.equal(h.home.plots[0].plant.growthMs,C.HOUR/2);
 h.act('harvest',{id});assert.equal(h.home.produce.lanternberry,3);assert.equal(h.home.plots[0].plant,null);assert.throws(()=>h.act('harvest',{id}),/not mature/);
});
test('perennials preserve planting date, hold one harvest, and start a fresh unwatered cycle',()=>{
 const h=harness();h.act('buildHouse');const id=h.plot('sunapple',true);h.act('water',{id});h.setTime(START+C.DAY);h.act('water',{id});h.setTime(START+50*C.DAY);h.act('harvest',{id});
 const p=h.home.plots[0].plant;assert.equal(p.plantedAt,START);assert.equal(p.cycleStartedAt,h.time);assert.equal(p.harvestNumber,1);assert.equal(p.requiredGrowthMs,16*C.HOUR);assert.equal(p.growthMs,0);assert.equal(h.home.produce.sunapple,5);
});
test('fertilizer applies only once per cycle and compost consumes both ingredients',()=>{
 const h=harness();const id=h.plot('stormgrape');h.home.fertilizer=2;h.act('fertilize',{id});assert.equal(h.home.fertilizer,1);assert.equal(h.home.plots[0].plant.requiredGrowthMs,10.8*C.HOUR);assert.throws(()=>h.act('fertilize',{id}),/once/);
 h.craft('composter',11,9);h.home.produce.lanternberry=2;const fiber=h.wallet.materials.fiber;h.act('compost',{crop:'lanternberry'});assert.equal(h.wallet.materials.fiber,fiber-1);assert.equal(h.home.produce.lanternberry,0);assert.equal(h.home.fertilizer,2);
});
test('soil upgrades improve moisture but do not retroactively extend old watering',()=>{
 const h=harness(),id=h.plot('stormgrape');h.act('water',{id});h.act('upgradeSoil',{id});assert.equal(C.waterHours(h.home.plots[0]),12);assert.equal(h.home.plots[0].plant.wateredUntil,START+8*C.HOUR);
 assert.throws(()=>h.act('upgradeSoil',{id}),/Manor/);h.act('buildHouse');h.act('buildHouse');h.act('upgradeSoil',{id});assert.equal(C.waterHours(h.home.plots[0]),16);
});
test('irrigation exhausts finite stored water and never overwaters mature plants',()=>{
 const h=harness();h.act('buildHouse');const id=h.plot('emberpepper'),tank=h.craft('cistern',11,9);h.craft('sprinkler',12,9);h.act('refillCistern',{id:tank});h.home.items.find(i=>i.id===tank).water=1;
 h.setTime(START+30*C.HOUR);C.settle(h.home,h.time);assert.equal(h.home.plots[0].plant.growthMs,8*C.HOUR);assert.equal(h.home.items.find(i=>i.id===tank).water,0);
 h.act('refillCistern',{id:tank});h.setTime(START+40*C.HOUR);C.settle(h.home,h.time);assert.equal(C.ready(h.home.plots[0]),true);assert.equal(h.home.items.find(i=>i.id===tank).water,31);
});
test('installing sprinklers later never grants watering before installation',()=>{
 const h=harness();h.act('buildHouse');h.plot('emberpepper');const tank=h.craft('cistern',11,9);h.act('refillCistern',{id:tank});h.setTime(START+20*C.HOUR);h.craft('sprinkler',12,9);assert.equal(h.home.plots[0].plant.growthMs,0);
 h.setTime(START+21*C.HOUR);C.settle(h.home,h.time);assert.equal(h.home.plots[0].plant.growthMs,C.HOUR);
});
test('coin production caps at one day and never banks excess time behind the cap',()=>{
 const h=harness(),id=h.craft('coinbloom');h.setTime(START+10*C.DAY);h.act('collect',{id});const gold=h.wallet.gold;h.act('collect',{id});assert.equal(h.wallet.gold,gold);
 h.setTime(h.time+15*C.MINUTE);h.act('collect',{id});assert.equal(h.wallet.gold,gold+1);
});
test('packing, moving and upgrading a producer preserve earned output without free refills',()=>{
 const h=harness(),id=h.craft('coinbloom');h.setTime(START+C.HOUR);h.act('place',{id,x:7,y:4});assert.equal(h.home.items.find(i=>i.id===id).stored,4);
 h.act('pack',{id});h.setTime(START+10*C.HOUR);h.act('place',{id,x:7,y:4});assert.equal(h.home.items.find(i=>i.id===id).stored,4);
 h.act('upgradeProducer',{id});h.setTime(h.time+C.HOUR);C.settle(h.home,h.time);assert.equal(h.home.items.find(i=>i.id===id).stored,10);
});
test('only one placed producer per resource and gift dismantles never generate money',()=>{
 const h=harness();h.craft('coinbloom');h.act('craft',{kind:'coinbloom'});assert.throws(()=>h.act('place',{id:h.home.items.at(-1).id,x:8,y:4}),/one active producer/);
 const gift=h.home.items.find(i=>i.gift),gold=h.wallet.gold;h.act('dismantle',{id:gift.id});assert.equal(h.wallet.gold,gold);
});
test('duplicate request IDs never spend or collect twice',()=>{
 const h=harness(),id=h.craft('coinbloom');h.setTime(START+C.HOUR);h.act('collect',{id,requestId:'same'});const gold=h.wallet.gold;h.setTime(START+2*C.HOUR);const r=h.act('collect',{id,requestId:'same'});assert.equal(r.duplicate,true);assert.equal(h.wallet.gold,gold);
});
test('placement prevents overlaps, blocked doors and disconnected rooms',()=>{
 const h=harness();h.act('craft',{kind:'partition'});const item=h.home.items.at(-1);
 assert.match(C.placement(h.home,{...item,x:8,y:8,packed:false}),/entrance/);
 assert.match(C.placement(h.home,{...item,x:0,y:0,packed:false}),/within/);
 h.act('place',{id:item.id,x:6,y:4});h.act('craft',{kind:'chair'});assert.throws(()=>h.act('place',{id:h.home.items.at(-1).id,x:6,y:4}),/occupies/);
 // A complete horizontal partition disconnects the top two rows.
 h.home.items=[{id:'a',kind:'partition',x:6,y:6,rotation:1},{id:'b',kind:'partition',x:8,y:6,rotation:1}];
 assert.match(C.placement(h.home,{id:'c',kind:'partition',x:10,y:6,rotation:1}),/cuts off/);
});
test('house and garden limits cannot spill into roads or grow beyond their tier',()=>{
 const h=harness();assert.throws(()=>h.act('moveHouse',{x:12,y:5}),/clearing/);assert.throws(()=>h.act('bed',{x:0,y:0}),/garden/);
 for(let x=11;x<15;x++)h.act('bed',{x,y:2});assert.throws(()=>h.act('bed',{x:15,y:2}),/Expand/);assert.throws(()=>h.act('bed',{x:2,y:1,tree:true}),/Expand/);
});
test('portals validate visited settlements and continent or shadow gates',()=>{
 const ctx={visited:['sunmere','prism'],unlocked:['verdant','meridian'],shadowUnlocked:false,shadowWaystones:[1]};
 assert.equal(C.portalReason({kind:'villagePortal'},{id:'sunmere',town:true,continent:'verdant'},ctx),'');
 assert.match(C.portalReason({kind:'villagePortal'},{id:'prism',town:true,continent:'meridian'},ctx),/Continental/);
 assert.match(C.portalReason({kind:'continentalPortal'},{id:'unknown',town:true,continent:'verdant'},ctx),/Visit/);
 assert.match(C.portalReason({kind:'shadowPortal'},{shadow:true,depth:1},ctx),/Earn/);
 assert.equal(C.portalReason({kind:'shadowPortal'},{shadow:true,depth:1},{...ctx,shadowUnlocked:true}),'');
});
test('recipes consume real produce and require a placed station',()=>{
 const h=harness();h.home.produce.lanternberry=6;assert.throws(()=>h.act('cook',{recipe:'berryMeal'}),/station/);h.craft('stove');h.act('cook',{recipe:'berryMeal'});assert.equal(h.home.produce.lanternberry,3);assert.equal(h.home.meals.berryMeal,1);const r=h.act('eat',{recipe:'berryMeal'});assert.equal(r.effects[0].fraction,.35);assert.equal(h.home.meals.berryMeal,0);
});
test('normalization preserves journey and planting timestamps without granting elapsed growth',()=>{
 const h=harness();h.plot();const raw=JSON.parse(JSON.stringify(h.home)),n=C.normalize(raw,'new',START+C.DAY);assert.equal(n.journeyId,'journey');assert.equal(n.plots[0].plant.plantedAt,START);assert.equal(n.plots[0].plant.growthMs,0);
});
test('a backward clock never takes back or double-counts earned progress',()=>{
 const h=harness(),id=h.plot('stormgrape');h.act('water',{id});h.setTime(START+2*C.HOUR);C.settle(h.home,h.time);C.settle(h.home,START);assert.equal(h.home.plots[0].plant.growthMs,2*C.HOUR);C.settle(h.home,START+3*C.HOUR);assert.equal(h.home.plots[0].plant.growthMs,3*C.HOUR);
});
