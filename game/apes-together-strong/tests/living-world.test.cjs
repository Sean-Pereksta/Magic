'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {loadEngine}=require('./performance-harness.cjs');
function openWorld(){const c=loadEngine(),w=new c.ATSWorld('living-world');w.ensure=()=>{};w.terrain=()=>({biome:'forest',water:false});w.waterBlocked=()=>false;return {c,w};}
test('cover blocks enemy movement, admits its owner and does not stop outbound bullets',()=>{
 const {w}=openWorld(),human=w.createFortification({id:'human-line',x:0,y:0,team:'human'}),ape=w.createFortification({id:'ape-line',x:200,y:0,team:'ape',weak:true});
 assert.equal(human.maxHp,300);assert.equal(w.actorBlocked(0,0,12,'ape'),true);assert.equal(w.actorBlocked(0,0,12,'human'),false);
 assert.equal(w.actorBlocked(200,0,12,'ape'),false);assert.equal(w.actorBlocked(200,0,12,'human'),true);assert.equal(w.vehicleBlocked(200,0,31,'tank'),true);
 assert.equal(w.projectileBlocked(0,0),false);assert.equal(w.lineClear(-80,0,80,0),true);assert.equal(w.fortificationAt(0,0,12,'human'),human);
 w.damageFortification(human,200,4);assert.equal(human.damageStage,'damaged');w.damageFortification(human,100,5);assert.equal(w.actorBlocked(0,0,12,'ape'),false);assert.equal(human.solid,false);assert.equal(human.damageStage,'destroyed');assert.ok(w.objects.has(human.id),'breach debris persists');
 w.damageFortification(ape,1000,6);assert.equal(w.vehicleBlocked(200,0,31,'tank'),false);
});
test('tree work yields wood once and keeps selected scenery and building collisions after saving',()=>{
 const {c,w}=openWorld(),tree={id:'tree-work',type:'tree',x:0,y:0,r:20,moveRadius:8,size:1,hp:155,maxHp:155,solid:true},shade={...tree,id:'shade',x:80};
 for(const o of [tree,shade]){w.objects.set(o.id,o);w._indexObject(o)}const plot=w.settlementPlot(0,0,25);assert.equal(plot.valid,true);assert.equal(plot.trees.length,1);
 assert.equal(w.clearTree(tree,{settlementId:'home',time:3}),10);assert.equal(w.clearTree(tree,{settlementId:'home'}),0);assert.equal(shade.dead,undefined);
 const s={id:'home',huts:[{id:'hut',x:150,y:0,stage:4,hp:100,maxHp:100}]};w.syncSettlementBuildings(s);assert.equal(w.vehicleBlocked(150,0,31,'tank'),true);
 const loaded=c.ATSWorld.fromJSON(JSON.parse(JSON.stringify(w.serialize())));assert.equal(loaded.objects.get(tree.id).clearedBy,'home');assert.equal(loaded.objects.get('hut:collision').hiddenRender,true);assert.equal(loaded.clearTree(tree.id),0);
 s.huts[0].hp=0;w.syncSettlementBuildings(s);assert.equal(w.vehicleBlocked(150,0,31,'tank'),false);
});
test('surviving facilities reorganize finite reserves while destroyed infrastructure removes that capacity',()=>{
 const {w}=openWorld(),site={id:'base',military:true,tier:5,strength:0,initialStrength:40,objects:[],vehicleInventory:{tank:0},initialInventory:{tank:1},armorCapacity:0,campaignReserve:{personnel:50,vehicles:{tank:2},armor:24},nextMobilizationAt:90};
 assert.equal(w.replenishInstallation(site,89),false);assert.equal(site.strength,0);assert.equal(w.replenishInstallation(site,90),true);assert.equal(site.strength,33);assert.equal(site.campaignReserve.personnel,17);assert.equal(site.vehicleInventory.tank,1);assert.equal(site.armorCapacity,12);
 site.strength=0;site.vehicleInventory.tank=0;w.replenishInstallation(site,180);assert.equal(site.strength,17);assert.equal(site.campaignReserve.personnel,0);assert.equal(site.campaignReserve.vehicles.tank,0);
 site.strength=0;site.vehicleInventory.tank=0;assert.equal(w.replenishInstallation(site,270),false);assert.equal(site.strength,0);
 site.campaignReserve={personnel:40,vehicles:{tank:1},armor:12};site.barracksDown=site.fuelDown=true;w.replenishInstallation(site,360);assert.equal(site.strength,0);assert.equal(site.campaignReserve.personnel,0);assert.equal(Object.keys(site.campaignReserve.vehicles).length,0);
});
test('distant plots discover procedural trees and defer to streaming before construction approval',()=>{
 const c=loadEngine(),reference=new c.ATSWorld('distant-plot');reference.ensure(8500,8500,500);
 const tree=[...reference.objects.values()].find(o=>o.type==='tree'&&o.x>8000&&o.y>8000&&!reference.waterBlocked(o.x,o.y,24));assert.ok(tree);
 const w=new c.ATSWorld('distant-plot'),before=w.chunks.size,plot=w.settlementPlot(tree.x,tree.y,24,{settlementId:'remote'});
 assert.ok(plot.trees.some(o=>o.id===tree.id),'tree is discovered before a far home is approved');assert.ok(w.chunks.size-before<=9,'plot generation stays local');
 const streamed=new c.ATSWorld('distant-plot');streamed._streaming=true;const pending=streamed.settlementPlot(tree.x,tree.y,24,{settlementId:'remote'});
 assert.equal(pending.valid,false);assert.equal(pending.pending,true);assert.equal(streamed.chunks.size,0,'a colony tick does not synchronously generate chunks');
 for(let i=0;i<100&&!streamed.boundsReady(tree.x-24,tree.y-24,tree.x+24,tree.y+24);i++)streamed.stream(tree.x,tree.y,0,{maxSteps:12,budgetMs:100});
 const ready=streamed.settlementPlot(tree.x,tree.y,24,{settlementId:'remote'});assert.equal(ready.pending,undefined);assert.ok(ready.trees.some(o=>o.id===tree.id));
});
function renderer(){const gradient={addColorStop(){}},context=()=>new Proxy({globalAlpha:1,createLinearGradient:()=>gradient,createRadialGradient:()=>gradient},{get(o,k){return k in o?o[k]:(...args)=>{for(const a of args)if(typeof a==='number')assert.ok(Number.isFinite(a),String(k))}},set(o,k,v){o[k]=v;return true}}),canvas=()=>({getBoundingClientRect:()=>({width:1440,height:900}),getContext:context});const c=vm.createContext({console,Math,Map,Set,devicePixelRatio:1,innerWidth:1440,innerHeight:900,document:{createElement:canvas}});c.window=c;for(const name of ['world','render','render-details'])vm.runInContext(fs.readFileSync(path.join(__dirname,'..',name+'.js'),'utf8'),c);const r=new c.ATSRenderer(canvas());r.actorSpriteBudget=100;return r;}
test('living village and damaged defensive geometry stay finite and reuse bounded sprites',()=>{
 const r=renderer(),c=r.ctx;r.time=10;for(const level of [1,3,5,7,10])r.drawSettlement(c,{name:'Moonroot',level,radius:550,population:500,food:500,attack:level===10});
 for(let i=0;i<80;i++){r.actorSpriteBudget=100;r.drawHut(c,{id:'home-'+i,variant:i%4,stage:4,hp:[100,50,20,0][i%4],maxHp:100});for(const kind of ['garden','cooking','storage','workShelter','lookout'])r.drawSettlementProp(c,{kind,hp:100,variant:i%3});r.drawObject(c,{id:'cover-'+i,team:i%2?'ape':'human',kind:i%3?'basic':'heavy',type:'humanBarricade',fortification:true,hp:[300,160,50,0][i%4],maxHp:300,w:74,h:16});}
 for(const stage of [0,1,2,3])r.drawConstruction(c,{kind:'hut',stage,progress:stage/4});r.drawObject(c,{type:'tree',dead:true,clearedBy:'home'});
 assert.ok(r.hutSprites.size<=20);assert.ok(r.barrierSprites.size<=72);assert.ok(r.villageSprites.size<=48);assert.ok(r.lodgeSprites.size<=10);
});
