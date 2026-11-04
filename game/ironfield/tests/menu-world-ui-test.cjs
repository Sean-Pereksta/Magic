'use strict';
// Actual app handlers, engine, data, and HTML; only browser layout/drawing are mocked.
const assert=require('node:assert/strict');
const path=require('node:path');
const {bootUI}=require('./command-ui-test.cjs');
const ROOT=process.argv[2]||path.resolve(__dirname,'..');
const results=[];
function test(name,fn){try{const result=fn();results.push({name,passed:true,result});console.log('PASS '+name);}catch(error){results.push({name,passed:false,error:error.message});console.error('FAIL '+name+': '+error.stack);process.exitCode=1;}}
function change(ui,id,value){const el=ui.document.getElementById(id);el.value=value;el.dispatch('change');return el;}
function button(ui,id){return ui.document.getElementById(id);}
function gallery(ui,id){const el=ui.document.querySelector('[data-menu-map="'+id+'"]');assert(el,'Missing battlefield card '+id);button(ui,'menuMapGrid').dispatch('click',{target:el});}
function load(ui,config){
  ui.storage.set('ironfield.scenarios.v1',JSON.stringify([{format:'ironfield-scenario',version:2,name:config.name||'Large field audit',config}]));
  button(ui,ui.window.IRONFIELD.menuOpen?'menuSaved':'scenariosButton').click();
  const list=button(ui,'savedScenarios'),link=list.querySelector('[data-load-scenario="0"]');assert(link);list.dispatch('click',{target:link});
}
function largeConfig(){return{name:'Colossal saved layout',map:'custom',seed:7137,customMap:{name:'Great field',width:16000,height:11000,biome:'grassland',regions:[{type:'forest',x:13100,y:9100,w:900,h:700},{type:'hill',x:8000,y:4000,w:1400,h:900}]},armies:[{name:'West',doctrine:'steady',roster:{hoplite:32}},{name:'East',doctrine:'rapid',roster:{cavalry:16}}],formations:[{team:0,type:'hoplite',count:32,x:11000,y:7000,facing:0,formation:'shieldwall'},{team:1,type:'cavalry',count:16,x:14100,y:7000,facing:Math.PI,formation:'line'}]};}
function positions(engine){return engine.units.map(u=>[u.id,u.type,u.x,u.y]);}

test('Startup menu blocks battlefield commands and performs no hidden battle rendering',()=>{
  const ui=bootUI(ROOT,{menu:true}),e=ui.engine,r=ui.window.IRONFIELD.renderer;
  assert.equal(ui.window.IRONFIELD.menuOpen,true);assert.equal(button(ui,'mainMenu').hidden,false);assert.equal(button(ui,'app').inert,true);
  ui.key(' ');ui.point(1600,2200);ui.key('c');for(let t=100;t<5000;t+=100)ui.frame(t);
  assert.equal(e.phase,'deployment');assert.equal(e.time,0);assert.equal(ui.calls.length,0);assert.equal(r.renderCalls||0,0);assert.equal(r.minimapCalls||0,0);
  assert.equal(button(ui,'menuResume').hidden,true);
  return{hiddenSimulationSeconds:0,hiddenCanvasRedraws:0,blockedCommands:ui.calls.length};
});
test('Menu offers every authored battlefield and prepares the chosen larger scenario',()=>{
  const ui=bootUI(ROOT,{menu:true}),D=ui.window.GameData,maps=D.MAPS.filter(m=>m.id!=='custom');
  assert.equal(ui.document.querySelectorAll('[data-menu-map]').length,maps.length);assert.equal(maps.length,22);
  const biggest=maps.find(m=>m.width===16000);assert(biggest);gallery(ui,biggest.id);
  assert.equal(button(ui,'menuMapTitle').textContent,biggest.name);assert.match(button(ui,'menuMapDimensions').textContent,/6\.25/);
  button(ui,'menuDeploy').click();assert.equal(ui.window.IRONFIELD.menuOpen,false);assert.equal(ui.engine.map.id,biggest.id);assert.equal(ui.engine.width,16000);assert.equal(ui.engine.height,11000);assert.equal(ui.engine.phase,'deployment');
  return{authoredBattlefields:maps.length,chosenMap:biggest.id,width:ui.engine.width,height:ui.engine.height};
});
test('Quick Battle starts the selected scenario and menu Resume preserves the live battle and orders',()=>{
  const ui=bootUI(ROOT,{menu:true});button(ui,'menuQuick').click();assert.equal(ui.engine.phase,'running');
  const e=ui.engine,r=e.regiments.find(r=>r.team===0&&r.type==='swordsman');e.command([r.id],'move',2600,2200);for(let i=0;i<30;i++)e.step(1/30);
  const before=positions(e),time=e.time;button(ui,'menuButton').click();assert.equal(e.phase,'paused');assert.equal(button(ui,'menuResume').hidden,false);
  ui.key(' ');ui.key('c');ui.point(4000,2200,{button:2});ui.frame(100);ui.frame(60000);assert.deepEqual(positions(e),before);assert.equal(e.time,time);assert.equal(r.order,'move');
  button(ui,'menuResume').click();assert.equal(ui.engine,e);assert.equal(e.phase,'running');assert.equal(r.order,'move');assert.deepEqual(positions(e),before);
  ui.frame(60016);assert(e.time-time<=1/30+1e-9,'Menu time was added to the simulation backlog');
  return{sameBattle:true,preservedOrder:r.order,simulatedAfterReturn:e.time-time};
});
test('Returning from the menu keeps an intentionally paused battle paused',()=>{
  const ui=bootUI(ROOT);ui.key(' ');ui.key(' ');assert.equal(ui.engine.phase,'paused');const e=ui.engine;
  button(ui,'menuButton').click();button(ui,'menuSettings').click();assert(button(ui,'settingsDialog').open);
  ui.document.querySelector('[data-close="settingsDialog"]').click();assert.equal(button(ui,'mainMenu').hidden,false);
  ui.key('Escape');assert.equal(ui.window.IRONFIELD.menuOpen,false);assert.equal(ui.engine,e);assert.equal(e.phase,'paused');
  return{phase:e.phase,settingsReturnedToMenu:true};
});
test('Custom encounter starts genuinely empty and all four world sizes work through the editor',()=>{
  const ui=bootUI(ROOT,{menu:true});button(ui,'menuCustom').click();assert.equal(ui.engine.units.length,0);assert.equal(ui.engine.map.regions.length,0);
  const observed=[];for(const size of ui.window.GameData.WORLD_SIZES){change(ui,'customWorldSize',size.id);assert.equal(ui.engine.width,size.width);assert.equal(ui.engine.height,size.height);assert.equal(ui.engine.units.length,0);assert.equal(ui.window.IRONFIELD.config.customMap.width,size.width);observed.push([size.width,size.height]);}
  assert.equal(observed.length,4);return{sizes:observed,empty:true};
});
test('Large custom terrain, deployments and battle plans survive a real save and menu reload',()=>{
  const ui=bootUI(ROOT,{menu:true});load(ui,largeConfig());assert.equal(ui.engine.width,16000);assert.equal(ui.engine.height,11000);
  assert.equal(ui.engine.map.regions[0].x,13100);assert.equal(ui.engine.map.regions[0].w,900);assert(ui.engine.units.every(u=>u.x>10000&&u.y>6500),'Large deployment was clamped to the old field');
  const before=positions(ui.engine);button(ui,'scenarioName').value='Large deployment preserved';button(ui,'saveScenario').click();
  const snapshot=JSON.parse(ui.storage.get('ironfield.scenarios.v1'))[0];assert.equal(snapshot.config.customMap.width,16000);assert.equal(snapshot.config.customMap.height,11000);
  assert.equal(snapshot.config.armies[0].doctrine,'steady');assert.equal(snapshot.config.armies[1].doctrine,'rapid');
  button(ui,'menuButton').click();button(ui,'menuSaved').click();const list=button(ui,'savedScenarios');list.dispatch('click',{target:list.querySelector('[data-load-scenario="0"]')});
  assert.equal(ui.window.IRONFIELD.menuOpen,false);assert.deepEqual(positions(ui.engine),before);assert.equal(ui.engine.map.regions[0].y,9100);
  return{width:ui.engine.width,height:ui.engine.height,soldiers:ui.engine.units.length,exactCoordinatesPreserved:true};
});
test('Growing a custom world preserves coordinates while unsafe shrinking keeps the entire layout',()=>{
  const ui=bootUI(ROOT);button(ui,'newEncounter').click();
  button(ui,'placeTroop').value='swordsman';button(ui,'placeCount').value='32';button(ui,'placeFormationButton').click();ui.point(1800,2200);const before=positions(ui.engine);
  const D=ui.window.GameData,colossal=D.WORLD_SIZES.find(s=>s.width===16000),standard=D.WORLD_SIZES.find(s=>s.width===6400);
  change(ui,'customWorldSize',colossal.id);assert.deepEqual(positions(ui.engine),before);
  load(ui,largeConfig());const e=ui.engine,large=positions(e),terrain=JSON.stringify(e.map.regions);change(ui,'customWorldSize',standard.id);
  assert.equal(ui.engine,e);assert.deepEqual(positions(e),large);assert.equal(JSON.stringify(e.map.regions),terrain);assert.equal(e.width,16000);assert.equal(button(ui,'customWorldSize').value,colossal.id);assert.match(ui.toast().at(-1),/smaller field/i);
  return{growthPreservesCoordinates:true,unsafeShrinkRejected:true};
});
test('Corpse persistence and animation settings persist and follow the renderer across battles',()=>{
  const ui=bootUI(ROOT,{menu:true});button(ui,'menuSettings').click();change(ui,'corpseMode','battle');change(ui,'animations','reduced');
  assert.equal(ui.window.IRONFIELD.renderer.options.corpseMode,'battle');assert.equal(ui.window.IRONFIELD.renderer.options.animations,'reduced');
  const saved=JSON.parse(ui.storage.get('ironfield.preferences.v1'));assert.equal(saved.corpseMode,'battle');assert.equal(saved.animations,'reduced');
  ui.document.querySelector('[data-close="settingsDialog"]').click();button(ui,'menuDeploy').click();assert.equal(ui.window.IRONFIELD.renderer.options.corpseMode,'battle');
  const again=bootUI(ROOT,{menu:true,preferences:saved});assert.equal(button(again,'corpseMode').value,'battle');assert.equal(button(again,'animations').value,'reduced');assert.equal(again.window.IRONFIELD.renderer.options.corpseMode,'battle');
  return{mode:saved.corpseMode,animations:saved.animations,persistsAcrossBoot:true};
});
test('Battlefield filters expose the additional large maps and fortified assaults',()=>{
  const ui=bootUI(ROOT,{menu:true}),D=ui.window.GameData;ui.document.querySelector('[data-map-filter="large"]').click();
  const visible=()=>ui.document.querySelectorAll('[data-menu-map]').filter(e=>!e.hidden).map(e=>D.MAPS.find(m=>m.id===e.dataset.menuMap));
  const large=visible();assert.equal(large.length,12);assert(large.every(m=>m.width>6400));
  ui.document.querySelector('[data-map-filter="siege"]').click();const siege=visible();assert(siege.length>=6);assert(siege.every(m=>m.regions.some(r=>r.type==='wall'||r.type==='gate')));
  ui.document.querySelector('[data-map-filter="all"]').click();assert.equal(visible().length,22);
  return{additionalLargeMaps:large.length,fortifiedAssaultMaps:siege.length};
});
test('Rapid Encirclement exposes single-versus-group scope and manual Hold removes the automatic status',()=>{
  const ui=bootUI(ROOT);const observed=[];
  for(const count of [1,2]){
    const cfg={name:'Scope feedback',map:'custom',seed:9911,customMap:{width:6400,height:4400,biome:'grassland',regions:[]},
      armies:[{doctrine:'rapid',roster:{swordsman:64,lancer:48}},{doctrine:'defensive',roster:{guardian:64*count}}],
      formations:[{team:0,type:'swordsman',count:64,x:1900,y:2200,facing:0},{team:0,type:'lancer',count:24,x:1800,y:1550,facing:0},{team:0,type:'lancer',count:24,x:1800,y:2850,facing:0},
        {team:1,type:'guardian',count:64,x:3400,y:count===1?2200:2020,facing:Math.PI}]};
    if(count===2)cfg.formations.push({team:1,type:'guardian',count:64,x:3400,y:2380,facing:Math.PI});
    load(ui,cfg);ui.engine.command(ui.engine.regiments.filter(r=>r.team===1).map(r=>r.id),'hold');ui.key(' ');
    for(let i=0;i<20;i++)ui.engine.step(1/30);
    const wing=ui.engine.regiments.find(r=>r.team===0&&r.type==='lancer');ui.select(wing);
    const status=button(ui,'selectedTactics').textContent;assert.match(status,count===1?/Single formation/:/Enemy position \(2 squads\)/);assert.equal(wing._tactic.targetCount,count);
    ui.key('h');assert.equal(wing.order,'hold');assert.doesNotMatch(button(ui,'selectedTactics').textContent,/Enemy position|Single formation/);
    observed.push({count,status});
  }
  return{scopes:observed,manualOrderWins:true};
});
test('A delayed battle report waits for menu Resume and Fight Again enters the actual battlefield',()=>{
  const ui=bootUI(ROOT);const ended=ui.engine;ended.phase='ended';ui.frame(400);
  button(ui,'menuButton').click();ui.flushTimers();assert.equal(button(ui,'resultsDialog').open,false,'The finished battle opened a report over the main menu');
  button(ui,'menuResume').click();ui.frame(1000);ui.flushTimers();assert.equal(button(ui,'resultsDialog').open,true,'The deferred report disappeared after Resume');
  button(ui,'rematchButton').click();assert.equal(ui.window.IRONFIELD.menuOpen,false);assert.equal(ui.engine.phase,'running');assert.notEqual(ui.engine,ended);assert.equal(button(ui,'mainMenu').hidden,true);
  return{reportDeferred:true,rematchRunning:true};
});
test('A captured touch cannot place troops or finish an order after returning from the menu',()=>{
  const ui=bootUI(ROOT);button(ui,'newEncounter').click();button(ui,'placeTroop').value='swordsman';button(ui,'placeCount').value='32';button(ui,'placeFormationButton').click();
  const canvas=button(ui,'battlefield');canvas.dispatch('pointerdown',{clientX:1800,clientY:2200,pointerType:'touch',pointerId:10});
  button(ui,'menuButton').click();button(ui,'menuResume').click();canvas.dispatch('pointerup',{clientX:1800,clientY:2200,pointerType:'touch',pointerId:10});
  assert.equal(ui.engine.units.length,0,'A stale pre-menu touch placed a formation');
  ui.point(1800,2200);assert.equal(ui.engine.units.length,32,'Cancelling the old gesture disabled new input');return{stalePlacementCancelled:true,freshInputWorks:true};
});
test('Changing to a larger authored battlefield scales formation centers without stretching their ranks',()=>{
  const ui=bootUI(ROOT);const cfg=largeConfig();cfg.customMap={width:6400,height:4400,biome:'grassland',regions:[]};cfg.formations[0].x=1500;cfg.formations[0].y=1800;cfg.formations[1].x=4900;cfg.formations[1].y=2600;load(ui,cfg);
  const before=ui.engine.regiments.map(r=>({x:r.x,y:r.y,facing:r.facing,offsets:r.unitIds.map(id=>({x:ui.engine.units[id].x-r.x,y:ui.engine.units[id].y-r.y}))}));
  change(ui,'mapSelect','longgrass');assert.equal(ui.engine.width,16000);assert.equal(ui.engine.height,11000);
  for(let i=0;i<before.length;i++){const a=before[i],r=ui.engine.regiments[i];assert(Math.abs(r.x-a.x*2.5)<1e-6);assert(Math.abs(r.y-a.y*2.5)<1e-6);assert.equal(r.facing,a.facing);for(let j=0;j<r.unitIds.length;j++){const u=ui.engine.units[r.unitIds[j]];assert(Math.abs(u.x-r.x-a.offsets[j].x)<1e-6);assert(Math.abs(u.y-r.y-a.offsets[j].y)<1e-6);}}
  return{relativeDeploymentPreserved:true,bodySpacingUnchanged:true,world:[ui.engine.width,ui.engine.height]};
});
console.log(JSON.stringify({passed:results.filter(r=>r.passed).length,failed:results.filter(r=>!r.passed).length,tests:results},null,2));
