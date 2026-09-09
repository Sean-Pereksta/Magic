import assert from 'node:assert/strict';
import test from 'node:test';
import {createSimulation} from './simulation-harness.mjs';

const visualPatches=['galaxy-visual-model.js','galaxy-visual-art.js','galaxy-visual-renderer.js'];
const json=(h,source)=>JSON.parse(h.run(`JSON.stringify(${source})`));
function scene(){
  const h=createSimulation();
  h.run(`state.ships=[];state.fleets=[];state.battles=[];state.deepSpaceBattles=[];state.deepSpaceBases=[];
    state.camera={x:2000,y:2000,zoom:1.2};state.planets.forEach((p,i)=>{p.x=1700+(i%8)*200;p.y=1800+Math.floor(i/8)*180;p.underAttack=false});
    state.selected=state.planets[0];state.planets[0].owner=0;draw();`);
  return h;
}
test('rendering is observational: serialized gameplay and RNG match the unmodified simulation',()=>{
  const a=createSimulation(),b=createSimulation({excludePatches:visualPatches});
  for(let i=0;i<50;i++){
    a.run('simulate(.12);draw();draw()');b.run('simulate(.12)');
  }
  a.run('saveGame(false)');b.run('saveGame(false)');
  assert.equal(a.run('state.rngState'),b.run('state.rngState'));
  assert.equal(a.run('localStorage.getItem(SAVE_KEY)'),b.run('localStorage.getItem(SAVE_KEY)'));
  assert.equal(a.run('JSON.stringify(state).includes("stxGV")'),false);
});
test('all planet families are deterministic and ownership does not recolor their geology',()=>{
  const h=scene(),before=json(h,'state.planets.map(stxGVFamily)');
  assert.equal(new Set(before).size,11);
  h.run('state.planets.forEach(p=>p.owner=(p.owner+1)%7)');
  assert.deepEqual(json(h,'state.planets.map(stxGVFamily)'),before);
  assert.equal(h.run('stxGVFamily({biome:"Ocean"})'),'Ocean');
});
test('drawing orbital facilities cannot alter the physical docking clock or ship coordinates',()=>{
  const a=createSimulation(),b=createSimulation({excludePatches:visualPatches});
  for(const h of [a,b]){
    h.context.performance.now=()=>120000;
    h.run(`state.ships=[];state.p=owned(0)[0];state.destination=owned(1)[0];
      state.destination.orbitalFacilities=[{id:'dock',kind:'trade',tier:2,hp:100,maxHp:100,modules:['exchange'],orbit:1,angle:.2}];
      createShip('freighter',state.p,state.destination,0,{commercial:true,crossBorder:true,cargo:{iron:3},stationTargetId:'dock'});
      state.ships[0].progress=.8;state.ships[0].speed=1;`);
  }
  a.run('draw();tickShips(.12);draw()');b.run('tickShips(.12)');
  assert.deepEqual(json(a,'state.ships'),json(b,'state.ships'));
  assert.deepEqual(json(a,'stxOLFacilityPosition(state.destination,state.destination.orbitalFacilities[0])'),
    json(b,'stxOLFacilityPosition(state.destination,state.destination.orbitalFacilities[0])'));
});
test('a contested blockade renders fleet combat without inventing an invasion or mutating strength',()=>{
  const h=scene();h.run(`state.planets[1].owner=1;state.fleets=[
    {id:'raider',owner:0,strength:90,deepSpaceOperationId:'raid',location:'p0'},
    {id:'defender',owner:1,strength:40,maxServiceStrength:50,location:'p1'}];
    state.deepSpaceOperations=[{id:'raid',fleetId:'raider',owner:0,targetType:'planet',targetId:'p1',kind:'blockade',x:state.planets[1].x,y:state.planets[1].y,strength:100,nextControlCheckAt:5,active:true,radius:520}];
    state.beforeCombat=JSON.stringify(state.fleets);draw()`);
  assert.equal(h.run('stxGV.index.battles[0].invasion'),false);
  assert.equal(h.run('stxGV.index.battles[0].b.attackerStrength'),90);
  assert.equal(h.run('JSON.stringify(state.fleets)'),h.run('state.beforeCombat'));
  assert.equal(h.run('state.battles.length'),0);
});
test('planet development, fortress and specialist roles reflect existing infrastructure',()=>{
  const h=scene();
  const tiers=json(h,`[stxGVDevelopment({owner:null}),stxGVDevelopment({owner:0,infra:{city:1}}),
    stxGVDevelopment({owner:0,infra:{city:4,factory:3}}),stxGVDevelopment({owner:0,infra:{city:8,factory:7}}),
    stxGVDevelopment({owner:0,home:true})]`);
  assert.deepEqual(tiers,[0,1,2,3,4]);
  assert.equal(h.run('stxGVRoles({owner:0,infra:{defense:9,city:2}})[0]'),'fortress');
  assert.equal(h.run('stxGVRoles({owner:0,infra:{factory:2},stxEconomicFocus:"components"})[0]'),'components');
});
test('patrol to doomstack has increasing silhouette count and footprint at every zoom',()=>{
  const h=scene();
  for(const zoom of [.3,.7,1.2]){
    const out=json(h,`[5,20,60,150,350,500].map(power=>{state.camera.zoom=${zoom};
      stxGV.budget={...STX_GV_LIMITS};return stxGVFormation({x:600,y:400},power,0,'#ffffff')})`);
    for(let i=1;i<out.length;i++){
      assert.ok(out[i].count>out[i-1].count,`${zoom}: count ${i}`);
      assert.ok(out[i].span>out[i-1].span,`${zoom}: span ${i}`);
    }
  }
});
test('deep-space cargo routes use real coordinates, and a destroyed station breaks observed shipping',()=>{
  const h=scene();
  h.run(`state.deepSpaceBases=[{id:'base',x:2300,y:2000,owner:0,name:'Logistics hub',type:'logistics',tier:2,status:'operational'}];
    state.ships=[{id:'cargo',type:'freighter',owner:0,from:state.planets[0].id,to:state.planets[0].id,
      startX:state.planets[0].x,startY:state.planets[0].y,targetX:2300,targetY:2000,x:2100,y:1950,
      deepBaseId:'base',stxDeepTransit:true,cargo:{components:20}}];stxGV.index=stxGVIndex();`);
  assert.deepEqual(json(h,'stxGVRoutes().map(r=>[r.a.id,r.b.id,r.kind])'),[['p0','base','logistics']]);
  h.run('state.deepSpaceBases[0].status="wreck";state.ships=[];stxGV.index=stxGVIndex()');
  assert.equal(h.run('stxGVRoutes()[0].blocked'),'Station destroyed');
});
test('trade, military and logistics have separate routes and embargoes suppress active shipping style',()=>{
  const h=scene();
  h.run(`state.planets[1].owner=1;state.ships=[
    {id:'a',from:'p0',to:'p1',type:'freighter',commercial:true,owner:0,cargo:{iron:20},x:1900,y:1800},
    {id:'b',from:'p0',to:'p1',type:'fleet',owner:0,strength:500,x:1900,y:1800},
    {id:'c',from:'p0',to:'p1',type:'supply',owner:0,cargo:{equipment:10},x:1900,y:1800}];
    state.rivalDiplomacy.agreements=[{kind:'embargo',by:0,with:1,active:true,expiresAt:100}];stxGV.index=stxGVIndex();`);
  assert.deepEqual(json(h,'stxGVRoutes().map(r=>[r.kind,stxGVRouteBlocked(r)])'),
    [['trade','Embargo'],['military',''],['logistics','Embargo']]);
});
test('global combat budget is bounded and selected engagements are allocated first',()=>{
  const h=scene();
  h.run(`state.camera={x:2100,y:2000,zoom:1.2};state.selected=state.planets[9];
    state.battles=state.planets.slice(0,10).map((p,i)=>({id:'b'+i,planetId:p.id,attacker:0,defender:1,
      attackerStrength:500,defenderStrength:500,attackerInitial:500,defenderInitial:500,attackerFleetIds:[],defenderFleetIds:[]}));draw();`);
  const stats=json(h,'SpaceTyrantsVisuals.diagnostics()');
  assert.ok(stats.combat<=240);
  assert.ok(stats.battles.b9>=70,'selected battle gets its full scene before array predecessors');
  for(const n of Object.values(stats.battles))assert.ok(n<=100);
});
test('skirmishes use fewer combat entities than major battles',()=>{
  const h=scene();
  assert.deepEqual(json(h,'[20,150,600,1500].map(p=>stxGVBattleBudget(p,true,1.2))'),[18,44,78,100]);
});
test('observation emits capture, invasion, reinforcement, war and station-loss pulses once',()=>{
  const h=scene();
  h.run(`state.planets[0].owner=1;state.planets[1].owner=null;
    state.deepSpaceBases=[{id:'base',owner:0,name:'Station',x:2000,y:2000,status:'operational',type:'military',tier:2}];
    state.simTime++;draw();stxGV.pulses.forEach(f=>f.until=0);
    state.planets[0].owner=0;state.planets[1].owner=0;state.deepSpaceBases[0].status='wreck';
    state.battles=[{id:'battle',planetId:'p0',attacker:0,defender:1,attackerStrength:50,defenderStrength:50,
      attackerInitial:50,defenderInitial:50,attackerFleetIds:[],defenderFleetIds:[]}];
    state.wars=[{id:'war',a:0,b:1,active:true}];state.simTime++;draw();`);
  const kinds=json(h,'stxGV.pulses.filter(f=>f.until>stxGV.time).map(f=>f.kind)');
  for(const kind of ['capture','colony','station-destroyed','invasion','war'])assert.ok(kinds.includes(kind),kind);
  h.run('state.battles[0].attackerInitial+=40;state.battles[0].attackerStrength+=40;state.simTime++;draw();draw()');
  assert.equal(h.run('stxGV.pulses.filter(f=>f.until>stxGV.time&&f.kind==="reinforcement").length'),1);
});
test('loading seeds event snapshots without replaying captures or active shipping',()=>{
  const h=scene();h.run(`state.simTime=200;state.planets=state.planets.map(p=>({...p}));
    state.ships=[{id:'t',type:'freighter',owner:0,commercial:true,from:'p0',to:'p1',x:1900,y:1800,cargo:{iron:5}}];draw()`);
  assert.equal(h.run('SpaceTyrantsVisuals.diagnostics().pulseCount'),0);
});
test('sprite creation and cache size stay bounded under a large visible galaxy',()=>{
  const h=scene();
  h.run('state.camera={x:2400,y:2200,zoom:.25};for(let i=0;i<110;i++)draw()');
  assert.ok(h.run('stxGV.sprites.size')<=96);
  assert.equal(h.run('stxGV.stats.spriteBuilds||0'),0,'warm overview does not churn its cache');
});
test('no hidden enemy fleet is revealed by formations or routes',()=>{
  const h=scene();
  h.run(`state.planets.forEach(p=>p.owner=1);state.planets[0].owner=0;state.planets[0].x=0;state.planets[0].y=0;
    state.fleets=[{id:'hidden',owner:1,location:'p1',strength:800,name:'Hidden armada',destroyed:false}];
    state.ships=[{id:'secret',fleetId:'hidden',type:'fleet',owner:1,from:'p1',to:'p2',x:2000,y:2000,strength:800}];
    state.stxNetworkOverlay='military';draw()`);
  assert.equal(h.run('stxGV.hits.some(h=>h.kind==="fleet"||h.kind==="route")'),false);
});
test('crossing routes can be hit even with both endpoints offscreen; drag-independent hit priority prefers objects',()=>{
  const h=scene();
  assert.equal(h.run('stxGVRouteOnscreen({x:-100,y:400},{x:1400,y:400})'),true);
  h.run(`stxGV.hits=[{kind:'route',id:'r',a:{x:-100,y:400},b:{x:1400,y:400},x:650,y:400,r:6},
    {kind:'fleet',id:'f',x:600,y:400,r:20}]`);
  assert.equal(h.run('stxGVFindHit(600,400).id'),'f');
  assert.equal(h.run('stxGVFindHit(900,400).id'),'r');
});
test('pause and reduced motion retain static invasion state and stop visual time',()=>{
  const h=scene();let now=1000;h.context.performance.now=()=>now;
  h.run('stxGV.reduced=true;state.speed=0;draw()');const before=h.run('stxGV.time');
  now+=100;h.run('draw()');assert.equal(h.run('stxGV.time'),before);
  const first=json(h,'stxGVBattlePoint({x:0,y:0},1,10,1,40,1,.7)');
  assert.deepEqual(json(h,'stxGVBattlePoint({x:0,y:0},1,10,1,40,100,.7)'),first);
});
test('pulse pool and route history remain bounded under sustained event pressure',()=>{
  const h=scene();
  h.run(`for(let i=0;i<1000;i++)stxGVPulse('war',{x:2000+i,y:2000},'#ff0000')`);
  assert.equal(h.run('stxGV.pulses.length'),48);
  h.run(`state.ships=Array.from({length:800},(_,i)=>({id:'c'+i,type:'freighter',owner:0,commercial:true,stxDeepTransit:true,
    startX:2000,startY:2000,targetX:2100+i,targetY:2000,x:2000,y:2000,cargo:{iron:2}}));stxGV.index=stxGVIndex();stxGVRoutes()`);
  assert.ok(h.run('stxGV.routeHistory.size')<=320);
});
