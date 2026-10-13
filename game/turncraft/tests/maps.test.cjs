const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');

// Run the real game integration with inert presentation and no Firebase writes.
function simulation(savedMap='classic'){
  const noop=()=>{},elements=new Map();
  const canvas=new Proxy({measureText:()=>({width:10}),createLinearGradient:()=>({addColorStop:noop}),createRadialGradient:()=>({addColorStop:noop})},{get:(o,k)=>o[k]||noop});
  function element(){return {style:{setProperty:noop},dataset:{},classList:{add:noop,remove:noop,toggle:noop,contains:()=>false},appendChild:noop,append:noop,remove:noop,setAttribute:noop,addEventListener:noop,querySelector:()=>null,querySelectorAll:()=>[],getContext:()=>canvas,getBoundingClientRect:()=>({left:0,top:700,width:1280,height:100,right:1280,bottom:800}),textContent:'',innerHTML:'',scrollTop:0,scrollLeft:0};}
  const document={getElementById:id=>{if(!elements.has(id))elements.set(id,element());return elements.get(id)},createElement:element,querySelector:()=>null,querySelectorAll:()=>[],addEventListener:noop,body:element(),documentElement:element()};
  const sandbox={console,document,location:{search:'',href:'http://turncraft.test/game/turncraft.html'},URL,URLSearchParams,localStorage:{getItem:key=>key==='csf.map'?savedMap:null,setItem:noop},innerWidth:1280,innerHeight:900,devicePixelRatio:1,performance:{now:()=>0},setTimeout:()=>0,clearTimeout:noop,setInterval:()=>0,requestAnimationFrame:noop,matchMedia:()=>({matches:false,addEventListener:noop}),HTMLInputElement:class{},HTMLSelectElement:class{},Image:class{},initializeApp:()=>({}),getAuth:()=>({}),getFirestore:()=>({}),navigator:{},screen:{},addEventListener:noop};
  sandbox.window=sandbox;
  const context=vm.createContext(sandbox);
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../maps.js'),'utf8'),context,{timeout:30000});
  const html=fs.readFileSync(path.join(__dirname,'../../turncraft.html'),'utf8');
  const code=html.match(/<script type="module">([\s\S]*?)<\/script>/)[1].replace(/^import .*;$/gm,'').replace('\nboot();','\nrefreshUI=()=>{};renderMapChoices=()=>{};showCommandMarker=()=>{};pushBattleAlert=()=>{};globalThis.testEvaluate=source=>eval(source);');
  vm.runInContext(code,context,{timeout:30000,filename:'turncraft.html'});
  return code=>vm.runInContext(`testEvaluate(${JSON.stringify(code)})`,context,{timeout:30000});
}
const mapIds=['classic','ember-rift','frostline-basin','verdant-divide','shattered-dunes','orbital-foundry'];

test('map selection loads saved valid choices and safely falls back from unknown choices',()=>{
  for(const id of mapIds)assert.equal(simulation(id)('chosenMapId'),id,id);
  const run=simulation('missing-map');
  assert.equal(run('chosenMapId'),'classic');
  assert.equal(run("mapDefinition('missing-map').id"),'classic');
  assert.equal(run("mapLayout('missing-map',1234).id"),'classic');
});

for(const id of mapIds)test(`${id}: actual match uses its starts, expansions, resources and objectives`,()=>{
  const run=simulation(id);
  const result=run(`(()=>{chosenGameMode='conquest';createGame([{name:'One',factionId:'frontier',ai:false},{name:'Two',factionId:'ascendant',ai:false},{name:'Three',factionId:'brood',ai:false}]);const layout=mapLayout(),key=p=>p.x+','+p.y,bases=entities.filter(e=>isBase(e)),nodes=resourceNodes.filter(n=>n.amount>0),walkable=p=>inBounds(p.x,p.y)&&!groundBlocked(p.x,p.y);const seen=new Set([key(startPositions()[0])]),queue=[startPositions()[0]];for(let i=0;i<queue.length;i++)for(const [dx,dy]of ORTHO){const p={x:queue[i].x+dx,y:queue[i].y+dy},k=key(p);if(walkable(p)&&!seen.has(k)){seen.add(k);queue.push(p)}}return {map:chosenMapId,bases:bases.map(b=>key(b)),starts:layout.starts.map(key),expansions:expansionPositions().map(key),expectedExpansions:layout.expansions.map(key),conquest:modeState.conquest.map(key),expectedConquest:layout.conquestSites.map(key),relic:key(modeState.relic),expectedRelic:key(layout.relic),nodeCount:nodes.length,uniqueNodes:new Set(nodes.map(key)).size,resourceBaseOverlap:nodes.some(n=>bases.some(b=>key(b)===key(n))),workersOpen:playerUnits(0).filter(isWorker).every(walkable),connected:[...layout.starts,...layout.expansions,...layout.conquestSites,layout.relic].every(p=>seen.has(key(p))),resourcesReachable:nodes.every(n=>n.type==='mineral'?seen.has(key(n)):DIR8.some(([dx,dy])=>seen.has((n.x+dx)+','+(n.y+dy)))),allBasesConnectGas:bases.every(b=>nodes.some(n=>n.type==='gas'&&resourceConnected(b.faction,n))),neutralDistinct:new Set(entities.filter(e=>e.faction<0).map(key)).size===entities.filter(e=>e.faction<0).length}})()`);
  assert.equal(result.map,id);assert.deepEqual(Array.from(result.bases),Array.from(result.starts));assert.deepEqual(Array.from(result.expansions),Array.from(result.expectedExpansions));assert.deepEqual(Array.from(result.conquest),Array.from(result.expectedConquest));assert.equal(result.relic,result.expectedRelic);assert.ok(result.nodeCount>=50);assert.equal(result.uniqueNodes,result.nodeCount);assert.equal(result.resourceBaseOverlap,false);assert.equal(result.workersOpen,true);assert.equal(result.connected,true);assert.equal(result.resourcesReachable,true);assert.equal(result.allBasesConnectGas,true);assert.equal(result.neutralDistinct,true);
});

test('a map round-trip preserves terrain, layout, objectives and future snapshot identity',()=>{
  const run=simulation();
  for(const id of mapIds){
    const result=run(`(()=>{chosenMapId=${JSON.stringify(id)};chosenGameMode='relic';createGame([{name:'One',factionId:'frontier',ai:false},{name:'Two',factionId:'brood',ai:false}]);const snapshot=JSON.parse(JSON.stringify(serializeState())),before={terrain:encodeTerrain(),starts:JSON.stringify(startPositions()),expansions:JSON.stringify(expansionPositions()),relic:JSON.stringify(modeState.relic)};chosenMapId='classic';mapSeed=1;terrain=[];modeState=null;applyRemoteState(snapshot);return {savedMap:snapshot.chosenMapId,map:chosenMapId,terrain:encodeTerrain()===before.terrain,starts:JSON.stringify(startPositions())===before.starts,expansions:JSON.stringify(expansionPositions())===before.expansions,relic:JSON.stringify(modeState.relic)===before.relic,nextMap:serializeState().chosenMapId}})()`);
    assert.equal(result.savedMap,id);assert.equal(result.map,id);assert.equal(result.nextMap,id);assert.equal(result.terrain,true);assert.equal(result.starts,true);assert.equal(result.expansions,true);assert.equal(result.relic,true);
  }
});

test('old and invalid shared snapshots resolve to the classic layout before decoding',()=>{
  const run=simulation('orbital-foundry');
  for(const missing of [false,true]){
    const result=run(`(()=>{chosenMapId='classic';createGame([{name:'One',factionId:'frontier',ai:false},{name:'Two',factionId:'brood',ai:false}]);const snapshot=JSON.parse(JSON.stringify(serializeState()));${missing?'delete snapshot.chosenMapId':"snapshot.chosenMapId='missing-map'"};chosenMapId='orbital-foundry';applyRemoteState(snapshot);return {map:chosenMapId,starts:JSON.stringify(startPositions()),classic:JSON.stringify(mapLayout('classic',snapshot.mapSeed).starts)}})()`);
    assert.equal(result.map,'classic');assert.equal(result.starts,result.classic);
  }
});

test('map voting uses the majority, then earliest ready vote, then stable map order',()=>{
  const run=simulation();
  const cases=[
    {records:[],expected:'classic'},
    {records:[{mapVote:'missing-map',readyAtMs:1}],expected:'classic'},
    {records:[{mapVote:'ember-rift',readyAtMs:1},{mapVote:'orbital-foundry',readyAtMs:2},{mapVote:'orbital-foundry',readyAtMs:3}],expected:'orbital-foundry'},
    {records:[{mapVote:'frostline-basin',readyAtMs:7},{mapVote:'verdant-divide',readyAtMs:2}],expected:'verdant-divide'},
    {records:[{mapVote:'orbital-foundry',readyAtMs:2},{mapVote:'ember-rift',readyAtMs:2}],expected:'ember-rift'}
  ];
  for(const {records,expected}of cases){assert.equal(run(`resolveMapVote(${JSON.stringify(records)})`),expected);assert.equal(run(`resolveMapVote(${JSON.stringify(records.slice().reverse())})`),expected);}
});

test('a shared map snapshot retains a valid zero seed',()=>{
  const run=simulation('ember-rift');
  const result=run(`(()=>{createGame([{name:'One',factionId:'frontier',ai:false},{name:'Two',factionId:'brood',ai:false}]);const snapshot=JSON.parse(JSON.stringify(serializeState()));snapshot.mapSeed=0;mapSeed=12345;applyRemoteState(snapshot);return {seed:mapSeed,nextSeed:serializeState().mapSeed,map:chosenMapId}})()`);
  assert.equal(result.map,'ember-rift');assert.equal(result.seed,0);assert.equal(result.nextSeed,0);
});

test('relic respawns at its own battlefield location after delivery',()=>{
  for(const id of mapIds){
    const run=simulation(id);
    const result=run(`(()=>{chosenGameMode='relic';createGame([{name:'One',factionId:'frontier',ai:false},{name:'Two',factionId:'brood',ai:false}]);const home=mapLayout().relic;modeState.relic.x=1;modeState.relic.y=1;modeState.relic.respawnAt=1;updateObjectives(.1);return {x:modeState.relic.x,y:modeState.relic.y,homeX:home.x,homeY:home.y,respawnAt:modeState.relic.respawnAt}})()`);
    assert.equal(result.x,result.homeX,id);assert.equal(result.y,result.homeY,id);assert.equal(result.respawnAt,0,id);
  }
});

test('locking in a commander publishes the selected map vote',async()=>{
  const run=simulation('verdant-divide');
  const result=await run(`(async()=>{let payload;globalThis.setDoc=async(ref,data)=>payload=data;globalThis.serverTimestamp=()=>null;lobbyRef={};username='Map Tester';await publishReady();return payload.chessStarfrontReady[safeId(username)]})()`);
  assert.equal(result.mapVote,'verdant-divide');assert.equal(result.ready,true);assert.equal(result.factionId,'frontier');
});

test('the multiplayer controller starts the majority map and publishes its matching snapshot',async()=>{
  const run=simulation('classic');
  const result=await run(`(async()=>{let written;globalThis.getDoc=async()=>({exists:()=>false});writeState=async()=>{written=serializeState()};lobbyId='test-lobby';isController=true;gameStarted=false;humanNames=['One','Two','Three'];lobbyData={chessStarfrontReady:{One:{ready:true,readyAtMs:1,mapVote:'ember-rift',factionId:'frontier',objectiveVote:'conquest'},Two:{ready:true,readyAtMs:2,mapVote:'orbital-foundry',factionId:'ascendant',objectiveVote:'conquest'},Three:{ready:true,readyAtMs:3,mapVote:'orbital-foundry',factionId:'brood',objectiveVote:'conquest'}}};await maybeInitializeMultiplayer();return {started:gameStarted,map:chosenMapId,snapshotMap:written?.chosenMapId,bases:entities.filter(isBase).map(e=>e.x+','+e.y),starts:mapLayout('orbital-foundry',mapSeed).starts.map(e=>e.x+','+e.y),mode:modeState.mode}})()`);
  assert.equal(result.started,true);assert.equal(result.map,'orbital-foundry');assert.equal(result.snapshotMap,'orbital-foundry');assert.deepEqual(Array.from(result.bases),Array.from(result.starts));assert.equal(result.mode,'conquest');
});

test('simulation randomness leaves the selected battlefield layout and cache stable',()=>{
  const run=simulation('ember-rift');
  const result=run(`(()=>{createGame([{name:'One',factionId:'frontier',ai:false},{name:'Two',factionId:'brood',ai:false}]);const before=mapLayout(),initial=activeMapSeed,terrainBefore=JSON.stringify(before.terrain);for(let i=0;i<200;i++)rand();const after=mapLayout(),snapshot=serializeState();return {sameObject:before===after,sameTerrain:terrainBefore===JSON.stringify(after.terrain),sameSeed:activeMapSeed===initial,randomChanged:mapSeed!==initial,savedSeed:snapshot.activeMapSeed,initial}})()`);
  assert.equal(result.sameObject,true);assert.equal(result.sameTerrain,true);assert.equal(result.sameSeed,true);assert.equal(result.randomChanged,true);assert.equal(result.savedSeed,result.initial);
});

test('shared snapshots preserve the fixed layout seed including zero and migrate older seeds',()=>{
  const run=simulation('orbital-foundry');
  for(const legacy of [false,true]){
    const result=run(`(()=>{createGame([{name:'One',factionId:'frontier',ai:false},{name:'Two',factionId:'brood',ai:false}]);const snapshot=JSON.parse(JSON.stringify(serializeState()));snapshot.mapSeed=${legacy?'9876':'1234'};${legacy?'delete snapshot.activeMapSeed':'snapshot.activeMapSeed=0'};activeMapSeed=555;applyRemoteState(snapshot);const expected=${legacy?'9876':'0'};return {seed:activeMapSeed,savedSeed:serializeState().activeMapSeed,sameTerrain:JSON.stringify(mapLayout().terrain)===JSON.stringify(window.TurncraftMaps.generate(chosenMapId,expected).terrain)}})()`);
    assert.equal(result.seed,legacy?9876:0);assert.equal(result.savedSeed,legacy?9876:0);assert.equal(result.sameTerrain,true);
  }
});
