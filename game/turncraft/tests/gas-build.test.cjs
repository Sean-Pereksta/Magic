const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');

// Exercise the actual authoritative game script, without network or rendering.
const noop=()=>{};
function simulation(faction='frontier'){
  const canvas=new Proxy({measureText:()=>({width:10}),createLinearGradient:()=>({addColorStop:noop}),createRadialGradient:()=>({addColorStop:noop})},{get:(o,k)=>o[k]||noop});
  function element(){return {style:{setProperty:noop},dataset:{},classList:{add:noop,remove:noop,toggle:noop,contains:()=>false},appendChild:noop,append:noop,remove:noop,setAttribute:noop,addEventListener:noop,querySelector:()=>null,querySelectorAll:()=>[],getContext:()=>canvas,getBoundingClientRect:()=>({left:0,top:700,width:1280,height:100,right:1280,bottom:800}),textContent:'',innerHTML:'',scrollTop:0,scrollLeft:0};}
  const elements=new Map(),document={getElementById:id=>{if(!elements.has(id))elements.set(id,element());return elements.get(id)},createElement:element,querySelector:()=>null,querySelectorAll:()=>[],addEventListener:noop,body:element(),documentElement:element()};
  const sandbox={console,document,location:{search:'',href:'http://turncraft.test/game/turncraft.html'},URL,URLSearchParams,localStorage:{getItem:()=>null,setItem:noop},innerWidth:1280,innerHeight:900,devicePixelRatio:1,performance:{now:()=>0},setTimeout:()=>0,clearTimeout:noop,setInterval:()=>0,requestAnimationFrame:noop,matchMedia:()=>({matches:false,addEventListener:noop}),HTMLInputElement:class{},HTMLSelectElement:class{},Image:class{},initializeApp:()=>({}),getAuth:()=>({}),getFirestore:()=>({}),navigator:{},screen:{},addEventListener:noop};
  sandbox.window=sandbox;sandbox.testClock=100000;
  const html=fs.readFileSync(path.join(__dirname,'../../turncraft.html'),'utf8');
  const code=html.match(/<script type="module">([\s\S]*?)<\/script>/)[1].replace(/^import .*;$/gm,'').replace('\nboot();','\nrefreshUI=()=>{};showCommandMarker=()=>{};pushBattleAlert=()=>{};globalThis.addTestNode=(...args)=>{addNode(...args);return resourceNodes.at(-1)};globalThis.testEvaluate=source=>eval(source);');
  const context=vm.createContext(sandbox);
  vm.runInContext(code,context,{timeout:30000,filename:'turncraft.html'});
  const run=code=>vm.runInContext(`testEvaluate(${JSON.stringify(code)})`,context,{timeout:30000});
  run(`nowMs=()=>testClock;entities=[];resourceNodes=[];nextEntityId=1;nextNodeId=1;players=[makePlayer(0,'Player',${JSON.stringify(faction)},false),makePlayer(1,'Remote','ascendant',false)];players.forEach(p=>{p.minerals=500;p.gas=0});terrain=Array.from({length:BOARD_W*BOARD_H},(_,i)=>({x:i%BOARD_W,y:Math.floor(i/BOARD_W),type:'ground',blocked:false}));gameStarted=true;gameOver=false;paused=false;PLAYER=0;selectedId=null;selectedIds=[];modeState=null;pendingImpacts=[];areaEffects=[];temporaryObstacles=[];visionCacheAt=0;tileVisibleToFaction=()=>true;initialCameraSet=true;`);
  return run;
}

for(const faction of ['frontier','ascendant','brood'])test(`${faction}: distant gas order walks, spends once on arrival, then completes`,()=>{
  const run=simulation(faction);
  const result=run(`(()=>{const fac=FACTIONS[players[0].factionId],w=addEntity('unit',fac.worker,0,3,5),gas=addTestNode('gas',15,5,1800);applyCommand({type:'build',slot:0,entityId:w.id,buildType:fac.gas,x:gas.x,y:gas.y});const queued=!!w.pendingBuild,initialCost=500-players[0].minerals,before=entities.filter(e=>e.kind==='structure').length;let built;for(let i=0;i<100;i++){testClock+=500;updatePersistentOrders();updateCombat(.5);built=entities.find(e=>e.type===fac.gas);if(built)break}const started=!!built&&built.complete===false,workerSurvived=w.alive!==false,spent=500-players[0].minerals;for(let i=0;i<30;i++){testClock+=500;updatePersistentOrders();updateProduction(.5)}return {queued,initialCost,before,started,spent,cost:STRUCTURES[fac.gas].cost[0],count:entities.filter(e=>e.type===fac.gas).length,complete:built?.complete,workerSurvived,pending:!!w.pendingBuild,near:built&&dist(w,built)<=1.5}})()`);
  assert.equal(result.queued,true);assert.equal(result.initialCost,0);assert.equal(result.before,0);assert.equal(result.started,true);assert.equal(result.spent,result.cost);assert.equal(result.count,1);assert.equal(result.complete,true);assert.equal(result.workerSurvived,faction!=='brood');assert.equal(result.pending,false);assert.equal(result.near,true);
});

test('remote human command validates its owner, faction and gas tile',()=>{
  const run=simulation();
  const result=run(`(()=>{const w=addEntity('unit','probe',1,3,5);addTestNode('gas',15,5,1800);applyCommand({type:'build',slot:0,entityId:w.id,buildType:'assimilator',x:15,y:5});const forged=!!w.pendingBuild;applyCommand({type:'build',slot:1,entityId:w.id,buildType:'refinery',x:15,y:5});const wrongFaction=!!w.pendingBuild;applyCommand({type:'build',slot:1,entityId:w.id,buildType:'assimilator',x:14,y:5});const emptyTile=!!w.pendingBuild;applyCommand({type:'build',slot:1,entityId:w.id,buildType:'assimilator',x:15,y:5});const accepted=!!w.pendingBuild;for(let i=0;i<100;i++){testClock+=500;updatePersistentOrders()}return {forged,wrongFaction,emptyTile,accepted,built:entities.some(e=>e.type==='assimilator'&&e.faction===1),remoteCost:500-players[1].minerals,localCost:500-players[0].minerals}})()`);
  assert.equal(result.forged,false);assert.equal(result.wrongFaction,false);assert.equal(result.emptyTile,false);assert.equal(result.accepted,true);assert.equal(result.built,true);assert.equal(result.remoteCost,80);assert.equal(result.localCost,0);
});

test('unaffordable, depleted, hidden, occupied and unreachable vents reject without moving or spending',()=>{
  for(const invalid of ['unaffordable','depleted','hidden','occupied','unreachable','mineral','fractional']){
    const run=simulation();
    const result=run(`(()=>{const w=addEntity('unit','engineer',0,3,5),gas=addTestNode('gas',15,5,1800),invalid=${JSON.stringify(invalid)};if(invalid==='unaffordable')players[0].minerals=79;if(invalid==='depleted')gas.amount=0;if(invalid==='hidden')tileVisibleToFaction=()=>false;if(invalid==='occupied')addEntity('structure','refinery',1,15,5,{complete:false});if(invalid==='unreachable')for(const [dx,dy] of DIR8)tileAt(15+dx,5+dy).blocked=true;if(invalid==='mineral')gas.type='mineral';const minerals=players[0].minerals,ok=issueBuildOrder(w,'refinery',invalid==='fractional'?15.5:15,5);return {ok,pending:!!w.pendingBuild,x:w.x,y:w.y,spent:minerals-players[0].minerals}})()`);
    assert.equal(result.ok,false,invalid);assert.equal(result.pending,false,invalid);assert.equal(result.x,3,invalid);assert.equal(result.y,5,invalid);assert.equal(result.spent,0,invalid);
  }
});

test('new movement, attack, stop, hold, mining and worker death cancel the route without a charge',()=>{
  for(const command of ['move','groupMove','attack','groupAttack','attackMove','groupAttackMove','stop','groupStop','hold','groupHold','mine','death']){
    const run=simulation();
    const result=run(`(()=>{const w=addEntity('unit','engineer',0,3,5),enemy=addEntity('unit','zergling',1,7,9);addEntity('structure','commandHub',0,4,9);const mineral=addTestNode('mineral',5,9,1800);addTestNode('gas',15,5,1800);issueBuildOrder(w,'refinery',15,5);const command=${JSON.stringify(command)};if(command==='death')killEntity(w);else applyCommand({type:command,slot:0,entityId:w.id,entityIds:[w.id],x:5,y:8,targetId:enemy.id,nodeId:mineral.id});for(let i=0;i<100;i++){testClock+=500;updatePersistentOrders()}return {pending:!!w.pendingBuild,spent:500-players[0].minerals,built:entities.some(e=>e.type==='refinery')}})()`);
    assert.equal(result.pending,false,command);assert.equal(result.spent,0,command);assert.equal(result.built,false,command);
  }
});

test('site reservation prevents duplicate routes; race invalidation and lost affordability cancel cleanly',()=>{
  for(const change of ['occupied','depleted','unaffordable','blocked']){
    const run=simulation();
    const result=run(`(()=>{const first=addEntity('unit','engineer',0,3,5),second=addEntity('unit','engineer',0,3,6),gas=addTestNode('gas',15,5,1800);issueBuildOrder(first,'refinery',15,5);const duplicate=issueBuildOrder(second,'refinery',15,5),change=${JSON.stringify(change)};if(change==='occupied')addEntity('structure','refinery',1,15,5);if(change==='depleted')gas.amount=0;if(change==='unaffordable')players[0].minerals=0;if(change==='blocked')for(const [dx,dy] of DIR8)tileAt(15+dx,5+dy).blocked=true;const initial=players[0].minerals;for(let i=0;i<150;i++){testClock+=500;updatePersistentOrders()}return {duplicate,pending:!!first.pendingBuild,spent:initial-players[0].minerals,built:entities.some(e=>e.type==='refinery'&&e.faction===0)}})()`);
    assert.equal(result.duplicate,false,change);assert.equal(result.pending,false,change);assert.equal(result.spent,0,change);assert.equal(result.built,false,change);
  }
});

test('worker standing on a vent steps aside, and pending construction survives a shared-state snapshot',()=>{
  const run=simulation();
  const result=run(`(()=>{const w=addEntity('unit','engineer',0,15,5);addTestNode('gas',15,5,1800);issueBuildOrder(w,'refinery',15,5);const built=entities.find(e=>e.type==='refinery'),aside=!!built&&(w.x!==15||w.y!==5);const other=addEntity('unit','engineer',0,3,10);addTestNode('gas',15,10,1800);issueBuildOrder(other,'refinery',15,10);const id=other.id,snapshot=JSON.parse(JSON.stringify(serializeState())),saved=!!snapshot.entities.find(e=>e.id===id).pendingBuild;applyRemoteState(snapshot);for(let i=0;i<100;i++){testClock+=500;updatePersistentOrders()}return {aside,saved,pending:!!entityById(id)?.pendingBuild,built:entities.some(e=>e.type==='refinery'&&e.x===15&&e.y===10),spent:500-players[0].minerals}})()`);
  assert.equal(result.aside,true);assert.equal(result.saved,true);assert.equal(result.pending,false);assert.equal(result.built,true);assert.equal(result.spent,160);
});

test('gas placement highlights distant visible vents and ordinary construction stays adjacent',()=>{
  const run=simulation();
  const result=run(`(()=>{const w=addEntity('unit','engineer',0,3,5);addTestNode('gas',15,5,1800);addTestNode('gas',20,5,1800);tileVisibleToFaction=(f,x)=>x<20;selectedId=w.id;selectedIds=[w.id];interactionMode='build';interactionPayload='refinery';refreshLegalTiles();const gas=legalTiles.map(t=>t.x+','+t.y);interactionPayload='supplyDepot';refreshLegalTiles();return {gas,normal:legalTiles.length>0&&legalTiles.every(t=>Math.max(Math.abs(t.x-w.x),Math.abs(t.y-w.y))<=1)}})()`);
  assert.deepEqual(Array.from(result.gas),['15,5']);assert.equal(result.normal,true);
});

test('gas build route leaves its old formation and does not wait for distant peers',()=>{
  const run=simulation();
  const result=run(`(()=>{const w=addEntity('unit','engineer',0,3,5),peer=addEntity('unit','engineer',0,3,6);w.formationId=peer.formationId='old';peer.moveDestination={x:3,y:7};peer.moveReadyAt=1e9;addTestNode('gas',39,5,1800);issueBuildOrder(w,'refinery',39,5);const detached=w.formationId===null&&w.formationWaitAt===0;for(let i=0;i<140;i++){testClock+=500;updatePersistentOrders()}return {detached,built:entities.some(e=>e.type==='refinery'),spent:500-players[0].minerals,pending:!!w.pendingBuild}})()`);
  assert.equal(result.detached,true);assert.equal(result.built,true);assert.equal(result.spent,80);assert.equal(result.pending,false);
});

test('a reachable across-map route lasting over a minute still reaches construction',()=>{
  const run=simulation();
  const result=run(`(()=>{const w=addEntity('unit','engineer',0,1,1);addTestNode('gas',40,28,1800);const start=testClock;issueBuildOrder(w,'refinery',40,28);let elapsed=0;for(let i=0;i<220;i++){testClock+=500;updatePersistentOrders();if(entities.some(e=>e.type==='refinery')){elapsed=testClock-start;break}}return {elapsed,built:entities.some(e=>e.type==='refinery'),spent:500-players[0].minerals,pending:!!w.pendingBuild}})()`);
  assert.ok(result.elapsed>60000);assert.equal(result.built,true);assert.equal(result.spent,80);assert.equal(result.pending,false);
});

test('a paused or backgrounded match resumes a reachable gas route before checking for stalls',()=>{
  const run=simulation();
  const result=run(`(()=>{const w=addEntity('unit','engineer',0,3,5);addTestNode('gas',25,5,1800);issueBuildOrder(w,'refinery',25,5);updatePersistentOrders();const before={x:w.x,y:w.y};testClock+=30000;updatePersistentOrders();const resumed=!!w.pendingBuild,progress=w.x!==before.x||w.y!==before.y;for(let i=0;i<100;i++){testClock+=500;updatePersistentOrders()}return {resumed,progress,built:entities.some(e=>e.type==='refinery'),spent:500-players[0].minerals}})()`);
  assert.equal(result.resumed,true);assert.equal(result.progress,true);assert.equal(result.built,true);assert.equal(result.spent,80);
});
