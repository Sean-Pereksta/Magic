import test from 'node:test';
import assert from 'node:assert/strict';
import { BUILDINGS, RESOURCES } from '../data.mjs';
import { PLAYER, armiesOf, atWar, declareWar, economyProjection, kingdom, makePeace, mergeArmies, orderArmy, orderStructureAttack, parseSave, relation, resolveMovement, settlements, strategyTurn, treaty } from '../core.mjs';
import { createGame } from './fixtures/legacy-game.mjs';
import { buildingLevel, emptyUnits, tileProduction } from '../economy.mjs';
import { endTurn, makeContext, scriptedReply } from '../diplomacy.mjs';
import { activePlan, createPlan, finishPlans, prunePlans, recordPlayerPlans, transitionPlan } from '../plans.mjs';
import { assignSpy, captureSpy, counterStrength, detectionRisk, discoverPlan, gatherIntelligence, knownRelationships, officeLevel, paySpyRansom, recruitSpy, resolveCaptive, resolveEspionage, spyCapacity, spyUpkeep, visiblePlans } from '../espionage.mjs';
import { politicalAttitude, updateAttitudes } from '../politics.mjs';
import { bombardRange, damageStructure, structureHealth, structureMaximum } from '../structures.mjs';
import { intelligencePanel, politicalCard, structureActions } from '../intelligence-ui.mjs';
import { rivalTurnReports } from '../expansion-ui.mjs';

function stock(s) {for(const k of s.kingdoms){for(const r of RESOURCES)k.resources[r]=400;k.population=160;k.commands=8;}}
function office(s,owner=PLAYER,level=3){const t=settlements(s,owner)[0];t.intelligenceOffice=true;t.levels.intelligenceOffice=level;return t;}
function agent(s,{owner=PLAYER,host='wintermere',network=90,mission='plans'}={}) {
  office(s,owner);kingdom(s,owner).resources.gold=400;kingdom(s,owner).commands=8;
  const r=recruitSpy(s,owner);assert.equal(r.ok,true);const a=s.intelligence.agents.find(a=>a.id===r.spyId);
  assert.equal(assignSpy(s,owner,a.id,host,mission).ok,true);a.status='Embedded';a.network=network;return a;
}
function hostileStructure(type='lumber',level=1) {
  const s=createGame(100),a=s.armies[0],t=s.tiles['18,3'];
  Object.assign(t,{owner:'wintermere',terrain:'plains',building:type,levels:{[type]:level,road:1},road:true,walls:0,fortIntegrity:0});
  a.tile=t.id;declareWar(s,PLAYER,'wintermere');s.armies=s.armies.filter(e=>e.owner===PLAYER);return {s,a,t};
}

test('current-tile Attack damages hostile lumber while Move and Hold remain Hold',()=>{
  const {s,a,t}=hostileStructure();
  for(const order of ['move','hold']){assert.equal(orderArmy(s,PLAYER,a.id,t.id,order).ok,true);assert.equal(a.order,'hold');resolveMovement(s);assert.equal(structureHealth(t,'lumber'),60);}
  assert.equal(orderArmy(s,PLAYER,a.id,t.id,'attack').ok,true);assert.equal(a.order,'attack');assert.equal(a.structureTarget,'lumber');
  resolveMovement(s);assert.ok(structureHealth(t,'lumber')<60);assert.equal(a.order,'attack');
  const before=structureHealth(t,'lumber');resolveMovement(s);assert.equal(structureHealth(t,'lumber'),before,'one damage action per army per turn');
});
test('ordinary troops destroy industry over turns without changing deposits or roads',()=>{
  const {s,a,t}=hostileStructure();t.resource='wood';t.quality='rich';t.river=true;
  const before=tileProduction(t,'wintermere').wood;assert.ok(before>0);orderArmy(s,PLAYER,a.id,t.id,'attack');
  for(let n=0;n<20&&t.building;n++){resolveMovement(s);s.turn++;}
  assert.equal(t.building,null);assert.equal(buildingLevel(t,'lumber'),0);assert.equal(tileProduction(t,'wintermere').wood,0);
  assert.equal(t.resource,'wood');assert.equal(t.quality,'rich');assert.equal(t.river,true);assert.equal(t.road,true);assert.equal(t.levels.road,1);
  assert.ok(s.militaryEvents.some(e=>e.action==='structure'&&e.destroyed));assert.equal(a.order,'hold');
});
test('structure combat covers every catalog entry, preserves unrelated add-ons and cancels matching upgrades',()=>{
  for(const type of Object.keys(BUILDINGS)) {
    const {s,a,t}=hostileStructure(BUILDINGS[type].settlement||['wall','road'].includes(type)?'town':type);
    if(BUILDINGS[type].settlement)t[type]=true;
    t.levels[type]=1;if(type==='wall')t.walls=60;
    t.market=true;t.levels.market=1;
    t.project={type,remaining:2,total:2,owner:t.owner,level:1};a.units={...emptyUnits(),trebuchet:100};
    assert.equal(orderStructureAttack(s,PLAYER,a.id,t.id,type).ok,true,type);resolveMovement(s);
    assert.equal(buildingLevel(t,type),0,type);assert.equal(t.project,null,type);
    if(type!=='market')assert.equal(t.market,true,type);
    if(type==='city')assert.equal(t.building,'town');
  }
});
test('siege uses per-unit range, blocks mountains and cannot attack friendly or peaceful structures',()=>{
  const {s,a,t}=hostileStructure();a.tile='16,3';
  assert.equal(orderStructureAttack(s,PLAYER,a.id,t.id,'lumber','bombard').ok,false);
  a.units={...emptyUnits(),ram:8};assert.equal(bombardRange(a),1);assert.equal(orderStructureAttack(s,PLAYER,a.id,t.id,'lumber','bombard').ok,false);
  a.units.catapult=2;a.units.scout=1;s.tiles['17,3'].terrain='plains';assert.equal(orderStructureAttack(s,PLAYER,a.id,t.id,'lumber','bombard').ok,true);
  const pos=a.tile;resolveMovement(s);assert.equal(a.tile,pos);assert.ok(structureHealth(t,'lumber')<60);
  s.turn++;s.tiles['17,3'].terrain='mountain';assert.match(orderStructureAttack(s,PLAYER,a.id,t.id,'lumber','bombard').error,/Mountains/);
  s.tiles['17,3'].terrain='plains';a.tile='15,3';assert.match(orderStructureAttack(s,PLAYER,a.id,t.id,'lumber','bombard').error,/range/);
  s.tiles['16,3'].terrain='plains';a.units.trebuchet=1;assert.equal(orderStructureAttack(s,PLAYER,a.id,t.id,'lumber','bombard').ok,true);
  makePeace(s,PLAYER,'wintermere');assert.equal(orderStructureAttack(s,PLAYER,a.id,t.id,'lumber','bombard').ok,false);
  t.owner=PLAYER;assert.equal(orderStructureAttack(s,PLAYER,a.id,t.id,'lumber').ok,false);
});
test('ranged engines do not gain distant damage from rams or shorter-range siege',()=>{
  const {s,a,t}=hostileStructure('fort');t.fortIntegrity=45;a.tile='15,3';s.tiles['16,3'].terrain=s.tiles['17,3'].terrain='plains';
  a.units={...emptyUnits(),trebuchet:1,catapult:50,ram:50,scout:1};orderStructureAttack(s,PLAYER,a.id,t.id,'fort','bombard');resolveMovement(s);
  assert.equal(t.fortIntegrity,21,'only the trebuchet reaches three hexes');assert.equal(structureHealth(t,'fort'),150,'bombardment weakens the fort without demolishing it');
});
test('surviving defenders protect structure durability from occupying and ranged attackers',()=>{
  for(const ranged of [false,true]) {
    const {s,a,t}=hostileStructure();const d={...structuredClone(a),id:'guard',owner:'wintermere',units:{...emptyUnits(),heavyInfantry:200},order:'hold',target:null,path:[]};s.armies.push(d);
    if(ranged){a.tile='17,3';a.units.catapult=2;}
    assert.equal(orderStructureAttack(s,PLAYER,a.id,t.id,'lumber',ranged?'bombard':'attack').ok,true);resolveMovement(s);
    assert.equal(structureHealth(t,'lumber'),60);assert.equal(t.owner,'wintermere');
  }
});
test('hostility, target existence and range are rechecked when queued bombardments resolve',()=>{
  for(const mutate of [(s,a,t)=>makePeace(s,PLAYER,t.owner),(s,a,t)=>t.building=null,(s,a)=>a.units.catapult=0,(s,a)=>a.tile='5,6']) {
    const {s,a,t}=hostileStructure();a.units.catapult=2;orderStructureAttack(s,PLAYER,a.id,t.id,'lumber','bombard');mutate(s,a,t);resolveMovement(s);
    assert.equal(a.order,'hold');assert.ok(!s.militaryEvents.some(e=>e.action==='structure'));
  }
});
test('durability scales by level and fortifications exceed economic structures',()=>{
  const {t}=hostileStructure();assert.equal(structureMaximum(t,'lumber'),60);t.levels.lumber=2;assert.equal(structureMaximum(t,'lumber'),90);
  t.building='fort';t.levels.fort=1;assert.equal(structureMaximum(t,'fort'),150);t.levels.fort=2;assert.equal(structureMaximum(t,'fort'),240);
});

test('political attitudes use all-House relations, persist through small changes and react to war',()=>{
  const s=createGame(),r=relation(s,'wintermere','thornwall');Object.assign(r,{trust:65,opinion:60,reliability:80});
  s.treaties.push({id:'allied',type:'alliance',parties:['wintermere','thornwall'],expires:20});
  s.turn++;updateAttitudes(s);assert.notEqual(politicalAttitude(s,'wintermere','thornwall').label,'Steadfast Ally');
  s.turn++;updateAttitudes(s);assert.equal(politicalAttitude(s,'wintermere','thornwall').label,'Steadfast Ally');
  r.trust=49;s.turn++;updateAttitudes(s);assert.equal(politicalAttitude(s,'wintermere','thornwall').label,'Steadfast Ally');
  r.trust=51;s.turn++;updateAttitudes(s);assert.equal(politicalAttitude(s,'wintermere','thornwall').label,'Steadfast Ally');
  declareWar(s,'thornwall','wintermere');assert.match(politicalAttitude(s,'wintermere','thornwall').label,/Hostile|Vengeful|Mortal Enemy/);
  assert.notDeepEqual(relation(s,'wintermere','thornwall'),relation(s,'wintermere','sunspire'));
});
test('plans form real AI alliances and military access after preparation and a court response',()=>{
  const s=createGame();for(const k of s.kingdoms)k.commands=0;
  relation(s,'wintermere','thornwall').trust=60;relation(s,'thornwall','wintermere').trust=60;
  strategyTurn(s);const p=s.intrigue.plans.find(p=>p.actor==='wintermere'&&p.type==='seekAlliance');assert.ok(p);assert.equal(p.status,'Preparing');
  s.turn+=2;strategyTurn(s);assert.equal(p.status,'Committed');assert.equal(treaty(s,'wintermere','thornwall','alliance'),undefined);
  s.turn++;strategyTurn(s);assert.equal(p.status,'Completed');assert.ok(treaty(s,'wintermere','thornwall','alliance'));assert.ok(treaty(s,'wintermere','thornwall','access'));
});
test('committed invasion plans declare real wars and issue real marching orders',()=>{
  const s=createGame();stock(s);s.turn=12;const k=kingdom(s,'wintermere'),a=armiesOf(s,k.id)[0];a.units={...emptyUnits(),knight:100};
  for(const h of s.kingdoms)h.commands=0;
  const p=createPlan(s,k.id,'invasion',{target:'thornwall',targetTile:'31,5',objective:'Capture Briarhold.',delay:2,requiredForces:30});
  strategyTurn(s);assert.equal(p.status,'Preparing');assert.equal(atWar(s,k.id,'thornwall'),false);
  s.turn+=2;strategyTurn(s);assert.equal(atWar(s,k.id,'thornwall'),true);assert.equal(p.status,'Executing');assert.equal(a.target,'31,5');assert.ok(a.path.length);assert.ok(p.assignedArmies.includes(a.id));
});
test('a scouted difficult defended fortress makes plans build actual siege equipment',()=>{
  const s=createGame();stock(s);s.turn=12;const k=kingdom(s,'wintermere'),home=s.tiles['17,4'];
  home.workshop=true;home.levels.workshop=1;
  Object.assign(s.tiles['31,5'],{walls:180,levels:{...s.tiles['31,5'].levels,wall:3}});
  armiesOf(s,'thornwall')[0].units={...emptyUnits(),levy:100};
  // Siege requirements must come from a real observation of the enemy fortress.
  s.armies.push({...structuredClone(armiesOf(s,k.id)[0]),id:'siege-scout',tile:'29,5',units:{...emptyUnits(),scout:1},path:[],target:null,order:'hold'});
  const p=createPlan(s,k.id,'invasion',{target:'thornwall',targetTile:'31,5',requiredForces:20,requiredSiege:2,delay:1});
  strategyTurn(s);assert.equal(p.status,'Preparing');assert.equal(home.project?.type,'siegeWorks');
  assert.ok(s.intrigue.audit.some(e=>e.planId===p.id&&/build: siegeWorks/.test(e.message)));
});
test('peace, army destruction and a threatened capital abandon plans with a recorded reason',()=>{
  for(const reason of ['treaty','army','capital']) {
    const s=createGame();for(const k of s.kingdoms)k.commands=0;declareWar(s,'wintermere','thornwall');
    const a=armiesOf(s,'wintermere')[0],p=createPlan(s,'wintermere','invasion',{target:'thornwall',targetTile:'31,5',delay:0});p.assignedArmies=[a.id];
    if(reason==='treaty')s.treaties.push({id:'truce',type:'peace',parties:['wintermere','thornwall'],expires:20});
    if(reason==='army')s.armies=s.armies.filter(x=>x!==a);
    if(reason==='capital'){const invader=armiesOf(s,'thornwall')[0];invader.tile='18,4';invader.units={...emptyUnits(),knight:150};}
    strategyTurn(s);assert.equal(p.status,'Abandoned',reason);assert.ok(p.cancellationReason);assert.ok(s.intrigue.audit.some(e=>e.planId===p.id&&/Abandoned/.test(e.message)));
  }
});
test('a discovered infrastructure plan produces actual AI attacks and destruction',()=>{
  const s=createGame();for(const k of s.kingdoms)k.commands=0;
  const t=s.tiles['6,5'],a=armiesOf(s,'wintermere')[0];assert.equal(t.building,'lumber');a.tile=t.id;a.units={...emptyUnits(),levy:35};
  s.armies=s.armies.filter(x=>x.owner!=='ashen');declareWar(s,'wintermere',PLAYER);
  const p=createPlan(s,'wintermere','infrastructure',{target:PLAYER,targetTile:t.id,structure:'lumber',objective:'Destroy Ashen timber production.',delay:0,requiredForces:16});
  const spy=agent(s);const r=discoverPlan(s,spy,p);assert.equal(r.planId,p.id);assert.equal(r.snapshot.targetTile,t.id);
  for(let n=0;n<8&&activePlan(p);n++){strategyTurn(s);resolveMovement(s);finishPlans(s);s.turn++;}
  assert.equal(t.building,null);assert.equal(p.status,'Completed');assert.ok(p.assignedArmies.includes(a.id));
  assert.ok(s.militaryEvents.some(e=>e.action==='structure'&&e.destroyed&&e.attacker==='wintermere'));
  assert.equal(r.snapshot.status,'Considering','past reports never silently refresh');
});
test('merging an assigned army preserves its plan assignment',()=>{
  const s=createGame(),a=armiesOf(s,'wintermere')[0],b={...structuredClone(a),id:'army-extra'};s.armies.push(b);
  const p=createPlan(s,'wintermere','invasion',{target:'thornwall',targetTile:'31,5'});p.assignedArmies=[b.id];mergeArmies(s,'wintermere',a.tile);
  assert.deepEqual(p.assignedArmies,[a.id]);
});
test('player orders are authoritative discoverable plans and changed orders abandon them',()=>{
  const {s,a,t}=hostileStructure();orderStructureAttack(s,PLAYER,a.id,t.id,'lumber');recordPlayerPlans(s);
  const p=s.intrigue.plans.find(p=>p.actor===PLAYER);assert.equal(p.status,'Executing');
  const spy=agent(s,{owner:'wintermere',host:PLAYER});assert.equal(discoverPlan(s,spy,p).planId,p.id);
  orderArmy(s,PLAYER,a.id,t.id,'hold');recordPlayerPlans(s);assert.equal(p.status,'Abandoned');assert.match(p.cancellationReason,/changed/);
});

test('spy office capacity, paid recruitment and recurring upkeep share the real treasury',()=>{
  const s=createGame();assert.equal(recruitSpy(s).ok,false);stock(s);
  for(const level of [1,2,3]){office(s,PLAYER,level);assert.equal(spyCapacity(s,PLAYER),[0,1,3,5][level]);}
  const projection=economyProjection(s,PLAYER).income.gold,gold=kingdom(s,PLAYER).resources.gold,commands=kingdom(s,PLAYER).commands;
  for(let n=0;n<5;n++)assert.equal(recruitSpy(s).ok,true);assert.equal(recruitSpy(s).ok,false);
  assert.equal(kingdom(s,PLAYER).resources.gold,gold-300);assert.equal(kingdom(s,PLAYER).commands,commands-5);
  assert.equal(spyUpkeep(s,PLAYER),10);assert.equal(economyProjection(s,PLAYER).income.gold,projection-10);
});
test('travel and network progress are turn-based, persist and cannot be repeated within a turn',()=>{
  const s=createGame();office(s);const id=recruitSpy(s).spyId;assert.equal(assignSpy(s,PLAYER,id,'wintermere','plans').ok,true);
  const a=s.intelligence.agents[0];assert.equal(a.status,'Traveling');
  for(let n=0;n<15;n++){resolveEspionage(s,{roll:()=>.99});const copy=JSON.stringify(s);resolveEspionage(s,{roll:()=>0});assert.equal(JSON.stringify(s),copy);s.turn++;}
  assert.equal(a.status,'Embedded');assert.ok(a.network>60);assert.ok(a.experience>0);
  assert.deepEqual(parseSave(JSON.stringify(s)).intelligence,s.intelligence);
});
test('spy report detail reveals missing pieces of one actual authoritative plan',()=>{
  const s=createGame(),a=agent(s),p=createPlan(s,'wintermere','infrastructure',{target:PLAYER,targetTile:'6,5',structure:'lumber',objective:'Destroy Ashen timber.',requiredSiege:2});
  for(const [network,detail] of [[40,1],[60,2],[90,3]]) {
    a.network=network;const r=discoverPlan(s,a,p);assert.equal(r.planId,p.id);assert.equal(r.detail,detail);assert.equal(r.snapshot.actor,p.actor);
    assert.equal('targetTile' in r.snapshot,detail===3);assert.equal('type' in r.snapshot,detail>=2);
  }
  assert.deepEqual(parseSave(JSON.stringify(s)).intelligence.reports,s.intelligence.reports);
  const fabricated={...p,id:'PLAN-999999'};
  assert.equal(discoverPlan(s,a,fabricated),null);
  // A report without an authoritative source must be rejected on import.
  const bad=structuredClone(s);bad.intelligence.reports[0].planId=fabricated.id;assert.throws(()=>parseSave(JSON.stringify(bad)),/intelligence/);
});
test('no active plan produces no invented invasion, and court/economic/military reports use real snapshots',()=>{
  const s=createGame(),a=agent(s);const r=gatherIntelligence(s,a);assert.equal(r.planId,null);assert.match(r.text,/No active/);
  for(const mission of ['court','economy','military','diplomacy']){a.mission=mission;assert.ok(gatherIntelligence(s,a));}
  const military=s.intelligence.reports.find(r=>r.mission==='military');assert.ok(military.snapshot.armies.some(row=>row.id===armiesOf(s,'wintermere')[0].id));
  const economic=s.intelligence.reports.find(r=>r.mission==='economy');assert.equal(economic.snapshot.production.wood,tileProduction(s.tiles['18,3'],'wintermere').wood+tileProduction(s.tiles['17,3'],'wintermere').wood);
});
test('counterintelligence increases detection, spies can be captured, and incidents affect diplomacy',()=>{
  const s=createGame(),foreign=agent(s,{owner:'wintermere',host:PLAYER});const baseline=detectionRisk(s,foreign);
  const counter=agent(s,{owner:PLAYER,host:PLAYER,mission:'counter'});counter.network=90;
  assert.ok(counterStrength(s,PLAYER)>0);assert.ok(detectionRisk(s,foreign)>baseline);
  const trust=relation(s,PLAYER,'wintermere').trust;resolveEspionage(s,{roll:()=>0});assert.equal(foreign.status,'Captured');assert.equal(foreign.captor,PLAYER);assert.ok(relation(s,PLAYER,'wintermere').trust<trust);
});
test('execution during allied peace causes a larger incident than wartime execution',()=>{
  const impacts=[];
  for(const war of [false,true]) {
    const s=createGame(),a=agent(s,{owner:'wintermere',host:PLAYER});
    if(war)declareWar(s,PLAYER,'wintermere');else s.treaties.push({id:'ally',type:'alliance',parties:[PLAYER,'wintermere'],expires:20});
    captureSpy(s,a);const trust=relation(s,'wintermere',PLAYER).trust;assert.equal(resolveCaptive(s,PLAYER,a.id,'execute').ok,true);assert.equal(a.status,'Dead');impacts.push(trust-relation(s,'wintermere',PLAYER).trust);
    assert.equal(resolveCaptive(s,PLAYER,a.id,'execute').ok,false);
  }
  assert.ok(impacts[0]>impacts[1]*3);
});
test('captives support imprisonment, expulsion, paid ransom and reciprocal exchanges',()=>{
  for(const action of ['imprison','expel','ransom','exchange']) {
    const s=createGame(),a=agent(s,{owner:'wintermere',host:PLAYER});captureSpy(s,a);
    if(action==='exchange'){const other=agent(s);captureSpy(s,other);}
    const gold=kingdom(s,PLAYER).resources.gold;assert.equal(resolveCaptive(s,PLAYER,a.id,action).ok,true,action);
    assert.equal(a.status,action==='imprison'?'Imprisoned':'Available');
    if(action==='ransom')assert.equal(kingdom(s,PLAYER).resources.gold,gold+40);
    if(action==='exchange')assert.equal(s.intelligence.agents.find(a=>a.owner===PLAYER).status,'Available');
  }
});
test('AI captors never withdraw the player ransom without the player accepting',()=>{
  const s=createGame(),a=agent(s);captureSpy(s,a);const gold=kingdom(s,PLAYER).resources.gold;
  assert.equal(resolveCaptive(s,'wintermere',a.id,'ransom').ok,false);assert.equal(kingdom(s,PLAYER).resources.gold,gold);
  assert.equal(paySpyRansom(s,PLAYER,a.id).ok,true);assert.equal(kingdom(s,PLAYER).resources.gold,gold-40);assert.equal(a.status,'Available');
});
test('foreign relations, plan UI and model context do not expose undiscovered plans',()=>{
  const s=createGame(),p=createPlan(s,'wintermere','infrastructure',{target:PLAYER,targetTile:'6,5',structure:'lumber',objective:'PRIVATE LUMBER OPERATION'});
  assert.ok(knownRelationships(s,PLAYER,'wintermere').some(r=>r.label==='Unknown'));
  assert.doesNotMatch(intelligencePanel(s),/PRIVATE LUMBER/);assert.doesNotMatch(politicalCard(s,'wintermere'),/PRIVATE LUMBER/);
  const ctx=makeContext(s,'wintermere','Your plans?');assert.deepEqual(ctx.world.disclosedPlans,[]);assert.doesNotMatch(JSON.stringify(ctx),/PRIVATE LUMBER/);
  assert.doesNotMatch(scriptedReply(s,'wintermere','Tell me your plans').reply,/PRIVATE LUMBER/);
  const a=agent(s,{network:40});discoverPlan(s,a,p);const weak=makeContext(s,'wintermere','Your plans?');assert.equal(weak.world.disclosedPlans[0].planId,p.id);assert.equal(weak.world.disclosedPlans[0].targetTile,undefined);
  a.network=90;discoverPlan(s,a,p);assert.match(intelligencePanel(s),/PRIVATE LUMBER/);assert.equal(visiblePlans(s,PLAYER).length,1);
  assert.equal(makeContext(s,'wintermere','Your plans?').world.disclosedPlans[0].targetTile,'6,5');
});
test('structure and intelligence controls expose actionable orders with escaped names',()=>{
  const {s,a,t}=hostileStructure();a.units.catapult=2;assert.match(structureActions(s,t,a.id),/Attack Logging Camp/);assert.match(structureActions(s,t,a.id),/Bombard Logging Camp/);
  const spy=agent(s);spy.name='<img src=x onerror=alert(1)>';const html=intelligencePanel(s);assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img src=x/);assert.match(html,/data-assign-spy/);
});
test('new systems migrate old saves, continue deterministically and reject damaged cross-references',()=>{
  const s=createGame(),old=structuredClone(s);delete old.intelligence;delete old.intrigue;for(const k of old.kingdoms)for(const r of Object.values(k.relations))delete r.political;
  const migrated=parseSave(JSON.stringify(old));assert.deepEqual(migrated.intelligence.reports,[]);assert.deepEqual(migrated.intrigue.plans,[]);
  const a=agent(s);createPlan(s,'wintermere','invasion',{target:PLAYER,targetTile:'5,6'});endTurn(s);const copy=parseSave(JSON.stringify(s));endTurn(s);endTurn(copy);assert.deepEqual(copy,s);
  for(const mutate of [x=>x.intrigue.plans[0].targetTile='bad',x=>x.intelligence.agents[0].network=101,x=>x.intelligence.lastTurn=x.turn+1,x=>x.tiles['6,5'].structureDamage={lumber:999}]) {
    const bad=structuredClone(s);mutate(bad);assert.throws(()=>parseSave(JSON.stringify(bad)),/Damaged/);
  }
});
test('bounded plan retention prunes linked reports together and retains active plans',()=>{
  const s=createGame(),a=agent(s);let first;
  for(let n=0;n<130;n++){const p=createPlan(s,'wintermere','invasion',{target:PLAYER,targetTile:'5,6'});first||=p;discoverPlan(s,a,p);transitionPlan(s,p,'Completed');s.turn++;}
  prunePlans(s);assert.ok(s.intrigue.plans.length<=120);assert.ok(!s.intrigue.plans.includes(first));assert.ok(s.intelligence.reports.length<=24);
  for(const r of s.intelligence.reports.filter(r=>r.planId))assert.ok(s.intrigue.plans.some(p=>p.id===r.planId));
  assert.doesNotThrow(()=>parseSave(JSON.stringify(s)));
});

test('fresh intelligence can discover a real cancellation without rewriting an older report',()=>{
  const s=createGame(),a=agent(s),p=createPlan(s,'wintermere','invasion',{target:PLAYER,targetTile:'5,6',objective:'Capture Emberkeep.'});
  transitionPlan(s,p,'Preparing');const old=discoverPlan(s,a,p);const trust=relation(s,PLAYER,'wintermere').trust;
  discoverPlan(s,a,p);assert.equal(relation(s,PLAYER,'wintermere').trust,trust,'one diplomatic reaction per plan');
  s.turn++;transitionPlan(s,p,'Abandoned','An invasion threatens the capital.');const fresh=gatherIntelligence(s,a);
  assert.equal(fresh.planId,p.id);assert.equal(fresh.snapshot.status,'Abandoned');assert.match(fresh.text,/capital/);assert.equal(old.snapshot.status,'Preparing');
  assert.doesNotThrow(()=>parseSave(JSON.stringify(s)));
});
test('AI spies recruit with paid orders and investigate their actual strategic target',()=>{
  const s=createGame();stock(s);office(s,'wintermere',2);s.turn=10;
  const p=createPlan(s,'wintermere','invasion',{target:'thornwall',targetTile:'31,5',requiredForces:100});
  strategyTurn(s);const a=s.intelligence.agents.find(a=>a.owner==='wintermere');assert.ok(a);
  assert.equal(a.assignedHouse,p.target);assert.equal(a.mission,'plans');assert.equal(a.status,'Traveling');
  assert.ok(s.strategy.history.at(-1).houses.find(h=>h.owner==='wintermere').actions.some(a=>a.kind==='spy'));
  assert.ok(kingdom(s,'wintermere').resources.gold<=340);
});
test('a discovered offensive plan changes relations and provokes AI defensive priorities',()=>{
  const s=createGame(),a=agent(s,{owner:'wintermere',host:'thornwall'});
  const p=createPlan(s,'thornwall','invasion',{target:'wintermere',targetTile:'17,4',requiredForces:100});transitionPlan(s,p,'Preparing');
  const trust=relation(s,'wintermere','thornwall').trust;discoverPlan(s,a,p);assert.ok(relation(s,'wintermere','thornwall').trust<trust);
  for(const k of s.kingdoms)k.commands=0;strategyTurn(s);
  assert.equal(kingdom(s,'wintermere').goal,'GUARD_FRONTIER');
});
test('import rejects malformed report payloads before a UI can read them',()=>{
  const s=createGame(),a=agent(s,{mission:'court'});gatherIntelligence(s,a);
  for(const mutate of [r=>r.snapshot.relations='not an array',r=>r.snapshot.relations[0].house='missing',r=>r.snapshot.agreements=null]) {
    const damaged=structuredClone(s);mutate(damaged.intelligence.reports[0]);assert.throws(()=>parseSave(JSON.stringify(damaged)),/intelligence/);
  }
});
