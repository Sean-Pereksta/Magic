import test from 'node:test';
import assert from 'node:assert/strict';
import {FACTIONS,DRAFT_ROLES,RECRUIT_ROLES,GROUND_EFFECTS,createGame,draftCost,armyCost,defaultAIArmyForBudget,unitDef,legalActions,applyAction,advance,formationStatus,movementRange,groundEffectsAt,chooseAIAction,recruit} from './engine.mjs';

function arena(player='bloom',enemy='chaos'){
  const s=createGame({playerFaction:player,aiFaction:enemy,opponentMode:'local',startingMoney:40,seed:11});
  s.units=[];s.nextId=0;s.tiles.forEach(t=>t.terrain='grass');s.nextAiAt=999;s.nextPulseAt=999;
  add(s,'player','leader',0,6);add(s,'ai','leader',8,0);return s;
}
function add(s,side,role,x,y,extra={}){
  const faction=s.factions[side],d=FACTIONS[faction].units[role],u={id:`u${++s.nextId}`,side,faction,role,x,y,hp:d.hp,maxHp:d.hp,readyAt:s.time,abilityReadyAt:s.time,abilityReadyAts:d.abilities.map(()=>s.time),shield:0,rootedUntil:0,cursed:0,...extra};
  s.units.push(u);return u;
}
function cast(s,u,kind,p){
  const index=unitDef(u).abilities.findIndex(a=>a.kind===kind),a=legalActions(s,u.id).find(a=>a.type==='ability'&&a.skillIndex===index&&(!p||a.x===p.x&&a.y===p.y));
  assert.ok(a,`${unitDef(u).name}: ${kind}`);const result=applyAction(s,a);assert.equal(result.ok,true);return result;
}
function move(s,u,x,y){const action=legalActions(s,u.id).find(a=>a.type==='move'&&a.x===x&&a.y===y);assert.ok(action,`${u.id} can move ${x},${y}`);assert.equal(applyAction(s,action).ok,true);}
function hit(s,u,v){const action=legalActions(s,u.id).find(a=>a.type==='attack'&&a.targetId===v.id);assert.ok(action);return applyAction(s,action);}
function tick(s,seconds){const events=[];while(seconds>0&&!s.winner){const dt=Math.min(.1,seconds);events.push(...advance(s,dt));seconds-=dt;}return events;}
function field(s,source,kind,x,y){s.groundEffects.push({id:`g${++s.nextGroundId}`,kind,x,y,side:source.side,sourceId:source.id,createdAt:s.time,expiresAt:s.time+GROUND_EFFECTS[kind].duration,nextTickAt:s.time+3});}

test('each faction has seven purchasable main troops, nine new identities, and two specific skills per new role',()=>{
  assert.deepEqual(DRAFT_ROLES,['leader','scout','brute','mystic','guardian','ranger','shaper']);assert.equal(RECRUIT_ROLES.length,6);
  const names=new Set(),skills=new Set();
  for(const faction of Object.values(FACTIONS))for(const role of ['guardian','ranger','shaper']){
    const d=faction.units[role];names.add(d.name);assert.equal(d.abilities.length,2);assert.equal(d.draftCost,draftCost(role));
    for(const skill of d.abilities){skills.add(skill.kind);assert.ok(['move','attack','heal','support'].includes(skill.color));assert.ok(skill.target);}
  }
  assert.equal(names.size,9);assert.equal(skills.size,18);
  for(const seed of [0,1,2,3,4,99]){const army=defaultAIArmyForBudget(24,seed);assert.equal(army.length,5);assert.equal(armyCost(army),21);assert.ok(army.some(role=>['guardian','ranger','shaper'].includes(role)));}
  const gentle=createGame({difficulty:'easy'});assert.deepEqual(gentle.armies.ai,['leader','scout','scout','brute','mystic']);
});

test('Brood connected formation uses orthogonal living members, extends movement and heals on the shared pulse',()=>{
  const s=arena(),a=add(s,'player','scout',2,3,{hp:3}),b=add(s,'player','guardian',3,3,{hp:7}),c=add(s,'player','shaper',3,4,{hp:4});
  assert.equal(formationStatus(s,a).active,true);assert.deepEqual(new Set(formationStatus(s,a).unitIds),new Set([a.id,b.id,c.id]));assert.equal(movementRange(s,a),4);
  s.tiles[a.y*s.width+a.x].terrain='grove';assert.equal(movementRange(s,a),5);s.nextPulseAt=6;tick(s,6);assert.equal(a.hp,5);assert.equal(b.hp,8);assert.equal(c.hp,5);
  c.hp=0;assert.equal(formationStatus(s,a).active,false);assert.equal(movementRange(s,a),4);b.x=3;b.y=4;assert.equal(formationStatus(s,a).active,false);
});

test('Carnival pincer requires exact opposite squares and affects only the flanked enemy',()=>{
  const s=arena('chaos','bloom'),a=add(s,'player','scout',3,3),b=add(s,'player','ranger',5,3),target=add(s,'ai','brute',4,3);
  assert.equal(formationStatus(s,a).active,true);assert.deepEqual(formationStatus(s,a).targetIds,[target.id]);hit(s,a,target);assert.equal(target.hp,target.maxHp-3);
  b.y=2;assert.equal(formationStatus(s,a).active,false);tick(s,2.4);hit(s,a,target);assert.equal(target.hp,target.maxHp-5);
});

test('Choir consecutive procession grants ranged defense and a stronger resonance ward; gaps break it',()=>{
  const s=arena('choir','bloom'),a=add(s,'player','guardian',2,3,{shield:3}),b=add(s,'player','ranger',3,3),c=add(s,'player','shaper',4,3),foe=add(s,'ai','mystic',3,1);
  assert.equal(formationStatus(s,b).active,true);hit(s,foe,b);assert.equal(b.hp,b.maxHp-1);s.nextPulseAt=6;tick(s,6);assert.equal(a.shield,3);assert.equal(b.shield,2);
  c.x=5;b.shield=0;assert.equal(formationStatus(s,b).active,false);foe.readyAt=s.time;hit(s,foe,b);assert.equal(b.hp,b.maxHp-3);
});

test('mycelium heals on three-second ticks and increases enemy ground step cost without blocking flight',()=>{
  const s=arena('bloom','choir'),caster=add(s,'player','shaper',3,4),ally=add(s,'player','ranger',4,3,{hp:2}),ground=add(s,'ai','brute',5,3);
  cast(s,caster,'mycelium',{x:4,y:3});assert.equal(s.groundEffects.length,5);assert.equal(legalActions(s,ground.id).some(a=>a.type==='move'&&a.x===4&&a.y===2),false);
  tick(s,2.9);assert.equal(ally.hp,2);tick(s,.1);assert.equal(ally.hp,3);
  const flying=add(s,'ai','scout',5,2);assert.ok(legalActions(s,flying.id).some(a=>a.type==='move'&&a.x===4&&a.y===2));
});

test('briar fields harm ground foes, spare flyers and allies, and expire without a final extra tick',()=>{
  const s=arena('bloom','choir'),caster=add(s,'player','guardian',4,4),ground=add(s,'ai','brute',4,3),flying=add(s,'ai','scout',5,3);
  cast(s,caster,'thornfield',{x:4,y:3});tick(s,3);assert.equal(ground.hp,ground.maxHp-1);assert.equal(flying.hp,flying.maxHp);assert.equal(caster.hp,caster.maxHp);
  const events=tick(s,9);assert.equal(ground.hp,ground.maxHp-3);assert.equal(s.groundEffects.length,0);assert.equal(events.filter(e=>e.type==='ground-expire').length,5);
});

test('cinders hurt on arrival and periodically, while repainting a live identical field never stacks damage',()=>{
  const s=arena('chaos','bloom'),caster=add(s,'player','guardian',3,4),foe=add(s,'ai','brute',4,3),runner=add(s,'ai','scout',6,3);
  s.orders.player=2;
  cast(s,caster,'cinders',{x:4,y:3});assert.equal(foe.hp,foe.maxHp-1);const second=add(s,'player','guardian',4,4);cast(s,second,'cinders',{x:4,y:3});assert.equal(foe.hp,foe.maxHp-1);assert.equal(s.groundEffects.length,5);
  move(s,runner,5,3);assert.equal(runner.hp,runner.maxHp-1);tick(s,3);assert.equal(foe.hp,foe.maxHp-2);assert.equal(runner.hp,runner.maxHp-2);
});

test('rifts curse enemies and grant a whole move of phasing only when an ally begins on one',()=>{
  const s=arena('chaos','bloom'),caster=add(s,'player','shaper',1,3),ally=add(s,'player','guardian',4,3),foe=add(s,'ai','brute',5,3);
  assert.equal(legalActions(s,ally.id).some(a=>a.type==='move'&&a.x===6&&a.y===3),false);cast(s,caster,'rift',{x:4,y:3});assert.equal(foe.cursed,1);assert.equal(movementRange(s,ally),3);
  tick(s,2.4);move(s,ally,6,3);assert.equal(movementRange(s,ally),2);assert.equal(groundEffectsAt(s,4,3)[0].kind,'rift');
});

test('sanctuary cleanses roots and curses while its ward preserves stronger protection',()=>{
  const s=arena('choir','chaos'),caster=add(s,'player','guardian',3,4),ally=add(s,'player','brute',4,3,{shield:4,rootedUntil:99,cursed:1});
  cast(s,caster,'sanctum',{x:4,y:3});tick(s,3);assert.equal(ally.shield,4);assert.equal(ally.cursed,0);assert.equal(ally.rootedUntil,0);ally.shield=0;tick(s,3);assert.equal(ally.shield,1);tick(s,3);assert.equal(ally.shield,1);
});

test('prism lattice strengthens ranged basic attacks, while Dawn Seal purges hostile marks in its area',()=>{
  const s=arena('choir','chaos'),ranger=add(s,'player','ranger',3,3),scribe=add(s,'player','shaper',3,5),foe=add(s,'ai','mystic',3,1),enemyCaster=s.units.find(u=>u.side==='ai');
  cast(s,ranger,'prismwall',{x:3,y:3});tick(s,2.4);hit(s,ranger,foe);assert.equal(foe.hp,foe.maxHp-3);
  tick(s,2.4);field(s,enemyCaster,'cinders',4,3);field(s,enemyCaster,'rift',7,3);cast(s,scribe,'dawnseal',{x:4,y:3});assert.equal(groundEffectsAt(s,4,3).some(e=>e.side==='ai'),false);assert.equal(groundEffectsAt(s,4,3)[0].kind,'sanctum');assert.equal(groundEffectsAt(s,7,3)[0].kind,'rift');
});

test('Curtain Collapse consumes its marked patch and converts a curse into one amplified burst',()=>{
  const s=arena('chaos','bloom'),caster=add(s,'player','shaper',3,5),foe=add(s,'ai','brute',4,3);
  cast(s,caster,'rift',{x:4,y:3});assert.equal(foe.cursed,1);tick(s,2.8);cast(s,caster,'detonate',{x:4,y:3});assert.equal(foe.hp,foe.maxHp-4);assert.equal(foe.cursed,0);assert.equal(s.groundEffects.length,0);
  assert.equal(legalActions(s,caster.id).some(a=>a.type==='ability'&&a.skillIndex===1),false);
});

test('Pollen Skate and Riftstep leave exactly two visible lasting marks and respect rooted casters',()=>{
  for(const [faction,kind,fieldKind]of [['bloom','pollenstep','mycelium'],['chaos','riftstep','rift']]){
    const s=arena(faction),caster=add(s,'player','ranger',2,4);cast(s,caster,kind,{x:4,y:3});assert.equal(caster.x,4);assert.equal(caster.y,3);assert.equal(s.groundEffects.length,2);assert.ok(s.groundEffects.every(e=>e.kind===fieldKind));
    caster.abilityReadyAts[1]=s.time;caster.rootedUntil=3;assert.equal(legalActions(s,caster.id).some(a=>a.type==='ability'&&a.skillIndex===1),false);
  }
});

test('Germinate refreshes only the standard order on friendly mycelium and never another special',()=>{
  const s=arena(),caster=add(s,'player','shaper',3,4),ally=add(s,'player','scout',4,3,{hp:2,readyAt:5,abilityReadyAt:10,abilityReadyAts:[10]});
  cast(s,caster,'germinate',ally);assert.equal(ally.hp,5);assert.equal(ally.readyAt,5);tick(s,2.8);caster.abilityReadyAts[1]=s.time;
  field(s,caster,'mycelium',ally.x,ally.y);cast(s,caster,'germinate',ally);assert.equal(ally.hp,6);assert.equal(ally.readyAt,s.time);assert.equal(ally.abilityReadyAt,10);
});

test('pinning quills root, crosscuts reward pincers, and Sunlance requires a lane and pierces one shield',()=>{
  let s=arena(),caster=add(s,'player','ranger',2,3),foe=add(s,'ai','brute',4,3);cast(s,caster,'pinshot',foe);assert.equal(foe.hp,foe.maxHp-2);assert.equal(foe.rootedUntil,2.5);
  s=arena('chaos','bloom');caster=add(s,'player','ranger',3,3);foe=add(s,'ai','brute',4,3);add(s,'player','scout',5,3);cast(s,caster,'crosscut',foe);assert.equal(foe.hp,foe.maxHp-4);
  s=arena('choir','bloom');caster=add(s,'player','ranger',2,3);foe=add(s,'ai','brute',4,3,{shield:5});const diagonal=add(s,'ai','scout',4,2);assert.equal(legalActions(s,caster.id).some(a=>a.type==='ability'&&a.skillIndex===0&&a.targetId===diagonal.id),false);cast(s,caster,'sunlance',foe);assert.equal(foe.hp,foe.maxHp-1);assert.equal(foe.shield,3);
});

test('Rootguard, Grinning Ward and Aegis Link support allies without granting healing',()=>{
  for(const [faction,kind]of [['bloom','rootguard'],['chaos','grinward']]){
    const s=arena(faction),caster=add(s,'player','guardian',3,3,{hp:5}),ally=add(s,'player','scout',3,4,{hp:3,shield:4}),foe=add(s,'ai','scout',4,3);cast(s,caster,kind);assert.equal(caster.shield,2);assert.equal(ally.shield,4);assert.equal(caster.hp,5);assert.equal(ally.hp,3);if(faction==='chaos')assert.equal(foe.rootedUntil,1);
  }
  const s=arena('choir'),caster=add(s,'player','guardian',3,3),ally=add(s,'player','scout',5,3,{hp:3,rootedUntil:10,cursed:1});cast(s,caster,'aegislink',ally);assert.equal(ally.hp,3);assert.equal(ally.shield,3);assert.equal(ally.cursed,0);assert.equal(ally.rootedUntil,0);
});

test('Procession exchanges allies atomically, grants shields and rejects a grounded ally landing on water',()=>{
  const s=arena('choir'),caster=add(s,'player','shaper',2,3),ground=add(s,'player','brute',4,3),flying=add(s,'player','scout',3,3);s.tiles[3*9+2].terrain='water';
  assert.equal(legalActions(s,caster.id).some(a=>a.type==='ability'&&a.skillIndex===1&&a.targetId===ground.id),false);cast(s,caster,'choirswap',flying);assert.deepEqual([caster.x,caster.y,flying.x,flying.y],[3,3,2,3]);assert.equal(caster.shield,2);assert.equal(flying.shield,2);
});

test('ground persists after its caster falls, cannot stack same-side layers, and never paints blocked terrain',()=>{
  const s=arena(),caster=add(s,'player','shaper',3,4),ally=add(s,'player','scout',4,3,{hp:2});s.tiles[2*9+4].terrain='rock';s.tiles[3*9+5].terrain='water';
  cast(s,caster,'mycelium',{x:4,y:3});assert.equal(s.groundEffects.length,3);assert.ok(s.groundEffects.every(e=>s.tiles[e.y*9+e.x].terrain==='grass'));caster.hp=0;tick(s,3);assert.equal(ally.hp,3);
});

test('forced swaps and new arrivals trigger the same hostile ground rule as ordinary movement',()=>{
  const s=arena('chaos','choir'),caster=add(s,'player','mystic',3,3),guardian=add(s,'player','guardian',3,5),foe=add(s,'ai','guardian',4,3);
  cast(s,guardian,'cinders',{x:3,y:3});const afterPainting=foe.hp;tick(s,2.4);cast(s,caster,'crookedswap',foe);assert.equal(foe.x,3);assert.equal(foe.hp,afterPainting-1);
  const summon=arena('bloom','chaos'),leader=summon.units.find(u=>u.side==='player'),enemy=summon.units.find(u=>u.side==='ai');field(summon,enemy,'cinders',1,6);
  cast(summon,leader,'broodlings',{x:1,y:6});assert.equal(summon.units.find(u=>u.role==='minion'&&u.x===1&&u.y===6).hp,2);
  const clash=createGame({playerFaction:'bloom',mode:'reinforcements',startingMoney:40,playerArmy:['leader'],opponentMode:'local'}),source=clash.units.find(u=>u.side==='ai');field(clash,source,'cinders',3,6);
  assert.equal(recruit(clash,'ranger').ok,true);const fresh=clash.units.at(-1);assert.deepEqual([fresh.x,fresh.y],[3,6]);assert.equal(fresh.hp,fresh.maxHp-1);
});

test('all new troop roles can be recruited only in Clash with their own startup cooldowns',()=>{
  for(const faction of Object.keys(FACTIONS))for(const role of ['guardian','ranger','shaper']){
    const s=createGame({playerFaction:faction,mode:'reinforcements',startingMoney:40,playerArmy:['leader']});const result=recruit(s,role);assert.equal(result.ok,true);const fresh=s.units.at(-1);assert.equal(fresh.role,role);assert.equal(fresh.abilityReadyAts.length,2);assert.equal(legalActions(s,fresh.id).length,0);assert.equal(recruit(createGame({playerFaction:faction,startingMoney:40}),role).ok,false);
  }
});

test('new-special AI decisions remain legal with mixed formations and simultaneous opposing ground layers',()=>{
  for(const faction of Object.keys(FACTIONS)){
    const s=createGame({playerFaction:'bloom',aiFaction:faction,startingMoney:40,aiArmy:DRAFT_ROLES,difficulty:'hard',seed:4});s.nextAiAt=Infinity;tick(s,5);
    for(const u of s.units)if(u.side==='ai'){u.readyAt=0;u.abilityReadyAt=0;u.abilityReadyAts=unitDef(u).abilities.map(()=>0);}
    for(let n=0;n<15&&!s.winner;n++){const action=chooseAIAction(s);assert.ok(action);const result=applyAction(s,action);assert.equal(result.ok,true);tick(s,2.4);}
  }
});
