import test from 'node:test';
import assert from 'node:assert/strict';
import { FACTIONS, MAPS, MODES, DIFFICULTIES, BOARD_SIZES, DRAFT_BUDGET,DEFAULT_ARMY,draftCost,armyCost,defaultArmyForBudget,createGame, unitDef, skillDef, skillReadyAt, movementRange, at, legalActions, applyAction, advance, chooseAIAction, recruit } from './engine.mjs';

function arena(options={}) {
  const s=createGame(options);s.units=[];s.nextAiAt=Infinity;s.aiRecruitAt=Infinity;
  for(const t of s.tiles)t.terrain='grass';
  for(const f of s.flags){f.owner=null;f.contested=false;}
  return s;
}
function add(s,side,faction,role,x,y,extra={}) {
  const def=FACTIONS[faction].units[role];
  const u={id:`t${s.units.length}`,side,faction,role,x,y,hp:def.hp,maxHp:def.hp,readyAt:s.time,abilityReadyAt:s.time,abilityReadyAts:def.abilities.map(()=>s.time),shield:0,rootedUntil:0,cursed:0,...extra};
  s.units.push(u);return u;
}
const find=(s,u,type,target)=>legalActions(s,u.id).find(a=>a.type===type&&(!target||a.x===target.x&&a.y===target.y));
const play=(s,u,type,target)=>{const a=find(s,u,type,target);assert.ok(a,`${unitDef(u).name} has ${type}`);const r=applyAction(s,a);assert.equal(r.ok,true);return r;};
const cast=(s,u,index,target)=>{const a=legalActions(s,u.id).find(a=>a.type==='ability'&&a.skillIndex===index&&(!target||a.x===target.x&&a.y===target.y));assert.ok(a,`${unitDef(u).name} can use ${skillDef(u,index).name}`);const r=applyAction(s,a);assert.equal(r.ok,true);return r;};
function tick(s,seconds) {const events=[];while(seconds>0){const dt=Math.min(0.1,seconds);events.push(...advance(s,dt));seconds-=dt;}return events;}

test('factions have 21 unique skills; player pieces start ready and AI respects the opening grace',()=>{
  const names=new Set();
  for(const faction of Object.keys(FACTIONS))for(const map of Object.keys(MAPS)) {
    const s=createGame({playerFaction:faction,map});
    assert.equal(s.units.length,10);assert.equal(s.tiles.length,63);assert.equal(s.time,0);assert.equal(s.goal,30);
    assert.equal(s.units.filter(u=>u.side==='player'&&u.readyAt===0).length,5);
    assert.equal(s.units.filter(u=>u.side==='ai'&&u.readyAt===DIFFICULTIES.normal.grace).length,5);
    assert.ok(s.tiles.some(t=>t.terrain==='water'));assert.ok(s.tiles.some(t=>t.terrain==='rock'));
  }
  for(const f of Object.values(FACTIONS))for(const [role,d]of Object.entries(f.units)) {
    if(role==='minion'){assert.equal(d.ability,null);assert.equal(d.abilities.length,0);continue;}
    assert.equal(d.ability,d.abilities[0]);const summons=d.abilities.some(a=>['broodlings','pocketmaws','bellmotes'].includes(a.kind));assert.equal(d.abilities.length,['leader','mystic'].includes(role)?2+(summons?1:0):1);
    for(const skill of d.abilities){names.add(skill.kind);assert.ok(skill.cooldown>=8);assert.ok(skill.cooldown<=12);}
  }
  assert.equal(names.size,21);
  assert.throws(()=>createGame({map:'missing'}));assert.throws(()=>createGame({playerFaction:'missing'}));
});

test('movement follows paths, shares attack cooldown, and rejects premature repeats',()=>{
  const s=arena(),u=add(s,'player','bloom','brute',4,4);add(s,'ai','chaos','leader',4,0);
  s.tiles[4*9+5].terrain='rock';assert.equal(find(s,u,'move',{x:6,y:4}),undefined);
  assert.equal(applyAction(s,{type:'move',unitId:u.id,x:8,y:6}).ok,false);
  const a=find(s,u,'move',{x:4,y:3});assert.equal(applyAction(s,a).ok,true);assert.equal(u.readyAt,3.5);
  assert.equal(find(s,u,'move'),undefined);assert.equal(find(s,u,'attack'),undefined);assert.equal(applyAction(s,a).ok,false);
  tick(s,3.4);assert.equal(find(s,u,'move'),undefined);tick(s,0.1);assert.ok(find(s,u,'move'));
});

test('both sides act independently with no turn switch; allies allow passage but not shared destinations',()=>{
  const s=arena(),u=add(s,'player','bloom','scout',0,0),v=add(s,'ai','chaos','scout',8,6);add(s,'player','bloom','brute',1,0);
  assert.equal(find(s,u,'move',{x:1,y:0}),undefined);assert.ok(find(s,u,'move',{x:2,y:0}));
  play(s,u,'move',{x:2,y:0});play(s,v,'move',{x:6,y:6});assert.equal(u.readyAt,2);assert.equal(v.readyAt,2);
});

test('specials remain available during movement recovery and have their own cooldown',()=>{
  const s=arena(),u=add(s,'player','bloom','scout',2,5);add(s,'ai','chaos','leader',8,0);
  play(s,u,'move',{x:2,y:4});assert.equal(find(s,u,'move'),undefined);
  play(s,u,'ability',{x:2,y:3});assert.equal(u.abilityReadyAt,8);assert.equal(u.readyAt,2);
  assert.equal(find(s,u,'ability'),undefined);tick(s,2);assert.ok(find(s,u,'move'));assert.equal(find(s,u,'ability'),undefined);
  tick(s,6);assert.ok(find(s,u,'ability'));
});

test('beacon capture persists after leaving and scores exactly at the common 6-second pulse',()=>{
  const s=arena(),u=add(s,'player','bloom','scout',2,4,{hp:3});add(s,'ai','chaos','leader',8,0);
  const r=play(s,u,'move',{x:2,y:3});assert.ok(r.events.some(e=>e.type==='capture'));assert.equal(u.hp,5);
  tick(s,2);play(s,u,'move',{x:1,y:3});tick(s,3.9);assert.equal(s.scores.player,0);assert.equal(s.supplies.player,3);
  tick(s,0.1);assert.equal(s.scores.player,1);assert.equal(s.supplies.player,3);assert.equal(s.supplies.ai,3);assert.equal(s.flags[0].owner,'player');
});

test('attacks apply shields, grove cover, curses, and Chaos temper',()=>{
  const s=arena(),u=add(s,'player','chaos','mystic',2,3,{hp:6}),v=add(s,'ai','bloom','leader',4,3,{shield:1,cursed:1});
  s.tiles[3*9+4].terrain='grove';const hp=v.hp;play(s,u,'attack',v);
  assert.equal(v.hp,hp-3);assert.equal(v.shield,0);assert.equal(v.cursed,0);assert.equal(u.readyAt,2.8);
  assert.equal(applyAction(s,{type:'attack',unitId:u.id,x:v.x,y:v.y}).ok,false);
});

test('rocks and water block ranged sight; shield absorption never heals a target',()=>{
  const s=arena(),u=add(s,'player','choir','mystic',2,3),v=add(s,'ai','chaos','leader',4,3,{shield:5});
  s.tiles[3*9+3].terrain='water';assert.equal(find(s,u,'attack',v),undefined);
  s.tiles[3*9+3].terrain='grass';play(s,u,'attack',v);assert.equal(v.hp,v.maxHp);assert.equal(v.shield,3);
});

test('guard shares movement cooldown and caps shield at five',()=>{
  const s=arena(),u=add(s,'player','bloom','brute',4,4,{shield:4});add(s,'ai','chaos','leader',4,0);
  play(s,u,'guard');assert.equal(u.readyAt,3.5);assert.equal(u.shield,5);assert.equal(find(s,u,'guard'),undefined);
});

test('Bloomcall heals allies in radius, never enemies, and starts a ten-second special cooldown',()=>{
  const s=arena(),u=add(s,'player','bloom','leader',4,4,{hp:5}),ally=add(s,'player','bloom','scout',4,3,{hp:2}),foe=add(s,'ai','chaos','leader',5,4,{hp:5});
  play(s,u,'ability',u);assert.equal(u.hp,8);assert.equal(ally.hp,5);assert.equal(foe.hp,5);assert.equal(u.abilityReadyAt,10);assert.equal(u.readyAt,3);
});

test('Sporehop crosses obstacles and plants a grove; roots block movement and warp',()=>{
  const s=arena(),u=add(s,'player','bloom','scout',4,5);add(s,'ai','chaos','leader',8,0);s.tiles[4*9+4].terrain='rock';
  play(s,u,'ability',{x:4,y:3});assert.equal(u.y,3);assert.equal(s.tiles[5*9+4].terrain,'grove');
  u.readyAt=0;u.abilityReadyAt=0;u.rootedUntil=2.5;
  assert.equal(find(s,u,'ability'),undefined);assert.equal(find(s,u,'move'),undefined);
  tick(s,2.5);assert.ok(find(s,u,'move'));assert.ok(find(s,u,'ability'));
});

test('Gravity Belch pulls and roots exactly 2.5 seconds without blocking attacks',()=>{
  const s=arena(),u=add(s,'player','bloom','brute',4,5),foe=add(s,'ai','chaos','leader',4,2);
  play(s,u,'ability',foe);assert.equal(foe.hp,9);assert.equal(foe.y,3);assert.equal(foe.rootedUntil,2.5);
  const adjacent=add(s,'player','bloom','scout',3,3);assert.ok(find(s,foe,'attack',adjacent));
  tick(s,2.4);assert.equal(find(s,foe,'move'),undefined);tick(s,0.1);assert.ok(find(s,foe,'move'));
});

test('Sporeburst and Wrong Note affect enemy groups without friendly fire',()=>{
  for(const [faction,role,position] of [['bloom','mystic',{x:4,y:5}],['chaos','leader',{x:4,y:4}]]) {
    const s=arena(),u=add(s,'player',faction,role,position.x,position.y),friend=add(s,'player',faction,'scout',3,3),v=add(s,'ai','choir','leader',4,3),w=add(s,'ai','choir','scout',5,3);
    play(s,u,'ability',faction==='bloom'?v:u);assert.ok(v.hp<v.maxHp);assert.ok(w.hp<w.maxHp);assert.equal(friend.hp,friend.maxHp);
    if(faction==='chaos'){assert.equal(v.rootedUntil,2.5);assert.equal(w.rootedUntil,2.5);}
  }
});

test('Sideways Bite warps beside its target, deals damage, and claims a landing beacon',()=>{
  const s=arena(),u=add(s,'player','chaos','scout',2,5),v=add(s,'ai','bloom','leader',2,2);
  play(s,u,'ability',v);assert.equal(Math.abs(u.x-v.x)+Math.abs(u.y-v.y),1);assert.equal(v.hp,7);assert.equal(s.flags[0].owner,'player');
});

test('Foldhorn knocks back or deals bonus damage when the landing tile is blocked',()=>{
  for(const obstacle of [false,true]) {
    const s=arena(),u=add(s,'player','chaos','brute',4,5),v=add(s,'ai','bloom','leader',4,3);if(obstacle)s.tiles[2*9+4].terrain='rock';
    play(s,u,'ability',v);assert.equal(v.hp,obstacle?5:6);assert.equal(v.y,obstacle?3:2);
  }
});

test('Bad Omen modifies one incoming hit and leaves an independent cooldown',()=>{
  const s=arena(),u=add(s,'player','chaos','mystic',4,5),ally=add(s,'player','chaos','leader',4,4),v=add(s,'ai','bloom','leader',4,3);
  play(s,u,'ability',v);assert.equal(v.hp,8);assert.equal(v.cursed,1);assert.equal(u.abilityReadyAt,8);
  play(s,ally,'attack',v);assert.equal(v.hp,3);assert.equal(v.cursed,0);
});

test('Concordance refreshes other allies shared cooldown without refreshing itself or other specials',()=>{
  const s=arena(),u=add(s,'player','choir','leader',4,5),ally=add(s,'player','choir','scout',4,4,{readyAt:5,abilityReadyAt:8});add(s,'ai','chaos','leader',8,0);
  play(s,u,'ability',u);assert.equal(ally.readyAt,0);assert.equal(ally.abilityReadyAt,8);assert.equal(ally.shield,1);assert.equal(u.readyAt,3);assert.equal(u.shield,0);
});

test('Mercy Arc heals and cleanses while Sanctuary shields nearby allies',()=>{
  const s=arena(),u=add(s,'player','choir','scout',4,5),ally=add(s,'player','choir','brute',4,4,{hp:5,rootedUntil:2.5,cursed:1});add(s,'ai','chaos','leader',8,0);
  play(s,u,'ability',ally);assert.equal(ally.hp,9);assert.equal(ally.rootedUntil,0);assert.equal(ally.cursed,0);
  play(s,ally,'ability',ally);assert.equal(u.shield,4);assert.equal(ally.shield,4);
});

test('Judgment Ray pierces multiple enemies but stops at obstacles',()=>{
  const s=arena(),u=add(s,'player','choir','mystic',4,6),a=add(s,'ai','chaos','leader',4,4),b=add(s,'ai','chaos','brute',4,3),c=add(s,'ai','chaos','mystic',4,1);
  s.tiles[2*9+4].terrain='rock';play(s,u,'ability',a);assert.equal(a.hp,8);assert.equal(b.hp,9);assert.equal(c.hp,7);
});

test('six-second common pulses heal Brood groves and shield adjacent Choir on either side',()=>{
  const s=arena();add(s,'player','bloom','leader',8,6);const a=add(s,'ai','bloom','scout',2,2,{hp:2}),b=add(s,'ai','choir','leader',6,1),c=add(s,'ai','choir','scout',6,2);
  s.tiles[2*9+2].terrain='grove';tick(s,5.9);assert.equal(a.hp,2);assert.equal(b.shield,0);
  tick(s,0.1);assert.equal(a.hp,3);assert.equal(b.shield,1);assert.equal(c.shield,1);
});

test('recruitment enforces costs, cap, side, deployment and independent startup cooldowns',()=>{
  const s=createGame({mode:'reinforcements',startingMoney:26});s.nextAiAt=Infinity;
  assert.equal(recruit(s,'leader').ok,false);assert.equal(recruit(s,'brute').ok,false);assert.equal(recruit(s,'scout','invalid').ok,false);
  assert.equal(recruit(s,'scout').ok,true);assert.equal(s.supplies.player,1);
  const fresh=s.units.at(-1);assert.equal(fresh.side,'player');assert.equal(fresh.readyAt,2);assert.equal(fresh.abilityReadyAt,8);assert.equal(legalActions(s,fresh.id).length,0);
  tick(s,2);assert.ok(find(s,fresh,'move'));assert.equal(find(s,fresh,'ability'),undefined);
  s.supplies.player=20;assert.equal(recruit(s,'scout').ok,true);assert.equal(recruit(s,'scout').ok,true);assert.equal(recruit(s,'scout').ok,false);
  assert.equal(recruit(s,'scout','ai').ok,true);assert.equal(s.units.at(-1).side,'ai');
});

test('advance clamps large deltas, ignores invalid deltas and never depends on wall time',()=>{
  const s=createGame();s.nextAiAt=Infinity;
  assert.deepEqual(advance(s,-1),[]);assert.deepEqual(advance(s,NaN),[]);assert.deepEqual(advance(s,Infinity),[]);assert.equal(s.time,0);
  advance(s,4000);assert.equal(s.time,1);advance(s,0.1);assert.equal(s.time,1.1);
});

test('score, elimination, and the 180-second tiebreak end play without further actions',()=>{
  const s=arena(),u=add(s,'player','chaos','leader',4,4);add(s,'ai','bloom','scout',4,3,{hp:1});
  play(s,u,'attack',{x:4,y:3});assert.equal(s.winner,'player');assert.deepEqual(advance(s,1),[]);assert.equal(recruit(s,'scout').ok,false);
  const score=arena();add(score,'player','bloom','leader',0,6);add(score,'ai','bloom','leader',8,0);score.scores.player=29;score.flags[0].owner='player';tick(score,6);assert.equal(score.winner,'player');
  const tie=arena();add(tie,'player','bloom','leader',0,6);add(tie,'ai','bloom','leader',8,0);tie.time=179;tie.nextPulseAt=180;advance(tie,1);assert.equal(tie.winner,'draw');
});

test('AI acts automatically after a grace period on every battlefield and difficulty',()=>{
  for(const map of Object.keys(MAPS))for(const difficulty of ['easy','normal','hard']) {
    const s=createGame({map,difficulty,seed:31});const positions=s.units.filter(u=>u.side==='ai').map(u=>[u.x,u.y]);
    tick(s,1.9);assert.deepEqual(s.units.filter(u=>u.side==='ai').map(u=>[u.x,u.y]),positions);
    const events=tick(s,6.1);assert.ok(events.some(e=>['move','recruit','ability','damage'].includes(e.type)));
    const occupied=new Set();for(const u of s.units.filter(v=>v.hp>0)){assert.ok(u.hp<=u.maxHp);assert.ok(Number.isFinite(u.readyAt));assert.ok(!occupied.has(`${u.x},${u.y}`));occupied.add(`${u.x},${u.y}`);assert.equal(at(s,u.x,u.y).id,u.id);}
  }
});

test('seeded realtime matches complete with legal AI actions and beacon control',()=>{
  for(const faction of Object.keys(FACTIONS)) {
    const s=createGame({playerFaction:faction,aiFaction:({bloom:'chaos',chaos:'choir',choir:'bloom'})[faction],seed:7});
    let aiCaptured=false,ticks=0;
    while(!s.winner&&ticks++<1801) {advance(s,0.1);if(s.flags.some(f=>f.owner==='ai'))aiCaptured=true;}
    assert.ok(aiCaptured);assert.ok(['player','ai','draw'].includes(s.winner));assert.ok(s.time<=180);
    assert.equal(chooseAIAction(s),null);
  }
});

test('custom boards have reproducible connected terrain, safe deployment, and symmetric opportunities',()=>{
  const walkable=t=>!['rock','water'].includes(t.terrain);
  for(const width of [7,9,11])for(const height of [5,7,9])for(const terrain of ['sparse','balanced','wild'])for(const seed of [1,5,31,84]) {
    const options={width,height,terrain,seed,map:seed===5?'fracture':seed===31?'sanctuary':'glade'};
    const s=createGame(options),clone=createGame(options),visited=new Set(),queue=[{x:0,y:0}];
    assert.equal(s.tiles.length,width*height);assert.deepEqual(s.tiles,clone.tiles);
    assert.equal(new Set(s.flags.map(f=>`${f.x},${f.y}`)).size,3);
    for(let i=0;i<queue.length;i++) {
      const p=queue[i],k=`${p.x},${p.y}`;
      if(visited.has(k)||p.x<0||p.y<0||p.x>=width||p.y>=height||!walkable(s.tiles[p.y*width+p.x]))continue;
      visited.add(k);for(const [dx,dy]of [[1,0],[-1,0],[0,1],[0,-1]])queue.push({x:p.x+dx,y:p.y+dy});
    }
    for(const t of s.tiles) {
      if(walkable(t))assert.ok(visited.has(`${t.x},${t.y}`),`${width}x${height} ${terrain} seed${seed} connected`);
      assert.equal(t.terrain,s.tiles[(height-1-t.y)*width+width-1-t.x].terrain,'mirrored terrain');
    }
    for(const p of [...s.flags,...s.units])assert.ok(visited.has(`${p.x},${p.y}`));
    assert.equal(new Set(s.units.map(u=>`${u.x},${u.y}`)).size,10);
  }
  assert.equal(BOARD_SIZES.length,3);assert.equal(Object.keys(MODES).length,4);
  assert.throws(()=>createGame({width:8}));assert.throws(()=>createGame({height:6}));assert.throws(()=>createGame({mode:'bad'}));assert.throws(()=>createGame({terrain:'bad'}));
  assert.throws(()=>createGame({goal:61}));assert.throws(()=>createGame({duration:301}));
  const campaign=createGame({mode:'control',width:11,height:7,terrain:'wild',goal:24,duration:210});assert.equal(campaign.goal,24);assert.equal(campaign.duration,210);
});

test('Zone Control uses orthogonal nearby troop majority, makes ties contested, and scores only pulses',()=>{
  const s=arena({mode:'control'}),flag=s.flags[1];
  add(s,'player','chaos','leader',4,4);add(s,'ai','chaos','leader',4,2);
  const diagonal=add(s,'player','chaos','scout',3,2);
  advance(s,0.1);assert.equal(flag.owner,null);assert.equal(flag.contested,true);
  tick(s,5.9);assert.equal(s.scores.player,0);assert.equal(s.scores.ai,0);
  diagonal.x=4;diagonal.y=3;const events=advance(s,0.1);
  assert.equal(flag.owner,'player');assert.equal(flag.contested,false);assert.ok(events.some(e=>e.type==='capture'&&e.x===4&&e.y===3));
  assert.equal(s.scores.player,0);tick(s,5.9);assert.equal(s.scores.player,1);assert.equal(s.goal,20);
  for(const u of s.units){u.x=u.side==='player'?0:8;u.y=u.id===diagonal.id?6:0;}
  advance(s,0.1);assert.equal(flag.owner,null);assert.equal(flag.contested,false);
});

test('Zone Control refreshes immediately when a troop moves or dies',()=>{
  const s=arena({mode:'control'}),u=add(s,'player','chaos','leader',4,5),foe=add(s,'ai','chaos','leader',4,2,{hp:1});
  add(s,'ai','chaos','scout',8,0);
  play(s,u,'move',{x:4,y:4});assert.equal(s.flags[1].owner,null);assert.equal(s.flags[1].contested,true);
  tick(s,3);play(s,u,'ability',u);assert.equal(foe.hp,0);assert.equal(s.flags[1].owner,'player');assert.equal(s.flags[1].contested,false);
});

test('Last Army Standing suppresses light scoring and resolves time by HP then held flags',()=>{
  const s=arena({mode:'elimination'});add(s,'player','bloom','leader',0,6,{hp:6});add(s,'ai','bloom','leader',8,0,{hp:7});
  for(const f of s.flags)f.owner='player';tick(s,6);assert.equal(s.goal,0);assert.equal(s.scores.player,0);assert.equal(s.scores.ai,0);assert.equal(s.winner,null);
  s.time=179;s.nextPulseAt=180;advance(s,1);assert.equal(s.winner,'ai');
  const tied=arena({mode:'elimination'});add(tied,'player','bloom','leader',0,6);add(tied,'ai','bloom','leader',8,0);tied.flags[0].owner='player';tied.time=179;tied.nextPulseAt=180;
  advance(tied,1);assert.equal(tied.winner,'player');
});

test('Brood root networks extend only grove-start movement and still respect ground blockers',()=>{
  const s=arena(),u=add(s,'player','bloom','leader',0,3);add(s,'ai','chaos','leader',8,0);
  assert.equal(movementRange(s,u),2);assert.equal(find(s,u,'move',{x:3,y:3}),undefined);
  s.tiles[3*9].terrain='grove';assert.equal(movementRange(s,u),3);assert.ok(find(s,u,'move',{x:3,y:3}));
  s.tiles[3*9+1].terrain='rock';assert.equal(find(s,u,'move',{x:3,y:3}),undefined);
  assert.equal(unitDef(u).groveMoveBonus,1);assert.match(unitDef(u).movementDescription,/grove/);
});

test('Chaos scouts phase through enemy troops, while ordinary ground troops cannot',()=>{
  for(const role of ['leader','scout']) {
    const s=arena(),u=add(s,'player','chaos',role,0,3),foe=add(s,'ai','bloom','leader',1,3);
    assert.equal(find(s,u,'move',foe),undefined);
    assert.equal(!!find(s,u,'move',{x:2,y:3}),role==='scout');
  }
});

test('Choir flight crosses blockers and troops, can land on water, and never lands on rocks or troops',()=>{
  for(const role of ['leader','scout','mystic','brute']) {
    const s=arena(),u=add(s,'player','choir',role,0,3);add(s,'ai','bloom','leader',8,0);
    for(let y=0;y<s.height;y++)s.tiles[y*9+1].terrain='rock';s.tiles[3*9+1].terrain='water';
    const flies=role!=='brute';assert.equal(!!find(s,u,'move',{x:1,y:3}),flies);assert.equal(!!find(s,u,'move',{x:2,y:3}),flies);assert.equal(find(s,u,'move',{x:1,y:2}),undefined);
    s.tiles[3*9+1].terrain='grass';const foe=add(s,'ai','chaos','scout',1,3);
    assert.equal(find(s,u,'move',foe),undefined);assert.equal(!!find(s,u,'move',{x:2,y:3}),flies);
    u.rootedUntil=2.5;assert.equal(find(s,u,'move'),undefined);
  }
});

test('AI actions stay legal and finish matches across custom boards and all modes',()=>{
  for(const [mode,width,height]of [['beacons',7,5],['control',11,7],['elimination',11,9],['reinforcements',9,7]]) {
    const s=createGame({mode,width,height,terrain:'wild',duration:60,seed:17,aiFaction:'choir',playerFaction:'chaos'});let ticks=0;
    while(!s.winner&&ticks++<601) {
      advance(s,0.1);
      const positions=new Set();
      for(const u of s.units.filter(v=>v.hp>0)) {
        assert.ok(u.x>=0&&u.y>=0&&u.x<width&&u.y<height);assert.ok(!positions.has(`${u.x},${u.y}`));positions.add(`${u.x},${u.y}`);
        assert.notEqual(s.tiles[u.y*width+u.x].terrain,'rock');
      }
    }
    assert.ok(s.winner);assert.ok(s.time<=60);
  }
});

test('local two-player starts both sides ready and advances without any automatic orders or recruitment',()=>{
  const s=createGame({opponentMode:'local',mode:'reinforcements',startingMoney:26,playerFaction:'chaos',aiFaction:'chaos'});
  assert.equal(s.opponentMode,'local');assert.ok(s.units.every(u=>u.readyAt===0&&u.abilityReadyAt===0));
  const before=s.units.map(u=>({id:u.id,x:u.x,y:u.y,hp:u.hp}));
  const events=tick(s,60);
  assert.equal(chooseAIAction(s),null);assert.equal(s.units.length,10);
  assert.deepEqual(s.units.map(u=>({id:u.id,x:u.x,y:u.y,hp:u.hp})),before);
  assert.equal(events.some(e=>['move','damage','ability','recruit'].includes(e.type)),false);
  assert.equal(s.supplies.player,25);assert.equal(s.supplies.ai,25);assert.equal(s.time,60);
  assert.throws(()=>createGame({opponentMode:'online'}));
});

test('local players share the same cooldown, supply, recruitment and simultaneous scoring rules',()=>{
  const s=arena({opponentMode:'local',mode:'reinforcements',startingMoney:26}),p=add(s,'player','bloom','scout',2,4),q=add(s,'ai','chaos','scout',6,2);
  play(s,p,'move',{x:2,y:3});play(s,q,'move',{x:6,y:3});
  assert.equal(p.readyAt,2);assert.equal(q.readyAt,2);assert.equal(find(s,p,'move'),undefined);assert.equal(find(s,q,'move'),undefined);
  tick(s,2);assert.ok(find(s,p,'move'));assert.ok(find(s,q,'move'));
  assert.equal(recruit(s,'scout','player').ok,true);assert.equal(recruit(s,'scout','ai').ok,true);
  assert.equal(s.supplies.player,1);assert.equal(s.supplies.ai,1);
  const recruits=s.units.slice(-2);assert.ok(recruits.every(u=>u.readyAt===4&&u.abilityReadyAt===10));
  tick(s,4);assert.deepEqual(s.scores,{player:1,ai:1});assert.deepEqual(s.supplies,{player:4,ai:4});
  assert.ok(recruits.every(u=>legalActions(s,u.id).some(a=>a.type==='move')));
});

test('local scoring resolves both armies on a common pulse, including fair tied goals',()=>{
  const s=arena({opponentMode:'local',goal:1});
  add(s,'player','bloom','leader',0,6);add(s,'ai','bloom','leader',8,0);
  s.flags[0].owner='player';s.flags[2].owner='ai';tick(s,6);
  assert.deepEqual(s.scores,{player:1,ai:1});assert.equal(s.winner,'draw');
  const win=arena({opponentMode:'local',goal:1});add(win,'player','bloom','leader',0,6);add(win,'ai','bloom','leader',8,0);win.flags[2].owner='ai';tick(win,6);assert.equal(win.winner,'ai');
});

test('two specials recharge independently while the primary timestamp alias remains compatible',()=>{
  const s=arena(),u=add(s,'player','bloom','mystic',4,5),ally=add(s,'player','bloom','scout',5,5,{hp:3}),foe=add(s,'ai','chaos','leader',4,3);
  cast(s,u,0,foe);assert.equal(u.abilityReadyAt,10);assert.deepEqual(u.abilityReadyAts,[10,0]);assert.ok(legalActions(s,u.id).some(a=>a.type==='ability'&&a.skillIndex===1));
  cast(s,u,1,ally);assert.deepEqual(u.abilityReadyAts,[10,8]);assert.equal(u.abilityReadyAt,10);assert.equal(skillReadyAt(u,1),8);
  assert.equal(legalActions(s,u.id).some(a=>a.type==='ability'),false);
  tick(s,8);assert.ok(legalActions(s,u.id).some(a=>a.type==='ability'&&a.skillIndex===1));assert.equal(legalActions(s,u.id).some(a=>a.type==='ability'&&a.skillIndex===0),false);
  tick(s,2);assert.ok(legalActions(s,u.id).some(a=>a.type==='ability'&&a.skillIndex===0));
});

test('ability indexes enforce exact target rules and omitted indexes retain primary behavior',()=>{
  const s=arena(),u=add(s,'player','bloom','leader',4,5,{hp:5});add(s,'ai','chaos','leader',8,0);
  assert.equal(applyAction(s,{type:'ability',unitId:u.id,skillIndex:99,x:4,y:3}).ok,false);
  assert.equal(applyAction(s,{type:'ability',unitId:u.id,skillIndex:-1,x:4,y:3}).ok,false);
  assert.equal(applyAction(s,{type:'ability',unitId:u.id,skillIndex:0.5,x:4,y:3}).ok,false);
  assert.equal(applyAction(s,{type:'ability',unitId:u.id,x:4,y:3}).ok,false);
  assert.equal(applyAction(s,{type:'ability',unitId:u.id,skillIndex:1,x:u.x,y:u.y}).ok,false);
  delete u.abilityReadyAts;
  assert.equal(applyAction(s,{type:'ability',unitId:u.id,x:u.x,y:u.y}).ok,true);
  assert.equal(u.hp,8);assert.equal(u.abilityReadyAts.length,3);assert.equal(u.abilityReadyAts[0],u.abilityReadyAt);
  assert.equal(skillDef(u,9),undefined);
});

test('Seedgrove creates terrain only on empty legal ground and feeds the Brood root network',()=>{
  const s=arena(),u=add(s,'player','bloom','leader',4,5);add(s,'ai','chaos','leader',8,0);
  s.tiles[4*9+4].terrain='rock';s.tiles[4*9+5].terrain='water';
  const skills=legalActions(s,u.id).filter(a=>a.type==='ability'&&a.skillIndex===1);
  assert.equal(skills.some(a=>a.x===4&&a.y===4),false);assert.equal(skills.some(a=>a.x===5&&a.y===4),false);assert.equal(skills.some(a=>a.x===4&&a.y===5),false);
  cast(s,u,1,{x:5,y:5});assert.equal(s.tiles[5*9+5].terrain,'grove');assert.equal(u.abilityReadyAt,0);assert.equal(u.abilityReadyAts[1],8);
  tick(s,3);play(s,u,'move',{x:5,y:5});assert.equal(movementRange(s,u),3);
});

test('Nectar Mote restores two HP only to injured allies',()=>{
  const s=arena(),u=add(s,'player','bloom','mystic',4,5),ally=add(s,'player','bloom','scout',4,4,{hp:3}),foe=add(s,'ai','chaos','leader',4,3,{hp:5});
  assert.equal(legalActions(s,u.id).some(a=>a.type==='ability'&&a.skillIndex===1&&a.targetId===foe.id),false);
  cast(s,u,1,ally);assert.equal(ally.hp,5);assert.equal(foe.hp,5);assert.equal(u.abilityReadyAt,0);
});

test('Laughing Hex deals no immediate damage and amplifies one subsequent hit',()=>{
  const s=arena(),u=add(s,'player','chaos','leader',4,5),ally=add(s,'player','chaos','brute',4,4),foe=add(s,'ai','bloom','leader',4,3);
  cast(s,u,1,foe);assert.equal(foe.hp,10);assert.equal(foe.cursed,1);
  play(s,ally,'attack',foe);assert.equal(foe.hp,5);assert.equal(foe.cursed,0);
});

test('Crooked Swap moves both troops atomically and updates beacon claims',()=>{
  const s=arena(),u=add(s,'player','chaos','mystic',4,5),foe=add(s,'ai','bloom','leader',4,3);s.flags[1].owner='ai';
  const r=cast(s,u,1,foe);assert.deepEqual([u.x,u.y,foe.x,foe.y],[4,3,4,5]);assert.equal(s.flags[1].owner,'player');assert.equal(r.events.filter(e=>e.type==='move').length,2);assert.equal(at(s,4,3).id,u.id);assert.equal(at(s,4,5).id,foe.id);
  const illegal=arena(),a=add(illegal,'player','chaos','mystic',4,5),b=add(illegal,'ai','choir','leader',4,3);illegal.tiles[3*9+4].terrain='water';
  assert.equal(legalActions(illegal,a.id).some(x=>x.type==='ability'&&x.skillIndex===1&&x.targetId===b.id),false);
  illegal.tiles[3*9+4].terrain='grass';a.rootedUntil=2.5;assert.equal(legalActions(illegal,a.id).some(x=>x.type==='ability'&&x.skillIndex===1),false);
});

test('Starstep crosses barriers, lands on water, shields the caster and respects roots',()=>{
  const s=arena(),u=add(s,'player','choir','leader',4,5,{shield:4});add(s,'ai','chaos','leader',8,0);s.tiles[2*9+4].terrain='water';s.tiles[3*9+4].terrain='rock';
  assert.equal(legalActions(s,u.id).some(a=>a.type==='ability'&&a.skillIndex===1&&a.x===4&&a.y===3),false);
  cast(s,u,1,{x:4,y:2});assert.equal(u.y,2);assert.equal(u.shield,5);assert.equal(u.abilityReadyAts[1],10);assert.equal(u.abilityReadyAt,0);
  u.abilityReadyAts[1]=0;u.rootedUntil=2.5;assert.equal(legalActions(s,u.id).some(a=>a.type==='ability'&&a.skillIndex===1),false);
});

test('Ward Ray shields allies to the cap without healing or affecting enemies',()=>{
  const s=arena(),u=add(s,'player','choir','mystic',4,5),ally=add(s,'player','choir','scout',4,4,{hp:3,shield:4}),foe=add(s,'ai','chaos','leader',4,3);
  assert.equal(legalActions(s,u.id).some(a=>a.type==='ability'&&a.skillIndex===1&&a.targetId===foe.id),false);
  cast(s,u,1,ally);assert.equal(ally.shield,5);assert.equal(ally.hp,3);assert.equal(foe.shield,0);
});

test('AI can choose and legally use each of the six secondary specials',()=>{
  for(const [faction,role]of [['bloom','leader'],['bloom','mystic'],['chaos','leader'],['chaos','mystic'],['choir','leader'],['choir','mystic']]) {
    const s=arena(),u=add(s,'ai',faction,role,4,3,{readyAt:1000,abilityReadyAt:1000});u.abilityReadyAts=unitDef(u).abilities.map((_,i)=>i===1?0:1000);
    add(s,'player','chaos','leader',4,5);
    if(faction==='bloom'&&role==='mystic'||faction==='choir'&&role==='mystic')add(s,'ai',faction,'scout',5,3,{hp:2,readyAt:1000,abilityReadyAt:1000,abilityReadyAts:[1000]});
    const action=chooseAIAction(s);assert.ok(action,`${faction} ${role}`);assert.equal(action.type,'ability');assert.equal(action.skillIndex,1);assert.equal(action.unitId,u.id);assert.equal(applyAction(s,action).ok,true);
  }
});

test('five difficulty levels have distinct opening grace and global decision intervals',()=>{
  const expected=[['story','Story',8,4.8],['easy','Relaxed',6,3.2],['normal','Balanced',3,1.7],['hard','Tactician',2,1],['expert','Riftlord',1.5,0.6]];
  for(const [difficulty,name,grace,interval]of expected) {
    const d=DIFFICULTIES[difficulty];assert.equal(d.name,name);assert.equal(d.grace,grace);assert.equal(d.interval,interval);assert.ok(d.description);
    const s=createGame({difficulty,seed:19,mode:'reinforcements',aiArmy:['leader','scout','scout','scout','scout']});assert.equal(s.difficulty,difficulty);assert.equal(s.nextAiAt,grace);
    assert.ok(s.units.filter(u=>u.side==='ai').every(u=>u.readyAt===grace&&u.abilityReadyAt===grace&&u.abilityReadyAts.every(t=>t===grace)));
    const positions=s.units.map(u=>[u.x,u.y]);tick(s,grace-0.1);assert.equal(s.units.length,10);assert.deepEqual(s.units.map(u=>[u.x,u.y]),positions);
    advance(s,0.1);assert.equal(s.units.length,11);assert.ok(Math.abs(s.nextAiAt-(grace+interval))<1e-6);
    const afterRecruit=s.units.map(u=>[u.x,u.y]);tick(s,interval-0.1);assert.deepEqual(s.units.map(u=>[u.x,u.y]),afterRecruit);
    advance(s,0.1);assert.notDeepEqual(s.units.map(u=>[u.x,u.y]),afterRecruit);
    for(const u of s.units.filter(u=>u.side==='ai')) {
      assert.equal(u.maxHp,FACTIONS[u.faction].units[u.role].hp);
      if(u.readyAt>s.time&&u.readyAt!==grace)assert.ok(u.readyAt-s.time<=unitDef(u).moveCooldown+1e-6);
    }
  }
});

test('Story and Relaxed prefer useful defense over distant aggression but respond to adjacent foes',()=>{
  for(const difficulty of ['story','easy']) {
    const s=arena({difficulty,seed:11}),u=add(s,'ai','chaos','mystic',4,3),foe=add(s,'player','bloom','leader',4,5);u.abilityReadyAts[2]=1000;
    for(const f of s.flags)f.owner='ai';
    const defensive=chooseAIAction(s);assert.equal(defensive.type,'guard');assert.equal(defensive.unitId,u.id);
    foe.y=4;const response=chooseAIAction(s);assert.ok(['attack','ability'].includes(response.type));assert.equal(response.targetId,foe.id);
  }
});

test('Relaxed opens substantially slower than Balanced in seeded idle-player benchmarks',()=>{
  const results={};
  for(const difficulty of ['story','easy','normal']) {
    const rows=[];
    for(let seed=1;seed<=8;seed++) {
      const s=createGame({difficulty,seed});let firstFlag=null,firstAttack=null;
      while(!s.winner&&s.time<120&&(firstFlag===null||firstAttack===null)) {
        const events=advance(s,0.1);
        if(firstFlag===null&&s.flags.some(f=>f.owner==='ai'))firstFlag=s.time;
        if(firstAttack===null&&events.some(e=>e.type==='damage'&&s.units.find(u=>u.id===e.unitId)?.side==='player'))firstAttack=s.time;
      }
      assert.ok(firstFlag!==null);rows.push({firstFlag,firstAttack:firstAttack??120});
    }
    results[difficulty]={firstFlag:rows.reduce((n,r)=>n+r.firstFlag,0)/rows.length,firstAttack:rows.reduce((n,r)=>n+r.firstAttack,0)/rows.length};
  }
  assert.ok(results.easy.firstFlag>results.normal.firstFlag*1.8);
  assert.ok(results.easy.firstAttack>results.normal.firstAttack*1.8);
  assert.ok(results.story.firstFlag>results.easy.firstFlag);
  assert.ok(results.story.firstAttack>results.easy.firstAttack);
});

test('gentle levels remain winnable with a simple opening capture plan and no combat automation',()=>{
  for(const difficulty of ['story','easy']) {
    let wins=0;
    for(let seed=1;seed<=8;seed++) {
      const s=createGame({difficulty,seed});
      const order=(id,x,y)=>{const a=legalActions(s,id).find(a=>a.type==='move'&&a.x===x&&a.y===y);assert.ok(a);assert.equal(applyAction(s,a).ok,true);};
      order('u2',2,3);order('u3',6,3);order('u1',4,4);
      tick(s,3);order('u1',4,3);
      while(!s.winner&&s.time<180)advance(s,0.1);
      if(s.winner==='player')wins++;
    }
    assert.ok(wins>=3,`${difficulty} opening plan won ${wins} of 8 seeded battles`);
  }
});

test('all five local difficulty choices leave both human armies ready and disable AI orders',()=>{
  for(const difficulty of Object.keys(DIFFICULTIES)) {
    const s=createGame({difficulty,opponentMode:'local'}),positions=s.units.map(u=>[u.x,u.y]);
    assert.ok(s.units.every(u=>u.readyAt===0&&u.abilityReadyAts.every(t=>t===0)));
    tick(s,12);assert.equal(s.units.length,10);assert.deepEqual(s.units.map(u=>[u.x,u.y]),positions);assert.equal(chooseAIAction(s),null);
  }
});

test('drafting accepts variable affordable armies with one commander and carries unspent coins',()=>{
  assert.equal(DRAFT_BUDGET,24);assert.equal(armyCost(DEFAULT_ARMY),21);assert.deepEqual(['leader','scout','brute','mystic'].map(draftCost),[6,3,5,4]);assert.equal(draftCost('minion'),Infinity);assert.equal(armyCost(null),Infinity);
  const low=createGame({startingMoney:12});assert.deepEqual(low.armies.player,['leader','scout','scout']);assert.deepEqual(defaultArmyForBudget(12),low.armies.ai);assert.deepEqual(low.supplies,{player:0,ai:0});
  const custom=['leader','mystic','scout'],s=createGame({startingMoney:24,playerArmy:custom,aiArmy:['leader']});
  custom.push('brute');assert.deepEqual(s.armies.player,['leader','mystic','scout']);assert.equal(s.units.filter(u=>u.side==='player').length,3);assert.deepEqual(s.supplies,{player:11,ai:18});
  assert.deepEqual(s.units.filter(u=>u.side==='player').map(u=>u.role),s.armies.player);
  const full=['leader',...Array(7).fill('scout')],large=createGame({startingMoney:40,playerArmy:full,aiArmy:full,width:7,height:5});assert.equal(large.units.length,16);assert.equal(new Set(large.units.map(u=>`${u.x},${u.y}`)).size,16);
  for(const army of [[],null,['scout'],['leader','leader'],['leader','minion'],['leader','brute','brute','brute','brute'],['leader',...Array(8).fill('scout')]])assert.throws(()=>createGame({playerArmy:army}));
  for(const money of [11,41,24.5,'24',NaN])assert.throws(()=>createGame({startingMoney:money}));
});

test('custom deployment preserves ordering, validates home rows and guarantees passable deployment ground',()=>{
  const playerArmy=['leader','scout','mystic'],aiArmy=['leader','brute'];
  const playerDeployment=[{x:0,y:5},{x:8,y:6},{x:3,y:5}],aiDeployment=[{x:8,y:1},{x:0,y:0}];
  const s=createGame({playerArmy,aiArmy,playerDeployment,aiDeployment,terrain:'wild'});
  assert.deepEqual(s.units.filter(u=>u.side==='player').map(({x,y})=>({x,y})),playerDeployment);assert.deepEqual(s.deployments.ai,aiDeployment);
  playerDeployment[0].x=1;assert.equal(s.deployments.player[0].x,0);
  for(const t of s.tiles.filter(t=>t.y<=1||t.y>=s.height-2))assert.ok(!['rock','water'].includes(t.terrain));
  for(const bad of [null,[],[{x:0,y:6}],[{x:0,y:6},{x:0,y:6},{x:2,y:6}],[{x:0,y:4},{x:1,y:6},{x:2,y:6}],[{x:9,y:6},{x:1,y:6},{x:2,y:6}],[{x:0.5,y:6},{x:1,y:6},{x:2,y:6}]])assert.throws(()=>createGame({playerArmy,playerDeployment:bad}));
  assert.throws(()=>createGame({aiArmy,aiDeployment:[{x:0,y:6},{x:1,y:0}]}));
});

test('purchases and automatic buying are exclusive to Reinforcement Clash',()=>{
  for(const mode of ['beacons','control','elimination']) {
    const s=createGame({mode,startingMoney:40});s.supplies.ai=40;s.aiRecruitAt=0;
    assert.equal(recruit(s,'scout','player').ok,false);assert.equal(recruit(s,'scout','ai').ok,false);assert.equal(applyAction(s,{type:'recruit',role:'mystic',side:'ai'}).ok,false);
    const events=tick(s,20);assert.equal(events.some(e=>e.type==='recruit'),false);
  }
  const s=createGame({mode:'reinforcements',startingMoney:40,playerArmy:['leader'],aiArmy:['leader']});assert.deepEqual(s.supplies,{player:34,ai:34});
  assert.equal(recruit(s,'scout','player').ok,true);assert.equal(s.supplies.player,30);assert.equal(recruit(s,'minion','player').ok,false);
  s.time=s.aiRecruitAt;const ai=chooseAIAction(s);assert.equal(ai.type,'recruit');assert.equal(ai.side,'ai');assert.equal(applyAction(s,ai).ok,true);
  s.nextAiAt=Infinity;for(const f of s.flags)f.owner='player';tick(s,18);assert.equal(s.supplies.player,40);assert.equal(s.scores.player,9);assert.equal(s.goal,30);assert.equal(s.flags[0].owner,'player');
});

test('all three summoning skills create controllable tiny troops in every battle mode',()=>{
  for(const mode of Object.keys(MODES))for(const [faction,role,kind]of [['bloom','leader','broodlings'],['chaos','mystic','pocketmaws'],['choir','mystic','bellmotes']]) {
    const s=arena({mode}),u=add(s,'player',faction,role,4,4);add(s,'ai','chaos','leader',8,0);
    assert.equal(skillDef(u,2).kind,kind);assert.equal(skillDef(u,2).cooldown,12);
    const r=cast(s,u,2,{x:3,y:3}),tiny=s.units.filter(v=>v.role==='minion');assert.equal(tiny.length,2);assert.equal(r.events.filter(e=>e.type==='summon'&&e.sourceId===u.id).length,2);
    assert.equal(u.abilityReadyAts[2],12);assert.equal(u.abilityReadyAt,0);assert.equal(u.abilityReadyAts[1],0);
    for(const v of tiny) {const d=unitDef(v);assert.equal(v.hp,3);assert.equal(d.damage,1);assert.equal(d.move,2);assert.equal(d.range,1);assert.deepEqual(d.abilities,[]);assert.deepEqual(v.abilityReadyAts,[]);assert.equal(v.abilityReadyAt,0);assert.equal(v.readyAt,1);assert.deepEqual(legalActions(s,v.id),[]);}
    tick(s,1);assert.ok(tiny.every(v=>legalActions(s,v.id).some(a=>a.type==='move')));assert.ok(tiny.every(v=>legalActions(s,v.id).every(a=>a.type!=='ability')));
    assert.equal(unitDef(tiny[0]).movement==='Flight',faction==='choir');assert.equal(unitDef(tiny[0]).groveMoveBonus,faction==='bloom'?1:0);
  }
});

test('summons validate empty ground targets and respect the shared army cap without spending coins',()=>{
  const s=arena(),u=add(s,'player','bloom','leader',4,4);add(s,'ai','chaos','leader',8,0);
  for(let x=0;x<6;x++)add(s,'player','bloom','scout',x,6);
  s.tiles[3*9+3].terrain='water';s.tiles[3*9+4].terrain='rock';
  const targets=legalActions(s,u.id).filter(a=>a.type==='ability'&&a.skillIndex===2);assert.equal(targets.some(a=>a.x===3&&a.y===3),false);assert.equal(targets.some(a=>a.x===4&&a.y===3),false);assert.equal(targets.some(a=>a.x===4&&a.y===4),false);assert.equal(targets.some(a=>Math.abs(a.x-u.x)+Math.abs(a.y-u.y)>2),false);
  const coins=s.supplies.player;cast(s,u,2,{x:5,y:4});assert.equal(s.units.filter(v=>v.side==='player'&&v.hp>0).length,8);assert.equal(s.units.filter(v=>v.role==='minion').length,1);assert.equal(s.supplies.player,coins);
  tick(s,12);assert.equal(legalActions(s,u.id).some(a=>a.type==='ability'&&a.skillIndex===2),false);
  s.units.find(v=>v.role==='minion').hp=0;assert.ok(legalActions(s,u.id).some(a=>a.type==='ability'&&a.skillIndex===2));
});

test('summoning can produce one troop when adjacent ground is unavailable, and tiny troops can fight',()=>{
  const s=arena(),u=add(s,'player','chaos','mystic',2,3),foe=add(s,'ai','bloom','leader',4,3);
  for(const[x,y]of [[2,2],[1,1],[3,1],[2,0]])s.tiles[y*9+x].terrain='rock';
  cast(s,u,2,{x:2,y:1});const tiny=s.units.filter(v=>v.role==='minion');assert.equal(tiny.length,1);assert.deepEqual([tiny[0].x,tiny[0].y],[2,1]);
  s.tiles[1*9+3].terrain='grass';tick(s,1);play(s,tiny[0],'move',{x:3,y:2});tick(s,2.4);foe.x=3;foe.y=3;
  play(s,tiny[0],'attack',foe);assert.equal(foe.hp,9);assert.equal(tiny[0].readyAt,s.time+2.4);
});

test('AI can summon and then issue legal commands to the tiny troops without automatic recruitment',()=>{
  for(const [faction,role]of [['bloom','leader'],['chaos','mystic'],['choir','mystic']]) {
    const s=arena({aiFaction:faction}),u=add(s,'ai',faction,role,4,3,{readyAt:1000,abilityReadyAt:1000,abilityReadyAts:[1000,1000,0]});add(s,'player','chaos','leader',8,6);
    const action=chooseAIAction(s);assert.equal(action.type,'ability');assert.equal(action.skillIndex,2);assert.equal(applyAction(s,action).ok,true);
    assert.equal(s.units.filter(v=>v.role==='minion').length,2);tick(s,1);const next=chooseAIAction(s);assert.equal(s.units.find(v=>v.id===next.unitId).role,'minion');assert.equal(applyAction(s,next).ok,true);
  }
});
