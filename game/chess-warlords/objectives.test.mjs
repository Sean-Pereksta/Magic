import test from 'node:test';
import assert from 'node:assert/strict';
import {createWar, advanceWar, absorbWarRealm, objectiveVictory, capitalProduction, capitalKingExposed, enemyCapitalsHeld, ObjectivePlanner, STRATEGIC_STYLES, connectWarRealms} from './objectives.mjs';
import {snapshotDelta,restoreSnapshot} from './core.mjs';

const defs={king:{value:100},pawn:{value:1},knight:{value:5,pattern:'knight'},general:{value:7,pattern:'general'},tower:{value:6,pattern:'tower'},queen:{value:9},building:{value:6,building:true}};
function world({count=6,width=50,height=43,mode='grand'}={}){
  const board=Array.from({length:width*height},(_,i)=>({x:i%width,y:Math.floor(i/width),type:'plain',owner:null}));
  const starts=Array.from({length:count},(_,i)=>({x:Math.round(width/2+Math.cos(i*Math.PI*2/count)*width*.38),y:Math.round(height/2+Math.sin(i*Math.PI*2/count)*height*.34)}));
  const factions=Array.from({length:count},(_,idx)=>({idx,id:['crownward','ember','iron','verdant','shadow','tide'][idx%6],short:`Realm ${idx}`,alive:true,aiStyle:Object.keys(STRATEGIC_STYLES)[idx%7]}));
  return {board,width,height,starts,factions,war:createWar({board,width,height,starts,factions,mode})};
}
const unit=(id,faction,x,y,type='pawn')=>({id,faction,x,y,type,alive:true,hp:1,maxHp:1});
const advance=(w,dt,pieces=[])=>advanceWar(w.war,dt,pieces,w.factions,defs);

test('seven separated banners are passable, central, away from homes, and reachable on all supported map sizes',()=>{
  for(const [width,height,count] of [[34,22,2],[37,32,3],[40,34,4],[43,37,5],[47,40,6],[50,43,8],[86,86,7]]){
    const w=world({width,height,count}),banners=w.war.objectives.filter(o=>o.kind==='banner');
    assert.equal(banners.length,7);assert.equal(banners.filter(b=>b.central).length,1);
    for(const b of banners){
      assert.ok(!['water','mountain'].includes(w.board[b.y*width+b.x].type));
      assert.ok(w.starts.every(s=>Math.hypot(s.x-b.x,s.y-b.y)>=Math.max(5,Math.min(width,height)*.18)));
      assert.ok(banners.every(other=>other===b||Math.hypot(other.x-b.x,other.y-b.y)>=Math.max(5,Math.min(width,height)*.15)));
    }
  }
});

test('disconnected realms get deterministic narrow crossings before objective placement',()=>{
  const w=world({count:2,width:34,height:22});
  for(const t of w.board)if(t.x===17)t.type='water';
  const copy=structuredClone(w.board);connectWarRealms(w.board,w.width,w.height,w.starts);connectWarRealms(copy,w.width,w.height,w.starts);
  assert.deepEqual(w.board,copy);
  assert.ok(w.board.some(t=>t.x===17&&t.type==='road'));
  const war=createWar({...w,mode:'grand'});assert.equal(war.objectives.filter(o=>o.kind==='banner').length,7);
  assert.ok(w.board.filter(t=>t.x===17&&t.type==='road').length<=2,'only a narrow bridge is added');
});

test('six-second captures persist without leaving a unit on the flag and unit count never accelerates capture',()=>{
  const w=world(),banner=w.war.objectives[0],army=Array.from({length:12},(_,i)=>unit(i,0,banner.x,banner.y));
  advance(w,5.75,army);assert.equal(banner.owner,null);assert.equal(banner.status,'capturing');
  advance(w,.25,army);assert.equal(banner.owner,0);assert.equal(w.war.stats[0].banners,1);
  advance(w,10,[]);assert.equal(banner.owner,0);assert.equal(w.war.scores[0],4);
});

test('contested captures freeze, including hostile forces attacking an occupied capital',()=>{
  const w=world(),o=w.war.objectives[0],a=unit(1,0,o.x,o.y),b=unit(2,1,o.x+1,o.y);
  advance(w,3,[a]);const progress=o.progress;
  advance(w,10,[a,b]);assert.equal(o.progress,progress);assert.equal(o.owner,null);assert.equal(o.status,'contested');
  advance(w,3,[a]);assert.equal(o.owner,0);
  advance(w,15,[a,b]);assert.equal(w.war.scores[0],0,'contested ownership earns no points');
});

test('a raid immediately denies points and accumulated points survive losing the flag',()=>{
  const w=world(),o=w.war.objectives[0],a=unit(1,0,o.x,o.y),b=unit(2,1,o.x,o.y);
  advance(w,6,[a]);advance(w,5);assert.equal(w.war.scores[0],2);
  advance(w,6,[b]);assert.equal(o.owner,1);assert.equal(w.war.scores[0],2);
  advance(w,5);assert.equal(w.war.scores[1],2);
});

test('central and outer banners score two and one points every five seconds',()=>{
  const w=world();w.war.objectives[0].owner=0;w.war.objectives[1].owner=0;
  advance(w,4.75);assert.equal(w.war.scores[0],0);advance(w,.25);assert.equal(w.war.scores[0],3);
  advance(w,10);assert.equal(w.war.scores[0],9);
});

test('different attackers cannot borrow capture progress and abandoned progress decays',()=>{
  const w=world(),o=w.war.objectives[0];advance(w,3,[unit(1,0,o.x,o.y)]);
  advance(w,1,[unit(2,1,o.x,o.y)]);assert.equal(o.claimant,1);assert.ok(Math.abs(o.progress-1/6)<1e-6);
  advance(w,2);assert.equal(o.claimant,null);assert.equal(o.progress,0);
});

test('stationary buildings defend but cannot capture an unattended banner',()=>{
  const w=world(),o=w.war.objectives[0];advance(w,20,[unit(1,0,o.x,o.y,'building')]);assert.equal(o.owner,null);
});

test('generals and towers outside the capture ring slow takeover, with no instant recovery',()=>{
  for(const [types,seconds] of [[['general'],8],[['tower'],9],[['general','tower'],12]]){
    const w=world(),o=w.war.objectives[0];o.owner=0;
    const pieces=[unit(1,1,o.x,o.y),...types.map((type,i)=>unit(i+2,0,o.x+3*(i? -1:1),o.y,type))];
    advance(w,seconds-.25,pieces);assert.equal(o.owner,0);advance(w,.25,pieces);assert.equal(o.owner,1);
  }
});

test('capital capture, king exposure, production reduction and recapture are reversible',()=>{
  const w=world(),o=w.war.objectives.find(o=>o.kind==='capital'&&o.home===0),location={x:o.x,y:o.y};
  const king=unit(5,0,o.x+10,o.y,'king'),invader=unit(1,1,o.x,o.y);
  advance(w,11.75,[invader]);assert.equal(capitalProduction(w.war,0),1);
  advance(w,.25,[invader]);assert.equal(o.owner,1);assert.equal(capitalProduction(w.war,0),.5);assert.equal(capitalKingExposed(w.war,king),true);
  advance(w,12,[unit(2,0,o.x,o.y)]);assert.equal(o.owner,0);assert.equal(capitalProduction(w.war,0),1);assert.equal(capitalKingExposed(w.war,king),false);assert.deepEqual({x:o.x,y:o.y},location);
});

test('king absorption awards a single bonus, transfers objectives and leaves rival score history intact',()=>{
  const w=world();w.war.scores[1]=45;w.war.objectives[0].owner=1;
  absorbWarRealm(w.war,0,1);absorbWarRealm(w.war,0,1);
  assert.equal(w.war.scores[0],15);assert.equal(w.war.scores[1],45);assert.equal(w.war.stats[0].kings,1);
  assert.equal(w.war.objectives[0].owner,0);assert.equal(w.war.objectives.find(o=>o.home===1).owner,0);
});

test('victory modes and simultaneous capital counts scale to two, three and eight realms',()=>{
  for(const count of [2,3,8]){
    const w=world({count,mode:'capital'});assert.equal(w.war.capitalTarget,Math.min(4,count-1));
    const enemy=w.war.objectives.filter(o=>o.kind==='capital'&&o.home!==0);
    enemy.slice(0,w.war.capitalTarget).forEach(o=>o.owner=0);
    assert.equal(enemyCapitalsHeld(w.war,0),w.war.capitalTarget);assert.deepEqual(objectiveVictory(w.war,w.factions),{slot:0,reason:'imperial'});
    enemy[0].owner=enemy[0].home;assert.equal(objectiveVictory(w.war,w.factions),null);
  }
  for(const mode of ['grand','dominion','capital','regicide']){
    const w=world({mode});w.war.scores[0]=120;
    assert.equal(objectiveVictory(w.war,w.factions)?.reason,['grand','dominion'].includes(mode)?'dominion':undefined);
    w.factions.slice(1).forEach(f=>f.alive=false);assert.equal(objectiveVictory(w.war,w.factions).reason,'conquest');
    if(mode==='regicide')assert.equal(w.war.objectives.length,0);
  }
});

test('Dominion stops at a winning tick even for a large update; elapsed time ignores invalid updates',()=>{
  const w=world();w.war.objectives[0].owner=1;w.war.scores[1]=118;
  for(const dt of [NaN,Infinity,-1,0])advance(w,dt);assert.equal(w.war.elapsed,0);
  advance(w,100);assert.deepEqual(w.war.victory,{slot:1,reason:'dominion'});assert.equal(w.war.scores[1],120);assert.equal(w.war.elapsed,5);
  advance(w,10);assert.equal(w.war.elapsed,5);
});

test('objective capture and scoring remain equivalent across variable frame intervals',()=>{
  const small=world(),large=world();const o=small.war.objectives[0];
  for(let i=0;i<300;i++)advance(small,.1,[unit(1,0,o.x,o.y)]);
  advance(large,30,[unit(1,0,o.x,o.y)]);
  assert.equal(small.war.objectives[0].owner,large.war.objectives[0].owner);
  assert.equal(small.war.scores[0],large.war.scores[0]);
});

test('cumulative snapshots recover skipped objective ownership, scoring and capital victories',()=>{
  const w=world(),base={version:1,mapSeed:4,controllerEpoch:1,pieces:[],factions:[],boardOwners:'-',boardKinds:'.',boardWalls:'-',war:structuredClone(w.war)};
  w.war.objectives[0].owner=2;w.war.scores[2]=119;w.war.victory={slot:2,reason:'dominion'};
  const next={...base,version:5,war:structuredClone(w.war)},packet=snapshotDelta(base,next);
  assert.deepEqual(restoreSnapshot(base,packet).war,w.war);assert.equal(base.war.scores[2],0);
});

function planFixture(style='balanced',n=24){
  const w=world();w.factions[0].aiStyle=style;
  const home=w.starts[0],king=unit(100,0,home.x,home.y,'king');
  const army=Array.from({length:n},(_,i)=>unit(i+1,0,home.x-(i%5),home.y-Math.floor(i/5),i%6===0?'tower':i%4===0?'knight':'pawn'));
  const pieces=[king,...army],planner=new ObjectivePlanner();
  const run=(time=0,force=true)=>planner.plan({...w,war:w.war,faction:w.factions[0],pieces,defs,time,force});
  return {...w,army,pieces,planner,run};
}

test('all personalities maintain home defense, reserves, frontier forces and bounded objective squads',()=>{
  for(const style of Object.keys(STRATEGIC_STYLES)){
    const w=planFixture(style),plan=w.run(),roles=[...plan.assignments.values()].map(a=>a.role);
    for(const role of ['capital_defender','reserve','frontier'])assert.ok(roles.includes(role)||role==='frontier'&&roles.includes('enemy_hunter'),`${style}: ${role}`);
    for(const squad of plan.squads)assert.ok(squad.ids.length<=Math.ceil(w.army.length*.3));
    assert.ok(plan.squads.flatMap(s=>s.ids).length<=Math.floor(w.army.length*.4));
    assert.ok(plan.squads.length>=2,'offense is distributed');
    assert.ok(plan.assignments.get(100).role==='king_guard');
  }
});

test('capital threats override expansion without abandoning king protection',()=>{
  const w=planFixture('expansion'),home=w.war.objectives.find(o=>o.home===0);
  w.pieces.push(...Array.from({length:10},(_,i)=>unit(1000+i,1,home.x,home.y,'queen')));
  const plan=w.run();assert.equal(plan.homeThreat,true);
  assert.ok([...plan.assignments.values()].filter(a=>a.role==='capital_defender').length>=8);
  assert.ok([...plan.assignments.values()].some(a=>a.role==='reserve'));
});

test('AI targets the score leader, respects travel cost, and replans when the leader approaches victory',()=>{
  const w=planFixture('aggressive');const target=w.war.objectives.find(o=>o.kind==='banner'&&!o.central);target.owner=1;
  const before=w.run(),beforeValue=before.evaluation.find(e=>e.objective.id===target.id).score;
  w.war.scores[1]=119;const after=w.run(1,false),afterValue=after.evaluation.find(e=>e.objective.id===target.id).score;
  assert.notEqual(before,after);assert.ok(afterValue>beforeValue+90);assert.equal(after.emergency,true);
  assert.ok(after.evaluation.some(e=>e.travel>0));
});

test('overwhelming defenders cause retreat; plausible fights stage and release together',()=>{
  const w=planFixture('balanced'),target=w.war.objectives[0];
  // Put our frontier close enough that this objective remains strategically interesting.
  w.pieces.forEach(p=>{p.x=target.x+12;p.y=target.y;});
  const enemy=unit(1000,1,target.x,target.y,'queen');w.pieces.push(enemy);
  let plan=w.run();const squad=plan.squads.find(s=>s.objectiveId===target.id);assert.ok(squad);assert.equal(squad.phase,'stage');
  const assigned=squad.ids.map(id=>w.pieces.find(p=>p.id===id));assigned.forEach((p,i)=>{p.x=squad.staging.x+i%2;p.y=squad.staging.y;});
  plan=w.run(3);assert.equal(plan.squads.find(s=>s.objectiveId===target.id).phase,'attack');
  w.pieces.push(...Array.from({length:12},(_,i)=>unit(1100+i,1,target.x,target.y,'queen')));
  plan=w.run(6);const hopeless=plan.squads.find(s=>s.objectiveId===target.id);
  assert.ok(!hopeless || hopeless.phase==='retreat','planner abandons or retreats from the hopeless objective');
});

test('loss memory discourages repeated suicidal attacks and unreachable terrain is rejected',()=>{
  const w=planFixture(),plan=w.run(),squad=plan.squads[0];
  w.pieces.find(p=>p.id===squad.ids[0]).alive=false;
  const next=w.run(3);assert.ok(next.attrition[squad.objectiveId]>=1);
  const later=w.run(4);assert.ok(later.evaluation.find(e=>e.objective.id===squad.objectiveId).losses>0);
  const target=w.war.objectives[0];w.board[target.y*w.width+target.x].type='mountain';w.planner.reset();
  const unreachable=w.run(5);assert.equal(unreachable.evaluation.find(e=>e.objective.id===target.id).travel,30000);
});

test('cached plans invalidate on deaths, ownership changes, threats and score pressure; routes remain bounded',()=>{
  const w=planFixture(),plan=w.run();assert.equal(w.run(1,false),plan);
  w.war.objectives[0].owner=1;assert.notEqual(w.run(1.1,false),plan);
  for(let i=0;i<100;i++)w.planner.route(w.board,w.width,w.height,{x:i%w.width,y:Math.floor(i/w.width)});
  assert.ok(w.planner.routes.size<=64);
});
