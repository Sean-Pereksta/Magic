import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import * as S from './social-combat.mjs';

function save(species='capuchin',seed=1234){return C.newSave('Social test',species,'gps',seed);}
function members(species='capuchin',count=1){
 const s=save(species);for(let i=1;i<count;i++){const p=structuredClone(save('macaque',i).run.monkey);p.id=`reserve-${i}`;s.collection[p.id]=p;s.run.party.push(p.id);}const snapshot=S.snapshotTeam(s);for(const p of snapshot)p.loadout=['strike','brace','breathe'];return snapshot;
}
function duel(a=members(),b=members('langur'),seed=12){return S.newDuel({id:'alice',name:'Alice',team:a},{id:'bob',name:'Bob',team:b},seed);}
function act(d,id='strike'){return S.duelAction(d,d.players[d.turn],{type:'move',id,expectedSequence:d.sequence});}
function learn(team,id){for(const p of team){p.learned.push(id);p.loadout=['strike',id,'brace','breathe'];}return team;}

test('team snapshots restore disposable health and energy without changing expedition injuries',()=>{
 const s=save();s.run.monkey.hp=3;s.run.monkey.energy=1;s.run.monkey.status.poison=2;s.run.monkey.shield=40;s.run.monkey.cooldowns.strike=8;
 const before=structuredClone(s),snapshot=S.snapshotTeam(s);assert.deepEqual(s,before);assert.equal(snapshot[0].hp,snapshot[0].maxHp);assert.equal(snapshot[0].energy,snapshot[0].maxEnergy);assert.deepEqual(snapshot[0].status,{});assert.deepEqual(snapshot[0].cooldowns,{});assert.equal(snapshot[0].shield,0);
 snapshot[0].hp=0;assert.equal(s.run.monkey.hp,3);
});
test('snapshots preserve equipped stat benefits while stripping boss-only multipliers',()=>{
 const s=save();s.meta.gear.fang=2;s.run.monkey.equipment.weapon='fang';const expected=C.stats(s.run.monkey,C.equipped(s));s.run.monkey.elite=true;s.run.monkey.bossOnly=true;
 assert.deepEqual(S.snapshotTeam(s)[0].combatStats,expected);
});
test('invalid teams and forged unknown techniques cannot enter social battles',()=>{
 assert.throws(()=>duel([],members()),/one to three/);const t=members();assert.throws(()=>duel([t[0],t[0]],members()),/repeat/);t[0].loadout=['strike','not-a-move'];assert.throws(()=>duel(t,members()),/loadout/);
 const a=members();a[0].level=999;assert.throws(()=>duel(a,members()),/level/);
});
test('raid attempts always finish after ten choices, never damage team or mutate source',()=>{
 const source=members('capuchin',3),before=structuredClone(source),r=S.newRaidAttempt(source,1,4321);
 for(let i=0;i<S.RAID_TURNS;i++){const result=S.raidAction(r,{type:'move',id:'strike',expectedSequence:i});assert.equal(result,r);assert.equal(r.turns,i+1);assert.ok(r.team.every(p=>p.hp===p.maxHp));}
 assert.equal(r.status,'finished');assert.ok(r.totalDamage>0);assert.deepEqual(source,before);assert.throws(()=>S.raidAction(r,{type:'move',id:'strike'}),/finished/);
});
test('raid turn sequence and relay selection reject stale or invalid actions atomically',()=>{
 const r=S.newRaidAttempt(members('capuchin',2),1,4321),before=structuredClone(r);
 assert.throws(()=>S.raidAction(r,{type:'move',id:'strike',expectedSequence:2}),/changed/);assert.deepEqual(r,before);
 S.raidAction(r,{type:'switch',index:1});assert.equal(r.active,1);assert.equal(r.turns,1);
 const after=structuredClone(r);assert.throws(()=>S.raidAction(r,{type:'switch',index:1}),/another/);assert.deepEqual(r,after);
});
test('bosses increase in health and award bounded shared reward definitions',()=>{
 for(let i=2;i<=50;i++)assert.ok(S.bossHealth(i)>S.bossHealth(i-1));assert.ok(S.bossHealth(1000)<=50000000);assert.throws(()=>S.bossHealth(0));
 assert.deepEqual(Object.keys(S.bossReward(3)).sort(),['fruit','items','xp']);assert.ok(S.bossReward(2).xp>S.bossReward(1).xp);assert.equal(S.bossReward(3).items.bondCharm,1);
});
test('duels alternate real turns and duplicate or out-of-turn submissions cannot act twice',()=>{
 const d=duel(),source=structuredClone(d);assert.throws(()=>S.duelAction(d,'bob',{type:'move',id:'strike'}),/turn/);assert.deepEqual(d,source);
 S.duelAction(d,'alice',{type:'move',id:'strike',expectedSequence:0});assert.equal(d.turn,1);assert.equal(d.sequence,1);
 const after=structuredClone(d);assert.throws(()=>S.duelAction(d,'alice',{type:'move',id:'strike',expectedSequence:0}),/turn/);assert.throws(()=>S.duelAction(d,'bob',{type:'move',id:'strike',expectedSequence:0}),/changed/);assert.deepEqual(d,after);
 S.duelAction(d,'bob',{type:'move',id:'strike',expectedSequence:1});assert.equal(d.turn,0);assert.equal(d.sequence,2);
});
test('deterministic duel and raid transcripts replay identically after JSON round trips',()=>{
 const a=members('capuchin',3),b=members('macaque',3),first=duel(a,b,56),second=JSON.parse(JSON.stringify(duel(a,b,56)));
 for(let i=0;i<10&&first.status==='active';i++){const action={type:'move',id:'strike',expectedSequence:i};S.duelAction(first,first.players[first.turn],action);S.duelAction(second,second.players[second.turn],action);assert.deepEqual(first,second);}
 const r1=S.newRaidAttempt(a,4,25),r2=JSON.parse(JSON.stringify(S.newRaidAttempt(a,4,25)));for(let i=0;i<10;i++){const action={type:'move',id:'strike',expectedSequence:i};S.raidAction(r1,action);S.raidAction(r2,action);}assert.deepEqual(r1,r2);
});
test('energy, cooldowns and use limits remain enforced on a player’s own turns',()=>{
 const d=duel(learn(members(),'fieldMend'),members()),p=d.teams[0][0];p.hp=10;
 act(d,'fieldMend');assert.ok(p!==d.teams[0][0],'Reducer replaces nested disposable copies, never original references');assert.ok(d.teams[0][0].hp>10);
 act(d,'breathe');const before=structuredClone(d);assert.throws(()=>act(d,'fieldMend'),/cooldown/);assert.deepEqual(d,before);
 const t=duel(learn(members(),'teamMend'),members());t.teams[0][0].uses.teamMend=2;assert.throws(()=>act(t,'teamMend'),/uses/);
 const e=duel(learn(members(),'heavyPalm'),members());e.teams[0][0].energy=0;assert.throws(()=>act(e,'heavyPalm'),/energy/);
});
test('effective self healing and team healing restore only conscious companions',()=>{
 const d=duel(learn(members('langur',3),'teamMend'),members()),before=d.teams[0].map(p=>p.maxHp);d.teams[0][0].hp=10;d.teams[0][1].hp=10;d.teams[0][2].hp=0;
 act(d,'teamMend');assert.ok(d.teams[0][0].hp>10);assert.ok(d.teams[0][1].hp>10);assert.equal(d.teams[0][2].hp,0);assert.ok(d.teams[0].every((p,i)=>p.hp<=before[i]));
});
test('brace from either turn position protects the next incoming hit',()=>{
 const first=duel(),control=structuredClone(first);act(first,'brace');act(control,'breathe');act(first,'strike');act(control,'strike');assert.ok(first.teams[0][0].hp>control.teams[0][0].hp);
 const second=duel(),control2=structuredClone(second);act(second,'breathe');act(control2,'breathe');act(second,'brace');act(control2,'breathe');act(second,'strike');act(control2,'strike');assert.ok(second.teams[1][0].hp>control2.teams[1][0].hp);
});
test('root prevents voluntary switching but a knockout automatically chooses a reserve',()=>{
 const d=duel(learn(members('capuchin',2),'rootedChallenge'),members('macaque',2));act(d,'rootedChallenge');const before=structuredClone(d);assert.throws(()=>S.duelAction(d,'bob',{type:'switch',index:1}),/Rooted/);assert.deepEqual(d,before);
 act(d,'breathe');d.teams[1][0].hp=1;act(d,'strike');assert.equal(d.active[1],1);assert.equal(d.status,'active');assert.equal(d.turn,1);
});
test('lethal results are final, both players get XP, and fruit limits follow loser levels',()=>{
 const d=duel();d.teams[1][0].hp=1;act(d,'strike');assert.equal(d.status,'finished');assert.equal(d.winner,'alice');assert.throws(()=>act(d),/finished/);
 const rewards=S.duelRewards(d);assert.ok(rewards.alice.xp>0&&rewards.bob.xp>0);assert.equal(rewards.alice.fruitCap,2);assert.equal(rewards.bob.fruitCap,0);
 assert.equal(S.duelStake([{level:60}]),10);assert.equal(S.duelStake([{level:2},{level:3}]),4);
});
test('surrender and bounded passive battles finish deterministically without expedition death',()=>{
 const d=duel();S.duelAction(d,'bob',{type:'surrender',expectedSequence:0});assert.equal(d.winner,'alice');assert.equal(d.status,'finished');
 const passive=duel();for(let i=0;i<S.DUEL_TURN_LIMIT;i++)act(passive,'breathe');assert.equal(passive.status,'finished');assert.equal(passive.winner,null);assert.ok(passive.teams.flat().every(p=>p.hp>0));
});

test('every catalogue technique resolves deterministically with bounded health and energy',()=>{
 for(const [id,move]of Object.entries(C.MOVES)){
  const roster=members('capuchin',3),p=roster[0];
  if(move.signature){p.species=Object.entries(C.SPECIES).find(([,s])=>s.signature===id)[0];p.types=[...(C.SPECIES[p.species].types||[])];}
  p.stage=3;p.learned=[...new Set([...p.learned,id])];p.loadout=['strike',id,'brace','breathe'].filter((id,index,array)=>array.indexOf(id)===index);
  const first=duel(roster,members('macaque',3),8765),second=structuredClone(first);
  for(const d of [first,second]){d.teams[0][0].hp=20;d.teams[0][1].hp=20;d.teams[1][0].hp=1000;d.teams[1][0].maxHp=1000;}
  const action={type:'move',id,expectedSequence:0,...(move.switch?{index:1}:{})};S.duelAction(first,'alice',action);S.duelAction(second,'alice',action);assert.deepEqual(first,second,id);
  assert.ok(first.teams.flat().every(p=>p.hp>=0&&p.hp<=p.maxHp&&p.energy>=0&&p.energy<=p.maxEnergy),id);
 }
});
