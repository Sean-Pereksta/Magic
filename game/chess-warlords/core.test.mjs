import test from 'node:test';
import assert from 'node:assert/strict';
import {snapshotDelta, restoreSnapshot, rankProgress, placementRating, applyWarResult, seasonKey, pickHit, cosmeticBudget} from './core.mjs';

const checkpoint = () => ({version:1,controllerEpoch:1,mapSeed:42,fullSnapshot:true,
  pieces:Array.from({length:80},(_,i)=>({i,x:i,y:0,hp:3,t:'pawn'})),
  factions:[{idx:0,alive:true},{idx:1,alive:true}],boardOwners:'-'.repeat(800),boardWalls:'-'.repeat(800),boardKinds:'.'.repeat(800)});

test('latest cumulative delta recovers skipped moves, captures, spawns, and territory reversions',()=>{
  const base=checkpoint();
  const intermediate=structuredClone(base);intermediate.version=2;intermediate.pieces[0].x=12;intermediate.boardOwners='1'+base.boardOwners.slice(1);
  const first=restoreSnapshot(base,snapshotDelta(base,intermediate));assert.equal(first.pieces[0].x,12);
  const final=structuredClone(base);final.version=7;final.pieces[1].x=45;final.pieces.splice(3,1);final.pieces.push({i:100,x:4,y:8,hp:2,t:'rook'});final.factions[1].alive=false;
  const packet=snapshotDelta(base,final);assert.equal(packet.fullSnapshot,false);
  const received=restoreSnapshot(base,packet);
  assert.deepEqual(received.pieces,final.pieces);assert.deepEqual(received.factions,final.factions);
  assert.equal(received.boardOwners,base.boardOwners,'must undo ownership changed by an earlier packet');
  assert.equal(received.pieces[0].x,base.pieces[0].x,'must undo a piece moving back to its checkpoint position');
  assert.deepEqual(restoreSnapshot(base,packet),received,'replay is idempotent');
  assert.ok(JSON.stringify(packet).length<JSON.stringify(final).length*.2,'one-action packet stays compact');
});

test('recovery rejects the wrong checkpoint, map, epoch, and stale revision',()=>{
  const base=checkpoint(),next={...base,version:3};const packet=snapshotDelta(base,next);
  assert.equal(restoreSnapshot(null,packet),null);
  for(const field of ['version','mapSeed','controllerEpoch'])assert.equal(restoreSnapshot({...base,[field]:99},packet),null);
  assert.equal(restoreSnapshot(base,{...packet,version:1}),null);
  const takeover=snapshotDelta(base,{...next,controllerEpoch:2});assert.equal(takeover.fullSnapshot,true);
});

test('ranks have divisions, progress, tier boundaries and a final tier',()=>{
  assert.equal(rankProgress(100).label,'Bronze II');assert.equal(rankProgress(300).label,'Gold III');
  assert.equal(rankProgress(399).nextAt,400);assert.equal(rankProgress(400).label,'Diamond III');
  assert.equal(rankProgress(650).label,'Grandmaster');assert.equal(rankProgress(650).nextAt,null);
});

const match = () => ({participants:Array.from({length:8},(_,slot)=>({slot,elo:300})),placements:{0:1,1:2,2:3,3:4,4:5,5:6,6:7,7:8},kings:{1:2},territory:{1:25}});
test('multiplayer rating differentiates 2nd from 7th and incorporates opponent strength',()=>{
  const record=match(),second=placementRating(record,1),seventh=placementRating(record,6);
  assert.ok(second.delta>0);assert.ok(seventh.delta<0);assert.ok(second.delta>seventh.delta);
  const stronger=match();stronger.participants[1].elo=600;
  assert.ok(placementRating(stronger,1).delta<second.delta);
  assert.equal(placementRating({...record,placements:{}},1),null);
  const noBonus=placementRating({...record,kings:{}},1);
  assert.ok(second.delta-noBonus.delta<=3);
});

test('war records preserve history, floor ratings, and isolate seasons and factions',()=>{
  const result=placementRating(match(),1),profile={elo:300,games:4,wins:2,highestElo:340,streak:2};
  const next=applyWarResult(profile,result,'shadow','2026-Q3');
  assert.equal(next.games,5);assert.equal(next.wins,2);assert.equal(next.streak,0);assert.equal(next.highestElo,340);
  assert.equal(next.topThree,1);assert.equal(next.kingsCaptured,2);assert.equal(next.territoryCaptured,25);
  assert.equal(next.seasons['2026-Q3'].points,7);assert.equal(next.factions.shadow.games,1);
  assert.equal(profile.games,4,'must not mutate stored baseline');
  assert.equal(applyWarResult({elo:2},{...result,delta:-24},'iron').lastResult.delta,-2);
  assert.equal(seasonKey(Date.UTC(2026,9,1)),'2026-Q4');
});

test('board position wins over overlapping sprite depth; empty inputs stay empty',()=>{
  const candidates=[{id:1,hit:true,exactTile:true,boardDistance:.5,spriteDistance:30,depth:2},{id:2,hit:true,exactTile:false,boardDistance:1,spriteDistance:0,depth:10}];
  assert.equal(pickHit(candidates),1);
  candidates[0].exactTile=false;assert.equal(pickHit(candidates),1);
  assert.equal(pickHit(candidates.map(c=>({...c,hit:false}))),null);
});

test('Auto sheds optional work in the requested order',()=>{
  assert.equal(cosmeticBudget(1).ambient,false);assert.equal(cosmeticBudget(1).decoration,true);
  assert.equal(cosmeticBudget(2).decoration,false);assert.equal(cosmeticBudget(2).effects,2);
  assert.equal(cosmeticBudget(3).effects,1);assert.equal(cosmeticBudget(3).shadows,true);
  assert.equal(cosmeticBudget(4).shadows,false);assert.equal(cosmeticBudget(4).terrain,true);
  assert.equal(cosmeticBudget(5).terrain,false);
});
