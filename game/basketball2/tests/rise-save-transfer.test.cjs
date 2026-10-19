const test=require('node:test');
const assert=require('node:assert/strict');
const Rise=require('../js/campaign/rise-to-glory.js');

function storage(initial={}){
  const data=new Map(Object.entries(initial));
  return {data,getItem:key=>data.get(key)??null,setItem:(key,value)=>data.set(key,String(value)),removeItem:key=>data.delete(key)};
}
function progressedCampaign(){
  const state=Rise.create({name:'Portable Comets',seed:24}),captain=state.players[state.captainId];
  const recruit=Rise.market(state,{affordable:true,sort:'cost'})[0];
  assert.equal(Rise.sign(state,recruit.id).ok,true);
  assert.equal(Rise.setStarters(state,[state.starters[0],state.starters[1],recruit.id]).ok,true);
  Rise.setRole(state,captain.id,'shooter',3);captain.abilities.catchAndShoot=2;captain.abilities.quickRelease=1;
  captain.ratings.threePoint=Math.min(captain.caps.threePoint,66);captain.upgradePoints=8;
  Rise.awardTraining(state,captain.id,'threePoint',{tier:1,attempts:8,successes:7});
  const game=Rise.nextGame(state);
  assert.equal(Rise.recordGame(state,{gameId:game.id,won:true,homeScore:game.targetScore,awayScore:5,
    players:state.starters.map(id=>({id,stats:{points:6,assists:2,threeMade:2,threeAttempts:3}}))}).ok,true);
  return state;
}

test('portable JSON restores roster, assigned roles, abilities, upgrades and season progress',()=>{
  const original=progressedCampaign(),exported=Rise.exportSave(original);
  assert.equal(exported.ok,true);assert.match(exported.filename,/basketball2-portable-comets-season-1\.json$/);
  const envelope=JSON.parse(exported.serialized);
  assert.equal(envelope.format,Rise.SAVE_FORMAT);assert.equal(envelope.transferVersion,Rise.TRANSFER_VERSION);
  const inspected=Rise.inspectSave(exported.serialized);
  assert.equal(inspected.ok,true);assert.equal(inspected.summary.teamName,'Portable Comets');
  const target=storage(),result=Rise.importSave(exported.serialized,target),loaded=Rise.load(target);
  assert.equal(result.ok,true);assert.equal(result.backedUp,false);
  assert.deepEqual(loaded.roster,original.roster);assert.deepEqual(loaded.starters,original.starters);
  const captain=loaded.players[loaded.captainId],before=original.players[original.captainId];
  for(const field of ['ratings','abilities','role','priority','training','xp','level','upgradePoints','stats','equipment'])assert.deepEqual(captain[field],before[field],field);
  assert.deepEqual(loaded.season.completed,original.season.completed);
  assert.deepEqual(loaded.record,original.record);assert.equal(loaded.currency,original.currency);
  assert.equal(Rise.nextGame(loaded).id,Rise.nextGame(original).id);
});

test('inspection is read only, supports raw versioned Rise saves, and keeps other modes separate',()=>{
  const original=progressedCampaign(),raw=JSON.stringify(original),unchanged=JSON.stringify(original);
  assert.equal(Rise.inspectSave(raw).ok,true);assert.equal(JSON.stringify(original),unchanged);
  const old={...original,version:1};assert.equal(Rise.inspectSave(JSON.stringify(old)).ok,true);
  assert.equal(Rise.inspectSave(JSON.stringify({...original,mode:'national-tour'})).ok,false);
  assert.equal(Rise.inspectSave(JSON.stringify({...original,version:Rise.VERSION+1})).ok,false);
  assert.equal(Rise.inspectSave(JSON.stringify({...original,version:undefined})).ok,false);
});

test('a signed offseason prospect and championship history survive export after actual season advancement',()=>{
  const state=Rise.create({name:'Second Season Club'});
  for(let played=0;Rise.nextGame(state);played++){
    assert.ok(played<12);const game=Rise.nextGame(state);
    assert.equal(Rise.recordGame(state,{gameId:game.id,won:true,homeScore:game.targetScore,awayScore:4,players:[]}).ok,true);
  }
  assert.equal(Rise.advanceSeason(state).ok,true);
  const prospect=Rise.market(state).find(p=>p.id.startsWith('prospect-2-'));
  assert.ok(prospect);assert.equal(Rise.sign(state,prospect.id).ok,true);Rise.setRole(state,prospect.id,'anchor',2);
  Rise.setStarters(state,[state.starters[0],state.starters[1],prospect.id]);
  const result=Rise.importSave(Rise.exportSave(state).serialized,storage());assert.equal(result.ok,true);
  assert.equal(result.state.season.number,2);assert.equal(result.state.season.division,1);
  assert.deepEqual(result.state.history,state.history);assert.deepEqual(result.state.awards,Rise.normalize(state).awards);
  assert.equal(result.state.players[prospect.id].role,'anchor');assert.deepEqual(result.state.players[prospect.id].ratings,prospect.ratings);
  assert.deepEqual(result.state.starters,state.starters);
});

test('bad JSON, oversized files, unknown envelope versions and malformed rosters are rejected without writes',()=>{
  const original=progressedCampaign(),serialized=Rise.exportSave(original).serialized,envelope=JSON.parse(serialized),raw=JSON.parse(JSON.stringify(original));
  const variants=[null,'not json','{}','[]','x'.repeat(Rise.MAX_SAVE_BYTES+1),
    JSON.stringify({...envelope,transferVersion:999}),JSON.stringify({...envelope,format:'something-else'}),
    JSON.stringify({...raw,players:[]}),JSON.stringify({...raw,roster:[raw.roster[0],raw.roster[0],raw.roster[2]]}),
    JSON.stringify({...raw,starters:[]}),JSON.stringify({...raw,captainId:'someone-else'}),
    JSON.stringify({...raw,seed:'24'}),JSON.stringify({...raw,season:{...raw.season,number:Infinity}}),
    serialized.replace('"team": {','"team": {"__proto__":{"polluted":true},')];
  const broken=JSON.parse(JSON.stringify(raw));delete broken.players[broken.roster[0]].ratings.threePoint;variants.push(JSON.stringify(broken));
  const target=storage({[Rise.SAVE_KEY]:'current bytes',[Rise.BACKUP_KEY]:'backup bytes',basketballNationalTourV2:'tour bytes'}),before=[...target.data];
  for(const candidate of variants){assert.equal(Rise.importSave(candidate,target).ok,false,String(candidate).slice(0,80));assert.deepEqual([...target.data],before);}
  assert.equal({}.polluted,undefined);
});

test('valid imported progress uses the existing sanitizer for bounds, physical limits and text',()=>{
  const raw=progressedCampaign(),id=raw.captainId;
  raw.team.name='<b>Travel Club</b>';raw.players[id].ratings.threePoint=99999;raw.players[id].caps.threePoint=99999;
  raw.players[id].abilities.quickRelease=99;raw.players[id].abilities.notAnAbility=3;
  raw.players[id].physical={heightCm:500,weightKg:500};raw.players[id].role='invented';
  const checked=Rise.inspectSave(JSON.stringify(raw));assert.equal(checked.ok,true);
  assert.equal(checked.summary.teamName,'bTravel Club/b');assert.ok(checked.state.players[id].ratings.threePoint<=99);
  assert.ok(checked.state.players[id].physical.heightCm<230);assert.equal(checked.state.players[id].role,'balanced');
  assert.equal(checked.state.players[id].abilities.notAnAbility,undefined);assert.equal(checked.state.players[id].abilities.quickRelease,3);
});

test('successful replacement backs up exact previous save bytes and leaves Tour saves unchanged',()=>{
  const original=Rise.create({name:'Current Club'}),target=storage({basketballNationalTourV2:'tour bytes'});
  Rise.save(original,target);const previous=target.getItem(Rise.SAVE_KEY);
  const result=Rise.importSave(Rise.exportSave(progressedCampaign()).serialized,target);
  assert.equal(result.ok,true);assert.equal(result.backedUp,true);assert.equal(target.getItem(Rise.BACKUP_KEY),previous);
  assert.equal(Rise.load(target).team.name,'Portable Comets');assert.equal(target.getItem('basketballNationalTourV2'),'tour bytes');
});

test('failed replacement preserves both current save and backup after storage failure',()=>{
  const original=Rise.create({name:'Current Club'}),target=storage();Rise.save(original,target);
  target.setItem(Rise.BACKUP_KEY,'the older backup');const before=[...target.data],normalWrite=target.setItem;
  target.setItem=(key,value)=>{if(key===Rise.SAVE_KEY)throw new Error('quota');return normalWrite(key,value);};
  assert.equal(Rise.importSave(Rise.exportSave(progressedCampaign()).serialized,target).ok,false);
  assert.deepEqual([...target.data],before);assert.equal(Rise.load(target).team.name,'Current Club');
  target.setItem=(key,value)=>{if(key===Rise.BACKUP_KEY)throw new Error('denied');return normalWrite(key,value);};
  assert.equal(Rise.importSave(Rise.exportSave(progressedCampaign()).serialized,target).ok,false);
  assert.deepEqual([...target.data],before);
});
