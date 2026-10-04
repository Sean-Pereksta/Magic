import test from 'node:test';
import assert from 'node:assert/strict';
import {createPlayTracker,playsFromSummary,playIdentity,TRANSCRIPT_LIMIT} from './play-tracker.mjs';
import {readPlayByPlaySettings,settingsForGame} from './play-settings.mjs';
const event={id:'g1',status:{type:{state:'in'}},competitions:[{competitors:[]}]};
const play=(id,text=`Runner rushes for ${id} yards.`)=>({id:String(id),sequenceNumber:String(id),text});
test('initial history is silent; every play between polls is delivered in order',()=>{
 const tracker=createPlayTracker();
 assert.equal(tracker.ingest(event,[play(1),play(2)],{complete:true}).announcements.length,0);
 const result=tracker.ingest(event,[play(1),play(2),play(3),play(4)],{complete:true});
 assert.deepEqual(result.announcements.map(a=>a.key),['id:3','id:4']);
 assert.ok(result.announcements.every(a=>a.text===a.speech&&a.text===a.play));
});
test('duplicate IDs, old snapshots, and ESPN text corrections never speak twice',()=>{
 const tracker=createPlayTracker();tracker.ingest(event,[play(1)],{complete:true});
 assert.equal(tracker.ingest(event,[play(2)],{complete:true}).announcements.length,1);
 assert.equal(tracker.ingest(event,[play(1)],{complete:true}).announcements.length,0);
 assert.equal(tracker.ingest(event,[play(2,'Runner rushes for 4 yards. Corrected spot.')],{complete:true}).announcements.length,0);
 assert.match(tracker.history('g1').find(p=>p.key==='id:2').text,/Corrected spot/);
 assert.equal(playIdentity(play(2)),playIdentity(play(2,'Corrected text')));
});
test('duplicate drive/current/scoreboard entries merge into one detailed play',()=>{
 const data={drives:{previous:[{plays:[{...play(1),start:{down:1,distance:10}}]}],current:{plays:[play(1)]}},scoringPlays:[play(1,'Short score summary')]};
 const plays=playsFromSummary(data,play(1));assert.equal(plays.length,1);assert.equal(plays[0].start.down,1);
});
test('first successful history after a failed summary primes past plays and catches new ones',()=>{
 const tracker=createPlayTracker();tracker.ingest(event,[play(5)]);
 const result=tracker.ingest(event,[play(1),play(2),play(3),play(4),play(5),play(6),play(7)],{complete:true});
 assert.deepEqual(result.announcements.map(p=>p.key),['id:6','id:7']);
});
test('touchdown filter cannot replay older skipped plays when mode changes',()=>{
 const tracker=createPlayTracker();tracker.ingest(event,[play(1)],{complete:true});
 const batch=[play(1),play(2),play(3,'Runner for 3 yards, TOUCHDOWN.')];
 assert.deepEqual(tracker.ingest(event,batch,{complete:true,settings:{mode:'touchdowns'}}).announcements.map(p=>p.key),['id:3']);
 assert.equal(tracker.ingest(event,batch,{complete:true,settings:{mode:'every'}}).announcements.length,0);
});
test('selected-player filtering is isolated per monitored game',()=>{
 const storage={getItem:()=>JSON.stringify({enabled:true,mode:'players',selectedPlayersByGame:{g1:[{id:'runner',name:'Some Runner',team:'DET'}],g2:[{id:'other',name:'Other Runner',team:'GB'}]}})};
 const settings=readPlayByPlaySettings(storage),tracker=createPlayTracker();
 const baseline=play(1);tracker.ingest(event,[baseline]);tracker.ingest({...event,id:'g2'},[baseline]);
 const next={...play(2),participants:[{athlete:{id:'runner'}}]};
 assert.equal(tracker.ingest(event,[baseline,next],{settings:settingsForGame(settings,'g1')}).announcements.length,1);
 assert.equal(tracker.ingest({...event,id:'g2'},[baseline,next],{settings:settingsForGame(settings,'g2')}).announcements.length,0);
});
test('transcripts stay bounded while deduplication outlives visible history',()=>{
 const tracker=createPlayTracker();const initial=Array.from({length:TRANSCRIPT_LIMIT+30},(_,i)=>play(i+1));
 tracker.ingest(event,initial,{complete:true});assert.equal(tracker.history('g1').length,TRANSCRIPT_LIMIT);
 assert.equal(tracker.ingest(event,[play(1)],{complete:true}).announcements.length,0);
 tracker.retain([]);assert.deepEqual(tracker.history('g1'),[]);
});
test('settings migrate old on/off values and reject malformed player preferences',()=>{
 assert.equal(readPlayByPlaySettings({getItem:()=>'bad json'}).enabled,false);
 const old=readPlayByPlaySettings({getItem:()=>' {"enabled":true} '});assert.equal(old.mode,'every');assert.equal(old.enabled,true);
 const current=readPlayByPlaySettings({getItem:()=>JSON.stringify({mode:'players',selectedPlayersByGame:{g1:[null,{}, {id:'p',name:'Player'}]}})});
 assert.deepEqual(settingsForGame(current,'g1').selectedPlayerIds,['p']);
});
