import test from 'node:test';
import assert from 'node:assert/strict';
globalThis.location={search:'?league=college'};
const {parseSchedule,team,resolveVoice}=await import('./core.mjs');
const {API,SCOREBOARD_URL,storagePrefix,isTop25}=await import('./league.mjs');
const {selectedGameIds}=await import('./play-settings.mjs');
const {collegeStationSearches}=await import('./college-stations.mjs');
const {discoverTeamStreams}=await import('./station-discovery.mjs');
const competitor=(id,location,abbreviation,homeAway,rank)=>({homeAway,score:'21',curatedRank:{current:rank},team:{id,location,abbreviation,name:'Tigers',displayName:location+' Tigers'}});
const event=(id,rankA,rankB)=>({id,date:'2026-10-10T16:00:00Z',status:{type:{state:'in'}},competitions:[{competitors:[competitor('99','Test College','TC','home',rankA),competitor('100','Other College','OC','away',rankB)]}]});
test('college scoreboard includes unranked teams, ranks either side, and preserves identity',()=>{
 const games=parseSchedule({events:[event('a',99,25),event('b',1,2),event('c',99,99),event('d',undefined,undefined)]});
 assert.equal(games.length,4);assert.deepEqual(games.filter(isTop25).map(g=>g.id),['a','b']);
 assert.equal(team(games[0].home).name,'Test College Tigers');assert.equal(games[0].home,'college:99');
 assert.equal(resolveVoice('Test College',games).kind,'game');
 assert.match(API,/college-football$/);assert.match(SCOREBOARD_URL,/groups=80&limit=1000/);
 assert.equal(storagePrefix,'college-dial');
 assert.deepEqual(selectedGameIds({getItem:key=>key==='college-dial:rotation'?'["a"]':'["nfl"]'}),['a']);
});
test('rank sentinel, zero, negatives and missing values never count as ranked',()=>{
 for(const rank of [0,-1,26,99,undefined,NaN])assert.equal(isTop25({homeRank:rank}),false);
});
test('college station searches use mapped school and can discover public direct streams',async()=>{
 const searches=collegeStationSearches({city:'Ohio State'});assert.ok(searches.includes('WBNS'));
 const urls=[];
 const feeds=await discoverTeamStreams('college:194',{searches,fetchImpl:async url=>{urls.push(url);return {ok:true,json:async()=>[{name:'WBNS',stationuuid:'s',url_resolved:'https://example.com/audio',lastcheckok:1,codec:'MP3'}]};}});
 assert.equal(feeds.length,1);assert.equal(feeds[0].team,'college:194');assert.equal(feeds[0].gameAudio,false);assert.equal(urls.length,searches.length);
 assert.deepEqual(collegeStationSearches({city:'Unmapped',name:'Unmapped Bears'}),['Unmapped Bears','Unmapped sports']);
});
