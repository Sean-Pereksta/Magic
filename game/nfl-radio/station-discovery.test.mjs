import test from 'node:test';
import assert from 'node:assert/strict';
import {discoverTeamStreams,isUsablePublicStream,mergeInAppQueues,TEAM_STATION_SEARCHES} from './station-discovery.mjs';

test('all 32 NFL teams have direct-stream search aliases',()=>{
 assert.equal(Object.keys(TEAM_STATION_SEARCHES).length,32);
 for(const [team,queries] of Object.entries(TEAM_STATION_SEARCHES)){
  assert.ok(queries.length>0,`${team} needs at least one search`);
 }
});

test('usable public streams must be healthy HTTPS audio',()=>{
 const base={lastcheckok:1,ssl_error:0,url_resolved:'https://radio.example/live.mp3',codec:'MP3'};
 assert.equal(isUsablePublicStream(base),true);
 assert.equal(isUsablePublicStream({...base,lastcheckok:0}),false);
 assert.equal(isUsablePublicStream({...base,ssl_error:1}),false);
 assert.equal(isUsablePublicStream({...base,url_resolved:'http://radio.example/live.mp3'}),false);
 assert.equal(isUsablePublicStream({...base,codec:'UNKNOWN'}),false);
});

test('discovery searches every alias, filters broken streams, and deduplicates resolved URLs',async()=>{
 const calls=[];
 const fetchImpl=async url=>{
  calls.push(String(url));
  const parsed=new URL(url);const q=parsed.searchParams.get('name');
  return {ok:true,json:async()=>[
   {stationuuid:`${q}-1`,name:q,url_resolved:'https://radio.example/shared.mp3',lastcheckok:1,ssl_error:0,codec:'MP3',bitrate:128,votes:100},
   {stationuuid:`${q}-2`,name:`${q} broken`,url_resolved:'https://radio.example/broken.mp3',lastcheckok:0,ssl_error:0,codec:'MP3',bitrate:128,votes:999}
  ]};
 };
 const feeds=await discoverTeamStreams('DET',{fetchImpl});
 assert.equal(calls.length,TEAM_STATION_SEARCHES.DET.length);
 assert.equal(feeds.length,1);
 assert.equal(feeds[0].streamUrl,'https://radio.example/shared.mp3');
 assert.equal(feeds[0].access,'public-direct');
 assert.equal(feeds[0].gameAudio,false);
});

test('mergeInAppQueues preserves priority while removing duplicate stream URLs',()=>{
 const a={id:'a',streamUrl:'https://a.example/live'};
 const duplicate={id:'b',streamUrl:'https://a.example/live'};
 const c={id:'c',streamUrl:'https://c.example/live'};
 assert.deepEqual(mergeInAppQueues([a],[duplicate,c]).map(x=>x.id),['a','c']);
});
