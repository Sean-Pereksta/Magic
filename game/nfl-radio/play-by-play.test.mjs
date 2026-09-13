import test from 'node:test';
import assert from 'node:assert/strict';
import {PLAY_POLL_MS,latestPlayAnnouncement,collectNewPlayAnnouncements,readPlayByPlaySettings,selectedGameIds} from './play-by-play.mjs';

const makeEvent=({id='g1',playId='p1',text='J. Goff pass complete to A. St. Brown for 12 yards.',playTeam='8',possession='8'}={})=>({
  id,
  status:{type:{state:'in'}},
  competitions:[{
    status:{type:{state:'in'}},
    competitors:[
      {id:'8',homeAway:'home',team:{id:'8',abbreviation:'DET',shortDisplayName:'Lions'}},
      {id:'9',homeAway:'away',team:{id:'9',abbreviation:'GB',shortDisplayName:'Packers'}}
    ],
    situation:{
      possession,
      lastPlay:{id:playId,text,team:playTeam?{id:playTeam}:undefined,period:{number:2},clock:{displayValue:'8:41'}}
    }
  }]
});

test('builds a concise team plus play announcement',()=>{
  const out=latestPlayAnnouncement(makeEvent());
  assert.equal(out.team,'Lions');
  assert.match(out.speech,/^Lions\. J\. Goff pass complete/);
  assert.equal(out.gameId,'g1');
});

test('falls back to possession when the play has no explicit team',()=>{
  const out=latestPlayAnnouncement(makeEvent({playTeam:null,possession:'9',text:'Rush for 6 yards.'}));
  assert.equal(out.team,'Packers');
  assert.equal(out.speech,'Packers. Rush for 6 yards.');
});

test('primes without speaking, then queues only a changed play',()=>{
  const seen=new Map();
  const ids=['g1'];
  assert.deepEqual(collectNewPlayAnnouncements([makeEvent()],ids,seen),[]);
  assert.deepEqual(collectNewPlayAnnouncements([makeEvent()],ids,seen),[]);
  const next=collectNewPlayAnnouncements([makeEvent({playId:'p2',text:'J. Gibbs left tackle for 9 yards.'})],ids,seen);
  assert.equal(next.length,1);
  assert.match(next[0].speech,/Lions\. J\. Gibbs/);
});

test('ignores unselected and non-live games',()=>{
  const seen=new Map();
  const event=makeEvent();
  assert.deepEqual(collectNewPlayAnnouncements([event],['other'],seen,{announceInitial:true}),[]);
  const finalEvent=makeEvent();
  finalEvent.competitions[0].status.type.state='post';
  finalEvent.status.type.state='post';
  assert.deepEqual(collectNewPlayAnnouncements([finalEvent],['g1'],seen,{announceInitial:true}),[]);
});

test('uses a fixed five-second poll and safe persisted settings',()=>{
  assert.equal(PLAY_POLL_MS,5000);
  const storage={getItem:key=>key.endsWith('livePlayByPlay')?'{"enabled":true}':'["g1","g2",3]'};
  assert.equal(readPlayByPlaySettings(storage).enabled,true);
  assert.deepEqual(selectedGameIds(storage),['g1','g2']);
});
