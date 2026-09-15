import test from 'node:test';
import assert from 'node:assert/strict';
import {
  PLAY_POLL_MS,
  DEFAULT_SCORE_INTERVAL_MINUTES,
  latestPlayAnnouncement,
  collectNewPlayAnnouncements,
  readPlayByPlaySettings,
  selectedGameIds,
  radioLabelMatchup,
  matchupMatchesRadioLabel,
  eventMatchesRadioLabel,
  normalizeScoreInterval,
  scoreLine,
  buildScoreUpdateSpeech
} from './play-by-play.mjs';

const makeEvent=({
  id='g1',
  playId='p1',
  text='J. Goff pass complete to A. St. Brown for 12 yards.',
  playTeam='8',
  possession='8',
  state='in',
  homeScore='17',
  awayScore='10',
  shortDetail=''
}={})=>({
  id,
  status:{type:{state,shortDetail}},
  competitions:[{
    status:{type:{state,shortDetail}},
    competitors:[
      {id:'8',homeAway:'home',score:homeScore,team:{id:'8',abbreviation:'DET',shortDisplayName:'Lions'}},
      {id:'9',homeAway:'away',score:awayScore,team:{id:'9',abbreviation:'GB',shortDisplayName:'Packers'}}
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
  assert.deepEqual(out.matchup,['GB','DET']);
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
  const finalEvent=makeEvent({state:'post'});
  assert.deepEqual(collectNewPlayAnnouncements([finalEvent],['g1'],seen,{announceInitial:true}),[]);
});

test('recognizes the matchup currently shown in the radio player',()=>{
  const event=makeEvent();
  assert.deepEqual(radioLabelMatchup('GB vs DET · LIVE'),['GB','DET']);
  assert.equal(matchupMatchesRadioLabel(['GB','DET'],'GB vs DET · LIVE'),true);
  assert.equal(matchupMatchesRadioLabel(['GB','DET'],'TB vs CIN · LIVE'),false);
  assert.equal(eventMatchesRadioLabel(event,'GB vs DET · LIVE'),true);
});

test('uses a fixed five-second poll and persists radio ducking plus score settings',()=>{
  assert.equal(PLAY_POLL_MS,5000);
  const storage={getItem:key=>key.endsWith('livePlayByPlay')
    ?'{"enabled":true,"duckRadio":false,"scoreIntervalMinutes":10,"scoreScope":"all"}'
    :'["g1","g2",3]'};
  const settings=readPlayByPlaySettings(storage);
  assert.equal(settings.enabled,true);
  assert.equal(settings.duckRadio,false);
  assert.equal(settings.scoreIntervalMinutes,10);
  assert.equal(settings.scoreScope,'all');
  assert.deepEqual(selectedGameIds(storage),['g1','g2']);
});

test('older saved settings get score updates every five minutes for rotation games',()=>{
  const storage={getItem:()=>'{"enabled":true}'};
  const settings=readPlayByPlaySettings(storage);
  assert.equal(settings.duckRadio,true);
  assert.equal(settings.scoreIntervalMinutes,DEFAULT_SCORE_INTERVAL_MINUTES);
  assert.equal(settings.scoreScope,'rotation');
});

test('score interval accepts off and clamps excessive values',()=>{
  assert.equal(normalizeScoreInterval(0),0);
  assert.equal(normalizeScoreInterval('15'),15);
  assert.equal(normalizeScoreInterval(999),120);
  assert.equal(normalizeScoreInterval('nope'),DEFAULT_SCORE_INTERVAL_MINUTES);
});

test('builds live, halftime, and final score lines',()=>{
  assert.equal(scoreLine(makeEvent()),'Packers 10, Lions 17.');
  assert.equal(scoreLine(makeEvent({shortDetail:'Halftime'})),'Halftime: Packers 10, Lions 17.');
  assert.equal(scoreLine(makeEvent({state:'post',homeScore:'31',awayScore:'24'})),'Final: Packers 24, Lions 31.');
  assert.equal(scoreLine(makeEvent({state:'pre'})),'');
});

test('score summary can target rotation games or every started game',()=>{
  const rotation=makeEvent({id:'g1'});
  const other=makeEvent({id:'g2',homeScore:'7',awayScore:'14'});
  const upcoming=makeEvent({id:'g3',state:'pre'});
  assert.equal(
    buildScoreUpdateSpeech([rotation,other,upcoming],{selectedIds:['g1'],scope:'rotation'}),
    'NFL score update. Packers 10, Lions 17.'
  );
  assert.equal(
    buildScoreUpdateSpeech([rotation,other,upcoming],{selectedIds:['g1'],scope:'all'}),
    'NFL score update. Packers 10, Lions 17. Packers 14, Lions 7.'
  );
});
