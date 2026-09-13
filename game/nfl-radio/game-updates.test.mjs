import test from 'node:test';
import assert from 'node:assert/strict';
import {buildGameUpdate,readSettings,selectedGameIds} from './game-updates.mjs';

const event={
 id:'g1',
 status:{type:{state:'in',shortDetail:'12:47 - 3rd'}},
 competitions:[{
  status:{type:{state:'in',shortDetail:'12:47 - 3rd'}},
  competitors:[
   {id:'21',homeAway:'home',score:'14',team:{abbreviation:'PHI',shortDisplayName:'Eagles'}},
   {id:'28',homeAway:'away',score:'9',team:{abbreviation:'WSH',shortDisplayName:'Commanders'}}
  ],
  situation:{possession:'28',downDistanceText:'2nd & 6 at WSH 45',lastPlay:{text:'Runner up the middle for 4 yards.',scoreValue:0}},
  leaders:[
   {name:'passingYards',leaders:[{value:122,athlete:{displayName:'Jalen Hurts'}}]},
   {name:'rushingYards',leaders:[{value:63,athlete:{displayName:'Saquon Barkley'}}]},
   {name:'receivingYards',leaders:[{value:73,athlete:{displayName:'Dontayvion Wicks'}}]}
  ]
 }]
};
const summary={scoringPlays:[
 {id:'s1',text:'Kicker makes a 42 yard field goal.'},
 {id:'s2',text:'Quarterback pass to Receiver for a touchdown.'}
]};

test('builds score, possession, field position, last play and leaders',()=>{
 const out=buildGameUpdate(event,summary,{seenScoreIds:new Set(['s1'])});
 assert.match(out.speech,/Commanders 9, Eagles 14/);
 assert.match(out.speech,/Commanders has the ball, 2nd and 6 at Commanders 45/);
 assert.match(out.speech,/Last play/);
 assert.match(out.speech,/Recent scoring: Quarterback pass/);
 assert.match(out.speech,/Passing leader Jalen Hurts, 122 yards/);
 assert.match(out.speech,/Rushing leader Saquon Barkley, 63 yards/);
 assert.match(out.speech,/Receiving leader Dontayvion Wicks, 73 yards/);
 assert.deepEqual(out.scoreIds,['s1','s2']);
});

test('can suppress optional spoken detail',()=>{
 const out=buildGameUpdate(event,summary,{scoring:false,lastPlay:false,leaders:false});
 assert.doesNotMatch(out.speech,/Recent scoring|Last play|leader/);
 assert.match(out.speech,/Commanders has the ball/);
});

test('settings enforce supported intervals and rotation parsing',()=>{
 const storage={getItem:key=>key.endsWith('spokenUpdates')?'{"enabled":true,"minutes":7}':'["g1","g2",4]'};
 assert.equal(readSettings(storage).minutes,1);
 assert.equal(readSettings(storage).enabled,true);
 assert.deepEqual(selectedGameIds(storage),['g1','g2']);
});
