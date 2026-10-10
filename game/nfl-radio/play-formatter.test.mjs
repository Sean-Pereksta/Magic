import test from 'node:test';
import assert from 'node:assert/strict';
import {formatPlay,normalizePlayer,playInvolvesSelectedPlayer,shouldAnnouncePlay,rosterPlayers,summaryPlayers} from './play-formatter.mjs';
const players=[
 ['goff','Jared','Goff','QB','8'],['gibbs','Jahmyr','Gibbs','RB','8'],['brown','Amon-Ra','St. Brown','WR','8'],
 ['laporta','Sam','LaPorta','TE','8'],['bates','Jake','Bates','K','8'],['fox','Jack','Fox','P','8'],
 ['mckinney','Xavier','McKinney','S','9'],['gary','Rashan','Gary','DE','9'],['nixon','Keisean','Nixon','CB','9']
].map(([id,firstName,lastName,position,team])=>normalizePlayer({id,firstName,lastName,position,shortName:`${firstName[0]}. ${lastName}`,team:{id:team}}));
const event={id:'g1',competitions:[{competitors:[{id:'8',team:{id:'8',abbreviation:'DET',location:'Detroit',shortDisplayName:'Lions'}},{id:'9',team:{id:'9',abbreviation:'GB',location:'Green Bay',shortDisplayName:'Packers'}}]}]};
const format=(text,extra={})=>formatPlay({text,...extra},{event,players});
test('normal pass expands ESPN names and keeps receiver, direction, and yards',()=>{
 assert.equal(format('J.Goff pass short middle to A.St. Brown to DET 42 for 14 yards.').text,'Jared Goff finds Amon-Ra St. Brown over the middle for 14 yards.');
});
test('runs, losses, no gain, and first downs remain short and accurate',()=>{
 assert.equal(format('J.Gibbs left tackle to DET 42 for 8 yards.').text,'Jahmyr Gibbs runs left for 8 yards.');
 assert.equal(format('J.Gibbs up the middle for -2 yards.').text,'Jahmyr Gibbs runs up the middle for a loss of 2 yards.');
 assert.equal(format('J.Gibbs right guard for no gain.').text,'Jahmyr Gibbs runs right for no gain.');
 const out=format('J.Goff pass short right to S.LaPorta to DET 42 for 11 yards.',{type:{text:'Pass Reception'},start:{down:3,distance:2,shortDownDistanceText:'3rd & 2',team:{id:'8'}},end:{down:1,team:{id:'8'}}});
 assert.equal(out.text,'Third and 2. Jared Goff finds Sam LaPorta to the right for 11 yards and a first down.');
 assert.equal(out.label,'3rd & 2 — DET');
});
test('touchdown is emphatic without invented route or score information',()=>{
 const out=format('J.Goff pass short middle to A.St. Brown for 22 yards, TOUCHDOWN.',{team:{id:'8'},scoreValue:6});
 assert.equal(out.text,'Jared Goff finds Amon-Ra St. Brown over the middle for 22 yards — touchdown Detroit!');
 assert.equal(out.label,'TOUCHDOWN — DET');assert.equal(out.important,true);
});
test('interception preserves intended receiver, defender, and return',()=>{
 const out=format('J.Goff pass intended for A.St. Brown INTERCEPTED by X.McKinney at GB 20. X.McKinney to GB 35 for 15 yards.',{type:{text:'Pass Interception Return'},isTurnover:true});
 assert.match(out.text,/Interception!/);assert.match(out.text,/Amon-Ra St. Brown/);assert.match(out.text,/Xavier McKinney/);assert.match(out.text,/15 yards/);assert.equal(out.turnover,true);
});
test('sack preserves defender and loss',()=>{
 const out=format('J.Goff sacked at DET 25 for -7 yards (R.Gary).',{type:{text:'Sack'}});
 assert.equal(out.text,'Jared Goff is sacked by Rashan Gary for a loss of 7 yards.');assert.equal(out.important,true);
});
test('made, missed, and blocked field goals retain the actual outcome',()=>{
 assert.equal(format('J.Bates 43 yard field goal is GOOD, Center-S.Daly, Holder-J.Fox.').text,'Jake Bates makes a 43-yard field goal.');
 assert.match(format('J.Bates 43 yard field goal is NO GOOD, Wide Left.').text,/no good, Wide Left/i);
 assert.match(format('J.Bates 43 yard field goal BLOCKED by R.Gary.').text,/blocked by Rashan Gary/i);
 assert.equal(format('J.Bates 43 yard field goal is GOOD.').touchdown,false);
});
test('punts, penalties, and fumbles preserve their result and participants',()=>{
 assert.match(format('J.Fox punts 48 yards to GB 10. K.Nixon to GB 22 for 12 yards.').text,/Jack Fox punts 48 yards/);
 const penalty=format('J.Gibbs left tackle for 8 yards. PENALTY on DET, Offensive Holding, 10 yards, No Play.');
 assert.match(penalty.text,/No Play/);assert.match(penalty.text,/Holding, 10 yards/);assert.equal(penalty.kind,'penalty');
 const recovered=format('J.Gibbs FUMBLES. RECOVERED by DET-J.Goff at DET 30.',{start:{team:{id:'8'}},end:{team:{id:'8'}}});
 assert.equal(recovered.turnover,false);assert.match(recovered.text,/recovered by Lions-Jared Goff/);
 const lost=format('J.Gibbs FUMBLES. RECOVERED by GB-X.McKinney at DET 30.',{isTurnover:true});
 assert.equal(lost.turnover,true);assert.match(lost.text,/Xavier McKinney/);
});
test('no-play and overturned scores never pass touchdown-only filtering',()=>{
 for(const text of ['J.Gibbs for 10 yards, TOUCHDOWN. Penalty, No Play.','Touchdown overturned. Runner out at the 1.','No touchdown. Incomplete pass.'])assert.equal(shouldAnnouncePlay({text,scoreValue:6},{mode:'touchdowns'}),false);
 assert.equal(shouldAnnouncePlay({text:'Touchdown Detroit!',scoreValue:6},{mode:'touchdowns'}),true);
 assert.equal(shouldAnnouncePlay({text:'J.Bates 43 yard field goal is GOOD.',scoreValue:3},{mode:'touchdowns'}),false);
});
test('selected players are detected in every role by ID or full, compact, spaced names',()=>{
 for(const [id,text] of [['goff','J.Goff pass to A.St. Brown.'],['brown','J. Goff pass to A. St. Brown.'],['gibbs','J.Gibbs runs left.'],['gary','J.Goff sacked by R.Gary.'],['mckinney','INTERCEPTED by X.McKinney.'],['nixon','K.Nixon returns for 12 yards.'],['bates','J.Bates field goal.'],['fox','J.Fox punts.']]){
  assert.equal(playInvolvesSelectedPlayer({text},[id],players),true,id);
  assert.equal(shouldAnnouncePlay({text},{mode:'players',selectedPlayerIds:[id]},{players}),true,id);
 }
 assert.equal(playInvolvesSelectedPlayer({text:'Tackle.',participants:[{athlete:{id:'gary'},type:'tackler'}]},['gary']),true);
 assert.equal(playInvolvesSelectedPlayer({text:'Rush.',athletesInvolved:[{id:'gibbs'}]},['gibbs']),true);
 assert.equal(shouldAnnouncePlay({text:'J.Goff passes.'},{mode:'players',selectedPlayerIds:[]},{players}),false);
 assert.equal(playInvolvesSelectedPlayer({text:'J.Goff passes.'},['bates'],players),false);
});
test('ambiguous initial/surname combinations never guess an athlete',()=>{
 const roster=[normalizePlayer({id:'1',displayName:'John Smith'}),normalizePlayer({id:'2',displayName:'James Smith'})];
 assert.equal(playInvolvesSelectedPlayer({text:'J.Smith tackles.'},['1'],roster),false);
 assert.equal(formatPlay({text:'J.Smith tackles.'},{players:roster}).text,'J. Smith tackles.');
 assert.equal(playInvolvesSelectedPlayer({text:'J.Smith tackles.',participants:[{athlete:{id:'2'}}]},['2'],roster),true);
 assert.equal(playInvolvesSelectedPlayer({text:'J.Smith tackles.',participants:[{athlete:{id:'2'}}]},['1'],roster),false);
});
test('ESPN grouped rosters and boxscore athletes supply both teams and positions',()=>{
 const roster=rosterPlayers({team:{id:'8',abbreviation:'DET'},athletes:[{position:'offense',items:[{id:'goff',displayName:'Jared Goff',position:{abbreviation:'QB'}}]}]});
 assert.equal(roster[0].team,'DET');assert.equal(roster[0].position,'QB');
 const summary=summaryPlayers({boxscore:{players:[{team:{id:'9',abbreviation:'GB'},statistics:[{athletes:[{athlete:{id:'gary',displayName:'Rashan Gary'}}]}]}]}});
 assert.equal(summary[0].teamId,'9');
});
test('unknown and complicated plays keep ESPN facts instead of embellishing',()=>{
 const out=format('J.Goff pass deep right to A.St. Brown for 35 yards. Lateral to J.Gibbs for 5 yards.');
 assert.match(out.text,/Lateral to Jahmyr Gibbs for 5 yards/);
 assert.doesNotMatch(out.text,/drops back|dives|blazing|wide open/);
 const anonymous=formatPlay({text:'Q.Unknown runs for 3 yards.'});assert.match(anonymous.text,/Q\. Unknown/);
});
test('defensive touchdowns use the scoring team, never the next possession',()=>{
 const out=format('J.Goff pass INTERCEPTED by X.McKinney. X.McKinney for 20 yards, TOUCHDOWN.',{start:{team:{id:'8'}},end:{team:{id:'9'}},isTurnover:true});
 assert.equal(out.label,'TOUCHDOWN — GB');
});

test('incomplete passes keep depth, side, intended receiver, and defender',()=>{
 const out=format('J.Goff pass incomplete short right to A.St. Brown (X.McKinney).');
 assert.equal(out.text,'Jared Goff’s short pass intended for Amon-Ra St. Brown to the right is incomplete. Defended by Xavier McKinney.');
 assert.equal(out.touchdown,false);
});
test('ESPN short scoring summaries become calls and retain conversion outcome',()=>{
 const out=format('Amon-Ra St. Brown 22 Yd pass from Jared Goff (Jake Bates Kick)',{type:{text:'Passing Touchdown'},team:{id:'8'}});
 assert.equal(out.text,'Jared Goff finds Amon-Ra St. Brown for 22 yards — touchdown Detroit! Extra point by Jake Bates!');
 const run=format('Jahmyr Gibbs 8 Yd Rush (Kick Failed)',{scoringType:{name:'touchdown'},team:{id:'8'}});
 assert.match(run.text,/Jahmyr Gibbs runs for 8 yards/);assert.match(run.text,/Kick Failed/);
});
test('suffixes in full ESPN names do not prevent abbreviated player matching',()=>{
 const roster=[normalizePlayer({id:'cook',displayName:'James Cook III',firstName:'James',lastName:'Cook III'})];
 assert.equal(playInvolvesSelectedPlayer({text:'J.Cook left end for 4 yards.'},['cook'],roster),true);
 assert.equal(formatPlay({text:'J.Cook left end for 4 yards.'},{players:roster}).text,'James Cook III runs left for 4 yards.');
});

test('spoken calls lead with the pre-play down and distance, including goal to go',()=>{
 const play={text:'J.Goff pass short middle to A.St. Brown for 14 yards.',type:{text:'Pass Reception'},start:{down:2,distance:10},end:{down:1,distance:10}};
 assert.match(formatPlay(play,{event,players}).text,/^Second and 10\. Jared Goff/);
 assert.match(formatPlay({...play,start:{down:1,distance:3,shortDownDistanceText:'1st & Goal'}},{event,players}).text,/^First and goal\./);
 assert.match(formatPlay({...play,start:{shortDownDistanceText:'4th & 2'}},{event,players}).text,/^Fourth and 2\./);
 assert.doesNotMatch(formatPlay({...play,start:{}},{event,players}).text,/^(First|Second|Third|Fourth) and/);
});
