import test from 'node:test';
import assert from 'node:assert/strict';
import {fieldPosition,fieldModel} from './field-view.mjs';
const competitors=[{homeAway:'home',team:{id:'h',abbreviation:'HOME'}},{homeAway:'away',team:{id:'a',abbreviation:'AWAY'}}];
const point=(team,yards)=>({team:{id:team},yardsToEndzone:yards});
const event=situation=>({status:{type:{state:'in'}},competitions:[{competitors,situation}]});
test('field coordinates stay consistent for both offenses and possession changes',()=>{
 assert.equal(fieldPosition(point('h',70),competitors),30);
 assert.equal(fieldPosition(point('a',30),competitors),30);
 assert.equal(fieldPosition({possessionText:'HOME 22'},competitors),22);
 assert.equal(fieldPosition({possessionText:'AWAY 22'},competitors),78);
 assert.equal(fieldPosition({yardLine:44},competitors),null,'unknown coordinate convention is not guessed');
 assert.equal(fieldPosition({yardsToEndzone:null,team:{id:'h'}},competitors),null);
});
test('completion connects start and end, current spot and first down update from scoreboard',()=>{
 const m=fieldModel(event({possession:'h',possessionText:'HOME 44',distance:10,shortDownDistanceText:'1st & 10'}),{raw:{type:{text:'Pass Reception'},start:point('h',70),end:point('h',56)}});
 assert.deepEqual(m.route,{start:30,end:44,kind:'pass'});assert.equal(m.ball,44);assert.equal(m.target,54);assert.equal(m.direction,1);
});
test('runs, losses, incompletions, penalties and turnovers never invent completed routes',()=>{
 const raw={type:{text:'Rush'},start:point('a',40),end:point('a',44)};
 const m=fieldModel(event(null),{raw});assert.deepEqual(m.route,{start:40,end:44,kind:'run'});assert.equal(m.ball,44);
 for(const text of ['Pass Incompletion','Pass Interception Return','Penalty'])assert.equal(fieldModel(event(null),{raw:{...raw,type:{text}}}).route,null);
 const flipped=fieldModel(event({possession:'h',possessionText:'HOME 44'}),{raw:{...raw,isTurnover:true,end:point('h',56)}});
 assert.equal(flipped.ball,44);assert.equal(flipped.direction,1);assert.equal(flipped.route,null);
});
test('pregame and missing positions show no invented ball; final retains last spot',()=>{
 assert.equal(fieldModel({competitions:[{competitors}],status:{type:{state:'pre'}}},{raw:{end:point('h',50)}}).ball,null);
 assert.equal(fieldModel(event(null)).ball,null);
 assert.equal(fieldModel({...event(null),status:{type:{state:'post'}}},{raw:{end:point('h',0)}}).ball,100);
});
