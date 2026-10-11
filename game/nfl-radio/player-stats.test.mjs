import test from 'node:test';
import assert from 'node:assert/strict';
import {gamePlayerStats,fantasyBreakdown,statLeaders,stat,playStatFooter} from './player-stats.mjs';
const group=(name,labels,stats,id='rb')=>({name,labels,athletes:[{athlete:{id,displayName:id==='rb'?'Chase Brown':'Joe Burrow'},stats}]});
const fixture={boxscore:{players:[{team:{id:'4',abbreviation:'CIN'},statistics:[group('rushing',['CAR','YDS','TD'],['12','100','1']),group('receiving',['REC','YDS','TD','TGTS'],['5','50','1','6']),group('fumbles',['FUM','LOST'],['2','1']),group('passing',['C/ATT','YDS','TD','INT'],['20/30','250','2','1'],'qb')]}]}};
test('merge categories by athlete ID; include skill roster with no stats; omit defenders',()=>{
 const rows=gamePlayerStats(fixture,[{id:'rb',name:'Chase Brown',position:'RB',team:'CIN'},{id:'te',name:'Tight End',position:'TE'},{id:'s',name:'Safety',position:'S'}]);
 assert.equal(rows.length,3);assert.equal(rows.find(p=>p.id==='rb').position,'RB');assert.equal(rows.find(p=>p.id==='te').hasStats,false);assert.equal(stat(rows[0],'receiving','REC'),5);
});
test('PPR/standard totals include all offensive categories, interceptions and fumbles lost only',()=>{
 const rows=gamePlayerStats(fixture),rb=rows.find(p=>p.id==='rb'),qb=rows.find(p=>p.id==='qb');
 assert.equal(fantasyBreakdown(rb,'ppr').total,30);assert.equal(fantasyBreakdown(rb,'standard').total,25);assert.equal(fantasyBreakdown(qb).total,16);
});
test('corrections replace totals; labels take precedence over mismatched keys; negative yards and missing values',()=>{
 const x=structuredClone(fixture);x.boxscore.players[0].statistics[0].keys=['wrong','wrong2','wrong3','extra'];x.boxscore.players[0].statistics[0].athletes[0].stats=['12','-5','0'];
 const p=gamePlayerStats(x)[0];assert.equal(stat(p,'rushing','YDS'),-5);assert.equal(stat(p,'rushing','TGTS'),null);assert.equal(statLeaders([p],'rushing')[0],p);assert.deepEqual(statLeaders([p],'passing'),[]);
});
test('leaders retain ties and footers show the runner/receiver latest totals',()=>{
 const rows=gamePlayerStats(fixture);assert.equal(statLeaders([...rows,{...rows[0],id:'tied'}],'rushing').length,2);
 assert.match(playStatFooter({kind:'run',text:'First and 10. Chase Brown runs right for 10 yards.',playerIds:['rb']},rows),/100 rushing yd/);
 assert.match(playStatFooter({kind:'pass',text:'Joe Burrow finds Chase Brown for 9 yards.',playerIds:['qb','rb']},rows),/5 rec · 50 receiving yd/);
 assert.equal(playStatFooter({kind:'run',text:'Unknown runs for 4 yards.',playerIds:[]},rows),'');
});
