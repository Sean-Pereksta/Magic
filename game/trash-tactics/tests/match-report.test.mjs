import test from 'node:test';
import assert from 'node:assert/strict';
import {normalizeMatchReport,matchReportMetrics,reportChartSamples,reportLineChart,matchReportHTML,renderMatchReport,reportTime} from '../match-report.js';

const sample=(time,extra={})=>({time,trash:300,collected:0,bonus:0,spent:0,armyCount:3,armyValue:150,workers:5,buildings:3,supplyUsed:8,supplyCap:20,...extra});
function fixture(players=4){return {schemaVersion:1,mapId:'inkdelta',mapName:'Inkspill Delta',difficulty:'impossible',duration:120,winner:0,teams:Array.from({length:players},(_,team)=>({team,totals:{physicalCollected:1000+team*200,bonus:team?400+team*100:0,spent:800+team*100,kills:12-team,losses:4+team,workerLosses:team+1,trained:20+team,damageDealt:2000,damageTaken:1300,buildingsCompleted:7,lossValue:250,killValue:750,firstExpansionAt:48,expansions:2,peakArmyCount:17,peakArmyCountAt:90,peakArmyValue:1950,peakArmyValueAt:105},spentByCategory:{train:500,research:150,build:150},trainedByType:{rock:10,paper:6,twig:3,cd:1},finalCountByType:{rock:5,paper:3,twig:6,cd:1,desk:2},lossesByType:{rock:{count:2,value:100},twig:{count:1,value:25},cd:{count:1,value:75},printer:{count:1,value:150}},killsByType:{scissors:{count:3,value:150}},samples:[sample(0),sample(30,{armyCount:8,armyValue:400,collected:250,spent:150,trash:400}),sample(90,{armyCount:12,armyValue:650,collected:700,bonus:team?300:0,spent:550,trash:450}),sample(120,{armyCount:9,armyValue:475,collected:1000+team*200,bonus:team?400+team*100:0,spent:800+team*100,trash:500+team*100})],events:[]})),events:[{time:48,team:0,type:'expansion',entityType:'desk',id:100},{time:60,team:0,type:'workerDeath',entityType:'twig',id:101,killerTeam:1},{time:100,team:1,type:'buildingDestroyed',entityType:'printer',id:102,killerTeam:0},{time:120,team:1,type:'teamEliminated'}]};}
function frozen(value){if(value&&typeof value==='object'){for(const child of Object.values(value))frozen(child);Object.freeze(value);}return value;}

test('metrics use exact summary peaks and expansions, independently of decimated samples and capped events',()=>{
 const input=fixture();input.events=[];const before=JSON.stringify(input),model=normalizeMatchReport(frozen(input)),m=matchReportMetrics(model.teams[0],model.events);
 assert.deepEqual(m.peakArmy,{count:17,time:90});assert.deepEqual(m.peakValue,{value:1950,time:105});
 assert.equal(m.firstExpansion,48);assert.equal(m.expansions,2);assert.equal(m.bank,500);
 assert.deepEqual(m.losses,{combat:2,workers:1,transports:1,buildings:1,other:0});
 assert.equal(m.produced,20);assert.equal(m.remainingCombat,8);assert.equal(JSON.stringify(input),before,'analysis does not mutate a frozen report');
});

test('resource grants stay separate from physical gathering and from the human economy',()=>{
 const model=normalizeMatchReport(fixture()),human=model.teams[0],rival=model.teams[1];
 assert.equal(human.totals.physicalCollected,1000);assert.equal(human.totals.bonus,0);
 assert.equal(rival.totals.physicalCollected,1200);assert.equal(rival.totals.bonus,500);
 const html=matchReportHTML(model,{tab:'economy',focus:0,compare:true});
 assert.match(html,/1,200 earned \+ 500 bonus/);assert.match(html,/1,000 earned/);
 assert.doesNotMatch(html,/1,500 earned/);assert.match(html,/never counted as worker earnings/);
 assert.match(html,/data-chart-metric="collected" data-chart-final="1200"/);
});

test('zero-length, absent-sample and malformed-number reports remain finite and legible',()=>{
 for(const raw of [{},{duration:0,teams:[{team:0,samples:[],totals:{}}]},{duration:NaN,teams:[{team:0,samples:[sample(0,{armyValue:Infinity,trash:-50})],totals:{bonus:NaN}}]}]){
  const model=normalizeMatchReport(raw),html=matchReportHTML(model);
  assert(!/NaN|Infinity|undefined/.test(html));
  if(model.teams.length)for(const tab of ['overview','economy','army','timeline'])assert(!/NaN|Infinity|undefined/.test(matchReportHTML(model,{tab})));
 }
 const noSamples=normalizeMatchReport({duration:0,teams:[{team:0,samples:[],totals:{}}]});
 assert.match(matchReportHTML(noSamples),/No timeline samples recorded/);
 assert.equal(reportTime(0),'0:00');assert.equal(reportTime(7260),'121:00');
});

test('four-team curves plot actual final values and accessible data, with focus-only comparison',()=>{
 const model=normalizeMatchReport(fixture());
 const html=matchReportHTML(model,{tab:'economy',economyMetric:'trash',focus:2,compare:true});
 assert.equal((html.match(/data-chart-team=/g)||[]).length,4);
 for(let team=0;team<4;team++)assert.match(html,new RegExp(`data-chart-team="${team}" data-chart-metric="trash" data-chart-final="${500+team*100}"`));
 assert.match(html,/data-report-sample-value="800">800<\/td>/);
 assert.match(html,/aria-labelledby="match-report-economy-title match-report-economy-desc"/);
 const focusOnly=matchReportHTML(model,{tab:'army',armyMetric:'armyCount',focus:2,compare:false});
 assert.equal((focusOnly.match(/data-chart-team=/g)||[]).length,1);assert.match(focusOnly,/data-chart-team="2"/);
 assert.match(focusOnly,/Workers and buildings are separate/);
});

test('long-match chart thinning retains first/final observations and bucket extremes without inventing samples',()=>{
 const samples=Array.from({length:5000},(_,i)=>sample(i*5,{armyValue:i===1777?99999:200+Math.sin(i/20)*100})),reduced=reportChartSamples(samples,'armyValue',240);
 assert(reduced.length<=240);assert.equal(reduced[0],samples[0]);assert.equal(reduced.at(-1),samples.at(-1));
 assert(reduced.includes(samples[1777]),'the strongest army observation survives chart thinning');
 for(const point of reduced)assert(samples.includes(point),'every plotted point is a real observation');
 const chart=reportLineChart([{team:0,samples}], 'armyValue',{duration:25000,title:'Long match',unit:'Trash'});
 assert(!/NaN|Infinity/.test(chart));assert.match(chart,/data-chart-samples="5000"/);
});

test('army breakdown separates worker, transport and building losses and shows real production/survivors',()=>{
 const model=normalizeMatchReport(fixture()),html=matchReportHTML(model,{tab:'army',focus:0});
 for(const label of ['Combat troops','Twig workers','CD transports','Buildings','Unit and building breakdown'])assert(html.includes(label));
 assert.match(html,/Flying CDs<\/span>/,'transports appear in remaining military composition');
 assert.match(html,/Starting units are not counted as trained/);
 assert.match(html,/Peak army size/);assert.match(matchReportHTML(model,{tab:'overview',focus:0}),/1,950/);
});

test('timeline attributes worker losses to economic pressure and never invents raid or expansion outcomes',()=>{
 const model=normalizeMatchReport(fixture()),all=matchReportHTML(model,{tab:'timeline',focus:0,compare:true});
 assert.match(all,/Expansion completed/);assert.match(all,/Worker lost/);assert.match(all,/by The Paper Pushers/);assert.match(all,/Printer destroyed/);
 const focusOnly=matchReportHTML(model,{tab:'timeline',focus:3,compare:false});
 assert.match(focusOnly,/No key events were recorded for this selection/);
 assert.doesNotMatch(focusOnly,/Worker lost/);
});

test('report labels and event details escape markup from data',()=>{
 const raw=fixture(2);raw.mapName='<img src=x onerror=alert(1)>';raw.teams[0].name='<script>bad</script>';raw.events.push({time:90,team:0,type:'researchCompleted',entityType:'<img onerror=x>'});
 const model=normalizeMatchReport(raw),html=matchReportHTML(model,{tab:'timeline'});
 assert(!html.includes('<script>bad'));assert(!html.includes('<img src=x'));assert(!html.includes('<img onerror=x>'));assert.match(html,/&lt;script&gt;bad&lt;\/script&gt;/);
});

class Container{
 constructor(){this.listeners={};this.innerHTML='';this.scroll=0;}
 addEventListener(type,fn){(this.listeners[type]??=[]).push(fn);}
 removeEventListener(type,fn){this.listeners[type]=(this.listeners[type]||[]).filter(f=>f!==fn);}
 contains(){return true;}
 querySelector(selector){return selector==='.mr-body'?{scrollTop:this.scroll}:null;}
 dispatch(type,dataset={},more={}){const target={dataset,value:more.value,matches:selector=>selector==='[data-report-focus]'&&own(dataset,'reportFocus'),closest(){return this;}};for(const listener of this.listeners[type]||[])listener({target,key:more.key,preventDefault(){},stopPropagation(){}});}
}
const own=(obj,key)=>Object.prototype.hasOwnProperty.call(obj,key);
test('rendered report binds tabs, focus, compare and metrics once, and cleans up safely',()=>{
 const container=new Container();const cleanup=renderMatchReport(container,frozen(fixture()));
 assert.match(container.innerHTML,/The match, in focus/);
 container.dispatch('click',{reportTab:'economy'});container.dispatch('click',{reportMetric:'economy',reportValue:'bonus'});
 assert.match(container.innerHTML,/data-chart-metric="bonus"/);
 container.dispatch('change',{reportFocus:''},{value:'2'});container.dispatch('click',{reportCompare:'focus'});
 assert.equal((container.innerHTML.match(/data-chart-team=/g)||[]).length,1);assert.match(container.innerHTML,/data-chart-team="2"/);
 container.dispatch('keydown',{reportTab:'economy'},{key:'ArrowRight'});assert.match(container.innerHTML,/Build, fight, rebuild/);
 const second=renderMatchReport(container,fixture(2));assert.equal(container.listeners.click.length,1);cleanup();assert.equal(container.listeners.click.length,1,'an old cleanup cannot remove newer bindings');second();assert.equal(container.listeners.click.length,0);
 const minimal={innerHTML:'',addEventListener(){},querySelector(){return null;}};assert.doesNotThrow(()=>renderMatchReport(minimal,fixture())());
});

test('the real completed simulation supplies plotted accounting without adding Impossible bonuses to human earnings',async()=>{
 const {Game}=await import('../engine.js');const game=new Game({players:2,difficulty:'impossible'});game._think=Infinity;
 const printer=game.entities.find(e=>e.team===0&&e.type==='printer');assert(game.train(printer.id,'rock').ok);
 for(let t=0;t<6;t+=.05)game.update(.05);
 game.winner=0;const report=game.getMatchReport();assert(Object.isFrozen(report));
 const model=normalizeMatchReport(report),human=model.teams[0],bot=model.teams[1];
 assert.equal(human.totals.spent,50);assert.equal(human.trainedByType.rock,1);assert.equal(human.totals.physicalCollected,game.stats.collected[0]);assert.equal(human.totals.bonus,0);assert(bot.totals.bonus>=300);
 const html=matchReportHTML(model,{tab:'economy',economyMetric:'collected'});
 assert.match(html,new RegExp(`data-chart-team="0" data-chart-metric="collected" data-chart-final="${game.stats.collected[0]}"`));
 assert.equal(matchReportMetrics(human,model.events).peakArmy.count,report.teams[0].totals.peakArmyCount);
});
