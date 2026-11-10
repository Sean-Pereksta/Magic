import test from 'node:test';
import assert from 'node:assert/strict';
import {Game,TYPES} from '../engine.js';
import {captureTelemetry,recordTelemetryEvent,TELEMETRY_MAX_SAMPLES,TELEMETRY_MAX_EVENTS,TELEMETRY_MAX_TEAM_EVENTS} from '../match-telemetry.js';

function arena(options={}){const g=new Game(options);g._think=Infinity;g.map.obstacles=[];g.map.plateaus=[];g.map.ramps=[];g.map.cliffs=[];g._collisionBuildings=null;g.canSee=()=>true;return g;}
function advance(g,seconds){for(let t=0;t<seconds-1e-9;){const dt=Math.min(.04,seconds-t);g.update(dt);t+=dt;}}
const own=(g,type,team=0)=>g.entities.find(e=>e.type===type&&e.team===team);

test('samples start at real initial conditions, advance on simulation time and include a precise short final sample',()=>{
 const g=new Game();g._think=Infinity;assert.equal(g.getMatchReport(),null);
 const first=g.telemetry.teams[0].samples[0];assert.equal(first.time,0);assert.equal(first.workers,5);assert.equal(first.armyCount,3);assert.equal(first.armyValue,150);assert.equal(first.buildings,3);assert.equal(first.trash,300);
 advance(g,5.2);assert.equal(g.telemetry.teams[0].samples.length,2);assert(g.telemetry.teams[0].samples[1].time>=5);
 const before=g.time;g.winner=0;g.update(1);assert.equal(g.time,before,'ended matches do not advance');
 const report=g.getMatchReport();assert.equal(report.duration,Math.round(before*1000)/1000);assert.equal(report.teams[0].samples.at(-1).time,report.duration);
 assert.equal(report.teams[0].totals.physicalCollected,g.stats.collected[0]);assert(Object.isFrozen(report));assert(Object.isFrozen(report.teams[0].samples[0]));assert.throws(()=>{report.teams[0].samples[0].trash=999;});
 const snapshot=JSON.stringify(report);g.teams[0].trash=90000;assert.equal(JSON.stringify(g.getMatchReport()),snapshot,'the frozen report never changes after its endpoint');
});

test('successful queued spending is exact, failed actions are free, and activation never double-counts it',()=>{
 const g=arena(),printer=own(g,'printer'),desk=own(g,'desk'),worker=own(g,'twig');g.teams[0].trash=1000;
 assert(g.train(printer.id,'rock').ok);assert(g.research(printer.id,'paper').ok);
 const first=g.build([worker.id],'cabinet',desk.x+400,desk.y+150,{queue:true}),second=g.build([worker.id],'mouse',desk.x+650,desk.y+150,{queue:true});assert(first.ok&&second.ok);
 const entry=g.telemetry.teams[0];assert.equal(entry.totals.spent,450);assert.deepEqual(entry.spentByCategory,{train:50,research:150,build:250});
 const bank=g.teams[0].trash;assert(!g.build([worker.id],'cabinet',desk.x,desk.y,{queue:true}).ok);assert.equal(g.teams[0].trash,bank);assert.equal(entry.totals.spent,450);
 advance(g,35);assert.equal(entry.totals.spent,450);assert.equal(entry.trainedByType.rock,1);assert.equal(entry.totals.trained,1);assert(entry.events.some(e=>e.type==='researchCompleted'));assert.equal(entry.totals.buildingsCompleted,2);
});

test('combat accounting caps overkill, records real worker losses and credits one kill only',()=>{
 const g=arena(),attacker=g._spawn('rock',0,3000,3000),worker=g._spawn('twig',1,3040,3000);g._damage(worker,200,attacker);
 assert.equal(g.telemetry.teams[0].totals.damageDealt,75);assert.equal(g.telemetry.teams[1].totals.damageTaken,75);advance(g,.08);
 const ours=g.telemetry.teams[0],theirs=g.telemetry.teams[1];assert.equal(ours.totals.kills,1);assert.equal(theirs.totals.losses,1);assert.equal(theirs.totals.workerLosses,1);
 assert.deepEqual(theirs.lossesByType.twig,{count:1,value:25});assert.deepEqual(ours.killsByType.twig,{count:1,value:25});assert(theirs.events.some(e=>e.type==='workerDeath'&&e.killerTeam===0));
 advance(g,.2);assert.equal(ours.totals.kills,1);assert.equal(g.stats.destroyed[0],1,'original kill stat semantics are intact');
});

test('physical harvesting and Impossible bonuses remain separate and player bonuses stay zero',()=>{
 const g=new Game({difficulty:'impossible'});g._think=Infinity;advance(g,12);g.winner=0;const report=g.getMatchReport();
 for(const team of [0,1]){const r=report.teams[team];assert.equal(r.totals.physicalCollected,g.stats.collected[team]);assert(Math.abs(r.samples.at(-1).trash-(300+r.totals.physicalCollected+r.totals.bonus))<1e-6);}
 assert.equal(report.teams[0].totals.bonus,0);assert(report.teams[1].totals.bonus>300);assert.equal(report.teams[1].samples[0].bonus,300);
});

test('completion events distinguish real expansions and preserve production peaks between sample times',()=>{
 const g=arena(),home=g.starts[0],worker=own(g,'twig');g.teams[0].trash=2000;
 const expansion=g.build([worker.id],'desk',home.x+650,home.y+150,{queue:true});assert(expansion.ok);advance(g,20);
 const totals=g.telemetry.teams[0].totals;assert.equal(totals.expansions,1);assert(totals.firstExpansionAt>0);assert(g.telemetry.teams[0].events.some(e=>e.type==='expansion'&&e.id===expansion.entityId));
 const troop=g._spawn('rollingrock',0,3000,3000);g.time=21.25;g._event('trained',{entity:troop});const peak=totals.peakArmyValue;troop.hp=0;g._event('death',{entity:troop});
 assert(peak>=500);assert.equal(totals.peakArmyValue,peak);assert.equal(totals.peakArmyValueAt,21.25);assert(totals.peakArmyCount>=4);assert.equal(g.telemetry.teams[0].trainedByType.rollingrock,1);
});

test('elimination accounts for remaining losses once and allows a report when the human exits a free-for-all',()=>{
 const g=arena({players:3});for(const worker of g.entities.filter(e=>e.team===0&&e.type==='twig'))worker.order={type:'stop'};
 const before=g.entities.filter(e=>e.team===0).length;own(g,'desk').hp=0;advance(g,.08);assert(g.teams[0].eliminated);assert.equal(g.winner,null);
 const report=g.getMatchReport();assert(report.playerEliminated);assert.equal(report.teams[0].totals.losses,before);assert(report.events.some(e=>e.type==='teamEliminated'&&e.team===0));
 assert.deepEqual(report.teams[0].finalCountByType,{});assert.equal(report.teams[0].samples.at(-1).workers,0);
});

test('long matches preserve whole-history endpoints and exact cumulative summaries within bounded storage',()=>{
 const g=arena(),home=g.starts[0],desk=g._spawn('desk',0,home.x+700,home.y);g.time=12;g._event('buildingCompleted',{entity:desk});
 const troop=g._spawn('rollingrock',0,3000,3000);g.time=13;g._event('trained',{entity:troop});const peak=g.telemetry.teams[0].totals.peakArmyValue;troop.hp=0;g._event('death',{entity:troop});
 for(let i=1;i<=2500;i++){g.time=i*5;g.stats.collected[0]=i*5;captureTelemetry(g);recordTelemetryEvent(g,'researchCompleted',{team:0,researchType:'paper'});}
 g.time=12502.375;g.winner=0;const report=g.getMatchReport(),team=report.teams[0];
 assert(team.samples.length<=TELEMETRY_MAX_SAMPLES);assert.equal(team.samples[0].time,0);assert.equal(team.samples.at(-1).time,12502.375);assert(team.sampleCompactions>0);assert(team.samples.every((s,i)=>!i||s.time>team.samples[i-1].time));
 assert.equal(team.totals.firstExpansionAt,12);assert.equal(team.totals.expansions,1);assert.equal(team.totals.peakArmyValue,peak);assert.equal(team.totals.peakArmyValueAt,13);assert.equal(team.totals.physicalCollected,12500);
 assert(team.events.length<=TELEMETRY_MAX_TEAM_EVENTS);assert(report.events.length<=TELEMETRY_MAX_EVENTS);assert(!team.events.some(e=>e.type==='expansion'),'the exact summary survives even after the old event rotates out');
});

test('zero-duration reports contain finite real data and restarting creates fresh accounting',()=>{
 const g=new Game();g.winner=0;const report=g.getMatchReport();assert.equal(report.duration,0);assert.equal(report.teams[0].samples.length,1);assert.equal(report.teams[0].totals.spent,0);
 assert(!/NaN|Infinity/.test(JSON.stringify(report)));const next=new Game();assert.equal(next.getMatchReport(),null);assert.equal(next.telemetry.teams[0].samples[0].time,0);assert.notEqual(next.telemetry,g.telemetry);assert.equal(next.telemetry.report,null);
});
