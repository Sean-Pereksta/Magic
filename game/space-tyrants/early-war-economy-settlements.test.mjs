import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const moduleSource=readFileSync(new URL("./early-war-economy-settlements.js",import.meta.url),"utf8");
const loaderSource=readFileSync(new URL("../space-tyrants.html",import.meta.url),"utf8");

function planet(id,owner,{yard=0,factory=1,stock={},garrison=12}={}){
  return{id,name:id,owner,x:owner*100,y:0,underAttack:false,garrison,infra:{shipyard:yard,factory,defense:0},stock:{components:0,helium:0,titanium:0,trained:0,...stock},buildQueue:[]};
}
function createHarness(){
  const state={simTime:0,ships:[],fleets:[],battles:[],wars:[],deepSpacePeaceOffers:[],planets:[],empires:[]};
  state.empires=[
    {id:0,name:"Mandate",credits:100,foreignPolicy:{aggression:.45},relations:{1:0}},
    {id:1,name:"Rival",credits:30,foreignPolicy:{aggression:.7},relations:{0:0}}
  ];
  state.planets=[planet("p0",0,{yard:1,stock:{components:60,helium:40,titanium:30,trained:.02}}),planet("r1",1,{yard:1}),planet("r2",1),planet("r3",1)];
  let launchCalls=0,reserveCalls=0,queueCalls=0;const agreements=[];
  const empire=id=>state.empires.find(e=>e.id===id),owned=id=>state.planets.filter(p=>p.owner===id),getWar=(a,b)=>state.wars.find(w=>w.active&&((w.a===a&&w.b===b)||(w.a===b&&w.b===a)))||null;
  const context={
    console,Math,Set,Map,Number,String,Boolean,Object,Array,RegExp,state,globalThis:null,
    clamp:(v,a,b)=>Math.max(a,Math.min(b,v)),random:()=>.5,rand:(a,b)=>(a+b)/2,empire,owned,getWar,
    adjustRelation:(a,b,d)=>{empire(a).relations[b]=(empire(a).relations[b]||0)+d},stxWBPowerBreakdown:id=>({total:empire(id).testPower||20}),
    stxEWPAssembleReserveFleet:()=>{reserveCalls++;return{id:"free"}},stxEWPQueueEmergencyFleet:()=>{queueCalls++;return true},
    launchWarCampaign:(e,foeId)=>{launchCalls++;const target=owned(foeId)[0];state.ships.push({id:`s${launchCalls}`,owner:e.id,type:"fleet",to:target.id,strength:18});return true},
    endWar:(w,reason)=>{w.active=false;w.reason=reason;return w},stxWCCSetTruce:(a,b,d)=>{state.lastTruce={a,b,d};return state.simTime+d},
    stxDSAddAgreement:(kind,a,b,duration,extra)=>{const x={kind,a,b,duration,...extra};agreements.push(x);return x},
    stxDSDiplomacyPanel:id=>`<section><div>base ${id}</div></section>`,stxDSHandleDiplomacy:()=>false,
    stxDSAcceptPeace:(offer,reason)=>{const w=state.wars.find(x=>x.id===offer.warId&&x.active);if(w)context.endWar(w,reason);offer.status="accepted";return true},stxDSPeaceCard:()=>"<article>base peace</article>",aiTick:()=>true,
    fmtEta:n=>`${Math.ceil(n)}s`,showToast:()=>{},renderRivals:()=>{},renderTransmissions:()=>{},updateBadges:()=>{},updateHud:()=>{},galacticNews:()=>{},logEvent:()=>{}
  };
  context.globalThis=context;vm.createContext(context);vm.runInContext(moduleSource,context,{filename:"early-war-economy-settlements.js"});
  return{context,state,agreements,metrics:{get launchCalls(){return launchCalls},get reserveCalls(){return reserveCalls},get queueCalls(){return queueCalls}}};
}

test("loader installs early-war parity after economy, diplomacy, and war continuity",()=>{
  const early=loaderSource.indexOf("'./space-tyrants/early-war-economy-settlements.js'"),continuity=loaderSource.indexOf("'./space-tyrants/war-campaign-continuity.js'"),economy=loaderSource.indexOf("'./space-tyrants/integrated-economy-fortresses.js'");
  assert.ok(early>continuity);assert.ok(early>economy);assert.equal(loaderSource.match(/early-war-economy-settlements\.js/g)?.length,1);
});

test("2-3 world rivals use frontier pacing and one inbound offensive",()=>{
  const {context}=createHarness(),p=context.SpaceTyrantsEarlyWarBalance.profile(1);assert.equal(p.stage,"frontier");assert.equal(p.maxInbound,1);assert.equal(p.waveCap,2);assert.ok(p.cooldown>=58);assert.ok(p.mobilization>=30);
});

test("6-7 world rivals scale up only with real yards and replacement stock",()=>{
  const {context,state}=createHarness();state.planets.push(planet("r4",1),planet("r5",1),planet("r6",1,{yard:1,stock:{components:70,helium:40,titanium:35,trained:.02}}));state.planets.find(p=>p.id==="r1").stock={components:70,helium:40,titanium:35,trained:.02};
  const p=context.SpaceTyrantsEarlyWarBalance.profile(1);assert.equal(p.stage,"developing");assert.equal(p.worlds,6);assert.equal(p.maxInbound,2);assert.equal(p.maxQueued,2);
});

test("AI can no longer convert regenerated garrison directly into a reserve fleet",()=>{
  const {context,metrics}=createHarness();assert.equal(context.stxEWPAssembleReserveFleet(context.empire(1),context.owned(0)[0]),null);assert.equal(metrics.reserveCalls,0);
});

test("early AI offensive waves respect mobilization, cooldown, and rolling wave cap",()=>{
  const {context,state,metrics}=createHarness();state.wars.push({id:"w1",a:1,b:0,active:true,startedAt:0});
  state.simTime=20;assert.equal(context.launchWarCampaign(context.empire(1),0),false);assert.equal(metrics.launchCalls,0);
  state.simTime=40;assert.equal(context.launchWarCampaign(context.empire(1),0),true);assert.equal(metrics.launchCalls,1);state.ships.length=0;
  state.simTime=60;assert.equal(context.launchWarCampaign(context.empire(1),0),false);assert.equal(metrics.launchCalls,1);
  state.simTime=115;assert.equal(context.launchWarCampaign(context.empire(1),0),true);assert.equal(metrics.launchCalls,2);state.ships.length=0;
  state.simTime=150;assert.equal(context.launchWarCampaign(context.empire(1),0),false);assert.equal(metrics.launchCalls,2);
  state.simTime=200;assert.equal(context.launchWarCampaign(context.empire(1),0),true);assert.equal(metrics.launchCalls,3);
});

test("small rivals cannot stack multiple emergency fleet commissions",()=>{
  const {context,state,metrics}=createHarness();state.planets.find(p=>p.id==="r1").buildQueue.push({type:"fleet"});assert.equal(context.stxEWPQueueEmergencyFleet(context.empire(1),state.planets[0]),false);assert.equal(metrics.queueCalls,0);
});

test("Rival Powers exposes multiple settlement categories and reparations transfer real credits",()=>{
  const {context,state,agreements}=createHarness();context.empire(0).testPower=12;context.empire(1).testPower=28;state.wars.push({id:"w2",a:0,b:1,active:true,startedAt:0});state.simTime=45;
  const html=context.stxDSDiplomacyPanel(1);assert.match(html,/Armistice/);assert.match(html,/Reparations Settlement/);assert.match(html,/Commercial Peace/);
  const cost=context.SpaceTyrantsEarlyWarBalance.settlementCosts(1).reparations,before=context.empire(0).credits,rivalBefore=context.empire(1).credits;assert.equal(context.stxDSHandleDiplomacy("ewes-reparations",1),true);assert.equal(context.empire(0).credits,before-cost);assert.equal(context.empire(1).credits,rivalBefore+cost);assert.equal(state.wars[0].active,false);assert.equal(state.lastTruce.d,170);assert.ok(agreements.some(a=>a.kind==="non-aggression"));
});

test("military stalemates unlock mutual demobilization",()=>{
  const {context,state}=createHarness();context.empire(0).testPower=22;context.empire(1).testPower=20;state.wars.push({id:"w3",a:0,b:1,active:true,startedAt:0});state.simTime=40;assert.match(context.stxDSDiplomacyPanel(1),/Mutual Demobilization/);assert.equal(context.stxDSHandleDiplomacy("ewes-demobilize",1),true);assert.equal(state.lastTruce.d,180);
});

test("economically exhausted early rivals can propose an armistice instead of another wave",()=>{
  const {context,state}=createHarness();state.wars.push({id:"w4",a:1,b:0,active:true,startedAt:0});state.simTime=110;const e=context.empire(1);e.stxEarlyWarEconomy={version:1,lastOffensiveLaunchAt:80,lastArmisticeOfferAt:-999,launches:[{at:40},{at:80}]};assert.equal(context.SpaceTyrantsEarlyWarBalance.maybeOfferArmistice(state.wars[0]),true);assert.equal(state.deepSpacePeaceOffers.length,1);assert.equal(state.deepSpacePeaceOffers[0].kind,"armistice");assert.match(context.stxDSPeaceCard(state.deepSpacePeaceOffers[0]),/fleet losses and replacement strain/i);
});
