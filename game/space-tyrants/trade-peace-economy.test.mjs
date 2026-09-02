import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const earlySource=readFileSync(new URL("./early-war-economy-settlements.js",import.meta.url),"utf8");
const moduleSource=readFileSync(new URL("./trade-peace-economy.js",import.meta.url),"utf8");
const loaderSource=readFileSync(new URL("../space-tyrants.html",import.meta.url),"utf8");

function planet(id,owner,{tradeLevel=0,tradeVolume=0}={}){
  return{id,name:id,owner,x:owner*100,y:0,underAttack:false,garrison:10,infra:{shipyard:1,factory:1,defense:0},stock:{components:50,helium:30,titanium:25,trained:.02},buildQueue:[],tradeVolume,tradeStation:tradeLevel?{level:tradeLevel,hp:100,maxHp:100}:null};
}
function createHarness(){
  const state={simTime:0,ships:[],fleets:[],battles:[],wars:[],deepSpacePeaceOffers:[],deepSpaceBases:[],rivalDiplomacy:{agreements:[]},planets:[planet("p0",0),planet("r1",1),planet("r2",1),planet("r3",1)],empires:[{id:0,name:"Mandate",credits:100,foreignPolicy:{aggression:.45,commercialism:.65},relations:{1:0}},{id:1,name:"Rival",credits:40,foreignPolicy:{aggression:.6,commercialism:.4},relations:{0:0}}]};
  const empire=id=>state.empires.find(e=>e.id===id),owned=id=>state.planets.filter(p=>p.owner===id),getWar=(a,b)=>state.wars.find(w=>w.active&&((w.a===a&&w.b===b)||(w.a===b&&w.b===a)))||null;
  const agreements=[];const context={console,Math,Set,Map,Number,String,Boolean,Object,Array,RegExp,state,globalThis:null,clamp:(v,a,b)=>Math.max(a,Math.min(b,v)),random:()=>.5,rand:(a,b)=>(a+b)/2,empire,owned,getWar,adjustRelation:()=>{},stxWBPowerBreakdown:id=>({total:empire(id).testPower||20}),stxEWPAssembleReserveFleet:()=>null,stxEWPQueueEmergencyFleet:()=>false,launchWarCampaign:()=>false,endWar:w=>{w.active=false;return w},stxWCCSetTruce:()=>0,stxDSAddAgreement:(kind,a,b,duration,extra)=>{const x={kind,a,b,duration,...extra};agreements.push(x);return x},stxDSDiplomacyPanel:id=>`<section>${id}</section>`,stxDSHandleDiplomacy:()=>false,stxDSAcceptPeace:()=>true,stxDSPeaceCard:()=>"",aiTick:()=>{},fmtEta:n=>`${n}s`,showToast:()=>{},renderRivals:()=>{},renderTransmissions:()=>{},updateBadges:()=>{},updateHud:()=>{},galacticNews:()=>{},logEvent:()=>{},stxDSBases:(owner,operational)=>state.deepSpaceBases.filter(b=>b.owner===owner&&(!operational||b.status==="operational")),simulate:()=>true,stxTickTradeStationEconomy:(p,dt)=>{if(!p.tradeStation)return;const s=p.tradeStation,level=s.level,health=s.hp/s.maxHp;empire(p.owner).credits+=dt*.0018*level*health*(1+(p.tradeVolume||0)*.018)}};
  context.globalThis=context;vm.createContext(context);vm.runInContext(earlySource,context,{filename:"early-war-economy-settlements.js"});vm.runInContext(moduleSource,context,{filename:"trade-peace-economy.js"});return{context,state,agreements};
}

test("loader installs trade-peace economy after early-war settlement layer",()=>{
  const early=loaderSource.indexOf("'./space-tyrants/early-war-economy-settlements.js'"),trade=loaderSource.indexOf("'./space-tyrants/trade-peace-economy.js'");assert.ok(trade>early);assert.equal(loaderSource.match(/trade-peace-economy\.js/g)?.length,1);
});

test("frontier peace starts cheaper and grows with game time and war duration",()=>{
  const {context,state}=createHarness();state.wars.push({id:"w1",a:0,b:1,active:true,startedAt:0});context.empire(0).testPower=16;context.empire(1).testPower=22;state.simTime=30;const early=context.SpaceTyrantsTradePeaceEconomy.settlementCosts(1);assert.ok(early.reparations<24);assert.ok(early.commercial<14);state.simTime=420;const late=context.SpaceTyrantsTradePeaceEconomy.settlementCosts(1);assert.ok(late.reparations>early.reparations);assert.ok(late.commercial>early.commercial);
});

test("larger mature empires face higher settlement costs than small frontier rivals",()=>{
  const {context,state}=createHarness();state.wars.push({id:"w2",a:0,b:1,active:true,startedAt:0});state.simTime=220;const small=context.SpaceTyrantsTradePeaceEconomy.settlementCosts(1);for(let i=4;i<=9;i++)state.planets.push(planet(`r${i}`,1));const large=context.SpaceTyrantsTradePeaceEconomy.settlementCosts(1);assert.ok(large.reparations>small.reparations);assert.ok(large.commercial>small.commercial);
});

test("tier 3 trade stations earn substantially more than tier 1 stations",()=>{
  const {context,state}=createHarness(),p=state.planets[0];p.tradeVolume=24;p.tradeStation={level:1,hp:100,maxHp:100};context.empire(0).credits=0;context.stxTickTradeStationEconomy(p,100);const tier1=context.empire(0).credits;p.tradeStation.level=3;context.empire(0).credits=0;context.stxTickTradeStationEconomy(p,100);const tier3=context.empire(0).credits;assert.ok(tier3>tier1*3);
});

test("trade agreements and foreign commercial traffic increase station credit income",()=>{
  const {context,state}=createHarness(),p=state.planets[0];p.tradeVolume=18;p.tradeStation={level:2,hp:100,maxHp:100};context.empire(0).credits=0;context.stxTickTradeStationEconomy(p,100);const isolated=context.empire(0).credits;state.rivalDiplomacy.agreements.push({kind:"trade-agreement",by:0,with:1,active:true,expiresAt:999});state.ships.push({id:"trade1",owner:0,type:"freighter",from:"p0",to:"r1"});context.empire(0).credits=0;context.stxTickTradeStationEconomy(p,100);const connected=context.empire(0).credits;assert.ok(connected>isolated);
});

test("deep-space trade infrastructure can earn network dividends from real trade links",()=>{
  const {context,state}=createHarness();state.deepSpaceBases.push({id:"b1",owner:0,type:"trade",tier:2,status:"operational",supplyReadiness:1});state.rivalDiplomacy.agreements.push({kind:"trade-agreement",by:0,with:1,active:true,expiresAt:999});state.ships.push({id:"trade2",owner:0,type:"courier",from:"p0",to:"r1"});context.empire(0).credits=0;context.SpaceTyrantsTradePeaceEconomy.tradeDividendTick(100);assert.ok(context.empire(0).credits>0);assert.ok(context.empire(0).stxTradeCreditEconomy.totalBonusEarned>0);
});
