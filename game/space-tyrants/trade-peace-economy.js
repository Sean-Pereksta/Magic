/* Space Tyrants — progressive peace pricing + commercial credit economy.
   Frontier wars should be cheap to settle. As the galaxy, empires, and a war
   mature, negotiated payouts become more consequential. Trade stations and real
   commercial relationships now provide a stronger treasury path to support them. */

const STX_TPE_VERSION=1;
const STX_TPE_COMMERCIAL_TYPES=new Set(["freighter","tanker","courier","luxury","liner","trade"]);

function stxTPEWar(enemyId){return typeof getWar==="function"?getWar(0,enemyId):null}
function stxTPEWarAge(enemyId){const w=stxTPEWar(enemyId);return w&&typeof stxEWESWarStartedAt==="function"?Math.max(0,state.simTime-stxEWESWarStartedAt(w)):0}
function stxTPEPower(owner){return typeof stxEWESPower==="function"?stxEWESPower(owner):1}
function stxTPEPeaceScope(enemyId){
  const playerWorlds=owned(0).length,rivalWorlds=owned(enemyId).length;
  return{playerWorlds,rivalWorlds,scopeWorlds:Math.max(rivalWorlds,Math.ceil((playerWorlds+rivalWorlds)/2))};
}

/* Prices intentionally start well below the former 18-credit floor. They scale
   with galaxy age, duration of the current war, empire size, and only then the
   amount by which the rival materially outguns the player. */
function stxTPESettlementCosts(enemyId){
  const {playerWorlds,rivalWorlds,scopeWorlds}=stxTPEPeaceScope(enemyId),gameAge=Math.max(0,Number(state.simTime)||0),warAge=stxTPEWarAge(enemyId);
  const playerPower=stxTPEPower(0),rivalPower=stxTPEPower(enemyId),ratio=playerPower/Math.max(1,rivalPower),powerDeficit=Math.max(0,rivalPower-playerPower);
  const eraCost=Math.min(34,gameAge*.045),warCost=Math.min(24,warAge*.07),sizeCost=scopeWorlds*1.8,defeatCost=Math.min(24,powerDeficit*.12);
  const reparations=clamp(Math.round(5+sizeCost+eraCost+warCost+defeatCost),8,95);
  const commercial=clamp(Math.round(3+scopeWorlds*1.05+Math.min(18,gameAge*.022)+Math.min(14,warAge*.035)),5,55);
  return{reparations,commercial,ratio,playerPower,rivalPower,gameAge,warAge,playerWorlds,rivalWorlds,scopeWorlds};
}
if(typeof stxEWESSettlementCosts==="function")stxEWESSettlementCosts=stxTPESettlementCosts;
if(globalThis.SpaceTyrantsEarlyWarBalance)globalThis.SpaceTyrantsEarlyWarBalance.settlementCosts=stxTPESettlementCosts;

function stxTPEAgreements(owner){
  return(state.rivalDiplomacy?.agreements||[]).filter(a=>a.active!==false&&a.expiresAt>state.simTime&&a.kind==="trade-agreement"&&((a.by===owner||a.a===owner)||(a.with===owner||a.b===owner))).length;
}
function stxTPEForeignCommercialTraffic(owner){
  return(state.ships||[]).filter(s=>{
    if(s.owner!==owner||!STX_TPE_COMMERCIAL_TYPES.has(s.type))return false;
    const to=state.planets.find(p=>p.id===s.to),from=state.planets.find(p=>p.id===s.from);
    return !!((to&&to.owner!==null&&to.owner!==owner)||(from&&from.owner!==null&&from.owner!==owner));
  }).length;
}
function stxTPEPlanetStationNetwork(owner){
  const stations=owned(owner).filter(p=>p.tradeStation&&p.tradeStation.hp>0).map(p=>{
    const s=p.tradeStation,level=clamp(Number(s.level)||1,1,3),health=clamp((Number(s.hp)||0)/Math.max(1,Number(s.maxHp)||1),.2,1);
    return{p,s,level,health,value:level*health,volume:Math.max(0,Number(p.tradeVolume)||0)};
  });
  return{stations,value:stations.reduce((n,x)=>n+x.value,0),volume:stations.reduce((n,x)=>n+x.volume,0)};
}
function stxTPEDeepTradeValue(owner){
  if(typeof stxDSBases!=="function")return 0;
  return stxDSBases(owner,true).filter(b=>b.type==="trade").reduce((n,b)=>n+clamp(Number(b.tier)||1,1,3)*clamp(Number(b.supplyReadiness)||.65,.2,1),0);
}
function stxTPETradeNetwork(owner){
  const planet=stxTPEPlanetStationNetwork(owner),deep=stxTPEDeepTradeValue(owner),agreements=stxTPEAgreements(owner),traffic=stxTPEForeignCommercialTraffic(owner),e=empire(owner),commercialism=clamp(Number(e?.foreignPolicy?.commercialism)||0,0,1);
  return{...planet,deep,agreements,traffic,commercialism,infrastructure:planet.value+deep};
}
function stxTPEStationBonusRate(p){
  if(!p?.tradeStation||p.tradeStation.hp<=0||p.owner===null)return 0;
  const n=stxTPETradeNetwork(p.owner),s=p.tradeStation,level=clamp(Number(s.level)||1,1,3),health=clamp((Number(s.hp)||0)/Math.max(1,Number(s.maxHp)||1),.2,1),volume=Math.max(0,Number(p.tradeVolume)||0);
  const maturity=1+Math.min(2.4,volume*.012),network=1+n.agreements*.18+Math.min(.7,n.traffic*.055),policy=1+n.commercialism*.3;
  return .0024*level*level*health*maturity*network*policy;
}
function stxTPERecordIncome(owner,amount,source){
  if(!(amount>0))return;const e=empire(owner);if(!e)return;e.stxTradeCreditEconomy=e.stxTradeCreditEconomy||{version:STX_TPE_VERSION,totalBonusEarned:0,bySource:{},lastUpdatedAt:state.simTime};
  const r=e.stxTradeCreditEconomy;r.version=STX_TPE_VERSION;r.totalBonusEarned=(Number(r.totalBonusEarned)||0)+amount;r.bySource=r.bySource||{};r.bySource[source]=(Number(r.bySource[source])||0)+amount;r.lastUpdatedAt=state.simTime;
}

/* Existing trade-station revenue remains intact. This adds the commercial upside
   that was missing: higher tiers scale quadratically, strong local trade volume
   matters, and treaties/foreign traffic make the station more valuable. */
if(typeof stxTickTradeStationEconomy==="function"){
  const STX_TPE_stxTickTradeStationEconomy=stxTickTradeStationEconomy;
  stxTickTradeStationEconomy=function(p,dt){
    const result=STX_TPE_stxTickTradeStationEconomy(p,dt);if(!p?.tradeStation||p.owner===null)return result;
    const bonus=Math.max(0,Number(dt)||0)*stxTPEStationBonusRate(p),e=empire(p.owner);if(e&&bonus>0){e.credits=(Number(e.credits)||0)+bonus;stxTPERecordIncome(p.owner,bonus,"tradeStations")}
    return result;
  };
}

/* Trade agreements and actual foreign commercial traffic create a second revenue
   channel. It is deliberately weak without infrastructure, but compounds with a
   mature network and also recognizes operational deep-space trade bases. */
function stxTPETradeDividendTick(dt){
  const elapsed=Math.max(0,Number(dt)||0);if(!elapsed)return;
  state.empires.forEach(e=>{
    if(!e||!owned(e.id).length)return;const n=stxTPETradeNetwork(e.id);if(n.infrastructure<=0)return;
    const relationshipValue=n.agreements*.00105+n.traffic*.00048;if(relationshipValue<=0)return;
    const infrastructureMultiplier=1+Math.min(2.5,n.infrastructure*.24)+Math.min(.8,Math.sqrt(Math.max(0,n.volume))*.025),policy=1+n.commercialism*.35;
    const dividend=elapsed*relationshipValue*infrastructureMultiplier*policy;if(dividend>0){e.credits=(Number(e.credits)||0)+dividend;stxTPERecordIncome(e.id,dividend,"tradeNetwork")}
  });
}
if(typeof simulate==="function"){
  const STX_TPE_simulate=simulate;
  simulate=function(dt){const result=STX_TPE_simulate(dt);stxTPETradeDividendTick(dt);return result};
}

globalThis.SpaceTyrantsTradePeaceEconomy={
  settlementCosts:stxTPESettlementCosts,
  tradeNetwork:stxTPETradeNetwork,
  stationBonusRate:stxTPEStationBonusRate,
  tradeDividendTick:stxTPETradeDividendTick
};
