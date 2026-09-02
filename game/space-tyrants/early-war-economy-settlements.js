/* Space Tyrants — early-war economic parity + negotiated conflict resolution.
   Small rival empires must replace fleets through real shipyard queues instead of
   converting endlessly regenerated garrisons into free expeditionary waves. War
   pacing scales with 2–7 world economies, and Rival Powers gains several distinct
   settlement paths beyond status quo / total continuation. */

const STX_EWES_VERSION=1;
const STX_EWES_FLEET_COST={components:38,helium:18,titanium:15,trained:.009};
const STX_EWES_ARMISTICE_TRUCE=120;
const STX_EWES_REPARATIONS_TRUCE=170;
const STX_EWES_COMMERCIAL_TRUCE=110;
const STX_EWES_DEMOBILIZATION_TRUCE=180;

function stxEWESState(e){
  if(!e)return null;
  const s=e.stxEarlyWarEconomy||(e.stxEarlyWarEconomy={version:STX_EWES_VERSION,launches:[],lastOffensiveLaunchAt:-999,lastArmisticeOfferAt:-999});
  s.version=STX_EWES_VERSION;s.launches=Array.isArray(s.launches)?s.launches:[];
  if(!Number.isFinite(s.lastOffensiveLaunchAt))s.lastOffensiveLaunchAt=-999;
  if(!Number.isFinite(s.lastArmisticeOfferAt))s.lastArmisticeOfferAt=-999;
  return s;
}
function stxEWESWorldCount(e){return e?owned(e.id).length:0}
function stxEWESOwnerStock(owner,resource){return owned(owner).reduce((n,p)=>n+(Number(p.stock?.[resource])||0),0)}
function stxEWESQueuedMilitary(owner){return owned(owner).reduce((n,p)=>n+(p.buildQueue||[]).filter(q=>q.type==="fleet"||q.type==="patrol").length,0)}
function stxEWESActiveFleetCount(owner){return state.fleets.filter(f=>f.owner===owner&&!f.destroyed).length}
function stxEWESReplacementCapacity(e){
  if(!e)return{worlds:0,yards:0,factories:0,queued:0,equivalents:0};
  const worlds=stxEWESWorldCount(e),yards=owned(e.id).filter(p=>(p.infra?.shipyard||0)>0&&!p.underAttack).length,factories=owned(e.id).reduce((n,p)=>n+(Number(p.infra?.factory)||0),0),queued=stxEWESQueuedMilitary(e.id);
  const equivalents=Math.max(0,Math.min(...Object.entries(STX_EWES_FLEET_COST).map(([r,c])=>stxEWESOwnerStock(e.id,r)/c)));
  return{worlds,yards,factories,queued,equivalents,active:stxEWESActiveFleetCount(e.id)};
}
function stxEWESProfile(e){
  const cap=stxEWESReplacementCapacity(e),worlds=cap.worlds,aggression=Number(e?.foreignPolicy?.aggression??e?.aggression??.45);
  let stage="major",cooldown=28,mobilization=10,maxInbound=2,waveWindow=180,waveCap=8,maxQueued=Math.max(1,cap.yards);
  if(worlds<=3){stage="frontier";cooldown=64-aggression*6;mobilization=34;maxInbound=1;waveWindow=150;waveCap=2;maxQueued=1}
  else if(worlds<=5){stage="early";cooldown=53-aggression*7;mobilization=26;maxInbound=1;waveWindow=160;waveCap=3;maxQueued=1}
  else if(worlds<=7){stage="developing";cooldown=43-aggression*7;mobilization=19;maxInbound=cap.yards>=2&&cap.equivalents>=1.4&&aggression>=.66?2:1;waveWindow=170;waveCap=4;maxQueued=Math.min(2,Math.max(1,cap.yards))}
  cooldown=Math.max(22,cooldown+(worlds<=7&&cap.equivalents<.55?12:worlds<=7&&cap.equivalents<1?6:0));
  return{stage,worlds,cooldown,mobilization,maxInbound,waveWindow,waveCap,maxQueued,capacity:cap};
}
function stxEWESWarStartedAt(w){
  if(!w)return state.simTime;
  if(Number.isFinite(w.stxEconomicWarStartedAt))return w.stxEconomicWarStartedAt;
  const known=[w.startedAt,w.createdAt,w.startTime].find(Number.isFinite);
  w.stxEconomicWarStartedAt=Number.isFinite(known)?known:state.simTime;
  return w.stxEconomicWarStartedAt;
}
function stxEWESRecentLaunches(e,window=180){
  const s=stxEWESState(e),cut=state.simTime-window;
  s.launches=s.launches.filter(x=>Number(x?.at)>=cut);
  return s.launches;
}
function stxEWESIncomingCount(owner,foeId){
  const targets=new Set(owned(foeId).map(p=>p.id));
  return state.ships.filter(s=>s.owner===owner&&(s.type==="fleet"||s.type==="patrol")&&targets.has(s.to)).length;
}
function stxEWESPower(owner){
  if(typeof stxWBPowerBreakdown==="function")return Math.max(1,Number(stxWBPowerBreakdown(owner)?.total)||1);
  const fleets=state.fleets.filter(f=>f.owner===owner&&!f.destroyed).reduce((n,f)=>n+(Number(f.strength)||0),0),moving=state.ships.filter(s=>s.owner===owner&&(s.type==="fleet"||s.type==="patrol")).reduce((n,s)=>n+(Number(s.strength)||0),0),garrison=owned(owner).reduce((n,p)=>n+(Number(p.garrison)||0)*.18,0);
  return Math.max(1,fleets+moving+garrison);
}
function stxEWESPairRelation(a,b,delta){if(typeof adjustRelation==="function"){adjustRelation(a,b,delta);adjustRelation(b,a,delta)}}
function stxEWESSetLongTruce(a,b,duration){
  if(typeof stxWCCSetTruce==="function")stxWCCSetTruce(a,b,duration);
  const until=state.simTime+duration,ea=empire(a),eb=empire(b);if(ea)ea.peaceGuaranteedUntil=Math.max(Number(ea.peaceGuaranteedUntil||0),until);if(eb)eb.peaceGuaranteedUntil=Math.max(Number(eb.peaceGuaranteedUntil||0),until);
}
function stxEWESAddAgreement(kind,a,b,duration,extra={}){if(typeof stxDSAddAgreement==="function")return stxDSAddAgreement(kind,a,b,duration,{source:"negotiated-war-settlement",...extra});return null}

/* The old wartime reserve path converted regenerating planetary garrison directly
   into naval fleet records. Strategic-navy commissioning now already uses the same
   physical shipyard queue as the player, so AI reserve conversion is retired. */
if(typeof stxEWPAssembleReserveFleet==="function"){
  const STX_EWES_stxEWPAssembleReserveFleet=stxEWPAssembleReserveFleet;
  stxEWPAssembleReserveFleet=function(e,target){return !e||e.id===0?STX_EWES_stxEWPAssembleReserveFleet(e,target):null};
}

/* Prevent small rivals from stacking emergency commissions across several yards.
   One real replacement can be underway at 2–5 worlds; 6–7 world empires can run
   two only when they actually possess the shipyards to support them. */
if(typeof stxEWPQueueEmergencyFleet==="function"){
  const STX_EWES_stxEWPQueueEmergencyFleet=stxEWPQueueEmergencyFleet;
  stxEWPQueueEmergencyFleet=function(e,target){
    if(!e||e.id===0)return STX_EWES_stxEWPQueueEmergencyFleet(e,target);
    const profile=stxEWESProfile(e),cap=profile.capacity;if(!cap.yards||cap.queued>=profile.maxQueued)return false;
    return STX_EWES_stxEWPQueueEmergencyFleet(e,target);
  };
}

/* Offensive cadence now reflects empire scale. Existing built fleets remain real
   assets and may be committed, but small empires cannot launch them as an endless
   conveyor belt. Poor replacement stocks lengthen the recovery window further. */
const STX_EWES_launchWarCampaign=launchWarCampaign;
launchWarCampaign=function(e,foeId){
  if(!e||e.id===0)return STX_EWES_launchWarCampaign(e,foeId);
  const w=getWar(e.id,foeId);if(!w)return false;
  const s=stxEWESState(e),profile=stxEWESProfile(e),warAge=state.simTime-stxEWESWarStartedAt(w),recent=stxEWESRecentLaunches(e,profile.waveWindow);
  if(warAge<profile.mobilization)return false;
  if(state.simTime-s.lastOffensiveLaunchAt<profile.cooldown)return false;
  if(recent.length>=profile.waveCap)return false;
  if(stxEWESIncomingCount(e.id,foeId)>=profile.maxInbound)return false;
  const before=stxEWESIncomingCount(e.id,foeId),ok=STX_EWES_launchWarCampaign(e,foeId),after=stxEWESIncomingCount(e.id,foeId);
  if(ok||after>before){s.lastOffensiveLaunchAt=state.simTime;s.launches.push({at:state.simTime,foeId,worlds:profile.worlds,stage:profile.stage});stxEWESRecentLaunches(e,profile.waveWindow)}
  return ok;
};

function stxEWESSettlementCosts(enemyId){
  const rivalWorlds=owned(enemyId).length,playerPower=stxEWESPower(0),rivalPower=stxEWESPower(enemyId),ratio=playerPower/Math.max(1,rivalPower);
  const reparations=clamp(Math.round(18+rivalWorlds*5+Math.max(0,rivalPower-playerPower)*.22),18,72),commercial=clamp(8+rivalWorlds*2,10,24);
  return{reparations,commercial,ratio,playerPower,rivalPower};
}
function stxEWESSettlementMarkup(enemyId,war){
  if(!war)return"";const costs=stxEWESSettlementCosts(enemyId),age=state.simTime-stxEWESWarStartedAt(war),parity=costs.ratio>=.72&&costs.ratio<=1.38&&age>=30,losing=costs.ratio<1.15;
  return`<div class="section-label">WAR RESOLUTION OPTIONS</div><div class="subtle" style="margin-bottom:8px">Choose a settlement structure, not only whether the war continues.</div><div class="choice-row stx-ewes-settlements"><button class="choice-btn" data-ds-diplomacy="ewes-armistice" data-ds-rival="${enemyId}">Armistice · extended ceasefire</button>${losing?`<button class="choice-btn" data-ds-diplomacy="ewes-reparations" data-ds-rival="${enemyId}">Reparations Settlement · ${costs.reparations} cr</button>`:""}<button class="choice-btn" data-ds-diplomacy="ewes-commercial" data-ds-rival="${enemyId}">Commercial Peace · ${costs.commercial} cr</button>${parity?`<button class="choice-btn" data-ds-diplomacy="ewes-demobilize" data-ds-rival="${enemyId}">Mutual Demobilization</button>`:""}</div>`;
}
if(typeof stxDSDiplomacyPanel==="function"){
  const STX_EWES_stxDSDiplomacyPanel=stxDSDiplomacyPanel;
  stxDSDiplomacyPanel=function(enemyId){
    const html=STX_EWES_stxDSDiplomacyPanel(enemyId),war=getWar(0,enemyId);if(!war)return html;
    return html.replace(/<\/section>\s*$/,`${stxEWESSettlementMarkup(enemyId,war)}</section>`);
  };
}
function stxEWESFinishSettlement(enemyId,kind){
  const w=getWar(0,enemyId),player=empire(0),rival=empire(enemyId);if(!w||!player||!rival)return false;
  const costs=stxEWESSettlementCosts(enemyId),age=state.simTime-stxEWESWarStartedAt(w);
  let truce=STX_EWES_ARMISTICE_TRUCE,reason="negotiated armistice",toast="Armistice accepted",relationDelta=.02;
  if(kind==="ewes-reparations"){
    if(player.credits<costs.reparations){if(typeof showToast==="function")showToast(`Need ${costs.reparations} credits`);return false}
    player.credits-=costs.reparations;rival.credits=(Number(rival.credits)||0)+costs.reparations;truce=STX_EWES_REPARATIONS_TRUCE;reason="reparations settlement";toast=`Reparations paid · ${costs.reparations} cr`;relationDelta=.06;
  }else if(kind==="ewes-commercial"){
    if(player.credits<costs.commercial){if(typeof showToast==="function")showToast(`Need ${costs.commercial} credits`);return false}
    player.credits-=costs.commercial;rival.credits=(Number(rival.credits)||0)+costs.commercial;truce=STX_EWES_COMMERCIAL_TRUCE;reason="commercial peace settlement";toast="Commercial peace established";relationDelta=.04;
  }else if(kind==="ewes-demobilize"){
    if(costs.ratio<.72||costs.ratio>1.38||age<30){if(typeof showToast==="function")showToast("Mutual demobilization requires a sustained military stalemate");return false}
    truce=STX_EWES_DEMOBILIZATION_TRUCE;reason="mutual demobilization";toast="Both powers demobilize from the frontier";relationDelta=.025;
  }else if(kind!=="ewes-armistice")return false;
  endWar(w,reason);stxEWESSetLongTruce(0,enemyId,truce);stxEWESAddAgreement("non-aggression",0,enemyId,truce,{settlement:kind});
  if(kind==="ewes-commercial")stxEWESAddAgreement("trade-agreement",0,enemyId,180,{settlement:kind});
  stxEWESPairRelation(0,enemyId,relationDelta);
  [player,rival].forEach(e=>{const s=stxEWESState(e);s.lastOffensiveLaunchAt=state.simTime;s.launches=[];e.lastAttack=state.simTime;e.enemyWarPressure=e.enemyWarPressure||{};e.enemyWarPressure.defenseHoldUntil=Math.max(Number(e.enemyWarPressure.defenseHoldUntil)||0,state.simTime+18)});
  if(typeof galacticNews==="function")galacticNews(`${player.name.toUpperCase()} AND ${rival.name.toUpperCase()} REACH ${reason.toUpperCase()}`,`The war has ended through ${reason}. A ${Math.round(truce)}-second strategic truce now protects the settlement from immediate renewed attacks.`,"good");
  if(typeof showToast==="function")showToast(toast);if(typeof renderRivals==="function")renderRivals();if(typeof renderTransmissions==="function")renderTransmissions();if(typeof updateBadges==="function")updateBadges();if(typeof updateHud==="function")updateHud(true);return true;
}
if(typeof stxDSHandleDiplomacy==="function"){
  const STX_EWES_stxDSHandleDiplomacy=stxDSHandleDiplomacy;
  stxDSHandleDiplomacy=function(action,enemyId){return String(action||"").startsWith("ewes-")?stxEWESFinishSettlement(enemyId,action):STX_EWES_stxDSHandleDiplomacy(action,enemyId)};
}

/* A small empire that has spent its early offensive waves and lacks material for
   another replacement will sometimes seek an armistice instead of idling forever
   and then restarting the same attack loop. The player still chooses whether to
   accept; AI-vs-AI wars use the existing automatic limited-war settlement path. */
function stxEWESBattleBetween(a,b){return state.battles.some(x=>(x.attacker===a&&x.defender===b)||(x.attacker===b&&x.defender===a))}
function stxEWESMaybeOfferArmistice(w){
  if(!w?.active||!(w.a===0||w.b===0)||typeof stxDSAcceptPeace!=="function")return false;
  const enemyId=w.a===0?w.b:w.a,e=empire(enemyId);if(!e)return false;const profile=stxEWESProfile(e);if(profile.worlds>7)return false;
  const s=stxEWESState(e),age=state.simTime-stxEWESWarStartedAt(w),recent=stxEWESRecentLaunches(e,profile.waveWindow),exhaustedWaves=recent.length>=Math.max(2,profile.waveCap-1),strained=profile.capacity.equivalents<.35&&profile.capacity.active<=1;
  if(age<85||(!exhaustedWaves&&!strained)||stxEWESIncomingCount(enemyId,0)>0||stxEWESBattleBetween(0,enemyId)||state.simTime-s.lastArmisticeOfferAt<95)return false;
  state.deepSpacePeaceOffers=Array.isArray(state.deepSpacePeaceOffers)?state.deepSpacePeaceOffers:[];if(state.deepSpacePeaceOffers.some(o=>o.warId===w.id&&o.status==="pending"))return false;
  state.deepSpacePeaceOffers.push({id:`ewes-peace-${Math.floor(random()*1e9)}`,kind:"armistice",warId:w.id,from:enemyId,to:0,status:"pending",createdAt:state.simTime,expiresAt:state.simTime+50,goal:"war exhaustion and fleet replacement strain"});s.lastArmisticeOfferAt=state.simTime;
  if(typeof logEvent==="function")logEvent(`${e.name} requests an armistice after sustained fleet losses strained its early-war economy.`,"warning");if(typeof renderTransmissions==="function")renderTransmissions();if(typeof updateBadges==="function")updateBadges();return true;
}
if(typeof stxDSPeaceCard==="function"){
  const STX_EWES_stxDSPeaceCard=stxDSPeaceCard;
  stxDSPeaceCard=function(o){
    if(o?.kind!=="armistice")return STX_EWES_stxDSPeaceCard(o);const w=state.wars.find(x=>x.id===o.warId&&x.active);if(!w)return"";const esc=typeof stxDSEscape==="function"?stxDSEscape:String;
    return`<article class="transmission-card urgent stx-ds-peace"><div class="card-kicker">WAR EXHAUSTION // ${esc(empire(o.from)?.name||"Rival Power")}</div><div class="card-title"><strong>Armistice Proposal</strong><span class="news-time">${fmtEta(o.expiresAt-state.simTime)}</span></div><p class="card-copy">The rival reports that repeated fleet losses and replacement strain are making further offensives costly. They propose an extended ceasefire rather than another immediate wave.</p><div class="choice-row"><button class="choice-btn primary-choice" data-ds-peace="${o.id}" data-ds-peace-action="accept">Accept Armistice</button><button class="choice-btn danger-choice" data-ds-peace="${o.id}" data-ds-peace-action="continue">Continue the War</button></div></article>`;
  };
}
if(typeof stxDSAcceptPeace==="function"){
  const STX_EWES_stxDSAcceptPeace=stxDSAcceptPeace;
  stxDSAcceptPeace=function(offer,reason){const w=state.wars.find(x=>x.id===offer?.warId&&x.active),pair=w?[w.a,w.b]:null,kind=offer?.kind,result=STX_EWES_stxDSAcceptPeace(offer,reason||(kind==="armistice"?"war-exhaustion armistice":undefined));if(kind==="armistice"&&pair){stxEWESSetLongTruce(pair[0],pair[1],STX_EWES_ARMISTICE_TRUCE);stxEWESAddAgreement("non-aggression",pair[0],pair[1],STX_EWES_ARMISTICE_TRUCE,{settlement:"ai-armistice"});stxEWESPairRelation(pair[0],pair[1],.02)}return result};
}
if(typeof aiTick==="function"){
  const STX_EWES_aiTick=aiTick;
  aiTick=function(){const result=STX_EWES_aiTick();const s=state.stxEarlyWarPeaceReview||(state.stxEarlyWarPeaceReview={lastAt:-999});if(state.simTime-s.lastAt>=5){s.lastAt=state.simTime;state.wars.filter(w=>w.active&&(w.a===0||w.b===0)).forEach(stxEWESMaybeOfferArmistice)}return result};
}

globalThis.SpaceTyrantsEarlyWarBalance={
  profile:owner=>stxEWESProfile(typeof owner==="object"?owner:empire(owner)),
  replacementCapacity:owner=>stxEWESReplacementCapacity(typeof owner==="object"?owner:empire(owner)),
  settlementCosts:stxEWESSettlementCosts,
  recentLaunches:owner=>stxEWESRecentLaunches(typeof owner==="object"?owner:empire(owner),stxEWESProfile(typeof owner==="object"?owner:empire(owner)).waveWindow),
  maybeOfferArmistice:stxEWESMaybeOfferArmistice,
  constants:{fleetCost:{...STX_EWES_FLEET_COST},armistice:STX_EWES_ARMISTICE_TRUCE,reparations:STX_EWES_REPARATIONS_TRUCE,commercial:STX_EWES_COMMERCIAL_TRUCE,demobilization:STX_EWES_DEMOBILIZATION_TRUCE}
};
