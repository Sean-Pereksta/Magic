/* Space Tyrants — war campaign continuity and treaty stability.
   Keep manually committed invasion waves together, do not resolve a battle while
   tagged reinforcements for the losing side are still inbound, prevent immediate
   post-peace redeclarations, and make wartime command hands leave room for three
   normal investment/directive choices. */

const STX_WCC_TRUCE_DURATION=60;
const STX_WCC_SECURE_DURATION=12;
const STX_WCC_MAX_REINFORCEMENT_WAIT=60;

function stxWCCState(){
  const s=state.stxWarCampaignContinuity||(state.stxWarCampaignContinuity={truces:{}});
  s.truces=s.truces||{};
  return s;
}
function stxWCCPairKey(a,b){return [Number(a),Number(b)].sort((x,y)=>x-y).join(":")}
function stxWCCTruceUntil(a,b){return Number(stxWCCState().truces[stxWCCPairKey(a,b)]||0)}
function stxWCCSetTruce(a,b,duration=STX_WCC_TRUCE_DURATION){
  const until=state.simTime+Math.max(1,Number(duration)||STX_WCC_TRUCE_DURATION),key=stxWCCPairKey(a,b),s=stxWCCState();
  s.truces[key]=Math.max(Number(s.truces[key]||0),until);
  return s.truces[key];
}
function stxWCCManualPlanForBattle(b,p){
  const plans=empire(b?.attacker)?.invasionPlans||[],ids=new Set(b?.attackerFleetIds||[]);
  return plans.find(plan=>plan?.stxManualAllocation&&plan.targetId===p?.id&&(plan.stxAssignedFleetIds||[]).some(id=>ids.has(id)))||null;
}
function stxWCCCommittedInbound(b,p){
  if(!b||!p)return[];
  const plan=stxWCCManualPlanForBattle(b,p),assigned=new Set(plan?.stxAssignedFleetIds||[]);
  return state.ships.filter(s=>{
    if(s.to!==p.id||!["fleet","patrol"].includes(s.type))return false;
    if(s.battleId===b.id&&(s.owner===b.attacker||s.owner===b.defender))return true;
    return !!plan&&s.owner===b.attacker&&s.invasionPlanId===plan.id&&assigned.has(s.fleetId);
  });
}
function stxWCCNearestHome(owner,p){
  return owned(owner).filter(q=>q!==p&&!q.underAttack).sort((a,b)=>dist(a,p)-dist(b,p))[0]||owned(owner).find(q=>q!==p)||null;
}
function stxWCCDisengageArrival(s,p,reason="Ceasefire orders"){ 
  const f=fleetRecord(s?.fleetId),home=stxWCCNearestHome(s.owner,p),strength=Math.max(.1,Number(s?.strength)||Number(f?.strength)||1);
  if(f){
    f.strength=strength;f.location=null;f.status=home?`Withdrawing from ${p.name}`:"Disengaged after the campaign";
  }
  if(home&&typeof createShip==="function"){
    createShip("fleet",p,home,s.owner,{strength,fleetId:f?.id||s.fleetId,vesselName:f?.name||s.vesselName,retreat:true,speedBoost:1.2});
  }else if(f){
    f.destroyed=true;f.strength=0;f.status="Disbanded after total defeat";
  }
  if((s.owner===0||p.owner===0)&&typeof stxActivity==="function")stxActivity(`${s.vesselName||f?.name||"Fleet"} disengaged at ${p.name}: ${reason}.`,p.id,f?.id||null,"warning");
  return true;
}

/* Every negotiated peace creates a pair-specific truce. This is deliberately
   attached to endWar so status-quo offers, AI treaties and special peace paths
   all receive the same protection. */
const STX_WCC_endWar=endWar;
endWar=function(w,reason){
  const a=w?.a,b=w?.b,wasActive=!!w?.active,result=STX_WCC_endWar(w,reason);
  if(wasActive&&Number.isFinite(a)&&Number.isFinite(b)){
    const until=stxWCCSetTruce(a,b);w.stxTruceUntil=until;
    const ea=empire(a),eb=empire(b);if(ea)ea.peaceGuaranteedUntil=Math.max(Number(ea.peaceGuaranteedUntil||0),until);if(eb)eb.peaceGuaranteedUntil=Math.max(Number(eb.peaceGuaranteedUntil||0),until);
  }
  return result;
};

/* A treaty must be allowed to breathe. The final declaration wrapper runs after
   the diplomacy/deep-space layers, so automatic pressure cannot instantly reopen
   the same war. Player and AI alike are held to the treaty for this short window. */
const STX_WCC_declareWar=declareWar;
declareWar=function(a,b,reason="frontier tensions"){
  const until=stxWCCTruceUntil(a,b);
  if(until>state.simTime){
    if((a===0||b===0)&&typeof showToast==="function")showToast(`Peace treaty in force · ${Math.ceil(until-state.simTime)}s`);
    return null;
  }
  return STX_WCC_declareWar(a,b,reason);
};

/* Do not announce a victory while forces already committed to that exact battle
   (or to the player's manual invasion plan) are still on the way. The nearly
   exhausted side holds an evasive orbital posture until its committed wave gets
   there, up to one mandate window. Redirecting those ships removes them from the
   inbound set, so Concentrate Forces / a later order naturally calls the attack off. */
const STX_WCC_resolveBattle=resolveBattle;
resolveBattle=function(b,p,index){
  const inbound=stxWCCCommittedInbound(b,p),attackerInbound=inbound.some(s=>s.owner===b.attacker),defenderInbound=inbound.some(s=>s.owner===b.defender);
  const attackerWaiting=b.attackerStrength<=.6&&attackerInbound,defenderWaiting=b.defenderStrength<=.6&&defenderInbound,timedOut=b.elapsed>=b.maxDuration&&inbound.length>0;
  const waitNeeded=attackerWaiting||defenderWaiting||timedOut;
  if(waitNeeded){
    if(!Number.isFinite(b.stxContinuityWaitStarted))b.stxContinuityWaitStarted=state.simTime;
    if(state.simTime-b.stxContinuityWaitStarted<STX_WCC_MAX_REINFORCEMENT_WAIT){
      if(attackerWaiting)b.attackerStrength=Math.max(.65,b.attackerStrength);
      if(defenderWaiting)b.defenderStrength=Math.max(.65,b.defenderStrength);
      b.maxDuration=Math.max(b.maxDuration,b.elapsed+4);b.stxAwaitingCommittedReinforcements=true;p.underAttack=true;
      if(!b.stxContinuityNotified&&(b.attacker===0||b.defender===0)){
        b.stxContinuityNotified=true;
        if(typeof stxActivity==="function")stxActivity(`Battle at ${p.name} remains active while ${inbound.length} committed reinforcement group${inbound.length===1?"":"s"} closes on the system.`,p.id,null,"warning");
      }
      return;
    }
  }
  const attackerWon=b.attackerStrength>b.defenderStrength*1.04,winner=attackerWon?b.attacker:b.defender,loser=attackerWon?b.defender:b.attacker;
  const result=STX_WCC_resolveBattle(b,p,index);
  if(p?.owner===winner){
    p.stxSecuredOwner=winner;p.stxSecuredFrom=loser;p.stxSecuredUntil=state.simTime+STX_WCC_SECURE_DURATION;
  }
  return result;
};

/* Ships that arrive after a treaty or inside the short post-victory consolidation
   window turn back instead of silently opening a brand-new battle and flipping the
   planet seconds after the UI announced victory. */
const STX_WCC_resolveFleetArrival=resolveFleetArrival;
resolveFleetArrival=function(s,p){
  if(p&&s&&p.owner!==null&&p.owner!==s.owner){
    const truce=stxWCCTruceUntil(s.owner,p.owner);
    if(truce>state.simTime&&!empiresAtWar(s.owner,p.owner))return stxWCCDisengageArrival(s,p,"the peace treaty is still in force");
    if(p.stxSecuredOwner===p.owner&&p.stxSecuredFrom===s.owner&&Number(p.stxSecuredUntil||0)>state.simTime)return stxWCCDisengageArrival(s,p,"the previous battle is over and the system is consolidating");
  }
  return STX_WCC_resolveFleetArrival(s,p);
};

/* Wartime should not erase the empire-management half of the game. Preserve the
   two direct force controls, then deliberately fill three additional executable
   non-force choices, preferring economy/research/trade/infrastructure options. */
function stxWCCCivilPriority(c){
  const text=`${c?.cat||""} ${c?.title||""} ${c?.desc||""}`;
  return /econom|industr|factory|mining|trade|research|science|infrastructure|population|city|development|resource/i.test(text)?30:/military|war|fleet|defen/i.test(text)?-8:8;
}
const STX_WCC_stxFCAPostHand=stxFCAPostHand;
stxFCAPostHand=function(){
  STX_WCC_stxFCAPostHand();
  if($("commandModal").hidden||!stxFCAWars().length)return;
  const extras=[],ids=new Set();
  const add=c=>{const choice=stxFCAChoiceCandidate(c);if(!choice||ids.has(choice.id)||extras.length>=3)return;ids.add(choice.id);extras.push(choice)};
  state.commandChoices.forEach(add);
  if(extras.length<3){
    COMMANDS.map(c=>{let score=stxWCCCivilPriority(c);try{score+=Number(c.score?.()||0)+rand(-4,4)}catch(_){score+=0}return{c,score}}).sort((a,b)=>b.score-a.score).forEach(x=>add(x.c));
  }
  state.commandChoices=[stxFCAChoice("invade"),stxFCAChoice("concentrate"),...extras.slice(0,3)];
  if(state.commandSelected?.clear)state.commandSelected.clear();
  renderCommands();
};

/* Small public surface for deterministic regression tests and future tactical
   layers that need to respect treaty/campaign continuity. */
globalThis.SpaceTyrantsWarContinuity={
  truceUntil:stxWCCTruceUntil,
  setTruce:stxWCCSetTruce,
  committedInbound:stxWCCCommittedInbound,
  constants:{truce:STX_WCC_TRUCE_DURATION,secure:STX_WCC_SECURE_DURATION,maxWait:STX_WCC_MAX_REINFORCEMENT_WAIT}
};
