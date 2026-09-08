/* A declaration needs a remembered cause, a visible crisis and preparation.
   These checks also protect older declaration/arrival paths in the patch stack. */
function stxConflictPhase(t){return t>=78?"Crisis":t>=62?"Hostile":t>=48?"Tense":t>=36?"Competitive":t>=24?"Uneasy":"Neutral"}
function stxConflictThreshold(a){
  const pace={relaxed:1.2,standard:1,hard:.9,relentless:.78}[stxRDDifficultyId()]||1,age=state.simTime/pace;
  return age<60?99:age<180?95:Math.max(78,88-(empire(a)?.foreignPolicy?.aggression||0)*9);
}
function stxConflictCanDeclare(a,b,reason=""){
  if(a===b||!empire(a)||!empire(b)||!owned(a).length||!owned(b).length)return false;
  if(getWar(a,b))return true;const r=stxRDPair(a,b);if(!r)return false;
  if(state.simTime<Math.max(r.warCooldownUntil||0,stxWCCTruceUntil(a,b)))return false;
  if(a===0)return /imperial|invasion|embargo enforcement|attack on.*embargo|direct.*declaration/i.test(reason);
  if(stxDSAgreementBetween("non-aggression",a,b)||(typeof stxPolicyHas==="function"&&stxPolicyHas(b,"diplomatic-reassurance")))return false;
  const p=empire(a).foreignPolicy||{},cause=r.grievances.some(g=>g.holder===a&&state.simTime-g.time<240&&(g.severity>=2||/border|embargo|military|frontier|treaty/.test(g.type)))||r.contestedWorlds.length>0;
  return relation(a,b)<=-.35&&r.tension>=stxConflictThreshold(a)&&cause&&Number.isFinite(r.stxCrisisWarnedAt)&&state.simTime-r.stxCrisisWarnedAt>=25&&Number.isFinite(r.stxMobilizedAt)&&state.simTime-r.stxMobilizedAt>=18&&(stxRDThreatSupport(a,b)>=.5||((empire(a).warDamage||0)>.6&&p.aggression>.7));
}
const STX_CONFLICT_grievance=stxRDAddGrievance;
stxRDAddGrievance=function(holder,against,type,text,severity=1,targetId=null){
  const r=stxRDPair(holder,against),before=r?.tension||0,g=STX_CONFLICT_grievance(holder,against,type,text,severity,targetId);if(!g||!r)return g;
  const p=empire(holder)?.foreignPolicy||{},factor=.55+(p.aggression||0)*.6+(/territor|colon|frontier/.test(type)?(p.expansionism||0)*.25:0);
  r.tension=clamp(before+(r.tension-before)*factor,0,100);g.weight*=factor;return g;
};
function stxConflictEncounter(s,place){
  if(!place||place.owner==null||place.owner===s.owner)return false;
  const other=place.owner,r=stxRDPair(s.owner,other),f=fleetRecord(s.fleetId);
  if(r&&state.simTime-(r.stxLastStandoffAt??-999)>=35&&state.simTime>=(r.warCooldownUntil||0)){
    r.stxLastStandoffAt=state.simTime;stxRDAddGrievance(other,s.owner,"border standoff",`Uninvited fleet at ${place.name}`,2,place.id);
    state.rivalDiplomacy.incidents.unshift({id:stxRDId("stand"),kind:"standoff",from:other,to:s.owner,targetId:place.id,status:s.owner===0?"pending":"withdrawn",createdAt:state.simTime,expiresAt:state.simTime+45,fleetId:f?.id,title:`Standoff at ${place.name}`});
    state.rivalDiplomacy.incidents=state.rivalDiplomacy.incidents.slice(0,40);
    galacticNews(`STANDOFF AT ${place.name.toUpperCase()}`,`${empire(s.owner).name} and ${empire(other).name} disputed fleet access. Weapons remain silent; the visiting fleet is withdrawing.`,"warning",place.sponsorPlanetId||place.id);
  }
  const home=owned(s.owner).filter(p=>!p.underAttack).sort((a,b)=>dist(a,place)-dist(b,place))[0];
  if(!home){s.stxArrivalBlocked="No safe port for peaceful withdrawal";return false}
  const returning=createShip("fleet",home,home,s.owner,{strength:s.strength,fleetId:f?.id||s.fleetId,vesselName:f?.name||s.vesselName,retreat:true,stxPeacefulWithdrawal:true});
  if(!returning){s.stxArrivalBlocked="Waiting for withdrawal transport capacity";return false}
  returning.x=returning.startX=place.x;returning.y=returning.startY=place.y;returning.distance=Math.max(1,dist(place,home));if(f)f.status="Withdrawing from standoff";return true;
}
function stxConflictRespondIncident(id,action){
  const q=state.rivalDiplomacy.incidents.find(q=>q.id===id&&q.status==="pending"&&q.to===0);if(!q||state.simTime>=q.expiresAt||!["withdraw","hold","escalate"].includes(action))return false;
  if(action==="withdraw")stxRDAddCooperation(q.from,q.to,"peaceful withdrawal","The visiting fleet respected the frontier",9);
  else stxRDAddGrievance(q.from,q.to,action==="hold"?"maintained frontier claim":"escalation threat",action==="hold"?"The Mandate maintained its claim":"The Mandate threatened escalation",action==="hold"?2:5,q.targetId);
  q.status=action;stxActionComplete(q);showToast(action==="withdraw"?"Withdrawal confirmed; tension eased":"Diplomatic position recorded");renderTransmissions();return true;
}
function stxConflictTick(){
  for(const r of Object.values(state.rivalDiplomacy?.relations||{})){
    const dt=clamp(state.simTime-(r.stxPressureAt??state.simTime),0,10);r.stxPressureAt=state.simTime;
    const {a,b}=r;if(getWar(a,b)||!owned(a).length||!owned(b).length)continue;
    if(state.simTime<(r.warCooldownUntil||0)){r.tension=Math.max(0,r.tension-dt*.07);continue}
    const pa=empire(a).foreignPolicy||{},pb=empire(b).foreignPolicy||{},border=Math.min(...owned(a).flatMap(x=>owned(b).map(y=>dist(x,y))))<900;
    const fleets=stxRDBorderFleets(a,b).length+stxRDBorderFleets(b,a).length,bases=(state.deepSpaceBases||[]).filter(x=>x.type==="military"&&x.status==="operational"&&[a,b].includes(x.owner)&&owned(x.owner===a?b:a).some(p=>dist(x,p)<850)).length;
    const embargo=!!stxDSAgreementBetween("embargo",a,b),trade=!!stxDSAgreementBetween("trade-agreement",a,b)||state.ships.some(s=>s.commercial&&s.crossBorder&&((s.owner===a&&s.tradePartner===b)||(s.owner===b&&s.tradePartner===a)));
    const powerful=Math.max(owned(a).length,owned(b).length)/Math.max(1,Math.min(owned(a).length,owned(b).length))>2.6;
    const pressure=((border?.008:0)+(r.contestedWorlds.length?.025:0)+Math.min(5,fleets)*.012+bases*.025+(embargo?.045:0)+(powerful&&border?.02:0))*(.55+((pa.aggression||0)+(pb.aggression||0))*.25);
    const relief=trade?.022+((pa.commercialism||0)+(pb.commercialism||0))*.012:pressure?0:.025,early=state.simTime<60?.08:state.simTime<180?.4:1;
    r.tension=clamp(r.tension+dt*(pressure*early-relief),0,100);
    if(pressure>.04&&r.tension>45&&state.simTime-(r.stxPressureGrievanceAt??-999)>70){r.stxPressureGrievanceAt=state.simTime;const holder=(pa.aggression||0)>(pb.aggression||0)?a:b;stxRDAddGrievance(holder,holder===a?b:a,"frontier military pressure",embargo?"An embargo is hardening the border dispute":"Military buildup is straining frontier relations",2)}
    const phase=stxConflictPhase(r.tension);if(phase!==r.stxConflictPhase&&r.tension>=36)galacticNews(`${phase.toUpperCase()} RELATIONS`,`${empire(a).name} and ${empire(b).name}: ${phase.toLowerCase()} relations after accumulating frontier pressure. Diplomacy can still reduce tension.`,"warning");r.stxConflictPhase=phase;
    if(r.tension>=62&&!Number.isFinite(r.stxMobilizedAt)){r.stxMobilizedAt=state.simTime;r.borderPosture="Mobilizing";stxRDMobilize(a===0?b:a,a===0?a:b,false)}
    if(r.tension>=78&&!Number.isFinite(r.stxCrisisWarnedAt)){r.stxCrisisWarnedAt=state.simTime;galacticNews("DIPLOMATIC CRISIS",`${empire(a).name} and ${empire(b).name} are approaching open conflict. Fleets are preparing; cooperation or withdrawal can still avert war.`,"warning")}
    if(r.tension<55){delete r.stxCrisisWarnedAt;delete r.stxMobilizedAt}
  }
  for(const q of state.rivalDiplomacy?.incidents||[])if(q.kind==="standoff"&&q.status==="pending"&&state.simTime>=q.expiresAt)q.status="withdrawn";
}
function stxConflictCeasefire(w){
  for(const list of [state.battles,state.deepSpaceBattles||[]])for(const b of [...list]){
    if(!((b.attacker===w.a&&b.defender===w.b)||(b.attacker===w.b&&b.defender===w.a)))continue;
    const p=b.baseId?stxDSBase(b.baseId):state.planets.find(p=>p.id===b.planetId);if(!p)continue;
    for(const side of ["attacker","defender"]){const fs=(b[side+"FleetIds"]||[]).map(fleetRecord).filter(Boolean),total=fs.reduce((n,f)=>n+Math.max(0,f.strength),0)||1;
      for(const f of fs){f.strength=Math.max(0,b[side+"Strength"])*Math.max(0,f.strength)/total;if(f.strength<=.6){f.destroyed=true;continue}f.lastX=p.x;f.lastY=p.y;
        if(side==="defender"){if(b.baseId)stxDSDockFleet(f,p);else{f.location=p.id;f.status=`Stationed at ${p.name}`}}
        else stxDSReturnFleet(f,p,f.owner,"Withdrawing under ceasefire");
      }
    }
    p.underAttack=false;if(!b.baseId)p.garrison=Math.max(p.garrison||0,b.defenderStrength*.58);list.splice(list.indexOf(b),1);
  }
}
const STX_CONFLICT_endWar=endWar;
endWar=function(w,...args){const active=w?.active,result=STX_CONFLICT_endWar(w,...args);if(active){const r=stxRDPair(w.a,w.b);r.warCooldownUntil=Math.max(r.warCooldownUntil||0,state.simTime+180);r.stxExhaustionUntil=state.simTime+180;r.tension=Math.min(28,r.tension);delete r.stxCrisisWarnedAt;delete r.stxMobilizedAt;r.stxConflictPhase=stxConflictPhase(r.tension);stxWCCSetTruce(w.a,w.b,180);stxConflictCeasefire(w)}return result};
globalThis.SpaceTyrantsConflict={canDeclare:stxConflictCanDeclare,tick:stxConflictTick,encounter:stxConflictEncounter,respondIncident:stxConflictRespondIncident,phase:stxConflictPhase};
