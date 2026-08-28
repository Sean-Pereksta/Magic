/* Space Tyrants — invasion fleet assembly fix.
   "Prepare an Invasion Fleet" must create usable active fleet formations now.
   Existing stationed military formations are regrouped first, planetary reserves
   are activated second, and new shipbuilding is only the final fallback. */

function stxIFAEngagedFleetIds(){
  if(typeof stxDGActiveBattleFleetIds==="function")return stxDGActiveBattleFleetIds();
  const ids=new Set();
  state.battles.forEach(b=>{
    (b.attackerFleetIds||[]).forEach(id=>ids.add(id));
    (b.defenderFleetIds||[]).forEach(id=>ids.add(id));
  });
  return ids;
}

function stxIFAStationedMilitaryFleets(){
  const engaged=stxIFAEngagedFleetIds();
  return state.fleets.filter(f=>{
    if(f.owner!==0||f.destroyed||engaged.has(f.id))return false;
    if(!f.location||state.ships.some(s=>s.fleetId===f.id))return false;
    const p=state.planets.find(q=>q.id===f.location);
    return !!p&&p.owner===0&&!p.underAttack&&(Number(f.strength)||0)>4;
  }).sort((a,b)=>{
    const ar=a.role==="fleet"||!a.role?1:0,br=b.role==="fleet"||!b.role?1:0;
    return br-ar||(Number(b.strength)||0)-(Number(a.strength)||0)||(b.veterans||0)-(a.veterans||0);
  });
}

function stxIFAReserveWorlds(){
  return playerWorlds().filter(p=>!p.underAttack&&(Number(p.garrison)||0)>20)
    .sort((a,b)=>(Number(b.garrison)||0)-(Number(a.garrison)||0)||borderThreat(a)-borderThreat(b));
}

function stxIFAMarkAssemblyDirective(source,planetId=null){
  const e=empire(0);if(!e)return;
  addDirective(e,"fleet",planetId,72);
  setModifier(e,"militaryMorale",1.06,54);
  if(typeof stxMERecordPath==="function")stxMERecordPath("invasionFleetAssembly",source,planetId,"Fleet ready for invasion orders");
}

function stxIFAAssembleActiveFleets(source="Prepare an invasion fleet"){
  const ready=typeof stxDGDeployableFleets==="function"?stxDGDeployableFleets():[];
  if(ready.length){
    ready.slice(0,2).forEach(f=>{
      const p=state.planets.find(q=>q.id===f.location);
      f.role="fleet";
      f.status=`Prepared for invasion${p?` at ${p.name}`:""}`;
      if(p)p.mandateGlow=1;
    });
    const p=state.planets.find(q=>q.id===ready[0].location);
    stxIFAMarkAssemblyDirective(source,p?.id||null);
    logEvent(`INVASION PREPARATION: ${Math.min(2,ready.length)} active battle fleet${ready.length===1?"":"s"} assigned to the invasion reserve.`,"good");
    if(typeof stxActivity==="function")stxActivity(`Active battle fleets were assigned to the invasion reserve${p?` at ${p.name}`:""}.`,p?.id||null,ready[0]?.id||null,"good");
    if(typeof stxRefreshFleetLocator==="function")stxRefreshFleetLocator();
    return Math.min(2,ready.length);
  }

  /* Patrol flotillas and other idle named formations are already real ships.
     Regroup them into battle-fleet duty instead of pretending the navy is empty. */
  const stationed=stxIFAStationedMilitaryFleets().slice(0,2);
  if(stationed.length){
    stationed.forEach(f=>{
      const p=state.planets.find(q=>q.id===f.location);
      f.role="fleet";
      f.status=`Regrouped for invasion${p?` at ${p.name}`:""}`;
      if(p)p.mandateGlow=1;
    });
    const p=state.planets.find(q=>q.id===stationed[0].location);
    stxIFAMarkAssemblyDirective(source,p?.id||null);
    logEvent(`INVASION PREPARATION: ${stationed.length} active naval formation${stationed.length===1?" was":"s were"} regrouped into invasion-capable battle fleets.`,"good");
    if(typeof stxActivity==="function")stxActivity(`${stationed.length} active naval formation${stationed.length===1?" was":"s were"} regrouped for invasion duty.`,p?.id||null,stationed[0]?.id||null,"good");
    if(typeof stxRefreshFleetLocator==="function")stxRefreshFleetLocator();
    return stationed.length;
  }

  /* If no named formation is idle, activate existing planetary military reserves
     immediately. This represents crews/ships already in service, not new hulls. */
  let made=0,firstPlanet=null,firstFleet=null;
  for(const p of stxIFAReserveWorlds().slice(0,2)){
    const available=Math.max(0,(Number(p.garrison)||0)-12);
    const strength=Math.min(16,available,Math.max(7,(Number(p.garrison)||0)*.30));
    if(strength<=4)continue;
    p.garrison=Math.max(12,p.garrison-strength);
    const f=registerFleet(0,p,strength,"fleet");
    if(!f){p.garrison+=strength;continue}
    f.location=p.id;
    f.role="fleet";
    f.status=`Reserve assembled for invasion at ${p.name}`;
    p.mandateGlow=1;
    made++;
    if(!firstPlanet){firstPlanet=p;firstFleet=f}
    if(typeof stxActivity==="function")stxActivity(`${f.name} activated from planetary reserves for invasion duty.`,p.id,f.id,"good");
  }
  if(made){
    stxIFAMarkAssemblyDirective(source,firstPlanet?.id||null);
    logEvent(`INVASION PREPARATION: ${made} named battle fleet${made===1?" was":"s were"} assembled immediately from active planetary reserves.`,"good");
    showToast(`${made} invasion fleet${made===1?"":"s"} assembled and active`);
    if(typeof stxRefreshFleetLocator==="function")stxRefreshFleetLocator();
    return made;
  }
  return 0;
}

/* The prerequisite card in mandate-economy previously jumped directly to a
   shipyard commission. Assemble existing forces first; preserve its original
   shipbuilding/shipyard fallback only when the empire truly has no usable force. */
if(typeof stxMEStartPrerequisite==="function"){
  const STX_IFA_startPrerequisite=stxMEStartPrerequisite;
  stxMEStartPrerequisite=function(kind,label){
    if(kind==="fleet"&&stxIFAAssembleActiveFleets(label||"Prepare an invasion fleet")>0)return true;
    return STX_IFA_startPrerequisite(kind,label);
  };
}

if(typeof stxMEPrerequisiteChoice==="function"){
  const STX_IFA_prerequisiteChoice=stxMEPrerequisiteChoice;
  stxMEPrerequisiteChoice=function(c){
    const choice=STX_IFA_prerequisiteChoice(c);
    if(choice?.title==="Prepare an Invasion Fleet"){
      choice.desc=`${c?.title||"This invasion"} needs an invasion-capable fleet. Regroup active naval formations or activate planetary reserves now; build new ships only if no usable forces exist.`;
      choice.effects=["Existing active fleets regroup first","Planetary reserves can become named fleets immediately","New shipbuilding is used only as a fallback"];
    }
    return choice;
  };
}

/* Keep the generic no-fleet command consistent with Prepare Invasion Fleet. */
if(typeof stxDGAssembleFleetChoice==="function"){
  const STX_IFA_assembleFleetChoice=stxDGAssembleFleetChoice;
  stxDGAssembleFleetChoice=function(){
    const choice=STX_IFA_assembleFleetChoice();if(!choice)return null;
    return {...choice,
      desc:"Assemble a real invasion-capable navy from active formations and planetary reserves before ordering new hull construction.",
      effects:["Existing active formations regroup immediately","Planetary reserves activate as named fleets","Shipyards commission a fleet only if needed"],
      apply:e=>{
        if(stxIFAAssembleActiveFleets("Assemble battle fleets")>0)return;
        if(typeof stxDIQueueFleetProgram==="function")stxDIQueueFleetProgram(e,1,"Battle-fleet assembly fallback",false);
      }
    };
  };
}

/* Invasion plans that are waiting for a fleet use the same assembly rule. If
   reserves can form a fleet now, the original mobilizer immediately sees that
   deployable formation and launches it instead of idling on a build queue. */
if(typeof stxMobilizeInvasionPlan==="function"){
  const STX_IFA_mobilizeInvasionPlan=stxMobilizeInvasionPlan;
  stxMobilizeInvasionPlan=function(plan,force=false){
    const target=state.planets.find(p=>p.id===plan?.targetId);
    if(plan&&target&&target.owner!==null&&target.owner!==0&&empiresAtWar(0,target.owner)&&!target.underAttack&&
       typeof stxDGHasDeployableFleet==="function"&&!stxDGHasDeployableFleet()){
      const made=stxIFAAssembleActiveFleets(`Invasion force for ${target.name}`);
      if(made>0){
        plan.status="fleet assembled";
        plan.lastFleetCommissionAt=state.simTime;
      }
    }
    return STX_IFA_mobilizeInvasionPlan(plan,force);
  };
}
