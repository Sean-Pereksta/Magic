/* Space Tyrants — rival pressure, diplomatic memory, physical expansion,
   visible mobilization, conflict causes, and interactive foreign policy.

   This is a final additive runtime layer. The core remains authoritative for
   simulation, fleets, freight, colonies, battles, treaties, and saves. This
   module decides why and when rivals use those physical systems. */

const STX_RD_VERSION=1;
const STX_RD_PLAYER_GRACE=95;
const STX_RD_WAR_COOLDOWN=145;
const STX_RD_REQUEST_LIMIT=2;
const STX_RD_RESOURCES=["iron","titanium","rare","silicates","helium","components","equipment"];
const STX_RD_DEFAULT_DIFFICULTY="standard";
const STX_RD_DIFFICULTIES={
  relaxed:{label:"Relaxed",description:"Rivals expand slowly and usually stop after a compact regional realm.",policyBias:-.12,strongShare:.17,cooldown:1.35,worldCap:4,intentBonus:-.04,development:1.18,reach:1750},
  standard:{label:"Standard",description:"Mixed rival personalities create a steady, competitive colonial race.",policyBias:0,strongShare:.34,cooldown:1,worldCap:5,intentBonus:0,development:1,reach:2100},
  hard:{label:"Hard",description:"More rivals prioritize territory, develop frontiers faster, and reach farther.",policyBias:.12,strongShare:.5,cooldown:.78,worldCap:7,intentBonus:.16,development:.8,reach:2450},
  relentless:{label:"Relentless",description:"Most rivals race for neutral worlds and sustain large expansion programs.",policyBias:.22,strongShare:.67,cooldown:.6,worldCap:9,intentBonus:.3,development:.66,reach:2850}
};
let stxRDDeclarationContext=null;

function stxRDEscape(value){return String(value??"").replace(/[&<>\"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"})[c])}
function stxRDId(prefix){return `${prefix}${Math.floor(random()*1e9)}`}
function stxRDHash(text){let h=2166136261;for(const c of String(text)){h^=c.charCodeAt(0);h=Math.imul(h,16777619)}return h>>>0}
function stxRDUnit(seed,salt=0){let x=(seed+Math.imul(salt+1,2654435761))>>>0;x^=x>>>16;x=Math.imul(x,2246822507);x^=x>>>13;return(x>>>0)/4294967295}
function stxRDPairKey(a,b){return a<b?`${a}:${b}`:`${b}:${a}`}
function stxRDRecent(items,limit=5){return(items||[]).filter(x=>state.simTime-(x.time||0)<380).slice(0,limit)}
function stxRDDifficultyId(value=state.enemyExpansionDifficulty){return STX_RD_DIFFICULTIES[value]?value:STX_RD_DEFAULT_DIFFICULTY}
function stxRDDifficultyProfile(value=state.enemyExpansionDifficulty){return STX_RD_DIFFICULTIES[stxRDDifficultyId(value)]}

function stxRDPolicyArchetype(p){
  if(p.expansionism>=.72&&p.aggression>=.52)return"Expansionist Power";
  if(p.aggression>=.7)return"Militarist Rival";
  if(p.commercialism>=.68)return"Merchant League";
  if(p.caution>=.7&&p.expansionism>=.48)return"Cautious Hegemon";
  if(p.caution>=.68)return"Defensive State";
  if(p.opportunism>=.72)return"Opportunistic Power";
  if(p.expansionism<=.3&&p.commercialism<=.48)return"Isolationist State";
  return p.expansionism>=.55?"Regional Competitor":"Pragmatic State";
}
function stxRDBuildPolicy(e,strongExpansionist=false){
  const seed=stxRDHash(`${e.name}:${e.id}:${e.doctrine}`),u=n=>stxRDUnit(seed,n),doctrine=e.doctrine,difficulty=e.id===0?STX_RD_DIFFICULTIES.standard:stxRDDifficultyProfile(),baseExpansion=strongExpansionist ? .74+u(1)*.15 : clamp(.2+u(1)*.42+(doctrine==="expansion" ? .1 : 0),.14,.66);
  const p={
    expansionism:clamp(baseExpansion+difficulty.policyBias,.08,.96),
    aggression:clamp((Number(e.aggression)||.45)*.72+u(2)*.28+(doctrine==="military" ? .08 : 0),.14,.9),
    pride:clamp(.24+u(3)*.64,.18,.9),patience:clamp(.24+u(4)*.62,.2,.9),
    opportunism:clamp(.2+u(5)*.68,.16,.9),
    caution:clamp(.78-(Number(e.aggression)||.45)*.48+u(6)*.3+(doctrine==="fortress" ? .12 : 0),.14,.92),
    commercialism:clamp(.18+u(7)*.58+(doctrine==="commerce" ? .24 : 0),.12,.94)
  };
  p.archetype=stxRDPolicyArchetype(p);return p;
}
function stxRDPolicyDescriptors(e){
  const p=e?.foreignPolicy;if(!p)return[];
  const choices=[
    [p.expansionism,p.expansionism>=.7?"Expansionist":"Reserved"],
    [p.aggression,p.aggression>=.66?"Militaristic":"Measured"],
    [p.pride,p.pride>=.62?"Proud":"Pragmatic"],
    [p.patience,p.patience>=.62?"Patient":"Impatient"],
    [p.opportunism,p.opportunism>=.66?"Opportunistic":"Steadfast"],
    [p.caution,p.caution>=.66?"Cautious":"Bold"],
    [p.commercialism,p.commercialism>=.66?"Commercial":"Strategic"]
  ];
  return choices.sort((a,b)=>Math.abs(b[0]-.5)-Math.abs(a[0]-.5)).slice(0,3).map(x=>x[1]);
}
function stxRDAssignPolicies(){
  const rivals=state.empires.filter(e=>e.id!==0),difficulty=stxRDDifficultyProfile(),strongCount=clamp(Math.round(rivals.length*difficulty.strongShare),1,rivals.length);
  const strong=new Set(rivals.slice().sort((a,b)=>{
    const av=stxRDHash(a.name)%100+(a.doctrine==="expansion"?45:0),bv=stxRDHash(b.name)%100+(b.doctrine==="expansion"?45:0);return bv-av
  }).slice(0,strongCount).map(e=>e.id));
  state.empires.forEach(e=>{if(!e.foreignPolicy||e.foreignPolicy.version!==STX_RD_VERSION){e.foreignPolicy={...stxRDBuildPolicy(e,strong.has(e.id)),version:STX_RD_VERSION}}else{e.foreignPolicy.archetype=e.foreignPolicy.archetype||stxRDPolicyArchetype(e.foreignPolicy)}});
}

function stxRDNewState(){return{version:STX_RD_VERSION,difficulty:stxRDDifficultyId(),createdAt:state.simTime,graceUntil:state.simTime+STX_RD_PLAYER_GRACE,relations:{},requests:[],incidents:[],contestedWorlds:[],agreements:[],nextPlayerRequestAt:state.simTime+rand(115,155),nextAIRequestAt:state.simTime+rand(130,180),lastTickAt:state.simTime}}
function stxRDPair(a,b){
  const d=state.rivalDiplomacy,key=stxRDPairKey(a,b);if(!d)return null;
  if(!d.relations[key]){
    const rel=relation(a,b);d.relations[key]={a:Math.min(a,b),b:Math.max(a,b),tension:clamp(22-rel*26,4,58),goodwill:clamp(8+rel*28,0,40),grievances:[],cooperation:[],contestedWorlds:[],refusedRequests:0,ignoredRequests:0,escalationStage:0,borderPosture:"Quiet",lastIncidentAt:-999,lastDemandAt:-999,lastPressureAt:-999,lastWarAt:-999,lastPeaceAt:-999,warCooldownUntil:-999,ultimatumRejectedAt:-999,nextRequestAt:state.simTime+rand(90,145)}
  }
  const r=d.relations[key];r.grievances=Array.isArray(r.grievances)?r.grievances:[];r.cooperation=Array.isArray(r.cooperation)?r.cooperation:[];r.contestedWorlds=Array.isArray(r.contestedWorlds)?r.contestedWorlds:[];r.tension=Number.isFinite(r.tension)?r.tension:20;r.goodwill=Number.isFinite(r.goodwill)?r.goodwill:0;r.refusedRequests=Number.isFinite(r.refusedRequests)?r.refusedRequests:0;r.ignoredRequests=Number.isFinite(r.ignoredRequests)?r.ignoredRequests:0;r.escalationStage=Number.isFinite(r.escalationStage)?r.escalationStage:0;r.warCooldownUntil=Number.isFinite(r.warCooldownUntil)?r.warCooldownUntil:-999;r.ultimatumRejectedAt=Number.isFinite(r.ultimatumRejectedAt)?r.ultimatumRejectedAt:-999;return r;
}
function stxRDEnsureState(reset=false){
  if(!state.empires?.length)return null;
  const storedDifficulty=!reset&&(state.rivalDiplomacy?.difficulty||empire(0)?.rivalDiplomacy?.difficulty);state.enemyExpansionDifficulty=stxRDDifficultyId(storedDifficulty||state.enemyExpansionDifficulty);
  stxRDAssignPolicies();
  const saved=!reset&&empire(0)?.rivalDiplomacy;
  if(reset||!state.rivalDiplomacy||state.rivalDiplomacy.version!==STX_RD_VERSION)state.rivalDiplomacy=saved&&saved.version===STX_RD_VERSION?saved:stxRDNewState();
  const d=state.rivalDiplomacy;d.difficulty=stxRDDifficultyId(d.difficulty||state.enemyExpansionDifficulty);state.enemyExpansionDifficulty=d.difficulty;d.relations=d.relations||{};d.requests=Array.isArray(d.requests)?d.requests:[];d.incidents=Array.isArray(d.incidents)?d.incidents:[];d.contestedWorlds=Array.isArray(d.contestedWorlds)?d.contestedWorlds:[];d.agreements=Array.isArray(d.agreements)?d.agreements:[];
  for(let a=0;a<state.empires.length;a++)for(let b=a+1;b<state.empires.length;b++)stxRDPair(a,b);
  state.wars.forEach(w=>{if(!w.warCause)w.warCause={type:w.reason||"Legacy Conflict",detail:`An ongoing war inherited from an earlier save over ${w.reason||"an unresolved dispute"}.`,recordedAt:w.startedAt||state.simTime};w.aggressor=Number.isFinite(w.aggressor)?w.aggressor:w.a;if(!Array.isArray(w.strategicObjectives)){const target=stxRDObjective(w.aggressor,w.aggressor===w.a?w.b:w.a,w.warCause);w.strategicObjectives=target?[{planetId:target.id,label:target.name,type:w.warCause.type,status:"active"}]:[]}});
  empire(0).rivalDiplomacy=d;stxRDRefreshDifficultySelector();return d;
}
function stxRDPersist(){if(state.empires?.length&&state.rivalDiplomacy){state.rivalDiplomacy.difficulty=stxRDDifficultyId();empire(0).rivalDiplomacy=state.rivalDiplomacy}}

const STX_RD_generateGalaxy=generateGalaxy;
generateGalaxy=function(){STX_RD_generateGalaxy();state.rivalDiplomacy=null;stxRDEnsureState(true);const difficulty=stxRDDifficultyProfile();logEvent(`Rival expansion difficulty: ${difficulty.label}. ${difficulty.description}`,"warning")};
const STX_RD_loadGame=loadGame;
loadGame=function(){const ok=STX_RD_loadGame();if(ok){state.rivalDiplomacy=empire(0)?.rivalDiplomacy||null;stxRDEnsureState(false)}return ok};
const STX_RD_saveGame=saveGame;
saveGame=function(notify=true){stxRDPersist();return STX_RD_saveGame(notify)};

function stxRDAddGrievance(holder,against,type,text,severity=1,targetId=null){
  const r=stxRDPair(holder,against);if(!r)return null;const repeated=r.grievances.filter(g=>g.holder===holder&&g.type===type&&state.simTime-g.time<240).length,weight=(3+severity*3)*(1+Math.min(2,repeated)*.35),g={id:stxRDId("gr"),holder,against,type,text,severity,weight,time:state.simTime,targetId};
  r.grievances.unshift(g);r.grievances=r.grievances.slice(0,18);r.tension=clamp(r.tension+weight,0,100);r.goodwill=Math.max(0,r.goodwill-weight*.35);r.lastIncidentAt=state.simTime;adjustRelation(holder,against,-Math.min(.16,weight/360));return g;
}
function stxRDAddCooperation(a,b,type,text,value=7){
  const r=stxRDPair(a,b);if(!r)return;const c={id:stxRDId("co"),a,b,type,text,value,time:state.simTime};r.cooperation.unshift(c);r.cooperation=r.cooperation.slice(0,14);r.goodwill=clamp(r.goodwill+value,0,100);r.tension=Math.max(0,r.tension-value*.72);adjustRelation(a,b,Math.min(.14,value/110));
}
function stxRDActiveAgreement(kind,by,planetId=null,withId=null){return state.rivalDiplomacy?.agreements.find(a=>a.active!==false&&a.kind===kind&&a.by===by&&(planetId===null||a.planetId===planetId)&&(withId===null||a.with===withId)&&a.expiresAt>state.simTime)}
function stxRDAddAgreement(kind,by,withId,duration,extra={}){
  const a={id:stxRDId("ag"),kind,by,with:withId,createdAt:state.simTime,expiresAt:state.simTime+duration,active:true,...extra};state.rivalDiplomacy.agreements.push(a);return a;
}
function stxRDCancelExpansion(owner,targetId){owned(owner).forEach(p=>{if(p.expansionProject?.targetId===targetId){p.expansionProject=null;p.mandateGlow=1}})}

function stxRDRegisterContest(p,a,b,reason="competing colonial interest"){
  if(!p||a===b)return null;const d=state.rivalDiplomacy,key=`${p.id}:${stxRDPairKey(a,b)}`;let c=d.contestedWorlds.find(x=>x.key===key&&x.status==="active");
  if(!c){c={id:stxRDId("cw"),key,planetId:p.id,a:Math.min(a,b),b:Math.max(a,b),claims:[a,b],status:"active",reason,createdAt:state.simTime,lastChangedAt:state.simTime};d.contestedWorlds.unshift(c);d.contestedWorlds=d.contestedWorlds.slice(0,24);const r=stxRDPair(a,b);if(!r.contestedWorlds.includes(p.id))r.contestedWorlds.unshift(p.id);r.tension=clamp(r.tension+5,0,100);if(a===0||b===0)galacticNews(`${empire(a===0?b:a).name.toUpperCase()} ASSERTS CLAIM TO ${p.name.toUpperCase()}`,`${empire(a===0?b:a).name} has identified ${p.name} as a strategic frontier objective. Both powers now have a visible interest in the world.`,"warning",p.id)}
  else{c.claims=[...new Set([...c.claims,a,b])];c.lastChangedAt=state.simTime}return c;
}
function stxRDContestFor(a,b){return state.rivalDiplomacy?.contestedWorlds.find(c=>c.status==="active"&&((c.a===a&&c.b===b)||(c.a===b&&c.b===a)))||null}
function stxRDSharedFrontierWorld(a,b){
  const aw=owned(a),bw=owned(b);if(!aw.length||!bw.length)return null;
  return state.planets.filter(p=>p.owner===null&&!stxRDActiveAgreement("non-colonization",a,p.id,b)&&!stxRDActiveAgreement("non-colonization",b,p.id,a)).map(p=>{const da=Math.min(...aw.map(x=>dist(x,p))),db=Math.min(...bw.map(x=>dist(x,p))),value=Math.max(...Object.values(p.quality||{}))*20+Object.values(p.quality||{}).reduce((n,v)=>n+v,0)*3;return{p,da,db,score:value-Math.abs(da-db)*.05-(da+db)*.012}}).filter(x=>x.da<1150&&x.db<1150).sort((x,y)=>y.score-x.score)[0]?.p||null;
}
function stxRDClaimFromExpansion(e,source,target){
  const rivals=state.empires.filter(x=>x.id!==e.id&&owned(x.id).length).map(x=>({id:x.id,d:Math.min(...owned(x.id).map(p=>dist(p,target)))})).filter(x=>x.d<1050).sort((a,b)=>a.d-b.d);
  const otherProject=state.planets.find(p=>p.owner!==e.id&&p.expansionProject?.targetId===target.id);const rivalId=otherProject?.owner??rivals[0]?.id;
  if(rivalId!==undefined)stxRDRegisterContest(target,e.id,rivalId,"active colony expeditions and overlapping frontier claims");
  if(e.id!==0&&(rivalId===0||Math.min(...playerWorlds().map(p=>dist(p,target)))<900))galacticNews(`${e.name.toUpperCase()} LAUNCHES EXPANSION TOWARD ${target.name.toUpperCase()}`,`${source.name} has opened volunteer rolls and begun assembling a physical colony expedition. The settlement will not exist until its vessel arrives.`,"warning",target.id);
}
const STX_RD_startExpansionProject=startExpansionProject;
startExpansionProject=function(e,source,target,directed=false){
  if(stxRDActiveAgreement("non-colonization",e?.id,target?.id,null)){if(e?.id===0)showToast(`${target.name} is protected by a temporary non-colonization agreement`);return false}
  const ok=STX_RD_startExpansionProject(e,source,target,directed);if(ok)stxRDClaimFromExpansion(e,source,target);return ok;
};

function stxRDExpansionScore(e,p,sources){
  const nearest=Math.min(...sources.map(s=>dist(s,p))),quality=Object.values(p.quality||{}),resource=Math.max(...quality)*25+quality.reduce((n,v)=>n+v,0)*4,neighbors=state.planets.filter(q=>q!==p&&dist(q,p)<850).length,choke=neighbors>=4?24:0,frontier=owned(e.id).some(q=>dist(q,p)<850)?32:0,shipyard=(p.capacity||.2)*55+(p.quality?.titanium||1)*8;return resource+choke+frontier+shipyard-nearest*.055;
}
function stxRDExpansionRules(e,worldCount=owned(e.id).length){
  const p=e.foreignPolicy||stxRDBuildPolicy(e,false),difficulty=e.id===0?STX_RD_DIFFICULTIES.standard:stxRDDifficultyProfile();return{maxWorlds:Math.max(worldCount,difficulty.worldCap+Math.round(p.expansionism*2)),cooldown:Math.max(24,(42+(1-p.expansionism)*112)*difficulty.cooldown),intent:clamp(.1+p.expansionism*.34+difficulty.intentBonus,.06,.94),reach:difficulty.reach,development:difficulty.development};
}
const STX_RD_attemptExpansion=attemptExpansion;
attemptExpansion=function(e){
  if(!e||e.id===0)return STX_RD_attemptExpansion(e);
  const p=e.foreignPolicy||stxRDBuildPolicy(e,false),worlds=owned(e.id),rules=stxRDExpansionRules(e,worlds.length),active=worlds.some(x=>x.expansionProject)||state.ships.some(s=>s.owner===e.id&&s.type==="colony");if(active)return;
  if(worlds.length>=rules.maxWorlds)return;
  if(state.simTime-(e.lastExpansion||0)<rules.cooldown)return;
  const wants=worlds.length<3||p.expansionism>=.82||random()<rules.intent;if(!wants)return;
  const sources=worlds.filter(x=>!x.underAttack&&!x.expansionProject&&x.pop>.06).sort((a,b)=>b.pop-a.pop);if(!sources.length)return;
  const targets=state.planets.filter(x=>x.owner===null&&!stxRDActiveAgreement("non-colonization",e.id,x.id,null)).map(x=>({p:x,score:stxRDExpansionScore(e,x,sources)})).sort((a,b)=>b.score-a.score);const target=targets[0]?.p;if(!target)return;
  const source=sources.sort((a,b)=>dist(a,target)-dist(b,target))[0];if(dist(source,target)>rules.reach)return;if(startExpansionProject(e,source,target,true)){addDirective(e,"claim",target.id,115);e.lastExpansion=state.simTime}
};

function stxRDBorderFleets(owner,foe){
  const foeWorlds=owned(foe);if(!foeWorlds.length)return[];
  return state.fleets.filter(f=>f.owner===owner&&!f.destroyed&&f.location).map(f=>({f,p:state.planets.find(x=>x.id===f.location)})).filter(x=>x.p&&Math.min(...foeWorlds.map(q=>dist(x.p,q)))<720);
}
function stxRDRequestResource(from,to){const needs=empireNeed(from).filter(x=>STX_RD_RESOURCES.includes(x.resource)),available=needs.filter(x=>empireResource(to,x.resource)>=35);return(available[0]||needs[0])?.resource||"titanium"}
function stxRDRequestContext(from,to,kind,target=null,resource=null,amount=0){
  if(kind==="territorial")return{title:`Territorial Request — ${target.name}`,message:`${empire(from).name} requests that ${empire(to).name} abandon colonization plans for ${target.name}.`,reason:"Both powers consider the world strategically important.",consequence:"Refusal may turn the shared claim into a formal grievance.",context:`${target.name} lies near both frontiers and has valuable development potential.`,actionKind:"nonColonization",severity:3};
  if(kind==="military")return{title:"Frontier Fleet Withdrawal",message:`${empire(from).name} requests that nearby ${empire(to).name} fleets withdraw from the shared frontier.`,reason:"Their command believes the present deployment could support a surprise attack.",consequence:"Refusal may trigger patrol increases and defensive mobilization.",context:`${stxRDBorderFleets(to,from).length} visible fleet formation(s) are operating near their worlds.`,actionKind:"withdrawal",severity:2};
  if(kind==="tribute")return{title:"Demand for Political Concessions",message:`${empire(from).name} demands ${amount} credits to recognize the present frontier without further action.`,reason:"A proud rival is converting unresolved complaints into financial pressure.",consequence:"Refusal will deepen the grievance but does not automatically begin war.",context:"The demand is deliberately unfavorable and may be refused.",actionKind:"tribute",severity:3};
  if(kind==="trade")return{title:"Preferred Trade Access",message:`${empire(from).name} requests a temporary preferred-access agreement for civilian commerce.`,reason:"Its commercial ministries want reliable access to Mandate markets.",consequence:"Acceptance builds goodwill; refusal has only a minor consequence.",context:"Both sides will gain open commercial access for several cycles.",actionKind:"tradeAccess",severity:1};
  return{title:`Emergency ${RESOURCE_LABEL[resource]||resource} Request`,message:`${empire(from).name} requests ${amount} ${RESOURCE_LABEL[resource]||resource} for a developing frontier world.`,reason:"A real resource shortage is delaying its expansion and construction.",consequence:"Helping creates goodwill and improves near-term trade relations.",context:"Accepted material will leave Mandate stockpiles aboard visible freight vessels.",actionKind:"resource",severity:1};
}
function stxRDCreateRequest(from,to,preferred=null,forcedTarget=null){
  const d=state.rivalDiplomacy,e=empire(from),p=e.foreignPolicy,r=stxRDPair(from,to);if(!e||!owned(from).length||!owned(to).length||getWar(from,to))return null;
  if(d.requests.some(x=>x.status==="pending"&&x.from===from&&x.to===to))return null;
  let kind=preferred,target=forcedTarget||null,resource=null,amount=0,contest=stxRDContestFor(from,to);
  if(!kind){
    target=contest&&state.planets.find(x=>x.id===contest.planetId)||stxRDSharedFrontierWorld(from,to);
    const military=stxRDBorderFleets(to,from).length>0,roll=random();
    if(target&&(contest||p.expansionism>.58)&&roll<.48)kind="territorial";
    else if(military&&r.tension>28&&roll<.64)kind="military";
    else if(p.pride>.68&&r.tension>48&&roll<.78){kind="tribute";amount=Math.ceil(45+p.pride*55+r.tension*.35)}
    else if(p.commercialism>.62&&roll<.5)kind="trade";
    else kind="resource";
  }
  if(kind==="territorial"&&!target)target=stxRDSharedFrontierWorld(from,to);if(kind==="territorial"&&!target)return null;
  if(kind==="resource"){resource=stxRDRequestResource(from,to);amount=Math.ceil(clamp(empireResource(to,resource)*(.1+p.pride*.04),28,72))}
  if(kind==="tribute"&&!amount)amount=Math.ceil(55+p.pride*65+r.tension*.4);
  const copy=stxRDRequestContext(from,to,kind,target,resource,amount),deadline=48+p.patience*34,request={id:stxRDId("rq"),from,to,kind,...copy,targetId:target?.id||null,resource,amount,status:"pending",stage:1,createdAt:state.simTime,expiresAt:state.simTime+deadline,reminderAt:state.simTime+deadline*.58,deferred:false,autoRespondAt:to===0?null:state.simTime+rand(16,34)};
  d.requests.unshift(request);d.requests=d.requests.slice(0,40);r.lastDemandAt=state.simTime;r.nextRequestAt=state.simTime+rand(78,132)+(1-p.patience)*30;
  if(kind==="territorial")stxRDRegisterContest(target,from,to,"a formal territorial demand");if(to===0){logEvent(`${e.name} sent a ${kind} request.`,"warning");renderTransmissions();updateBadges()}
  return request;
}
function stxRDCreateUltimatum(from,to){
  const r=stxRDPair(from,to),contest=stxRDContestFor(from,to),target=contest&&state.planets.find(p=>p.id===contest.planetId),amount=Math.ceil(80+empire(from).foreignPolicy.pride*80+r.tension*.45),actionKind=target?"nonColonization":stxRDBorderFleets(to,from).length?"withdrawal":"tribute";
  const q={id:stxRDId("rq"),from,to,kind:"ultimatum",actionKind,targetId:target?.id||null,amount,status:"pending",stage:5,severity:5,createdAt:state.simTime,expiresAt:state.simTime+42+empire(from).foreignPolicy.patience*18,reminderAt:Infinity,deferred:false,title:target?`ULTIMATUM — ABANDON ${target.name.toUpperCase()}`:actionKind==="withdrawal"?"ULTIMATUM — WITHDRAW FRONTIER FLEETS":"ULTIMATUM — PAY REPARATIONS",message:target?`${empire(from).name} demands that ${empire(to).name} abandon its claim to ${target.name}.`:actionKind==="withdrawal"?`${empire(from).name} demands the immediate withdrawal of frontier fleets.`:`${empire(from).name} demands ${amount} credits in reparations.`,reason:"Repeated unresolved disputes have reached the final diplomatic warning stage.",consequence:"Refusal makes formal conflict possible if the rival judges war supportable.",context:"War is not automatic; fleet strength, distance, other wars, and strategic value still matter.",autoRespondAt:to===0?null:state.simTime+rand(14,28)};
  state.rivalDiplomacy.requests.unshift(q);r.lastDemandAt=state.simTime;if(to===0){galacticNews(`${empire(from).name.toUpperCase()} ISSUES FINAL ULTIMATUM`,q.message,"danger",target?.id||null);renderTransmissions();updateBadges()}return q;
}

function stxRDAvailableResource(owner,resource){return owned(owner).reduce((n,p)=>n+Math.max(0,(p.stock[resource]||0)-8),0)}
function stxRDCanAcceptRequest(q){if(q.actionKind==="resource")return stxRDAvailableResource(q.to,q.resource)>=q.amount;if(q.actionKind==="tribute")return(empire(q.to)?.credits||0)>=q.amount;return true}
function stxRDSendResource(q){
  if(stxRDAvailableResource(q.to,q.resource)<q.amount||state.ships.length+Math.ceil(q.amount/36)>=280)return false;const target=owned(q.from).sort((a,b)=>(a.stock[q.resource]||0)-(b.stock[q.resource]||0))[0];if(!target)return false;let remaining=q.amount;
  for(const source of owned(q.to).filter(p=>(p.stock[q.resource]||0)>8).sort((a,b)=>(b.stock[q.resource]||0)-(a.stock[q.resource]||0))){if(remaining<=.1)break;const take=Math.min(remaining,Math.max(0,(source.stock[q.resource]||0)-8),36);if(take<=0)continue;source.stock[q.resource]-=take;const type=q.resource==="helium"?"tanker":q.resource==="components"?"construction":"freighter",ship=createShip(type,source,target,q.to,{cargo:{[q.resource]:take},commercial:true,crossBorder:true,tradeKind:"diplomatic-aid",tradePartner:q.from,requestId:q.id,vesselName:vesselName(type)});if(!ship){source.stock[q.resource]+=take;return false}remaining-=take}
  return remaining<=.1;
}
function stxRDRepositionFleet(f,to,reason="frontier withdrawal"){
  const from=state.planets.find(p=>p.id===f.location);if(!from||!to||from.id===to.id||state.ships.some(s=>s.fleetId===f.id))return false;f.location=null;f.status=`Repositioning to ${to.name}`;const s=createShip("fleet",from,to,f.owner,{strength:f.strength,fleetId:f.id,vesselName:f.name,rdReposition:true,rdReason:reason,speedBoost:1.05});if(!s){f.location=from.id;return false}return true;
}
function stxRDWithdrawBorderFleets(owner,foe){
  const border=stxRDBorderFleets(owner,foe),foeWorlds=owned(foe);let moved=0;border.slice(0,3).forEach(({f})=>{const safe=owned(owner).filter(p=>!p.underAttack).sort((a,b)=>Math.min(...foeWorlds.map(q=>dist(b,q)))-Math.min(...foeWorlds.map(q=>dist(a,q))))[0];if(stxRDRepositionFleet(f,safe,"diplomatic withdrawal"))moved++});return moved;
}
function stxRDApplyAcceptedRequest(q){
  if(q.actionKind==="resource")return stxRDSendResource(q);
  if(q.actionKind==="tribute"){if((empire(q.to)?.credits||0)<q.amount)return false;empire(q.to).credits-=q.amount;empire(q.from).credits+=q.amount;return true}
  if(q.actionKind==="tradeAccess"){empire(q.to).commercialAccess[q.from]=empire(q.from).commercialAccess[q.to]=state.simTime+145;setModifier(empire(q.to),"foreignTrade",1.22,120);return true}
  if(q.actionKind==="withdrawal"){stxRDWithdrawBorderFleets(q.to,q.from);stxRDAddAgreement("border-withdrawal",q.to,q.from,95);return true}
  if(q.actionKind==="nonColonization"){stxRDCancelExpansion(q.to,q.targetId);stxRDAddAgreement("non-colonization",q.to,q.from,135,{planetId:q.targetId});return true}
  return true;
}
function stxRDRecalculatePair(a,b){
  const r=stxRDPair(a,b),war=getWar(a,b);if(war){r.escalationStage=6;r.borderPosture="Mobilizing";return r}
  const active=state.rivalDiplomacy.requests.filter(q=>q.status==="pending"&&((q.from===a&&q.to===b)||(q.from===b&&q.to===a))),weight=stxRDRecent(r.grievances,14).reduce((n,g)=>n+(g.weight||0),0);let stage=active.length?Math.max(...active.map(q=>q.stage||1)):0;
  if(weight>=6||r.refusedRequests+r.ignoredRequests>=1)stage=Math.max(stage,3);if(weight>=17||r.tension>=55)stage=Math.max(stage,4);if(weight>=31||r.tension>=72||active.some(q=>q.kind==="ultimatum"))stage=Math.max(stage,5);r.escalationStage=stage;
  r.borderPosture=stage>=5||r.tension>=74?"Mobilizing":stage>=4||r.tension>=56?"Reinforcing":stage>=3||r.tension>=38?"Patrols Increased":"Quiet";return r;
}
function stxRDRespondRequest(id,action,automatic=false){
  const q=state.rivalDiplomacy.requests.find(x=>x.id===id&&x.status==="pending");if(!q)return false;const r=stxRDPair(q.from,q.to);
  if(action==="defer"){
    if(q.deferred){if(!automatic)showToast("This request has already been deferred once");return false}q.deferred=true;q.stage=Math.max(2,q.stage);q.expiresAt+=22+empire(q.from).foreignPolicy.patience*18;r.tension=clamp(r.tension+2.5,0,100);if(!automatic)showToast(`Response deferred · ${fmtEta(q.expiresAt-state.simTime)} remaining`);
  }else if(action==="accept"){
    if(!stxRDCanAcceptRequest(q)||!stxRDApplyAcceptedRequest(q)){if(!automatic)showToast("The requested commitment cannot be fulfilled from current reserves");return false}q.status="accepted";q.respondedAt=state.simTime;stxRDAddCooperation(q.from,q.to,q.kind,q.actionKind==="resource"?`${empire(q.to).name} delivered ${q.amount} ${RESOURCE_LABEL[q.resource]||q.resource}`:`${empire(q.to).name} accepted ${q.title}`,q.kind==="ultimatum"?12:8);if(!automatic)showToast(`${q.title} accepted`);
  }else{
    q.status="refused";q.respondedAt=state.simTime;r.refusedRequests=(r.refusedRequests||0)+1;const severity=q.kind==="ultimatum"?5:(q.severity||1),text=q.kind==="ultimatum"?`${empire(q.to).name} rejected a final ultimatum`:`${q.title} refused`;stxRDAddGrievance(q.from,q.to,q.kind==="territorial"?"territorial refusal":q.kind,text,severity,q.targetId);if(q.kind==="ultimatum")r.ultimatumRejectedAt=state.simTime;if(!automatic)showToast(`${q.title} refused`);
  }
  stxRDRecalculatePair(q.from,q.to);if(q.to===0||q.from===0){renderTransmissions();renderRivals();updateBadges();updateHud(true)}return true;
}
function stxRDAutoRespond(q){
  const responder=empire(q.to),p=responder.foreignPolicy,r=stxRDPair(q.from,q.to),power=empireFleet(q.to)/Math.max(1,empireFleet(q.from)),acceptScore=p.caution*.35+p.commercialism*.22+Math.max(0,relation(q.from,q.to))*.32+(power<.75 ? .18 : 0)-(p.pride*.28+q.severity*.06);
  const action=stxRDCanAcceptRequest(q)&&acceptScore>random()*.72?"accept":"refuse";stxRDRespondRequest(q.id,action,true);
}
function stxRDProcessRequests(){
  const requests=state.rivalDiplomacy.requests;
  requests.filter(q=>q.status==="pending").forEach(q=>{
    if(q.to!==0&&state.simTime>=(q.autoRespondAt||Infinity)){stxRDAutoRespond(q);return}
    if(q.stage===1&&state.simTime>=q.reminderAt){q.stage=2;q.title=q.kind==="ultimatum"?q.title:`REMINDER — ${q.title}`;if(q.to===0){logEvent(`${empire(q.from).name} repeated an unanswered request.`,"warning");renderTransmissions()}}
    if(state.simTime<q.expiresAt)return;
    q.status="ignored";q.respondedAt=state.simTime;const r=stxRDPair(q.from,q.to);r.ignoredRequests=(r.ignoredRequests||0)+1;const severity=q.kind==="ultimatum"?5:(q.severity||1)+1;stxRDAddGrievance(q.from,q.to,q.kind==="territorial"?"ignored territorial warning":"ignored request",`${q.title} expired without a response`,severity,q.targetId);if(q.kind==="ultimatum")r.ultimatumRejectedAt=state.simTime;if(q.to===0){galacticNews(`${empire(q.from).name.toUpperCase()} RECORDS DIPLOMATIC GRIEVANCE`,`${q.title} expired without an answer. The response is political pressure, not an automatic declaration of war.`,"warning",q.targetId);renderTransmissions();updateBadges()}
  });
}

function stxRDBorderWorld(owner,foe){const fw=owned(foe);return owned(owner).filter(p=>!p.underAttack).sort((a,b)=>Math.min(...fw.map(q=>dist(a,q)))-Math.min(...fw.map(q=>dist(b,q))))[0]||null}
function stxRDBestYard(owner){return typeof stxBestFleetYard==="function"?stxBestFleetYard(owner):owned(owner).filter(p=>p.infra.shipyard>0&&!p.underAttack).sort((a,b)=>(b.infra.shipyard||0)-(a.infra.shipyard||0))[0]||null}
function stxRDMobilize(owner,foe,urgent=false){
  const e=empire(owner),border=stxRDBorderWorld(owner,foe);if(!e||!border)return false;const now=state.simTime;if(!urgent&&now-(e.rdLastMobilizationAt||-999)<38)return false;e.rdLastMobilizationAt=now;let acted=false;
  const candidates=(typeof stxOLFleetCandidates==="function"?stxOLFleetCandidates(owner):state.fleets.filter(f=>f.owner===owner&&!f.destroyed&&f.location&&!state.ships.some(s=>s.fleetId===f.id))).filter(f=>now-(f.rdDiplomaticMobilizationAt||-999)>45);
  const facility=(border.orbitalFacilities||[]).find(f=>f.hp>0&&(f.kind==="military"||f.kind==="station"));
  candidates.slice(0,urgent?2:1).forEach(f=>{let ok=false;if(facility&&typeof stxOLStageFleet==="function")ok=!!stxOLStageFleet(facility.id,f.id);else ok=stxRDRepositionFleet(f,border,"diplomatic mobilization");if(ok){f.rdDiplomaticMobilizationAt=now;acted=true}});
  if(!candidates.length){const yard=stxRDBestYard(owner);if(yard&&typeof stxQueueFleetCommission==="function"&&!yard.buildQueue?.some(q=>q.commissionSource==="Diplomatic frontier mobilization")){acted=stxQueueFleetCommission(yard,"fleet","Diplomatic frontier mobilization")||acted}}
  if(!border.localProject&&border.infra.defense<4)acted=startLocalProject(border,"defense","Foreign ministry frontier reinforcement")||acted;
  if(urgent&&!border.orbitalProject&&typeof queueOrbitalProject==="function")acted=queueOrbitalProject(border,"base",true)||acted;
  return acted;
}
function stxRDApplyPressure(holder,against,r){
  if(state.simTime-(r.lastPressureAt||-999)<42)return;r.lastPressureAt=state.simTime;empire(holder).commercialAccess[against]=0;empire(against).commercialAccess[holder]=0;stxRDMobilize(holder,against,false);const contest=stxRDContestFor(holder,against),p=contest&&state.planets.find(x=>x.id===contest.planetId);
  if(p&&!owned(holder).some(x=>x.expansionProject)&&!state.ships.some(s=>s.owner===holder&&s.type==="colony"))attemptExpansion(empire(holder));
  if(holder===0||against===0)galacticNews(`${empire(holder).name.toUpperCase()} INCREASES FRONTIER PRESSURE`,`${empire(holder).name} has worsened trade access, reinforced frontier installations, and begun visible fleet preparations${p?` around ${p.name}`:""}.`,"warning",p?.id||null);
}

function stxRDRelativePower(a,b){const av=empireFleet(a)+empireIndustry(a)*4+owned(a).length*5,bv=empireFleet(b)+empireIndustry(b)*4+owned(b).length*5;return av/Math.max(1,bv)}
function stxRDMilitaryLabel(a,b){const ratio=stxRDRelativePower(a,b);return ratio<.55?"Much Weaker":ratio<.82?"Weaker":ratio<1.22?"Comparable":ratio<1.75?"Stronger":"Much Stronger"}
function stxRDRelationshipLabel(a,b){if(getWar(a,b))return"At War";const v=relation(a,b),t=stxRDPair(a,b)?.tension||0;return v>.55&&t<25?"Friendly":v>.22&&t<38?"Cooperative":v>-.08&&t<48?"Neutral":v>-.3&&t<62?"Guarded":v>-.58&&t<78?"Tense":"Hostile"}
function stxRDThreatSupport(a,b){
  const p=empire(a).foreignPolicy,ratio=stxRDRelativePower(a,b),wars=state.wars.filter(w=>w.active&&(w.a===a||w.b===a)).length,distance=frontierDistance(a,b),targetWars=state.wars.filter(w=>w.active&&(w.a===b||w.b===b)).length,vulnerable=owned(b).reduce((n,x)=>n+(x.warDamage||0),0)+targetWars*.45;
  return clamp(.34+Math.log2(Math.max(.25,ratio))*.19+(distance<900 ? .12 : distance>1800 ? -.16 : 0)+p.aggression*.18+p.opportunism*Math.min(.16,vulnerable*.1)-p.caution*.18-wars*.2,0,1);
}
function stxRDConflictAssessment(a,b){
  const r=stxRDPair(a,b),war=getWar(a,b),contest=stxRDContestFor(a,b),recent=stxRDRecent(r.grievances,6),ratio=stxRDRelativePower(b,a);let score=war?100:r.tension+recent.reduce((n,g)=>n+g.severity*2,0)+(contest?9:0)+(r.escalationStage>=4?9:0)-r.goodwill*.3;
  const reasons=[];if(war)reasons.push(`War cause: ${war.warCause?.type||war.reason||"Active conflict"}`);const active=state.rivalDiplomacy.requests.find(q=>q.status==="pending"&&q.from===b&&q.to===a);if(active)reasons.push(active.kind==="ultimatum"?"Final ultimatum unanswered":`${active.stage>=2?"Repeated":"Active"} ${active.kind} request`);if(contest){const p=state.planets.find(x=>x.id===contest.planetId);reasons.push(`${p?.name||"Frontier world"} remains contested`)}if(recent[0])reasons.push(recent[0].text);if(r.borderPosture!=="Quiet")reasons.push(`${empire(b).name} posture: ${r.borderPosture}`);if(ratio>1.22)reasons.push(`${empire(b).name} has the stronger strategic position`);if(!reasons.length)reasons.push(r.goodwill>20?"Recent cooperation is containing tensions":"No major unresolved dispute");
  score=clamp(score,0,100);return{score,label:score<34?"Low":score<58?"Elevated":score<78?"High":"Critical",reasons:reasons.slice(0,4)};
}
function stxRDWarCause(a,b){const r=stxRDPair(a,b),contest=stxRDContestFor(a,b),g=stxRDRecent(r.grievances,8);if(g.some(x=>/station|raid/i.test(x.text)))return{type:"Retaliation for Station Attack",detail:g.find(x=>/station|raid/i.test(x.text))?.text,targetId:g.find(x=>/station|raid/i.test(x.text))?.targetId};if(g.some(x=>/promise|treaty|agreement/i.test(x.text)))return{type:"Repeated Treaty Violations",detail:"Diplomatic commitments were repeatedly violated."};if(contest)return{type:"Territorial Dispute",detail:`Competing claims over ${state.planets.find(p=>p.id===contest.planetId)?.name||"a frontier world"}.`,targetId:contest.planetId};if(stxRDBorderFleets(a,b).length||stxRDBorderFleets(b,a).length)return{type:"Border Crisis",detail:"Fleet concentrations turned a shared-frontier crisis into open war."};return{type:"Expansion Conflict",detail:"Competing regional ambitions exhausted the available diplomatic options."}}
function stxRDObjective(a,b,cause){const desired=cause?.targetId&&state.planets.find(p=>p.id===cause.targetId&&p.owner===b);if(desired)return desired;const sources=owned(a);return owned(b).sort((x,y)=>{const xv=(x.infra.shipyard||0)*30+(x.orbitals?.base||0)*28+(x.tradeStation?18:0)-Math.min(...sources.map(s=>dist(s,x)))*.02,yv=(y.infra.shipyard||0)*30+(y.orbitals?.base||0)*28+(y.tradeStation?18:0)-Math.min(...sources.map(s=>dist(s,y)))*.02;return yv-xv})[0]||null}
function stxRDLaunchWarObjective(w,aggressor,defender){
  const target=state.planets.find(p=>p.id===w.strategicObjectives?.[0]?.planetId)||stxRDObjective(aggressor,defender,w.warCause),sources=owned(aggressor).filter(p=>!p.underAttack);if(!target||!sources.length)return false;
  const fleet=state.fleets.filter(f=>f.owner===aggressor&&!f.destroyed&&f.location&&!state.ships.some(s=>s.fleetId===f.id)).sort((a,b)=>b.strength-a.strength)[0];if(fleet){const from=state.planets.find(p=>p.id===fleet.location);if(from&&from.id!==target.id){fleet.location=null;fleet.status=`War objective: ${target.name}`;const ship=createShip("fleet",from,target,aggressor,{strength:fleet.strength,fleetId:fleet.id,vesselName:fleet.name,warId:w.id,rdWarObjective:true});if(ship){w.lastCampaign=state.simTime;return true}fleet.location=from.id}}
  const source=sources.filter(p=>p.garrison>18).sort((a,b)=>dist(a,target)-dist(b,target)||b.garrison-a.garrison)[0];if(source){const strength=Math.max(9,source.garrison*(.34+empire(aggressor).foreignPolicy.aggression*.18));source.garrison-=strength;deployFleet(source,target,aggressor,strength,{warId:w.id,status:`War objective: ${target.name}`});w.lastCampaign=state.simTime;return true}
  const yard=stxRDBestYard(aggressor);if(yard&&typeof stxQueueFleetCommission==="function"){stxQueueFleetCommission(yard,"fleet",`War objective: ${target.name}`);w.pendingObjectiveLaunch=true;return true}return false;
}

const STX_RD_declareWar=declareWar;
declareWar=function(a,b,reason="border dispute"){
  const r=stxRDPair(a,b),physical=/contested frontier|station attack|station raid|imperial invasion order|direct imperial declaration|broken peace guarantee/i.test(reason),playerChoice=a===0;
  if(!playerChoice&&!stxRDDeclarationContext&&!physical&&((r?.escalationStage||0)<5||state.simTime-(r?.ultimatumRejectedAt||-999)>95||state.simTime-(r?.ultimatumRejectedAt||-999)<10))return null;
  const directCause=playerChoice&&/direct imperial|invasion order/i.test(reason)?{type:"Direct Imperial Declaration",detail:reason}:null,ctx=stxRDDeclarationContext,cause=ctx?.cause||directCause||stxRDWarCause(a,b),w=STX_RD_declareWar(a,b,cause?.detail||reason);if(!w)return w;
  w.warCause={type:cause?.type||"Border Crisis",detail:cause?.detail||reason,recordedAt:state.simTime};w.reason=w.warCause.type;w.aggressor=a;const objective=stxRDObjective(a,b,cause);w.strategicObjectives=objective?[{planetId:objective.id,label:objective.name,type:w.warCause.type,status:"active"}]:[];r.escalationStage=6;r.lastWarAt=state.simTime;r.borderPosture="Mobilizing";
  stxRDAddGrievance(b,a,"war declaration",`${empire(a).name} declared war over ${w.warCause.type}`,5,objective?.id||null);if(a!==0){const launched=stxRDLaunchWarObjective(w,a,b);w.pendingObjectiveLaunch=!launched;stxRDMobilize(a,b,true)}return w;
};
const STX_RD_endWar=endWar;
endWar=function(w,reason="treaty",tribute=0,payer=null){const a=w?.a,b=w?.b;STX_RD_endWar(w,reason,tribute,payer);if(a===undefined||b===undefined)return;const r=stxRDPair(a,b);r.escalationStage=0;r.tension=Math.max(18,r.tension-24);r.lastPeaceAt=state.simTime;r.warCooldownUntil=state.simTime+STX_RD_WAR_COOLDOWN;r.borderPosture="Quiet";r.ultimatumRejectedAt=-999;stxRDAddCooperation(a,b,"peace",`Peace concluded by ${reason}`,10)};

function stxRDMaybeEscalatePair(a,b){
  const r=stxRDRecalculatePair(a,b),previous=r.lastAppliedStage||0,recent=stxRDRecent(r.grievances,10),rivalHolder=a===0?b:(recent[0]?.holder??(empire(a).foreignPolicy.aggression>empire(b).foreignPolicy.aggression?a:b));if(r.escalationStage>=4&&previous<4){const against=rivalHolder===a?b:a;stxRDApplyPressure(rivalHolder,against,r)}
  if(r.escalationStage>=5&&!getWar(a,b)&&!state.rivalDiplomacy.requests.some(q=>q.status==="pending"&&q.kind==="ultimatum"&&((q.from===a&&q.to===b)||(q.from===b&&q.to===a)))&&state.simTime-(r.lastDemandAt||-999)>18){const holder=a===0?b:(recent[0]?.holder??(empire(a).foreignPolicy.pride>empire(b).foreignPolicy.pride?a:b)),against=holder===a?b:a;if(against!==0||!state.rivalDiplomacy.requests.some(q=>q.status==="pending"&&q.kind==="ultimatum"&&q.to===0))stxRDCreateUltimatum(holder,against)}
  r.lastAppliedStage=r.escalationStage;
}
function stxRDMaybeDeclareWar(a,b){
  const r=stxRDPair(a,b);if(getWar(a,b)||r.escalationStage<5||state.simTime<(r.warCooldownUntil||0)||state.simTime-(r.ultimatumRejectedAt||-999)>95||state.simTime-r.ultimatumRejectedAt<10)return false;
  const grievance=stxRDRecent(r.grievances,10)[0],aggressor=grievance?.holder??(empire(a).foreignPolicy.aggression>empire(b).foreignPolicy.aggression?a:b),defender=aggressor===a?b:a;if(aggressor===0)return false;if(defender===0&&state.simTime<(state.rivalDiplomacy.graceUntil||0))return false;
  const attackerWars=state.wars.filter(w=>w.active&&(w.a===aggressor||w.b===aggressor)).length,targetWars=state.wars.filter(w=>w.active&&(w.a===defender||w.b===defender)).length,p=empire(aggressor).foreignPolicy,support=stxRDThreatSupport(aggressor,defender);if(attackerWars>0&&!(p.opportunism>.78&&support>.76))return false;if(defender===0&&targetWars>0&&!(p.opportunism>.75&&r.tension>84&&support>.72))return false;
  if(support<.5){if(state.simTime-r.ultimatumRejectedAt>38&&random()<.25){r.tension=Math.max(45,r.tension-12);r.escalationStage=4;r.ultimatumRejectedAt=-999;stxRDMobilize(aggressor,defender,false);if(defender===0)galacticNews(`${empire(aggressor).name.toUpperCase()} HOLDS AT THE FRONTIER`,`${empire(aggressor).name} has not acted on its ultimatum. Its fleets remain reinforced, but military planners appear unwilling to risk a larger war.`,"warning")}return false}
  if(random()>.22+p.aggression*.2+(support-.5)*.45)return false;const cause=stxRDWarCause(aggressor,defender);stxRDDeclarationContext={cause};try{return!!declareWar(aggressor,defender,cause.detail)}finally{stxRDDeclarationContext=null}
}

function stxRDObserveLegacyProposals(){state.proposals.filter(p=>p.from>0&&!p.rdMemoryRecorded&&["accepted","declined","expired"].includes(p.status)).forEach(p=>{p.rdMemoryRecorded=true;if(p.status==="accepted")stxRDAddCooperation(0,p.from,p.kind,`${p.title} accepted`,4);else stxRDAddGrievance(p.from,0,p.status==="expired"?"ignored request":"refused request",`${p.title} ${p.status}`,1)})}
const STX_RD_respondProposal=respondProposal;
respondProposal=function(id,action){const result=STX_RD_respondProposal(id,action);stxRDObserveLegacyProposals();return result};

function stxRDDetectContests(){
  const projects=state.planets.filter(p=>p.expansionProject).map(p=>({owner:p.owner,target:state.planets.find(x=>x.id===p.expansionProject.targetId)})).filter(x=>x.target);for(let i=0;i<projects.length;i++)for(let j=i+1;j<projects.length;j++)if(projects[i].owner!==projects[j].owner&&projects[i].target.id===projects[j].target.id)stxRDRegisterContest(projects[i].target,projects[i].owner,projects[j].owner,"competing physical colony missions");
}
function stxRDDevelopmentTick(){
  state.empires.slice(1).forEach(e=>{const p=e.foreignPolicy,rules=stxRDExpansionRules(e);if(state.simTime<(e.rdNextDevelopmentAt||0))return;e.rdNextDevelopmentAt=state.simTime+(rand(55,95)+(1-p.expansionism)*40)*rules.development;const worlds=owned(e.id);if(!worlds.length)return;const wars=state.wars.filter(w=>w.active&&(w.a===e.id||w.b===e.id)),foe=wars[0]&&(wars[0].a===e.id?wars[0].b:wars[0].a),border=foe!==undefined?stxRDBorderWorld(e.id,foe):worlds.slice().sort((a,b)=>borderThreat(b)-borderThreat(a))[0];
    if((wars.length||borderThreat(border)>.38)&&border&&!border.localProject&&border.infra.defense<4){startLocalProject(border,"defense","Rival frontier program");return}
    if(p.expansionism>=.6){const candidate=worlds.filter(x=>!x.localProject&&!x.underAttack).sort((a,b)=>(a.infra.mine+a.infra.factory)-(b.infra.mine+b.infra.factory))[0];if(candidate){const type=candidate.infra.factory<candidate.infra.mine?"factory":"mine";if(startLocalProject(candidate,type,"Rival expansion program"))return}}
    if(worlds.length>=4&&!stxRDBestYard(e.id)){const candidate=worlds.filter(x=>!x.localProject&&!x.underAttack).sort((a,b)=>(b.infra.factory+b.pop*6)-(a.infra.factory+a.pop*6))[0];if(candidate&&startLocalProject(candidate,"shipyard","Rival naval development"))return}
    if(border&&!border.orbitalProject&&p.aggression>.55)queueOrbitalProject(border,"base",false);
  });
}
function stxRDGenerateRequests(){
  const d=state.rivalDiplomacy;if(state.simTime>=d.graceUntil&&state.simTime>=d.nextPlayerRequestAt){const pending=d.requests.filter(q=>q.status==="pending"&&q.to===0).length;if(pending<STX_RD_REQUEST_LIMIT){const candidates=state.empires.slice(1).filter(e=>owned(e.id).length&&!getWar(0,e.id)&&state.simTime>=(stxRDPair(0,e.id).nextRequestAt||0)).sort((a,b)=>{const ar=stxRDPair(0,a.id),br=stxRDPair(0,b.id);return(br.tension+b.foreignPolicy.pride*18+b.foreignPolicy.expansionism*14)-(ar.tension+a.foreignPolicy.pride*18+a.foreignPolicy.expansionism*14)});if(candidates[0])stxRDCreateRequest(candidates[0].id,0)}d.nextPlayerRequestAt=state.simTime+rand(62,96)}
  if(state.simTime>=d.nextAIRequestAt){const pairs=[];for(let a=1;a<state.empires.length;a++)for(let b=a+1;b<state.empires.length;b++)if(owned(a).length&&owned(b).length&&!getWar(a,b)&&state.simTime>=(stxRDPair(a,b).nextRequestAt||0))pairs.push([a,b]);if(pairs.length){const [a,b]=pick(pairs),from=empire(a).foreignPolicy.pride+empire(a).foreignPolicy.expansionism>empire(b).foreignPolicy.pride+empire(b).foreignPolicy.expansionism?a:b;stxRDCreateRequest(from,from===a?b:a)}d.nextAIRequestAt=state.simTime+rand(105,160)}
}
function stxRDDecay(dt){
  Object.values(state.rivalDiplomacy.relations).forEach(r=>{r.grievances=r.grievances.filter(g=>state.simTime-g.time<430);r.cooperation=r.cooperation.filter(c=>state.simTime-c.time<430);if(!getWar(r.a,r.b)){r.tension=Math.max(0,r.tension-dt*.018);r.goodwill=Math.max(0,r.goodwill-dt*.008)}});state.rivalDiplomacy.agreements.forEach(a=>{if(a.active!==false&&a.expiresAt<=state.simTime)a.active=false});state.rivalDiplomacy.contestedWorlds.forEach(c=>{const p=state.planets.find(x=>x.id===c.planetId);if(!p)c.status="resolved";else if(p.owner!==null){c.status="resolved";c.resolvedAt=state.simTime;c.winner=p.owner}})
}
function stxRDWarObjectiveTick(){
  state.wars.filter(w=>w.active&&w.aggressor!==0).forEach(w=>{const aggressor=Number.isFinite(w.aggressor)?w.aggressor:w.a,defender=aggressor===w.a?w.b:w.a,inbound=state.ships.some(s=>s.warId===w.id&&s.owner===aggressor);if(inbound)return;if(w.pendingObjectiveLaunch||state.simTime-(w.lastCampaign||0)>38){const launched=stxRDLaunchWarObjective(w,aggressor,defender);w.pendingObjectiveLaunch=!launched}});
}
function stxRDTick(){
  const d=stxRDEnsureState(false);if(!d)return;const dt=Math.max(0,state.simTime-(d.lastTickAt||state.simTime));d.lastTickAt=state.simTime;stxRDDecay(dt);stxRDObserveLegacyProposals();stxRDProcessRequests();stxRDDetectContests();stxRDGenerateRequests();stxRDDevelopmentTick();stxRDWarObjectiveTick();
  for(let a=0;a<state.empires.length;a++)for(let b=a+1;b<state.empires.length;b++){if(!owned(a).length||!owned(b).length)continue;stxRDMaybeEscalatePair(a,b);stxRDMaybeDeclareWar(a,b)}stxRDPersist();if(!$("rivalsModal")?.hidden)renderRivals();
}
const STX_RD_diplomacyTick=diplomacyTick;
diplomacyTick=function(){STX_RD_diplomacyTick();stxRDTick()};

const STX_RD_arriveShip=arriveShip;
arriveShip=function(s,p){
  if(s?.rdReposition){const f=fleetRecord(s.fleetId);if(f){f.location=p.id;f.strength=s.strength;f.status=`Repositioned at ${p.name}`;f.readiness="patrol"}return}
  const previousOwner=p?.owner,result=STX_RD_arriveShip(s,p);if(s?.type==="colony"&&previousOwner===null&&p?.owner!==null){state.rivalDiplomacy?.contestedWorlds.filter(c=>c.status==="active"&&c.planetId===p.id).forEach(c=>{c.status="resolved";c.winner=p.owner;c.resolvedAt=state.simTime;const loser=c.a===p.owner?c.b:c.a;if(loser!==p.owner){stxRDAddGrievance(loser,p.owner,"lost colonial race",`${empire(p.owner).name} won the settlement race for ${p.name}`,2,p.id);if(loser===0||p.owner===0)galacticNews(`${p.name.toUpperCase()} COLONIAL RACE DECIDED`,`${empire(p.owner).name} established the first settlement. ${empire(loser).name}'s competing claim remains a diplomatic grievance.`,"warning",p.id)}})}return result;
};
if(typeof stxRaidTradeStation==="function"){
  const STX_RD_raidTradeStation=stxRaidTradeStation;
  stxRaidTradeStation=function(target,owner=0){const victim=target?.owner,ok=STX_RD_raidTradeStation(target,owner);if(ok&&victim!==null&&victim!==undefined)stxRDAddGrievance(victim,owner,"station raid",`${empire(owner).name} ordered a raid against ${target.name}'s trade station`,4,target.id);return ok};
}

function stxRDRequestCard(q){
  const disabled=!stxRDCanAcceptRequest(q)?"disabled":"",target=q.targetId&&state.planets.find(p=>p.id===q.targetId),type=q.kind==="ultimatum"?"FINAL ULTIMATUM":q.stage>=2?"DIPLOMATIC REMINDER":`${q.kind.toUpperCase()} REQUEST`;return `<article class="transmission-card urgent stx-rd-request ${q.kind==="ultimatum"?"stx-rd-ultimatum":""}"><div class="card-kicker">${type} // ${stxRDEscape(empire(q.from)?.name)}</div><div class="card-title"><strong>${stxRDEscape(q.title)}</strong><span class="news-time">${fmtEta(q.expiresAt-state.simTime)}</span></div><p class="card-copy">${stxRDEscape(q.message)}</p><div class="stx-rd-context"><span><b>Reason</b>${stxRDEscape(q.reason)}</span><span><b>Consequence</b>${stxRDEscape(q.consequence)}</span><span><b>Strategic context</b>${stxRDEscape(q.context)}${target?` · ${stxRDEscape(target.specialization)}`:""}</span></div><div class="choice-row"><button class="choice-btn primary-choice" data-rd-request="${q.id}" data-rd-action="accept" ${disabled}>Accept</button><button class="choice-btn" data-rd-request="${q.id}" data-rd-action="defer" ${q.deferred||q.kind==="ultimatum"?"disabled":""}>Defer</button><button class="choice-btn danger-choice" data-rd-request="${q.id}" data-rd-action="refuse">Refuse</button></div></article>`}
const STX_RD_renderTransmissions=renderTransmissions;
renderTransmissions=function(){STX_RD_renderTransmissions();if(!state.rivalDiplomacy)return;const box=$("transmissionList");if(!box)return;const pending=state.rivalDiplomacy.requests.filter(q=>q.status==="pending"&&q.to===0);if(pending.length){if(box.querySelector(".empty-hub"))box.innerHTML="";box.insertAdjacentHTML("beforeend",pending.map(stxRDRequestCard).join(""))}box.querySelectorAll("[data-rd-request]").forEach(b=>b.onclick=()=>stxRDRespondRequest(b.dataset.rdRequest,b.dataset.rdAction))};
const STX_RD_updateBadges=updateBadges;
updateBadges=function(){STX_RD_updateBadges();const m=$("messageCount");if(!m)return;const count=state.proposals.filter(p=>p.status==="pending"&&(p.to===0||p.empireId===0)).length+(state.militaryRequests||[]).filter(r=>r.status==="pending"&&r.expiresAt>state.simTime).length+(state.rivalDiplomacy?.requests||[]).filter(q=>q.status==="pending"&&q.to===0).length;m.textContent=Math.min(99,count);m.hidden=!count};

function stxRDRivalCard(e){
  const r=stxRDPair(0,e.id),assessment=stxRDConflictAssessment(0,e.id),contest=stxRDContestFor(0,e.id),recentG=stxRDRecent(r.grievances.filter(g=>g.holder===e.id||g.against===e.id),2),recentC=stxRDRecent(r.cooperation,2),active=state.rivalDiplomacy.requests.find(q=>q.status==="pending"&&q.from===e.id&&q.to===0),p=contest&&state.planets.find(x=>x.id===contest.planetId);
  return `<div class="stx-rd-profile"><div class="stx-rd-tags"><b>${stxRDEscape(e.foreignPolicy.archetype)}</b><span>${stxRDPolicyDescriptors(e).map(stxRDEscape).join(" • ")}</span></div><div class="stx-rd-grid"><span><label>Relationship</label><b>${stxRDRelationshipLabel(0,e.id)}</b></span><span><label>Military</label><b>${stxRDMilitaryLabel(0,e.id)}</b></span><span class="risk-${assessment.label.toLowerCase()}"><label>Conflict Risk</label><b>${assessment.label}</b></span><span><label>Border Posture</label><b>${r.borderPosture}</b></span></div>${p?`<div class="stx-rd-line"><b>Current dispute</b><span>${stxRDEscape(p.name)} — Contested World</span></div>`:""}${active?`<div class="stx-rd-line"><b>Active request</b><span>${stxRDEscape(active.title)} · ${fmtEta(active.expiresAt-state.simTime)}</span></div>`:""}<div class="stx-rd-why"><b>Why risk is ${assessment.label.toLowerCase()}</b>${assessment.reasons.map(x=>`<span>${stxRDEscape(x)}</span>`).join("")}</div>${recentG.length?`<div class="stx-rd-line"><b>Recent grievances</b><span>${recentG.map(x=>stxRDEscape(x.text)).join(" · ")}</span></div>`:""}${recentC.length?`<div class="stx-rd-line good"><b>Recent cooperation</b><span>${recentC.map(x=>stxRDEscape(x.text)).join(" · ")}</span></div>`:""}</div>`;
}
function stxRDTributeCost(enemyId){const r=stxRDPair(0,enemyId);return Math.ceil(32+r.tension*.72+empire(enemyId).foreignPolicy.pride*28)}
function stxRDSelectedActions(enemyId){
  const e=empire(enemyId),q=state.rivalDiplomacy.requests.find(x=>x.status==="pending"&&x.from===enemyId&&x.to===0),contest=stxRDContestFor(0,enemyId),p=contest&&state.planets.find(x=>x.id===contest.planetId),war=getWar(0,enemyId),cost=stxRDTributeCost(enemyId),cause=war?.warCause;
  return `<section class="stx-rd-actions"><div class="stx-ol-phase">FOREIGN MINISTRY · ${stxRDEscape(e.name)}</div><h3>${war?`War Cause: ${stxRDEscape(cause?.type||war.reason)}`:"Manage the relationship"}</h3>${war&&cause?.detail?`<p class="subtle">${stxRDEscape(cause.detail)}</p>`:""}<div class="choice-row">${q?`<button class="choice-btn primary-choice" data-rd-panel-request="${q.id}" data-rd-panel-action="accept" ${stxRDCanAcceptRequest(q)?"":"disabled"}>Accept Request</button><button class="choice-btn danger-choice" data-rd-panel-request="${q.id}" data-rd-panel-action="refuse">Refuse Request</button>`:""}${!war?`<button class="choice-btn" data-rd-tribute="${enemyId}" ${empire(0).credits<cost?"disabled":""}>Pay ${cost} cr Tribute</button>`:""}${p&&!war?`<button class="choice-btn" data-rd-assert="${enemyId}">Assert Claim to ${stxRDEscape(p.name)}</button><button class="choice-btn" data-rd-backdown="${enemyId}">Back Down</button>`:""}${p&&!war?`<button class="choice-btn" data-rd-compensate="${enemyId}" ${empire(0).credits<Math.ceil(cost*.65)?"disabled":""}>Offer Compensation</button>`:""}${!war&&e.foreignPolicy.expansionism>.5?`<button class="choice-btn" data-rd-warn="${enemyId}">Warn Against Expansion</button>`:""}</div><div class="subtle stx-rd-action-note">Diplomatic actions change remembered grievances and goodwill. They do not erase the history of the relationship instantly.</div></section>`;
}
function stxRDHandlePanelAction(type,enemyId){
  const r=stxRDPair(0,enemyId),contest=stxRDContestFor(0,enemyId),p=contest&&state.planets.find(x=>x.id===contest.planetId),cost=stxRDTributeCost(enemyId);
  if(type==="tribute"){if(empire(0).credits<cost)return;empire(0).credits-=cost;empire(enemyId).credits+=cost;stxRDAddCooperation(0,enemyId,"tribute",`${cost} credits paid as a political concession`,9);showToast(`${cost} credits paid to ${empire(enemyId).name}`)}
  if(type==="assert"&&p){contest.claims=[...new Set([...contest.claims,0])];stxRDAddGrievance(enemyId,0,"asserted claim",`The Mandate asserted its claim to ${p.name}`,2,p.id);galacticNews(`MANDATE ASSERTS CLAIM TO ${p.name.toUpperCase()}`,`The Foreign Ministry has refused to concede ${p.name} to ${empire(enemyId).name}.`,"warning",p.id)}
  if(type==="backdown"&&p){stxRDCancelExpansion(0,p.id);stxRDAddAgreement("non-colonization",0,enemyId,135,{planetId:p.id});contest.withdrawnBy=0;stxRDAddCooperation(0,enemyId,"territorial concession",`The Mandate withdrew its claim to ${p.name}`,10);showToast(`The Mandate withdrew its claim to ${p.name}`)}
  if(type==="compensate"&&p){const pay=Math.ceil(cost*.65);if(empire(0).credits<pay)return;empire(0).credits-=pay;empire(enemyId).credits+=pay;r.tension=Math.max(0,r.tension-13);stxRDAddCooperation(0,enemyId,"compensation",`${pay} credits offered over the ${p.name} dispute`,7);showToast(`Compensation offered to ${empire(enemyId).name}`)}
  if(type==="warn"&&p&&!state.rivalDiplomacy.requests.some(q=>q.status==="pending"&&q.from===0&&q.to===enemyId)){stxRDCreateRequest(0,enemyId,"territorial",p);showToast(`Warning sent to ${empire(enemyId).name}`)}
  renderRivals();updateHud(true);
}
function stxRDDecorateRivals(){
  const grid=$("rivalGrid");if(!grid||!state.rivalDiplomacy)return;const rivals=state.empires.filter(e=>e.id!==0),cards=[...grid.querySelectorAll(".rival-card")];cards.forEach((card,i)=>{const e=rivals[i];if(!e)return;if(!card.rdDiplomacyBound){const baseClick=card.onclick;card.onclick=ev=>{baseClick?.call(card,ev);if(!ev.target.closest("button"))stxRDDecorateRivals()};card.rdDiplomacyBound=true}if(card.querySelector(".stx-rd-profile"))return;const footer=card.querySelector(".rival-footer");if(footer)footer.insertAdjacentHTML("beforebegin",stxRDRivalCard(e));else card.insertAdjacentHTML("beforeend",stxRDRivalCard(e))});
  let panel=$("stxRDDiplomaticActions");if(!panel){panel=document.createElement("div");panel.id="stxRDDiplomaticActions";const anchor=$("stxOlRivalActions");if(anchor)anchor.before(panel);else grid.after(panel)}const enemyId=Number.isFinite(state.stxSelectedRivalId)?state.stxSelectedRivalId:rivals[0]?.id;if(!empire(enemyId))return;panel.innerHTML=stxRDSelectedActions(enemyId);
  panel.querySelectorAll("[data-rd-panel-request]").forEach(b=>b.onclick=()=>stxRDRespondRequest(b.dataset.rdPanelRequest,b.dataset.rdPanelAction));panel.querySelectorAll("[data-rd-tribute]").forEach(b=>b.onclick=()=>stxRDHandlePanelAction("tribute",Number(b.dataset.rdTribute)));panel.querySelectorAll("[data-rd-assert]").forEach(b=>b.onclick=()=>stxRDHandlePanelAction("assert",Number(b.dataset.rdAssert)));panel.querySelectorAll("[data-rd-backdown]").forEach(b=>b.onclick=()=>stxRDHandlePanelAction("backdown",Number(b.dataset.rdBackdown)));panel.querySelectorAll("[data-rd-compensate]").forEach(b=>b.onclick=()=>stxRDHandlePanelAction("compensate",Number(b.dataset.rdCompensate)));panel.querySelectorAll("[data-rd-warn]").forEach(b=>b.onclick=()=>stxRDHandlePanelAction("warn",Number(b.dataset.rdWarn)));
}
const STX_RD_renderRivals=renderRivals;
renderRivals=function(){STX_RD_renderRivals();stxRDDecorateRivals()};

function stxRDRefreshDifficultySelector(){
  const box=$("stxRDDifficulty");if(!box)return;const id=stxRDDifficultyId(),profile=stxRDDifficultyProfile(id);box.querySelectorAll('input[name="stxEnemyExpansionDifficulty"]').forEach(input=>input.checked=input.value===id);const summary=$("stxRDDifficultySummary");if(summary)summary.textContent=`${profile.label}: ${profile.description}`;
}
function stxRDSetDifficulty(value){
  const id=stxRDDifficultyId(value);state.enemyExpansionDifficulty=id;if(state.rivalDiplomacy&&!state.running)state.rivalDiplomacy.difficulty=id;stxRDRefreshDifficultySelector();return stxRDDifficultyProfile(id);
}
function stxRDInstallDifficultySelector(){
  if($("stxRDDifficulty"))return;const actions=$("newGameBtn")?.parentElement;if(!actions)return;const box=document.createElement("fieldset");box.id="stxRDDifficulty";box.className="stx-rd-difficulty";box.innerHTML=`<legend>Difficulty · Rival Expansion</legend><div class="stx-rd-difficulty-grid">${Object.entries(STX_RD_DIFFICULTIES).map(([id,p])=>`<label><input type="radio" name="stxEnemyExpansionDifficulty" value="${id}"><span><b>${p.label}</b><small>${p.description}</small></span></label>`).join("")}</div><p id="stxRDDifficultySummary" aria-live="polite"></p><small class="stx-rd-difficulty-note">This changes enemy colonization pace, reach, and territorial ceiling. Rival personalities remain varied.</small>`;actions.before(box);box.addEventListener("change",event=>{const input=event.target;if(input?.matches?.('input[name="stxEnemyExpansionDifficulty"]'))stxRDSetDifficulty(input.value)});stxRDRefreshDifficultySelector();
}

function stxRDInstallStyles(){
  if($("stxRDStyles"))return;const style=document.createElement("style");style.id="stxRDStyles";style.textContent=`
  .stx-rd-request{border-color:rgba(255,185,78,.28)!important}.stx-rd-ultimatum{border-color:rgba(255,75,105,.5)!important;box-shadow:0 0 24px rgba(255,75,105,.08)}.stx-rd-context{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin:9px 0}.stx-rd-context span{padding:7px;border-radius:7px;background:rgba(255,255,255,.035);font:600 9px/1.35 system-ui;color:#9fb0c6}.stx-rd-context b{display:block;margin-bottom:3px;color:#e8f4ff;text-transform:uppercase;font-size:8px;letter-spacing:.08em}.stx-rd-profile{margin-top:10px;padding-top:10px;border-top:1px solid rgba(255,255,255,.07)}.stx-rd-tags{display:flex;justify-content:space-between;gap:8px;align-items:flex-start}.stx-rd-tags b{font:800 10px system-ui;color:#dcecff}.stx-rd-tags span{font:700 8px/1.3 system-ui;color:#8fa6bd;text-align:right}.stx-rd-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:5px;margin:8px 0}.stx-rd-grid>span{padding:6px;border-radius:7px;background:rgba(5,12,28,.58)}.stx-rd-grid label{display:block;font:700 7px system-ui;letter-spacing:.08em;text-transform:uppercase;color:#7187a2}.stx-rd-grid b{font:800 9px system-ui;color:#e9f5ff}.stx-rd-grid .risk-high b,.stx-rd-grid .risk-critical b{color:#ff6f7f}.stx-rd-grid .risk-elevated b{color:#ffc466}.stx-rd-line{display:flex;justify-content:space-between;gap:8px;padding:5px 0;font:700 8px/1.3 system-ui}.stx-rd-line>b{color:#91a5bc}.stx-rd-line>span{color:#e4edf7;text-align:right}.stx-rd-line.good>span{color:#73e9a4}.stx-rd-why{display:grid;gap:3px;margin:7px 0;padding:7px;border-radius:7px;background:rgba(255,124,85,.055)}.stx-rd-why>b{font:800 8px system-ui;color:#ffbf73;text-transform:uppercase}.stx-rd-why>span{font:600 8px system-ui;color:#aebed0}.stx-rd-why>span:before{content:"• ";color:#ff8b72}.stx-rd-actions{margin-top:12px;padding:14px;border:1px solid rgba(255,185,78,.22);border-radius:12px;background:linear-gradient(145deg,rgba(255,185,78,.055),rgba(5,10,22,.78))}.stx-rd-actions h3{margin:5px 0 8px}.stx-rd-action-note{margin-top:8px}.living-start{max-height:calc(100vh - 24px);overflow:auto}.stx-rd-difficulty{margin:12px 0 14px;padding:0;border:0}.stx-rd-difficulty legend{padding:0;margin-bottom:7px;color:#a9bee1;font:900 9px system-ui;letter-spacing:.14em;text-transform:uppercase}.stx-rd-difficulty-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:6px}.stx-rd-difficulty label{cursor:pointer;position:relative}.stx-rd-difficulty input{position:absolute;opacity:0;pointer-events:none}.stx-rd-difficulty label span{display:block;height:100%;padding:8px 9px;border:1px solid rgba(121,159,238,.18);border-radius:9px;background:rgba(35,55,103,.18);transition:.15s ease}.stx-rd-difficulty label b{display:block;color:#e5f1ff;font-size:10px}.stx-rd-difficulty label small{display:block;margin-top:2px;color:#8398b9;font:600 8px/1.3 system-ui}.stx-rd-difficulty input:checked+span{border-color:rgba(78,231,255,.62);background:linear-gradient(135deg,rgba(78,231,255,.13),rgba(111,87,255,.14));box-shadow:0 0 0 1px rgba(78,231,255,.09)}.stx-rd-difficulty input:focus-visible+span{outline:2px solid var(--cyan);outline-offset:2px}.stx-rd-difficulty>p{margin:7px 0 2px;color:#bcefff;font:700 9px/1.35 system-ui}.stx-rd-difficulty-note{display:block;color:#7188aa;font:600 8px/1.35 system-ui}@media(max-width:720px){.stx-rd-context{grid-template-columns:1fr}.stx-rd-tags{flex-direction:column}.stx-rd-tags span{text-align:left}.stx-rd-difficulty-grid{grid-template-columns:1fr 1fr}}
  `;document.head.appendChild(style);
}

globalThis.SpaceTyrantsRivalDiplomacy={version:STX_RD_VERSION,difficulties:STX_RD_DIFFICULTIES,difficulty:stxRDDifficultyProfile,setDifficulty:stxRDSetDifficulty,buildPolicy:stxRDBuildPolicy,expansionRules:stxRDExpansionRules,ensureState:stxRDEnsureState};
stxRDEnsureState(false);stxRDInstallStyles();stxRDInstallDifficultySelector();
