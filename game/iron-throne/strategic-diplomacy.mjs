import { militaryArmiesOf } from './core.mjs';
import { personalWillingness } from './emotions.mjs';
import { marriageSupport } from './marriage.mjs';
import { planningView } from './ai-knowledge.mjs';
import { premiumObjectives } from './ambition-ai.mjs';
import { economicNeeds, tradeRoute } from './trade-economy.mjs';
import { tileProduction } from './economy.mjs';
import { decreeRemaining } from './opening.mjs';
import { alive, armiesOf, atWar, distance, kingdom, log, relation, settlements, strength, treaty } from './core.mjs';
import { isAiHouse, isHumanHouse } from './house-control.mjs';
import { changeRelation, contact, recordPoliticalMemory, tradeBlocked } from './living.mjs';
import { activePlan, createPlan, transitionPlan } from './plans.mjs';
import { initializeCooperation, pruneCooperation } from './cooperation-state.mjs';

export const NEGOTIATION_KINDS=['alliance','trade','embargo'];
const power=(s,id)=>militaryArmiesOf(s,id).reduce((n,a)=>n+strength(a),0)+settlements(s,id).length*35;
const sharedEnemies=(s,a,b)=>s.kingdoms.filter(k=>atWar(s,a,k.id)&&atWar(s,b,k.id)).length;
const separation=(s,a,b)=>Math.min(...settlements(s,a).flatMap(x=>settlements(s,b).map(y=>distance(x,y))));
const observedSupply=(s,owner,resource)=>Object.values(s.tiles).some(t=>t.fog!=='unknown'&&!t.inferredOwner&&t.owner===owner&&tileProduction(t,owner)[resource]>0);
export function cooperationInterest(s,observer,partner,kind,target=null) {
  s=planningView(s,observer);
  const k=kingdom(s,observer),r=relation(s,observer,partner),common=sharedEnemies(s,observer,partner),near=separation(s,observer,partner)<=14;
  const reasons=[];
  let score=r.trust*.6+r.reliability*.16-r.grievance*.6+common*20+(near?8:-8);
  score+=personalWillingness(s,observer,partner)+marriageSupport(s,observer,partner);
  if(r.trust>=30)reasons.push('Trust in this court');
  if(r.grievance>=25)reasons.push('Unresolved grievances');
  if(common)reasons.push('Shared enemies');
  if(near)reasons.push('Useful geographic position');
  const ratio=power(s,partner)/Math.max(1,power(s,observer));
  if(kind==='alliance'){
    score+=Math.min(15,ratio*7)+k.honor*8+r.fear*.1+r.dependency*.12;
    if(target){const danger=relation(s,observer,target);score+=danger.fear*.25+danger.grievance*.2-danger.dependency*.3;if(atWar(s,observer,target))score+=20;reasons.push(`Security concerning ${kingdom(s,target).name}`);}
  }else if(kind==='trade'){
    score+=15+k.greed*12+r.dependency*.2;
    if(economicNeeds(s,observer).some(n=>n.need>=8&&observedSupply(s,partner,n.resource))){score+=15;reasons.push('Known production can meet our resource needs');}
  }else if(target){const rival=relation(s,observer,target);score+=rival.grievance*.5-rival.dependency*.8+(atWar(s,observer,target)?20:0);reasons.push('Pressure on a rival weighed against lost trade');}
  return {score:Math.round(score),reasons:reasons.slice(0,4)};
}
function proposalError(s,p) {
  if(!alive(s,p.from)||!alive(s,p.to)||atWar(s,p.from,p.to))return 'War or defeat prevents cooperation.';
  if(p.kind==='trade'&&tradeBlocked(s,p.from,p.to))return 'Trade is blocked.';
  if(p.kind==='embargo'&&(!alive(s,p.target)||[p.from,p.to].some(id=>treaty(s,id,p.target,'alliance')||relation(s,id,p.target).dependency>=40)))return 'Existing alliances or trade dependence prevent an embargo.';
  return null;
}
export function proposeCooperation(s,from,to,kind,{target=null,planId=null,reason='Seek strategic cooperation.'}={}) {
  initializeCooperation(s);
  if(!NEGOTIATION_KINDS.includes(kind)||from===to||!kingdom(s,from)||!kingdom(s,to)||target&&(!kingdom(s,target)||[from,to].includes(target)))return null;
  if(s.cooperation.proposals.some(p=>p.kind===kind&&[p.from,p.to].includes(from)&&[p.from,p.to].includes(to)&&s.turn-p.created<6))return null;
  if(s.cooperation.proposals.filter(p=>['pending','counter'].includes(p.status)).length>=24)return null;
  const p={id:`NEG-${s.nextId}`,from,to,kind,target,planId,reason:reason.slice(0,200),created:s.turn,updated:s.turn,expires:s.turn+4,duration:12,status:'pending',response:''};
  if(proposalError(s,p)||treaty(s,from,to,kind))return null;
  s.nextId++;s.cooperation.proposals.push(p);return p;
}
function finish(s,p,status,reason) {
  p.status=status;p.response=reason.slice(0,240);p.updated=s.turn;
  const plan=s.intrigue.plans.find(x=>x.id===p.planId);
  if(plan&&activePlan(plan))transitionPlan(s,plan,status==='accepted'?'Completed':'Abandoned',reason);
}
export function respondCooperation(s,actor,id,decision) {
  const p=s.cooperation?.proposals.find(p=>p.id===id);
  if(s.outcome||!p||!['pending','counter'].includes(p.status)||s.turn>p.expires||actor!==(p.status==='counter'?p.from:p.to)||!['accept','decline','counter'].includes(decision))return {ok:false,error:'This proposal cannot be answered by your House.'};
  const error=proposalError(s,p);if(error){finish(s,p,'rejected',error);return {ok:false,error};}
  if(decision==='decline'){
    recordPoliticalMemory(s,p.from,p.to,'cooperation',`${kingdom(s,p.to).name} declined our ${p.kind} proposal: ${p.reason}`,6);
    if(p.kind==='alliance'&&p.target&&atWar(s,p.from,p.target))changeRelation(s,p.from,p.to,{respect:-1},'Declined requested security cooperation during our war.');
    finish(s,p,'rejected','The invited court declined these terms.');return {ok:true};
  }
  if(decision==='counter'){
    if(p.status==='counter')return {ok:false,error:'Counterterms must be accepted or declined.'};
    p.status='counter';p.duration=6;p.updated=s.turn;p.response='Offer a limited six-turn agreement instead of twelve turns.';return {ok:true};
  }
  if(!treaty(s,p.from,p.to,p.kind))s.treaties.push({id:`treaty-${s.nextId++}`,type:p.kind,parties:[p.from,p.to],expires:s.turn+p.duration,...(p.kind==='embargo'?{targetId:p.target}:{})});
  if(p.kind==='alliance'&&!treaty(s,p.from,p.to,'access'))s.treaties.push({id:`treaty-${s.nextId++}`,type:'access',parties:[p.from,p.to],expires:s.turn+p.duration});
  if(p.kind==='embargo')s.treaties=s.treaties.filter(t=>!(['trade','recurring'].includes(t.type)&&t.parties.includes(p.target)&&t.parties.some(id=>[p.from,p.to].includes(id))));
  for(const [a,b] of [[p.from,p.to],[p.to,p.from]]){
    changeRelation(s,a,b,{trust:4,respect:3},`Accepted a negotiated ${p.kind} agreement.`);
    recordPoliticalMemory(s,a,b,'cooperation',`${kingdom(s,b).name} accepted ${p.duration} turns of ${p.kind}.`,7);
  }
  finish(s,p,'accepted','Both courts accepted binding terms.');
  log(s,`${kingdom(s,p.from).name} and ${kingdom(s,p.to).name} ratify a ${p.duration}-turn ${p.kind} agreement.`,'diplomacy');
  return {ok:true};
}
export function negotiatePoliticalPlan(s,p) {
  const kind={seekAlliance:'alliance',secureTrade:'trade',embargo:'embargo'}[p.type],to=p.type==='embargo'?p.allies[0]:p.target;
  const existing=s.cooperation?.proposals.find(x=>x.planId===p.id);
  if(existing)return;
  const interest=to&&cooperationInterest(s,p.actor,to,kind,p.type==='embargo'?p.target:null);
  if(!to||interest.score<15){transitionPlan(s,p,'Abandoned','Strategic benefit does not justify these terms.');return;}
  const offer=proposeCooperation(s,p.actor,to,kind,{target:p.type==='embargo'?p.target:null,planId:p.id,reason:interest.reasons.join('; ')||p.objective});
  if(offer)transitionPlan(s,p,'Committed','Terms sent to the other court; awaiting a response.');
  else transitionPlan(s,p,'Abandoned','Existing terms or a recent negotiation prevent a new proposal.');
}
export function balanceResponse(s,observer,dominant) {
  const k=kingdom(s,observer),r=relation(s,observer,dominant);
  if(r.dependency>=30||k.greed>=.8&&r.grievance<25)return 'neutral';
  if(treaty(s,observer,dominant,'alliance')||r.fear>=40&&k.honor<.6&&r.grievance<35)return 'align';
  if(k.honor>=.7||r.grievance>=30||k.paranoia>=.7)return 'coalition';
  if(k.aggression>=.7)return 'support';
  return 'fortify';
}

// Motives are selected from observations and private economic needs. The
// simulation sends deterministic intentions; the existing conversation system
// remains responsible for expressing and discussing them with Gemini.
export function strategicOpportunities(world, owner) {
  const s=planningView(world,owner),k=kingdom(s,owner),needs=economicNeeds(s,owner),objectives=premiumObjectives(s,owner,needs),result=[];
  for(const other of s.kingdoms.filter(o=>o.id!==owner&&alive(s,o.id)&&!atWar(s,owner,o.id))){
    const r=relation(s,owner,other.id),near=separation(s,owner,other.id)<=14;
    if(!near)continue;
    const wanted=needs.filter(n=>n.need>=8&&observedSupply(s,other.id,n.resource)).sort((a,b)=>b.need*b.value-a.need*a.value||a.resource.localeCompare(b.resource))[0];
    if(wanted&&!tradeBlocked(s,owner,other.id)&&!treaty(s,owner,other.id,'trade')){
      const route=tradeRoute(s,owner,other.id);
      if(route.safe&&route.path.every(id=>s.tiles[id]?.fog!=='unknown'))result.push({to:other.id,kind:'trade',target:null,
        key:`amb-supply-${wanted.resource}-${wanted.state}`,score:35+k.greed*20+Math.min(20,wanted.need/3),
        reason:`Our ${wanted.resource} reserves need support, and our scouts know your productive holdings. Seek a trade agreement before negotiating shipments.`});
    }
    const common=s.kingdoms.find(enemy=>atWar(s,owner,enemy.id)&&atWar(s,other.id,enemy.id));
    if(!treaty(s,owner,other.id,'alliance')&&r.trust>=10&&(common||r.trust>=45&&r.wariness<20))result.push({to:other.id,kind:'alliance',target:common?.id||null,
      key:`amb-security-${common?.id||'trust'}-${common?'shared':Math.floor(r.trust/20)}`,
      score:(common?55:30)+k.honor*14+cooperationInterest(s,owner,other.id,'alliance',common?.id).score*.2,
      reason:common?`${kingdom(s,common.id).name} threatens both our Houses. Seek mutual defense with concrete obligations.`:'Our courts have earned mutual confidence. Discuss a binding alliance and the duties it creates.'});
    const claimed=Object.values(s.tiles).filter(t=>t.fog!=='unknown'&&t.owner===other.id&&t.premiumRegion&&needs.some(n=>n.resource===t.premiumYield&&(n.need>0||n.production<6)))
      .map(tile=>({tile,resource:tile.premiumYield,distance:Math.min(...settlements(s,owner).map(t=>distance(t,tile))),score:30}))
      .filter(o=>o.distance<=11).sort((a,b)=>a.distance-b.distance||a.tile.id.localeCompare(b.tile.id))[0];
    const contested=objectives.find(o=>o.rival===other.id&&o.tile.owner!==owner&&o.distance<=11&&o.score>=20)||claimed;
    if(contested)result.push({to:other.id,kind:'frontier',target:null,
      key:`amb-frontier-${contested.tile.premiumRegion}-${contested.tile.owner||'neutral'}`,
      score:40+k.ambition*14+k.aggression*8+Math.max(0,contested.score)*.2,
      reason:contested.tile.owner===other.id?`Your control of the rich ${contested.resource} territory at ${contested.tile.id} affects our frontier interests. Our council seeks an agreement on access and future boundaries.`:
        k.aggression>.7&&power(s,owner)>=power(s,other.id)*.8?`Your holdings approach the rich ${contested.resource} site at ${contested.tile.id}. We intend to secure its approaches; let us discuss boundaries before competing claims become a crisis.`:
          `Our scouts have found valuable ${contested.resource} territory at ${contested.tile.id}, close to your holdings. Our council seeks an agreement on access and competing frontier claims.`});
  }
  return result.sort((a,b)=>b.score-a.score||a.to.localeCompare(b.to)||a.key.localeCompare(b.key));
}
function initiateStrategicContact(s,k,view=null) {
  const influence=k.opening?.decree==='influence'&&decreeRemaining(s,k.id)>0;
  const cooldown=k.greed>=.75||k.honor>=.8?3:k.paranoia>=.75?6:4;
  const threshold=39+k.paranoia*9-k.greed*5-k.honor*4-(influence?5:0);
  if(s.cooperation.proposals.filter(p=>p.from===k.id&&['pending','counter'].includes(p.status)).length>=2)return;
  const options=strategicOpportunities(view||s,k.id);
  for(const opportunity of options){
    const r=relation(s,k.id,opportunity.to);
    if(opportunity.score<threshold||s.turn-(r.contacts['amb-initiative']??-100)<cooldown||r.contacts[opportunity.key]!==undefined)continue;
    if(s.cooperation.proposals.some(p=>[p.from,p.to].includes(k.id)&&[p.from,p.to].includes(opportunity.to)&&['pending','counter'].includes(p.status)))continue;
    if(isHumanHouse(s,opportunity.to)&&Object.values(r.contacts).some(turn=>turn===s.turn))continue;
    const proposal=opportunity.kind==='frontier'?null:proposeCooperation(s,k.id,opportunity.to,opportunity.kind,{target:opportunity.target,reason:opportunity.reason});
    if(opportunity.kind!=='frontier'&&!proposal)continue;
    if(isHumanHouse(s,opportunity.to)){
      if(!contact(s,k.id,opportunity.key,opportunity.reason,100000,opportunity.to))continue;
    }else{
      r.contacts[opportunity.key]=s.turn;
      recordPoliticalMemory(s,k.id,opportunity.to,'cooperation',opportunity.reason,6);
    }
    if(opportunity.kind==='frontier'){
      const neutralKey=opportunity.key.replace(/-[^-]+$/,'-neutral');
      if(neutralKey!==opportunity.key&&r.contacts[neutralKey]!==undefined){
        changeRelation(s,k.id,opportunity.to,{opinion:-2,wariness:3},'Expanded into a rich frontier region after our court raised competing interests.');
        recordPoliticalMemory(s,k.id,opportunity.to,'territory',opportunity.reason,7);
      }
    }
    r.contacts['amb-initiative']=s.turn;
    r.contacts=Object.fromEntries(Object.entries(r.contacts).sort((a,b)=>b[1]-a[1]).slice(0,36));
    return;
  }
}
function offerBalance(s,from,to,kind,dominant,signature,reason){
  const r=relation(s,from,to),key=`amb-balance-${dominant}-${kind}-${signature}`;
  if(r.contacts[key]!==undefined||s.turn-(r.contacts['amb-initiative']??-100)<4)return null;
  const offer=proposeCooperation(s,from,to,kind,{target:kind==='alliance'&&to!==dominant?dominant:null,reason});
  if(offer){
    r.contacts[key]=s.turn;r.contacts['amb-initiative']=s.turn;
    r.contacts=Object.fromEntries(Object.entries(r.contacts).sort((a,b)=>b[1]-a[1]).slice(0,36));
  }
  return offer;
}
export function runStrategicDiplomacy(s,onlyOwner=null) {
  initializeCooperation(s);
  if(!onlyOwner&&s.cooperation.lastDiplomacyTurn>=s.turn)return;
  for(const p of s.cooperation.proposals.filter(p=>['pending','counter'].includes(p.status))) {
    if(s.turn>p.expires){finish(s,p,'expired','The negotiation expired without consent.');continue;}
    if(p.updated>=s.turn)continue;
    const actor=p.status==='counter'?p.from:p.to,other=actor===p.from?p.to:p.from;
    if(!isAiHouse(s,actor)||onlyOwner&&actor!==onlyOwner)continue;
    const interest=cooperationInterest(s,actor,other,p.kind,p.target);
    const decision=interest.score>=38?'accept':interest.score>=20&&p.status==='pending'?'counter':interest.score>=25&&p.status==='counter'?'accept':'decline';
    respondCooperation(s,actor,p.id,decision);
  }
  const views=new Map(s.kingdoms.filter(k=>alive(s,k.id)&&isAiHouse(s,k.id)&&(!onlyOwner||k.id===onlyOwner)).map(k=>[k.id,planningView(s,k.id)]));
  {
    s.cooperation.balance=onlyOwner?s.cooperation.balance.filter(b=>b.house!==onlyOwner):[];
    for(const k of s.kingdoms.filter(k=>alive(s,k.id)&&isAiHouse(s,k.id)&&(!onlyOwner||k.id===onlyOwner))) {
      const view=views.get(k.id);
      const knownRich=id=>new Set(Object.values(view.tiles).filter(t=>t.fog!=='unknown'&&t.owner===id&&t.premiumRegion).map(t=>t.premiumRegion)).size;
      const ranked=view.kingdoms.filter(h=>alive(view,h.id)).map(h=>({id:h.id,rich:knownRich(h.id),power:power(view,h.id)})).sort((a,b)=>(b.power+b.rich*30)-(a.power+a.rich*30)||a.id.localeCompare(b.id));
      const dominant=ranked[0],total=ranked.reduce((n,h)=>n+h.power,0);
      const territorialConcern=dominant?.rich>=2&&dominant.rich>Math.max(0,...ranked.filter(h=>h.id!==dominant.id).map(h=>h.rich))&&separation(view,k.id,dominant.id)<=16;
      if(!dominant || dominant.id===k.id || !territorialConcern&&(dominant.power/total<.36 || dominant.power<=(ranked[1]?.power||0)*1.6))continue;
      const response=balanceResponse(s,k.id,dominant.id);
      const signature=`${dominant.rich}-${Math.floor(dominant.power/80)}`;
      s.cooperation.balance.push({house:k.id,dominant:dominant.id,response,turn:s.turn});
      if(response==='neutral')continue;
      const home=settlements(view,k.id)[0];
      if(response==='align'){offerBalance(s,k.id,dominant.id,'alliance',dominant.id,signature,'Seek security by aligning with the strongest House.');continue;}
      const frontier=settlements(view,k.id).sort((a,b)=>separationFrom(a)-separationFrom(b))[0];
      function separationFrom(t){return Math.min(...settlements(view,dominant.id).map(x=>distance(t,x)));}
      if(!s.intrigue.plans.some(p=>p.actor===k.id&&activePlan(p)&&p.type==='buildDefenses'))createPlan(s,k.id,'buildDefenses',{targetTile:frontier?.id||home.id,building:'wall',objective:'Fortify against the growing regional power.',delay:0});
      if(response==='coalition') {
        const partner=view.kingdoms.filter(o=>![k.id,dominant.id].includes(o.id)&&alive(view,o.id)&&!atWar(view,k.id,o.id)&&!treaty(view,o.id,dominant.id,'alliance'))
          .map(o=>({id:o.id,score:cooperationInterest(s,k.id,o.id,'alliance',dominant.id).score})).sort((a,b)=>b.score-a.score)[0];
        if(partner)offerBalance(s,k.id,partner.id,'alliance',dominant.id,signature,'A growing empire threatens the balance of power; seek mutual guarantees.');
      }
      // Support creates a real diplomatic goal. Resource aid remains governed
      // by the existing treasury/war-aid simulation, never narrative commands.
      if(response==='support') {
        const enemy=view.kingdoms.find(o=>o.id!==k.id&&atWar(view,o.id,dominant.id)&&!atWar(view,k.id,o.id));
        if(enemy)offerBalance(s,k.id,enemy.id,'trade',dominant.id,signature,'Sustain commerce with a rival of the dominant House.');
      }
    }
  }
  for(const k of s.kingdoms.filter(k=>alive(s,k.id)&&isAiHouse(s,k.id)&&(!onlyOwner||k.id===onlyOwner)))initiateStrategicContact(s,k,views.get(k.id));
  s.cooperation.lastDiplomacyTurn=s.turn;pruneCooperation(s);
}
