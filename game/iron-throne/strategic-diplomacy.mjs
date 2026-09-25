import { alive, armiesOf, atWar, distance, kingdom, log, relation, settlements, strength, treaty } from './core.mjs';
import { isAiHouse } from './house-control.mjs';
import { changeRelation, recordPoliticalMemory, tradeBlocked } from './living.mjs';
import { activePlan, createPlan, transitionPlan } from './plans.mjs';
import { initializeCooperation, pruneCooperation } from './cooperation-state.mjs';

export const NEGOTIATION_KINDS=['alliance','trade','embargo'];
const power=(s,id)=>armiesOf(s,id).reduce((n,a)=>n+strength(a),0)+settlements(s,id).length*35;
const sharedEnemies=(s,a,b)=>s.kingdoms.filter(k=>atWar(s,a,k.id)&&atWar(s,b,k.id)).length;
const separation=(s,a,b)=>Math.min(...settlements(s,a).flatMap(x=>settlements(s,b).map(y=>distance(x,y))));
export function cooperationInterest(s,observer,partner,kind,target=null) {
  const k=kingdom(s,observer),r=relation(s,observer,partner),common=sharedEnemies(s,observer,partner),near=separation(s,observer,partner)<=14;
  const reasons=[];
  let score=r.trust*.6+r.reliability*.16-r.grievance*.6+common*20+(near?8:-8);
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
    const ours=kingdom(s,observer).resources,theirs=kingdom(s,partner).resources;
    if(Object.keys(ours).some(key=>ours[key]<60&&theirs[key]>100)){score+=15;reasons.push('Complementary resource needs');}
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
  if(decision==='decline'){finish(s,p,'rejected','The invited court declined these terms.');return {ok:true};}
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
export function runStrategicDiplomacy(s) {
  initializeCooperation(s);
  if(s.cooperation.lastDiplomacyTurn>=s.turn)return;
  for(const p of s.cooperation.proposals.filter(p=>['pending','counter'].includes(p.status))) {
    if(s.turn>p.expires){finish(s,p,'expired','The negotiation expired without consent.');continue;}
    if(p.updated>=s.turn)continue;
    const actor=p.status==='counter'?p.from:p.to,other=actor===p.from?p.to:p.from;
    if(!isAiHouse(s,actor))continue;
    const interest=cooperationInterest(s,actor,other,p.kind,p.target);
    const decision=interest.score>=38?'accept':interest.score>=20&&p.status==='pending'?'counter':interest.score>=25&&p.status==='counter'?'accept':'decline';
    respondCooperation(s,actor,p.id,decision);
  }
  if(s.turn>=6&&s.turn%4===2) {
    const ranked=s.kingdoms.filter(k=>alive(s,k.id)).map(k=>({id:k.id,power:power(s,k.id)})).sort((a,b)=>b.power-a.power||a.id.localeCompare(b.id));
    const dominant=ranked[0],total=ranked.reduce((n,k)=>n+k.power,0);
    s.cooperation.balance=[];
    if(dominant&&dominant.power/total>=.36&&dominant.power>ranked[1]?.power*1.6)for(const k of s.kingdoms.filter(k=>k.id!==dominant.id&&alive(s,k.id)&&isAiHouse(s,k.id))) {
      const response=balanceResponse(s,k.id,dominant.id);
      s.cooperation.balance.push({house:k.id,dominant:dominant.id,response,turn:s.turn});
      if(response==='neutral')continue;
      const home=settlements(s,k.id)[0];
      if(response==='align'){proposeCooperation(s,k.id,dominant.id,'alliance',{reason:'Seek security by aligning with the strongest House.'});continue;}
      const frontier=settlements(s,k.id).sort((a,b)=>separationFrom(a)-separationFrom(b))[0];
      function separationFrom(t){return Math.min(...settlements(s,dominant.id).map(x=>distance(t,x)));}
      if(!s.intrigue.plans.some(p=>p.actor===k.id&&activePlan(p)&&p.type==='buildDefenses'))createPlan(s,k.id,'buildDefenses',{targetTile:frontier?.id||home.id,building:'wall',objective:'Fortify against the growing regional power.',delay:0});
      if(response==='coalition') {
        const partner=s.kingdoms.filter(o=>![k.id,dominant.id].includes(o.id)&&alive(s,o.id)&&!atWar(s,k.id,o.id)&&!treaty(s,o.id,dominant.id,'alliance'))
          .map(o=>({id:o.id,score:cooperationInterest(s,k.id,o.id,'alliance',dominant.id).score})).sort((a,b)=>b.score-a.score)[0];
        if(partner)proposeCooperation(s,k.id,partner.id,'alliance',{target:dominant.id,reason:'A growing empire threatens the balance of power; seek mutual guarantees.'});
      }
      // Support creates a real diplomatic goal. Resource aid remains governed
      // by the existing treasury/war-aid simulation, never narrative commands.
      if(response==='support') {
        const enemy=s.kingdoms.find(o=>o.id!==k.id&&atWar(s,o.id,dominant.id)&&!atWar(s,k.id,o.id));
        if(enemy)proposeCooperation(s,k.id,enemy.id,'trade',{reason:'Sustain commerce with a rival of the dominant House.'});
      }
    }
  }
  s.cooperation.lastDiplomacyTurn=s.turn;pruneCooperation(s);
}
