import { RESOURCE_VALUES } from './data.mjs';
import { PLAYER, alive, armiesOf, atWar, canAfford, kingdom, log, pay, relation, strength, treaty } from './core.mjs';
import { planningView } from './ai-knowledge.mjs';
import { appendConversation, changeRelation, contact, recordPoliticalMemory, recordTrade } from './living.mjs';
import { createPlayerPromise } from './promises.mjs';
import { emotionalEvent, personalWillingness } from './emotions.mjs';

// Three adult roles per court, no inheritance, children, ages, or dynasty simulation.
export const FAMILY_ROLES = ['ruler','daughter','son'];
export const MARRIAGE_FIELDS = ['actorMember','rulerMember','shipmentResource','shipmentAmount','shipmentTurns','defense','trade'];
const keyFor=(a,b)=>[a,b].sort().join(':');
export function initializeMarriage(s) {if(s.royalBonds===undefined)s.royalBonds={version:1,negotiations:{},marriages:[]};}
export function marriageBetween(s,a,b) {return s.royalBonds?.marriages.find(m=>m.parties.includes(a)&&m.parties.includes(b));}
export function marriageSupport(s,a,b) {const m=marriageBetween(s,a,b);return m?.status==='active'?12:m?.status==='strained'?3:m?.status==='broken'?-20:0;}
export function availableFamily(s,id) {
  return FAMILY_ROLES.filter(role=>!s.royalBonds?.marriages.some(m=>m.members.some(x=>x.house===id&&x.role===role)));
}
export function memberName(s,id,role) {return role==='ruler'?kingdom(s,id).ruler:`the adult ${role} of ${kingdom(s,id).name}`;}
export function normalizeMarriageTerms(v) {
  const i={actorMember:v.actorMember??'ruler',rulerMember:v.rulerMember??'daughter',
    shipmentResource:v.shipmentResource??'food',shipmentAmount:v.shipmentAmount??0,shipmentTurns:v.shipmentTurns??0,
    defense:v.defense??false,trade:v.trade??false};
  if(!FAMILY_ROLES.includes(i.actorMember)||!FAMILY_ROLES.includes(i.rulerMember)||!['gold','food','iron','horses'].includes(i.shipmentResource)||
    !Number.isInteger(i.shipmentAmount)||i.shipmentAmount<0||i.shipmentAmount>100||!Number.isInteger(i.shipmentTurns)||i.shipmentTurns<0||i.shipmentTurns>20||
    (!!i.shipmentAmount!==!!i.shipmentTurns)||typeof i.defense!=='boolean'||typeof i.trade!=='boolean')return null;
  return i;
}
export function isMarriageTopic(message) {
  return /\b(?:marry|marriage|betroth\w*|wedding|dowry)\b|(?:join|unit[ei]|bind)\w* (?:our |the |both )?famil|hand of your (?:daughter|son)|our houses.*(?:bond permanent|permanent bond)/i.test(message);
}
const declinesMarriage=text=>/\b(?:not interested in|no interest in|don't want|do not want)\b.*\b(?:marriage|marry|joining)\b|\b(?:never|not|don't|do not|won't) (?:wish to |want to )?(?:marry|discuss marriage)|(?:withdraw|cancel|decline|reject) (?:the |our |my )?(?:marriage|match|proposal)/i.test(text);
function marriageDiscussion(s,host,message,actor,proposal=null){
  const n=negotiation(s,actor,host);
  return isMarriageTopic(message)||!!(n&&declinesMarriage(message))||proposal?.type==='MARRIAGE'||!!(n&&s.turn-n.lastDiscussed<=3&&/\b(?:settlement|terms|offer|gold|food|iron|horses|shipments|defense|trade|consider|accept|agree)\b/i.test(message));
}
function negotiation(s,a,b){const n=s.royalBonds?.negotiations[keyFor(a,b)];return n?.proposer===a&&n.host===b?n:null;}
export function discussMarriage(s,host,message,actor=PLAYER) {
  if(s.knowledgeView||s.projectionOnly||!marriageDiscussion(s,host,message,actor)||actor===host)return null;
  if(declinesMarriage(message)){if(s.royalBonds)delete s.royalBonds.negotiations[keyFor(actor,host)];return null;}
  initializeMarriage(s);
  const key=keyFor(actor,host),old=negotiation(s,actor,host);
  const actorRole=message.match(/\bmy (daughter|son)\b/i)?.[1]?.toLowerCase();
  const hostRole=message.match(/\byour (daughter|son)\b/i)?.[1]?.toLowerCase()||(/\bmarry (?:you|me)\b/i.test(message)?'ruler':null);
  const n=old&&s.turn-old.lastDiscussed<=12?old:{proposer:actor,host,started:s.turn,lastDiscussed:s.turn,rounds:0,
    actorMember:actorRole||'ruler',rulerMember:hostRole||'daughter'};
  if(actorRole)n.actorMember=actorRole;if(hostRole)n.rulerMember=hostRole;
  if(!n.rounds||n.lastDiscussed<s.turn)n.rounds=Math.min(20,n.rounds+1);
  n.lastDiscussed=s.turn;
  s.royalBonds.negotiations[key]=n;
  return n;
}
export function continueMarriageReview(s,host,actor=PLAYER){
  if(negotiation(s,actor,host))discussMarriage(s,host,'Let us discuss marriage.',actor);
}
function eligibility(s,host,actor,{consentingHuman=false}={}) {
  const r=relation(s,host,actor),p=r.personal,k=kingdom(s,host);
  if(atWar(s,actor,host))return 'Our Houses are at war. Peace must come before a joining of families.';
  if(marriageBetween(s,actor,host))return 'Our Houses already have a recorded marriage. Its history cannot be erased by another proposal.';
  if(!consentingHuman){
    if(r.grievance>=35||r.trust<25||p?.feelings.betrayedFriendship>=25)return 'There are wounds between our Houses that a marriage settlement cannot buy away.';
    if(!p||s.turn-p.started<8||p.deedTurns.length<4||r.reliability<60||r.respect<25)return 'This is premature. I need a longer record of dependable deeds before placing my family in your trust.';
    const personal=p.feelings.affection*.35+p.feelings.admiration*.2+p.feelings.attachment*.2;
    const usefulness=(treaty(s,actor,host,'alliance')?15:0)+s.kingdoms.filter(h=>atWar(s,host,h.id)&&atWar(s,actor,h.id)).length*8;
    const score=r.trust*.45+r.reliability*.15+r.respect*.15+personal+usefulness+personalWillingness(s,host,actor)-k.paranoia*12-r.grievance*.6;
    if(score<52)return 'I can see the political possibility, but trust, personal regard, and mutual purpose must grow further.';
    if(s.pledges.some(p=>p.debtor===actor&&p.creditor===host&&p.status==='pending'&&p.breached))return 'First answer for the promise you have already broken.';
  }
  return null;
}
function requiredSettlement(s,host,actor) {
  const k=kingdom(s,host),r=relation(s,host,actor),view=planningView(s,host);
  const power=id=>armiesOf(view,id).reduce((n,a)=>n+strength(a)*(a.confidence??1),0);
  const imbalance=Math.max(.6,Math.min(2.5,power(host)/Math.max(25,power(actor))));
  const closeness=Math.max(0,personalWillingness(s,host,actor));
  const value=Math.round(Math.max(20,Math.min(420,(45+k.resources.gold*.12)*(0.6+k.greed*.6)*imbalance-closeness*2-r.trust*.2)));
  const endangered=s.kingdoms.some(h=>atWar(s,host,h.id));
  const need=k.resources.food<50?'food':k.resources.iron<20?'iron':null;
  return {value,defense:endangered||k.paranoia>=.75,need,shipmentAmount:need?Math.max(3,Math.min(15,Math.round(8+k.greed*6-closeness*.15))):0,shipmentTurns:need?4:0};
}
export function evaluateMarriage(s,host,i,actor=PLAYER,options={}) {
  const reject=reason=>({status:'reject',reason,intent:i,factors:[]});
  const n=negotiation(s,actor,host);
  if(!n||s.turn-n.lastDiscussed>12)return reject('Raise the possibility of joining your families in conversation first.');
  if(n.rounds<2||s.turn<=n.started)return reject('I will consider this privately. Return to the subject on a later turn; my family is not a passing bargain.');
  if(!availableFamily(s,actor).includes(i.actorMember)||!availableFamily(s,host).includes(i.rulerMember))return reject('One of the named adults is already bound by a royal marriage.');
  if(i.actorMember!==n.actorMember||i.rulerMember!==n.rulerMember)return reject('Discuss the named family members with this court before changing the proposed match.');
  const why=eligibility(s,host,actor,options);if(why)return reject(why);
  if(i.targetId||i.receiveAmount||i.duration<10||!['gold','food','iron','horses'].includes(i.giveResource)||i.shipmentTurns>i.duration)return reject('A marriage requires 10–20 turns of mutual peace, a clear settlement, and shipments within that period.');
  if(!canAfford(kingdom(s,actor),{[i.giveResource]:i.giveAmount}))return reject('Your treasury cannot cover the marriage settlement.');
  const required=requiredSettlement(s,host,actor);
  const amount=options.consentingHuman?i.giveAmount:Math.ceil(required.value/RESOURCE_VALUES[i.giveResource]);
  const counter={...i,giveAmount:Math.max(i.giveAmount,amount),defense:i.defense||(!options.consentingHuman&&required.defense)};
  if(!options.consentingHuman&&required.need){counter.shipmentResource=required.need;counter.shipmentAmount=Math.max(i.shipmentResource===required.need?i.shipmentAmount:0,required.shipmentAmount);counter.shipmentTurns=Math.max(i.shipmentTurns,required.shipmentTurns);}
  if(JSON.stringify(counter)!==JSON.stringify(i))return {status:'counter',intent:i,counter,factors:[],reason:'I would consider this match with the revised settlement and obligations below. Read each promise before giving your word.'};
  return {status:'accept',intent:i,factors:[],reason:`Our court consents to the match between ${memberName(s,actor,i.actorMember)} and ${memberName(s,host,i.rulerMember)} on these exact terms. Ratification joins our Houses.`};
}
export function marriageProposal(s,host,message,actor=PLAYER) {
  const n=negotiation(s,actor,host);if(!n)return null;
  const previous=(s.controllers?s.courts?.[actor]?.offers:s.diplomacy?.offers)?.[host]?.find(i=>i.type==='MARRIAGE');
  const required=s.knowledgeView?{value:50,defense:false,need:null}:requiredSettlement(s,host,actor);
  const i={type:'MARRIAGE',duration:12,giveResource:'gold',giveAmount:required.value,receiveResource:'food',receiveAmount:0,targetId:'',
    actorMember:n.actorMember,rulerMember:n.rulerMember,shipmentResource:required.need||'food',shipmentAmount:required.shipmentAmount||0,shipmentTurns:required.shipmentTurns||0,defense:required.defense,trade:false,...previous};
  i.actorMember=n.actorMember;i.rulerMember=n.rulerMember;
  const shipment=message.match(/(\d{1,3})\s+(gold|food|iron|horses)\s+(?:each|per|a) turn(?:\s+for\s+(\d{1,2})\s+turns)?/i);
  const upfront=message.replace(shipment?.[0]||'\0','').match(/(\d{1,4})\s+(gold|food|iron|horses)/i);
  if(upfront){i.giveAmount=Number(upfront[1]);i.giveResource=upfront[2].toLowerCase();}
  if(shipment){i.shipmentAmount=Number(shipment[1]);i.shipmentResource=shipment[2].toLowerCase();i.shipmentTurns=Number(shipment[3]||4);}
  if(/\b(?:no|without|end) (?:recurring )?shipments\b/i.test(message)){i.shipmentAmount=0;i.shipmentTurns=0;}
  if(/\b(?:defend|defensive|protect)\b/i.test(message))i.defense=!/\b(?:no|without) defens/i.test(message);
  if(/\btrade\b/i.test(message))i.trade=!/\b(?:no|without) trade/i.test(message);
  return i;
}
export function marriageReply(s,host,message,actor=PLAYER,proposal=null) {
  if(!marriageDiscussion(s,host,message,actor,proposal))return null;
  if(declinesMarriage(message))return {reply:'Then we will set the question of marriage aside. No family commitment has been made.',tone:'neutral',intents:[]};
  if(marriageBetween(s,actor,host)){
    const m=marriageBetween(s,actor,host);
    return {reply:m.status==='broken'?'Our families were joined, and that makes the breach between us more painful. The record of it remains.':m.status==='strained'?'Our families are joined, but the obligations between us have been neglected. I need deeds that restore confidence.':'Our families are joined. Let our conduct continue to honor that bond.',tone:m.status==='active'?'warm':'cold',intents:[]};
  }
  // Read-only preview for the online client; only the authoritative chat command records discussion.
  const preview=structuredClone(s);delete preview.knowledgeView;
  if(proposal?.type==='MARRIAGE')continueMarriageReview(preview,host,actor);
  else discussMarriage(preview,host,message,actor);
  const i=proposal||marriageProposal(preview,host,message,actor);
  if(!i)return {reply:'If you wish to discuss a marriage, tell me which members of our families you have in mind.',tone:'guarded',intents:[]};
  if(s.knowledgeView)return {reply:'Our court will weigh the proposed match and its obligations. Nothing is settled until both Houses ratify the terms.',tone:'guarded',intents:[i]};
  if(!normalizeMarriageTerms(i)||!Number.isInteger(i.giveAmount)||i.giveAmount<0||i.giveAmount>1000)return {reply:'Let us use a settlement of up to 1000 resources and shipments of up to 100 per turn for at most 20 turns.',tone:'guarded',intents:[]};
  const v=evaluateMarriage(preview,host,i,actor);
  return {reply:v.reason,tone:v.status==='accept'?'warm':'guarded',intents:v.status==='reject'?[]:[v.counter||i],speechAct:v.status==='counter'?'counteroffer':v.status==='accept'?'accept':'statement'};
}
export function marriageContext(s,host,actor=PLAYER) {
  const m=marriageBetween(s,actor,host),n=negotiation(s,actor,host);
  return {discussion:n?{...n}:null,available:{speaker:availableFamily(s,actor),ruler:availableFamily(s,host)},marriage:m?{status:m.status,turn:m.turn,members:m.members,terms:m.terms}:null,
    rule:'Never initiate or suggest marriage. Only discuss it when the player raises it. Adults only. Discussion, consent, and exact ratification are separate. Do not invent relatives or wedding events.'};
}
export function commitMarriage(s,host,i,actor=PLAYER) {
  initializeMarriage(s);
  const m={id:`marriage-${s.nextId++}`,parties:[actor,host],members:[{house:actor,role:i.actorMember},{house:host,role:i.rulerMember}],terms:structuredClone(i),turn:s.turn,
    status:'active',missed:0,lastPaid:s.turn,shipmentsPaid:0,eventAfter:s.nextId-1,called:[]};
  // evaluateDeal has checked everything before either treasury or family is changed.
  pay(kingdom(s,actor),{[i.giveResource]:i.giveAmount});pay(kingdom(s,host),{[i.giveResource]:i.giveAmount},1);
  recordTrade(s,actor,host,i.giveResource,i.giveAmount,'marriage');
  s.royalBonds.marriages.push(m);delete s.royalBonds.negotiations[keyFor(actor,host)];
  for(const type of ['non-aggression',...(i.trade?['trade']:[])])s.treaties.push({id:`treaty-${s.nextId++}`,type,parties:[actor,host],expires:s.turn+i.duration});
  for(const [a,b]of [[actor,host],[host,actor]]){
    emotionalEvent(s,a,b,'marriage',{key:m.id,text:'Our royal families joined in a negotiated marriage.'});
    changeRelation(s,a,b,{trust:15,opinion:12,grievance:-5},'Our families joined by marriage.');
    recordPoliticalMemory(s,a,b,'marriage',`Our families joined by marriage; the settlement includes ${i.giveAmount} ${i.giveResource} and ${i.duration} turns of peace.`,10);
  }
  log(s,`${kingdom(s,actor).name} and ${kingdom(s,host).name} joined their families in marriage.`,'diplomacy',{public:true});
  return {ok:true};
}
export function strainMarriage(s,harmed,actor,reason,{severe=false,key=`breach:${s.turn}`}={}) {
  const m=marriageBetween(s,harmed,actor);if(!m||m.status==='broken')return;
  if(relation(s,harmed,actor)?.personal?.history.some(e=>e.key===`${m.id}:${key}`))return;
  m.missed++;
  const broken=severe||m.missed>=3;m.status=broken?'broken':'strained';
  emotionalEvent(s,harmed,actor,broken?'marriageBroken':'abandonment',{key:`${m.id}:${key}`,text:reason});
  changeRelation(s,harmed,actor,{trust:broken?-45:-12,grievance:broken?55:15,opinion:broken?-35:-10},reason);
  recordPoliticalMemory(s,harmed,actor,broken?'marriage-broken':'marriage-strained',reason,10);
  contact(s,harmed,`${m.id}:${key}`,broken?'Our families were bound together, and you have made that bond a wound neither court will forget.':'Our families are joined. I expected your promises to mean more than this.',100000,actor);
}
export function marriageWar(s,actor,victim) {
  strainMarriage(s,victim,actor,'Declared war against the House joined to our family.',{severe:true,key:'war'});
  for(const m of s.royalBonds?.marriages||[]){
    if(!m.parties.includes(victim)||m.parties.includes(actor)||m.status==='broken')continue;
    const relative=m.parties.find(id=>id!==victim);
    emotionalEvent(s,relative,actor,'abandonment',{key:`relative-war:${s.turn}:${victim}`,text:`Attacked our relatives in ${kingdom(s,victim).name}.`});
    changeRelation(s,relative,actor,{trust:-12,grievance:20},'Attacked a House joined to our royal family.');
  }
}
export function resolveMarriages(s) {
  initializeMarriage(s);
  for(const [key,n]of Object.entries(s.royalBonds.negotiations))if(s.turn-n.lastDiscussed>12)delete s.royalBonds.negotiations[key];
  for(const m of s.royalBonds.marriages){
    if(m.status==='broken')continue;
    const [a,b]=m.parties,i=m.terms;
    if(!alive(s,a)||!alive(s,b))continue;
    if(i.shipmentAmount&&m.lastPaid<s.turn&&m.shipmentsPaid<i.shipmentTurns){
      m.lastPaid=s.turn;m.shipmentsPaid++;
      const cost={[i.shipmentResource]:i.shipmentAmount};
      if(canAfford(kingdom(s,a),cost)){
        pay(kingdom(s,a),cost);pay(kingdom(s,b),cost,1);recordTrade(s,a,b,i.shipmentResource,i.shipmentAmount,'marriage');
        emotionalEvent(s,b,a,'promise',{key:`${m.id}:shipment:${s.turn}`,text:'The promised marriage shipment arrived.'});
      }else strainMarriage(s,b,a,'A promised marriage shipment failed.',{key:`shipment:${s.turn}`});
    }
    if(m.status==='broken')continue;
    if(i.defense&&s.turn<=m.turn+i.duration)for(const event of s.diplomacy.warHistory.filter(e=>e.id>m.eventAfter&&m.parties.includes(e.defender)&&!m.parties.includes(e.attacker)&&!m.called.includes(e.id))){
      m.called.push(event.id);m.called=m.called.slice(-80);
      const defender=event.defender,debtor=m.parties.find(id=>id!==defender);
      if(atWar(s,debtor,event.attacker))continue;
      const p=createPlayerPromise(s,defender,{type:'PLEDGE_WAR',targetId:event.attacker,giveResource:'gold',giveAmount:0,receiveResource:'food',receiveAmount:0,duration:3},debtor);
      p.marriageId=m.id;
      contact(s,defender,`family-call:${event.id}`,'Our families are joined, and our House is under attack. Your defensive promise is called. Will you stand with us, even if another friendship stands in the way?',100000,debtor);
      appendConversation(s,defender,'council',`Marriage defense called: aid against ${kingdom(s,event.attacker).name} by turn ${p.deadline}. The ledger records the choice.`,{actorHouseId:debtor,kind:'marriage',unread:true});
    }
    if(m.status==='strained'&&m.parties.every((a,index)=>{const r=relation(s,a,m.parties[1-index]);return r.trust>=55&&r.grievance<20;}))m.status='active';
  }
}
export function validateMarriage(s) {
  initializeMarriage(s);
  const fail=()=>{throw new Error('Damaged royal marriage data.');},obj=x=>x&&typeof x==='object'&&!Array.isArray(x),house=id=>s.kingdoms.some(k=>k.id===id),turn=n=>Number.isInteger(n)&&n>=0&&n<=s.turn;
  const d=s.royalBonds;
  if(!obj(d)||d.version!==1||!obj(d.negotiations)||Object.keys(d.negotiations).length>s.kingdoms.length*(s.kingdoms.length-1)/2||!Array.isArray(d.marriages)||d.marriages.length>s.kingdoms.length*3/2)fail();
  for(const [key,n]of Object.entries(d.negotiations))if(!obj(n)||!house(n.proposer)||!house(n.host)||n.proposer===n.host||key!==keyFor(n.proposer,n.host)||!turn(n.started)||!turn(n.lastDiscussed)||n.started>n.lastDiscussed||!Number.isInteger(n.rounds)||n.rounds<1||n.rounds>20||!FAMILY_ROLES.includes(n.actorMember)||!FAMILY_ROLES.includes(n.rulerMember))fail();
  const used=new Set(),pairs=new Set(),ids=new Set();
  for(const m of d.marriages){
    if(!obj(m)||typeof m.id!=='string'||m.id.length>60||ids.has(m.id)||!Array.isArray(m.parties)||m.parties.length!==2||m.parties.some(id=>!house(id))||m.parties[0]===m.parties[1]||pairs.has(keyFor(...m.parties))||!Array.isArray(m.members)||m.members.length!==2||!['active','strained','broken'].includes(m.status)||!turn(m.turn)||!turn(m.lastPaid)||!Number.isInteger(m.missed)||m.missed<0||m.missed>1000||!Number.isInteger(m.shipmentsPaid)||m.shipmentsPaid<0||m.shipmentsPaid>20||!Number.isInteger(m.eventAfter)||m.eventAfter<0||!Array.isArray(m.called)||m.called.length>80||m.called.some(x=>!Number.isInteger(x)||x<0))fail();
    ids.add(m.id);pairs.add(keyFor(...m.parties));
    const i=m.terms;
    if(!obj(i)||i.type!=='MARRIAGE'||!normalizeMarriageTerms(i)||!['gold','food','iron','horses'].includes(i.giveResource)||!Number.isInteger(i.giveAmount)||i.giveAmount<0||i.giveAmount>1000||i.receiveAmount!==0||i.targetId!==''||!Number.isInteger(i.duration)||i.duration<10||i.duration>20||i.shipmentTurns>i.duration||m.shipmentsPaid>i.shipmentTurns)fail();
    for(const [index,x]of m.members.entries()){
      if(!obj(x)||x.house!==m.parties[index]||x.role!==(index===0?i.actorMember:i.rulerMember)||used.has(`${x.house}:${x.role}`))fail();
      used.add(`${x.house}:${x.role}`);
    }
  }
  for(const p of s.pledges)if(p.marriageId&&!ids.has(p.marriageId))fail();
}
