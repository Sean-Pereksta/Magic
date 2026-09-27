// Private, authoritative correspondence. Never pass this ledger to another
// player's view or to a language model. Only approved statements leave it.
export const KNOWLEDGE_LIMITS = Object.freeze({ facts:192, offers:24, receipts:48, decisions:64 });
const TYPES = ['hostility','approach','withdrawal','joint-war'];
const POLICIES = ['unknown','withhold','deny','disclose','quote','purchased','pending','closed'];
const own = (s,id) => s.kingdoms?.find(k=>k.id===id);
const name = (s,id) => own(s,id)?.name || id;
const relation = (s,a,b) => own(s,a)?.relations?.[b] || {};
const authoritative = s => !s.knowledgeView && !s.projectionOnly;
const atWar = (s,a,b) => (s.wars||[]).includes([a,b].sort().join(':'));
const allied = (s,a,b) => (s.treaties||[]).some(t=>t.type==='alliance'&&t.expires>s.turn&&t.parties.includes(a)&&t.parties.includes(b));
const married = (s,a,b) => (s.royalBonds?.marriages||[]).some(m=>m.status==='active'&&m.parties.includes(a)&&m.parties.includes(b));
const human = (s,id) => (s.controllers?.[id]?.kind || (id==='ashen'?'human':'ai'))==='human' && !s.controllers?.[id]?.substitute;
const clone = x => structuredClone(x);
const integer = (n,min=0,max=1000000) => Number.isSafeInteger(n)&&n>=min&&n<=max;
const text = (v,n) => typeof v==='string'&&v.length<=n;
const id = v => typeof v==='string'&&/^(?:fact|intel|receipt|decision)-[1-9]\d{0,11}$/.test(v);
const escapeRE = s => s.replace(/[.*+?^${}()|[\]\\]/g,'\\$&');
const normalize = s => String(s||'').replace(/[’‘]/g,"'").trim();
const say = (reply, tone='guarded', intents=[]) => ({reply,tone,intents,speechAct:intents.length?'counteroffer':'statement'});
const safeDecision = d => say(d.reply,d.tone,clone(d.intents||[]));
const freshDecision = (s,ruler,actor) => {
  const d=[...(s.rulerKnowledge?.decisions||[])].reverse().find(d=>d.ruler===ruler&&d.actor===actor&&d.turn<=s.turn&&s.turn-d.turn<=3);
  return d?.policy==='closed'?null:d;
};
const intentFor = q => ({type:'INTELLIGENCE',targetId:q.id,giveResource:'gold',giveAmount:q.price,receiveResource:'food',receiveAmount:0,duration:3});

function mention(s,message,exclude=[]) {
  return (s.kingdoms||[]).filter(k=>!exclude.includes(k.id)&&new RegExp(`\\b(?:${escapeRE(k.id)}|${escapeRE(k.name||k.id)})\\b`,'i').test(message));
}

// This records an utterance, not the truth of every claim inside it. Hearsay,
// questions about an accusation, negated attacks and model prose are not plots.
export function correspondenceFacts(s,ruler,message,actor,turn=s.turn) {
  const t=normalize(message);
  if(!own(s,ruler)||!own(s,actor)||actor===ruler||!integer(turn,0,s.turn)||!t)return [];
  const third=mention(s,t,[actor,ruler]);
  if(!third.length)return [];
  const question=/^(?:has|have|had|is|are|was|were|did|does|do(?! you (?:want|wish)\b))\b/i.test(t)||/\b(?:who|anyone|someone|somebody|rumou?rs?|heard|claims?|says?|said that|told me)\b/i.test(t);
  const withdrawal=!question&&/\b(?:withdraw(?:ing)?|abandon(?:ing)?|cancel(?:ling)?|retract(?:ing)?)\b.{0,55}\b(?:plan|proposal|attack|plot|campaign|scheme)\b/i.test(t);
  const negated=/\b(?:never|not|don't|do not|won't|will not|wouldn't|would not|cannot|can't|refuse to)\b.{0,45}\b(?:attack\w*|overthrow\w*|invad\w*|betray\w*|plot\w*|conspir\w*|depos\w*|destroy\w*|war)\b/i.test(t);
  const approach=!question&&!negated&&(/\b(?:let us|let's|we (?:should|could|must|can|will)|i (?:want|intend|plan|propose)|join me|help me|would you|will you|can you|could you|do you (?:want|wish)|shall we)\b.{0,90}\b(?:attack(?:ing)?|overthrow(?:ing)?|invad(?:e|ing)|betray(?:ing)?|plot(?:ting)?|conspir(?:e|ing)|depos(?:e|ing)|destroy(?:ing)?|mov(?:e|ing) against|bring(?:ing)? down|tak(?:e|ing) down|tak(?:e|ing) out)\b/i.test(t)||/^joint war\s*[·:]/i.test(t));
  const hostility=!question&&(/\bi (?:do not like|don't like|dislike|distrust|hate)\b/i.test(t)||/\bour (?:enemy|enemies)\b/i.test(t));
  const kind=withdrawal?'withdrawal':approach?'approach':hostility?'hostility':null;
  if(!kind)return [];
  return third.slice(0,3).map(k=>({turn,actor,ruler,subject:k.id,kind,evidence:t.slice(0,600)}));
}

function addFact(k,f) {
  if(k.facts.some(x=>x.turn===f.turn&&x.actor===f.actor&&x.ruler===f.ruler&&x.subject===f.subject&&x.kind===f.kind&&x.evidence===f.evidence))return;
  k.facts.push({id:`fact-${++k.sequence}`,...f});
  if(k.facts.length>KNOWLEDGE_LIMITS.facts){k.facts.splice(0,k.facts.length-KNOWLEDGE_LIMITS.facts);k.incomplete=true;}
}

function migrateCorrespondence(s,k) {
  // One-time recovery from old saves. Only actual player-authored entries are
  // eligible; an AI claiming an agreement never creates a verified agreement.
  const courts=s.controllers?s.courts||{}:{ashen:{conversations:s.conversations||{}}};
  const rows=[];
  for(const [actor,c] of Object.entries(courts))for(const [ruler,history] of Object.entries(c?.conversations||{})){
    if(!Array.isArray(history))continue;
    for(const m of history)if(m?.role==='player'&&typeof m.text==='string'&&integer(m.turn,0,s.turn))
      rows.push(...correspondenceFacts(s,ruler,m.text,actor,m.turn));
  }
  rows.sort((a,b)=>a.turn-b.turn).forEach(f=>addFact(k,f));
  k.migrated=true;
}

function ledger(s) {
  if(!authoritative(s))return null;
  if(s.rulerKnowledge===undefined)s.rulerKnowledge={version:1,sequence:0,migrated:false,incomplete:false,facts:[],offers:[],receipts:[],decisions:[]};
  if(!validateRulerKnowledge(s))throw new Error('Invalid private correspondence ledger.');
  const k=s.rulerKnowledge;
  if(!k.migrated)migrateCorrespondence(s,k);
  for(const q of k.offers)if(q.status==='open'&&q.expires<s.turn)q.status='expired';
  return k;
}

function queryFor(s,ruler,message,actor,proposal=null) {
  if(proposal)return proposal.type==='INTELLIGENCE'?{offerId:proposal.targetId,subject:actor,source:null}:null;
  const t=normalize(message);
  if(/\b(?:marry|married|marriage|wedding|dowry|betroth\w*)\b/i.test(t))return null;
  if(correspondenceFacts(s,ruler,t,actor).some(f=>['approach','hostility','withdrawal'].includes(f.kind)))return null;
  const danger=/\b(?:plot\w*|conspir\w*|overthrow\w*|depos\w*|schem\w*|betray\w*)\b/i.test(t);
  const correspondence=/\b(?:approach\w*|spoke|spoken|talk\w*|contact\w*|discuss\w*|ask\w*)\b/i.test(t);
  const question=/\?|\b(?:who|anyone|someone|somebody|has|have|did|does|is|are|what|tell|know|reveal|heard|information|rumou?r)\b/i.test(t);
  const direct=question&&(danger||correspondence&&/\b(?:against|about me|about us|my house|our house|attack\w*|war|take out|take down)\b/i.test(t));
  const prior=freshDecision(s,ruler,actor);
  const followup=prior&&/\b(?:pay|paid|gold|coin\w*|price|cost|information|tell me|reveal|disclose|who was|who is|who approached|those terms|that report)\b/i.test(t);
  if(!direct&&!followup)return null;
  // A new explicit subject overrides the last enquiry; pronouns always refer
  // to the current actor, never to the player's previous multiplayer seat.
  let subject=actor;
  if(direct){
    // "Tell me about Ashen plotting against Redharbor" targets Redharbor,
    // not the speaker merely because the request contains "me".
    const phrases=[...t.matchAll(/\b(?:against|overthrow(?:ing)?|depose|attack(?:ing)?)\s+(?:house\s+)?(my house|our house|me|us|[a-z]+)\b/gi)];
    const targets=phrases.map(m=>/^(?:me|us|my house|our house)$/i.test(m[1])?actor:own(s,m[1].toLowerCase())?.id).filter(Boolean);
    if(targets.length)subject=targets.at(-1);
  }else if(prior)subject=prior.subject;
  if(!own(s,subject)||subject===ruler)return null;
  const named=mention(s,t,[ruler,subject]);
  const source=direct?(/\b(?:have|did|was|am) i\b/i.test(t)?actor:named[0]?.id||null):prior?.source||null;
  return {subject,source,paid:/\b(?:pay|paid|gold|coin\w*|price|cost|buy|purchase)\b/i.test(t)};
}

function knownFacts(s,k,ruler,q) {
  return (k?.facts||[]).filter(f=>f.turn<=s.turn&&f.subject===q.subject&&[f.actor,f.ruler].includes(ruler)&&(!q.source||f.actor===q.source||f.ruler===q.source));
}
function bond(s,a,b) {
  if(a===b)return 200;
  return (married(s,a,b)?65:0)+(allied(s,a,b)?35:0)+Math.max(0,relation(s,a,b).trust||0)*.4;
}
function reportFor(s,rows,subject,identified,ruler) {
  const selected=rows.slice(-3);
  const lines=selected.map(f=>{
    const source=f.actor===ruler?'Our court':identified?name(s,f.actor):'Another House';
    const recipient=f.ruler===ruler?'our court':identified?name(s,f.ruler):'another court';
    if(f.kind==='joint-war')return `On turn ${f.turn}, ${source} and ${recipient} ratified joint war against ${name(s,subject)}. This records an agreement, not proof that an attack occurred.`;
    if(f.kind==='withdrawal')return `On turn ${f.turn}, ${source} said they were withdrawing their proposal concerning ${name(s,subject)}. That statement is not proof of their present intentions.`;
    if(f.kind==='approach')return `On turn ${f.turn}, ${source} approached ${recipient} about acting against ${name(s,subject)}. The approach alone does not establish consent or an active conspiracy.`;
    return `On turn ${f.turn}, ${source} voiced hostility toward ${name(s,subject)} in correspondence with ${recipient}. Hostility is not an agreed attack.`;
  });
  return `Dated correspondence through turn ${s.turn}: ${lines.join(' ')}${rows.length>selected.length?' This account covers only the three most recent relevant entries.':''}`.slice(0,1550);
}

function computeDecision(s,k,ruler,message,actor,q,{record=false}={}) {
  const base={ruler,actor,subject:q.subject,source:q.source||null,turn:s.turn,message:normalize(message).slice(0,600),intents:[],tone:'guarded',factIds:[]};
  const result=(policy,reply,extras={})=>({...base,policy,reply,...extras});
  if(q.offerId){
    const offer=(k?.offers||s.rulerDisclosures?.offers||[]).find(o=>o.id===q.offerId&&o.seller===ruler&&o.buyer===actor);
    if(!offer)return result('unknown','Ask my court for a current, specific information quotation before offering payment.');
    const v=evaluateIntelligence(s,ruler,intentFor(offer),actor);
    return result('quote',v.reason,{intents:v.status==='accept'?[intentFor(offer)]:[]});
  }
  if(!authoritative(s))return result('pending','My court must review what can be disclosed. No information sale or payment is agreed by these words.');
  const facts=knownFacts(s,k,ruler,q),discussed=facts.filter(f=>f.kind!=='withdrawal');
  base.factIds=facts.slice(-3).map(f=>f.id);
  if(!discussed.length)return result('unknown','I have no verified correspondence I can substantiate for that question. That is not assurance that no one is plotting. I will not sell you an invented report.');
  const bought=(k.receipts||[]).findLast(x=>x.seller===ruler&&x.buyer===actor&&x.subject===q.subject&&base.factIds.every(f=>x.factIds.includes(f)));
  if(bought)return result(bought.kind==='shared'?'disclose':'purchased',`${bought.kind==='shared'?'I have already shared this account with you; no payment is required.':'You have already paid for this account; there is no second charge.'} ${bought.report}`.slice(0,1600));
  const disclosed=(k.decisions||[]).findLast(x=>x.policy==='disclose'&&x.ruler===ruler&&x.actor===actor&&x.subject===q.subject&&base.factIds.every(f=>x.factIds.includes(f)));
  if(disclosed)return result('disclose',disclosed.reply,{tone:'warm'});
  const host=own(s,ruler),r=relation(s,ruler,actor),buyerBond=bond(s,ruler,actor);
  const protectedSources=discussed.filter(f=>{
    const source=f.actor===ruler?f.ruler:f.actor;
    return source!==actor&&bond(s,ruler,source)>=35&&bond(s,ruler,source)>buyerBond+10;
  });
  // A denial is allowed only as a recorded, deterministic act of concealment.
  // The true facts and motive stay in the private ledger, not in the response.
  if(protectedSources.length){
    if(!q.paid&&host.honor<.4&&(r.trust||0)<25)return result('deny',`No House has approached me about acting against ${q.subject===actor?'your House':name(s,q.subject)}.`,{motive:'conceal-correspondence-to-protect-a-closer-bond',factIds:protectedSources.slice(-3).map(f=>f.id)});
    return result('withhold','I will not disclose private correspondence at the expense of an existing bond. Gold does not purchase every confidence.');
  }
  if(atWar(s,ruler,actor)||(r.trust||0)<-10||(r.grievance||0)>=40)return result('withhold','There is too little confidence between our Houses for me to open private correspondence. Settle our grievances before asking for those confidences.');
  if(buyerBond>=50||(r.trust||0)>=50&&(r.reliability||0)>=60){
    const identified=married(s,ruler,actor)||(r.trust||0)>=65;
    return result('disclose',reportFor(s,facts,q.subject,identified,ruler),{tone:'warm'});
  }
  if(!q.paid)return result('withhold','I do not open private correspondence merely because I am asked. We may discuss a limited, dated account on explicit terms; I will not turn a suspicion into proof of a plot.');
  const selected=facts.slice(-3),factIds=selected.map(f=>f.id);
  const existing=k.offers.findLast(o=>o.status==='open'&&o.expires>=s.turn&&o.seller===ruler&&o.buyer===actor&&o.subject===q.subject&&JSON.stringify(o.factIds)===JSON.stringify(factIds));
  if(!record&&!existing)return result('pending','We can discuss an information purchase, but my court must first issue an exact quotation. No payment is authorized yet.');
  const price=Math.max(10,Math.min(150,Math.round(15+(host.greed||0)*25+(selected.some(f=>f.kind==='joint-war')?20:0)-(r.trust||0)*.15)));
  let offer=existing;
  if(!offer){
    if(k.offers.length>=KNOWLEDGE_LIMITS.offers){const removable=k.offers.findIndex(o=>o.status!=='open');if(removable<0)return result('withhold','Resolve or allow an outstanding information quotation to expire before requesting another.');k.offers.splice(removable,1);}
    const identified=host.honor<.65||(r.trust||0)>=35;
    offer={id:`intel-${++k.sequence}`,seller:ruler,buyer:actor,subject:q.subject,price,created:s.turn,expires:s.turn+3,status:'open',factIds,
      scope:`A dated account of ${selected.length} relevant correspondence entr${selected.length===1?'y':'ies'} concerning ${name(s,q.subject)}; ${identified?'named sources':'sources remain anonymous'}.`,
      report:reportFor(s,facts,q.subject,identified,ruler)};
    k.offers.push(offer);
  }
  return result('quote',`${offer.scope} The price is ${offer.price} gold. Review and Accept & Ratify this quotation by turn ${offer.expires}. Only ratification transfers the gold and delivers that account; ordinary gifts do not buy information.`,{intents:[intentFor(offer)]});
}

// Call only after the engine accepts a player's message. The multiplayer
// controller, not the requesting browser or model, records this decision.
export function recordRulerSpeech(s,ruler,message,actor='ashen') {
  if(!authoritative(s)||!own(s,ruler)||!own(s,actor)||actor===ruler||typeof message!=='string'||message.length>600)return null;
  const k=ledger(s);
  for(const f of correspondenceFacts(s,ruler,message,actor))addFact(k,f);
  if(human(s,ruler))return null; // Never write a human ruler's response for them.
  const q=queryFor(s,ruler,message,actor);
  if(!q&&!freshDecision(s,ruler,actor))return null;
  const d=q?computeDecision(s,k,ruler,message,actor,q,{record:true}):{ruler,actor,subject:actor,source:null,turn:s.turn,message:normalize(message),intents:[],tone:'neutral',factIds:[],policy:'closed',reply:''};
  if(d.policy==='disclose'&&d.factIds.length&&!k.receipts.some(r=>r.buyer===actor&&r.seller===ruler&&d.factIds.every(id=>r.factIds.includes(id)))){
    k.receipts.push({id:`receipt-${++k.sequence}`,quoteId:null,kind:'shared',seller:ruler,buyer:actor,subject:d.subject,turn:s.turn,price:0,report:d.reply.slice(0,1550),factIds:[...d.factIds]});
    if(k.receipts.length>KNOWLEDGE_LIMITS.receipts)k.receipts.splice(0,k.receipts.length-KNOWLEDGE_LIMITS.receipts);
  }
  d.id=`decision-${++k.sequence}`;k.decisions.push(d);
  if(k.decisions.length>KNOWLEDGE_LIMITS.decisions)k.decisions.splice(0,k.decisions.length-KNOWLEDGE_LIMITS.decisions);
  return q?safeDecision(d):null;
}

export function recordJointWar(s,ruler,intent,actor='ashen') {
  if(!authoritative(s)||intent.type!=='JOINT_WAR'||!own(s,intent.targetId)||[ruler,actor].includes(intent.targetId))return;
  const k=ledger(s);
  addFact(k,{turn:s.turn,actor,ruler,subject:intent.targetId,kind:'joint-war',evidence:'A joint-war agreement was ratified by the deterministic council.'});
}

export function disclosureReply(s,ruler,message,actor='ashen',proposal=null) {
  if(human(s,ruler))return null;
  const q=queryFor(s,ruler,message,actor,proposal);if(!q)return null;
  const d=freshDecision(s,ruler,actor);
  if(!proposal&&d?.turn===s.turn&&d.message===normalize(message))return safeDecision(d);
  // Read-only previews must not manufacture memory, spend gold or issue quotes.
  const k=s.rulerKnowledge;
  return safeDecision(computeDecision(s,k,ruler,message,actor,q));
}

export function evaluateIntelligence(s,ruler,i,actor='ashen') {
  const reject=reason=>({status:'reject',reason,intent:i,factors:[]});
  const quote=(authoritative(s)?s.rulerKnowledge?.offers:s.rulerDisclosures?.offers)?.find(q=>q.id===i.targetId&&q.seller===ruler&&q.buyer===actor);
  if(s.outcome||!own(s,ruler)||!own(s,actor)||actor===ruler)return reject('This court is unavailable.');
  if(!quote)return reject('Request a specific information quotation in conversation first.');
  if(quote.status!=='open')return reject(quote.status==='purchased'?'This account has already been purchased. No second payment is due.':'This information quotation is no longer open.');
  if(quote.expires<s.turn)return reject('This information quotation has expired. Request a fresh review.');
  const expected=intentFor(quote);
  if(Object.keys(i).some(key=>!(key in expected))||Object.keys(expected).some(key=>i[key]!==expected[key]))return reject('The payment must match the exact information quotation. Ask the ruler to reconsider instead of altering its terms.');
  if(authoritative(s)&&(s.rulerKnowledge?.receipts||[]).some(r=>r.buyer===actor&&r.seller===ruler&&quote.factIds.every(id=>r.factIds.includes(id))))return reject('You already have this account. No further payment is due.');
  if(atWar(s,ruler,actor))return reject('War has closed this information offer. No gold has been transferred.');
  if(!Number.isFinite(own(s,actor).resources?.gold)||own(s,actor).resources.gold<quote.price)return reject('Your treasury cannot cover this information purchase. No gold has been transferred.');
  return {status:'accept',reason:`Ratify to pay ${quote.price} gold and immediately receive the quoted, dated account. ${quote.scope}`,intent:i,factors:[]};
}

export function commitIntelligence(s,ruler,i,actor='ashen') {
  if(!authoritative(s))return {ok:false,error:'Only the authoritative court can complete an information purchase.'};
  const k=ledger(s),v=evaluateIntelligence(s,ruler,i,actor);
  if(v.status!=='accept')return {ok:false,error:v.reason};
  const q=k.offers.find(o=>o.id===i.targetId),seller=own(s,ruler),buyer=own(s,actor);
  if(!Number.isFinite(seller.resources?.gold))return {ok:false,error:'The receiving treasury is unavailable.'};
  // The quote contains the approved historical snapshot. Payment, delivery and
  // consumption happen synchronously, after every check and with no model call.
  const receipt={id:`receipt-${++k.sequence}`,quoteId:q.id,kind:'purchase',seller:ruler,buyer:actor,subject:q.subject,turn:s.turn,price:q.price,report:q.report,factIds:[...q.factIds]};
  buyer.resources.gold-=q.price;seller.resources.gold+=q.price;q.status='purchased';
  k.receipts.push(receipt);
  if(k.receipts.length>KNOWLEDGE_LIMITS.receipts)k.receipts.splice(0,k.receipts.length-KNOWLEDGE_LIMITS.receipts);
  return {ok:true,report:receipt.report,receiptId:receipt.id};
}

// The public/player projection contains neither undisclosed source identities,
// raw utterances, internal fact IDs nor the deliberate-deception audit.
export function projectRulerKnowledge(s,viewer) {
  const k=s.rulerKnowledge;
  const offers=(k?.offers||[]).filter(q=>q.buyer===viewer&&q.expires>=s.turn&&q.status==='open').map(q=>({id:q.id,seller:q.seller,buyer:q.buyer,subject:q.subject,price:q.price,created:q.created,expires:q.expires,status:q.status,scope:q.scope}));
  const receipts=(k?.receipts||[]).filter(r=>r.buyer===viewer).map(r=>({id:r.id,kind:r.kind,seller:r.seller,buyer:r.buyer,subject:r.subject,turn:r.turn,price:r.price,report:r.report}));
  return {offers,receipts};
}

export function validateRulerKnowledge(s) {
  const k=s.rulerKnowledge;if(k===undefined)return true;
  const houses=new Set((s.kingdoms||[]).map(h=>h.id)),house=x=>houses.has(x),turn=x=>integer(x,0,s.turn),refs=x=>Array.isArray(x)&&x.length<=3&&x.every(id);
  if(!k||k.version!==1||!integer(k.sequence,0,999999999999)||typeof k.migrated!=='boolean'||typeof k.incomplete!=='boolean')return false;
  for(const [key,max] of Object.entries(KNOWLEDGE_LIMITS))if(!Array.isArray(k[key])||k[key].length>max)return false;
  if(k.facts.some(f=>!f||!id(f.id)||!turn(f.turn)||![f.actor,f.ruler,f.subject].every(house)||new Set([f.actor,f.ruler,f.subject]).size!==3||!TYPES.includes(f.kind)||!text(f.evidence,600)))return false;
  if(k.offers.some(q=>!q||!id(q.id)||![q.seller,q.buyer,q.subject].every(house)||q.seller===q.buyer||!integer(q.price,10,150)||!turn(q.created)||!integer(q.expires,q.created,q.created+3)||!['open','expired','purchased'].includes(q.status)||!refs(q.factIds)||!text(q.scope,360)||!text(q.report,1550)))return false;
  if(k.receipts.some(r=>!r||!id(r.id)||!['purchase','shared'].includes(r.kind)||(r.kind==='shared'?r.quoteId!==null||r.price!==0:!id(r.quoteId)||!integer(r.price,10,150))||![r.seller,r.buyer,r.subject].every(house)||r.seller===r.buyer||!turn(r.turn)||!refs(r.factIds)||!text(r.report,1550)))return false;
  if(k.decisions.some(d=>!d||!id(d.id)||![d.ruler,d.actor,d.subject].every(house)||d.source!==null&&!house(d.source)||!turn(d.turn)||!text(d.message,600)||!text(d.reply,1600)||(d.motive!==undefined&&!text(d.motive,180))||!['warm','guarded','neutral','cold','hostile'].includes(d.tone)||!POLICIES.includes(d.policy)||!refs(d.factIds)||!Array.isArray(d.intents)||d.intents.length>1||d.intents.some(i=>i.type!=='INTELLIGENCE'||!id(i.targetId)||!integer(i.giveAmount,10,150)||i.giveResource!=='gold'||i.receiveAmount!==0||i.receiveResource!=='food'||i.duration!==3)))return false;
  const rows=[...k.facts,...k.offers,...k.receipts,...k.decisions];
  return new Set(rows.map(r=>r.id)).size===rows.length&&rows.every(r=>Number(r.id.split('-').at(-1))<=k.sequence);
}
