import test from 'node:test';
import assert from 'node:assert/strict';
import {KNOWLEDGE_LIMITS,correspondenceFacts,recordRulerSpeech,recordJointWar,disclosureReply,evaluateIntelligence,commitIntelligence,projectRulerKnowledge,validateRulerKnowledge} from '../ruler-knowledge.mjs';
const ids=['ashen','wintermere','sunspire','redharbor','vesper','thornwall'];
function game(){
  return {turn:1,wars:[],treaties:[],conversations:{},controllers:Object.fromEntries(ids.map(id=>[id,{uid:['ashen','redharbor'].includes(id)?id:null,kind:['ashen','redharbor'].includes(id)?'human':'ai',substitute:false}])),courts:{},kingdoms:ids.map(id=>({id,name:`House ${id[0].toUpperCase()+id.slice(1)}`,honor:id==='vesper'?.25:.45,greed:id==='sunspire'?.95:.5,resources:{gold:500},relations:Object.fromEntries(ids.filter(x=>x!==id).map(x=>[x,{trust:15,opinion:5,reliability:50,grievance:0}]))}))};
}
const house=(s,id)=>s.kingdoms.find(k=>k.id===id);
const plot=(s,ruler='sunspire')=>recordRulerSpeech(s,ruler,'I want to overthrow Redharbor. Will you help me?','ashen');
function quotation(s,ruler='sunspire'){
  recordRulerSpeech(s,ruler,'Has anyone spoken to you about overthrowing my House?','redharbor');
  return recordRulerSpeech(s,ruler,'Could I pay you 25 gold to know who?','redharbor');
}

test('screenshot hostility is remembered but is not an agreed overthrow',()=>{
 const s=game();recordRulerSpeech(s,'sunspire','Heu Lord Dorian, I do not like Redharbor, what do you want to do about them?','ashen');
 assert.equal(s.rulerKnowledge.facts[0].kind,'hostility');
 const q=quotation(s);assert.equal(q.intents.length,1);
 const purchase=commitIntelligence(s,'sunspire',q.intents[0],'redharbor');
 assert.equal(purchase.ok,true);assert.match(purchase.report,/voiced hostility/);assert.match(purchase.report,/not an agreed attack/);
});
test('a proposal to cooperate is not mistaken for a question about intelligence',()=>{
 const s=game();
 for(const t of ['Would you help me overthrow Redharbor?','We should consider overthrowing Redharbor.','Will you attack Redharbor?','Let us move against Redharbor.']){
   assert.equal(recordRulerSpeech(s,'sunspire',t,'ashen'),null);
 }
 assert.ok(s.rulerKnowledge.facts.length>=4);
 assert.ok(s.rulerKnowledge.facts.every(f=>f.kind==='approach'));
});
test('questions, hearsay, denials and negated threats do not create plans',()=>{
 const s=game();
 for(const t of ['Is Ashen plotting against Redharbor?','Did Ashen ask you to attack Redharbor?','I heard Ashen wants to overthrow Redharbor.','I do not want to overthrow Redharbor.','We should not attack Redharbor.','Did you cancel the plan against Redharbor?'])assert.deepEqual(correspondenceFacts(s,'sunspire',t,'wintermere'),[],t);
});
test('the ruler knows both private conversations, but unrelated rulers do not',()=>{
 const s=game();plot(s);
 const denial=recordRulerSpeech(s,'sunspire','Is Ashen plotting against me?','redharbor');
 assert.match(denial.reply,/private correspondence/);
 assert.doesNotMatch(denial.reply,/No House|no one|no verified/);
 const ignorant=recordRulerSpeech(s,'wintermere','Is Ashen plotting against me?','redharbor');
 assert.match(ignorant.reply,/no verified correspondence/);
 assert.equal(ignorant.intents.length,0);
});
test('no verified report means no invented report, no sale and no promise of safety',()=>{
 const s=game();const r=quotation(s);assert.equal(r.intents.length,0);assert.match(r.reply,/not assurance/);assert.equal(s.rulerKnowledge.offers.length,0);
});
test('paid information has an exact reviewable quote and is delivered once',()=>{
 const s=game();plot(s);const q=quotation(s),i=q.intents[0];
 assert.equal(i.type,'INTELLIGENCE');assert.match(q.reply,/Accept & Ratify/);
 const buyer=house(s,'redharbor'),seller=house(s,'sunspire'),gold=buyer.resources.gold,sellerGold=seller.resources.gold;
 assert.equal(buyer.resources.gold,500);assert.equal(evaluateIntelligence(s,'sunspire',i,'redharbor').status,'accept');
 const result=commitIntelligence(s,'sunspire',i,'redharbor');assert.equal(result.ok,true);
 assert.equal(buyer.resources.gold,gold-i.giveAmount);assert.equal(seller.resources.gold,sellerGold+i.giveAmount);
 assert.match(result.report,/House Ashen/);assert.match(result.report,/approached our court/);assert.match(result.report,/does not establish consent/);
 assert.equal(commitIntelligence(s,'sunspire',i,'redharbor').ok,false);assert.equal(buyer.resources.gold,gold-i.giveAmount);
 const repeat=recordRulerSpeech(s,'sunspire','Can I pay again for that information?','redharbor');assert.equal(repeat.intents.length,0);assert.match(repeat.reply,/no second charge/);
});
test('gifts, chat agreement and model words cannot ratify a purchase',()=>{
 const s=game();plot(s);const q=quotation(s);const before=house(s,'redharbor').resources.gold;
 recordRulerSpeech(s,'sunspire','Yes, I agree to pay for that information.','redharbor');
 assert.equal(house(s,'redharbor').resources.gold,before);assert.equal(s.rulerKnowledge.receipts.length,0);
 assert.equal(commitIntelligence(s,'sunspire',{...q.intents[0],type:'AID'},'redharbor').ok,false);
});
test('wrong buyer, tampered terms and inadequate funds never transfer gold',()=>{
 const s=game();plot(s);const i=quotation(s).intents[0],before=JSON.stringify(s.kingdoms.map(k=>k.resources));
 assert.equal(commitIntelligence(s,'sunspire',i,'wintermere').ok,false);
 for(const patch of [{giveAmount:1},{giveResource:'food'},{receiveAmount:20},{duration:10},{targetId:'intel-9999'},{actorMember:'ruler'}])assert.equal(commitIntelligence(s,'sunspire',{...i,...patch},'redharbor').ok,false);
 assert.equal(JSON.stringify(s.kingdoms.map(k=>k.resources)),before);
 house(s,'redharbor').resources.gold=0;assert.equal(commitIntelligence(s,'sunspire',i,'redharbor').ok,false);assert.equal(s.rulerKnowledge.receipts.length,0);
});
test('expired and wartime quotes fail closed',()=>{
 for(const change of [s=>s.turn+=4,s=>s.wars.push('redharbor:sunspire')]){
  const s=game();plot(s);const i=quotation(s).intents[0];change(s);
  assert.equal(commitIntelligence(s,'sunspire',i,'redharbor').ok,false);assert.equal(house(s,'redharbor').resources.gold,500);
 }
});
test('an intentional denial retains the true fact and private motive',()=>{
 const s=game();plot(s,'vesper');s.treaties.push({type:'alliance',parties:['ashen','vesper'],expires:20});
 house(s,'vesper').relations.ashen.trust=70;
 const r=recordRulerSpeech(s,'vesper','Has anyone approached you about overthrowing my House?','redharbor');
 assert.match(r.reply,/No House/);const audit=s.rulerKnowledge.decisions.at(-1);assert.equal(audit.policy,'deny');assert.equal(audit.factIds.length,1);assert.match(audit.motive,/protect/);
 const pay=recordRulerSpeech(s,'vesper','Could I pay you for the information?','redharbor');assert.equal(pay.intents.length,0);assert.match(pay.reply,/existing bond/);
 assert.doesNotMatch(JSON.stringify(r),/motive|factIds|conceal/);
});
test('an honorable ruler protects a closer ally without inventing a denial',()=>{
 const s=game();house(s,'sunspire').honor=.9;plot(s);s.treaties.push({type:'alliance',parties:['ashen','sunspire'],expires:20});
 const r=quotation(s);assert.match(r.reply,/will not disclose/);assert.equal(r.intents.length,0);assert.ok(s.rulerKnowledge.decisions.every(d=>d.policy!=='deny'));
});
test('earned confidence may disclose a limited account without demanding gold',()=>{
 const s=game();plot(s);Object.assign(house(s,'sunspire').relations.redharbor,{trust:55,reliability:70});
 const r=recordRulerSpeech(s,'sunspire','Who is plotting against me?','redharbor');assert.equal(r.intents.length,0);assert.match(r.reply,/Another House/);assert.doesNotMatch(r.reply,/House Ashen/);assert.equal(house(s,'redharbor').resources.gold,500);
});
test('a ratified marriage can support voluntary named disclosure',()=>{
 const s=game();plot(s);s.royalBonds={marriages:[{status:'active',parties:['sunspire','redharbor']}]};
 const r=recordRulerSpeech(s,'sunspire','Who is plotting against me?','redharbor');assert.match(r.reply,/House Ashen/);assert.equal(r.intents.length,0);
});
test('formal joint war and a withdrawal remain distinct historical facts',()=>{
 const s=game();plot(s);recordJointWar(s,'sunspire',{type:'JOINT_WAR',targetId:'redharbor'},'ashen');s.turn++;
 recordRulerSpeech(s,'sunspire','I withdraw the plan to attack Redharbor.','ashen');
 const i=quotation(s).intents[0],r=commitIntelligence(s,'sunspire',i,'redharbor');assert.match(r.report,/ratified joint war/);assert.match(r.report,/withdrawing their proposal/);assert.match(r.report,/not proof/);
});
test('private projection reveals no raw source, audit, or undelivered report',()=>{
 const s=game();plot(s);const i=quotation(s).intents[0];
 const view=projectRulerKnowledge(s,'redharbor');assert.equal(view.offers.length,1);assert.equal(view.receipts.length,0);
 assert.doesNotMatch(JSON.stringify(view),/House Ashen|factIds|evidence|motive|overthrow Redharbor/);
 assert.deepEqual(projectRulerKnowledge(s,'wintermere'),{offers:[],receipts:[]});assert.deepEqual(projectRulerKnowledge(s,'public'),{offers:[],receipts:[]});
 commitIntelligence(s,'sunspire',i,'redharbor');assert.match(projectRulerKnowledge(s,'redharbor').receipts[0].report,/House Ashen/);
 assert.deepEqual(projectRulerKnowledge(s,'wintermere'),{offers:[],receipts:[]});
});
test('projected clients never originate records, quotes or transfers',()=>{
 const s=game();plot(s);const i=quotation(s).intents[0];const v=structuredClone(s);v.knowledgeView='redharbor';v.rulerDisclosures=projectRulerKnowledge(s,'redharbor');delete v.rulerKnowledge;
 const before=JSON.stringify(v);assert.equal(recordRulerSpeech(v,'sunspire','I want to overthrow Ashen.','redharbor'),null);
 assert.equal(commitIntelligence(v,'sunspire',i,'redharbor').ok,false);assert.equal(JSON.stringify(v),before);
 const r=disclosureReply(v,'sunspire','Who is plotting against me?','redharbor');assert.match(r.reply,/must review/);assert.equal(r.intents.length,0);
});
test('legacy actual player correspondence migrates; ruler inventions do not',()=>{
 const s=game();s.courts.ashen={conversations:{sunspire:[{role:'player',text:'I want to overthrow Redharbor.',turn:1},{role:'ruler',text:'We ratified joint war against Redharbor.',turn:1}]}};
 const i=quotation(s).intents[0];assert.ok(i);assert.equal(s.rulerKnowledge.facts.length,1);assert.equal(s.rulerKnowledge.facts[0].kind,'approach');assert.equal(validateRulerKnowledge(s),true);
});
test('reload preserves the exact outstanding quote and receipt',()=>{
 const s=game();plot(s);const i=quotation(s).intents[0];const restored=JSON.parse(JSON.stringify(s));assert.equal(validateRulerKnowledge(restored),true);
 assert.equal(commitIntelligence(restored,'sunspire',i,'redharbor').ok,true);assert.equal(validateRulerKnowledge(restored),true);assert.equal(validateRulerKnowledge(JSON.parse(JSON.stringify(restored))),true);
});
test('a later trade or marriage conversation ends an intelligence follow-up',()=>{
 const s=game();plot(s);quotation(s);recordRulerSpeech(s,'sunspire','I have food. What do you need in trade?','redharbor');
 assert.equal(recordRulerSpeech(s,'sunspire','I can pay 25 gold for food.','redharbor'),null);
 assert.equal(recordRulerSpeech(s,'sunspire','Could we get married?','redharbor'),null);
});
test('bounded memory marks incompleteness instead of asserting universal safety',()=>{
 const s=game();for(let n=0;n<KNOWLEDGE_LIMITS.facts+20;n++){s.turn++;recordRulerSpeech(s,'sunspire',`I want to overthrow Redharbor. Proposal ${n}.`,'ashen');}
 assert.equal(s.rulerKnowledge.facts.length,KNOWLEDGE_LIMITS.facts);assert.equal(s.rulerKnowledge.incomplete,true);assert.equal(validateRulerKnowledge(s),true);
});
test('malformed ledgers are rejected rather than trusted as model evidence',()=>{
 const s=game();assert.equal(validateRulerKnowledge(s),true);plot(s);const bad=structuredClone(s);bad.rulerKnowledge.facts[0].actor='not-a-house';assert.equal(validateRulerKnowledge(bad),false);
 const future=structuredClone(s);future.rulerKnowledge.facts[0].turn=999;assert.equal(validateRulerKnowledge(future),false);
 const overflow=structuredClone(s);overflow.rulerKnowledge.facts.push(...Array(200).fill(s.rulerKnowledge.facts[0]));assert.equal(validateRulerKnowledge(overflow),false);
});

test('first-person request wording does not change an explicitly named target',()=>{
 const s=game();plot(s);
 recordRulerSpeech(s,'sunspire','Tell me whether Ashen is plotting against Redharbor.','wintermere');
 assert.equal(s.rulerKnowledge.decisions.at(-1).subject,'redharbor');
 const q=recordRulerSpeech(s,'sunspire','Can I pay for that information?','wintermere');
 assert.equal(q.intents.length,1);assert.match(q.reply,/Redharbor/);
});
test('direct invitations are recognized, while negated inflected attacks are not',()=>{
 const s=game();
 for(const t of ['Do you want to help me overthrow Redharbor?','Could you join me in attacking Redharbor?'])assert.equal(correspondenceFacts(s,'sunspire',t,'ashen')[0]?.kind,'approach');
 for(const t of ['I am not attacking Redharbor.','I do not plan on invading Redharbor.'])assert.deepEqual(correspondenceFacts(s,'sunspire',t,'ashen'),[]);
});
test('a report shared after a marriage makes an old paid quotation unnecessary',()=>{
 const s=game();plot(s);const i=quotation(s).intents[0];
 s.royalBonds={marriages:[{status:'active',parties:['sunspire','redharbor']}]};
 recordRulerSpeech(s,'sunspire','Who is plotting against me?','redharbor');
 assert.equal(s.rulerKnowledge.receipts[0].kind,'shared');
 assert.equal(commitIntelligence(s,'sunspire',i,'redharbor').ok,false);
 assert.equal(house(s,'redharbor').resources.gold,500);assert.equal(validateRulerKnowledge(s),true);
 // Disclosure survives normal rolling decision history, not only the chat window.
 s.rulerKnowledge.decisions=[];
 const repeated=recordRulerSpeech(s,'sunspire','Who is plotting against me?','redharbor');
 assert.match(repeated.reply,/already shared/);assert.equal(repeated.intents.length,0);
});
