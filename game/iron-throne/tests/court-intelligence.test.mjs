import test from 'node:test';
import assert from 'node:assert/strict';
import {
  classifyCourtApproach, initializeCourtIntelligence, recordCourtConversation,
  prepareCourtInquiry, intelligenceReply, projectCourtIntelligence,
  evaluateIntelligenceSale, commitIntelligenceSale, validateCourtIntelligence,
  isIntelligenceQuestion
} from '../court-intelligence.mjs';

function game() {
  const ids = ['ashen', 'sunspire', 'redharbor', 'wintermere'];
  return {
    turn: 1, wars: [], treaties: [], royalBonds: { marriages: [] },
    controllers: Object.fromEntries(ids.map(id => [id, { kind: id === 'sunspire' ? 'ai' : 'human', substitute: false }])),
    courts: Object.fromEntries(ids.map(id => [id, { conversations: {}, offers: {} }])),
    kingdoms: ids.map(id => ({ id, name: `House ${id[0].toUpperCase()}${id.slice(1)}`, honor: .4, greed: .9,
      resources: { gold: 500 }, relations: Object.fromEntries(ids.filter(x => x !== id).map(x => [x, { opinion: 5, trust: 15 }])) }))
  };
}
const ruler = (s, id) => s.kingdoms.find(k => k.id === id);
const approach = s => recordCourtConversation(s, 'sunspire', 'ashen', 'I want to overthrow Redharbor. Will you join me?');
const inquire = s => { prepareCourtInquiry(s, 'sunspire', 'Has anyone approached you about overthrowing our House?', 'redharbor'); return intelligenceReply(s, 'sunspire', 'Who is plotting against us?', 'redharbor'); };
const purchase = s => inquire(s).intents[0];

test('a ruler retains a first-hand approach across different players', () => {
  const s = game(); approach(s); const reply = inquire(s);
  assert.equal(s.courtIntelligence.facts.length, 1);
  assert.equal(s.courtIntelligence.facts[0].speaker, 'ashen');
  assert.equal(s.courtIntelligence.facts[0].target, 'redharbor');
  assert.equal(reply.intents[0].type, 'INTELLIGENCE');
  assert.doesNotMatch(reply.reply, /no one|nobody|no approaches/i);
});

test('the screenshot wording records hostility, not an agreed overthrow', () => {
  const s = game(); const f = classifyCourtApproach(s, 'sunspire', 'ashen', 'Hey Lord Dorian, I do not like Redharbor, what do you want to do about them?');
  assert.equal(f.length, 1); assert.equal(f[0].kind, 'hostility');
});

test('a suggestion never creates war, a pledge or an operation', () => {
  const s = game(); approach(s); assert.deepEqual(s.wars, []); assert.deepEqual(s.treaties, []);
  assert.equal(s.pledges, undefined); assert.equal(s.cooperation, undefined);
});

test('negated attacks, rumors, ambiguous targets and questions are not evidence', () => {
  const s = game();
  for (const text of [
    'I do not want to overthrow Redharbor.', "We should not attack Redharbor.",
    'I heard Ashen wants to overthrow Redharbor.', 'Has anyone mentioned overthrowing Redharbor?',
    'I want to attack Redharbor and Wintermere.', 'Redharbor sells iron.',
    'He said "I want to overthrow Redharbor".', "I don't plan to attack Redharbor."
  ]) assert.deepEqual(classifyCourtApproach(s, 'sunspire', 'ashen', text), [], text);
});

test('unknown is explicitly uncertainty and never a paid empty report', () => {
  const s = game(), reply = inquire(s);
  assert.equal(reply.intents.length, 0); assert.match(reply.reply, /not a guarantee/);
  assert.equal(s.courtIntelligence.offers.length, 0);
});

test('source marriage protects confidence, not a fabricated denial', () => {
  const s = game(); approach(s);
  s.royalBonds.marriages.push({ parties: ['sunspire', 'ashen'], status: 'active' });
  const reply = inquire(s); assert.match(reply.reply, /refusal is not a denial/); assert.equal(reply.intents.length, 0);
});

test('a trusted allied target can get a genuine, free warning', () => {
  const s = game(); approach(s);
  Object.assign(ruler(s, 'sunspire').relations.redharbor, { trust: 65, opinion: 60 });
  s.treaties.push({ type: 'alliance', parties: ['sunspire', 'redharbor'], expires: 20 });
  const reply = inquire(s); assert.match(reply.reply, /House Ashen/); assert.match(reply.reply, /not proof of an accepted conspiracy/);
  assert.equal(reply.intents.length, 0); assert.equal(s.courtIntelligence.receipts[0].paid, 0);
  assert.equal(ruler(s, 'redharbor').resources.gold, 500);
});

test('an unratified quotation or ordinary gift does not buy intelligence', () => {
  const s = game(); approach(s); const terms = purchase(s);
  assert.equal(ruler(s, 'redharbor').resources.gold, 500); assert.equal(s.courtIntelligence.receipts.length, 0);
  ruler(s, 'redharbor').resources.gold -= terms.giveAmount; ruler(s, 'sunspire').resources.gold += terms.giveAmount; // unrelated gift
  const reply = intelligenceReply(s, 'sunspire', 'I paid you gold, tell me.', 'redharbor');
  assert.equal(reply.intents.length, 1); assert.doesNotMatch(reply.reply, /House Ashen/);
});

test('ratification atomically charges once and delivers the dated report', () => {
  const s = game(); approach(s); const terms = purchase(s);
  assert.equal(evaluateIntelligenceSale(s, 'sunspire', terms, 'redharbor').status, 'accept');
  const result = commitIntelligenceSale(s, 'sunspire', terms, 'redharbor');
  assert.equal(result.ok, true); assert.match(result.report, /turn 1, House Ashen/);
  assert.equal(ruler(s, 'redharbor').resources.gold, 500 - terms.giveAmount);
  assert.equal(ruler(s, 'sunspire').resources.gold, 500 + terms.giveAmount);
  assert.equal(commitIntelligenceSale(s, 'sunspire', terms, 'redharbor').ok, false);
  assert.equal(ruler(s, 'redharbor').resources.gold, 500 - terms.giveAmount);
});

test('changed price, extra fields and wrong buyer cannot ratify a quote', () => {
  const s = game(); approach(s); const terms = purchase(s);
  for (const t of [{ ...terms, giveAmount: terms.giveAmount - 1 }, { ...terms, giveResource: 'food' }, { ...terms, forged: true }, { ...terms, receiveAmount: 50 }])
    assert.equal(commitIntelligenceSale(s, 'sunspire', t, 'redharbor').ok, false);
  assert.equal(commitIntelligenceSale(s, 'sunspire', terms, 'wintermere').ok, false);
  assert.equal(ruler(s, 'redharbor').resources.gold, 500);
});

test('empty treasury, expired quote and changed loyalties fail without payment', () => {
  for (const change of [s => { ruler(s, 'redharbor').resources.gold = 0; }, s => { s.turn += 3; }, s => { s.wars.push('redharbor:sunspire'); }, s => { s.royalBonds.marriages.push({ parties: ['sunspire', 'ashen'], status: 'active' }); }]) {
    const s = game(); approach(s); const terms = purchase(s); change(s);
    const before = ruler(s, 'redharbor').resources.gold;
    assert.equal(commitIntelligenceSale(s, 'sunspire', terms, 'redharbor').ok, false);
    assert.equal(ruler(s, 'redharbor').resources.gold, before); assert.equal(s.courtIntelligence.receipts.length, 0);
  }
});

test('repeated questions reuse an open quote and cannot farm free records', () => {
  const s = game(); approach(s); const first = purchase(s);
  for (let n = 0; n < 10; n++) assert.deepEqual(purchase(s), first);
  assert.equal(s.courtIntelligence.offers.length, 1); assert.equal(s.courtIntelligence.inquiries.length, 1);
});

test('unbought secrets and other buyers do not leak through projections', () => {
  const s = game(); approach(s); purchase(s);
  for (const viewer of ['redharbor', 'wintermere', 'public']) {
    const view = projectCourtIntelligence(s, viewer), serialized = JSON.stringify(view);
    assert.doesNotMatch(serialized, /court-fact-|fingerprint|I want to overthrow|"speaker"|"facts"/);
    assert.doesNotMatch(serialized, /ashen/i);
  }
});

test('only the purchasing player receives the report after disclosure', () => {
  const s = game(); approach(s); commitIntelligenceSale(s, 'sunspire', purchase(s), 'redharbor');
  assert.match(JSON.stringify(projectCourtIntelligence(s, 'redharbor')), /House Ashen/);
  assert.doesNotMatch(JSON.stringify(projectCourtIntelligence(s, 'wintermere')), /House Ashen/);
  assert.deepEqual(projectCourtIntelligence(s, 'public').receipts, []);
});

test('projected state cannot mutate, charge or manufacture private evidence', () => {
  const s = game(); approach(s); const terms = purchase(s);
  const view = { ...structuredClone(s), knowledgeView: 'redharbor', courtIntelligence: projectCourtIntelligence(s, 'redharbor') };
  const before = JSON.stringify(view);
  recordCourtConversation(view, 'sunspire', 'ashen', 'I want to overthrow Redharbor.');
  prepareCourtInquiry(view, 'sunspire', 'Who is plotting against us?', 'redharbor');
  assert.equal(commitIntelligenceSale(view, 'sunspire', terms, 'redharbor').ok, false);
  assert.equal(JSON.stringify(view), before);
});

test('older retained conversations backfill with original dates, not invented history', () => {
  const s = game(); s.turn = 30;
  s.courts.ashen.conversations.sunspire = [{ role: 'player', text: 'I want to overthrow Redharbor.', turn: 2, kind: '' }];
  initializeCourtIntelligence(s); assert.equal(s.courtIntelligence.facts[0].turn, 2);
  const terms = purchase(s); const result = commitIntelligenceSale(s, 'sunspire', terms, 'redharbor');
  assert.match(result.report, /turn 2/); assert.match(result.report, /not proof/);
});

test('a save round trip preserves secrets, purchases and replay protection', () => {
  const s = game(); approach(s); const terms = purchase(s); commitIntelligenceSale(s, 'sunspire', terms, 'redharbor');
  const restored = JSON.parse(JSON.stringify(s)); validateCourtIntelligence(restored);
  assert.equal(commitIntelligenceSale(restored, 'sunspire', terms, 'redharbor').ok, false);
  assert.match(intelligenceReply(restored, 'sunspire', 'Who is plotting against us?', 'redharbor').reply, /House Ashen/);
});

test('no other ruler gains knowledge just because Sunspire heard something', () => {
  const s = game(); approach(s); s.controllers.wintermere.kind = 'ai';
  prepareCourtInquiry(s, 'wintermere', 'Who is plotting against us?', 'redharbor');
  const reply = intelligenceReply(s, 'wintermere', 'Who is plotting against us?', 'redharbor');
  assert.equal(reply.intents.length, 0); assert.doesNotMatch(reply.reply, /House Ashen/);
});

test('marriage and trade do not get trapped in a previous intelligence discussion', () => {
  const s = game(); approach(s); inquire(s);
  for (const text of ['Let us discuss marriage.', 'I offer 100 gold as a dowry.', 'I want to trade food for 25 gold.', 'Let us negotiate a loan.'])
    assert.equal(intelligenceReply(s, 'sunspire', text, 'redharbor'), null);
  assert.equal(isIntelligenceQuestion('Who is plotting against me?'), true);
});

test('malformed imported ledger data fails closed', () => {
  for (const mutate of [d => { d.sequence = -1; }, d => { d.facts[0].target = 'not-a-house'; }, d => { d.offers[0].price = -10; }, d => { d.facts = Array(385).fill(d.facts[0]); }]) {
    const s = game(); approach(s); purchase(s); mutate(s.courtIntelligence);
    assert.throws(() => validateCourtIntelligence(s), /Damaged private court intelligence/);
  }
});

test('a directly named suspect question about the buyer is still an inquiry', () => {
  assert.equal(isIntelligenceQuestion('Is Ashen planning to overthrow me?'), true);
});

test('a conversational yes still requires explicit report ratification', () => {
  const s = game(); approach(s); inquire(s);
  prepareCourtInquiry(s, 'sunspire', 'Yes, please.', 'redharbor');
  const reply = intelligenceReply(s, 'sunspire', 'Yes, please.', 'redharbor');
  assert.equal(reply.intents[0].type, 'INTELLIGENCE'); assert.equal(ruler(s, 'redharbor').resources.gold, 500);
});

test('a new subject closes ambiguous intelligence follow-ups', () => {
  const s = game(); approach(s); inquire(s);
  prepareCourtInquiry(s, 'sunspire', 'Let us discuss marriage.', 'redharbor');
  assert.equal(intelligenceReply(s, 'sunspire', 'Yes, please.', 'redharbor'), null);
});

test('a temporarily substituted human ruler cannot automatically sell private correspondence', () => {
  const s = game(); approach(s); s.controllers.sunspire = { kind: 'human', substitute: true };
  prepareCourtInquiry(s, 'sunspire', 'Who is plotting against us?', 'redharbor');
  assert.equal(s.courtIntelligence.offers.length, 0);
  assert.equal(intelligenceReply(s, 'sunspire', 'Who is plotting against us?', 'redharbor'), null);
});
