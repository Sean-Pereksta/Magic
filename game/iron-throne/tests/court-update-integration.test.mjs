import { activateForTest } from './fixtures/online-game.mjs';
// Run in the actual repository AFTER applying the guarded source changes.
// These integration tests are separate from the standalone component test run.
import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { onlineGame } from './fixtures/online-game.mjs';
import { kingdom, relation, parseSave } from '../core.mjs';
import { appendConversation, applySpeech } from '../living.mjs';
import { applyCommand } from '../multiplayer-commands.mjs';
import { splitCampaign, joinCampaign, playerView } from '../multiplayer-state.mjs';
import { validateIntent, commitDeal, evaluateDeal, deliverPledge, relationshipResponse, makeContext } from '../diplomacy.mjs';
import { marriageProposal, marriageBetween, marriageReadiness, marriageContext } from '../marriage.mjs';
import { sanitizeContext } from '../worker/worker.mjs';

function commands(s, meta) {
  let sequence = 0;
  return (actor, type, args) => {activateForTest(s,meta,actor);return ({ id: `court-regression-${++sequence}`, clientId: 'court-regression', sequence,
    uid: meta.seats[actor].uid, actorHouseId: actor, turn: s.turn, stateVersion: meta.stateVersion, epoch: meta.epoch, activationId:meta.activationId, type, args });};
}
function earned() {
  const s = createGame(); kingdom(s, 'ashen').resources.gold = 2000;
  for (let turn = 2; turn <= 10; turn += 2) {
    s.turn = turn;
    assert.equal(commitDeal(s, 'wintermere', validateIntent({ type: 'PROMISE', giveAmount: 60, duration: 2 }), 'ashen').ok, true);
    assert.equal(deliverPledge(s, s.pledges.at(-1).id, 'ashen').ok, true);
  }
  s.treaties.push({ id: 'earned-court-alliance', parties: ['ashen', 'wintermere'], type: 'alliance', expires: 50 });
  return s;
}

test('authoritative online chat rejects false Gemini denial, retains truthful Gemini speech and protects the secret', () => {
  const { state: s, meta } = onlineGame(2), cmd = commands(s, meta), host = 'sunspire', buyer = 'wintermere';
  kingdom(s, host).greed = .9; kingdom(s, host).honor = .4;
  Object.assign(relation(s, host, buyer), { trust: 15, opinion: 5 });
  const secret = 'I want to overthrow Wintermere. Will you join me?';
  appendConversation(s, host, 'player', secret, { actorHouseId: 'ashen' });
  const invalid = cmd(buyer, 'chat', {
    targetHouseId: host, message: 'Has anyone spoken to you about overthrowing my House?',
    response: { reply: 'No one has ever spoken to us about overthrowing your House.', intents: [], tone: 'neutral', source: 'gemini' }
  });
  const unchanged=structuredClone(s);
  assert.equal(applyCommand(s, meta, invalid).ok,false);assert.deepEqual(s,unchanged);
  const truthful='My court has heard relevant discussions. I can offer the information under these exact terms.';
  const result = applyCommand(s, meta, cmd(buyer, 'chat', {
    targetHouseId: host, message: 'Has anyone spoken to you about overthrowing my House?',
    response: { reply: truthful, intents: [], tone: 'neutral', source: 'gemini' }
  }));
  assert.equal(result.ok, true);
  assert.equal(s.courts[buyer].conversations[host].at(-1).text, truthful);
  assert.equal(s.courts[buyer].conversations[host].at(-1).source, 'gemini');
  const terms = s.courts[buyer].offers[host].find(i => i.type === 'INTELLIGENCE'); assert.ok(terms);
  const before = splitCampaign(s);
  assert.equal(JSON.stringify(before.world).includes(secret), false);
  assert.equal(JSON.stringify(before.privateByHouse[buyer]).includes(secret), false);
  assert.equal(JSON.stringify(before.privateByHouse[buyer]).includes('court-fact-'), false);
  const context = makeContext(playerView(before.world, before.privateByHouse[buyer], buyer), host, 'What is the price?', { actorHouseId: buyer });
  assert.ok(sanitizeContext(context)); assert.equal(JSON.stringify(context).includes(secret), false);
  const gold = kingdom(s, buyer).resources.gold;
  assert.equal(commitDeal(s, host, terms, buyer).ok, true);
  assert.equal(kingdom(s, buyer).resources.gold, gold - terms.giveAmount);
  assert.match(s.courts[buyer].conversations[host].at(-1).text, /House Ashen/);
  assert.equal(commitDeal(s, host, terms, buyer).ok, false);
  const after = splitCampaign(s), restored = joinCampaign(after.canonical, after.privateByHouse);
  assert.deepEqual(restored.courtIntelligence, s.courtIntelligence);
  assert.deepEqual(parseSave(JSON.stringify(s)).courtIntelligence, s.courtIntelligence);
});

test('a legal marriage receives authoritative consent wording and is not created until ratified', () => {
  const s = earned(), host = 'wintermere';
  applySpeech(s, host, 'Would you consider marriage? I seek the hand of your daughter.');
  s.turn++; applySpeech(s, host, 'Let us discuss the marriage settlement.');
  const terms = validateIntent(marriageProposal(s, host, ''));
  const verdict = evaluateDeal(s, host, terms);
  const exact = verdict.counter || terms;
  assert.equal(marriageReadiness(s, host, 'ashen').status, 'ready');
  const corrected = relationshipResponse(s, host, 'Let us discuss the marriage settlement.', {
    reply: 'Marriage is impossible in this kingdom. Send me a gift instead.', tone: 'hostile',
    intents: [validateIntent({ type: 'AID', giveAmount: 25 })]
  }, { actorHouseId: 'ashen', proposal: exact });
  assert.doesNotMatch(corrected.reply, /impossible|gift instead/);
  assert.equal(corrected.intents[0].type, 'MARRIAGE');
  assert.equal(corrected.intents.some(i => i.type === 'AID'), false);
  assert.equal(marriageBetween(s, 'ashen', host), undefined);
  assert.equal(commitDeal(s, host, exact, 'ashen').ok, true);
  assert.equal(marriageBetween(s, 'ashen', host).status, 'active');
  assert.equal(commitDeal(s, host, exact, 'ashen').ok, false);
});

test('a receiving human can discuss the match without resetting it or invalidating the original proposal', () => {
  const { state: s, meta } = onlineGame(2), cmd = commands(s, meta), a = 'ashen', b = 'wintermere';
  const chat = (from, to, message) => applyCommand(s, meta, cmd(from, 'chat', { targetHouseId: to, message }));
  assert.equal(chat(a, b, 'I would like my son to marry your daughter.').ok, true);
  assert.equal(chat(b, a, 'I will consider marriage between my daughter and your son.').ok, true);
  assert.equal(marriageContext(s, b, a).discussion.rounds, 1);
  s.turn++;
  assert.equal(chat(a, b, 'Let us discuss marriage between my son and your daughter for 100 gold.').ok, true);
  const intent = validateIntent(marriageProposal(s, b, '100 gold', a));
  const proposed = cmd(a, 'humanProposal', { targetHouseId: b, intent });
  assert.equal(applyCommand(s, meta, proposed).ok, true);
  assert.equal(marriageBetween(s, a, b), undefined);
  const gold = kingdom(s, a).resources.gold;
  assert.equal(applyCommand(s, meta, cmd(b, 'respondProposal', { id: proposed.id, decision: 'accept' })).ok, true);
  const marriage = marriageBetween(s, a, b);
  assert.equal(marriage.status, 'active');
  assert.equal(marriage.members[0].role, 'son'); assert.equal(marriage.members[1].role, 'daughter');
  assert.equal(kingdom(s, a).resources.gold, gold - intent.giveAmount);
  assert.equal(applyCommand(s, meta, cmd(b, 'respondProposal', { id: proposed.id, decision: 'accept' })).ok, false);
});
