/**
 * Private, first-hand diplomatic knowledge. This module deliberately has no
 * imports: it can be used by the controller, save validator and fog projection
 * without adding another core/living/diplomacy import cycle.
 *
 * Model output is never evidence and never authorizes disclosure or payment.
 * An approach is not an accepted operation. Unknown is not a denial.
 */
export const INTELLIGENCE_INTENT = 'INTELLIGENCE';
const LIMIT = Object.freeze({ facts: 384, offers: 96, receipts: 96, inquiries: 144 });
const house = (s, id) => s.kingdoms.find(h => h.id === id);
const name = (s, id) => String(house(s, id)?.name || id).slice(0, 70);
const relation = (s, a, b) => house(s, a)?.relations?.[b] || {};
const key = (a, b) => `${a}:${b}`;
const projected = s => !!(s.knowledgeView || s.projectionOnly);
const war = (s, a, b) => (s.wars || []).includes([a, b].sort().join(':'));
const pact = (s, a, b, type) => (s.treaties || []).some(t => t.type === type && t.expires > s.turn && t.parties.includes(a) && t.parties.includes(b));
const married = (s, a, b) => (s.royalBonds?.marriages || []).some(m => m.status === 'active' && m.parties.includes(a) && m.parties.includes(b));
const clone = x => structuredClone(x);
const escapeRE = x => x.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
const factsKey = facts => facts.map(f => f.id).sort().join('|');
const wrap = (reply, intents = [], tone = 'guarded') => ({ reply, intents, tone, speechAct: intents.length ? 'counteroffer' : 'statement' });
const isHuman = (s, id) => (s.controllers?.[id]?.kind || (id === 'ashen' ? 'human' : 'ai')) === 'human';

function trim(d) {
  d.facts = d.facts.slice(-LIMIT.facts);
  d.offers = d.offers.slice(-LIMIT.offers);
  d.receipts = d.receipts.slice(-LIMIT.receipts);
  d.inquiries = d.inquiries.slice(-LIMIT.inquiries);
}

/** Only explicit first-person hostility/proposals are classified. Questions,
 * quoted rumors, negated attacks and mere mentions never become a real plot. */
export function classifyCourtApproach(s, witness, speaker, message) {
  if (!house(s, witness) || !house(s, speaker) || witness === speaker || typeof message !== 'string') return [];
  const text = message.replace(/[’‘]/g, "'").trim().slice(0, 600);
  if (!text || /["“”]/.test(text) || /\b(?:heard|rumou?r|someone says|they said|claims? that|is it true|has anyone|did anyone|who is|are you|am i)\b/i.test(text)) return [];
  const results = [];
  // Classify each clause independently so a negation in one sentence does not
  // reverse a separate proposal. Pronoun-only subjects are not guessed.
  for (const clause of text.split(/[.!;\n]/)) {
    const targets = s.kingdoms.filter(h => ![witness, speaker].includes(h.id) &&
      [h.id, h.name?.replace(/^House\s+/i, '')].filter(Boolean).some(alias => new RegExp(`\\b${escapeRE(alias)}\\b`, 'i').test(clause)));
    if (targets.length !== 1) continue; // Ambiguous target: retain chat, not an accusation.
    const target = targets[0].id;
    const refusal = /\b(?:not|never|won't|wouldn't|don't|cannot|can't|will not|do not)\s+(?:(?:want|plan|intend|agree|wish)\s+to\s+)?(?:help\s+)?(?:attack|overthrow|invade|betray|destroy|remove|plot|march|move against|strike)/i.test(clause);
    const approach = !refusal && /\b(?:let's|let us|we should|we could|we must|i (?:want|plan|intend|propose)|help me|join me|will you help|would you help|can you help)\b[^.!;]{0,150}\b(?:attack|overthrow|invad\w*|betray|destroy|remove|plot|move against|march on|strike)\b/i.test(clause);
    const hostility = /\bi\s+(?:(?:do not|don't)\s+like|dislike|hate|distrust)\b/i.test(clause);
    if (approach || hostility) results.push({ witness, speaker, target, kind: approach ? 'military-approach' : 'hostility', text });
  }
  return [...new Map(results.map(f => [f.target, f])).values()];
}

function addFacts(s, d, witness, speaker, message, turn) {
  for (const fact of classifyCourtApproach(s, witness, speaker, message)) {
    if (d.facts.some(f => f.witness === witness && f.speaker === speaker && f.target === fact.target && f.turn === turn && f.text === fact.text)) continue;
    d.facts.push({ ...fact, id: `court-fact-${++d.sequence}`, turn });
  }
  trim(d);
}

/** Backfill only conversations actually retained in an older authoritative save.
 * This is not a claim that older, already-truncated history is complete. */
export function initializeCourtIntelligence(s) {
  if (projected(s)) return null;
  if (s.courtIntelligence) return s.courtIntelligence;
  const d = s.courtIntelligence = { version: 1, sequence: 0, facts: [], offers: [], receipts: [], inquiries: [] };
  const courts = s.controllers ? Object.entries(s.courts || {}) : [['ashen', { conversations: s.conversations || {} }]];
  for (const [speaker, court] of courts) {
    for (const [witness, history] of Object.entries(court?.conversations || {})) {
      if (!house(s, speaker) || !house(s, witness)) continue;
      for (const m of history) if (m.role === 'player' && !m.kind && Number.isInteger(m.turn) && m.turn >= 0 && m.turn <= s.turn)
        addFacts(s, d, witness, speaker, m.text, m.turn);
    }
  }
  return d;
}

export function recordCourtConversation(s, witness, speaker, message, { kind = '' } = {}) {
  const d = initializeCourtIntelligence(s);
  if (!d || !house(s, witness) || !house(s, speaker) || witness === speaker) return;
  if (!kind) addFacts(s, d, witness, speaker, message, s.turn);
  prepareCourtInquiry(s, witness, message, speaker);
}

export function isIntelligenceQuestion(message) {
  if (typeof message !== 'string') return false;
  const text = message.replace(/[’‘]/g, "'");
  if (/\b(?:marriage|marry|dowry|wedding)\b/i.test(text)) return false;
  const concern = /\b(?:plot\w*|overthrow\w*|conspir\w*|schem\w*|attack\w*|betray\w*|threats?)\b/i.test(text);
  const inquiry = /\b(?:who|anyone|anybody|someone|is there|are there|have you heard|has .*spoken|did .*speak|tell me|do you know|am i|against (?:me|us|my|our))\b/i.test(text) || /^\s*(?:is|are|has|have|did|does)\b.*\b(?:me|us|my|our)\b/i.test(text);
  return concern && inquiry || /\b(?:buy|purchase|pay for|sell me|share|disclose)\b.{0,70}\b(?:intelligence|information about threats|private approaches)\b/i.test(text);
}

function inquiryFor(s, witness, buyer) {
  return s.courtIntelligence?.inquiries?.find(q => q.witness === witness && q.buyer === buyer);
}
function inTopic(s, witness, message, buyer, proposal) {
  if (proposal?.type === INTELLIGENCE_INTENT || isIntelligenceQuestion(message)) return true;
  if (/\b(?:marriage|marry|wedding|dowry|trade|barter|exchange|shipment|loan)\b/i.test(message)) return false;
  const q = inquiryFor(s, witness, buyer);
  return !!q && q.active !== false && s.turn - q.turn <= 2 && /\b(?:pay|gold|coin|price|cost|tell me|who|information|intelligence|know|terms|agree|accept|yes|please|go ahead)\b/i.test(message);
}

function decision(s, witness, buyer, facts) {
  if (!facts.length) return 'unknown';
  if (war(s, witness, buyer)) return 'withheld';
  const host = house(s, witness), r = relation(s, witness, buyer);
  // Marriage and strong sworn loyalty can protect a confidence. Gold alone does
  // not override them. A very trusted target can still receive a warning when
  // there is no stronger protected obligation to the source.
  const protectedSource = facts.some(f => married(s, witness, f.speaker) ||
    pact(s, witness, f.speaker, 'alliance') && (host.honor >= .6 || (relation(s, witness, f.speaker).trust || 0) >= 55));
  if (protectedSource) return 'withheld';
  if (married(s, witness, buyer) || (r.trust >= 50 && (pact(s, witness, buyer, 'alliance') || r.opinion >= 45))) return 'disclosed';
  if (host.greed >= .6 && (r.opinion || 0) >= -20 && (r.trust || 0) >= 0 && !isHuman(s, witness)) return 'offered';
  return 'withheld';
}
function report(s, facts) {
  const lines = facts.slice(0, 3).map(f => f.kind === 'military-approach'
    ? `On turn ${f.turn}, ${name(s, f.speaker)} approached our court about action against ${name(s, f.target)}.`
    : `On turn ${f.turn}, ${name(s, f.speaker)} expressed hostility toward ${name(s, f.target)} in a private conversation with our court.`);
  return `${lines.join(' ')} These are first-hand reports of what was said, not proof of an accepted conspiracy, an army order, or a current attack. Other conversations may remain undisclosed.`.slice(0, 1500);
}
function offerIntent(o) {
  return { type: INTELLIGENCE_INTENT, targetId: o.id, giveResource: 'gold', giveAmount: o.price,
    receiveResource: 'food', receiveAmount: 0, duration: 2 };
}
function addReceipt(s, d, witness, buyer, facts, { offerId = null, paid = 0 } = {}) {
  const fingerprint = factsKey(facts);
  const old = d.receipts.find(r => r.witness === witness && r.buyer === buyer && r.fingerprint === fingerprint);
  if (old) return old;
  const receipt = { id: `court-receipt-${++d.sequence}`, witness, buyer, turn: s.turn, offerId, paid,
    fingerprint, report: report(s, facts) };
  d.receipts.push(receipt); trim(d); return receipt;
}

/** Called only when the controller accepts an actual player message. */
export function prepareCourtInquiry(s, witness, message, buyer) {
  if (projected(s) || !house(s, witness) || !house(s, buyer) || witness === buyer || isHuman(s, witness)) return;
  if (!inTopic(s, witness, String(message), buyer)) {
    const previous = inquiryFor(s, witness, buyer);
    if (previous) previous.active = false;
    return;
  }
  const d = initializeCourtIntelligence(s);
  const facts = d.facts.filter(f => f.witness === witness && f.target === buyer && f.speaker !== buyer).slice(-3).reverse();
  let status = decision(s, witness, buyer, facts), offerId = null, receiptId = null;
  const known = facts.length && d.receipts.find(r => r.witness === witness && r.buyer === buyer && r.fingerprint === factsKey(facts));
  if (known) { status = 'disclosed'; receiptId = known.id; }
  else if (status === 'disclosed') receiptId = addReceipt(s, d, witness, buyer, facts).id;
  else if (status === 'offered') {
    let offer = d.offers.find(o => o.witness === witness && o.buyer === buyer && o.status === 'open' && o.expires >= s.turn && factsKey(o.facts) === factsKey(facts));
    if (!offer) {
      const price = Math.max(20, Math.min(90, Math.round(25 + (house(s, witness).greed - .6) * 50 - Math.max(0, relation(s, witness, buyer).trust || 0) * .15)));
      offer = { id: `court-offer-${++d.sequence}`, witness, buyer, created: s.turn, expires: s.turn + 2, price, status: 'open', facts: clone(facts) };
      d.offers.push(offer);
    }
    offerId = offer.id;
  }
  d.inquiries = d.inquiries.filter(q => key(q.witness, q.buyer) !== key(witness, buyer));
  d.inquiries.push({ witness, buyer, turn: s.turn, status, offerId, receiptId, active: true }); trim(d);
}

/** This function returns ONLY approved text and public contract terms. Safe for
 * the current buyer's browser and for the external language-model request. */
export function intelligenceReply(s, witness, message, buyer, proposal = null) {
  if (!inTopic(s, witness, String(message), buyer, proposal)) return null;
  const d = s.courtIntelligence, q = inquiryFor(s, witness, buyer);
  if (isHuman(s, witness)) return null;
  if (!q) return wrap('Our court must review what it can responsibly disclose. A lack of a report is not assurance that no one has approached us. No payment is agreed.');
  if (q.status === 'disclosed') {
    const receipt = d.receipts.find(r => r.id === q.receiptId && r.buyer === buyer && r.witness === witness);
    if (receipt) return wrap(receipt.report, [], 'neutral');
  }
  if (q.status === 'offered') {
    const offer = d.offers.find(o => o.id === q.offerId && o.witness === witness && o.buyer === buyer && o.status === 'open' && o.expires >= s.turn);
    if (offer) return wrap(`I can offer a dated report of private approaches concerning your House for ${offer.price} gold. Review and ratify this specific report purchase; discussing a price or sending an ordinary gift does not buy it. This is not a promise to disclose every confidence.`, [offerIntent(offer)]);
  }
  return q.status === 'unknown'
    ? wrap('I have no confirmed report I can supply from the conversations available to this court. That is not a guarantee that no schemes or older approaches exist. I will not charge you for information I cannot provide.')
    : wrap('I will not disclose those private conversations. My refusal is not a denial that an approach was made, and I will not take your gold for an answer I am withholding.');
}

export function evaluateIntelligenceSale(s, witness, intent, buyer) {
  const reject = reason => ({ status: 'reject', reason, intent, factors: [] });
  if (projected(s)) return reject('The authoritative court must review this report purchase.');
  const o = s.courtIntelligence?.offers.find(o => o.id === intent.targetId && o.witness === witness && o.buyer === buyer);
  if (!o || o.status !== 'open' || o.expires < s.turn) return reject('This report offer is unavailable, already purchased, or expired. Ask the court for a fresh review.');
  if (isHuman(s, witness) || s.outcome || war(s, witness, buyer)) return reject('This court cannot complete that purchase now. No gold has changed hands.');
  if (Object.keys(offerIntent(o)).some(k => intent[k] !== offerIntent(o)[k]) || Object.keys(intent).some(k => !Object.hasOwn(offerIntent(o), k)))
    return reject('Only the exact quoted report and payment can be ratified. Ordinary gifts are not information purchases.');
  if (s.courtIntelligence.receipts.some(r => r.witness === witness && r.buyer === buyer && r.fingerprint === factsKey(o.facts)))
    return reject('This report has already been disclosed to your House. No second payment is needed.');
  if (decision(s, witness, buyer, o.facts) !== 'offered') return reject('The court’s disclosure conditions have changed. Ask again before spending gold.');
  if (!Number.isFinite(house(s, buyer)?.resources?.gold) || house(s, buyer).resources.gold < o.price) return reject('Your treasury cannot cover this report purchase.');
  return { status: 'accept', reason: `Ratification transfers ${o.price} gold once and immediately delivers the quoted, dated report.`, intent, factors: [] };
}
export function commitIntelligenceSale(s, witness, intent, buyer) {
  const v = evaluateIntelligenceSale(s, witness, intent, buyer);
  if (v.status !== 'accept') return { ok: false, error: v.reason };
  const d = s.courtIntelligence, o = d.offers.find(o => o.id === intent.targetId);
  const receipt = addReceipt(s, d, witness, buyer, o.facts, { offerId: o.id, paid: o.price });
  house(s, buyer).resources.gold -= o.price;
  house(s, witness).resources.gold += o.price;
  o.status = 'sold';
  const q = inquiryFor(s, witness, buyer);
  if (q) Object.assign(q, { status: 'disclosed', receiptId: receipt.id, offerId: null });
  return { ok: true, report: receipt.report, receiptId: receipt.id };
}

/** Do not spread the private ledger. Even the offer's fact IDs/fingerprint could
 * leak a source. Only the buyer's approved replies, terms and receipts survive. */
export function projectCourtIntelligence(s, viewer) {
  const d = s.courtIntelligence;
  if (!d || !house(s, viewer)) return { version: 1, offers: [], receipts: [], inquiries: [] };
  return {
    version: 1,
    offers: d.offers.filter(o => o.buyer === viewer).map(({ id, witness, buyer, created, expires, price, status }) => ({ id, witness, buyer, created, expires, price, status })),
    receipts: d.receipts.filter(r => r.buyer === viewer).map(({ id, witness, buyer, turn, paid, report }) => ({ id, witness, buyer, turn, paid, report })),
    inquiries: d.inquiries.filter(q => q.buyer === viewer).map(q => clone(q))
  };
}

export function validateCourtIntelligence(s) {
  if (s.courtIntelligence === undefined) { initializeCourtIntelligence(s); return; }
  const d = s.courtIntelligence, ids = new Set(s.kingdoms.map(h => h.id));
  const fail = () => { throw new Error('Damaged private court intelligence.'); };
  const object = x => x && typeof x === 'object' && !Array.isArray(x);
  const turn = n => Number.isInteger(n) && n >= 0 && n <= s.turn;
  const string = (x, max) => typeof x === 'string' && x.length > 0 && x.length <= max;
  const parties = x => ids.has(x.witness) && ids.has(x.buyer) && x.witness !== x.buyer;
  if (!object(d) || d.version !== 1 || !Number.isSafeInteger(d.sequence) || d.sequence < 0 || d.sequence > 1e9) fail();
  for (const [k, max] of Object.entries(LIMIT)) if (!Array.isArray(d[k]) || d[k].length > max) fail();
  const all = new Set();
  const id = (value, prefix) => { const m = typeof value === 'string' && value.match(new RegExp(`^${prefix}([1-9][0-9]*)$`)); if (!m || Number(m[1]) > d.sequence || all.has(value)) fail(); all.add(value); };
  const fact = f => object(f) && string(f.id, 60) && ids.has(f.witness) && ids.has(f.speaker) && ids.has(f.target) && new Set([f.witness, f.speaker, f.target]).size === 3 && turn(f.turn) && ['hostility', 'military-approach'].includes(f.kind) && string(f.text, 600);
  for (const f of d.facts) { if (!fact(f)) fail(); id(f.id, 'court-fact-'); }
  for (const o of d.offers) {
    if (!object(o) || !parties(o) || !turn(o.created) || !Number.isInteger(o.expires) || o.expires !== o.created + 2 || !Number.isInteger(o.price) || o.price < 20 || o.price > 90 || !['open', 'sold'].includes(o.status) || !Array.isArray(o.facts) || !o.facts.length || o.facts.length > 3 || o.facts.some(f => !fact(f) || f.witness !== o.witness || f.target !== o.buyer)) fail();
    id(o.id, 'court-offer-');
  }
  for (const r of d.receipts) {
    if (!object(r) || !parties(r) || !turn(r.turn) || !Number.isInteger(r.paid) || r.paid < 0 || r.paid > 90 || !string(r.fingerprint, 200) || !string(r.report, 1500) || !(r.offerId === null || string(r.offerId, 60))) fail();
    id(r.id, 'court-receipt-');
  }
  const pairs = new Set();
  for (const q of d.inquiries) {
    if (!object(q) || (q.active !== undefined && typeof q.active !== 'boolean') || !parties(q) || !turn(q.turn) || !['unknown', 'withheld', 'offered', 'disclosed'].includes(q.status) || !(q.offerId === null || string(q.offerId, 60)) || !(q.receiptId === null || string(q.receiptId, 60)) || pairs.has(key(q.witness, q.buyer))) fail();
    pairs.add(key(q.witness, q.buyer));
  }
}
