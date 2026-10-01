import { tradeItems, itemTotal, normalizePackage, withItems, scalePackage } from './trade-package.mjs';
// Economic decisions and conversational influence belong to the simulation.
// Model text/signals are never accepted as relationship deltas or proof of deeds.
import { RESOURCES, RESOURCE_VALUES } from './data.mjs';
import { PLAYER, atWar, kingdom, relation } from './core.mjs';
import { planningView } from './ai-knowledge.mjs';
import { court, isHumanHouse } from './house-control.mjs';
import { appendConversation, borderThreat, changeRelation, economicRelationship, tradeBlocked } from './living.mjs';
import { economicNeeds, packageValue, tradeRoute } from './trade-economy.mjs';

export const COMMERCIAL_TYPES = new Set(['TRADE', 'EXCHANGE', 'RECURRING']);
const VALUES = RESOURCE_VALUES;
const ARGUMENTS = ['mutual_interest', 'relevant_supply', 'limited_trial', 'kept_word', 'verified_withdrawal', 'acknowledge_grievance'];
const POINTS = { mutual_interest: 4, relevant_supply: 5, limited_trial: 6, kept_word: 6, verified_withdrawal: 6, acknowledge_grievance: 2 };
const REASONS = {
  mutual_interest: 'Discussed the interests of both Houses rather than demanding a concession.',
  relevant_supply: 'Offered to discuss a resource our House needs; no shipment has been credited.',
  limited_trial: 'Addressed the risk of commitment by proposing a small initial exchange.',
  kept_word: 'Recalled a promise that the ledger confirms was fulfilled.',
  verified_withdrawal: 'Addressed frontier concerns after a verified withdrawal.',
  acknowledge_grievance: 'Acknowledged a real grievance; reconciliation still requires deeds.'
};
const normalized = text => String(text || '').toLowerCase().replace(/[’]/g, "'");
const serious = r => r.trust < -30 || (r.grievance || 0) > 40;
const object = value => value && typeof value === 'object' && !Array.isArray(value);
const aliases = { grain: 'food', provisions: 'food', timber: 'wood', lumber: 'wood', coins: 'gold', weapons: 'arms' };
const resource = word => aliases[word] || word;
const RESOURCE_WORDS = [...RESOURCES, ...Object.keys(aliases)].join('|');
const mentionedResources = text => [...new Set([...text.matchAll(new RegExp(`\\b(${RESOURCE_WORDS})\\b`, 'g'))].map(m => resource(m[1])))];
const activeHistory = (s, ruler, actor) => (court(s, actor).conversations[ruler] || []).filter(m => Number.isInteger(m.turn) && m.turn >= s.turn - 2 && m.turn <= s.turn && m.role === 'player');
const explicitTrade = text => /\b(?:trad(?:e|ing)|commerce|merchants?|barter|exchange|suppl(?:y|ies)|granaries|grain)\b/.test(text);
const needsQuestion = text => /\b(?:what|which|anything)\b.{0,65}\b(?:need|seek|lack|short|interest)\b|\bhow (?:can|could) (?:i|we) help\b/.test(text);
const refusesTrade = text => /\b(?:won't|will not|do not|don't|refuse to|not interested in)\s+(?:offer|send|supply|give|trade|exchange|barter|help|cooperate)\b/.test(text);

export function isTradeDiscussion(s, ruler, message, actor = PLAYER) {
  const text = normalized(message);
  if (refusesTrade(text)) return false;
  if (explicitTrade(text) || needsQuestion(text) && /\b(?:food|wood|iron|gold|resources?|neighbou?rs?|relationship|help)\b/.test(text)) return true;
  return /\b(?:smaller|modest|trial|risk|commitment|terms|price|expensive|concern|comfortable|fair|promise|word|withdraw|sorry|apologi[sz]e)\b/.test(text)
    && activeHistory(s, ruler, actor).some(m => explicitTrade(normalized(m.text)));
}

// Only a clear, two-sided, present-tense offer is a resource exchange. "I have
// 100 food" is not a payment, and a dated/conditional promise stays an oath.
export function parseBarter(message) {
  const text = normalized(message);
  if (refusesTrade(text) || /\b(?:if|might|maybe|perhaps|next turn|by turn|within)\b/.test(text)) return null;
  const match=text.match(/\b(?:i|we)\s+(?:(?:can|will|would)\s+)?(?:offer|give|trade|exchange|send)\s+(?:you\s+)?(.+?)\s+(?:in (?:exchange|return) )?for\s+(.+?)(?=\s+(?:(?:each|per|every) turn|for \d+ turns)|[.!?]|$)/);
  if(!match)return null;
  const parse=part=>{
    const rows=[...part.matchAll(new RegExp(`(\\d{1,4})\\s+(${RESOURCE_WORDS})\\b`,'g'))];
    const residue=part.replace(new RegExp(`\\d{1,4}\\s+(?:${RESOURCE_WORDS})\\b`,'g'),'').replace(/(?:\band\b|[,+&])/g,'').trim();
    return rows.length&&!residue?rows.map(m=>({resource:resource(m[2]),amount:Number(m[1])})):null;
  };
  const giveItems=parse(match[1]),receiveItems=parse(match[2]);if(!giveItems||!receiveItems)return null;
  const recurring=/\b(?:each|per|every) turn\b/.test(text),duration=recurring?Number(text.match(/\bfor (\d{1,2}) turns?\b/)?.[1]):2;
  if(duration<2||duration>20||!duration)return null;
  return normalizePackage({type:recurring?'RECURRING':'EXCHANGE',giveItems,receiveItems,duration,targetId:'',tradeKind:recurring?'recurring':'immediate'});

}

export function parseCommercialOffer(message) {
  const barter = parseBarter(message);
  if (barter) return barter;
  const text = normalized(message);
  if (refusesTrade(text) || /\b(?:if|might|maybe|perhaps|why|what|which|cost)\b/.test(text)
    || !/\b(?:trade (?:agreement|charter)|road trade)\b/.test(text)
    || !/\b(?:agree|sign|establish|propose|request|consider|let's|let us|open)\b/.test(text)) return null;
  const duration = Number(text.match(/\b(\d{1,2}) turns?\b/)?.[1] || 10);
  const payment = text.match(new RegExp(`\\b(?:i|we) (?:offer|give|pay) (\\d{1,4}) (${RESOURCE_WORDS})\\b`));
  if (duration < 2 || duration > 20 || payment && Number(payment[1]) > 1000) return null;
  return { type: 'TRADE', duration, giveResource: payment ? resource(payment[2]) : 'gold', giveAmount: payment ? Number(payment[1]) : 0, receiveResource: 'food', receiveAmount: 0, targetId: '' };
}

function facts(s, ruler, actor) {
  const k = kingdom(s, ruler), r = relation(s, ruler, actor);
  const view = planningView(s, ruler);
  return { k, r, needs: economicNeeds(s, ruler), military: borderThreat(view, ruler, actor),
    economy: economicRelationship(s, ruler, actor), route: tradeRoute(view, ruler, actor) };
}

// Deliberate voluntary disclosures, not a foreign treasury dump. A projected
// client never reinterprets redacted zeroes as a shortage. Multiplayer publishes
// the same dated, coarse briefing from the authoritative simulation.
export function tradeBriefing(s, ruler, actor = PLAYER) {
  if (s.knowledgeView) {
    const brief = s.tradeBriefings?.[ruler];
    return brief?.turn === s.turn && brief.actor === actor ? structuredClone(brief) : null;
  }
  if (!kingdom(s, ruler) || ruler === actor || s.controllers && isHumanHouse(s, ruler)) return null;
  const f = facts(s, ruler, actor), { r, military, route } = f;
  const unavailable = atWar(s, ruler, actor) ? 'war' : tradeBlocked(s, ruler, actor) ? 'embargo' : '';
  return {
    turn: s.turn, actor, imports: f.needs.filter(n => n.need > 5).sort((a, b) => b.need - a.need).slice(0, 3).map(n => n.resource),
    exports: f.needs.filter(n => n.surplus >= 12).sort((a, b) => b.surplus - a.surplus).map(n => n.resource),
    concern: unavailable || (military.score >= 20 ? 'border_security' : serious(r) ? 'broken_confidence' : r.trust < 20 ? 'untested_commitment' : 'fair_exchange'),
    roadTrade: route.safe && /road|highway/i.test(route.status) ? 'connected' : route.safe ? 'prospective' : 'unconfirmed',
    preference: f.k.greed > .8 ? 'material_benefit' : f.k.paranoia > .6 ? 'limited_commitment' : f.k.honor > .8 ? 'reliable_partner' : 'mutual_benefit',
    separateCharterRequiredForExchange: false
  };
}

function fulfilledPledge(s, ruler, actor, type) {
  return (s.pledges || []).filter(p => p.debtor === actor && p.creditor === ruler && p.status === 'fulfilled' && (!type || p.intent.type === type)).at(-1);
}
function speechAssessment(s, ruler, message, actor) {
  if (!isTradeDiscussion(s, ruler, message, actor)) return [];
  const text = normalized(message), r = relation(s, ruler, actor), out = [];
  const offer = /\b(?:i|we)\b.{0,35}\b(?:have|offer|provide|supply|send|trade)\b/.test(text);
  if (needsQuestion(text) && /\b(?:neighbou?rs?|relationship|both|mutual|together|cooperat\w*|help|offer|have)\b/.test(text) || /\b(?:both|mutual|together|each other|our (?:people|realms|houses))\b/.test(text) && /\b(?:benefit|prosper|help|cooperat\w*|gain|peace|trade)\b/.test(text)) out.push({ kind: 'mutual_interest', key: 'first-interest' });
  if (offer && !refusesTrade(text)) {
    const needs = economicNeeds(s, ruler), match = mentionedResources(text).find(id => needs.some(n => n.resource === id && n.need > 5));
    if (match) out.push({ kind: 'relevant_supply', key: match });
  }
  if (/\b(?:small|smaller|modest|limited|trial|one[- ]time|single)\b/.test(text) && /\b(?:exchange|trade|shipment|commitment|start|begin|risk|first)\b/.test(text)) out.push({ kind: 'limited_trial', key: 'first-trial' });
  if (/\b(?:kept|honou?red|fulfilled|delivered)\b/.test(text) && /\b(?:word|promise|agreement|pledge|shipment)\b/.test(text)) {
    const pledge = fulfilledPledge(s, ruler, actor);
    if (pledge) out.push({ kind: 'kept_word', key: pledge.id });
  }
  if (/\b(?:withdrew|withdrawn|moved (?:my|our|the) (?:troops|army|soldiers) (?:back|away))\b/.test(text)) {
    const pledge = fulfilledPledge(s, ruler, actor, 'PLEDGE_WITHDRAW');
    if (pledge && borderThreat(s, ruler, actor).score < 20) out.push({ kind: 'verified_withdrawal', key: pledge.id });
  }
  if ((r.grievance || 0) > 0 && /\b(?:sorry|apologi[sz]e|understand|acknowledge)\b/.test(text) && /\b(?:broke|broken|betray\w*|failed|grievance|hurt|wrong)\b/.test(text)) out.push({ kind: 'acknowledge_grievance', key: 'acknowledgment' });
  return out;
}

export function applyTradeSpeech(s, ruler, message, actor = PLAYER, hostile = false) {
  // Only paid/authorized chat handlers call this, never render, model validation,
  // inspection, or ratification. New save fields are optional for older campaigns.
  if (s.knowledgeView || s.outcome || s.controllers && isHumanHouse(s, ruler)) return;
  const r = relation(s, ruler, actor);
  if (hostile) { for (const entry of Object.values(r.negotiation?.arguments || {})) entry.revoked = true; return; }
  if (atWar(s, ruler, actor) || tradeBlocked(s, ruler, actor)) return;
  const assessed = speechAssessment(s, ruler, message, actor);
  if (!assessed.length) return;
  const n = r.negotiation ||= { arguments: {}, opinionGain: 0, trustGain: 0 };
  const seenSupplies = n.seenSupplies || (n.arguments.relevant_supply ? [n.arguments.relevant_supply.key] : []);
  const fresh = assessed.filter(a => n.arguments[a.kind]?.key !== a.key && (a.kind !== 'relevant_supply' || !seenSupplies.includes(a.key)));
  if (!fresh.length) return;
  // Two substantive observations per message; repetition changes neither the
  // timestamp nor influence. Lifetime social gains cannot be reset by waiting.
  const chosen = fresh.slice(0, 2), threatened = borderThreat(s, ruler, actor).score >= 20;
  let opinion = 0, trust = 0;
  for (const a of chosen) {
    n.arguments[a.kind] = { key: a.key, turn: s.turn };
    if (a.kind === 'relevant_supply') n.seenSupplies = [...seenSupplies, a.key];
    if (!threatened && (!serious(r) || a.kind === 'acknowledge_grievance')) opinion++;
    if (!threatened && !serious(r) && ['limited_trial', 'kept_word', 'verified_withdrawal'].includes(a.kind)) trust++;
  }
  opinion = Math.min(opinion, 6 - n.opinionGain); trust = Math.min(trust, 3 - n.trustGain);
  n.opinionGain += opinion; n.trustGain += trust;
  const explanation = REASONS[chosen.at(-1).kind];
  changeRelation(s, ruler, actor, { opinion, trust }, explanation);
  appendConversation(s, ruler, 'council', `Trade discussion${trust ? ` · TRUST +${trust}` : ''}${opinion ? ` · OPINION +${opinion}` : ''} — ${explanation}`, { actorHouseId: actor, kind: 'negotiation' });
}

function influence(s, ruler, actor, i, f) {
  let points = 0; const reasons = [];
  for (const [kind, evidence] of Object.entries(f.r.negotiation?.arguments || {})) {
    if (!ARGUMENTS.includes(kind) || evidence.revoked || s.turn - evidence.turn > 2 || evidence.turn > s.turn) continue;
    if (serious(f.r) && kind !== 'acknowledge_grievance') continue;
    if (f.military.score >= 20) continue;
    if (kind === 'relevant_supply' && (!tradeItems(i,'give').some(x=>x.resource===evidence.key) || !f.needs.some(n => n.resource === evidence.key && n.need > 5))) continue;
    if (kind === 'limited_trial' && !(i.type === 'TRADE' && i.duration <= 3 || i.type === 'EXCHANGE' && Math.max(itemTotal(tradeItems(i,'give')),itemTotal(tradeItems(i,'receive'))) <= 20 || i.type === 'RECURRING' && i.duration <= 3 && Math.max(itemTotal(tradeItems(i,'give')),itemTotal(tradeItems(i,'receive'))) <= 12)) continue;
    if (kind === 'kept_word' && !s.pledges.some(p => p.id === evidence.key && p.debtor === actor && p.creditor === ruler && p.status === 'fulfilled')) continue;
    if (kind === 'verified_withdrawal' && !s.pledges.some(p => p.id === evidence.key && p.debtor === actor && p.creditor === ruler && p.status === 'fulfilled' && p.intent.type === 'PLEDGE_WITHDRAW')) continue;
    points += POINTS[kind]; reasons.push(REASONS[kind]);
  }
  return { points: Math.min(10, points), reasons: reasons.slice(0, 2) };
}

function assessment(s, ruler, actor, i, f) {
  const { k, r, military, economy, route } = f;
  const discussion = influence(s, ruler, actor, i, f);
  const weight = id => k.resources[id] < 40 ? 1.8 : k.resources[id] < 80 ? 1.2 : 1;
  const given = i.giveAmount * VALUES[i.giveResource], received = i.receiveAmount * VALUES[i.receiveResource];
  const material = given * weight(i.giveResource) - received * weight(i.receiveResource);
  const factors = [...discussion.reasons];
  const deny = (code, reason) => ({ accepted: false, code, reason, factors });
  if (i.type === 'TRADE') {
    if (military.score >= 20) return deny('border_security', 'Our concern is the army near our frontier, not an entrance payment. Address that threat before seeking a lasting trade charter.');
    if (serious(r)) return deny('broken_confidence', 'Past breaches make a lasting commitment unacceptable. A small, fair exchange can precede any new charter; gold alone will not restore confidence.');
    if (!route.safe) return deny('route', 'Our merchants cannot yet confirm a usable connection. Establish a route before asking us to commit; payment does not create a road.');
    const connected = /road|highway/i.test(route.status);
    const benefit = connected ? 26 : 10;
    const threshold = 24 + Math.max(0, i.duration - 3) * (.35 + k.paranoia * .5) + Math.max(0, k.greed - .75) * 20;
    const utility = material * (.7 + k.greed * .35) + r.opinion * .4 + r.trust * (.4 + k.honor * .6)
      + economy.dependency * .2 + ((r.reliability ?? 50) - 50) * k.honor * .2 - (r.grievance || 0) * .4
      + benefit + discussion.points + Math.min(8, military.sharedEnemies.length * 4);
    factors.push(connected ? 'A connected road offers our merchants a mutual commercial benefit.' : 'We see scope for commerce, but a charter alone creates no road income.');
    if (utility >= threshold) return { accepted: true, code: 'mutual_benefit', reason: `${factors.join(' ')} ${i.giveAmount ? 'These terms are acceptable.' : 'No upfront payment is needed for this charter.'} Ratification makes the agreement binding.`, factors };
    return deny('commitment', 'The duration and risk of this commitment exceed our present confidence. A shorter charter or a useful initial exchange would be more convincing.');
  }
  if (i.type === 'RECURRING' && (military.score >= 20 || serious(r) || r.trust < 5 && i.duration > 3)) return deny('commitment', 'We are not ready to rely on repeated deliveries under these conditions. Begin with a small immediate exchange rather than paying to bypass the concern.');
  const give=tradeItems(i,'give'),receive=tradeItems(i,'receive');
  const duration=i.type==='RECURRING'?i.duration:1;
  for(const x of receive){
    const n=f.needs.find(n=>n.resource===x.resource);
    const required=x.amount*duration;
    const available=i.type==='RECURRING'?n.surplus+Math.max(0,n.production)*Math.max(0,duration-1):n.surplus;
    if(required>available)return deny('protected_reserve',`We need our ${x.resource} for reserves, committed deliveries and planned spending; only ${Math.max(0,Math.floor(available/duration))} per shipment is available.`);
  }
  const packageGiven=packageValue(f.needs,give),packageReceived=packageValue(f.needs,receive);
  const premiumRate=Math.max(0,k.greed-.65)*.25+(i.type==='RECURRING'&&r.trust<20?.03+k.paranoia*.03:0);
  const concession=Math.min(premiumRate,discussion.points*.008+Math.max(0,r.trust-15)*.001);
  const fair=packageGiven+1e-8>=packageReceived*(1+premiumRate-concession);
  const relevant=give.filter(x=>f.needs.find(n=>n.resource===x.resource).need>0);
  if(relevant.length)factors.push(`The offered ${relevant.map(x=>x.resource).join(' and ')} addresses our projected needs.`);
  if(fair)return {accepted:true,code:'fair_exchange',reason:`${factors.join(' ')} The full exchange serves our resource needs and requires no separate initiation fee. Ratify the exact package before delivery.`.trim(),factors};
  return deny('material_balance','The full package is worth less to us than the supplies requested. Add resources we need or request a smaller shipment.');

}

function sizedExchange(i, cap=12){return {...scalePackage(i,Math.min(1,cap/Math.max(itemTotal(tradeItems(i,'give')),itemTotal(tradeItems(i,'receive'))))),type:'EXCHANGE',tradeKind:'immediate',duration:2};}

function packageCounters(s,ruler,i,actor,judge,f,result){
  const candidates=[],give=tradeItems(i,'give'),receive=tradeItems(i,'receive');
  if(i.type==='RECURRING')candidates.push(sizedExchange(i));
  const actorNeeds=economicNeeds(s,actor);
  const bases=[i];
  for(const scale of [.9,.75,.5,.25,.1]){
    const rows=receive.map(x=>({...x,amount:Math.min(Math.max(1,Math.floor(x.amount*scale)),Math.floor(f.needs.find(n=>n.resource===x.resource).surplus/(i.type==='RECURRING'?i.duration:1)))}));
    if(rows.every(x=>x.amount>0))bases.push(withItems(i,give,rows));
  }
  for(const base of bases){
    candidates.push(base);
    const outgoing=tradeItems(base,'receive');
    const additions=f.needs.filter(n=>!outgoing.some(x=>x.resource===n.resource)).sort((a,b)=>b.need-a.need||b.value-a.value);
    for(const n of additions){
      const existing=give.find(x=>x.resource===n.resource)?.amount||0;
      const available=Math.min(1000,kingdom(s,actor).resources[n.resource],Math.floor(actorNeeds.find(x=>x.resource===n.resource).surplus));
      if(available<=existing)continue;
      const target=packageValue(f.needs,outgoing)*(1+Math.max(0,f.k.greed-.65)*.25);
      const extra=Math.max(1,Math.ceil((target-packageValue(f.needs,give))/n.value));
      if(existing+extra>available)continue;
      const rows=give.filter(x=>x.resource!==n.resource).concat({resource:n.resource,amount:existing+extra});
      candidates.push(withItems(base,rows,outgoing));
    }
  }
  for(const candidate of candidates){
    if(!assessment(s,ruler,actor,candidate,f).accepted)continue;
    const verdict=judge(candidate);
    if(verdict.status==='accept')return {status:'counter',reason:`${result.reason} These revised resource quantities address our needs while protecting our reserves.`,intent:i,counter:verdict.intent,factors:result.factors,reasonCode:result.code};
  }
  return {status:'reject',reason:result.reason,intent:i,factors:result.factors,reasonCode:result.code};
}

// The final candidate still passes evaluateDeal's full affordability, route,
// embargo, capacity, and human-consent checks. Never advertise an illegal price.
export function evaluateCommercial(s, ruler, i, actor, judge, allowCounter = true) {
  const f = facts(s, ruler, actor), result = assessment(s, ruler, actor, i, f);
  const response = { status: result.accepted ? 'accept' : 'reject', reason: result.reason, intent: i, factors: result.factors, reasonCode: result.code };
  if (result.accepted || !allowCounter) return response;
  if(['EXCHANGE','RECURRING'].includes(i.type))return packageCounters(s,ruler,i,actor,judge,f,result);
  const candidates = [], seen = new Set();
  const add = (candidate, explanation) => {
    const key = JSON.stringify(candidate);
    if (!seen.has(key)) { seen.add(key); candidates.push({ candidate, explanation }); }
  };
  const balanced = base => {
    // Binary-search only the pure score using the same read-only facts. One
    // complete rules check follows, instead of repeatedly cloning the world.
    let low = 1, high = 1000;
    if (!assessment(s, ruler, actor, {...base,giveItems:undefined,giveAmount:high}, f).accepted) return null;
    while (low < high) { const mid = Math.floor((low + high) / 2); if (assessment(s, ruler, actor, { ...base, giveAmount: mid }, f).accepted) high = mid; else low = mid + 1; }
    return { ...base, giveAmount: low };
  };
  const needs = f.needs.filter(n => n.need > 5).sort((a, b) => b.need - a.need);
  if (i.type === 'TRADE') {
    if (i.duration > 3) add({ ...i, duration: 3, giveAmount: 0 }, 'A short charter limits the commitment; no upfront payment is required.');
    // Security and major breaches cannot be bought away. Material concessions
    // are considered only for an actual domestic need, not because gold is the
    // default resource in an otherwise empty form.
    if (result.code === 'commitment') for (const need of needs.slice(0, 3)) {
      const candidate = balanced({ ...i, giveResource: need.resource, giveAmount: 0 });
      if (candidate) add(candidate, `Supplies of ${need.resource} would address our current needs while we undertake this charter.`);
    }
    const exports = f.needs.filter(n => n.surplus >= 12);
    for (const need of needs.slice(0, 2)) {
      const surplus = exports.find(n => n.resource !== need.resource); if (!surplus) continue;
      const candidate = balanced({ type: 'EXCHANGE', giveResource: need.resource, giveAmount: 1, receiveResource: surplus.resource, receiveAmount: 6, duration: 2, targetId: '', tradeKind: 'immediate' });
      if (candidate && candidate.giveAmount <= 20) add(candidate, `Let us first exchange ${need.resource} for ${surplus.resource} on a small scale. This is an exchange, not a paid charter.`);
    }
  }
  for (const { candidate, explanation } of candidates.slice(0, 9)) {
    if (!assessment(s, ruler, actor, candidate, f).accepted) continue;
    const verdict = judge(candidate);
    if (verdict.status === 'accept') return { ...response, status: 'counter', counter: verdict.intent, reason: `${result.reason} ${explanation}`, factors: [...result.factors, explanation].slice(0, 3) };
  }
  return response;
}

export function explorationReply(brief) {
  if (!brief) return 'Tell me which resources you would offer and what you seek in return. Our court must review availability before promising a shipment.';
  if (brief.concern === 'war') return 'There can be no safe exchange while we are at war. Let us discuss peace first.';
  if (brief.concern === 'embargo') return 'An active embargo prevents commerce between our Houses. It must end before we can exchange supplies.';
  const supply = brief.imports.length ? `We would consider imports of ${brief.imports.join(', ')}.` : 'We have no pressing shortage to address at present.';
  const exports = brief.exports.length ? ` We could discuss ${brief.exports.join(', ')} in return, subject to quantities.` : ' I will not promise exports before reviewing the quantities.';
  const concern = brief.concern === 'border_security' ? ' A lasting charter must wait until our frontier concerns are addressed.'
    : brief.concern === 'broken_confidence' ? ' Our past grievances remain. A modest immediate exchange is not a promise of lasting trust.'
    : brief.concern === 'untested_commitment' ? ' Let us begin with a modest exchange before relying on a long commitment.'
    : ' An exchange that serves both Houses needs no separate initiation payment.';
  return `${supply}${exports}${concern} Put the exchange terms before me when you are ready.`;
}

export function validateNegotiation(value, turn) {
  if (value === undefined) return true;
  const integer = (v, max) => Number.isInteger(v) && v >= 0 && v <= max;
  return object(value) && Object.keys(value).every(k => ['arguments', 'opinionGain', 'trustGain', 'seenSupplies'].includes(k))
    && (value.seenSupplies === undefined || Array.isArray(value.seenSupplies) && value.seenSupplies.length <= RESOURCES.length && new Set(value.seenSupplies).size === value.seenSupplies.length && value.seenSupplies.every(r => RESOURCES.includes(r)))
    && integer(value.opinionGain, 6) && integer(value.trustGain, 3) && object(value.arguments)
    && Object.keys(value.arguments).length <= ARGUMENTS.length && Object.entries(value.arguments).every(([kind, entry]) => ARGUMENTS.includes(kind)
      && object(entry) && Object.keys(entry).every(k => ['key', 'turn', 'revoked'].includes(k)) && (entry.revoked === undefined || typeof entry.revoked === 'boolean') && integer(entry.turn, turn)
      && typeof entry.key === 'string' && entry.key.length <= 80 && (kind === 'relevant_supply' ? RESOURCES.includes(entry.key)
        : ['kept_word', 'verified_withdrawal'].includes(kind) ? /^pledge-\d+$/.test(entry.key)
          : entry.key === ({ mutual_interest: 'first-interest', limited_trial: 'first-trial', acknowledge_grievance: 'acknowledgment' })[kind]));
}
