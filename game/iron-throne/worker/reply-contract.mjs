import { CAMPAIGN_HOUSES, INTENT_TYPES, RESOURCES } from '../data.mjs';
import { validateIntent, validateResponse } from '../diplomacy.mjs';
import { MARRIAGE_FIELDS } from '../marriage.mjs';
import { PLAYER_PROMISES } from '../promises.mjs';

const text = maxLength => ({ type: 'STRING', maxLength });
const amount = { type: 'INTEGER', minimum: 0, maximum: 1000 };
const common = {
  targetId: text(60), giveResource: { type: 'STRING', enum: RESOURCES }, giveAmount: amount,
  receiveResource: { type: 'STRING', enum: RESOURCES }, receiveAmount: amount
};
const tradeKind = { type: 'STRING', enum: ['immediate', 'recurring', 'purchase', 'strategic', 'emergency', 'preferential'] };
const itemList = { type: 'ARRAY', minItems: 1, maxItems: 8, items: {
  type: 'OBJECT', required: ['resource', 'amount'], properties: {
    resource: { type: 'STRING', enum: RESOURCES }, amount: { ...amount, minimum: 1 }
  }
} };
// Keep the provider grammar flat and explicitly typed. Repeating a union of
// every intent under intents/proposal/counterProposal/promiseDetected makes the
// generation schema much larger. Prompts and the unchanged engine validator
// enforce which optional terms belong to each intent and its minimum duration.
export const INTENT_SCHEMA = { type: 'OBJECT', required: ['type'], properties: {
  type: { type: 'STRING', enum: INTENT_TYPES }, ...common,
  duration: { type: 'INTEGER', minimum: 1, maximum: 20 }, tradeKind,
  conditionHouseId: { type: 'STRING', enum: CAMPAIGN_HOUSES.map(h => h.id) },
  giveItems: itemList, receiveItems: itemList,
  actorMember: { type: 'STRING', enum: ['ruler', 'daughter', 'son'] }, rulerMember: { type: 'STRING', enum: ['ruler', 'daughter', 'son'] },
  shipmentResource: { type: 'STRING', enum: ['gold', 'food', 'iron', 'horses'] },
  shipmentAmount: { type: 'INTEGER', minimum: 0, maximum: 100 }, shipmentTurns: { type: 'INTEGER', minimum: 0, maximum: 20 },
  defense: { type: 'BOOLEAN' }, trade: { type: 'BOOLEAN' }
} };
export const RESPONSE_SCHEMA = { type: 'OBJECT', required: ['reply', 'intents', 'tone'], properties: {
  reply: { ...text(1600), minLength: 1, description: 'Concise in-character reply. Terms await council validation and player ratification.' },
  tone: { type: 'STRING', enum: ['warm', 'neutral', 'cold', 'hostile', 'guarded'] },
  intents: { type: 'ARRAY', maxItems: 3, items: INTENT_SCHEMA },
  proposal: { ...INTENT_SCHEMA, nullable: true }, counterProposal: { ...INTENT_SCHEMA, nullable: true },
  promiseDetected: { ...INTENT_SCHEMA, nullable: true, properties: { ...INTENT_SCHEMA.properties, type: { type: 'STRING', enum: [...PLAYER_PROMISES] } } },
  relationshipSummary: { ...text(360), nullable: true, description: 'Conversation interpretation; never rewrite verified history.' },
  speechAct: { type: 'STRING', nullable: true, enum: ['statement', 'question', 'accept', 'reject', 'counteroffer', 'promise', 'warning', 'gratitude'] },
  relationshipSignals: { type: 'ARRAY', nullable: true, maxItems: 4, items: text(180) },
  memoryCandidates: { type: 'ARRAY', nullable: true, maxItems: 4, items: { ...text(180), description: 'Short interpretation; do not invent past actions.' } }
} };

const optionalIntentFields = [...Object.keys(common), 'duration', 'tradeKind', 'conditionHouseId', ...MARRIAGE_FIELDS, 'giveItems', 'receiveItems'];
export function normalizeModelIntent(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return value;
  const intent = { ...value };
  // A null optional field states no term. Do not coerce numbers, invent terms,
  // or remove incompatible nonempty fields. Client/menu validators stay strict.
  for (const key of optionalIntentFields) if (intent[key] === null) delete intent[key];
  if (!['EXCHANGE', 'RECURRING'].includes(intent.type))
    for (const key of ['giveItems', 'receiveItems']) if (Array.isArray(intent[key]) && !intent[key].length) delete intent[key];
  return intent;
}
export function validateModelResponse(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return { response: null, replyIssue: 'invalid_schema' };
  const out = { ...raw };
  if (Array.isArray(out.intents)) out.intents = out.intents.map(normalizeModelIntent);
  for (const key of ['proposal', 'counterProposal', 'promiseDetected']) if (out[key] != null) out[key] = normalizeModelIntent(out[key]);
  for (const key of ['speechAct', 'relationshipSummary', 'relationshipSignals', 'memoryCandidates']) if (out[key] === null) delete out[key];
  const response = validateResponse(out);
  if (response) return { response };
  const replyIssue = typeof out.reply !== 'string' || !out.reply.trim() || out.reply.length > 1600 ? 'invalid_reply'
    : !Array.isArray(out.intents) || out.intents.length > 3 || out.intents.some(i => !validateIntent(i)) ? 'invalid_intent'
    : ['proposal', 'counterProposal', 'promiseDetected'].some(key => out[key] != null && (!validateIntent(out[key]) || key === 'promiseDetected' && !PLAYER_PROMISES.has(out[key].type))) ? 'invalid_intent'
    : 'invalid_metadata';
  return { response: null, replyIssue };
}
