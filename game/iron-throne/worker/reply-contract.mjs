import { CAMPAIGN_HOUSES, INTENT_TYPES, RESOURCES } from '../data.mjs';
import { validateIntent, validateResponse } from '../diplomacy.mjs';
import { MARRIAGE_FIELDS } from '../marriage.mjs';
import { PLAYER_PROMISES } from '../promises.mjs';

// Provider schema restored from 66ff3a2, before the September 30 queue change.
// Keep game validation separate from the provider grammar.
export const RESPONSE_SCHEMA = {
  type: 'OBJECT', required: ['reply', 'intents', 'tone'], properties: {
    reply: { type: 'STRING', description: 'In-character response, at most 1600 characters. Terms are proposals awaiting council validation and player ratification.' },
    tone: { type: 'STRING', enum: ['warm', 'neutral', 'cold', 'hostile', 'guarded'] },
    intents: { type: 'ARRAY', maxItems: 3, items: { type: 'OBJECT', required: ['type'], properties: {
      actorMember:{type:'STRING',enum:['ruler','daughter','son']}, rulerMember:{type:'STRING',enum:['ruler','daughter','son']},
      shipmentResource:{type:'STRING',enum:['gold','food','iron','horses']}, shipmentAmount:{type:'INTEGER',minimum:0,maximum:100}, shipmentTurns:{type:'INTEGER',minimum:0,maximum:20}, defense:{type:'BOOLEAN'}, trade:{type:'BOOLEAN'},
      type: { type: 'STRING', enum: INTENT_TYPES }, targetId: { type: 'STRING' },
      giveResource: { type: 'STRING', enum: RESOURCES }, giveAmount: { type: 'INTEGER', minimum: 0, maximum: 1000 },
      receiveResource: { type: 'STRING', enum: RESOURCES }, receiveAmount: { type: 'INTEGER', minimum: 0, maximum: 1000 },
      tradeKind: {type:'STRING', enum:['immediate','recurring','purchase','strategic','emergency','preferential']},
      duration: { type: 'INTEGER', minimum: 1, maximum: 20 }, conditionHouseId: { type: 'STRING', enum: CAMPAIGN_HOUSES.map(h => h.id) }
    } } }
  }
};
export const INTENT_SCHEMA = RESPONSE_SCHEMA.properties.intents.items;
const intentSchema = INTENT_SCHEMA;
Object.assign(RESPONSE_SCHEMA.properties, {
  proposal: { ...intentSchema, nullable: true }, counterProposal: { ...intentSchema, nullable: true }, promiseDetected: { ...intentSchema, nullable: true },
  relationshipSummary: { type: 'STRING', description: 'Optional rolling conversation interpretation, at most 360 characters. Never rewrite verified history.' },
  speechAct: { type: 'STRING', enum: ['statement', 'question', 'accept', 'reject', 'counteroffer', 'promise', 'warning', 'gratitude'] },
  relationshipSignals: { type: 'ARRAY', maxItems: 4, items: { type: 'STRING' } },
  memoryCandidates: { type: 'ARRAY', maxItems: 4, items: { type: 'STRING', description: 'Short conversation interpretation, at most 180 characters; do not invent past actions.' } }
});

const optionalIntentFields = ['targetId', 'giveResource', 'giveAmount', 'receiveResource', 'receiveAmount', 'duration', 'tradeKind', 'conditionHouseId', ...MARRIAGE_FIELDS, 'giveItems', 'receiveItems'];
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
