import { INTENT_TYPES } from './data.mjs';
// Shared records have immutable audiences. A changed coalition starts a new
// thread; new allies never inherit an earlier coalition's private history.
const living=(s,id)=>s.knowledgeView?s.kingdoms.find(k=>k.id===id)?.knownAlive!==false:Object.values(s.tiles).some(t=>t.owner===id&&['city','town'].includes(t.building));
export const councilParticipants = (s, host) => !living(s,host)?[]:[host, ...s.kingdoms.filter(k => k.id !== host && k.knownAlive !== false &&
  (s.knowledgeView || Object.values(s.tiles).some(t => t.owner === k.id && ['city','town'].includes(t.building))) &&
  !s.wars.includes([k.id,host].sort().join(':')) &&
  s.treaties.some(t => t.type === 'alliance' && t.expires > s.turn && t.parties.includes(host) && t.parties.includes(k.id)))
  .map(k => k.id).sort()];

export function councilActive(s, c) {
  return !!c && c.participants.length > 1 && JSON.stringify(c.participants) === JSON.stringify(councilParticipants(s, c.host));
}
export function ownCouncil(s, host, create = false) {
  const participants = councilParticipants(s, host);
  if (participants.length < 2) return null;
  const records = s.allianceCouncils || [];
  let c = records.findLast(c => c.host === host && councilActive(s, c));
  if (!c && create && !s.knowledgeView) {
    c = { id: `council-${s.nextId++}`, host, participants, messages: [], read: {}, sequence: 0, initiatedTurn: 0, topics: {} };
    s.allianceCouncils = [...records, c].slice(-24);
  }
  return c;
}
export function councilUnread(c, actor) { return Math.max(0, c.sequence - (c.read[actor] || 0)); }
export function appendCouncil(s, c, speakerHouseId, message, extra = {}) {
  const entry = { id: ++c.sequence, turn: s.turn, speakerHouseId, message: String(message).slice(0, 900), ...extra };
  c.messages.push(entry); c.messages = c.messages.slice(-60);
  return entry;
}
export function projectCouncils(s, viewer) {
  return (s.allianceCouncils || []).filter(c => c.participants.includes(viewer)).map(c => ({
    ...structuredClone(c), read: { [viewer]: c.read[viewer] || 0 }, topics: {}
  }));
}
export function validateCouncilSave(s) {
  const records=s.allianceCouncils ?? [];
  const ids = new Set(s.kingdoms.map(k => k.id)), fail = () => { throw new Error('Damaged alliance council data.'); };
  const obj = x => x && typeof x === 'object' && !Array.isArray(x);
  if (!Array.isArray(records) || records.length > 24) fail();
  const seen = new Set();
  for (const c of records) {
    if (!obj(c) || typeof c.id !== 'string' || !/^council-\d+$/.test(c.id) || seen.has(c.id) || !ids.has(c.host) ||
      !Array.isArray(c.participants) || c.participants.length < 2 || c.participants.length > ids.size || c.participants[0] !== c.host ||
      c.participants.some(id => !ids.has(id)) || new Set(c.participants).size !== c.participants.length ||
      !Number.isSafeInteger(c.sequence) || c.sequence < 0 || !Number.isInteger(c.initiatedTurn) || c.initiatedTurn > s.turn ||
      !obj(c.read) || Object.entries(c.read).some(([id,n]) => !c.participants.includes(id) || !Number.isSafeInteger(n) || n < 0 || n > c.sequence) ||
      !obj(c.topics) || Object.keys(c.topics).length > 20 || Object.values(c.topics).some(n => !Number.isInteger(n) || n < 0 || n > s.turn) ||
      !Array.isArray(c.messages) || c.messages.length > 60) fail();
    seen.add(c.id); let previous = 0;
    for (const m of c.messages) {
      if (!obj(m) || !c.participants.includes(m.speakerHouseId) || typeof m.message !== 'string' || m.message.length > 900 ||
        !Number.isSafeInteger(m.id) || m.id <= previous || m.id > c.sequence || !Number.isInteger(m.turn) || m.turn > s.turn || m.turn < 0) fail();
      previous = m.id;
      // Suggestions are rebuilt by the normal treaty desk, never import actions.
      delete m.intent; delete m.requestedIntent;
    }
  }
  // The same turn's unused invitation survives saving; consumed invitations
  // are absent from the save. Old schema-3 campaigns need no migration.
  const credits=s.proposalFollowups ?? {};
  if(!obj(credits)||Object.keys(credits).length>132)fail();
  for(const [key,c]of Object.entries(credits)){
    if(!obj(c)||!ids.has(c.actor)||!ids.has(c.ruler)||c.actor===c.ruler||key!==`${c.actor}:${c.ruler}`||
      !Number.isInteger(c.turn)||c.turn<0||c.turn>s.turn||!INTENT_TYPES.includes(c.type)||typeof c.targetId!=='string'||c.targetId.length>60||
      typeof c.conversation!=='string'||!(c.conversation===`private:${c.ruler}`||records.some(x=>x.id===c.conversation&&[c.actor,c.ruler].every(id=>x.participants.includes(id)))))fail();
    if(c.turn!==s.turn)delete s.proposalFollowups[key];
  }
  if(s.councilInitiationTurns&&!obj(s.councilInitiationTurns))fail();
}
