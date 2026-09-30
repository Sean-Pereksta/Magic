import { councilForActor, makeCouncilContext, validateCouncilResponse } from './alliance-council.mjs';
import { isAiHouse } from './house-control.mjs';

export function nextCouncilDispatch(s, actor) {
  for (const c of s.allianceCouncils || []) {
    if (!councilForActor(s, actor, c.id)) continue;
    const opening = c.messages.findLast(m => m.initiated && m.turn === s.turn && !m.voiced);
    if (!opening) continue;
    const entries = c.messages.filter(m => m.id >= opening.id);
    if (!entries.length || entries.length > 3 || entries.some(m => m.turn !== s.turn || !isAiHouse(s, m.speakerHouseId))) continue;
    return { councilId: c.id, turn: s.turn, sequence: c.sequence, reason: opening.reason,
      entries: entries.map(m => ({ id: m.id, speakerHouseId: m.speakerHouseId, message: m.message })) };
  }
  return null;
}
export function councilDispatchCurrent(s, actor, dispatch) {
  const c = councilForActor(s, actor, dispatch.councilId);
  return !!c && s.turn === dispatch.turn && c.sequence === dispatch.sequence && dispatch.entries.every(e =>
    isAiHouse(s,e.speakerHouseId) && c.messages.some(m => m.id === e.id && m.message === e.message));
}
export function makeCouncilDispatchContext(s, c, actor, dispatch) {
  if (!councilDispatchCurrent(s, actor, dispatch)) return null;
  const context = makeCouncilContext(s, c, actor, 'Voice the supplied council event and the other rulers’ reactions.');
  if (!context) return null;
  context.history = context.history.filter(m => !dispatch.entries.some(e => e.speakerHouseId === m.speakerHouseId && e.message.slice(0,450) === m.message && m.turn === dispatch.turn));
  context.world.conversationMode = 'ai-initiated-council';
  context.world.dispatch = { reason: dispatch.reason, turn: dispatch.turn, entries: dispatch.entries };
  while (new TextEncoder().encode(JSON.stringify(context)).length > 22000 && context.history.length) context.history.shift();
  return new TextEncoder().encode(JSON.stringify(context)).length <= 22000 ? context : null;
}
export function applyCouncilDispatch(s, actor, dispatch, raw) {
  if (!councilDispatchCurrent(s, actor, dispatch)) return false;
  const c = councilForActor(s, actor, dispatch.councilId);
  const response = validateCouncilResponse(raw, c.participants, c.participants.filter(id => id !== actor && isAiHouse(s,id)));
  if (!response || response.responses.length !== dispatch.entries.length || response.responses.some((r,i) => r.speakerHouseId !== dispatch.entries[i].speakerHouseId || r.requestedIntent)) return false;
  response.responses.forEach((r,i) => {
    const entry = c.messages.find(m => m.id === dispatch.entries[i].id);
    entry.fact = entry.message; entry.message = r.message; entry.voiced = true; entry.source = 'gemini';
  });
  return true;
}
