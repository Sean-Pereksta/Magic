import { councilForActor, makeCouncilContext, validateCouncilResponse } from './alliance-council.mjs';
import { isAiHouse } from './house-control.mjs';
import { councilDiagnostic } from './council-state.mjs';

export const councilDispatchKey = d => `${d.turn}:${d.councilId}:${d.entries[0].id}`;
function automaticEntries(s, c, index) {
  const opening=c.messages[index],reaction=c.messages[index+1],entries=[opening];
  if (reaction && !reaction.initiated && reaction.turn === opening.turn && isAiHouse(s,reaction.speakerHouseId)) entries.push(reaction);
  return entries.filter(m=>!m.voiced&&!m.voiceFailed&&m.source==='scripted');
}
export function nextCouncilDispatch(s, actor, excluded = new Set()) {
  for (const c of s.allianceCouncils || []) {
    if (!councilForActor(s, actor, c.id)) continue;
    for (let i = 0; i < c.messages.length; i++) {
      const opening = c.messages[i];
      if (!opening.initiated || opening.turn !== s.turn || !isAiHouse(s,opening.speakerHouseId)) continue;
      // Only the automatic opening and its reaction belong to this dispatch.
      // Later player dialogue must neither be overwritten nor starve this job.
      const entries = automaticEntries(s,c,i);
      if (!entries.length) continue;
      const dispatch = { councilId: c.id, openingId:opening.id, turn: s.turn, reason: opening.reason,
        entries: entries.map(m => ({ id: m.id, speakerHouseId: m.speakerHouseId, message: m.message })) };
      if (!excluded.has(councilDispatchKey(dispatch))) return dispatch;
    }
  }
  return null;
}
export function councilDispatchCurrent(s, actor, dispatch) {
  if (!dispatch || !Array.isArray(dispatch.entries) || !dispatch.entries.length || dispatch.entries.length > 3 ||
    dispatch.entries.some(e => !e || !Number.isSafeInteger(e.id) || typeof e.message !== 'string') ||
    new Set(dispatch.entries.map(e => e.id)).size !== dispatch.entries.length) return false;
  const c = councilForActor(s, actor, dispatch.councilId);
  const index=c?.messages.findIndex(m=>m.id===(dispatch.openingId??dispatch.entries[0].id)&&m.initiated&&m.reason===dispatch.reason) ?? -1;
  if (!c || s.turn !== dispatch.turn || index < 0) return false;
  const entries=automaticEntries(s,c,index);
  // A dispatch may contain only its current speaker. The original opening is
  // the stable anchor after an earlier speaker has already been voiced.
  let previous = -1;
  return dispatch.entries.every(e => {
    const position = entries.findIndex(m => m.id === e.id), m = entries[position];
    if (position <= previous || !m) return false;
    previous = position;
    return isAiHouse(s,e.speakerHouseId)&&m.speakerHouseId===e.speakerHouseId&&m.turn===dispatch.turn&&!m.voiced&&!m.voiceFailed&&m.source==='scripted'&&m.message===e.message;
  });
}
export function makeCouncilDispatchContext(s, c, actor, dispatch) {
  if (!councilDispatchCurrent(s, actor, dispatch)) return null;
  const context = makeCouncilContext(s, c, actor, 'Voice the supplied council event and the other rulers’ reactions.');
  if (!context) return null;
  // Subsequent dialogue is not part of an earlier automatic opening.
  context.history = c.messages.filter(m => m.id < dispatch.entries[0].id && m.turn >= s.turn - 2 && (!isAiHouse(s,m.speakerHouseId) || m.source === 'gemini')).slice(-10)
    .map(m => ({speakerHouseId:m.speakerHouseId,turn:m.turn,message:m.message.slice(0,450)}));
  context.world.conversationMode = 'ai-initiated-council';
  context.world.dispatch = { reason: dispatch.reason, turn: dispatch.turn, entries: dispatch.entries };
  while (new TextEncoder().encode(JSON.stringify(context)).length > 22000 && context.history.length) context.history.shift();
  return new TextEncoder().encode(JSON.stringify(context)).length <= 22000 ? context : null;
}
export function applyCouncilDispatch(s, actor, dispatch, raw) {
  if (!councilDispatchCurrent(s, actor, dispatch)) return false;
  const c = councilForActor(s, actor, dispatch.councilId);
  if (raw?.source === 'failed' && !raw.responses?.length) {
    const diagnostic=councilDiagnostic(raw.diagnostic);
    for (const requested of dispatch.entries) {
      const entry=c.messages.find(m=>m.id===requested.id);
      entry.voiceFailed=true;
      if(diagnostic)entry.diagnostic=diagnostic;
    }
    return true;
  }
  if (raw?.source !== 'gemini') return false;
  const response = validateCouncilResponse(raw, c.participants, c.participants.filter(id => id !== actor && isAiHouse(s,id)));
  if (!response || response.responses.length !== dispatch.entries.length || response.responses.some((r,i) => r.speakerHouseId !== dispatch.entries[i].speakerHouseId || r.requestedIntent)) return false;
  response.responses.forEach((r,i) => {
    const entry = c.messages.find(m => m.id === dispatch.entries[i].id);
    entry.fact = entry.message; entry.message = r.message; entry.voiced = true; entry.source = 'gemini';
  });
  return true;
}
