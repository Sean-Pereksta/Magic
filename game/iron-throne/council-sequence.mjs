// Game-side pacing only. Every request owns its existing transport deadline;
// there is deliberately no deadline for the conversation as a whole.
export const COUNCIL_RESPONSE_GAP_MS = 750;
export const pauseCouncilResponse = ms => new Promise(resolve => setTimeout(resolve, Math.max(0, ms)));

// A local lock complements the authoritative anchor/commit checks. Re-rendering
// or reopening a panel must never start a second producer for the same council.
export function createCouncilSequenceRunner() {
  const owners = new Map();
  return {
    acquire(id, owner = 'conversation') {
      if (owners.has(id)) return null;
      const token = { id, owner };
      owners.set(id, token);
      return token;
    },
    release(token) { if (token && owners.get(token.id) === token) owners.delete(token.id); },
    busy: id => owners.has(id),
    clear: () => owners.clear()
  };
}

export async function runCouncilSequence({ next, isCurrent, consider, request, commit,
  fallback, onCommit = () => {}, finish = () => {}, gapMs = COUNCIL_RESPONSE_GAP_MS,
  pause = pauseCouncilResponse }) {
  while (isCurrent()) {
    const speaker = next();
    if (!speaker) { const result = await finish(); return result?.ok === false ? result : { ok: true }; }
    const ready = await consider(speaker);
    if (ready?.ok === false || !isCurrent()) break;
    let response;
    try { response = await request(speaker); }
    catch (error) {
      if (!isCurrent()) break;
      response = await fallback(speaker, error);
    }
    if (!isCurrent() || response?.source === 'cancelled') break;
    const result = await commit(speaker, response);
    if (result?.ok === false) return result;
    // Commit and render before even starting the inter-ruler pause.
    await onCommit(speaker, response);
    if (!isCurrent()) break;
    if (!next()) { const result = await finish(); return result?.ok === false ? result : { ok: true }; }
    if (gapMs > 0) await pause(gapMs);
  }
  return { ok: false, cancelled: true, error: 'Circumstances changed. Completed council replies have been kept.' };
}
