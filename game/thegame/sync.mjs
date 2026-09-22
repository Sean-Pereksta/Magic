// Pure protocol helpers shared by the browser and regression tests.
export const revisionOf = state => Number(state?.revision || 0);
export function sameVersion(a,b) {
  return Boolean(a && b && revisionOf(a) === revisionOf(b) &&
    (a.matchId || '') === (b.matchId || '') && a.updatedAt === b.updatedAt);
}
export function decorateCommit(state,patch,intent,replace = false) {
  const next = replace ? {...patch} : {...state,...patch};
  const result = {...patch,revision:revisionOf(state)+1,
    matchId:patch.matchId || state.matchId || intent.id,lastAction:intent};
  // Powers consume cards too: a last power card must finish the match.
  if (['game','game2'].includes(next.phase) && next.deck?.length === 0 &&
      Object.values(next.hands || {}).every(hand => hand.length === 0)) {
    result.phase = 'won';
    result.log = [...(next.log || []),{message:'Victory — all cards were played.',at:patch.updatedAt || Date.now()}];
  }
  if (result.log) result.log = result.log.slice(-120);
  return result;
}
