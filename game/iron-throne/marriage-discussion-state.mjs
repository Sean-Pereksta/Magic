/** A negotiation is bilateral even when its last stored speaker changes.
 * Return the same match from the requested actor's perspective, without
 * resetting its start turn, progress or family members. Read-only. */
export function marriageDiscussionFor(state, actor, host) {
  if (actor === host) return null;
  const n = state.royalBonds?.negotiations?.[[actor, host].sort().join(':')];
  if (!n) return null;
  if (n.proposer === actor && n.host === host) return n;
  if (n.proposer === host && n.host === actor) return {
    ...n, proposer: actor, host,
    actorMember: n.rulerMember, rulerMember: n.actorMember
  };
  return null;
}
