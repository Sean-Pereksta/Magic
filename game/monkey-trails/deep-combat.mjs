// Divide changes in a combined health bar without erasing either individual's injuries.
export function splitHealth(h, r, hm, rm, total) {
  const delta = Math.max(0, Math.min(total, hm + rm)) - h - r;
  const share = delta < 0 ? h / (h + r || 1) : (hm - h) / (hm + rm - h - r || 1);
  const host = Math.max(0, Math.min(hm, h + delta * share));
  return [host, Math.max(0, Math.min(rm, h + r + delta - host))];
}
export function deepSetup(m, actor, target, members, max, round) {
  switch (m.deep) {
    case 'driftmantle': {
      const ally = members.filter((p) => p.hp > 0).sort((a, b) => a.hp - b.hp)[0] || actor;
      ally.shield = Math.max(ally.shield || 0, Math.round(max(ally) * 0.28));
      const status = ['burn', 'poison'].find((k) => ally.status[k]);
      if (status) {
        delete ally.status[status];
        delete ally.statusPower[status];
      }
      break;
    }
    case 'vesperloom':
      actor.weave = { until: round + 1, damage: 0 };
      break;
    case 'mirrorkin':
      actor.double = true;
      break;
    case 'coppercoil':
      actor.groundwire = true;
      break;
    case 'kilnveil':
      actor.curtain = true;
      break;
    case 'rimewhistle':
      target.whiteNote = true;
      break;
    case 'nacreon':
      target.soakedBy = actor.id;
      break;
  }
}
export function deepHit(b, actor, target, m, damage, context) {
  const { round, interrupted, riderHost, max, follow } = context;
  let extra = 0;
  if (target.weave && round <= target.weave.until) target.weave.damage += damage;
  if (target.groundwire && target.guard && damage) {
    target.arc = true;
    delete target.groundwire;
  }
  if (m.deep === 'carilluma' && interrupted) actor.peal = { round: round + 1, damage };
  if (m.deep === 'keelknuckle' && target.deepActed === round) extra += follow(Math.round(damage * 0.5));
  if (m.deep === 'vitreus') {
    extra += follow(damage);
    extra += follow(damage);
  }
  if (actor.weave && round > actor.weave.until && damage) {
    extra += follow(actor.weave.damage);
    delete actor.weave;
  }
  if (damage && m.type === 'storm') {
    if (actor.arc) {
      extra += follow(damage);
      delete actor.arc;
    }
    if (target.whiteNote) {
      delete target.charging;
      target.prepare = 0;
      delete target.whiteNote;
    }
  }
  if (damage && riderHost) {
    if (target.soakedBy === riderHost.id) {
      extra += follow(damage + extra);
      delete target.soakedBy;
    }
    if (riderHost.crownElement && m.type !== riderHost.crownElement)
      riderHost.shield = Math.max(riderHost.shield || 0, Math.round(max * 0.25));
    delete riderHost.crownElement;
  }
  if (m.deep === 'orrakai') actor.crownElement = m.type;
  return extra;
}
export function pairDefense(host, move, riderMove, max) {
  if (
    move.deep === 'bridgeback' &&
    (riderMove.guard || riderMove.shield || riderMove.teamShield || riderMove.evasion || riderMove.counter)
  )
    host.shield = Math.max(host.shield || 0, Math.round(max * 0.25));
}
