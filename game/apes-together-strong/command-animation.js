/* Order-driven presentation. Weak state is neither saved nor consulted by combat. */
(() => {
  'use strict';
  const responses = new WeakMap(), TAU = Math.PI * 2;
  const ORDER_STATES = new Set(['charge', 'hold', 'follow', 'retreat', 'defend', 'attack']);
  const VISUAL_ORDERS = new Set(['charge', 'follow', 'hold', 'defend', 'retreat', 'attack', 'movement']);
  const RECALL_COMMANDS = new Set(['call', 'recall', 'recallField', 'recallAll']);
  const PRIMARY_POSES = new Set(['hurt', 'death', 'knockback', 'climb', 'throw', 'attack', 'heavyAttack', 'chargeAttack']);
  const aliases = Object.freeze({ move: 'movement', regroup: 'follow', rally: 'follow', shield: 'defend',
    assault: 'attack', sabotage: 'attack', attackArea: 'attack', climb: 'movement',
    spreadCharge: 'charge', attackNearest: 'defend', nearestTarget: 'charge', nearestHuman: 'charge',
    call: 'follow', recall: 'retreat', recallField: 'retreat', recallAll: 'retreat', settle: 'hold', settleAll: 'hold', patrol: 'defend' });
  const canonical = kind => aliases[kind] || kind;
  function orderFor(a, game, time = game?.time || 0) {
    if (!a || a.hp <= 0 || a.id === 'king' || a.captive || a.prisoner) return null;
    const group = a.armyOrder && game?.siege?.groups?.[a.species];
    if (group && group.id === a.armyOrder?.group) {
      const order = group.orders?.[a.armyOrder.index || 0];
      if (order) return canonical(order.type);
    }
    if (a.nearestOrder && a.nearestOrder.until > time) return 'charge';
    if (a.state === 'charge' && a.commandStyle === 'attackNearest') return 'defend';
    if (a.state === 'follow' && a.retreatUntil > time && a.recallOrder?.kind !== 'recruit') return 'retreat';
    if (ORDER_STATES.has(a.state)) return a.state;
    return null;
  }
  function response(a, game, time, knownOrder) {
    const r = responses.get(a);
    return r && time >= r.at && time < r.until && r.order === (knownOrder === undefined ? orderFor(a, game, time) : knownOrder) ? r : null;
  }
  function remember(a, game) {
    const order = orderFor(a, game);
    if (order) responses.set(a, { order, at: game.time, until: game.time + (order === 'charge' ? .22 : .3) });
    else responses.delete(a);
  }
  function signature(a) {
    return [a.state, a.armyOrder, a.nearestOrder, a.recallOrder, a.target];
  }
  const G = window.ATSGame?.prototype;
  if (G?.command) {
    const command = G.command, effect = G.effect;
    G.command = function (kind, ...args) {
      const known = canonical(kind), visual = VISUAL_ORDERS.has(known);
      if (!visual) return command.call(this, kind, ...args);
      const before = this.receiveHordeRecall && RECALL_COMMANDS.has(kind) ? [] : (this.apes || []).map(a => [a, signature(a)]), parent = this._commandVisualIssue;
      const aim = args[0] || this.aim || { x: 1, y: 0 }, dx = Number.isFinite(aim.x) ? aim.x : 1, dy = Number.isFinite(aim.y) ? aim.y : 0;
      this._commandVisualIssue = { commandKind: known, dir: Math.atan2(dy, dx),
        originX: this.king.x, originY: this.king.y };
      let result;
      try { result = command.call(this, kind, ...args); }
      finally { this._commandVisualIssue = parent; }
      if (result !== false) for (const [a, previous] of before) {
        const current = signature(a);
        if (current.some((value, i) => value !== previous[i])) remember(a, this);
      }
      return result;
    };
    if (effect) G.effect = function (type, x, y, options = {}) {
      const issue = this._commandVisualIssue;
      // Keep historical effect identifiers and lifetimes for saves, pooling and
      // existing consumers; only their painted representation changes.
      return effect.call(this, type, x, y, issue && (type === 'wave' || type === 'charge') ? { ...options, ...issue } : options);
    };
    if (G.receiveHordeRecall) {
      const recall = G.receiveHordeRecall;
      G.receiveHordeRecall = function (a, ...args) { const result = recall.call(this, a, ...args); remember(a, this); return result; };
    }
  }
  const S = window.ATSSiege?.prototype;
  if (S?.issue) {
    const issue = S.issue;
    S.issue = function (species, orders, append = false) {
      const result = issue.call(this, species, orders, append);
      if (!result || !orders?.length) return result;
      const list = this.members(species).filter(a => a.armyOrder?.group === this.groups[a.species]?.id);
      if (!list.length) return result;
      if (!append) for (const a of list) remember(a, this.g);
      const order = orders[0], kind = canonical(order.type), now = this.g.time;
      // Dispatching a mixed army issues once per species. Share one standard.
      if (!this.g.effects.some(e => e.type === 'command' && e.commandKind === kind &&
        e.targetId === order.targetId && Math.abs(e.x - order.x) + Math.abs(e.y - order.y) < 2 && now - e.issuedAt < .08)) {
        const center = this.center(species);
        this.g.effect('command', order.x, order.y, { commandKind: kind, targetId: order.targetId,
          targetKind: order.targetKind, issuedAt: now, life: .95, originX: center.x, originY: center.y,
          dir: Math.atan2(order.y - center.y, order.x - center.x) });
      }
      return result;
    };
  }
  window.ATSCommandAnimation = {
    version: 1, canonical, orderFor, response,
    tempo(a, game, time, knownOrder) {
      const order = knownOrder === undefined ? orderFor(a, game, time) : knownOrder;
      return order === 'charge' ? 1.42 : order === 'retreat' ? 1.28 : order === 'follow' ? 1.05 : 1;
    },
    apply(c, a, pose, time, reducedMotion = false) {
      if (!pose.commandOrder || PRIMARY_POSES.has(pose.state)) return;
      const heavy = a.species === 'gorilla', guard = pose.commandGuard;
      // Deform around the planted feet. Never move an actor or delay its order.
      if (guard) {
        const breath = reducedMotion ? 0 : Math.sin(time * (heavy ? 1.8 : 2.5) + (a.phase || 0)) * .006;
        c.scale(heavy ? 1.055 : 1.025, (heavy ? .93 : .96) + breath);
      } else if (pose.state === 'sprint') {
        const sx = (Math.cos(a.dir || 0) - Math.sin(a.dir || 0)) * (pose.mirror ? -1 : 1);
        c.transform(1, 0, -Math.sign(sx) * (heavy ? .07 : .1), .98, 0, 0);
      } else if (pose.commandCry && !reducedMotion) {
        c.scale(1.035, 1.025 + Math.sin(pose.phase * TAU) * .008);
      }
    }
  };
})();
