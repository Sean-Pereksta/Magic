/* Formation transactions are independent of the DOM and never copy troops. */
(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.TinyTroopsManagementRules = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';
  const id = u => u && String(u.ttId || u.id || '');
  const indexOK = (value, size) => Number.isInteger(value) && value >= 0 && value < size;
  function preparationReason(state) {
    if (!state?.tt || state.tt.mode === 'MENU') return 'Resume a run to manage troops.';
    if (state.tt.finished || state.runEnded) return 'This run has ended.';
    if (!['recruit', 'shop'].includes(state.phase) || state.battle) return 'Formation changes are available between battles.';
    if (state.placing || state.placingUpgrade || state.placingEffect || state.placingSpecialization) return 'Finish or cancel your selected card first.';
    if (state.evoTarget || state.ascTarget || state.starChoiceTarget || state.ultimate10Target || state.tt.event || state.tt.pathTarget || state.tt.relicDraft?.length) return 'Close or finish your current choice first.';
    return '';
  }
  function move(state, source, target) {
    const reason = preparationReason(state); if (reason) return { ok: false, reason };
    const squad = state.squad.slice(), bench = Array.from({ length: 3 }, (_, i) => state.bench?.[i] || null);
    const lists = { board: squad, bench };
    if (!lists[source?.where] || !lists[target?.where] || !indexOK(source.index, lists[source.where].length) || !indexOK(target.index, lists[target.where].length)) return { ok: false, reason: 'Choose a valid deployment space.' };
    const u = lists[source.where][source.index], other = lists[target.where][target.index];
    if (!u) return { ok: false, reason: 'That troop is no longer in its selected slot.' };
    if (source.unit && source.unit !== u) return { ok: false, reason: 'The selected troop changed. Select it again.' };
    if (source.where === target.where && source.index === target.index) return { ok: false, reason: 'Choose a different square.' };
    const bossSwap = source.where !== target.where && !!other;
    if (bossSwap && !state.bossSwapAvailable) return { ok: false, reason: 'Swapping a reserve with a deployed troop requires a post-boss switch. Empty spaces are free.' };
    lists[source.where][source.index] = other || null; lists[target.where][target.index] = u;
    const all = [...squad, ...bench].filter(Boolean);
    if (new Set(all).size !== all.length) return { ok: false, reason: 'A duplicate troop was detected. Restore the checkpoint before changing formation.' };
    return { ok: true, squad, bench, bossSwap, unit: u };
  }
  function sanitizePresets(value, size = 20) {
    return Array.from({ length: 3 }, (_, i) => {
      const p = Array.isArray(value) ? value[i] : null;
      if (!p || typeof p !== 'object' || !Array.isArray(p.board)) return null;
      const seen = new Set();
      return { name: String(p.name || 'Formation ' + (i + 1)).slice(0, 40), board: Array.from({ length: size }, (_, j) => {
        const key = typeof p.board[j] === 'string' ? p.board[j].slice(0, 160) : '';
        if (!key || seen.has(key)) return null; seen.add(key); return key;
      }) };
    });
  }
  function capture(squad, name) { return { name: String(name || 'Formation').slice(0, 40), board: squad.map(u => id(u) || null) }; }
  function restore(state, preset) {
    const reason = preparationReason(state); if (reason) return { ok: false, reason };
    if (!preset || !Array.isArray(preset.board)) return { ok: false, reason: 'Save a formation in this slot first.' };
    const deployed = state.squad.filter(Boolean), byId = new Map(deployed.map(u => [id(u), u]));
    if (deployed.some(u => !id(u)) || byId.size !== deployed.length) return { ok: false, reason: 'Troop identities are updating. Try saving the formation again.' };
    if (preset.board.some(key => key && !byId.has(key))) return { ok: false, reason: 'This preset contains a reserve or missing troop. Deploy its troops first; reserve swaps keep the post-boss rule.' };
    if (preset.board.slice(state.squad.length).some(Boolean)) return { ok: false, reason: 'This preset needs the additional board row.' };
    const squad = Array(state.squad.length).fill(null), used = new Set();
    preset.board.slice(0, squad.length).forEach((key, i) => { if (key && !used.has(key)) { squad[i] = byId.get(key); used.add(key); } });
    deployed.filter(u => !used.has(id(u))).forEach(u => squad[squad.indexOf(null)] = u);
    return { ok: true, squad };
  }
  function autoArrange(squad, role) {
    const rows = Math.ceil(squad.length / 4), result = Array(squad.length).fill(null);
    const preference = { ARMORED: [3, 2, 1, 0], MELEE: [2, 3, 1, 0], FAST: [2, 3, 1, 0], SUPPORT: [1, 0, 2, 3], MAGIC: [0, 1, 2, 3], RANGED: [0, 1, 2, 3] };
    const weight = { ARMORED: 0, RANGED: 1, MAGIC: 2, SUPPORT: 3, MELEE: 4, FAST: 5 };
    squad.filter(Boolean).map((u, i) => ({ u, i, role: role(u) })).sort((a, b) => (weight[a.role] ?? 9) - (weight[b.role] ?? 9) || a.i - b.i).forEach(({ u, role: kind }) => {
      for (const col of preference[kind] || preference.MELEE) {
        const rowOrder = Array.from({ length: rows }, (_, row) => row).sort((a, b) => result.slice(a * 4, a * 4 + 4).filter(Boolean).length - result.slice(b * 4, b * 4 + 4).filter(Boolean).length || a - b);
        const row = rowOrder.find(row => row * 4 + col < result.length && !result[row * 4 + col]);
        if (row !== undefined) { result[row * 4 + col] = u; return; }
      }
    });
    return result;
  }
  function previewEvolution(unit, option, stage, boost) {
    if (!unit || typeof option?.apply !== 'function') return { ok: false, reason: 'This evolution is no longer available.' };
    // Native apply functions only modify their unit. Run them on an isolated copy.
    try {
      const next = JSON.parse(JSON.stringify(unit)); option.apply(next);
      if (typeof boost === 'function') boost(next, stage, 'evolution-preview');
      next.hp = next.maxHp; next.shield = (next.shield || 0) + (stage === 6 ? 70 : 35);
      if (![next.maxHp, next.atk, next.spd, next.baseMaxHp, next.baseAtk, next.baseSpd].every(Number.isFinite)) return { ok: false, reason: 'This evolution has invalid stats. Choose another path.' };
      return { ok: true, unit: next };
    } catch { return { ok: false, reason: 'This evolution could not be previewed. Your troop is unchanged.' }; }
  }
  return { id, preparationReason, move, capture, restore, sanitizePresets, autoArrange, previewEvolution };
});
