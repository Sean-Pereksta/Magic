/* Presentation-only targeting. Canonical engine actions remain unchanged. */
const SELF_AREA_KINDS = new Set(['bloomcall', 'wrongnote', 'concordance', 'sanctuary']);
const distance = (a, b) => Math.abs(a.x - b.x) + Math.abs(a.y - b.y);
const actionSkill = (action, definition) => definition.abilities?.[action.skillIndex || 0] || definition.ability;

/** Describe an action's visual meaning, independently of its faction color. */
export function actionColor(action, unit, unitDef) {
  if (action.type === 'move') return 'move';
  if (action.type === 'attack') return 'attack';
  if (action.type === 'guard') return 'support';
  const kind = actionSkill(action, unitDef(unit)).kind;
  if (['mercy', 'bloomcall', 'nectar'].includes(kind)) return 'heal';
  if (['sporehop', 'starstep'].includes(kind)) return 'move';
  if (['sanctuary', 'concordance', 'seedgrove', 'wardray', 'broodlings', 'pocketmaws', 'bellmotes'].includes(kind)) return 'support';
  return 'attack';
}

/**
 * Let a self-centered area skill be selected by clicking an affected troop.
 * displayX/displayY belong to the UI; x/y/targetId still identify the engine's
 * canonical self-targeted action and must be passed through unchanged.
 */
export function displayTargets(state, unit, actions, unitDef) {
  if (!unit || !actions.length) return actions;
  const definition = unitDef(unit);
  const selfArea = action => action.type === 'ability' && action.x === unit.x && action.y === unit.y && SELF_AREA_KINDS.has(actionSkill(action, definition).kind);
  if (!actions.some(selfArea)) return actions;

  return actions.flatMap(action => {
    if (!selfArea(action)) return [action];
    const skill = actionSkill(action, definition);
    return state.units.filter(target => {
      if (target.hp <= 0 || distance(unit, target) > skill.range) return false;
      if (skill.kind === 'wrongnote') return target.side !== unit.side;
      if (target.side !== unit.side) return false;
      if (skill.kind === 'bloomcall') return target.hp < target.maxHp;
      if (skill.kind === 'sanctuary') return (target.shield ?? 0) < 5;
      return target.id !== unit.id && (target.readyAt > state.time || (target.shield ?? 0) < 3);
    }).map(target => ({ ...action, displayX: target.x, displayY: target.y }));
  });
}
