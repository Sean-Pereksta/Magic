/* Presentation-only targeting. Canonical engine actions remain unchanged. */
const SELF_AREA_KINDS = new Set(['bloomcall', 'wrongnote', 'concordance', 'sanctuary', 'rootguard', 'grinward']);
const distance = (a, b) => Math.abs(a.x - b.x) + Math.abs(a.y - b.y);
const actionSkill = (action, definition) => definition.abilities?.[action.skillIndex || 0] || definition.ability;

/** Describe an action's visual meaning, independently of its faction color. */
export function actionColor(action, unit, unitDef) {
  if (action.type === 'move') return 'move';
  if (action.type === 'attack') return 'attack';
  if (action.type === 'guard') return 'support';
  const skill=actionSkill(action, unitDef(unit));
  if(['move','attack','heal','support'].includes(skill.color))return skill.color;
  const kind = skill.kind;
  if (['mercy', 'bloomcall', 'nectar'].includes(kind)) return 'heal';
  if (['sporehop', 'starstep'].includes(kind)) return 'move';
  if (['sanctuary', 'concordance', 'seedgrove', 'wardray', 'broodlings', 'pocketmaws', 'bellmotes'].includes(kind)) return 'support';
  return 'attack';
}

/** Preview prospective painted ground without replacing its canonical center. */
export function groundPreviewCells(state,unit,action,unitDef){
  if(!unit||action?.type!=='ability'||!Number.isInteger(action.x)||!Number.isInteger(action.y))return [];
  const skill=actionSkill(action,unitDef(unit)),blast=skill.kind==='detonate';if(!skill.groundKind&&!blast)return [];
  const radius=Number.isInteger(skill.radius)?Math.max(0,skill.radius):0;
  const departure=['pollenstep','riftstep'].includes(skill.kind);
  return (state.tiles||[]).filter(tile=>(blast||['grass','grove'].includes(tile.terrain))&&(distance(tile,action)<=radius||departure&&tile.x===unit.x&&tile.y===unit.y)).map(({x,y})=>({x,y}));
}

/**
 * Let a self-centered area skill be selected by clicking an affected troop.
 * displayX/displayY belong to the UI; x/y/targetId still identify the engine's
 * canonical self-targeted action and must be passed through unchanged.
 */
export function displayTargets(state, unit, actions, unitDef) {
  if (!unit || !actions.length) return actions;
  const definition = unitDef(unit);
  const selfArea = action => action.type === 'ability' && action.x === unit.x && action.y === unit.y && (SELF_AREA_KINDS.has(actionSkill(action, definition).kind)||actionSkill(action,definition).target==='self');
  if (!actions.some(selfArea)) return actions;

  return actions.flatMap(action => {
    if (!selfArea(action)) return [action];
    const skill = actionSkill(action, definition);
    const targets=state.units.filter(target => {
      if (target.hp <= 0 || distance(unit, target) > skill.range) return false;
      if (skill.kind === 'wrongnote') return target.side !== unit.side;
      if (target.side !== unit.side) return false;
      if (skill.kind === 'bloomcall') return target.hp < target.maxHp;
      if (['sanctuary','rootguard','grinward'].includes(skill.kind)) return (target.shield ?? 0) < 5;
      return target.id !== unit.id && (target.readyAt > state.time || (target.shield ?? 0) < 3);
    });
    if(skill.kind==='grinward'&&!targets.length&&state.units.some(target=>target.hp>0&&target.side!==unit.side&&distance(unit,target)<=skill.range))return [action];
    return targets.map(target => ({ ...action, displayX: target.x, displayY: target.y }));
  });
}
