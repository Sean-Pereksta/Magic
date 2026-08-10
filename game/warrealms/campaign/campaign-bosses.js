export const CAMPAIGN_BOSSES = Object.freeze([
  Object.freeze({
    id: "world_eater",
    name: "Voracynth, World Eater",
    title: "The World Eater",
    authority: 120,
    startingShield: 10,
    startingBases: Object.freeze(["worldheart_core", "rift_maw"]),
    bossCard: "voracynth_boss_card",
    bossAbility: Object.freeze({
      trigger: "turn",
      every: 2,
      label: "Devour the Market",
      effect: Object.freeze({ devourCheapestMarket: true, gainCombatFromCost: true })
    }),
    phases: Object.freeze([
      Object.freeze({
        id: "phase_two",
        name: "Phase II · Endless Hunger",
        authorityAtOrBelow: 60,
        createBase: "rift_maw",
        bossCardFrequency: 1
      })
    ])
  })
]);

const bossMap = new Map(CAMPAIGN_BOSSES.map(boss => [boss.id, boss]));

export function getCampaignBoss(bossId) {
  return bossMap.get(String(bossId || "")) || null;
}

export function effectiveBossAbilityFrequency(boss, bossState = {}) {
  let frequency = Math.max(1, Math.floor(Number(boss?.bossAbility?.every) || 1));
  const triggered = new Set(bossState.triggeredPhases || []);
  for (const phase of boss?.phases || []) {
    if (triggered.has(phase.id) && Number.isFinite(Number(phase.bossCardFrequency))) frequency = Math.max(1, Math.floor(Number(phase.bossCardFrequency)));
  }
  frequency = Math.max(1, frequency - Math.max(0, Math.floor(Number(bossState.frequencyReduction) || 0)));
  return frequency;
}

export function bossAbilityStatus(boss, bossState = {}) {
  const turns = Math.max(0, Math.floor(Number(bossState.turns) || 0));
  const every = effectiveBossAbilityFrequency(boss, bossState);
  const remainder = turns % every;
  return {
    every,
    turns,
    due: turns > 0 && remainder === 0,
    activatesIn: remainder === 0 ? every : every - remainder
  };
}

export function advanceBossTurn(boss, bossState = {}) {
  const next = {
    ...bossState,
    turns: Math.max(0, Math.floor(Number(bossState.turns) || 0)) + 1,
    triggeredPhases: [...new Set(bossState.triggeredPhases || [])]
  };
  return { state: next, ability: bossAbilityStatus(boss, next) };
}

export function pendingBossPhases(boss, authority, bossState = {}) {
  const triggered = new Set(bossState.triggeredPhases || []);
  return (boss?.phases || []).filter(phase => !triggered.has(phase.id) && Number(authority) <= Number(phase.authorityAtOrBelow));
}

export function applyBossPhaseState(bossState = {}, phase) {
  return {
    ...bossState,
    triggeredPhases: [...new Set([...(bossState.triggeredPhases || []), phase.id])]
  };
}
