/** Durable class mastery. These helpers never read storage or mutate their inputs. */
import { CLASSES } from './classes.mjs?v=4';
export const MASTERY_RANK_XP = Object.freeze([0, 60, 180, 420, 800, 1350]);
export const MASTERY_MAX_XP = 50_000;
export const SPECIALIZATION_RANK = 3;
const branch = (classId, slug, name, description, mods) => Object.freeze({ id: `${classId}-${slug}`, classId, name, description, unlockRank: SPECIALIZATION_RANK, mods: Object.freeze(mods) });
const pair = (classId, first, second) => Object.freeze([branch(classId, ...first), branch(classId, ...second)]);
export const SPECIALIZATIONS = Object.freeze({
  'ember-mage': pair('ember-mage', ['pyromancer', 'Pyromancer', 'Burn deals 20% more damage and lasts two seconds longer.', { burnPower: 1.2, burnDuration: 2 }], ['blastweaver', 'Blastweaver', 'Splash radius grows by 24; Firewave strikes an extra enemy.', { splashRadius: 24, spellTargets: 1 }]),
  'ember-knight': pair('ember-knight', ['cinder-bulwark', 'Cinder Bulwark', 'Shields are 30% stronger; Cinder Charge grants 12% damage reduction for four seconds.', { shieldPower: 1.3, chargeFortify: .12 }], ['blazeblade', 'Blazeblade', 'Cinder Charge deals 35% more damage and roots its target for 1.2 seconds.', { chargeDamage: 1.35, chargeRoot: 1.2 }]),
  'phoenix-adept': pair('phoenix-adept', ['kindled-soul', 'Kindled Soul', 'Rebirth restores 50% health and grants 35 extra shield.', { rebirthHealth: .5, rebirthShield: 35 }], ['lifeflame', 'Lifeflame', 'Phoenix Flare healing is 25% stronger and burns one extra enemy.', { healPower: 1.25, phoenixTargets: 1 }]),
  'tide-ranger': pair('tide-ranger', ['farwinter', 'Farwinter', 'Gain 35 range; Piercing Frost strikes one extra enemy.', { range: 35, pierceTargets: 1 }], ['frostbound', 'Frostbound', 'Your slows are 8% stronger and last two seconds longer.', { slowPower: .08, slowDuration: 2 }]),
  cryomancer: pair('cryomancer', ['permafrost', 'Permafrost', 'Frost Nova freezes enemies for 0.6 seconds longer.', { freezeDuration: .6 }], ['icebreaker', 'Icebreaker', 'Strike frozen enemies for 30% more damage; Frost Nova affects an extra foe.', { frozenDamage: 1.3, freezeTargets: 1 }]),
  'tide-oracle': pair('tide-oracle', ['cleansing-current', 'Cleansing Current', 'Every ally cleansed by Restorative Tide also gains 20 shield.', { cleanseShield: 20 }], ['deepwell', 'Deepwell', 'Healing is 30% stronger and Restorative Tide heals an extra ally.', { healPower: 1.3, healingTargets: 1 }]),
  'grove-warden': pair('grove-warden', ['rootkeeper', 'Rootkeeper', 'Roots last 0.8 seconds longer and poison deals 15% more damage.', { rootDuration: .8, poisonPower: 1.15 }], ['oathspring', 'Oathspring', 'Passive healing is 35% stronger; Living Roots grants its healed ally 15 shield.', { passiveHealPower: 1.35, rootHealShield: 15 }]),
  'bloom-druid': pair('bloom-druid', ['everbloom', 'Everbloom', 'Regeneration lasts two seconds longer and restores 15% more health per second.', { regenDuration: 2, regenPower: 1.15 }], ['growing-circle', 'Growing Circle', 'Renewal Garden affects an extra ally and grants each healed ally 12 shield.', { healingTargets: 1, regenerationShield: 12 }]),
  'thorn-guardian': pair('thorn-guardian', ['ironbriar', 'Ironbriar', 'Gain 6% armor and reflect an additional 8% of melee health damage.', { armor: .06, reflectBonus: .08 }], ['grasping-forest', 'Grasping Forest', 'Thicket Bulwark roots one extra enemy for one second longer and deals 20% more spell damage.', { rootTargets: 1, rootDuration: 1, thornDamage: 1.2 }]),
  'storm-lancer': pair('storm-lancer', ['stormpiercer', 'Stormpiercer', 'Chain attacks reach an extra enemy; flying foes take 20% more damage.', { chainTargets: 1, antiAirPower: 1.2 }], ['thunderrush', 'Thunderrush', 'Move 15% faster and make basic attacks 12% faster.', { speed: 1.15, attackRate: 1.12 }]),
  'tempest-mage': pair('tempest-mage', ['storm-eye', 'Storm Eye', 'Thunderstorm strikes an extra enemy.', { spellTargets: 1 }], ['conductor', 'Conductor', 'Deal 20% more damage to controlled foes; lightning spells slow enemies by 18%.', { controlledDamage: 1.2, lightningSlow: .18 }]),
  windcaller: pair('windcaller', ['tailwind', 'Tailwind', 'Zephyr Aura is 8% stronger and lasts two seconds longer.', { rallyPower: .08, rallyDuration: 2 }], ['cyclone-guide', 'Cyclone Guide', 'Push enemies 18 farther, affect one extra enemy, and deal 15% more spell damage.', { pushDistance: 18, windTargets: 1, windDamage: 1.15 }]),
  'sun-paladin': pair('sun-paladin', ['aegis-keeper', 'Aegis Keeper', 'Shields are 30% stronger; Dawn Barrier grants 6% additional damage reduction.', { shieldPower: 1.3, barrierFortify: .06 }], ['dawnhammer', 'Dawnhammer', 'Attack 12% faster; Dawn Barrier scorches the nearest foe for 12 holy damage.', { attackRate: 1.12, barrierRadiance: 12 }]),
  'dawn-priest': pair('dawn-priest', ['mending-light', 'Mending Light', 'Healing is 25% stronger and Radiance shields an extra ally.', { healPower: 1.25, priestShieldTargets: 1 }], ['purifier', 'Purifier', 'Purge damage is 40% stronger; cleansed allies gain 10% damage reduction for four seconds.', { purgePower: 1.4, cleanseFortify: .1 }]),
  'sun-crusader': pair('sun-crusader', ['judicator', 'Judicator', 'Solar Verdict deals 30% more damage, with a further 35% bonus against wounded foes.', { verdictPower: 1.3, verdictExecute: .35 }], ['sunbreaker', 'Sunbreaker', 'Solar Verdict strikes an extra enemy and deals 40% more shield damage.', { shieldBreakPower: 1.4, verdictTargets: 1 }]),
  'void-reaper': pair('void-reaper', ['harvester', 'Harvester', 'Steal an additional 12% life; Soul Harvest healing is 25% stronger.', { drainBonus: .12, harvestHeal: 1.25 }], ['executioner', 'Executioner', 'Execute damage gains 20%; Soul Harvest executes below 45% health.', { executeBonus: .2, harvestThreshold: .45 }]),
  necromancer: pair('necromancer', ['bone-lord', 'Bone Lord', 'Raised guards have 25% more health and deal 15% more damage.', { guardHealth: 1.25, guardAttack: 1.15 }], ['grave-caller', 'Grave Caller', 'Raise two additional guards over your lifetime; every guard lasts five seconds longer.', { raiseLimit: 2, guardDuration: 5 }]),
  hexweaver: pair('hexweaver', ['doomweaver', 'Doomweaver', 'Curses increase damage taken by an additional 4% and last two seconds longer.', { cursePower: .04, curseDuration: 2 }], ['spellthief', 'Spellthief', 'Weakness is 8% stronger; each curse grants the caster 18 shield.', { weaknessPower: .08, curseShield: 18 }]),
});

const boundedXp = value => typeof value === 'number' && Number.isFinite(value) ? Math.min(MASTERY_MAX_XP, Math.max(0, Math.floor(value))) : 0;
export function masteryRank(value = 0) {
  const xp = boundedXp(typeof value === 'object' && value ? value.xp : value);
  let rank = 1;
  for (let index = 1; index < MASTERY_RANK_XP.length; index++) if (xp >= MASTERY_RANK_XP[index]) rank = index + 1;
  return rank;
}
export function masteryProgress(value = 0) {
  const xp = boundedXp(typeof value === 'object' && value ? value.xp : value);
  const rank = masteryRank(xp), rankXp = MASTERY_RANK_XP[rank - 1];
  const nextXp = MASTERY_RANK_XP[Math.min(rank, MASTERY_RANK_XP.length - 1)];
  return { rank, xp, rankXp, nextXp, progress: rank === 6 ? 1 : (xp - rankXp) / (nextXp - rankXp), maxRank: 6 };
}
export function specializationFor(classId, id) {
  return Object.prototype.hasOwnProperty.call(SPECIALIZATIONS, classId) ? SPECIALIZATIONS[classId].find(branch => branch.id === id) || null : null;
}

export function normalizeMastery(input) {
  const source = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  return Object.fromEntries(Object.keys(CLASSES).map(classId => {
    const record = Object.prototype.hasOwnProperty.call(source, classId) && source[classId] && typeof source[classId] === 'object' && !Array.isArray(source[classId]) ? source[classId] : {};
    const xp = boundedXp(record.xp), specialization = masteryRank(xp) >= SPECIALIZATION_RANK && specializationFor(classId, record.specialization) ? record.specialization : null;
    return [classId, { xp, specialization }];
  }));
}
export function masteryBonuses(classId, record = {}) {
  const rank = masteryRank(record?.xp), specialization = rank >= SPECIALIZATION_RANK ? specializationFor(classId, record?.specialization) : null;
  return { rank, hp: 1 + (rank - 1) * .02, attack: 1 + (rank - 1) * .015, cooldown: 1 - (rank - 1) * .015, specialization, mods: specialization?.mods || Object.freeze({}) };
}

/** Capped participation XP. Call only after a terminal battle, once per result/run. */
export function calculateMasteryAwards(classStats, { won, stage = 1 } = {}) {
  if (typeof won !== 'boolean' || !classStats || typeof classStats !== 'object') return [];
  const awards = [];
  for (const classId of Object.keys(CLASSES)) {
    const record = classStats[classId];
    const deployments = Math.min(40, Math.max(0, Number.isFinite(record?.deployments) ? Math.floor(record.deployments) : 0));
    if (!deployments) continue;
    const casts = Math.min(18, Math.max(0, Number.isFinite(record?.casts) ? Math.floor(record.casts) : 0));
    const xp = 8 + Math.min(20, deployments * 2) + casts + (won ? 10 : 0) + Math.min(6, Math.max(0, Math.floor((Number.isFinite(stage) ? stage : 1) / 24)));
    awards.push({ classId, xp });
  }
  return awards;
}
export function applyMasteryAwards(mastery, awards) {
  const result = normalizeMastery(mastery);
  if (!Array.isArray(awards)) return result;
  const seen = new Set();
  for (const award of awards) {
    if (!award || !Object.prototype.hasOwnProperty.call(result, award.classId) || seen.has(award.classId)) continue;
    seen.add(award.classId);
    result[award.classId].xp = boundedXp(result[award.classId].xp + boundedXp(award.xp));
  }
  return result;
}
export function selectSpecialization(mastery, classId, id) {
  const result = normalizeMastery(mastery);
  if (!Object.prototype.hasOwnProperty.call(result, classId) || masteryRank(result[classId].xp) < SPECIALIZATION_RANK) return result;
  if (id === null || specializationFor(classId, id)) result[classId].specialization = id;
  return result;
}
