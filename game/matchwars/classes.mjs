/** Pure class and formation metadata. No combat, browser, or persistence dependency. */
import { TOTAL_STAGES } from './campaign.mjs?v=3';
const ELEMENTS = ['ember', 'tide', 'grove', 'storm', 'sun', 'void'];
const COLORS = ['#ff805e', '#61c7f4', '#83d18b', '#e7c76d', '#ffe3a0', '#b79aef'];
const ICONS = ['flame', 'drop', 'leaf', 'bolt', 'sun', 'moon'];
const UNLOCK_LEVELS = { 'ember-knight': 4, cryomancer: 8, 'bloom-druid': 12, 'tempest-mage': 16, 'dawn-priest': 20, necromancer: 24,
  'phoenix-adept': 32, 'tide-oracle': 40, 'thorn-guardian': 48, windcaller: 60, 'sun-crusader': 72, hexweaver: 80 };
const spell = (id, name, description, cooldown, type) => Object.freeze({ id, name, description, cooldown, type });
const make = (id, key, name, role, description, stats, ability, traits = {}, column = null) => Object.freeze({
  id, key, element: key, name, role, description, unlockLevel: UNLOCK_LEVELS[id] || 1, color: COLORS[ELEMENTS.indexOf(key)], icon: ICONS[ELEMENTS.indexOf(key)],
  ...stats, spell: ability, ...traits, spriteAtlas: column === null ? 'troops' : 'classes',
  spriteRow: ELEMENTS.indexOf(key), spriteColumn: column || 0,
});

export const CLASSES = Object.freeze({
  'ember-mage': make('ember-mage', 'ember', 'Ember Mage', 'Burn / splash', 'Fireballs burn clustered enemies. Firewave scorches the front every seven seconds.',
    { hp: 70, attack: 15, speed: 36, range: 190, interval: 1.55 }, spell('firewave', 'Firewave', 'Burn up to three nearby enemies in a wave of flame.', 7, 'firewave'), { burn: true, splash: 48 }),
  'ember-knight': make('ember-knight', 'ember', 'Ember Knight', 'Frontline / charge', 'A durable fire warrior that charges a nearby enemy and forges its own shield.',
    { hp: 142, attack: 15, speed: 36, range: 36, interval: 1.45 }, spell('cinder-charge', 'Cinder Charge', 'Charge a nearby foe, strike hard, and gain a shield.', 9, 'firewave'), { burn: true, armor: .12 }, 0),
  'phoenix-adept': make('phoenix-adept', 'ember', 'Phoenix Adept', 'Rebirth / flame support', 'Flame magic restores wounded allies. The adept revives itself once per life.',
    { hp: 66, attack: 11, speed: 40, range: 175, interval: 1.55 }, spell('phoenix-flare', 'Phoenix Flare', 'Burn nearby foes and mend the most wounded ally. Rebirth saves this caster once.', 10, 'radiance'), { burn: true, rebirth: true }, 4),
  'tide-ranger': make('tide-ranger', 'tide', 'Tide Ranger', 'Ranged / slow', 'Long-range frost arrows slow threats. Piercing Frost strikes two enemies in formation.',
    { hp: 62, attack: 13, speed: 39, range: 245, interval: 1.3 }, spell('piercing-frost', 'Piercing Frost', 'Strike and slow two enemies with a piercing frost arrow.', 8, 'frostnova'), { slow: true }),
  cryomancer: make('cryomancer', 'tide', 'Cryomancer', 'Freeze / control', 'A control mage that freezes groups, setting up devastating Storm shatters.',
    { hp: 76, attack: 10, speed: 32, range: 170, interval: 1.7 }, spell('frostnova', 'Frost Nova', 'Damage up to three foes and freeze them briefly.', 10, 'frostnova'), { slow: true }, 0),
  'tide-oracle': make('tide-oracle', 'tide', 'Tide Oracle', 'Cleanse / healing', 'Restorative tides cleanse damage over time and heal the two allies who need it most.',
    { hp: 82, attack: 8, speed: 34, range: 195, interval: 1.6 }, spell('restorative-tide', 'Restorative Tide', 'Heal two wounded allies and wash away burn, poison, and curses.', 8, 'healbeam'), {}, 4),
  'grove-warden': make('grove-warden', 'grove', 'Grove Warden', 'Heal / poison', 'A dependable support that heals nearby allies, poisons enemies, and binds them with roots.',
    { hp: 100, attack: 9, speed: 31, range: 125, interval: 1.6 }, spell('living-roots', 'Living Roots', 'Root a nearby enemy and heal the most wounded ally.', 9, 'vines'), { poison: true, passiveHeal: true }),
  'bloom-druid': make('bloom-druid', 'grove', 'Bloom Druid', 'Regeneration / support', 'A dedicated healer that plants renewal on three allies. Gravebloom restores undead at full strength.',
    { hp: 80, attack: 7, speed: 32, range: 180, interval: 1.7 }, spell('renewal-garden', 'Renewal Garden', 'Heal three allies and grant five seconds of regeneration.', 9, 'healbeam'), {}, 0),
  'thorn-guardian': make('thorn-guardian', 'grove', 'Thorn Guardian', 'Tank / roots', 'A living bulwark that shields itself, roots attackers, and reflects a portion of melee damage.',
    { hp: 180, attack: 10, speed: 26, range: 35, interval: 1.7 }, spell('thicket-bulwark', 'Thicket Bulwark', 'Gain a strong shield and bind two nearby foes.', 10, 'vines'), { armor: .18, poison: true, reflect: .12 }, 4),
  'storm-lancer': make('storm-lancer', 'storm', 'Storm Lancer', 'Chain / anti-air', 'A fast lightning fighter that chains attacks and punctures flying enemies.',
    { hp: 90, attack: 17, speed: 56, range: 38, interval: 1.05 }, spell('chain-lance', 'Chain Lance', 'Discharge lightning through up to three nearby foes.', 8, 'lightningstorm'), { chain: true }),
  'tempest-mage': make('tempest-mage', 'storm', 'Tempest Mage', 'Lightning / area damage', 'Ranged storms punish groups and shatter frozen targets.',
    { hp: 68, attack: 12, speed: 34, range: 205, interval: 1.65 }, spell('thunderstorm', 'Thunderstorm', 'Call lightning onto three enemies, with extra damage against controlled foes.', 10, 'lightningstorm'), { chain: true }, 0),
  windcaller: make('windcaller', 'storm', 'Windcaller', 'Haste / displacement', 'Favorable winds hasten nearby allies and push enemies away from your line.',
    { hp: 76, attack: 8, speed: 43, range: 180, interval: 1.5 }, spell('zephyr-aura', 'Zephyr Aura', 'Hasten up to three allies and push back two enemies.', 9, 'whirlwind'), {}, 4),
  'sun-paladin': make('sun-paladin', 'sun', 'Sun Paladin', 'Tank / shield', 'A shielded, armored frontline that protects the formation with Dawn Barrier.',
    { hp: 155, attack: 12, speed: 29, range: 34, interval: 1.5 }, spell('dawn-barrier', 'Dawn Barrier', 'Shield and briefly fortify two nearby allies.', 9, 'blessing'), { armor: .15, shield: 22, shieldOnSpawn: true }),
  'dawn-priest': make('dawn-priest', 'sun', 'Dawn Priest', 'Heal / purge undead', 'Radiance heals and cleanses allies, while searing enemy undead and Void creatures.',
    { hp: 80, attack: 7, speed: 31, range: 185, interval: 1.7 }, spell('cleansing-radiance', 'Cleansing Radiance', 'Heal and cleanse three allies, shield one, and purge nearby undead.', 8, 'radiance'), { shield: 10 }, 0),
  'sun-crusader': make('sun-crusader', 'sun', 'Sun Crusader', 'Assault / judgment', 'An aggressive holy warrior whose verdict breaks shields and punishes undead.',
    { hp: 128, attack: 19, speed: 37, range: 40, interval: 1.4 }, spell('solar-verdict', 'Solar Verdict', 'Strike with a shield-breaking verdict. Undead take double spell damage.', 10, 'radiance'), { armor: .08, shieldBreak: true }, 4),
  'void-reaper': make('void-reaper', 'void', 'Void Reaper', 'Drain / execute', 'Steals life and finishes wounded targets with Soul Harvest.',
    { hp: 100, attack: 18, speed: 42, range: 40, interval: 1.3 }, spell('soul-harvest', 'Soul Harvest', 'Drain a target, with a large bonus against wounded enemies.', 8, 'voidrift'), { lifesteal: .28, execute: true }),
  necromancer: make('necromancer', 'void', 'Necromancer', 'Corpses / bone guards', 'Consumes fresh battlefield corpses to raise temporary bone guards. Raised troops leave no corpses or bounty.',
    { hp: 72, attack: 8, speed: 31, range: 205, interval: 1.75 }, spell('raise-boneguard', 'Raise Bone Guard', 'Consume one nearby corpse to raise a temporary guard. Each caster can raise five.', 7, 'resurrect'), { lifesteal: .1, raiseLimit: 5 }, 0),
  hexweaver: make('hexweaver', 'void', 'Hexweaver', 'Curse / weakness', 'Unravels enemy strength with curses that reduce attack and amplify incoming damage.',
    { hp: 78, attack: 10, speed: 33, range: 190, interval: 1.6 }, spell('unravel-curse', 'Unraveling Curse', 'Curse two enemies, reducing their attack and increasing damage taken.', 9, 'curse'), { lifesteal: .1 }, 4),
});

export const DEFAULT_LOADOUT = Object.freeze({ ember: 'ember-mage', tide: 'tide-ranger', grove: 'grove-warden', storm: 'storm-lancer', sun: 'sun-paladin', void: 'void-reaper' });
export const CLASS_CHOICES = Object.freeze(Object.fromEntries(ELEMENTS.map(key => [key, Object.freeze(Object.values(CLASSES).filter(def => def.key === key))])));

/** Accept only a class belonging to that element; unknown/prototype keys never survive. */
export function isClassUnlocked(classId, highestUnlockedStage) {
  const definition = Object.prototype.hasOwnProperty.call(CLASSES, classId) ? CLASSES[classId] : null;
  return !!definition && typeof highestUnlockedStage === 'number' && Number.isFinite(highestUnlockedStage) && highestUnlockedStage >= definition.unlockLevel;
}

export function normalizeLoadout(input, highestUnlockedStage = TOTAL_STAGES) {
  const source = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  return Object.fromEntries(ELEMENTS.map(key => {
    const id = Object.prototype.hasOwnProperty.call(source, key) ? source[key] : null;
    return [key, typeof id === 'string' && CLASSES[id]?.key === key && isClassUnlocked(id, highestUnlockedStage) ? id : DEFAULT_LOADOUT[key]];
  }));
}

export function getTroop(key, classId) {
  if (!Object.prototype.hasOwnProperty.call(DEFAULT_LOADOUT, key)) return null;
  return CLASSES[classId]?.key === key ? CLASSES[classId] : CLASSES[DEFAULT_LOADOUT[key]];
}

const combo = (id, name, elements, description) => Object.freeze({ id, name, elements: Object.freeze(elements), description, condition: 'Both elements must have living allied troops in the same lane.' });
export const COMBOS = Object.freeze([
  combo('verdant-covenant', 'Verdant Covenant', ['grove', 'sun'], 'Allied healing and new shields in this lane are 20% stronger.'),
  combo('shatter', 'Shatter', ['tide', 'storm'], 'Storm hits slowed, rooted, or frozen enemies for 25% more damage.'),
  combo('soulfire', 'Soulfire', ['ember', 'void'], 'Void hits burning enemies for 25% more damage.'),
  combo('gravebloom', 'Gravebloom', ['grove', 'void'], 'Undead receive full healing and newly raised guards last four seconds longer.'),
  combo('thunderous-aegis', 'Thunderous Aegis', ['sun', 'storm'], 'Shielded allies attack 12% faster and move 10% faster.'),
  combo('steamguard', 'Steamguard', ['ember', 'tide'], 'Burning enemies deal 15% less attack damage, and new allied shields are 10% stronger.'),
]);
