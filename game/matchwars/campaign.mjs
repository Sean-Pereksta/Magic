/** Authored campaign data shared by the map, save limits, and enemy director. */
export const TOTAL_STAGES = 48;
const ELEMENT_KEYS = new Set(['ember', 'tide', 'grove', 'storm', 'sun', 'void']);
const BOSS_KEYS = new Set(['rootking', 'frostqueen', 'eclipselord']);

const chapters = [
  {
    title: 'THE AWAKENING',
    description: 'Learn to read the front and drive the first invaders from the forest border.',
    theme: 'forest', color: '#91be91', elements: ['ember', 'grove', 'tide'],
    bossKey: 'rootking', bossName: 'The Root King',
    stages: [
      ['The Verdant Gate', 'Meet the raiders at the forest border.'],
      ['River of Glass', 'Frost snipers gather by the river.'],
      ['The Tangled Watch', 'Break the healers behind their living shields.'],
      ['The Root King', 'A regenerating monarch holds the middle lane.'],
    ],
  },
  {
    title: 'THE FROSTBOUND MARCH',
    description: 'Cross the high passes, challenge shielded formations, and weather the frozen court.',
    theme: 'frost', color: '#9ccddd', elements: ['tide', 'storm', 'sun'],
    bossKey: 'frostqueen', bossName: 'Frostbound Queen',
    stages: [
      ['Stormbreak Ridge', 'Bring lightning hunters down from the skies.'],
      ['The Golden Bastion', 'Crack the sun guards with Void Reapers.'],
      ['A Hollow Promise', 'Summoners hide behind their shades.'],
      ['Frostbound Queen', 'A frost volley threatens every formation.'],
    ],
  },
  {
    title: 'THE ECLIPSE',
    description: 'Balance your army against mixed elements and break the first great rift.',
    theme: 'void', color: '#bc9edb', elements: ['void', 'tide', 'ember'],
    bossKey: 'eclipselord', bossName: 'The Eclipse Lord',
    stages: [
      ['Ashes at Dawn', 'Fire, forest, and storm collide.'],
      ['Prismfall', 'Ranged-resistant sentinels demand a new plan.'],
      ['The Last Lantern', 'Keep your healers alive through the darkness.'],
      ['The Eclipse Lord', 'Break the ward. End the invasion.'],
    ],
  },
  {
    title: 'THE IRONWOOD SIEGE',
    description: 'Beyond the first rift, fortified healers shelter behind a living siege line.',
    theme: 'forest', color: '#b1c28d', elements: ['grove', 'sun', 'ember'],
    bossKey: 'rootking', bossName: 'Ironbark Regent',
    stages: [
      ['The Second Root', 'Armored Grove troops return. Burn their frontline before its support takes hold.'],
      ['Sentry Orchard', 'Sun shields protect the healers. Bring Void damage through the golden wall.'],
      ['The Living Rampart', 'An Ember flank punishes a pure Grove army. Split your counters across the lanes.'],
      ['Ironbark Regent', 'A regenerating siege lord commands the center. Sustain your tanks while burn eats through the guard.'],
    ],
  },
  {
    title: 'THE THUNDER ROAD',
    description: 'Fast raiders and flying hunters turn the open road into a test of lane discipline.',
    theme: 'storm', color: '#d9c27d', elements: ['storm', 'tide', 'ember'],
    bossKey: 'frostqueen', bossName: 'Tempest Matriarch',
    stages: [
      ['Gale Toll', 'Flying skirmishers rush the upper road. Field Storm hunters before the line is crossed.'],
      ['Three Broken Bridges', 'Tide slows your response while Ember raiders press another lane. Summon before the fronts split.'],
      ['The Runner’s Mile', 'Swift siege runners punish an empty lane. Save Rally for the formation that must catch them.'],
      ['Tempest Matriarch', 'A stormbound frost caster freezes the central road. Keep ranged support behind a durable screen.'],
    ],
  },
  {
    title: 'THE SUNKEN CROWN',
    description: 'A drowned court combines frost, life stealing, and shields to outlast careless attacks.',
    theme: 'frost', color: '#86c0c7', elements: ['tide', 'void', 'sun'],
    bossKey: 'frostqueen', bossName: 'The Drowned Oracle',
    stages: [
      ['Drowned Causeway', 'Frost snipers cover shield bearers. A Storm spearhead can open the flooded crossing.'],
      ['The Borrowed Heart', 'Void shades heal through combat. Focus wounded targets before their stolen life restores them.'],
      ['Pearlglass Battery', 'Sun sentinels resist ranged fire. Send Reapers into melee while Rangers hold the other lanes.'],
      ['The Drowned Oracle', 'The oracle’s frost guard hides a draining flank. Break the center without abandoning the shore.'],
    ],
  },
  {
    title: 'THE ASHEN CONCORD',
    description: 'Burning siege engines march beside shielded healers in an uneasy elemental alliance.',
    theme: 'ash', color: '#dd9b7c', elements: ['ember', 'sun', 'grove'],
    bossKey: 'rootking', bossName: 'Cinderheart Colossus',
    stages: [
      ['Ember Treaty', 'Furnace Brutes lead the assault. Tide slows their march while your frontline absorbs the first blows.'],
      ['The Hospice of Ash', 'Grove healers restore Sun shields. Counter each layer instead of flooding one element.'],
      ['Cinder Crossfire', 'Splash casters threaten clustered support. Establish pressure in all three lanes before the siege closes.'],
      ['Cinderheart Colossus', 'A living furnace renews its wounds behind burning guards. Chain empowered summons into a sustained assault.'],
    ],
  },
  {
    title: 'THE PRISM CRUSADE',
    description: 'The bright citadels weaponize shields, ranged resistance, and attacks from above.',
    theme: 'sun', color: '#e4cc91', elements: ['sun', 'void', 'storm'],
    bossKey: 'eclipselord', bossName: 'The Prism Tyrant',
    stages: [
      ['The Gilded Breach', 'Prism Sentinels anchor the gates. Void melee troops turn their ranged resistance against them.'],
      ['Shadow in the Halo', 'Shield breakers stalk the paladins. Healing and fresh reinforcements must replace a shield-only defense.'],
      ['The Aerial Procession', 'Storm flyers cover the crusade. Mix anti-air strikes with the counters needed on the ground.'],
      ['The Prism Tyrant', 'A radiant summoner hides inside a ward. Clear its reinforcements while a focused force breaks the throne.'],
    ],
  },
  {
    title: 'THE WILD HUNT',
    description: 'Regenerating beasts, poison hunters, and summoners turn the deep forest into a war of endurance.',
    theme: 'forest', color: '#89b89d', elements: ['grove', 'storm', 'void'],
    bossKey: 'rootking', bossName: 'The Briar Sovereign',
    stages: [
      ['Thornwake', 'Poison hunters wear down isolated soldiers. Build a supported frontline before committing your damage dealers.'],
      ['The Pack Divides', 'Storm runners and Grove tanks demand opposite answers. Keep Ember and Grove matches for different lanes.'],
      ['Hollow Nursery', 'Dusk Conjurers replenish the hunt. Reach their backline before another wave of shades can gather.'],
      ['The Briar Sovereign', 'Regeneration and poison protect the oldest root. Time your damage burst after the supporting hunters fall.'],
    ],
  },
  {
    title: 'THE NIGHTGLASS FRONT',
    description: 'The rift’s outer fortress layers frost control over burning and life-stealing assaults.',
    theme: 'void', color: '#b49ad1', elements: ['void', 'tide', 'ember'],
    bossKey: 'eclipselord', bossName: 'The Nightglass Regent',
    stages: [
      ['A Mirror Without Dawn', 'Void duelists wait behind frost fire. Use Sun pressure without surrendering your answer to Tide.'],
      ['The Slow Inferno', 'Frost snipers pin your troops under Ember splash. Renew the threatened lane and send fresh counters.'],
      ['The Summoner’s Stair', 'Successive conjurers turn delay into danger. Forge a special rune before the staircase fills.'],
      ['The Nightglass Regent', 'A life-stealing summoner commands a mixed guard. Spend spells to finish wounded threats before they recover.'],
    ],
  },
  {
    title: 'THE SHATTERED SKY',
    description: 'Skyborne raids and fortified frost formations challenge every part of your army.',
    theme: 'storm', color: '#c7c6a1', elements: ['storm', 'sun', 'tide'],
    bossKey: 'frostqueen', bossName: 'The Skybreaker',
    stages: [
      ['Cloudfall Outpost', 'Flying hunters advance beside Sun guards. Send Storm for the sky and Void for the shielded line.'],
      ['The Frozen Standard', 'Frost armor shelters the rear ranks. Build a counterattack that can survive the long approach.'],
      ['The Split Horizon', 'Fast raids strike the outer lanes while the center fortifies. Commit each combo where the forecast needs it.'],
      ['The Skybreaker', 'A frost sovereign commands the shattered heights. Hold both flanks while your strongest formation ends the volley.'],
    ],
  },
  {
    title: 'THE LAST RIFT',
    description: 'The final frontier combines siege pressure, support networks, and the rift’s deepest darkness.',
    theme: 'void', color: '#c4a0c8', elements: ['void', 'ember', 'grove'],
    bossKey: 'eclipselord', bossName: 'The Runeless Emperor',
    stages: [
      ['Beyond the Last Lantern', 'Burning brutes screen the final shades. Build dual counters and keep a reserve for the weakest lane.'],
      ['The Verdant Pyre', 'Grove healers and Ember siege troops hold the same front. Rotate your damage as each layer gives way.'],
      ['The Unmaking Gate', 'Summoners sustain a three-lane assault. Bank a chained special and spend Overdrive on the decisive reinforcements.'],
      ['The Runeless Emperor', 'The last ward binds a draining emperor to its guards. Break the supporting army, then commit every element to the final rift.'],
    ],
  },
];

// Fail loudly if authored data drifts away from the shared campaign contract.
if (chapters.length * 4 !== TOTAL_STAGES || chapters.some(chapter =>
  chapter.stages.length !== 4 || chapter.elements.length !== 3 ||
  new Set(chapter.elements).size !== 3 || chapter.elements.some(key => !ELEMENT_KEYS.has(key)) ||
  !BOSS_KEYS.has(chapter.bossKey) || !/^#[\da-f]{6}$/i.test(chapter.color))) {
  throw new Error('Invalid Match Wars campaign configuration.');
}

export const CHAPTERS = Object.freeze(chapters.map(({ stages, ...chapter }, index) => Object.freeze({
  number: index + 1,
  ...chapter,
  elements: Object.freeze([...chapter.elements]),
})));

export const STAGES = Object.freeze(chapters.flatMap(chapter => chapter.stages.map(([name, description]) =>
  Object.freeze([name, description, chapter.title]))));

function stageNumber(stage) {
  return typeof stage === 'number' && Number.isFinite(stage) ? Math.min(TOTAL_STAGES, Math.max(1, Math.floor(stage))) : 1;
}

export function getChapter(stage = 1) {
  return CHAPTERS[Math.floor((stageNumber(stage) - 1) / 4)];
}

/** Presentation-safe stage metadata; only every fourth battle has a boss. */
export function getStage(stage = 1) {
  const number = stageNumber(stage);
  const [name, description] = STAGES[number - 1];
  const chapter = getChapter(number);
  return Object.freeze({ number, name, description, chapter, boss: number % 4 === 0, bossKey: chapter.bossKey, bossName: chapter.bossName });
}
