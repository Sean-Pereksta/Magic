/** Authored campaign data shared by the map, save limits, and enemy director. */
export const TOTAL_STAGES = 84;
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
  {
    title: 'THE STARLESS VEIL',
    description: 'The emperor’s fall reveals a deeper darkness, where frost and lightning shelter the last cults.',
    theme: 'void', color: '#aa97ce', elements: ['void', 'tide', 'storm'],
    bossKey: 'eclipselord', bossName: 'The Starless Hierophant',
    stages: [
      ['The Unlit Shore', 'Shades cross under a curtain of frost. Break their draining frontline before the ranged cult gathers.'],
      ['Thunder Without Dawn', 'Flying hunters guard the cold road. Reserve anti-air troops while your Sun line holds the shadows.'],
      ['The Veiled Conclave', 'Summoners shelter beside frost support. Build an elemental bond before the outer lanes fill.'],
      ['The Starless Hierophant', 'The hidden cult binds its lord to an endless host. Break the support network and turn a cascade into the decisive push.'],
    ],
  },
  {
    title: 'THE VERDANT RETURN',
    description: 'Life returns to the ruined frontier, but the reborn forest answers to a new and restless crown.',
    theme: 'forest', color: '#add19d', elements: ['grove', 'ember', 'tide'],
    bossKey: 'rootking', bossName: 'The Dawnbloom Ancient',
    stages: [
      ['Seeds in the Ruins', 'Grove defenders reclaim the fallen gates. Layer burns over the living wall while your healers keep pace.'],
      ['The Blooming Torrent', 'Frost holds your front beneath a canopy of healers. Send fresh counters into the lane that can reach their support.'],
      ['The Orchard Aflame', 'Ember raiders threaten both flanks while the center regenerates. Divide your matched elements before committing a larger combo.'],
      ['The Dawnbloom Ancient', 'The awakened forest renews its ancient guardian. Sustain a long formation and spend your strongest bond against its recovery.'],
    ],
  },
  {
    title: 'THE ASHEN REBIRTH',
    description: 'From the old siege fields rises a phoenix court, forging radiant shields around a burning army.',
    theme: 'ash', color: '#e8a57f', elements: ['ember', 'sun', 'void'],
    bossKey: 'rootking', bossName: 'The Phoenix Regent',
    stages: [
      ['A Crown of Cinders', 'Burning siege troops return beneath Sun shields. Tide and Void pressure must break different layers of the same assault.'],
      ['The Hollow Sun', 'Void duelists hunt your shield bearers. Pair healing with renewed armor rather than relying on a single defense.'],
      ['Flight of the Furnace', 'The radiant guard advances behind concentrated fire. Keep your support alive until a line blast can open the formation.'],
      ['The Phoenix Regent', 'A reborn guardian leads the phoenix court. Chain your assault through its escort before the burning center overwhelms the keep.'],
    ],
  },
  {
    title: 'THE ASCENDANT COALITION',
    description: 'The surviving kingdoms unite their shields, healers, and sky hunters under a single war standard.',
    theme: 'sun', color: '#ead59a', elements: ['sun', 'storm', 'grove'],
    bossKey: 'frostqueen', bossName: 'The Ascendant Marshal',
    stages: [
      ['The Threefold Standard', 'Sun guards, Grove support, and Storm raiders attack together. Give every lane a counter and a protected backline.'],
      ['The Golden Canopy', 'Healers work behind the coalition shields. Use sustained melee pressure to expose the support your ranged troops cannot reach.'],
      ['The Highwind Accord', 'Sky hunters race ahead of the fortified center. Answer the early flanks without spending the force needed for the main line.'],
      ['The Ascendant Marshal', 'The coalition’s commander shelters behind a layered guard. Rotate your class skills and bonds until the central standard falls.'],
    ],
  },
  {
    title: 'THE SUNDERED HEAVENS',
    description: 'The broken sky rains frost and darkness onto the roads that lead to the world’s final ward.',
    theme: 'frost', color: '#abc9e1', elements: ['tide', 'storm', 'void'],
    bossKey: 'frostqueen', bossName: 'The Winterstar Empress',
    stages: [
      ['The Falling Firmament', 'Frost armor anchors a skyborne assault. Reach the snipers while your anti-air fighters hold the upper road.'],
      ['The Pale Meridian', 'Void support gathers behind lightning hunters. A single-element army cannot protect every front of the frozen crossing.'],
      ['The Last Constellation', 'A relentless mixed host approaches the final ward. Build your last class talents into a formation that can survive repeated waves.'],
      ['The Winterstar Empress', 'The empress commands the cold beneath the shattered stars. Keep both flanks alive while a bonded army breaks her frost court.'],
    ],
  },
  {
    title: 'THE WORLDWOUND',
    description: 'The broken horizon exposes an avatar that binds the burning and radiant hosts to the darkness.',
    theme: 'void', color: '#d0a0c0', elements: ['void', 'ember', 'sun'],
    bossKey: 'eclipselord', bossName: 'The Runebound Avatar',
    stages: [
      ['At the Edge of Everything', 'Burning siege troops and radiant guards defend the last approach. Split your counters before the shadow army arrives.'],
      ['The War of Two Dawns', 'Sun and Void pressure trade lethal blows while Ember punishes crowded support. Keep every element’s role distinct.'],
      ['The Final Alignment', 'The rift’s last armies fill all three roads. Forge your strongest special chain and preserve the bonds that carry your frontline.'],
      ['The Runebound Avatar', 'The worldwound binds an avatar to the invading host. Break its combined army to reveal the road beyond the shattered horizon.'],
    ],
  },
  {
    title: 'THE UNBOUND DAWN',
    description: 'Beyond the broken avatar, the surviving orders build a last alliance against the rift’s immortal armies.',
    theme: 'sun', color: '#e9d3a4', elements: ['sun', 'grove', 'storm'],
    bossKey: 'rootking', bossName: 'The Dawnsworn Titan',
    stages: [
      ['The Free Standard', 'Radiant guardians hold the new road while sky hunters strike the flanks. Send distinct class formations to each threat.'],
      ['The Garden of Banners', 'Grove support sustains the armored coalition. Bring enough pressure to break the healers before their shields recover.'],
      ['The Bright Rebellion', 'A fast aerial vanguard conceals a living siege line. Preserve the bonds that let your fragile attackers survive the crossing.'],
      ['The Dawnsworn Titan', 'The alliance’s ancient sentinel bars the radiant gate. Overcome its layered guard without losing the support that carries your army.'],
    ],
  },
  {
    title: 'THE RUNEFALL MARCH',
    description: 'Falling runes ignite the frozen siege roads, forcing the army through the rift’s final elemental furnace.',
    theme: 'ash', color: '#dca38d', elements: ['ember', 'tide', 'sun'],
    bossKey: 'frostqueen', bossName: 'The Runefall Sovereign',
    stages: [
      ['The Burning Meridian', 'Ember siege troops advance under frost cover. Rotate your counters as the pressure moves between lanes.'],
      ['The Sunken Furnace', 'Frost snipers shelter behind radiant armor. Build a mixed force that can reach the support without stalling at the shields.'],
      ['The Runes That Remain', 'The last siege column marches through ash and ice. Assemble your final class talents before the sovereign takes the field.'],
      ['The Runefall Sovereign', 'A frost monarch commands the falling stars. Keep each lane alive long enough for your strongest elemental bond to break the siege.'],
    ],
  },
  {
    title: 'THE WORLD’S LAST WARD',
    description: 'The final road leads to the power behind every rift, guarded by darkness, fire, and a sky of broken runes.',
    theme: 'void', color: '#cea0d4', elements: ['void', 'ember', 'storm'],
    bossKey: 'eclipselord', bossName: 'The Worldbreaker',
    stages: [
      ['The Road Without Return', 'Void summoners replenish a burning vanguard while Storm hunters threaten the rear. Commit every lane with a clear purpose.'],
      ['The Last Elemental War', 'The strongest invading formations answer the final call. Adapt your classes and preserve a counter for each fresh assault.'],
      ['The World Before Dawn', 'All roads converge before the final ward. Build a decisive special chain and protect the bonded troops that must cross it.'],
      ['The Worldbreaker', 'Choose your army from all eighteen classes, defeat the combined host, and unite your strongest bonds against the architect of every invasion.'],
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
