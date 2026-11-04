/* IRONFIELD — unit roster, battlefields, and repeatable scenarios.
 * Combat values are fictional game balance, measured in world units / seconds.
 * Historical scenarios are sourced, schematic adaptations of opening layouts.
 * No network access or third-party dependencies are needed.
 */
(function (global) {
  'use strict';

  const clamp = (value, min, max) => Math.max(min, Math.min(max, value));
  function RNG(seed) {
    let state = (Number(seed) >>> 0) || 1;
    const next = function () {
      state += 0x6D2B79F5;
      let t = state;
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
    next.next = next;
    next.int = (min, max) => Math.floor(next() * (max - min + 1)) + min;
    next.range = (min, max) => min + next() * (max - min);
    next.pick = (items) => items[Math.floor(next() * items.length)];
    return next;
  }

  const ARMOR_CLASSES = Object.freeze({
    light: Object.freeze({ id: 'light', label: 'Light', description: 'Cloth, leather, or limited protection. Arrows, sling stones, and fast cutting weapons are effective.' }),
    medium: Object.freeze({ id: 'medium', label: 'Medium', description: 'Mail, bronze protection, or reinforced equipment. A balance of protection and mobility.' }),
    heavy: Object.freeze({ id: 'heavy', label: 'Heavy', description: 'Plate, substantial barding, or a large armored body. Piercing bolts and poleaxes gain an advantage.' })
  });
  const profile = (label, light, medium, heavy) => Object.freeze({ label, multipliers: Object.freeze({ light, medium, heavy }) });
  const ATTACK_PROFILES = Object.freeze({
    spear: profile('Spear thrust', 1.08, 1, .90),
    sword: profile('Cutting blade', 1.08, 1, .88),
    pike: profile('Long pike', 1.04, 1, .92),
    poleaxe: profile('Piercing poleaxe', 1, 1.08, 1.18),
    club: profile('War club', 1.10, 1.04, .95),
    musket: profile('Musket ball', 1, 1.08, 1.16),
    shot: profile('Round shot', 1.10, 1.10, 1.16),
    arrow: profile('Arrow', 1.15, .98, .82),
    bolt: profile('Crossbow bolt', 1, 1.08, 1.18),
    javelin: profile('Throwing spear', 1.10, 1.04, .96),
    sling: profile('Sling stone', 1.12, .95, .78),
    lance: profile('Cavalry lance', 1.08, 1.06, .94),
    stone: profile('Siege stone', 1, 1.04, 1.08),
    heavyBolt: profile('Ballista bolt', 1, 1.10, 1.18),
    crush: profile('Crushing impact', 1.12, 1.06, .96)
  });
  const matchups = (strong, weak, roleNote) => Object.freeze({
    strongAgainst: Object.freeze(strong.map(([type, reason]) => Object.freeze({ type, reason }))),
    weakAgainst: Object.freeze(weak.map(([type, reason]) => Object.freeze({ type, reason }))), roleNote
  });
  const tactics = (armorClass, attackProfile, mass, strong, weak, roleNote) => Object.freeze({ armorClass, attackProfile, mass, matchups: matchups(strong, weak, roleNote) });
  const UNIT_TACTICS = Object.freeze({
    militia: tactics('light', 'spear', .85,
      [['archer', 'Cheap spears can overrun unsupported bowmen if they reach them.'], ['horseArcher', 'An inexpensive spear screen protects the troops behind it from mounted harassment at close range.']],
      [['berserker', 'Sweeping clubs overwhelm lightly protected levies.'], ['archer', 'Bowmen outrange the spear screen and punish its light armor.']],
      'A low-cost screen. Its modest mounted bonus is not a substitute for a braced pike line.'),
    swordsman: tactics('medium', 'sword', 1.1,
      [['militia', 'Shields, mail, and better morale favor sustained close combat.'], ['archer', 'Once in contact, blades beat lightly armored bowmen.']],
      [['lancer', 'An unbraced line is vulnerable to a clean lance charge.'], ['crossbow', 'Piercing bolts bypass much of the mail protection.']],
      'Reliable line infantry; combine with pikes against cavalry and missiles against heavy defenders.'),
    pikeman: tactics('light', 'pike', 1,
      [['cavalry', 'A settled cohesive front intercepts charges and adds strong mounted damage.'], ['lancer', 'Set pikes blunt lance momentum before the riders reach the line.']],
      [['archer', 'Longbows outrange pikes and exploit their light armor.'], ['swordsman', 'Infantry and attacks from the flank bypass the mounted bonus and frontal brace.']],
      'Use line or shield wall, face the charge, and allow the ranks to settle. Moving, loose, or flanked pikes cannot fully brace.'),
    hoplite: tactics('medium', 'spear', 1.5,
      [['militia', 'A cohesive shield front withstands light spears and can press lighter bodies backward.'], ['swordsman', 'Supported frontal spear ranks can hold and exert limited pressure; flanked hoplites lose that advantage.']],
      [['crossbow', 'Piercing bolts exploit the slow advance and bypass bronze armor.'], ['halberdier', 'Poleaxe penetration threatens the shield line, especially from its sides.'], ['catapult', 'Packed ranks are vulnerable to long-range area damage.']],
      'A firm formation fighter. Shield pressure needs cohesive, supported ranks and space behind the opponent; heavy bodies and terrain can stop it.'),
    halberdier: tactics('medium', 'poleaxe', 1.35,
      [['guardian', 'Armor penetration and the heavy-armor modifier help against plate; avoid trading into an entire shielded front.'], ['cavalry', 'Long polearms add mounted damage and a limited frontal brace.']],
      [['archer', 'Slow polearms cannot answer sustained long-range volleys.'], ['berserker', 'Sweeping clubs can exploit exposed flanks and the slower polearm recovery.']],
      'An armored and piercing alternative to pikes. Pikes provide the stronger dedicated charge defense.'),
    guardian: tactics('heavy', 'sword', 1.9,
      [['militia', 'Plate and a tower shield absorb weak frontal attacks.'], ['slinger', 'Light sling stones have little effect against heavy armor.']],
      [['crossbow', 'High penetration and a heavy-armor damage bonus defeat much of the protection.'], ['halberdier', 'Piercing poleaxes remain effective against plate, particularly when the shield is bypassed.']],
      'An expensive defensive anchor, not a fast pursuer. Heavy mass also resists shield pressure.'),
    berserker: tactics('light', 'club', 1.05,
      [['militia', 'Rapid heavy blows punish lightly protected infantry.'], ['pikeman', 'Broad swings can reach several closely spaced pikemen.']],
      [['archer', 'Light armor takes increased arrow damage before contact.'], ['lancer', 'A clean mounted charge punishes the unshielded approach.']],
      'High damage after reaching the fight; screen the approach and attack disordered or exposed fronts.'),
    archer: tactics('light', 'arrow', .75,
      [['pikeman', 'Long range and a light-armor modifier punish stationary pike fronts.'], ['berserker', 'Arrows are effective against exposed, lightly armored shock infantry.']],
      [['cavalry', 'Fast riders can close the distance and defeat an exposed bow line.'], ['guardian', 'Heavy armor and frontal shields blunt ordinary arrows.']],
      'Long range and frequent volleys favor light targets; support with piercing weapons against heavy armor.'),
    musketeer: tactics('light', 'musket', 0.95,
      [['guardian', 'Musket balls pierce plate and push a Guardian back while it crosses the firing lane.']],
      [['cavalry', 'Cavalry can ride around the screen and reach the gun line during a reload.']],
      'Three disciplined ranks fire in rotating volleys. Each soldier takes 24 seconds to reload; protect the line from close combat.'),
    crossbow: tactics('medium', 'bolt', .95,
      [['guardian', 'High armor penetration and a heavy-armor modifier make bolts useful against plate.'], ['lancer', 'Piercing bolts threaten armored riders while an infantry screen keeps them at range.']],
      [['archer', 'Longbows outrange crossbows and fire more frequently.'], ['cavalry', 'The long reload leaves unsupported crews vulnerable to a close charge.']],
      'A deliberate armor counter. Its numerical modifier does not remove the need for distance and a protective screen.'),
    javelin: tactics('light', 'javelin', .8,
      [['militia', 'Strong short-range throws and quick movement punish light infantry.'], ['pikeman', 'Throwers can damage settled pikes without entering their melee reach.']],
      [['archer', 'Longbows outrange the throwing bands.'], ['cavalry', 'Riders can close the short throwing distance if no spear screen intervenes.']],
      'Mobile close-range support. Automatic bands reposition between throws; an explicit Charge occupies a firing band.'),
    slinger: tactics('light', 'sling', .7,
      [['militia', 'Inexpensive, frequent stones work well against lightly armored levies.'], ['berserker', 'Lightly protected shock infantry are susceptible while crossing open ground.']],
      [['guardian', 'Heavy armor strongly reduces the effect of small stones.'], ['cavalry', 'An exposed sling screen cannot withstand mounted contact.']],
      'Economical pressure against light troops. Cost efficiency does not make slings an armor-piercing weapon.'),
    cavalry: tactics('medium', 'sword', 3.8,
      [['archer', 'Speed and charge impact exploit unsupported bow lines.'], ['slinger', 'Riders can overrun exposed light missile troops.']],
      [['pikeman', 'A facing, settled pike formation punishes frontal charges.'], ['halberdier', 'Piercing polearms add damage against mounted bodies.']],
      'Use speed to find open flanks or isolated missile troops; avoid charging into set pikes.'),
    lancer: tactics('heavy', 'lance', 4.2,
      [['swordsman', 'A clean run-up creates a powerful hit against infantry without a pike brace.'], ['crossbow', 'An exposed, reloading bolt line is vulnerable if riders close the gap.']],
      [['pikeman', 'Set pikes absorb lance momentum and punish the impact.'], ['ballista', 'Heavy piercing bolts threaten expensive armored riders at long range.']],
      'Strongest on the first impact. Flank braced polearms and avoid becoming trapped in sustained melee.'),
    horseArcher: tactics('light', 'arrow', 3.1,
      [['pikeman', 'Mounted bowmen can harass a slow pike line without a frontal charge.'], ['militia', 'Speed and ranged fire exploit slow, lightly protected infantry.']],
      [['archer', 'Longbows outrange mounted bows and punish their light protection.'], ['cavalry', 'Fast melee riders are dangerous if they catch the skirmishers.']],
      'Mobile harassment against targets it can keep at distance, not a frontal cavalry substitute.'),
    cannon: tactics('medium', 'shot', 12,
      [['hoplite', 'One round shot can scatter several ranks of a tightly packed phalanx.']],
      [['cavalry', 'An exposed gun crew is vulnerable during its reload.']],
      'Low-flying round shot slams into packed ranks, scattering soldiers around the impact.'),
    trebuchet: tactics('medium', 'stone', 12,
      [['guardian', 'Slow shield lines are easy to range, and a single boulder can crush several files.']],
      [['lancer', 'Lancers can cover open ground and reach the crew before its next stone is ready.']],
      'A counterweight casts a massive boulder over the field. Crews take time to wind down the arm and load the next stone.'),
    catapult: tactics('medium', 'stone', 9,
      [['hoplite', 'Area impacts punish densely packed shield ranks.'], ['guardian', 'Long-range splash can pressure a slow defensive mass.']],
      [['cavalry', 'Flanking riders can reach engines during their long reload.'], ['berserker', 'Fast infantry destroy an unprotected battery once in contact.']],
      'Targets concentrations of troops; loose formations and mobile threats reduce its advantage.'),
    ballista: tactics('medium', 'heavyBolt', 9,
      [['elephant', 'Powerful, highly penetrating bolts threaten large armored beasts.'], ['lancer', 'Fire at expensive armored riders from behind a protective infantry screen.']],
      [['cavalry', 'Mobile riders can flank and overrun exposed engines.'], ['militia', 'Numerous cheap infantry can overwhelm an unprotected single-target battery.']],
      'Slow single-target fire against valuable armored bodies; it needs protection from cheap or fast attackers.'),
    siegeTower: tactics('heavy', 'arrow', 18,
      [['archer', 'A protected firing deck can support an assault while the tower approaches a defended wall.'], ['militia', 'The tower can establish a raised assault passage past a lightly defended wall.']],
      [['catapult', 'Heavy stones damage the large wooden structure before it can deploy.'], ['ballista', 'Piercing siege bolts threaten the slow, expensive tower.']],
      'An engineering unit: escort it to an enemy wall under Auto. It deploys a ramp after arriving; the captured passage then remains usable.'),
    batteringRam: tactics('heavy', 'crush', 16,
      [['militia', 'Protected crews can withstand light attacks while breaking a gate.'], ['archer', 'The armored roof reduces weak missile damage during the slow approach.']],
      [['catapult', 'Heavy stones punish a slow engine and its packed escort.'], ['halberdier', 'Piercing polearms can damage the engine if its infantry escort is defeated.']],
      'An engineering unit: Auto drives to a defended gate and repeatedly batters it at physical contact range. Keep infantry beside it.'),
    elephant: tactics('heavy', 'crush', 14,
      [['militia', 'Large health reserves and sweeping impacts punish tightly packed light infantry.'], ['swordsman', 'A clean impact and splash pressure a dense line without dedicated polearms.']],
      [['pikeman', 'Long pikes bring several ranks of mounted damage to bear on a large body.'], ['ballista', 'Heavy piercing bolts are effective against a valuable armored target.']],
      'Very expensive shock beasts. Their mass resists shield shoves, but does not make them immune to polearms or missiles.'),
    commander: tactics('heavy', 'sword', 4,
      [['militia', 'A commander can survive scattered light spear attacks while supporting nearby allies; this is not a cost-efficient duel.'], ['slinger', 'Heavy protection resists weak sling hits while the commander rallies the line.']],
      [['pikeman', 'Mounted commanders are vulnerable to the same set-pike defenses as other riders.'], ['ballista', 'Piercing long-range bolts threaten an exposed, valuable commander.']],
      'Its main counter to enemy pressure is allied morale support and rallying. Keep it protected rather than using it as a frontline fighter.')
  });

  const base = {
    hp: 90, damage: 16, armor: 3, speed: 47, range: 24,
    cooldown: 1.15, radius: 6, cost: 2, morale: 76,
    ranged: false, mounted: false, siege: false, commander: false, projectile: null,
    splash: 0, charge: 0, antiCavalry: 0, armorPierce: 0
  };
  const unit = (id, values) => Object.freeze(Object.assign({}, base, { id }, UNIT_TACTICS[id], values));

  const TYPES = Object.freeze({
    militia: unit('militia', {
      name: 'Levy Spearmen', shortName: 'Levies', role: 'Light infantry',
      description: 'A broad line of hastily raised spears and wooden shields. Cheap numbers to screen the troops behind them.',
      strengths: 'Low cost · wide battle lines · light cavalry screen',
      weakness: 'Low armor and morale; break under sustained pressure.',
      hp: 67, damage: 12, armor: 1, speed: 48, range: 29, cooldown: 1.35,
      cost: 1, morale: 57, antiCavalry: 4, color: '#a69266'
    }),
    swordsman: unit('swordsman', {
      name: 'Shield Swordsmen', shortName: 'Swordsmen', role: 'Line infantry',
      description: 'Mail-clad soldiers with round shields and steel blades. Reliable troops for a long, contested front.',
      strengths: 'Balanced armor, morale, and close combat',
      weakness: 'Vulnerable to a clean cavalry charge or concentrated crossbow fire.',
      hp: 104, damage: 19, armor: 5, speed: 47, range: 25, cooldown: 1.12,
      cost: 3, morale: 80, color: '#bcc6c7'
    }),
    pikeman: unit('pikeman', {
      name: 'Pike Guard', shortName: 'Pikemen', role: 'Anti cavalry',
      description: 'Long ash pikes reach past the front rank. Their formations are built to punish horses and blunt charges.',
      strengths: 'Long melee reach · bonus damage against mounted troops',
      weakness: 'Slow turns and light armor leave them exposed to arrows and infantry.',
      hp: 88, damage: 14, armor: 3, speed: 42, range: 49, cooldown: 1.34,
      radius: 6, cost: 3, morale: 79, antiCavalry: 24, color: '#d5c894',
      brace: Object.freeze({ label: 'Set pikes', settle: .55, minCohesion: 40, mountedDamage: .85, mountedDefense: .55, chargeReduction: .88, chargeReturn: .62 })
    }),
    hoplite: unit('hoplite', {
      name: 'Bronze Hoplites', shortName: 'Hoplites', role: 'Shield-and-spear ranks',
      description: 'Bronze-helmed spear infantry carry large round shields. Cohesive ranks brace their front and press lighter opponents through real contact.',
      strengths: 'Firm supported ranks · frontal shield defense · bounded shield pressure',
      weakness: 'Slow turns and advance; piercing missiles, flanks, and area fire threaten the dense formation.',
      hp: 128, damage: 20, armor: 6, speed: 37, range: 37, cooldown: 1.55,
      radius: 7, cost: 5, morale: 90, antiCavalry: 10, armorPierce: .12, color: '#c9a365',
      rankStiffness: .58, contactStand: .40,
      frontalShield: Object.freeze({ melee: .80, ranged: .84 }),
      brace: Object.freeze({ label: 'Shield brace', settle: .55, minCohesion: 40, mountedDamage: .25, mountedDefense: .12, chargeReduction: .45, chargeReturn: .18 }),
      push: Object.freeze({ speed: 32, reach: 8, minCohesion: 45, maxChain: 8 })
    }),
    halberdier: unit('halberdier', {
      name: 'Crown Halberdiers', shortName: 'Halberdiers', role: 'Armored polearms',
      description: 'Armored foot soldiers carry long halberds with axe blades and piercing points. They strengthen the battle line against cavalry and heavy infantry.',
      strengths: 'Armor penetration · anti-cavalry reach · disciplined heavy infantry',
      weakness: 'Slower than ordinary infantry and vulnerable to sustained missile fire.',
      hp: 126, damage: 26, armor: 7, speed: 38, range: 44, cooldown: 1.38,
      radius: 7, cost: 6, morale: 86, antiCavalry: 17, armorPierce: .60, color: '#a7b4a5',
      brace: Object.freeze({ label: 'Polearm brace', settle: .55, minCohesion: 40, mountedDamage: .15, mountedDefense: .12, chargeReduction: .45, chargeReturn: .22 })
    }),
    guardian: unit('guardian', {
      name: 'Iron Guardians', shortName: 'Guardians', role: 'Heavy infantry',
      description: 'Heavy plate, tower shields, and stubborn discipline. They anchor a defense and shelter supporting missile troops.',
      strengths: 'Very high armor and morale · holds narrow passages',
      weakness: 'Slow and expensive; armor-piercing weapons and flanks still hurt.',
      hp: 168, damage: 22, armor: 12, speed: 32, range: 25, cooldown: 1.45,
      radius: 7.5, cost: 7, morale: 95, color: '#e1d6a8'
    }),
    berserker: unit('berserker', {
      name: 'Berserkers', shortName: 'Berserkers', role: 'Shock infantry',
      description: 'Lightly armored warriors wield two-handed war clubs. Each broad swing drives nearby enemies back across their front.',
      strengths: 'Sweeping splash damage · knockback · fast approach',
      weakness: 'Little protection against missile volleys or a heavy charge.',
      hp: 108, damage: 34, armor: 1, speed: 59, range: 31, cooldown: 1.65,
      sweep: Object.freeze({ radius: 42, halfAngle: Math.PI * .40, secondaryDamage: .68, shove: 76 }),
      radius: 6.5, cost: 5, morale: 88, armorPierce: .3, color: '#d99661'
    }),
    archer: unit('archer', {
      missile: Object.freeze({ label: 'Bow line', description: 'Spread into a broad firing line, draw, loose, and hold useful long range.', standoff: .79, ranks: 4, spacing: 1.08, maxWidth: 680, breakRange: 65, reloadStep: 0, armorPreference: -.012 }),
      name: 'Longbow Rangers', shortName: 'Archers', role: 'Ranged infantry',
      description: 'Hooded bowmen loose arcing volleys over the battle line. Give them cover and enough room to work.',
      strengths: 'Long reach · rapid volleys · effective against light troops',
      weakness: 'Low armor and melee damage; cavalry can overrun an exposed line.',
      hp: 60, damage: 14, armor: 1, speed: 45, range: 465, cooldown: 1.7,
      radius: 5.5, cost: 3, morale: 65, ranged: true, projectile: 'arrow', color: '#82a276'
    }),
    musketeer: unit('musketeer', {
      name: 'Musketeers', shortName: 'Musketeers', role: 'Gunpowder infantry',
      description: 'Three ranks plant their feet and fire one synchronized rank at a time. Hard-hitting balls reward careful positioning through a long 24-second individual reload.',
      strengths: 'Synchronized rank volleys · armor penetration · heavy musket damage', weakness: 'Close combat breaks volley discipline; cavalry can overrun the line during its long reload.',
      hp: 72, damage: 78, armor: 2, speed: 40, range: 425, cooldown: 24, radius: 6, cost: 7, morale: 71,
      ranged: true, siege: false, projectile: 'musketBall', projectileSpeed: 1500, projectileArc: 1, splash: 0, armorPierce: .85, color: '#ae967e',
      missile: Object.freeze({ label: 'Rotating rank volleys', description: 'Hold three ranks and fire one entire rank together, then pass the fire order to the next rank while the others reload.', standoff: .80, ranks: 3, spacing: 1.10, maxWidth: 680, breakRange: 55, reloadStep: 0, armorPreference: .018 })
    }),
    crossbow: unit('crossbow', {
      missile: Object.freeze({ label: 'Planted bolt line', description: 'Settle before firing, reload in place, and favor armored targets within reach.', standoff: .76, ranks: 5, spacing: 1.05, maxWidth: 540, breakRange: 45, reloadStep: 0, armorPreference: .026 }),
      name: 'Arbalest Company', shortName: 'Crossbows', role: 'Armor piercing',
      description: 'Steel crossbows and broad bolts trade firing speed for power. Their aim is best supported by a solid infantry screen.',
      strengths: 'High bolt damage · substantial armor penetration',
      weakness: 'Slower reload and shorter range than longbows.',
      hp: 76, damage: 32, armor: 4, speed: 39, range: 365, cooldown: 2.85,
      radius: 6, cost: 5, morale: 74, ranged: true, projectile: 'bolt', armorPierce: .8, color: '#b8a97d'
    }),
    javelin: unit('javelin', {
      missile: Object.freeze({ label: 'Loose skirmish bands', description: 'Open short-range throwing lanes, throw, then make brief local steps while recovering a spear.', standoff: .64, ranks: 5, spacing: 1.30, maxWidth: 540, breakRange: 115, reloadStep: 32, armorPreference: .004 }),
      name: 'Javelin Skirmishers', shortName: 'Javelins', role: 'Light skirmishers',
      description: 'Quick-footed soldiers carry throwing spears and small bucklers. Their short-range volleys work best on a wing or beside woodland cover.',
      strengths: 'Fast movement · heavy short-range throws · woodland mobility',
      weakness: 'Short range and light armor; cannot hold against a determined charge.',
      hp: 74, damage: 23, armor: 2, speed: 57, range: 255, cooldown: 2.4,
      radius: 5.5, cost: 3, morale: 68, ranged: true, projectile: 'javelin',
      armorPierce: .22, projectileSpeed: 340, projectileArc: 55,
      forestSpeed: 1.15, skirmishDistance: 115, color: '#bfac79'
    }),
    slinger: unit('slinger', {
      missile: Object.freeze({ label: 'Mobile sling screen', description: 'Keep a wide light screen, whirl and release stones, and make small spacing adjustments between shots.', standoff: .73, ranks: 4, spacing: 1.18, maxWidth: 620, breakRange: 100, reloadStep: 15, armorPreference: -.018 }),
      name: 'Levy Slingers', shortName: 'Slingers', role: 'Light missile troops',
      description: 'Cloth-clad levies whirl leather slings and loose stones from belt pouches. They add inexpensive ranged pressure behind a protective line.',
      strengths: 'Very low cost · steady ranged fire · agile in woodland',
      weakness: 'Unarmored and low in morale; stones struggle against heavy plate.',
      hp: 54, damage: 12, armor: 0, speed: 53, range: 385, cooldown: 1.6,
      radius: 5.4, cost: 1, morale: 60, ranged: true, projectile: 'slingStone',
      armorPierce: .05, projectileSpeed: 420, projectileArc: 105,
      forestSpeed: 1.1, skirmishDistance: 100, color: '#c8b79b'
    }),
    cavalry: unit('cavalry', {
      name: 'Royal Cavalry', shortName: 'Cavalry', role: 'Mobile shock',
      description: 'Armored riders with sabers and kite shields. A running charge shoves light ranks aside, carries into cleared space, and loses momentum against heavier or braced bodies.',
      strengths: 'Fast flanking · charge impact · durable in melee',
      weakness: 'Pikes punish a frontal charge; woodland slows their horses.',
      hp: 198, damage: 25, armor: 7, speed: 150, range: 33, cooldown: 1.2,
      radius: 10, cost: 9, morale: 88, mounted: true, charge: 34, color: '#b79670'
    }),
    lancer: unit('lancer', {
      name: 'Banner Lancers', shortName: 'Lancers', role: 'Heavy charge',
      description: 'Long lances, streaming pennants, and armored horses. Slower than lighter horse, but far faster than foot soldiers. A clean run-up drives through weak ranks; dense shields and prepared pikes absorb the momentum.',
      strengths: 'Exceptional charge · reach · heavy horse armor',
      weakness: 'Less efficient after the first impact; pikes counter them directly.',
      hp: 218, damage: 22, armor: 8, speed: 132, range: 47, cooldown: 1.65,
      radius: 11, cost: 12, morale: 92, mounted: true, charge: 64, color: '#d9c39d'
    }),
    horseArcher: unit('horseArcher', {
      missile: Object.freeze({ label: 'Mounted harassment', description: 'Use wide mounted files and larger local evasive steps while shooting from a wing.', standoff: .70, ranks: 3, spacing: 1.20, maxWidth: 680, breakRange: 150, reloadStep: 58, armorPreference: -.006 }),
      name: 'Steppe Outriders', shortName: 'Outriders', role: 'Mounted archers',
      description: 'Light riders carrying short recurved bows. Their pace makes them useful on a wing or against slow infantry.',
      strengths: 'Very fast · ranged harassment · wide flanking',
      weakness: 'Light armor; outranged by longbows and vulnerable if trapped.',
      hp: 118, damage: 12, armor: 2, speed: 162, range: 345, cooldown: 1.85,
      radius: 9.5, cost: 8, morale: 73, mounted: true, ranged: true, projectile: 'arrow', color: '#ba916c'
    }),
    cannon: unit('cannon', {
      name: 'Field Cannons', shortName: 'Cannons', role: 'Gunpowder artillery',
      description: 'Low-flying round shot slams into packed ranks, scattering soldiers around the impact.',
      strengths: 'Heavy blast damage · launches troops', weakness: 'An exposed gun crew is vulnerable during its reload.',
      hp: 245, damage: 160, armor: 6, speed: 18, range: 920, cooldown: 22, radius: 20, cost: 65, morale: 75,
      ranged: true, siege: true, projectile: 'cannonball', projectileSpeed: 890, projectileArc: 12, splash: 64, armorPierce: .85, color: '#ae967e',
      missile: Object.freeze({ label: 'Gunpowder artillery', description: 'Low-flying round shot slams into packed ranks, scattering soldiers around the impact.', standoff: .80, ranks: 3, spacing: 1.10, maxWidth: 680, breakRange: 0, reloadStep: 0, armorPreference: .018 })
    }),
    trebuchet: unit('trebuchet', {
      name: 'Counterweight Trebuchets', shortName: 'Trebuchets', role: 'Heavy siege artillery',
      description: 'A counterweight casts a massive boulder over the field. Crews take time to wind down the arm and load the next stone.',
      strengths: 'Longest reach · devastating area damage', weakness: 'Slow to reposition and exceptionally slow to reload.',
      hp: 285, damage: 320, armor: 4, speed: 13, range: 1450, cooldown: 24, radius: 22, cost: 110, morale: 76,
      ranged: true, siege: true, projectile: 'boulder', projectileSpeed: 330, projectileArc: 255, splash: 112, armorPierce: .85, color: '#ae967e',
      missile: Object.freeze({ label: 'Heavy siege artillery', description: 'A counterweight casts a massive boulder over the field. Crews take time to wind down the arm and load the next stone.', standoff: .80, ranks: 3, spacing: 1.10, maxWidth: 680, breakRange: 0, reloadStep: 0, armorPreference: .018 })
    }),
    catapult: unit('catapult', {
      missile: Object.freeze({ label: 'Stone battery', description: 'Deploy a stationary battery at long range and keep room between engines during the slow reload.', standoff: .82, ranks: 3, spacing: 1.10, maxWidth: 700, breakRange: 0, reloadStep: 0, armorPreference: 0 }),
      name: 'Stonebreak Catapults', shortName: 'Catapults', role: 'Siege artillery',
      description: 'Wheeled torsion engines launch heavy stones in high arcs. Each impact threatens a cluster of soldiers.',
      strengths: 'Extreme range · area damage · pressure on packed formations',
      weakness: 'Very slow reload and movement; defenseless when overrun.',
      hp: 245, damage: 68, armor: 5, speed: 20, range: 820, cooldown: 7.8,
      radius: 19, cost: 36, morale: 72, ranged: true, siege: true,
      projectile: 'stone', splash: 63, armorPierce: .45, color: '#b38b55'
    }),
    ballista: unit('ballista', {
      missile: Object.freeze({ label: 'Heavy bolt battery', description: 'Plant a spaced battery and concentrate powerful bolts on armored bodies within reach.', standoff: .82, ranks: 2, spacing: 1.10, maxWidth: 700, breakRange: 0, reloadStep: 0, armorPreference: .035 }),
      name: 'Ironwind Ballistae', shortName: 'Ballistae', role: 'Heavy bolt artillery',
      description: 'Large wheeled crossbow engines fire heavy bolts on low trajectories. Their armor-piercing shots punish armored riders, beasts, and exposed elite troops.',
      strengths: 'Long reach · powerful single-target bolts · exceptional armor penetration',
      weakness: 'Slow to move and reload; needs protection from infantry and flanking riders.',
      hp: 230, damage: 84, armor: 6, speed: 18, range: 740, cooldown: 6.8,
      radius: 20, cost: 34, morale: 75, ranged: true, siege: true,
      projectile: 'ballistaBolt', armorPierce: .95, projectileSpeed: 620,
      projectileArc: 12, forestSpeed: .72, skirmishDistance: 0, color: '#b5c1b6'
    }),
    siegeTower: unit('siegeTower', {
      missile: Object.freeze({ label: 'Tower firing deck', description: 'Protected bowmen support a slow engineering approach. The tower deploys an assault ramp after reaching a wall.', standoff: .75, ranks: 1, spacing: 1, maxWidth: 250, breakRange: 0, reloadStep: 0, armorPreference: 0 }),
      name: 'Assault Siege Tower', shortName: 'Siege tower', role: 'Wall assault',
      description: 'A tall wheeled wooden tower with a covered firing deck. Under Auto it rolls to an enemy wall and lowers a broad assault ramp after four seconds of deployment.',
      strengths: 'Creates an additional wall crossing · protected archers · high durability',
      weakness: 'Very slow and expensive; needs an escort and is vulnerable to artillery.',
      hp: 950, damage: 22, armor: 9, speed: 23, range: 345, cooldown: 2.4,
      radius: 26, cost: 80, morale: 100, ranged: true, siege: true, equipment: true,
      groupSize: 1, maxCount: 12, projectile: 'arrow', projectileSpeed: 500, projectileArc: 55,
      forestSpeed: .60, color: '#b8874f'
    }),
    batteringRam: unit('batteringRam', {
      name: 'Covered Battering Ram', shortName: 'Battering ram', role: 'Gate breaker',
      description: 'A suspended iron-tipped log under an armored timber roof. Under Auto its crew advances to an enemy gate and batters it until a breach opens.',
      strengths: 'Heavy gate damage · protected crew · opens a ground-level breach',
      weakness: 'Slow, short ranged, and vulnerable to concentrated artillery or exposed flank attacks.',
      hp: 780, damage: 27, armor: 9, speed: 26, range: 38, cooldown: 2.2,
      radius: 22, cost: 56, morale: 100, siege: true, equipment: true,
      groupSize: 1, maxCount: 12, armorPierce: .25, forestSpeed: .65, color: '#9e7952'
    }),
    elephant: unit('elephant', {
      name: 'War Elephants', shortName: 'Elephants', role: 'Heavy beasts',
      description: 'Tusked giants carrying armored howdahs. Their weight and sweeping attacks smash through tightly packed infantry.',
      strengths: 'Huge health · splash damage · powerful impact',
      weakness: 'Large, expensive targets; pikes and concentrated bolts bring them down.',
      hp: 960, damage: 43, armor: 8, speed: 48, range: 46, cooldown: 1.8,
      radius: 22, cost: 55, morale: 83, mounted: true, splash: 27, charge: 28,
      color: '#adada4'
    }),
    commander: unit('commander', {
      name: 'Field Commander', shortName: 'Commander', role: 'Command & rally',
      description: 'A mounted general carrying the army standard. Keep commanders near your line to steady formations and regroup troops that fall back.',
      strengths: 'Nearby morale support · rallies fallback formations · durable command presence',
      weakness: 'Expensive and few in number; an isolated commander cannot rescue an army alone.',
      hp: 420, damage: 24, armor: 12, speed: 150, range: 34, cooldown: 1.25,
      radius: 11, cost: 100, morale: 100, mounted: true, commander: true,
      charge: 20, armorPierce: .15, rallyRadius: 620, supportRadius: 360,
      rallyCooldown: 45, maxCount: 16,
      color: '#f2d784'
    })
  });

  const TYPE_ORDER = Object.freeze(Object.keys(TYPES));
  function armorMultiplier(attackerId, armorClass) {
    const type = TYPES[attackerId], profile = type && ATTACK_PROFILES[type.attackProfile];
    return profile && Number.isFinite(profile.multipliers[armorClass]) ? profile.multipliers[armorClass] : 1;
  }
  function getMatchup(attackerId, defenderId) {
    const attacker = TYPES[attackerId], defender = TYPES[defenderId];
    if (!attacker || !defender) return null;
    return { armorClass: defender.armorClass, attackProfile: attacker.attackProfile,
      armorMultiplier: armorMultiplier(attackerId, defender.armorClass), armorPierce: attacker.armorPierce,
      antiMountedBonus: defender.mounted ? attacker.antiCavalry : 0 };
  }
  // A formation's combat and speed multipliers blend toward 1 as cohesion falls.
  // Spacing also changes its physical footprint; loose ranks need more ground.
  const FORMATIONS = Object.freeze({
    volley: Object.freeze({
      id: 'volley', name: 'Three-rank volley', ranks: 3,
      description: 'A broad frontage three ranks deep. Musketeers fire one complete rank together, rotating front to rear while each soldier reloads. Close combat breaks the firing drill.',
      speed: .95, damage: 1, frontalMelee: 1, frontalRanged: 1,
      incomingMelee: 1, incomingRanged: 1, incomingSplash: 1, charge: 1, spacing: 1.1
    }),
    column: Object.freeze({
      id: 'column', name: 'Three-file column', files: 3,
      description: 'A narrow marching column three soldiers wide. Useful for passages, but exposes little firing frontage and is vulnerable to artillery along its depth.',
      speed: 1, damage: 1, frontalMelee: 1, frontalRanged: 1,
      incomingMelee: 1, incomingRanged: 1, incomingSplash: 1.08, charge: 1, spacing: 1
    }),
    line: Object.freeze({
      id: 'line', name: 'Battle line',
      description: 'A steady, balanced line. At full cohesion, frontal melee damage taken is reduced by 4%.',
      speed: 1, damage: 1, frontalMelee: .96, frontalRanged: 1,
      incomingMelee: 1, incomingRanged: 1, incomingSplash: 1, charge: 1, spacing: 1
    }),
    shieldwall: Object.freeze({
      id: 'shieldwall', name: 'Shield wall',
      description: 'Close ranks to resist frontal attacks. At full cohesion: 18% less frontal melee damage and 28% less frontal ranged damage, with 28% slower movement, 10% lower attack damage, and 20% less charge impact.',
      speed: .72, damage: .9, frontalMelee: .82, frontalRanged: .72,
      incomingMelee: 1, incomingRanged: 1, incomingSplash: 1, charge: .8, spacing: .9
    }),
    wedge: Object.freeze({
      id: 'wedge', name: 'Assault wedge',
      description: 'Concentrate a charge into a pointed formation. At full cohesion: 30% more charge impact and 10% faster movement, but 10% more melee damage taken.',
      speed: 1.1, damage: 1, frontalMelee: 1, frontalRanged: 1,
      incomingMelee: 1.1, incomingRanged: 1, incomingSplash: 1, charge: 1.3, spacing: 1
    }),
    loose: Object.freeze({
      id: 'loose', name: 'Loose order',
      description: 'Spread out to resist bombardment. At full cohesion: 28% less ranged damage, 40% less splash damage, and 5% faster movement, but 15% more melee damage taken. Uses wider spacing.',
      speed: 1.05, damage: 1, frontalMelee: 1, frontalRanged: 1,
      incomingMelee: 1.15, incomingRanged: .72, incomingSplash: .6, charge: 1, spacing: 1.45
    })
  });
  const WORLD_SIZES = Object.freeze([
    Object.freeze({ id: 'standard', name: 'Standard', width: 6400, height: 4400 }),
    Object.freeze({ id: 'grand', name: 'Grand', width: 9600, height: 6600 }),
    Object.freeze({ id: 'vast', name: 'Vast', width: 12800, height: 8800 }),
    Object.freeze({ id: 'colossal', name: 'Colossal', width: 16000, height: 11000 })
  ]);
  const normalizeWorldSize = (width, height) => {
    const size = WORLD_SIZES.find(size => size.width === Number(width) && size.height === Number(height)) || WORLD_SIZES[0];
    return { width: size.width, height: size.height };
  };
  const region = (type, x, y, w, h) => ({ type, x, y, w, h });
  const fort = (type, x, y, w, h, hp, team = 1) => ({ type, x, y, w, h, hp, team });
  // All four corners and every gate are ordinary, independently destructible
  // structures. The wall runs meet edge-to-edge, with no invisible blocker
  // left across a gate or a breach. Gate positions are measured along a side.
  function castleWalls(x, y, w, h, options = {}) {
    if (![x, y, w, h].every(Number.isFinite) || Math.min(w, h) < 320) return [];
    // Even at an arbitrary editor position, a closed face occupies a full
    // navigation cell, and the smallest footprint fits a 160-unit opening.
    const thick = options.thickness || clamp(Math.round(Math.min(w, h) / 18), 80, 120);
    const tower = Math.min(Math.round(thick * 1.5), (Math.min(w, h) - 160) / 2);
    const wallHp = options.wallHp || 2200, gateHp = options.gateHp || 1000;
    const team = options.team === 0 ? 0 : 1;
    const gates = options.gates || { west: [[h / 2, clamp(h / 4, 160, 320)]] };
    const parts = [];
    const maxPanel = Math.max(700, Math.max(w, h) / 2);
    function side(name, sx, sy, length, vertical) {
      const add = (type, a, b) => {
        const count = type === 'gate' ? 1 : Math.ceil((b - a) / maxPanel);
        for (let i = 0; i < count; i++) {
          const start = a + (b - a) * i / count, size = (b - a) / count;
          const part = fort(type, sx + (vertical ? 0 : start), sy + (vertical ? start : 0),
            vertical ? thick : size, vertical ? size : thick, type === 'gate' ? gateHp : wallHp, team);
          if (type === 'gate') part.castleGate = true;
          parts.push(part);
        }
      };
      let end = tower;
      for (const [at, span] of gates[name] || []) {
        let start = clamp(at - span / 2, end, length - tower);
        let stop = clamp(at + span / 2, start, length - tower);
        if (start - end < 20) start = end;
        if (length - tower - stop < 20) stop = length - tower;
        add('wall', end, start);
        if (stop > start) add('gate', start, stop);
        end = stop;
      }
      add('wall', end, length - tower);
    }
    side('west', x, y, h, true);
    side('north', x, y, w, false);
    side('east', x + w - thick, y, h, true);
    side('south', x, y + h - thick, w, false);
    for (const cx of [x, x + w - tower]) for (const cy of [y, y + h - tower]) {
      parts.push({ ...fort('wall', cx, cy, tower, tower, wallHp * 1.4, team), castleTower: true });
    }
    return parts;
  }
  const MAPS = [
    {
      id: 'meadow', name: 'The Amber Plains', subtitle: 'Open ground · room to maneuver',
      description: 'Wide rolling grassland with wooded wings and gentle central heights. A balanced battlefield for flanking and large formations.',
      biome: 'grassland', width: 6400, height: 4400, seed: 2141,
      regions: [
        region('forest', 2300, 280, 730, 530), region('forest', 3370, 3530, 750, 540),
        region('forest', 930, 3180, 560, 560), region('forest', 4880, 610, 550, 650),
        region('hill', 2690, 1500, 760, 620), region('hill', 3750, 2600, 620, 500),
        region('rock', 2850, 150, 150, 100), region('rock', 3500, 4060, 170, 110)
      ]
    },
    {
      id: 'river', name: 'Three Bridges', subtitle: 'River crossings · choke points',
      description: 'An impassable river cuts the valley in two. Three stone crossings reward a patient defense and a carefully timed attack on the wings.',
      biome: 'wetland', width: 6400, height: 4400, seed: 4399,
      regions: [
        region('water', 3070, 0, 260, 4400),
        region('bridge', 2890, 760, 620, 220),
        region('bridge', 2890, 2090, 620, 240),
        region('bridge', 2890, 3420, 620, 220),
        region('forest', 2210, 1220, 520, 530), region('forest', 3650, 2720, 520, 520),
        region('forest', 1160, 3050, 590, 540), region('forest', 4800, 430, 540, 580),
        region('hill', 2370, 2330, 440, 470), region('hill', 3640, 1220, 440, 470),
        region('rock', 2800, 370, 170, 150), region('rock', 3430, 3850, 180, 150)
      ]
    },
    {
      id: 'desert', name: 'Saffron Expanse', subtitle: 'Bare flanks · rocky highlands',
      description: 'Wind-swept sand, isolated ridges, and exposed approaches. Use the broad southern gap to turn an enemy line or place artillery on a rise.',
      biome: 'desert', width: 6400, height: 4400, seed: 7823,
      regions: [
        region('rock', 3010, 700, 300, 560), region('rock', 3010, 1490, 300, 360),
        region('hill', 2400, 2190, 620, 540), region('hill', 3500, 1340, 630, 540),
        region('hill', 3530, 3290, 730, 580), region('hill', 2250, 280, 630, 480),
        region('forest', 500, 3490, 430, 440), region('forest', 5430, 380, 420, 440),
        region('rock', 2760, 3590, 200, 170), region('rock', 3500, 390, 170, 130)
      ]
    },
    {
      id: 'frost', name: 'Wintermarch', subtitle: 'Pine forests · snowy ridges',
      description: 'Dense pine stands divide the snowy plain into lanes. Cover benefits missile troops, while open central ground still allows a decisive mounted charge.',
      biome: 'tundra', width: 6400, height: 4400, seed: 9917,
      regions: [
        region('forest', 2250, 350, 760, 780), region('forest', 3400, 3270, 770, 780),
        region('forest', 980, 2670, 650, 760), region('forest', 4780, 920, 620, 750),
        region('forest', 2780, 2820, 480, 440), region('forest', 3350, 1130, 470, 440),
        region('hill', 2420, 1610, 560, 570), region('hill', 3550, 2240, 570, 570),
        region('rock', 3090, 330, 170, 140), region('rock', 3060, 3990, 160, 130)
      ]
    },
    {
      id: 'greenwood', name: 'Greenwood Vale', subtitle: 'Woodland cover · broad glades',
      description: 'Separated woodland stands shelter missile troops while wide glades connect the field. Foot skirmishers can use the trees; cavalry must choose its lanes.',
      biome: 'woodland', width: 6400, height: 4400, seed: 18467,
      regions: [
        region('forest', 2350, 200, 600, 650), region('forest', 3450, 300, 550, 640),
        region('forest', 2700, 1280, 430, 600), region('forest', 3350, 2500, 480, 650),
        region('forest', 2400, 3440, 650, 700), region('forest', 3700, 3500, 550, 650),
        region('forest', 900, 300, 600, 650), region('forest', 4950, 3000, 650, 800),
        region('forest', 1000, 3100, 650, 700), region('forest', 4850, 600, 600, 850),
        region('hill', 3020, 2030, 470, 360), region('hill', 3890, 1560, 470, 420),
        region('rock', 3130, 140, 110, 100), region('rock', 3110, 4100, 140, 100)
      ]
    },
    {
      id: 'highland', name: 'Broken Crown Pass', subtitle: 'Three ridge passages · contested heights',
      description: 'A broken spine of rock offers three substantial passages instead of one gate. Secure a ridge approach, threaten another crossing, or send reserves through the middle.',
      biome: 'highland', width: 6400, height: 4400, seed: 27319,
      regions: [
        region('hill', 2660, 120, 930, 520), region('hill', 2680, 1200, 1100, 600),
        region('hill', 2750, 2520, 1050, 500), region('hill', 2710, 3650, 1100, 620),
        region('rock', 3040, 0, 300, 720), region('rock', 2980, 1100, 400, 850),
        region('rock', 3100, 2370, 320, 810), region('rock', 3000, 3600, 380, 800),
        region('forest', 1400, 420, 500, 600), region('forest', 4580, 3330, 530, 630),
        region('forest', 1530, 3380, 470, 550), region('forest', 4510, 450, 490, 570),
        region('hill', 2360, 1900, 420, 510), region('hill', 3630, 1900, 420, 510)
      ]
    },
    {
      id: 'basin', name: 'Ashen Basin', subtitle: 'Central heights · open outer flanks',
      description: 'An exposed central rise dominates the basin, with broad northern and southern approaches. Artillery can command the heights while mobile troops contest the outer wings.',
      biome: 'ashland', width: 6400, height: 4400, seed: 31841,
      regions: [
        region('hill', 2720, 1450, 1000, 1450), region('hill', 2080, 830, 700, 460),
        region('hill', 3680, 3050, 700, 460), region('hill', 2000, 3020, 620, 460),
        region('hill', 3860, 810, 620, 460),
        region('rock', 3090, 1590, 130, 110), region('rock', 3350, 2780, 150, 100),
        region('rock', 3160, 350, 220, 210), region('rock', 2990, 3850, 230, 250),
        region('forest', 2150, 180, 460, 500), region('forest', 3900, 3640, 430, 550),
        region('forest', 1170, 3280, 430, 540), region('forest', 4830, 520, 430, 540)
      ]
    },
    {
      id: 'stonekeep', name: 'Stonekeep Siege', subtitle: 'Square castle · west and north gates',
      description: 'Four corner towers enclose a square courtyard. Attack the west gate from the muster field, flank the north gate, or escort a tower to the curtain wall.',
      biome: 'grassland', width: 6400, height: 4400, seed: 271101,
      regions: [
        ...castleWalls(3520, 920, 2560, 2560, {
          gates: { west: [[1280, 240]], north: [[1560, 240]] }
        }),
        region('hill', 3820, 1560, 520, 1280), region('forest', 1180, 280, 600, 600),
        region('forest', 1180, 3520, 600, 600), region('rock', 2790, 480, 210, 490),
        region('rock', 2790, 3430, 210, 490)
      ]
    },
    {
      id: 'moat', name: 'The Twin Causeways', subtitle: 'Moat · two narrow bridge assaults',
      description: 'An unbroken moat funnels attackers across two causeways into defended gates. Break one entrance or divide the defense; towers can deploy beyond a secured crossing.',
      biome: 'wetland', width: 6400, height: 4400, seed: 271102,
      regions: [
        region('water', 3120, 0, 300, 4400),
        region('bridge', 3020, 1200, 520, 240), region('bridge', 3020, 2960, 520, 240),
        fort('wall', 3520, 0, 120, 1200, 2400), fort('gate', 3520, 1200, 120, 240, 1100),
        fort('wall', 3520, 1440, 120, 1520, 2400), fort('gate', 3520, 2960, 120, 240, 1100),
        fort('wall', 3520, 3200, 120, 1200, 2400),
        region('hill', 3860, 1040, 520, 560), region('hill', 3860, 2800, 520, 560),
        region('forest', 1780, 1840, 620, 720), region('forest', 4950, 180, 520, 720),
        region('forest', 4950, 3500, 520, 700)
      ]
    },
    {
      id: 'mountainfort', name: 'Crownwatch Bastion', subtitle: 'Rock corridors · layered defense',
      description: 'Rock shelves divide the western approach into three narrow lanes. Two fortified entrances lead into the upper bastion, where defenders can hold the shoulders of each breach.',
      biome: 'highland', width: 6400, height: 4400, seed: 271103,
      regions: [
        region('rock', 2820, 0, 380, 1000), region('rock', 2780, 1320, 460, 720),
        region('rock', 2780, 2360, 460, 720), region('rock', 2820, 3400, 380, 1000),
        fort('wall', 3720, 0, 120, 1040, 2500), fort('gate', 3720, 1040, 120, 240, 1200),
        fort('wall', 3720, 1280, 120, 1840, 2500), fort('gate', 3720, 3120, 120, 240, 1200),
        fort('wall', 3720, 3360, 120, 1040, 2500),
        region('hill', 3940, 700, 720, 1000), region('hill', 3940, 2700, 720, 1000),
        region('forest', 1350, 1600, 650, 700), region('rock', 4590, 1930, 350, 540)
      ]
    },
    {
      id: 'custom', name: 'Custom Map', subtitle: 'Blank field · build an encounter',
      description: 'An empty battlefield. Place formations and draw your own terrain before starting the encounter.',
      biome: 'grassland', width: 6400, height: 4400, seed: 142857, regions: []
    }
  ];

  // Larger fields preserve body scale: the extra ground is real maneuvering
  // space, not smaller soldiers or an enlarged picture of an old battlefield.
  MAPS.splice(MAPS.length - 1, 0,
    {
      id: 'grandplain', name: 'The Sovereign Plain', subtitle: 'Vast field · long cavalry wings',
      description: 'Four times the ground of the Amber Plains. Low central rises separate a broad infantry front from open mounted wings, with woodland islands sheltering distant reserves.',
      biome: 'grassland', width: 12800, height: 8800, seed: 282201,
      regions: [
        region('hill', 5650, 3150, 1500, 1900), region('hill', 4500, 1800, 900, 700),
        region('hill', 7450, 6100, 1000, 650), region('hill', 7700, 1800, 800, 700),
        region('forest', 2800, 700, 1000, 1000), region('forest', 8800, 6950, 1000, 1000),
        region('forest', 2300, 6500, 900, 1100), region('forest', 9600, 900, 900, 1100),
        region('rock', 6260, 900, 220, 300), region('rock', 6200, 7530, 230, 260)
      ]
    },
    {
      id: 'longgrass', name: 'The Endless Steppe', subtitle: 'Colossal field · sweeping encirclements',
      description: 'Six and a quarter times the original battlefield area. Widely separated ridgelines leave a huge central plain and distant routes for rapid wings, layered reserves, and army-scale envelopment.',
      biome: 'grassland', width: 16000, height: 11000, seed: 282202,
      regions: [
        region('hill', 4900, 1800, 1500, 850), region('hill', 9400, 8350, 1600, 850),
        region('hill', 7300, 4750, 1300, 1600), region('hill', 9600, 2900, 1100, 650),
        region('forest', 3100, 8500, 900, 1100), region('forest', 12000, 1500, 900, 1000),
        region('forest', 5000, 700, 950, 800), region('forest', 10100, 9500, 950, 800)
      ]
    },
    {
      id: 'silverwood', name: 'Silverwood Crossroads', subtitle: 'Grand woodland · connected clearings',
      description: 'Large separated groves form four broad forest corridors. The central glade favors ordered infantry, while light troops can cross the woods and cavalry can take the open outer lanes.',
      biome: 'woodland', width: 9600, height: 6600, seed: 282203,
      regions: [
        region('forest', 2900, 300, 1200, 1100), region('forest', 5200, 420, 1350, 1150),
        region('forest', 2950, 2150, 1000, 1050), region('forest', 5570, 3400, 1050, 1050),
        region('forest', 2870, 4730, 1250, 1400), region('forest', 5200, 5200, 1250, 1000),
        region('forest', 900, 900, 900, 1150), region('forest', 7700, 4450, 900, 1150),
        region('hill', 4200, 2770, 1200, 1000), region('hill', 6800, 1700, 600, 650),
        region('rock', 4580, 260, 260, 200), region('rock', 4700, 6140, 240, 210)
      ]
    },
    {
      id: 'blackforest', name: 'The Blackwood Marches', subtitle: 'Vast woodland · staggered avenues',
      description: 'Long forest belts stagger the approaches without sealing them. A steady line can use the broad roads; successive assault waves and skirmishers can contest separate clearings.',
      biome: 'woodland', width: 12800, height: 8800, seed: 282204,
      regions: [
        region('forest', 4200, 0, 1500, 1800), region('forest', 6900, 900, 1500, 1800),
        region('forest', 4000, 2900, 1550, 1800), region('forest', 7250, 4000, 1550, 1800),
        region('forest', 4400, 6000, 1500, 1800), region('forest', 7000, 7100, 1500, 1700),
        region('forest', 1500, 1100, 1100, 1600), region('forest', 10200, 5800, 1100, 1700),
        region('forest', 1600, 6200, 1000, 1500), region('forest', 10400, 1100, 950, 1500),
        region('hill', 5750, 3500, 1300, 1400), region('hill', 6100, 7100, 550, 850)
      ]
    },
    {
      id: 'fivefords', name: 'The Five Crossing War', subtitle: 'Vast river · five distant crossings',
      description: 'A wide river divides the entire field. Five crossings range from narrow outer bridges to a broad center, so reserves and simultaneous wing attacks matter more than feeding one crowded crossing.',
      biome: 'wetland', width: 12800, height: 8800, seed: 282205,
      regions: [
        region('water', 6180, 0, 440, 8800),
        region('bridge', 6000, 850, 800, 360), region('bridge', 6000, 2500, 800, 400),
        region('bridge', 6000, 4150, 800, 500), region('bridge', 6000, 5900, 800, 400),
        region('bridge', 6000, 7580, 800, 360),
        region('hill', 4850, 3650, 900, 1500), region('hill', 7050, 3650, 900, 1500),
        region('forest', 4300, 1450, 950, 800), region('forest', 7550, 6600, 950, 800),
        region('forest', 4300, 6750, 950, 800), region('forest', 7550, 1550, 950, 800)
      ]
    },
    {
      id: 'redcanyon', name: 'The Redstone Gates', subtitle: 'Grand canyon · four rock passages',
      description: 'A broken canyon wall offers four real passageways, each at least five hundred paces wide. Narrow columns can clear the stone, then deploy on the open eastern shelf.',
      biome: 'highland', width: 9600, height: 6600, seed: 282206,
      regions: [
        region('rock', 4440, 0, 650, 820), region('rock', 4320, 1420, 850, 900),
        region('rock', 4480, 2920, 600, 760), region('rock', 4320, 4280, 850, 900),
        region('rock', 4440, 5780, 650, 820),
        region('hill', 3600, 400, 700, 1200), region('hill', 5350, 950, 800, 1000),
        region('hill', 3500, 2850, 700, 1000), region('hill', 5380, 3850, 750, 1100),
        region('hill', 3500, 5100, 700, 1000), region('forest', 1750, 2500, 900, 1200),
        region('forest', 7100, 2900, 800, 1200)
      ]
    },
    {
      id: 'dunesea', name: 'The Sea of Copper', subtitle: 'Colossal desert · scattered escarpments',
      description: 'A vast desert supports sweeping cavalry routes. Rocky escarpments divide the northern sector, while the south stays open for rapid encirclement and countercharges.',
      biome: 'desert', width: 16000, height: 11000, seed: 282207,
      regions: [
        region('rock', 7600, 700, 650, 1700), region('rock', 6900, 3100, 500, 1200),
        region('rock', 8600, 3600, 550, 1300), region('rock', 7900, 9800, 380, 450),
        region('hill', 5700, 5000, 1300, 1200), region('hill', 9200, 5200, 1500, 1300),
        region('hill', 6900, 8000, 1900, 900), region('hill', 10300, 1800, 1300, 950),
        region('forest', 2800, 8200, 900, 1000), region('forest', 12500, 7200, 800, 1000),
        region('hill', 4100, 2200, 1000, 900)
      ]
    },
    {
      id: 'frostlake', name: 'The Wintermere Front', subtitle: 'Vast winter field · lake and outer routes',
      description: 'An unfrozen lake blocks the middle of the snowy plain. Broad northern and southern approaches force the armies to choose where to concentrate, with a smaller central causeway for a risky direct attack.',
      biome: 'tundra', width: 12800, height: 8800, seed: 282208,
      regions: [
        region('water', 5500, 2500, 1800, 3600), region('bridge', 5300, 4160, 2200, 480),
        region('forest', 3750, 650, 1250, 1100), region('forest', 7800, 6950, 1250, 1100),
        region('forest', 3100, 6200, 1200, 1300), region('forest', 8500, 1250, 1100, 1300),
        region('forest', 4300, 2900, 650, 1400), region('forest', 7850, 4600, 650, 1300),
        region('hill', 5500, 900, 1800, 800), region('hill', 5500, 7100, 1800, 800),
        region('rock', 6170, 200, 300, 250), region('rock', 6270, 8320, 280, 220)
      ]
    },
    {
      id: 'reeddelta', name: 'The Reedwater Delta', subtitle: 'Grand wetlands · offset island crossings',
      description: 'Two offset river channels create a connected middle island. Three crossings on each bank favor securing a bridgehead, reforming in the center, and attacking a second crossing with reserves.',
      biome: 'wetland', width: 9600, height: 6600, seed: 282209,
      regions: [
        region('water', 4050, 0, 300, 6600), region('water', 5250, 0, 300, 6600),
        region('bridge', 3900, 1050, 600, 360), region('bridge', 3900, 3100, 600, 400), region('bridge', 3900, 5150, 600, 360),
        region('bridge', 5100, 1600, 600, 360), region('bridge', 5100, 3400, 600, 400), region('bridge', 5100, 5200, 600, 360),
        region('forest', 4500, 2100, 550, 800), region('forest', 4480, 4250, 580, 650),
        region('forest', 1900, 2400, 900, 1100), region('forest', 6800, 1000, 900, 1100),
        region('hill', 4430, 650, 700, 650), region('hill', 4430, 5600, 700, 600)
      ]
    },
    {
      id: 'imperialwall', name: 'The Imperial Rampart', subtitle: 'Vast siege · three gates and tower fronts',
      description: 'A long stone rampart guards three separated gates. Rams can split the garrison between distant entrances, while escorted towers create new assaults along the wall.',
      biome: 'grassland', width: 12800, height: 8800, seed: 282210,
      regions: [
        fort('wall', 7000, 0, 140, 1800, 2700), fort('gate', 7000, 1800, 140, 400, 1300),
        fort('wall', 7000, 2200, 140, 2000, 2700), fort('gate', 7000, 4200, 140, 400, 1300),
        fort('wall', 7000, 4600, 140, 2000, 2700), fort('gate', 7000, 6600, 140, 400, 1300),
        fort('wall', 7000, 7000, 140, 1800, 2700),
        region('hill', 7500, 1100, 1000, 1600), region('hill', 7500, 3600, 1000, 1600),
        region('hill', 7500, 6000, 1000, 1600), region('forest', 3600, 400, 1100, 1300),
        region('forest', 3600, 7100, 1100, 1300), region('forest', 10300, 3500, 900, 1400)
      ]
    },
    {
      id: 'floodgate', name: 'Floodgate Citadel', subtitle: 'Castle siege · three causeways',
      description: 'Three causeways cross the moat to the west gates of a walled rectangular citadel. The garrison has room to regroup in the courtyard and an east gate for a second approach.',
      biome: 'wetland', width: 9600, height: 6600, seed: 282211,
      regions: [
        region('water', 4750, 0, 460, 6600),
        region('bridge', 4600, 1300, 1100, 360), region('bridge', 4600, 3120, 1100, 360), region('bridge', 4600, 4940, 1100, 360),
        ...castleWalls(5550, 700, 3280, 5200, {
          thickness: 130, wallHp: 2600, gateHp: 1200,
          gates: { west: [[780, 360], [2600, 360], [4420, 360]], east: [[2600, 320]] }
        }),
        region('hill', 5900, 900, 800, 1100), region('hill', 5900, 2750, 800, 1100),
        region('hill', 5900, 4550, 800, 1100), region('forest', 2700, 2150, 950, 800),
        region('forest', 2700, 3900, 950, 800), region('forest', 7500, 2750, 900, 1200)
      ]
    },
    {
      id: 'highcitadel', name: 'The Three Crowns', subtitle: 'Colossal siege · mountain rampart and square keep',
      description: 'Three broad mountain passages lead to three fortified entrances. Behind the outer rampart stands a square inner keep. Its west gate faces the assault, while a south gate gives attackers a longer route around the inner defenses.',
      biome: 'highland', width: 16000, height: 11000, seed: 282212,
      regions: [
        region('rock', 8000, 0, 750, 1900), region('rock', 8000, 2700, 750, 2400),
        region('rock', 8000, 5900, 750, 2400), region('rock', 8000, 9100, 750, 1900),
        fort('wall', 9200, 0, 140, 2000, 2900), fort('gate', 9200, 2000, 140, 600, 1400),
        fort('wall', 9200, 2600, 140, 2600, 2900), fort('gate', 9200, 5200, 140, 600, 1400),
        fort('wall', 9200, 5800, 140, 2600, 2900), fort('gate', 9200, 8400, 140, 600, 1400),
        fort('wall', 9200, 9000, 140, 2000, 2900),
        ...castleWalls(11000, 3420, 4120, 4120, {
          thickness: 120, gates: { west: [[1580, 600]], south: [[2520, 400]] }
        }),
        region('hill', 9700, 1300, 1200, 1500), region('hill', 9700, 4600, 1100, 1800),
        region('hill', 9700, 8000, 1200, 1500), region('hill', 11600, 4400, 1400, 1800),
        region('forest', 5100, 900, 1200, 1300), region('forest', 5100, 8800, 1200, 1300)
      ]
    }
  );

  const roster = (values) => Object.fromEntries(TYPE_ORDER.map(id => [id, values[id] || 0]));
  const army = (name, doctrine, values) => ({ name, doctrine, roster: roster(values) });
  const PRESETS = [
    {
      id: 'balanced', name: 'Clash of Kingdoms', description: '2,400 soldiers. Balanced armies meet on open ground.',
      map: 'meadow', seed: 20261101,
      armies: [
        army('Azure Crown', 'balanced', { militia: 78, swordsman: 212, pikeman: 160, hoplite: 48, halberdier: 50, guardian: 80, berserker: 60, archer: 202, crossbow: 80, javelin: 40, slinger: 48, cavalry: 70, lancer: 30, horseArcher: 28, catapult: 6, ballista: 2, elephant: 4, commander: 2 }),
        army('Ember Dominion', 'flank', { militia: 132, swordsman: 150, pikeman: 120, hoplite: 40, halberdier: 40, guardian: 70, berserker: 130, archer: 160, crossbow: 98, javelin: 50, slinger: 48, cavalry: 70, lancer: 40, horseArcher: 38, catapult: 6, ballista: 2, elephant: 4, commander: 2 })
      ]
    },
    {
      id: 'charge', name: 'The Broken Charge', description: 'Massed pikes and archers face a swift mounted host.',
      map: 'meadow', seed: 64172,
      armies: [
        army('Pikebound League', 'defensive', { militia: 118, swordsman: 132, pikeman: 650, hoplite: 48, guardian: 120, archer: 280, crossbow: 80, catapult: 6, commander: 2 }),
        army('Crimson Riders', 'flank', { swordsman: 74, hoplite: 24, berserker: 100, cavalry: 410, lancer: 260, horseArcher: 220, elephant: 8, commander: 2 })
      ]
    },
    {
      id: 'bridges', name: 'Hold the Crossings', description: 'An outnumbered defense stands between three bridges and a larger army.',
      map: 'river', seed: 38482,
      armies: [
        army('River Wardens', 'defensive', { swordsman: 100, hoplite: 48, pikeman: 260, guardian: 220, archer: 230, crossbow: 130, cavalry: 40, catapult: 16, elephant: 4, commander: 2 }),
        army('The Red Host', 'aggressive', { militia: 468, swordsman: 502, hoplite: 48, pikeman: 200, berserker: 270, archer: 280, crossbow: 100, cavalry: 100, lancer: 60, horseArcher: 60, catapult: 16, elephant: 10, commander: 2 })
      ]
    },
    {
      id: 'siege', name: 'Thunder on the Dunes', description: 'Long-range bombardment, armored screens, and elephant assaults.',
      map: 'desert', seed: 93634,
      armies: [
        army('Saffron Artillery', 'balanced', { swordsman: 170, hoplite: 48, pikeman: 160, guardian: 180, archer: 220, crossbow: 200, cavalry: 70, horseArcher: 50, catapult: 44, elephant: 6, commander: 2 }),
        army('Tusks of the East', 'aggressive', { militia: 218, swordsman: 272, hoplite: 48, pikeman: 90, guardian: 100, berserker: 200, archer: 200, crossbow: 100, cavalry: 80, lancer: 40, horseArcher: 40, catapult: 14, elephant: 42, commander: 2 })
      ]
    },
    {
      id: 'epic', name: 'War of Twelve Thousand', description: '12,000 individual soldiers. Vast winter armies with every troop class.',
      map: 'frost', seed: 120001,
      armies: [
        army('The Northern Alliance', 'balanced', { militia: 646, swordsman: 1190, hoplite: 160, pikeman: 650, halberdier: 220, guardian: 500, berserker: 300, archer: 950, crossbow: 330, javelin: 200, slinger: 250, cavalry: 300, lancer: 150, horseArcher: 120, catapult: 10, ballista: 10, elephant: 10, commander: 4 }),
        army('The Ember Imperium', 'flank', { militia: 846, swordsman: 890, hoplite: 160, pikeman: 500, halberdier: 200, guardian: 400, berserker: 600, archer: 900, crossbow: 330, javelin: 230, slinger: 240, cavalry: 300, lancer: 200, horseArcher: 170, catapult: 10, ballista: 10, elephant: 10, commander: 4 })
      ]
    },
    {
      id: 'woodland', name: 'War in the Greenwood', description: '3,000 soldiers. Woodland skirmishers, archers, and reserves contest the glades.',
      map: 'greenwood', seed: 174211,
      armies: [
        army('Vale Wardens', 'balanced', { militia: 120, swordsman: 132, hoplite: 48, pikeman: 100, halberdier: 100, guardian: 60, berserker: 40, archer: 320, crossbow: 80, javelin: 160, slinger: 106, cavalry: 80, lancer: 20, horseArcher: 120, catapult: 6, ballista: 4, elephant: 2, commander: 2 }),
        army('The Thorn Host', 'flank', { militia: 180, swordsman: 172, hoplite: 48, pikeman: 80, halberdier: 100, guardian: 80, berserker: 100, archer: 180, crossbow: 80, javelin: 160, slinger: 116, cavalry: 80, lancer: 40, horseArcher: 70, catapult: 6, ballista: 4, elephant: 2, commander: 2 })
      ]
    },
    {
      id: 'highland', name: 'Three Roads to the Crown', description: '4,000 soldiers. Armored polearms and bolt artillery fight for three mountain passages.',
      map: 'highland', seed: 174301,
      armies: [
        army('Crownward Legion', 'defensive', { militia: 150, swordsman: 240, hoplite: 80, pikeman: 220, halberdier: 220, guardian: 180, berserker: 60, archer: 280, crossbow: 120, javelin: 100, slinger: 132, cavalry: 90, lancer: 40, horseArcher: 60, catapult: 10, ballista: 12, elephant: 4, commander: 2 }),
        army('Ridgebreaker Host', 'aggressive', { militia: 220, swordsman: 200, hoplite: 80, pikeman: 180, halberdier: 180, guardian: 140, berserker: 180, archer: 240, crossbow: 120, javelin: 100, slinger: 96, cavalry: 120, lancer: 50, horseArcher: 60, catapult: 12, ballista: 12, elephant: 8, commander: 2 })
      ]
    },
    {
      id: 'basin', name: 'Battle for the Ashen Heights', description: '6,000 soldiers. Artillery commands the heights while broad wings contest the basin.',
      map: 'basin', seed: 174401,
      armies: [
        army('The Ashen Compact', 'balanced', { militia: 280, swordsman: 300, hoplite: 120, pikeman: 320, halberdier: 240, guardian: 180, berserker: 140, archer: 380, crossbow: 180, javelin: 180, slinger: 184, cavalry: 230, lancer: 100, horseArcher: 120, catapult: 18, ballista: 16, elephant: 8, commander: 4 }),
        army('Ember Expedition', 'flank', { militia: 360, swordsman: 260, hoplite: 120, pikeman: 220, halberdier: 220, guardian: 200, berserker: 240, archer: 320, crossbow: 160, javelin: 180, slinger: 150, cavalry: 260, lancer: 120, horseArcher: 140, catapult: 18, ballista: 16, elephant: 12, commander: 4 })
      ]
    }
  ];

  // Explicit siege deployments keep engines at the head of the assault and
  // defenders inside the fortification. Their bodies use the ordinary layout.
  const siegeScene = (id, name, description, map, seed, groups) => {
    const counts = [{}, {}];
    const formations = groups.map(([team, type, count, x, y, formation]) => {
      counts[team][type] = (counts[team][type] || 0) + count;
      return { team, type, count, x, y, facing: team === 0 ? 0 : Math.PI, formation: formation || 'line' };
    });
    return { id, name, description, map, seed, formations,
      armies: [army('Azure Assault Host', 'aggressive', counts[0]), army('Ember Garrison', 'defensive', counts[1])] };
  };
  PRESETS.push(
    siegeScene('stonekeepAssault', 'Storm the Stonekeep', 'Rams batter the central gate while two siege towers create additional approaches. Command the assault or take control of the garrison.', 'stonekeep', 271201, [
      [0,'swordsman',160,2700,1800],[0,'swordsman',160,2700,2600],[0,'hoplite',160,2480,2200],
      [0,'pikeman',120,2480,3100],[0,'archer',160,2350,1450],[0,'crossbow',120,2350,2900],
      [0,'cavalry',60,2090,3440],[0,'catapult',4,2630,3540],[0,'ballista',2,2630,890],
      [0,'batteringRam',1,3150,2140],[0,'batteringRam',1,3150,2260],
      [0,'siegeTower',1,3160,1240],[0,'siegeTower',1,3160,3080],[0,'commander',1,2180,2200],
      [1,'guardian',160,3920,2200,'shieldwall'],[1,'hoplite',160,4180,2200,'shieldwall'],
      [1,'pikeman',80,3900,1500],[1,'pikeman',80,3900,2900],
      [1,'archer',120,4160,1570],[1,'archer',120,4160,2830],
      [1,'crossbow',80,3990,1240],[1,'crossbow',80,3990,3160],
      [1,'catapult',2,4580,2200],[1,'commander',1,4620,2530]
    ]),
    siegeScene('thunderAtStonekeep', 'Thunder at Stonekeep', 'Outside the west gate, Berserkers meet the garrison’s advance beneath musket fire. Protect the field guns and give the trebuchets time to load their next stone.', 'stonekeep', 271250, [
      [0,'berserker',48,2880,2310],[0,'swordsman',60,2820,2040],
      [0,'musketeer',32,2630,2320],[0,'cannon',2,2450,2070],[0,'trebuchet',2,2180,2350],
      [1,'militia',96,3090,2310],[1,'hoplite',64,3140,2040],[1,'musketeer',24,3330,1830],
      [1,'guardian',64,3890,2210],[1,'crossbow',32,4090,2400]
    ]),
    siegeScene('causewayAssault', 'The Siege of Two Bridges', 'Two causeways constrain the assault. Protect the engines across the moat, break a gate, then bring reserves through the opening.', 'moat', 271202, [
      [0,'swordsman',160,2680,1320],[0,'swordsman',160,2680,3080],
      [0,'hoplite',120,2410,1320],[0,'hoplite',120,2410,3080],
      [0,'archer',120,2160,1320],[0,'crossbow',120,2160,3080],
      [0,'catapult',2,2740,1640],[0,'catapult',2,2740,2760],
      [0,'batteringRam',1,2950,1320],[0,'batteringRam',1,2950,3080],
      [0,'siegeTower',1,2780,1320],[0,'siegeTower',1,2780,3080],[0,'commander',1,2090,2200],
      [1,'guardian',120,3890,1320,'shieldwall'],[1,'guardian',120,3890,3080,'shieldwall'],
      [1,'hoplite',120,4150,1320,'shieldwall'],[1,'hoplite',120,4150,3080,'shieldwall'],
      [1,'archer',160,4270,1680],[1,'archer',160,4270,2720],
      [1,'ballista',2,4200,2200],[1,'commander',1,4720,2200]
    ]),
    siegeScene('bastionAssault', 'Assault on Crownwatch', 'Three rocky approach lanes feed two defended gates and a central tower assault. Keep the columns moving and deploy on the far side.', 'mountainfort', 271203, [
      [0,'swordsman',160,2530,1160],[0,'swordsman',160,2530,3240],
      [0,'hoplite',160,2530,2200],[0,'pikeman',120,2250,2200],
      [0,'javelin',120,2260,1160],[0,'crossbow',120,2260,3240],
      [0,'catapult',3,3460,1850],[0,'batteringRam',1,3440,1160],[0,'batteringRam',1,3440,3240],
      [0,'siegeTower',1,3460,2200],[0,'commander',1,2110,2610],
      [1,'guardian',120,4040,1160,'shieldwall'],[1,'guardian',120,4040,3240,'shieldwall'],
      [1,'hoplite',160,4060,2200,'shieldwall'],[1,'pikeman',120,4330,2200],
      [1,'archer',160,4350,1350],[1,'crossbow',120,4350,3050],
      [1,'ballista',2,4900,1700],[1,'commander',1,4860,2900]
    ])
  );

  PRESETS.push(
    { id: 'sovereignWar', name: 'War of the Sovereign Plain', map: 'grandplain', seed: 282301,
      description: 'Two broad armies contest the heights. A braced infantry advance meets a rapid attempt to surround the connected enemy position.',
      armies: [
        army('The Sovereign Guard', 'steady', { militia: 200, swordsman: 400, pikeman: 320, hoplite: 400, halberdier: 160, guardian: 200, archer: 360, crossbow: 160, cavalry: 240, lancer: 160, catapult: 8, ballista: 4, commander: 4 }),
        army('The Free Banners', 'rapid', { militia: 400, swordsman: 300, berserker: 240, hoplite: 160, archer: 320, javelin: 240, slinger: 160, cavalry: 340, lancer: 240, horseArcher: 200, elephant: 8, commander: 4 })] },
    { id: 'steppeEnvelopment', name: 'The Great Envelopment', map: 'longgrass', seed: 282302,
      description: 'A huge mounted host has room for separated wings and distant reserves. Its opponent advances as a connected army across the colossal steppe.',
      armies: [
        army('The Horizon Host', 'rapid', { militia: 600, swordsman: 700, pikeman: 360, hoplite: 240, berserker: 300, archer: 560, javelin: 240, cavalry: 750, lancer: 500, horseArcher: 450, elephant: 12, commander: 6 }),
        army('The Bronze Alliance', 'steady', { militia: 500, swordsman: 600, pikeman: 700, hoplite: 800, guardian: 320, halberdier: 300, archer: 640, crossbow: 300, cavalry: 320, lancer: 240, catapult: 10, ballista: 10, commander: 6 })] },
    { id: 'silverwoodScreen', name: 'The Silverwood Ambush', map: 'silverwood', seed: 282303,
      description: 'An elastic woodland defense gives ground through connected clearings. A succession of assault waves must keep its supports close.',
      armies: [
        army('Silverwood Rangers', 'elastic', { swordsman: 240, hoplite: 160, pikeman: 120, archer: 360, crossbow: 120, javelin: 280, slinger: 240, cavalry: 100, horseArcher: 120, commander: 3 }),
        army('The Iron Columns', 'echelon', { militia: 240, swordsman: 400, guardian: 160, halberdier: 160, berserker: 240, archer: 240, crossbow: 160, cavalry: 120, lancer: 80, commander: 3 })] },
    { id: 'blackwoodWaves', name: 'War in the Blackwood', map: 'blackforest', seed: 282304,
      description: 'A staggered assault tests a refused flank among broad forest belts. Success depends on which avenue receives the reserves.',
      armies: [
        army('The Blackwood Legion', 'echelon', { militia: 360, swordsman: 560, hoplite: 240, halberdier: 200, guardian: 200, archer: 480, crossbow: 200, javelin: 240, cavalry: 160, lancer: 120, catapult: 6, commander: 4 }),
        army('The Thornward Pact', 'refused', { militia: 400, swordsman: 400, pikeman: 240, hoplite: 240, berserker: 320, archer: 400, javelin: 320, slinger: 200, cavalry: 220, horseArcher: 160, commander: 4 })] },
    { id: 'fiveCrossings', name: 'Five Bridges, One Front', map: 'fivefords', seed: 282305,
      description: 'A concentrated breakthrough must secure a crossing while defenders keep a braced river line across a much larger theater.',
      armies: [
        army('The River Expedition', 'breakthrough', { militia: 360, swordsman: 560, hoplite: 400, guardian: 240, halberdier: 160, archer: 400, crossbow: 240, cavalry: 200, lancer: 160, catapult: 10, elephant: 8, commander: 4 }),
        army('The Five Wardens', 'steady', { militia: 280, swordsman: 400, pikeman: 480, hoplite: 400, guardian: 320, archer: 480, crossbow: 280, cavalry: 160, ballista: 12, commander: 4 })] },
    { id: 'redstoneAssault', name: 'Through the Redstone Gates', map: 'redcanyon', seed: 282306,
      description: 'Successive attack waves pass through four canyon lanes. A compact defense yields and counterattacks where the attackers lose support.',
      armies: [
        army('The Copper Vanguard', 'echelon', { militia: 240, swordsman: 400, hoplite: 240, berserker: 160, halberdier: 160, archer: 280, crossbow: 160, cavalry: 160, lancer: 100, catapult: 8, commander: 3 }),
        army('The Redstone Wardens', 'elastic', { swordsman: 320, pikeman: 280, hoplite: 320, guardian: 240, archer: 280, crossbow: 200, javelin: 160, cavalry: 120, ballista: 6, commander: 3 })] },
    { id: 'copperTempest', name: 'Tempest of Copper', map: 'dunesea', seed: 282307,
      description: 'Two fast armies use the vast desert differently: one surrounds the local enemy position, while the other concentrates on a weak seam.',
      armies: [
        army('The Saffron Riders', 'rapid', { militia: 400, swordsman: 320, berserker: 240, archer: 320, javelin: 200, cavalry: 500, lancer: 360, horseArcher: 320, elephant: 10, commander: 4 }),
        army('The Copper Spear', 'breakthrough', { militia: 320, swordsman: 400, pikeman: 240, hoplite: 320, guardian: 200, crossbow: 280, cavalry: 420, lancer: 380, horseArcher: 180, elephant: 16, catapult: 8, commander: 4 })] },
    { id: 'wintermereWings', name: 'The Wintermere Offensive', map: 'frostlake', seed: 282308,
      description: 'A lake separates the central causeway from two wide outer approaches. A refused wing must hold long enough for the opposite assault to develop.',
      armies: [
        army('The Winter Crown', 'refused', { swordsman: 400, pikeman: 320, hoplite: 320, guardian: 240, archer: 400, crossbow: 200, cavalry: 240, lancer: 200, catapult: 8, commander: 4 }),
        army('The Northern Storm', 'rapid', { militia: 400, swordsman: 360, berserker: 280, halberdier: 200, archer: 320, javelin: 240, cavalry: 340, lancer: 240, horseArcher: 200, commander: 4 })] },
    { id: 'reedwaterLanding', name: 'The Reedwater Campaign', map: 'reeddelta', seed: 282309,
      description: 'Two riverbanks demand two separate deployments. Steady ranks contend with a skirmishing defense among the island groves.',
      armies: [
        army('The Island Legion', 'steady', { militia: 240, swordsman: 320, pikeman: 240, hoplite: 400, guardian: 160, archer: 280, crossbow: 160, cavalry: 100, catapult: 6, commander: 3 }),
        army('The Reedwater Bands', 'skirmish', { militia: 320, swordsman: 240, pikeman: 200, hoplite: 160, archer: 320, javelin: 320, slinger: 240, cavalry: 140, horseArcher: 100, commander: 3 })] },
    siegeScene('imperialAssault', 'Fall of the Imperial Rampart', 'Three gates and three tower approaches divide a large garrison. Keep a reserve ready to exploit the first established passage.', 'imperialwall', 282310, [
      [0,'swordsman',240,5940,2000],[0,'swordsman',240,5940,4400],[0,'swordsman',240,5940,6800],
      [0,'hoplite',160,5600,2000],[0,'hoplite',160,5600,4400],[0,'hoplite',160,5600,6800],
      [0,'archer',160,5250,1700],[0,'crossbow',160,5250,4700],[0,'archer',160,5250,7100],
      [0,'batteringRam',1,6540,1900],[0,'batteringRam',1,6540,2100],
      [0,'batteringRam',1,6540,4300],[0,'batteringRam',1,6540,4500],
      [0,'batteringRam',1,6540,6700],[0,'batteringRam',1,6540,6900],
      [0,'siegeTower',1,6500,3100],[0,'siegeTower',1,6500,5700],[0,'siegeTower',1,6500,8000],
      [0,'catapult',4,6200,3650],[0,'catapult',4,6200,6100],[0,'cavalry',120,5350,8000],[0,'commander',2,5050,4400],
      [1,'guardian',160,7500,2000,'shieldwall'],[1,'guardian',160,7500,4400,'shieldwall'],[1,'guardian',160,7500,6800,'shieldwall'],
      [1,'hoplite',160,7900,2000,'shieldwall'],[1,'hoplite',160,7900,4400,'shieldwall'],[1,'hoplite',160,7900,6800,'shieldwall'],
      [1,'pikeman',120,7520,3100],[1,'pikeman',120,7520,5700],[1,'pikeman',120,7520,8000],
      [1,'archer',160,8250,2300],[1,'crossbow',160,8250,4700],[1,'archer',160,8250,7100],
      [1,'ballista',4,8600,3500],[1,'cavalry',100,8500,5500],[1,'commander',2,9000,4400]
    ]),
    siegeScene('floodgateAssault', 'The Three Causeways', 'Escort covered rams across three separate bridges, open the gates, then lead ordered infantry through the bridgeheads.', 'floodgate', 282311, [
      [0,'swordsman',160,3950,1480],[0,'swordsman',160,3950,3300],[0,'swordsman',160,3950,5120],
      [0,'hoplite',120,3600,1480],[0,'hoplite',120,3600,3300],[0,'hoplite',120,3600,5120],
      [0,'archer',120,3250,1480],[0,'crossbow',120,3250,3300],[0,'archer',120,3250,5120],
      [0,'batteringRam',1,4400,1480],[0,'batteringRam',1,4400,3300],[0,'batteringRam',1,4400,5120],
      [0,'siegeTower',1,4150,1480],[0,'siegeTower',1,4150,3300],[0,'siegeTower',1,4150,5120],
      [0,'catapult',4,4200,2150],[0,'catapult',4,4200,4450],[0,'commander',2,2950,3300],
      [1,'guardian',160,5800,1480,'shieldwall'],[1,'guardian',160,5800,3300,'shieldwall'],[1,'guardian',160,5800,5120,'shieldwall'],
      [1,'hoplite',120,6150,1480,'shieldwall'],[1,'hoplite',120,6150,3300,'shieldwall'],[1,'hoplite',120,6150,5120,'shieldwall'],
      [1,'archer',160,6500,1800],[1,'crossbow',160,6500,3620],[1,'archer',160,6500,5440],
      [1,'ballista',3,6800,2450],[1,'ballista',3,6800,4200],[1,'commander',2,7100,3300]
    ]),
    siegeScene('threeCrownsAssault', 'Siege of the Three Crowns', 'A colossal mountain fortress with three entrances and an inner redoubt. Open several approaches and leave room for the assault columns to unfold.', 'highcitadel', 282312, [
      [0,'swordsman',240,7380,2300],[0,'swordsman',240,7380,5500],[0,'swordsman',240,7380,8700],
      [0,'hoplite',240,7000,2300],[0,'hoplite',240,7000,5500],[0,'hoplite',240,7000,8700],
      [0,'archer',160,6600,2300],[0,'crossbow',160,6600,5500],[0,'archer',160,6600,8700],
      [0,'batteringRam',1,8840,2200],[0,'batteringRam',1,8840,2400],
      [0,'batteringRam',1,8840,5400],[0,'batteringRam',1,8840,5600],
      [0,'batteringRam',1,8840,8600],[0,'batteringRam',1,8840,8800],
      [0,'siegeTower',1,8950,3450],[0,'siegeTower',1,8950,7050],
      [0,'catapult',4,8900,4000],[0,'catapult',4,8900,7650],
      [0,'cavalry',160,6500,3800],[0,'lancer',120,6500,7000],[0,'commander',2,6150,5500],
      [1,'guardian',160,9700,2300,'shieldwall'],[1,'guardian',160,9700,5500,'shieldwall'],[1,'guardian',160,9700,8700,'shieldwall'],
      [1,'hoplite',160,10080,2300,'shieldwall'],[1,'hoplite',160,10080,5500,'shieldwall'],[1,'hoplite',160,10080,8700,'shieldwall'],
      [1,'pikeman',120,9700,3450],[1,'pikeman',120,9700,7050],
      [1,'archer',160,10400,2600],[1,'crossbow',160,10400,5800],[1,'archer',160,10400,9000],
      [1,'guardian',160,11450,5000,'shieldwall'],[1,'archer',160,11850,5300],
      [1,'ballista',4,11800,4200],[1,'cavalry',120,11800,7050],[1,'commander',2,12500,5500]
    ])
  );

  // Schematic, scaled historical deployments. The cited sources support the
  // arrangement, not the fictional statistics or an exact reconstructed map.
  // Explicit bodies preserve the distinctive frontage, reserves, and flanks.
  const historicalGroup = (team, type, count, x, y, name, columns, formation, facing) => {
    const t = TYPES[type], angle = Number.isFinite(facing) ? facing : team === 0 ? 0 : Math.PI;
    const group = { team, type, count, x, y, name, facing: angle, formation: formation || (type === 'musketeer' ? 'volley' : 'line') };
    // Pointed formations use the engine's triangular layout. A rectangular
    // saved offset array would otherwise overwrite that wedge at deployment.
    if (columns && type !== 'musketeer' && group.formation !== 'wedge') {
      const spacing = t.siege ? 58 : t.mounted ? 30 : 17;
      const rows = Math.ceil(count / columns), nx = Math.cos(angle), ny = Math.sin(angle);
      group.offsets = Array.from({ length: count }, (_, slot) => {
        const forward = ((rows - 1) / 2 - Math.floor(slot / columns)) * spacing;
        const cross = ((slot % columns) - (columns - 1) / 2) * spacing;
        return { x: forward * nx - cross * ny, y: forward * ny + cross * nx };
      });
    }
    return group;
  };
  const historySource = (title, url) => ({ title, url });
  const historicalScene = spec => {
    const formations = spec.groups.map(g => historicalGroup(...g)), counts = [{}, {}];
    for (const g of formations) counts[g.team][g.type] = (counts[g.team][g.type] || 0) + g.count;
    const historical = {
      year: spec.year, era: spec.era, location: spec.location, summary: spec.summary,
      deployment: spec.deployment,
      scaleNote: 'Scaled representative deployment, with compressed troop numbers and terrain. Historical unit roles use the game roster; results develop freely. ' + (spec.scaleNote || ''),
      unitNotes: spec.unitNotes || 'Equipment, armor, and movement use IRONFIELD troop archetypes rather than exact period statistics.',
      sources: spec.sources
    };
    return { id: spec.id, name: spec.name, description: spec.summary, map: 'custom', seed: spec.seed || Math.abs(spec.year * 7919 + spec.id.length * 101),
      customMap: { name: spec.name + ' · historical field', width: 6400, height: 4400, biome: spec.biome || 'grassland', regions: spec.regions || [] },
      formations, armies: spec.armies.map((a, team) => army(a[0], a[1], counts[team])), historical };
  };
  const HISTORICAL_BATTLES = Object.freeze([
    historicalScene({ id: 'historyMarathon', name: 'Marathon', year: -490, era: 'Ancient', location: 'Attica, Greece',
      summary: 'A thin Greek center and stronger hoplite wings face the Persian landing army.',
      deployment: 'Athens and Plataea occupy a continuous front, with shallow central ranks and deeper wings. Persian strength is concentrated opposite the center; marsh and coastal water bound the field.',
      sources: [historySource('Herodotus: Marathon · University of Texas at Arlington', 'https://websites.uta.edu/hunnicut/reading-list/readings-western-civilization/herodotus-the-battle-of-marathon/')],
      unitNotes: 'Hoplites represent Greek heavy foot; Persian infantry and archers use light-foot archetypes. No Persian cavalry is placed: its role in this battle is uncertain.',
      armies: [['Athens & Plataea', 'hammer'], ['Persian Expedition', 'breakthrough']],
      regions: [region('water', 5500, 0, 900, 4400), region('water', 3200, 0, 1900, 450), region('hill', 900, 1200, 900, 2200)],
      groups: [[0,'hoplite',120,2450,1400,'Plataean left wing',12,'shieldwall'],[0,'hoplite',64,2450,2200,'Shallow Greek center',32,'line'],[0,'hoplite',120,2450,3000,'Athenian right wing',12,'shieldwall'],[0,'commander',1,2150,2200,'Miltiades'],
        [1,'militia',120,3900,1400,'Persian right',20],[1,'swordsman',160,3850,2200,'Persians & Sacae',20],[1,'militia',120,3900,3000,'Persian left',20],[1,'archer',60,3700,1450,'Persian bow screen',30,'loose'],[1,'archer',60,3700,2950,'Persian bow screen',30,'loose'],[1,'commander',1,4300,2200,'Datis & Artaphernes']] }),
    historicalScene({ id: 'historyLeuctra', name: 'Leuctra', year: -371, era: 'Ancient', location: 'Boeotia, Greece',
      summary: 'Theban mass on the left confronts the Spartan royal right while cavalry screens the infantry.',
      deployment: 'The Theban left is much deeper than its center and refused right. The Spartan king stands opposite that concentration. Both sides begin with horse in front of their hoplites.',
      sources: [historySource('Xenophon: Hellenica VI.4 · Fordham University', 'https://sourcebooks.web.fordham.edu/ancient/371leuctra.asp')],
      scaleNote: 'The famous fifty-shield depth is represented by a proportionally deep block, not fifty full-size ranks.',
      armies: [['Thebes & Boeotia', 'refuseLeft'], ['Sparta & Allies', 'steady']],
      regions: [region('hill', 1200, 700, 1000, 2700),region('hill', 4600, 600, 1000, 2600)],
      groups: [[0,'hoplite',240,2650,1350,'Deep Theban left',12,'shieldwall'],[0,'hoplite',96,2350,2200,'Boeotian center',24],[0,'hoplite',72,2100,2900,'Refused Boeotian right',24],[0,'cavalry',36,3000,1400,'Theban cavalry screen',12],[0,'commander',1,2350,1350,'Epaminondas'],
        [1,'hoplite',144,3900,1350,'Spartan royal right',18,'shieldwall'],[1,'hoplite',144,3900,2200,'Peloponnesian center',24],[1,'hoplite',120,3900,2900,'Allied left',24],[1,'cavalry',30,3550,1400,'Spartan cavalry screen',10],[1,'commander',1,4200,1350,'Cleombrotus']] }),
    historicalScene({ id: 'historyGaugamela', name: 'Gaugamela', year: -331, era: 'Ancient', location: 'Northern Mesopotamia', biome: 'desert',
      summary: 'Alexander’s mounted right supports a pike center against a wider Persian battle line.',
      deployment: 'The Macedonian phalanx holds the center, the Companions the right, and Parmenion the left. Persian cavalry extends both wings and Darius commands from the center.',
      sources: [historySource('Arrian: Alexander at Gaugamela · University of Texas at Arlington', 'https://websites.uta.edu/hunnicut/reading-list/readings-western-civilization/arrian-alexander-at-the-battle-of-gaugamela/')],
      unitNotes: 'Pikemen stand for the sarissa phalanx, lancers for Companions, hoplites for Greek mercenaries. Scythed chariots are omitted because the roster has no equivalent.',
      armies: [['Alexander’s Army', 'hammer'], ['Darius III’s Army', 'flank']], regions: [region('hill', 700, 500, 650, 1100)],
      groups: [[0,'cavalry',48,2200,1100,'Parmenion’s left',12],[0,'pikeman',144,2500,1750,'Phalanx left',18],[0,'pikeman',144,2500,2350,'Phalanx right',18],[0,'hoplite',80,2600,2850,'Hypaspists',16],[0,'lancer',64,2700,3350,'Companion Cavalry',16,'wedge'],[0,'javelin',40,2900,3000,'Agrianian screen',20,'loose'],[0,'commander',1,2400,3300,'Alexander'],
        [1,'horseArcher',48,3950,650,'Persian outer right',12,'loose'],[1,'cavalry',64,3900,1200,'Persian right cavalry',16],[1,'hoplite',120,4000,1900,'Greek mercenary center',20],[1,'militia',160,4000,2500,'Persian center',20],[1,'cavalry',80,3900,3200,'Bactrian cavalry',16],[1,'horseArcher',48,3950,3800,'Sacaean horse',12,'loose'],[1,'commander',1,4400,2200,'Darius III']] }),
    historicalScene({ id: 'historyCannae', name: 'Cannae', year: -216, era: 'Ancient', location: 'Apulia, Italy',
      summary: 'Hannibal’s projecting center, African wing reserves, and superior cavalry meet dense Roman infantry.',
      deployment: 'Iberian and Gallic companies form a forward-bowed center. African veterans wait back on the wings, with cavalry outside them. Rome concentrates deep infantry between its cavalry flanks.',
      sources: [historySource('Polybius: Histories III.107–118 · Fordham University', 'https://sourcebooks.web.fordham.edu/ancient/polybius-cannae.asp')],
      unitNotes: 'Swordsmen represent legionaries and Iberian/Gallic infantry; hoplites represent African heavy foot. Numidian light cavalry use the cavalry archetype, without invented horse archers.',
      armies: [['Hannibal’s Army', 'hammer'], ['Roman Consular Army', 'breakthrough']], regions: [region('water', 0, 0, 6400, 400)],
      groups: [[0,'cavalry',96,2400,850,'Iberian & Gallic cavalry',16],[0,'hoplite',100,2400,1400,'African left reserve',10,'shieldwall'],[0,'swordsman',72,2700,1800,'Iberian left center',18],[0,'militia',96,2900,2200,'Projecting Gallic center',24],[0,'swordsman',72,2700,2600,'Iberian right center',18],[0,'hoplite',100,2400,3000,'African right reserve',10,'shieldwall'],[0,'cavalry',60,2400,3600,'Numidian cavalry',15,'loose'],[0,'slinger',48,3200,2200,'Forward light troops',24,'loose'],[0,'commander',1,2650,2200,'Hannibal'],
        [1,'cavalry',40,3900,850,'Roman citizen cavalry',10],[1,'swordsman',200,3900,1700,'Roman massed right',10],[1,'swordsman',200,3900,2200,'Roman massed center',10],[1,'swordsman',200,3900,2700,'Roman massed left',10],[1,'javelin',60,3550,2200,'Velite screen',30,'loose'],[1,'cavalry',60,3900,3550,'Allied cavalry',12],[1,'commander',1,4450,2200,'Consuls Paullus & Varro']] }),
    historicalScene({ id: 'historyHastings', name: 'Hastings', year: 1066, era: 'Medieval', location: 'Sussex, England',
      summary: 'Harold’s ridge-top shield wall receives Norman archers, infantry, and cavalry in successive depths.',
      deployment: 'English foot occupy the hill in a joined shield wall. Norman missile troops lead, with infantry behind and mounted troops farther back across three sectors.',
      sources: [historySource('What happened at Hastings · English Heritage', 'https://www.english-heritage.org.uk/visit/places/1066-battle-of-hastings-abbey-and-battlefield/history-and-stories/what-happened-battle-hastings')],
      unitNotes: 'Swordsmen and halberdiers approximate English shield bearers and axe-armed housecarls; poleaxes and cavalry equipment are generic visual stand-ins.',
      armies: [['Harold’s English Host', 'defensive'], ['William’s Norman Host', 'echelon']], regions: [region('hill', 1700, 850, 1200, 2800),region('forest', 600, 300, 800, 3800)],
      groups: [[0,'swordsman',160,2400,1400,'English left shield wall',32,'shieldwall'],[0,'halberdier',120,2400,2200,'Royal housecarls',30,'shieldwall'],[0,'swordsman',160,2400,3000,'English right shield wall',32,'shieldwall'],[0,'militia',120,2100,2200,'Fyrd behind the front',24,'shieldwall'],[0,'commander',1,2050,1850,'Harold’s standard'],
        [1,'archer',72,3500,1400,'Franco-Flemish bowmen',24],[1,'archer',72,3500,2200,'Norman bowmen',24],[1,'archer',72,3500,3000,'Breton bowmen',24],[1,'swordsman',120,3850,1400,'French infantry',24],[1,'swordsman',120,3850,2200,'Norman infantry',24],[1,'swordsman',120,3850,3000,'Breton infantry',24],[1,'lancer',48,4300,1400,'French horse',12],[1,'lancer',60,4300,2200,'Norman horse',15],[1,'lancer',48,4300,3000,'Breton horse',12],[1,'commander',1,4650,2200,'William']] }),
    historicalScene({ id: 'historyBannockburn', name: 'Bannockburn', year: 1314, era: 'Medieval', location: 'Stirling, Scotland',
      summary: 'Scottish spear blocks advance against crowded English cavalry with limited room to deploy.',
      deployment: 'Three Scottish schiltron blocks lead with a reserve behind. English cavalry occupy the forward space, with infantry crowded farther back between constrained flanks.',
      sources: [historySource('Bannockburn battlefield inventory · Historic Environment Scotland', 'https://portal.historicenvironment.scot/apex/f?p=1505%3A300%3A%3A%3A%3A%3AVIEWTYPE%2CVIEWREF%3Adesignation%2CBTL4')],
      scaleNote: 'This is a schematic second-day arrangement; the precise location and layout remain debated.', unitNotes: 'Pike blocks stand for schiltrons. The game cannot reproduce every hedge, bog, or the historical English command failure.',
      armies: [['Robert Bruce’s Scots', 'steady'], ['Edward II’s Host', 'aggressive']], regions: [region('forest', 900, 600, 1000, 3200),region('water', 3350, 0, 2100, 650),region('water', 3350, 3750, 2100, 650)],
      groups: [[0,'pikeman',160,2550,1400,'Moray’s schiltron',16,'shieldwall'],[0,'pikeman',160,2750,2200,'Edward Bruce’s schiltron',16,'shieldwall'],[0,'pikeman',160,2550,3000,'Douglas & Stewart',16,'shieldwall'],[0,'pikeman',120,2050,2200,'Bruce’s reserve',12,'shieldwall'],[0,'cavalry',40,2200,3450,'Keith’s light horse',10],[0,'archer',48,2150,1100,'Scottish archers',24],[0,'commander',1,1850,2200,'Robert Bruce'],
        [1,'lancer',72,3600,1400,'English vanguard',12],[1,'lancer',72,3600,2200,'English vanguard',12],[1,'cavalry',72,3650,3000,'Mounted men-at-arms',12],[1,'militia',200,4100,1800,'Crowded infantry',16],[1,'militia',200,4100,2700,'English rearward foot',16],[1,'archer',96,4400,2200,'English & Welsh bowmen',24],[1,'commander',1,4700,2200,'Edward II']] }),
    historicalScene({ id: 'historyAgincourt', name: 'Agincourt', year: 1415, era: 'Medieval', location: 'Artois, France',
      summary: 'An archer-heavy English front holds a narrow corridor against French men-at-arms and mounted wings.',
      deployment: 'English men-at-arms occupy the center, with longbow companies on either side. French foot stand in successive deep bodies, with mounted detachments at the flanks. Woods compress the frontage.',
      sources: [historySource('The battle of Agincourt · College of Arms', 'https://www.college-of-arms.gov.uk/news-grants/news/item/119-the-battle-of-agincourt')],
      scaleNote: 'Archer-wing placement is a conventional reconstruction; exact dispositions and the battlefield site are disputed. Stakes and mud are not separately simulated.',
      armies: [['Henry V’s Army', 'defensive'], ['French Royal Army', 'aggressive']], regions: [region('forest', 1200, 0, 3900, 1000),region('forest', 1200, 3400, 3900, 1000)],
      groups: [[0,'archer',144,2500,1350,'English & Welsh left bow',36],[0,'guardian',64,2550,2200,'Dismounted men-at-arms',32,'shieldwall'],[0,'archer',144,2500,3050,'English & Welsh right bow',36],[0,'archer',72,2200,1700,'Supporting bowmen',24],[0,'archer',72,2200,2700,'Supporting bowmen',24],[0,'commander',1,2100,2200,'Henry V'],
        [1,'guardian',160,3650,2200,'French vanguard',16],[1,'guardian',160,4100,2200,'French main battle',16],[1,'swordsman',160,4550,2200,'French rear battle',16],[1,'lancer',48,3700,1300,'Mounted left attack',12],[1,'lancer',48,3700,3100,'Mounted right attack',12],[1,'crossbow',48,4550,1350,'French crossbow reserve',24],[1,'commander',1,4850,2200,'French constable']] }),
    historicalScene({ id: 'historyBosworth', name: 'Bosworth', year: 1485, era: 'Medieval', location: 'Leicestershire, England',
      summary: 'Tudor foot face Richard’s larger host and field guns beside marshy ground.',
      deployment: 'Oxford’s compact vanguard shields Henry’s reserve against the Royal front. Richard’s mounted household sits behind his center, with the Stanley contingent represented at the Tudor flank.',
      sources: [historySource('Bosworth registered battlefield · Historic England', 'https://historicengland.org.uk/listing/the-list/list-entry/1000004?section=official-list-entry')],
      scaleNote: 'Deployments are conjectural. The two-team engine places the Stanleys with Henry from the start and cannot reproduce their delayed commitment or Northumberland’s inactivity.',
      armies: [['Henry Tudor & Stanley', 'defensive'], ['Richard III’s Royal Host', 'aggressive']], regions: [region('water', 2900, 0, 1400, 700),region('water', 2900, 3300, 1400, 1100),region('hill', 4400, 1200, 900, 1700)],
      groups: [[0,'halberdier',120,2450,1900,'Oxford’s vanguard',24],[0,'swordsman',120,2450,2500,'Tudor vanguard',24],[0,'archer',80,2250,1300,'Welsh archers',20],[0,'pikeman',96,1950,2200,'French foot',16],[0,'cavalry',48,1900,3050,'Stanley contingent',12],[0,'commander',1,1600,2200,'Henry Tudor'],
        [1,'archer',96,3500,1600,'Royal archers',24],[1,'halberdier',160,3800,1950,'Norfolk’s battle',20],[1,'swordsman',160,3800,2650,'Royal infantry',20],[1,'cannon',3,3500,2550,'Royal field guns',3],[1,'lancer',48,4350,2200,'Richard’s household',12],[1,'commander',1,4650,2200,'Richard III']] }),
    historicalScene({ id: 'historyNaseby', name: 'Naseby', year: 1645, era: 'Pike & shot', location: 'Northamptonshire, England',
      summary: 'Pike-and-shot centers contend between cavalry wings, with Parliamentary dragoons on a covered flank.',
      deployment: 'Both armies place foot in the center and cavalry on each wing. Cromwell leads Parliament’s right, Rupert the opposing Royalist right. A dismounted musket detachment represents the hedgerow dragoons.',
      sources: [historySource('Battle of Naseby · National Army Museum', 'https://www.nam.ac.uk/explore/battle-naseby')],
      unitNotes: 'Musketeers use the game’s three-rank drill instead of exact Civil War depths. Pike and shot are separate neighboring companies; dragoons begin dismounted.',
      armies: [['Fairfax’s New Model Army', 'hammer'], ['Charles I’s Royalists', 'aggressive']], regions: [region('hill', 1400, 800, 900, 2800),region('hill', 4200, 800, 900, 2800),region('forest', 2900, 500, 500, 500)],
      groups: [[0,'cavalry',72,2450,1000,'Ireton’s left horse',12],[0,'musketeer',60,2550,1700,'New Model shot left'],[0,'pikeman',80,2500,2200,'New Model pikes',16],[0,'musketeer',60,2550,2700,'New Model shot right'],[0,'cavalry',96,2450,3450,'Cromwell’s right horse',16],[0,'musketeer',30,3050,700,'Okey’s dismounted dragoons',0,'loose'],[0,'commander',1,2000,2200,'Fairfax'],
        [1,'cavalry',72,3900,1000,'Rupert’s right horse',12],[1,'musketeer',60,3850,1700,'Royalist shot right'],[1,'pikeman',80,3900,2200,'Royalist pikes',16],[1,'musketeer',60,3850,2700,'Royalist shot left'],[1,'cavalry',60,3900,3450,'Langdale’s left horse',12],[1,'cannon',2,4150,2200,'Royalist guns',2],[1,'commander',1,4400,2200,'Charles I']] }),
    historicalScene({ id: 'historyBlenheim', name: 'Blenheim', year: 1704, era: 'Musket age', location: 'Bavaria, Germany',
      summary: 'Allied infantry and horse cross the Nebel toward three defended village sectors.',
      deployment: 'French and Bavarian troops anchor on Blenheim, Oberglau, and Lutzingen, with weaker gaps between. Marlborough’s wing and Eugene’s wing begin opposite those sectors; the Danube closes one flank.',
      sources: [historySource('Battle of Blenheim · National Army Museum', 'https://www.nam.ac.uk/explore/battle-blenheim')],
      unitNotes: 'Woodland cover stands in for village clusters; the shallow Nebel is represented by several open crossings. The map is a schematic sector layout.',
      armies: [['Marlborough & Eugene', 'breakthrough'], ['French & Bavarian Army', 'defensive']], regions: [region('water', 0, 0, 6400, 500),region('water', 3200, 500, 160, 3900),region('bridge', 3100, 950, 360, 420),region('bridge', 3100, 2000, 360, 420),region('bridge', 3100, 3050, 360, 420),region('forest', 3750, 700, 500, 600),region('forest', 3750, 1850, 500, 600),region('forest', 3750, 3000, 500, 600)],
      groups: [[0,'musketeer',96,2500,1100,'Marlborough’s infantry'],[0,'musketeer',96,2450,2200,'Allied center'],[0,'musketeer',96,2500,3300,'Eugene’s infantry'],[0,'cavalry',72,2100,1700,'Allied center horse',12],[0,'cavalry',72,2100,2700,'Allied cavalry reserve',12],[0,'cannon',4,2100,2200,'Allied batteries',4],[0,'commander',1,1800,2500,'Marlborough'],
        [1,'musketeer',144,4000,1050,'Blenheim defenders'],[1,'musketeer',72,4100,2200,'Oberglau defenders'],[1,'musketeer',120,4000,3300,'Lutzingen defenders'],[1,'cavalry',60,4350,1600,'French center horse',12],[1,'cavalry',60,4350,2800,'French cavalry reserve',12],[1,'cannon',4,3700,2200,'French batteries',4],[1,'commander',1,4800,2600,'Tallard']] }),
    historicalScene({ id: 'historyDettingen', name: 'Dettingen', year: 1743, era: 'Musket age', location: 'Main Valley, Germany',
      summary: 'Two infantry lines with cavalry flanks meet beside the Main and a dangerous rearward ravine.',
      deployment: 'Both armies have two central infantry lines and horse on the wings. French guns fire from beyond the Main, while Gramont’s troops have left the ravine behind their battle line.',
      sources: [historySource('Battle of Dettingen · National Army Museum', 'https://www.nam.ac.uk/explore/battle-dettingen')],
      armies: [['George II’s Pragmatic Army', 'steady'], ['Gramont’s French Army', 'aggressive']], regions: [region('water', 0, 550, 6400, 350),region('water', 4550, 900, 160, 2800),region('bridge', 4400, 2000, 500, 400),region('hill', 800, 3600, 4700, 800)],
      groups: [[0,'cavalry',48,2450,1050,'Allied left horse',12],[0,'musketeer',96,2600,1800,'Allied first line left'],[0,'musketeer',96,2600,2650,'Allied first line right'],[0,'musketeer',96,2150,2200,'Allied second line'],[0,'cavalry',48,2450,3300,'Allied right horse',12],[0,'cannon',3,2400,2200,'Allied guns',3],[0,'commander',1,1800,2200,'George II'],
        [1,'cavalry',72,3850,1050,'Maison du Roi',12],[1,'musketeer',96,3800,1800,'French Guards'],[1,'musketeer',96,3800,2650,'French first line'],[1,'musketeer',96,4250,2200,'French second line'],[1,'cavalry',48,3850,3300,'French left horse',12],[1,'cannon',4,3300,300,'French cross-river battery',4],[1,'commander',1,4250,1500,'Gramont']] }),
    historicalScene({ id: 'historyCulloden', name: 'Culloden', year: 1746, era: 'Musket age', location: 'Inverness, Scotland', biome: 'highland',
      summary: 'Jacobite clan sectors confront Government musket lines, guns, and flanking cavalry.',
      deployment: 'A broad Jacobite front faces Government infantry in two depths with guns forward and horse outside. Wet ground impedes one Jacobite wing, while enclosures bound the approach.',
      sources: [historySource('Battle of Culloden · National Army Museum', 'https://www.nam.ac.uk/explore/battle-culloden?ms=homepage'),historySource('Culloden inventory · Historic Environment Scotland', 'https://portal.historicenvironment.scot/apex/f?p=1505%3A300%3A%3A%3A%3AVIEWTYPE%2CVIEWREF%3Adesignation%2CBTL6')],
      unitNotes: 'Swordsmen represent charging clan foot; their generic shields and armor differ from Highland equipment. Government and Jacobite shot share the three-rank game drill.',
      armies: [['Jacobite Army', 'aggressive'], ['Cumberland’s Government Army', 'defensive']], regions: [region('water', 2950, 650, 400, 500),region('rock', 2100, 0, 2300, 300),region('rock', 2100, 3900, 2300, 300),region('hill', 2700, 1600, 900, 1600)],
      groups: [[0,'swordsman',120,2550,1350,'MacDonald wing',24],[0,'swordsman',160,2550,2200,'Jacobite center',32],[0,'swordsman',160,2550,3050,'Clan Chattan right',32],[0,'musketeer',60,2100,2200,'Jacobite regular reserve'],[0,'cannon',2,2800,2200,'Jacobite light guns',2],[0,'commander',1,1800,2200,'Charles Edward Stuart'],
        [1,'musketeer',96,3900,1350,'Government first line north'],[1,'musketeer',96,3900,2200,'Government first line center'],[1,'musketeer',96,3900,3050,'Barrell & Munro sector'],[1,'musketeer',96,4400,2200,'Government second line'],[1,'cannon',4,3550,2200,'Government artillery',4],[1,'cavalry',48,4300,650,'Government northern horse',12],[1,'cavalry',48,4300,3600,'Government southern horse',12],[1,'commander',1,4800,2200,'Cumberland']] }),
    historicalScene({ id: 'historyBunkerHill', name: 'Bunker Hill', year: 1775, era: 'Musket age', location: 'Massachusetts, North America',
      summary: 'Provincial defenders occupy Breed’s Hill and a fence line against a British frontal and flank advance.',
      deployment: 'One Provincial body holds the hill redoubt sector while another extends toward the Mystic shore. British battalions prepare an uphill attack and a separate shoreward approach.',
      sources: [historySource('The Battle of Bunker Hill · National Park Service', 'https://home.nps.gov/articles/000/the-battle-of-bunker-hill.htm')],
      unitNotes: 'Hill and sparse wall sections approximate earthworks and rail fences. The game does not model the defenders’ historical ammunition shortage.',
      armies: [['Provincial Army', 'defensive'], ['British Army at Boston', 'aggressive']], regions: [region('hill', 1700, 1800, 1300, 1700),region('water', 0, 0, 6400, 500),region('wall', 2900, 2000, 80, 450),region('wall', 2900, 2750, 80, 450),region('hill', 2200, 800, 600, 900)],
      groups: [[0,'musketeer',96,2650,1200,'Provincial fence line'],[0,'musketeer',96,2650,1850,'Fence-line continuation'],[0,'musketeer',120,2600,2600,'Breed’s Hill redoubt sector'],[0,'musketeer',60,2100,2600,'Provincial reserve'],[0,'cannon',2,2500,3250,'Provincial light guns',2],[0,'commander',1,2050,2200,'Prescott & Putnam'],
        [1,'musketeer',96,3900,900,'British shoreward attack'],[1,'musketeer',96,3900,1750,'British flank battalions'],[1,'musketeer',120,3900,2600,'British redoubt attack'],[1,'musketeer',96,4400,2600,'British supports'],[1,'cannon',3,4400,3350,'British guns',3],[1,'commander',1,4700,2200,'Howe']] }),
    historicalScene({ id: 'historyFreemansFarm', name: 'Freeman’s Farm', year: 1777, era: 'Musket age', location: 'New York, North America', biome: 'woodland',
      summary: 'American riflemen and supporting foot meet separated British and German columns among farm clearings.',
      deployment: 'The Crown advances in three separated sectors, with Hamilton in the center, Fraser inland, and Riedesel by the Hudson. American detachments occupy the central and inland woodland approaches.',
      sources: [historySource('Battle of Freeman’s Farm · National Park Service', 'https://home.nps.gov/sara/learn/historyculture/battle-of-freemans-farm.htm')],
      scaleNote: 'Three approach sectors are present together; reinforcements and their historical arrival times are not scripted.', unitNotes: 'Loose-order musketeers represent Morgan’s riflemen, without separate rifle ballistics.',
      armies: [['Arnold’s American Division', 'skirmish'], ['Burgoyne’s Crown Army', 'echelon']], regions: [region('forest', 1100, 700, 1300, 1300),region('forest', 2700, 1600, 700, 900),region('forest', 3400, 2900, 1200, 800),region('water', 0, 3900, 6400, 500)],
      groups: [[0,'musketeer',48,2800,2200,'Morgan’s riflemen',0,'loose'],[0,'musketeer',96,2300,1350,'American inland supports'],[0,'musketeer',96,2300,2600,'American center supports'],[0,'musketeer',96,1800,2200,'Arnold’s reserve'],[0,'commander',1,1500,2200,'Benedict Arnold'],
        [1,'musketeer',96,3850,1000,'Fraser’s inland column',0,'column'],[1,'musketeer',120,3850,2200,'Hamilton’s center'],[1,'musketeer',96,4300,3450,'Riedesel’s river column',0,'column'],[1,'cannon',3,4200,2600,'Crown field guns',3],[1,'commander',1,4700,2200,'John Burgoyne']] }),
    historicalScene({ id: 'historyCowpens', name: 'Cowpens', year: 1781, era: 'Musket age', location: 'South Carolina, North America',
      summary: 'Morgan’s forward screen, militia line, and Continental reserve receive Tarleton’s attack.',
      deployment: 'Three American infantry positions stand one behind another, with cavalry behind a low hill. British foot and light guns lead their advance, with dragoon reserves on a wing.',
      sources: [historySource('The Battle of Cowpens · National Park Service', 'https://www.nps.gov/cowp/learn/historyculture/the-battle-of-cowpens.htm')],
      scaleNote: 'Militia withdrawal after two volleys must be ordered by the player; the scenario does not force the historical sequence.', unitNotes: 'All firearm infantry use musketeers. Differences in militia training, rifle range, and ammunition are not individually represented.',
      armies: [['Daniel Morgan’s Army', 'elastic'], ['Tarleton’s Legion', 'aggressive']], regions: [region('hill', 1400, 2600, 700, 1000),region('forest', 1100, 500, 900, 700),region('forest', 4400, 600, 900, 650)],
      groups: [[0,'musketeer',30,2950,2200,'Forward rifle screen',0,'loose'],[0,'musketeer',60,2500,2200,'Pickens’s militia'],[0,'musketeer',96,1950,2200,'Howard’s Continentals'],[0,'cavalry',36,1500,3000,'Washington’s cavalry reserve',12],[0,'commander',1,1400,2200,'Daniel Morgan'],
        [1,'musketeer',90,3800,1800,'British regular front'],[1,'musketeer',90,3800,2650,'Legion infantry front'],[1,'musketeer',60,4300,2200,'71st Foot reserve'],[1,'cannon',2,3550,2200,'British light guns',2],[1,'cavalry',48,4400,3150,'British dragoon reserve',12],[1,'commander',1,4650,2200,'Banastre Tarleton']] }),
    historicalScene({ id: 'historyGuilford', name: 'Guilford Courthouse', year: 1781, era: 'Musket age', location: 'North Carolina, North America', biome: 'woodland',
      summary: 'Greene’s three lines trade depth for ground while British battalions advance through woods.',
      deployment: 'North Carolina militia occupy the first position, Virginia militia the second, and Continentals the third. American detachments guard both flanks; British foot form a broad advancing front.',
      sources: [historySource('Guilford Courthouse cultural landscape · National Park Service', 'https://home.nps.gov/articles/000/guilford-courthouse-national-military-park-550002.htm')],
      unitNotes: 'Militia and Continentals share musket statistics. Woodland and spacing represent the layered defense; historical withdrawals are left to battlefield orders.',
      armies: [['Nathanael Greene’s Army', 'elastic'], ['Cornwallis’s Army', 'aggressive']], regions: [region('forest', 2000, 600, 450, 3200),region('forest', 3100, 600, 450, 3200),region('hill', 1000, 1500, 600, 1400)],
      groups: [[0,'musketeer',96,2900,2200,'North Carolina first line'],[0,'musketeer',96,2300,2200,'Virginia second line'],[0,'musketeer',120,1600,2200,'Continental third line'],[0,'musketeer',30,2700,1000,'American northern flank',0,'loose'],[0,'musketeer',30,2700,3400,'American southern flank',0,'loose'],[0,'cavalry',24,1450,1000,'Washington’s horse',8],[0,'cavalry',24,1450,3400,'Lee’s horse',8],[0,'cannon',2,1700,2800,'Continental guns',2],[0,'commander',1,1150,2200,'Nathanael Greene'],
        [1,'musketeer',96,4000,1550,'British northern front'],[1,'musketeer',96,4000,2850,'British southern front'],[1,'musketeer',96,4400,2200,'Guards reserve'],[1,'cavalry',36,4550,3350,'Tarleton’s cavalry',12],[1,'cannon',3,3850,2200,'British artillery',3],[1,'commander',1,4800,2200,'Cornwallis']] }),
    historicalScene({ id: 'historyYorktown', name: 'Yorktown', year: 1781, era: 'Musket age', location: 'Virginia, North America',
      summary: 'French and American assault sectors face the outer redoubts ahead of Cornwallis’s main defenses.',
      deployment: 'French troops face Redoubt 9 and Americans face Redoubt 10, with siege batteries behind the assault line. British detachments hold the outer works in front of their main position and the York River.',
      sources: [historySource('History of the siege of Yorktown · National Park Service', 'https://www.nps.gov/york/learn/historyculture/history-of-the-siege.htm')],
      unitNotes: 'Short wall panels represent earthen works. Musketeers approximate the bayonet assault parties; the player controls whether they shoot or close, and no nighttime modifier is imposed.',
      armies: [['American & French Allies', 'breakthrough'], ['Cornwallis’s Garrison', 'defensive']], regions: [region('water', 5600, 0, 800, 4400),region('wall', 3650, 1000, 80, 450),region('wall', 3650, 1800, 80, 450),region('wall', 3650, 2600, 80, 450),region('wall', 3650, 3400, 80, 450),region('hill', 3400, 1050, 900, 2600)],
      groups: [[0,'musketeer',96,2750,1500,'French assault on Redoubt 9'],[0,'musketeer',96,2750,3000,'American assault on Redoubt 10'],[0,'musketeer',96,2250,2200,'Allied supporting line'],[0,'cannon',4,2200,1400,'French siege battery',4],[0,'cannon',4,2200,3100,'American siege battery',4],[0,'commander',1,1750,1700,'Rochambeau'],[0,'commander',1,1750,2800,'George Washington'],
        [1,'musketeer',48,3900,1550,'Redoubt 9 defenders'],[1,'musketeer',48,3900,3100,'Redoubt 10 defenders'],[1,'musketeer',120,4650,2200,'British main defense'],[1,'cannon',4,4500,1600,'British batteries',4],[1,'cannon',2,4500,3000,'British riverward guns',2],[1,'commander',1,5050,2200,'Cornwallis']] }),
    historicalScene({ id: 'historyAlbuera', name: 'Albuera', year: 1811, era: 'Napoleonic', location: 'Extremadura, Spain',
      summary: 'A French demonstration at the bridge masks a stronger attack against the Allied right.',
      deployment: 'Allied foot cover the village sector and ridge, with Spanish infantry on the right and supports behind. A smaller French body faces the bridge while the main infantry and horse concentrate around the southern flank.',
      sources: [historySource('The Battle of Albuera · Mercian Regiment Museum', 'https://www.worcestershireandmercianregimentmuseum.org/the-battle-of-albuera-16-may-1811/')],
      scaleNote: 'This depicts the opening turn of the flank, rather than compressing all later redeployments into one line.',
      armies: [['Beresford’s Allied Army', 'refused'], ['Soult’s Army of the South', 'flank']], regions: [region('water', 3350, 0, 150, 2500),region('bridge', 3250, 1400, 350, 450),region('hill', 1800, 1700, 900, 2200),region('forest', 3500, 2450, 900, 650)],
      groups: [[0,'musketeer',72,2750,1000,'Portuguese left'],[0,'musketeer',60,2900,1650,'Alten’s village defenders'],[0,'musketeer',96,2550,2500,'Spanish right'],[0,'musketeer',96,2100,2200,'Stewart’s supporting division'],[0,'musketeer',72,1750,3000,'Cole’s reserve'],[0,'cavalry',48,2350,3500,'Allied right horse',12],[0,'cannon',4,2350,1850,'Allied artillery',4],[0,'commander',1,1600,2200,'Beresford'],
        [1,'musketeer',72,4000,1650,'Godinot’s demonstration'],[1,'musketeer',120,3850,3300,'Girard’s main attack',0,'column'],[1,'musketeer',120,4350,3300,'Gazan’s support',0,'column'],[1,'lancer',48,3250,3650,'French & Polish cavalry',12],[1,'cavalry',36,4350,3850,'French dragoon reserve',12],[1,'cannon',4,4100,2200,'French artillery',4],[1,'commander',1,4750,3200,'Soult']] }),
    historicalScene({ id: 'historySalamanca', name: 'Salamanca', year: 1812, era: 'Napoleonic', location: 'Castile, Spain',
      summary: 'Wellington’s concentrated force is poised to strike Marmont’s stretched left beneath the Arapiles.',
      deployment: 'Allied infantry wait near the Lesser Arapile, with a southern striking force and cavalry support. French divisions form a long, separated chain west of the Greater Arapile, exposing the leading left.',
      sources: [historySource('Battle of Salamanca · National Army Museum', 'https://www.nam.ac.uk/explore/battle-salamanca')],
      scaleNote: 'The map captures the exposed-left crisis; it does not attempt the full preceding day of maneuver.',
      armies: [['Wellington’s Allied Army', 'hammer'], ['Marmont’s Army of Portugal', 'echelon']], regions: [region('hill', 2300, 1200, 650, 700),region('hill', 3650, 1000, 850, 900),region('hill', 1200, 1800, 600, 1300)],
      groups: [[0,'musketeer',96,2450,1750,'Allied holding center'],[0,'musketeer',96,2450,2450,'Allied main body'],[0,'musketeer',120,2800,3350,'Pakenham’s striking division'],[0,'cavalry',72,2300,3600,'Le Marchant’s cavalry',12],[0,'musketeer',72,1900,2700,'Clinton’s reserve'],[0,'cannon',4,2050,2100,'Allied batteries',4],[0,'commander',1,1750,2200,'Wellington'],
        [1,'musketeer',120,3850,1100,'French right near the Arapile'],[1,'musketeer',96,3950,2100,'French center'],[1,'musketeer',96,4050,3000,'Stretched French left'],[1,'musketeer',96,3950,3800,'Exposed leading division',0,'column'],[1,'cavalry',48,4450,3000,'French cavalry support',12],[1,'cannon',5,4250,1600,'French batteries',5],[1,'commander',1,4700,2100,'Marmont']] }),
    historicalScene({ id: 'historyWaterloo', name: 'Waterloo', year: 1815, era: 'Napoleonic', location: 'Belgium',
      summary: 'Wellington’s reverse-slope defense and three forward strongpoints face Napoleon’s combined arms.',
      deployment: 'Allied infantry hold behind the Mont-Saint-Jean ridge, with forward detachments representing Hougoumont, La Haye Sainte, and Papelotte. French infantry, guns, and cavalry occupy successive depths opposite the ridge.',
      sources: [historySource('Battle of Waterloo · National Army Museum', 'https://www.nam.ac.uk/explore/battle-waterloo'),historySource('Waterloo contemporary map · The National Archives', 'https://www.nationalarchives.gov.uk/education/resources/significant-events/battle-of-waterloo-1815/')],
      scaleNote: 'Opening deployments only: Prussian arrivals and later cavalry-versus-square phases are not scripted.', unitNotes: 'Generic musketeers use three ranks instead of varied historical depths; hill cover approximates the reverse slope, and woodland patches stand in for farm strongpoints.',
      armies: [['Wellington’s Anglo-Allied Army', 'defensive'], ['Napoleon’s Army of the North', 'echelon']], regions: [region('hill', 2300, 700, 650, 3000),region('forest', 2900, 800, 450, 450),region('forest', 2900, 1950, 350, 450),region('forest', 2900, 3100, 450, 450)],
      groups: [[0,'musketeer',48,3100,1050,'Papelotte detachment'],[0,'musketeer',36,3050,2200,'La Haye Sainte detachment'],[0,'musketeer',48,3100,3350,'Hougoumont detachment'],[0,'musketeer',120,2300,1350,'Allied ridge left'],[0,'musketeer',120,2300,2200,'Allied ridge center'],[0,'musketeer',120,2300,3050,'Allied ridge right'],[0,'cavalry',60,1800,1550,'Allied cavalry reserve',15],[0,'cavalry',60,1800,2850,'Allied heavy horse',15],[0,'cannon',3,2750,1500,'Allied ridge battery',3],[0,'cannon',3,2750,2800,'Allied ridge battery',3],[0,'commander',1,1550,2200,'Wellington'],
        [1,'musketeer',120,4050,1350,'French right infantry'],[1,'musketeer',120,4050,2200,'French center infantry'],[1,'musketeer',120,4050,3050,'French left infantry'],[1,'musketeer',96,4900,2200,'Guard reserve'],[1,'cannon',4,3700,1650,'Grand Battery north',4],[1,'cannon',4,3700,2750,'Grand Battery south',4],[1,'cavalry',72,4550,1400,'French heavy cavalry',12],[1,'lancer',60,4550,3100,'French mounted reserve',12],[1,'commander',1,5300,2200,'Napoleon']] })
  ]);
  PRESETS.push(...HISTORICAL_BATTLES);

  // Plans issue real movement orders; Steady advance also preserves a partial
  // weapon brace only while bodies actually move forward in coherent ranks.
  // Keep the original four ids so older saved scenarios retain their meaning.
  const DOCTRINES = Object.freeze({
    balanced: Object.freeze({ id: 'balanced', name: 'Balanced', subtitle: 'Keep a connected front', description: 'Each formation advances toward its local opponent. Missile troops support the line and commanders stay behind it.' }),
    aggressive: Object.freeze({ id: 'aggressive', name: 'Full assault', subtitle: 'Commit the whole line', description: 'Melee formations commit to nearby opponents and keep pressing. Missile troops advance into their own firing bands.' }),
    defensive: Object.freeze({ id: 'defensive', name: 'Hold & counter', subtitle: 'Receive the attack', description: 'Keep the present line and fire from it. Counterattack when the enemy reaches a sector, or advance after a delay if the enemy waits.' }),
    flank: Object.freeze({ id: 'flank', name: 'Encircle', subtitle: 'Ride around, strike the rear', description: 'Infantry pins the front. Cavalry and lancers take a wide route around an enemy wing, form behind it, then charge. Horse archers harass from a flank.' }),
    hammer: Object.freeze({ id: 'hammer', name: 'Hammer & anvil', subtitle: 'Fix the front, release the cavalry', description: 'Infantry receives the attack while cavalry rounds the enemy wing. Riders wait briefly behind the enemy for the infantry to engage, then exploit the rear approach.' }),
    refuseLeft: Object.freeze({ id: 'refuseLeft', name: 'Attack left', subtitle: 'Advance one wing, guard the other', description: 'The left wing advances and cavalry turns that flank. The right wing guards the rear, then joins after contact or a delayed advance.' }),
    refuseRight: Object.freeze({ id: 'refuseRight', name: 'Attack right', subtitle: 'Advance one wing, guard the other', description: 'The right wing advances and cavalry turns that flank. The left wing guards the rear, then joins after contact or a delayed advance.' }),
    steady: Object.freeze({ id: 'steady', name: 'Steady advance', subtitle: 'Advance behind a braced front', description: 'Infantry advances deliberately in connected ranks. Aligned pikes and hoplites retain part of their brace while walking forward; turns, fast movement, and disrupted ranks lose it. A planted defense remains stronger.' }),
    rapid: Object.freeze({ id: 'rapid', name: 'Rapid encirclement', subtitle: 'Spread fast around the enemy position', description: 'A pinning force holds the front while separated wings run around a bounded nearby enemy group. Isolated formations are surrounded individually; adjacent squads are treated as one position instead of one tiny target.' }),
    refused: Object.freeze({ id: 'refused', name: 'Refused flank', subtitle: 'Withdraw one wing, attack with the other', description: 'Pull the exposed wing back into a guarded line while the opposite wing advances. Keep a reserve behind the bend, then commit it when the attack creates an opening.' }),
    breakthrough: Object.freeze({ id: 'breakthrough', name: 'Concentrated breakthrough', subtitle: 'Mass at a weak seam, exploit the opening', description: 'Choose a weak part of the local enemy front, concentrate an assault there, and bring supporting reserves through the same sector after contact.' }),
    elastic: Object.freeze({ id: 'elastic', name: 'Elastic defense', subtitle: 'Give ground, then counterattack', description: 'Receive the approach, withdraw in bounded stages when pressed, and counterattack enemies that follow into the prepared line. Fresh direct orders interrupt the plan immediately.' }),
    echelon: Object.freeze({ id: 'echelon', name: 'Echelon assault', subtitle: 'Commit successive supported waves', description: 'Advance in offset waves, starting with the leading sector and committing the following formations in sequence. The supporting line follows the developing fight instead of charging all at once.' }),
    skirmish: Object.freeze({ id: 'skirmish', name: 'Screen & skirmish', subtitle: 'Shoot, give ground, cover the retreat', description: 'Missile formations approach into range, plant to shoot, and pull back together when threatened. Infantry follows as a covering line and engages enemies that close.' })
  });

  global.GameData = Object.freeze({
    TYPES, TYPE_ORDER, FORMATIONS, MAPS, PRESETS, HISTORICAL_BATTLES, DOCTRINES, WORLD_SIZES, normalizeWorldSize, castleWalls, ARMOR_CLASSES, ATTACK_PROFILES, armorMultiplier, getMatchup,
    DEFAULT_CONFIG: JSON.parse(JSON.stringify(PRESETS[0])),
    MAX_UNITS: 20000, RNG, clamp
  });
})(typeof window !== 'undefined' ? window : globalThis);
