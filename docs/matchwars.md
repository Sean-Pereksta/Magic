# Match Wars — The Runeborn Front

Play at `/game/matchwars.html`. The game is listed in the library's **Complete** shelf, alongside the polished games. Its route and existing `matchwars` catalog key are preserved.

## The battle loop

Select one of three lanes, then swap adjacent runes to make a line of three or more. The matched element summons troops into the selected lane. Watch the incoming-enemy forecast and send counters where they will matter. Battles 1–12 have three waves, battles 13–32 have four, and battles 33–48 have five. A named boss guards every fourth battle.

The campaign has **48 battles across 12 chapters**. The original twelve encounters remain intact; defeating the Eclipse Lord at battle 12 opens nine further frontiers rather than ending the campaign. New campaigns start with 120 gold and an introductory primer, so you can choose an upgrade before the first wave. Earn more gold from enemy kills and battle rewards, buy upgrades in the forge, and revisit unlocked battles to try different army builds. Losing a battle lets you retry and adjust your strategy.

The campaign map shows one chapter at a time, with previous/next controls and a chapter selector. You can inspect locked frontiers, then continue at the highest unlocked battle. Later chapters add denser waves, elite enemies, mixed elemental fronts, and named boss variants. The last battle is against **The Runeless Emperor**.

| Battles | Chapter | Final encounter |
| --- | --- | --- |
| 1–4 | The Awakening | The Root King |
| 5–8 | The Frostbound March | Frostbound Queen |
| 9–12 | The Eclipse | The Eclipse Lord |
| 13–16 | The Ironwood Siege | Ironbark Regent |
| 17–20 | The Thunder Road | Tempest Matriarch |
| 21–24 | The Sunken Crown | The Drowned Oracle |
| 25–28 | The Ashen Concord | Cinderheart Colossus |
| 29–32 | The Prism Crusade | The Prism Tyrant |
| 33–36 | The Wild Hunt | The Briar Sovereign |
| 37–40 | The Nightglass Front | The Nightglass Regent |
| 41–44 | The Shattered Sky | The Skybreaker |
| 45–48 | The Last Rift | The Runeless Emperor |

## Elements and troop roles

| Element | Troop | What it brings |
| --- | --- | --- |
| Ember | Ember Mage | Ranged fireballs, splash damage, and lingering burns. |
| Tide | Tide Ranger | Long-range frost arrows that slow approaching enemies. |
| Grove | Grove Warden | Healing for allies and poison for enemies. |
| Storm | Storm Lancer | Fast strikes, chain lightning, and full damage against flying enemies. |
| Sun | Sun Paladin | Armor and shields to hold a lane for fragile ranged troops. |
| Void | Void Reaper | Life stealing and extra damage against wounded enemies. |

The main counter cycle is **Ember → Grove → Storm → Tide → Ember**. An arrow means the attacker is strong against the next element: a strong hit deals 1.5× damage, and attacking your weakness deals 0.7× damage. Sun and Void counter each other at 1.35× damage. Other matchups are neutral. The field guide and enemy forecast show these relationships during play.

A shielded Paladin in front of a Warden and Ranger holds a lane differently from a burst-heavy Mage and Lancer formation. Match the element you need, rather than automatically taking the first available match.

## Combos, specials, and relics

Cleared runes fall under gravity and refill from above. New matches cascade automatically; each additional chain strengthens the troops it summons. Larger matches summon more troops and can create elite units. Repeatedly fielding one element builds its resonance, adding another reason to specialize when the enemy composition allows it.

| Match or item | Effect |
| --- | --- |
| Four in a horizontal line | Creates a row-blast rune. |
| Four in a vertical line | Creates a column-blast rune. |
| Five or more in a straight line | Creates a wild rune. Swap it with a rune to clear that element. |
| Intersecting T or L matches | Creates a nova rune with an area blast. |
| Two wild runes swapped together | Clears the board. |
| Fury relic | Adds attack power to troops summoned by that element's match. |
| Aegis relic | Adds shields to the summoned troops. |
| Haste relic | Improves movement and attack speed. |
| Vitality relic | Increases summoned troop health. |

Specials activate when swapped and can trigger other specials inside their blast. The rune's shape, element color, and relic badge communicate different properties. A move that makes no match is reversed. If no moves remain, the board safely reshuffles.

## Enemy variety and upgrades

There are 21 base enemy definitions: three regular enemy roles for each of the six elements and three boss archetypes. Later chapters build on them with elite troops and distinct named boss variants. Threats include flying Harpies, regenerating Colossi, armored siege troops, shield bearers, healers, shield breakers, ranged casters, and summoners. Their traits matter as much as their element.

The forge offers **18 upgrade paths**, each with three ranks: two distinctive paths per troop and six command upgrades. Troop paths improve effects such as burn, splash, slow, healing, poison, lightning targets, shields, armor, life stealing, and execution damage. Command paths improve combo power, relic strength, gold rewards, keep health, spellcraft, and keep sentries. Rising rank costs make spending a strategic choice.

Meteor damages enemies in the selected lane; Rally empowers its allies; Renew restores the lane's allied troops and keep health. Spell cooldowns provide tactical support while you build the next match.

## Mobile and PC controls

- **Touch:** swipe between adjacent runes, or tap one rune and then its neighbor. Tap the lane buttons before matching to direct summons. Spell buttons, the forge, campaign, and field guide are available on screen.
- **Mouse:** drag between adjacent runes, or click one rune and then its neighbor.
- **Keyboard:** focus the board and use arrow keys to navigate; Enter selects a rune. Number keys 1–3 select lanes. Q, W, and E use Meteor, Rally, and Renew. H shows a possible match. P or Escape opens the pause menu.

The layout adapts from a side-by-side desktop war room to a stacked mobile layout. The game menu includes sound and reduced-motion settings. Opening a dialog or leaving the game tab pauses combat so you can inspect troops and make upgrade decisions.

## Saving and compatibility

Progress saves locally on the current device and browser after purchases, battle results, stage selection, and settings changes. Campaign progress, gold, purchased upgrades, and settings survive reloads. Refreshing restarts the selected battle with a new rune board; active troops, spell cooldowns, and unfinished battle rewards are not saved. The earlier Color Clash save is migrated into the new progression when available, preserving gold, a bounded campaign level, and mapped troop-upgrade ranks. The original save remains intact. Browser storage can be unavailable in restrictive/private browsing sessions; the game remains playable and reports a saving limitation.

Existing Match Wars saves retain their gold, upgrades, settings, and cleared battles. A save that completed battle 12 unlocks battle 13 when loaded into the expanded campaign.

The implementation uses native ES modules and Canvas 2D, with no game-framework download. Serve the repository over HTTP when testing locally; opening the HTML directly as a `file:` URL can prevent browsers from loading modules.

## FAQ

**Why did my swap move back?** It did not make a match and did not activate a special. You can use Hint to find an available move.

**Why are my troops struggling against a particular enemy?** Check both its element and its role. Use elemental counters, Storm against flyers, melee pressure against ranged-resistant troops, and support behind a shielded frontline.

**Can I change the lane of troops already deployed?** Lane selection directs subsequent summons and spells. Plan the lane before taking a match.

**Where is my progress on another device?** Saves live in this browser's local storage. There is no account-based cloud synchronization.

## Checks

Run `npm run test:matchwars` for the dedicated board, combat, campaign, save, and catalog checks. They include matching-board combat simulations, campaign-boundary checks, and save compatibility. The catalog tests load the actual showcase configuration to verify the Complete-shelf promotion rather than relying only on fallback title matching. The Match Wars GitHub workflow runs these checks for relevant changes.
