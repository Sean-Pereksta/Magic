# Campaign scale and player knowledge

Single-player setup offers 6, 8, 10 and 12 Houses. Multiplayer keeps the six
original Houses, its 40 × 30 map, seats, round system and starting balance.

| Houses | Map | Minimum capital separation |
| --- | --- | --- |
| 6 | 40 × 30 | 8 hexes |
| 8 | 48 × 36 | 9 hexes |
| 10 | 56 × 40 | 10 hexes |
| 12 | 64 × 44 | 11 hexes |

The regional generator scales region cells, mountain positions and river count.
Founding still verifies reachable starter farms, timber, stone, iron and
non-overlapping territory for every House. New Houses have distinct rulers,
personalities, colors and capital names. Map fitting uses actual dimensions.

## Observations, not a cosmetic mask

`fog.mjs` keeps bounded, dated tile and army observations for human-controlled
Houses. The founding preview is completely visible until that player confirms
Found City, and does not populate exploration memory. After confirmation, even
while other humans are still founding, normal knowledge restrictions apply.

* Visible tiles contain current information from an actual sight source.
* Explored tiles retain observed geography, roads, structures and last known
  ownership. They do not obtain new buildings, projects, defenses or enemy orders.
* Unknown tiles contain only faint terrain. Original capital locations and names
  remain known, without revealing ownership, improvements or surrounding tiles.
* Foreign live army markers disappear outside sight. Dated estimates stay at the
  observed location, are replaced or disproved by observation, and expire after
  20 turns. Estimates describe strength when observed; age is always shown.

`knowledgeView` is the shared boundary for the map, UI helpers and dialogue.
Tooltips, targeting choices, forecasts, rival reports, battle effects, alerts and
War Room objectives receive this projection. Structure targeting requires current
observation. Human routes use remembered geography; actual movement still checks
real borders, roads, enemies and terrain. Exploration is recorded at each movement
step. Battle forecasts cannot write simulated observations to the campaign.
Own economy forecasts are computed authoritatively and carried as private totals,
so missing foreign map details do not change the player's income calculations.
Loss of one's own holding is an administrative report; it does not disclose the
new garrison or subsequent improvements. Major House survival and formal wars
remain public political information.

## Vision and intelligence

| Source | Radius in hexes |
| --- | --- |
| Siege | 1 |
| Infantry | 2 |
| Ranged | 3 |
| Heavy cavalry / knights | 4 |
| Scouts / light cavalry | 5 |
| Capital | 4 |
| City / town | 3 / 2 |
| Fort, levels I–III | 3–5 |
| Watchtower, levels I–III | 6–8 |
| Ordinary production building | Its own tile |

Mixed armies use their best surviving scouting unit. Sight does not yet model
terrain obstruction. Strong alliances (the ally has at least 50 trust) share
one hex around their settlements/forts and armies of at least 20 troops. This
never shares the ally's exploration history, spy reports or other allies' sight.

Economic spy missions observe a region near an actual enemy settlement. Military
missions also observe actual major armies. Network access controls radius and
breadth: 1–3 hexes and up to three settlements/four armies. Deep military access
can record an army's real objective. These are snapshots taken when intelligence
is gathered, not continuous remote vision. A report is marked recent for up to
two turns only while its operative retains the same embedded access; movement,
reassignment or capture leaves dated knowledge. Reports and operation disclosures
remain grounded in the authoritative simulation.

## Dialogue, transport and saves

Gemini and scripted dialogue receive observed forces, public capital sites,
disclosed commitments and dated reports. They do not receive a ruler's hidden
plans, treasury, production or army orders. The simulation evaluates negotiations;
the private multiplayer snapshot carries its verdict and legal counteroffer,
without exposing the hidden inputs. New terms wait for controller review.

The public multiplayer document contains no live board assets. Each House's
private document contains its fog-filtered view. Only the existing trusted
simulation controller reads the full canonical world and can reconstruct it on
lease transfer. This preserves the existing controller trust model.

Existing saves migrate with empty exploration history and current local sight.
House count, dimensions, roster, snapshots and bounds are validated. Single-player
imports allow up to 12 MB; multiplayer retains its compressed document limits.
Simulation AI continues using the authoritative strategy systems; Gemini never
creates or executes hidden strategy.

## Verification

`npm run test:iron-throne` covers generation, diplomacy, operations, deterministic
reloading, fog privacy and multiplayer projections. `npm run test:iron-throne:fog-browser` checks twelve-House setup, founding, remembered
sightings, unknown sites and panels at desktop and phone sizes. Browser checks
produce no previews or screenshots. The Firestore rules suite uses only the local
`demo-iron-thrones` emulator.
