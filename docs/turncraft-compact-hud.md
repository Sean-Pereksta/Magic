# Compact Turncraft battle controls

This follow-up builds on merged PR #1839 and changes only local presentation, shortcut routing and army selection. The three faction skins, music, combat simulation, campaign, balance definitions and Firestore formats remain in place.

## Space and controls

| Layout | Previous dock height | Compact dock height |
| --- | ---: | ---: |
| Desktop | 250 px | 150 px |
| Phone portrait | 300 px | 218 px |
| Short landscape | Tall side dock | 202 px, lower right |

The dock keeps the selected portrait/name/health, buttons and visible shortcuts. Repeated instructions, order descriptions, separate timer chips, headings and readiness prose no longer consume space. Details remain in tooltips and Help; cooldown graphics and disabled states remain live. Temporary feedback uses the existing status region and disappears without adding a dock row.

Movement commands occupy a 96 px high, two-column scrolling box with 44 px buttons. Build/train/research cards use 52 px cells in a bounded scrolling grid. Utility controls remain reachable, including the campaign Menu button. Collapsing the phone dock uses 56 px and keeps the selected entity visible.

The command-group strip occupies the first screen row. Resources sit alongside it on wide desktops and on a second row on narrower screens. Groups scroll horizontally on phones; the portrait strip uses the full width. The minimap, Sound control and campaign objectives avoid these rows.

## Shortcuts and army selection

- Construction, production, morph and research buttons use unmodified single keys, beginning with the available Q/E/R/T keys. No Shift is required. Camera/command keys, numbers and other reserved controls are skipped; ability shortcuts avoid the context cards. The displayed key invokes the same guarded button action as a click.
- **0 — All Army** selects every living owned non-worker unit, including off-screen, support, air, garrisoned and embarked troops. Deployed troops lead the selection when available. Selecting the army neither moves troops nor unloads passengers. Subsequent movement, attack and ability dispatch filters passengers appropriately.
- The 0 chip is an immediate selection action, not an assignable saved slot. Numbered groups 1–9 keep their tap/hold/double-tap behavior. Typing, menus, pause, repeats, composition and modifier chords cannot trigger army selection.
- Stop/Hold resolve the current deployed selection when clicked. Loading, unloading or restored snapshots update the same controls without leaving stale passenger IDs in orders or omitting newly deployed troops.

## Verification

Desktop, phone and landscape browser checks verify dock bounds, top-row non-overlap, readable selection, scrolling to the last movement/build action, 44 px touch targets, seven utility controls, collapsed layout, cooldown presentation and temporary feedback. Separate browser checks cover every faction's single-key context actions, actual click/key dispatch, key collisions, ownership/offscreen/passenger army selection and blocked-input behavior.

The 599-case full regression/browser suite exercised the update. Four assertions still assumed the previous visible readiness text or landscape audio position; those presentation expectations were updated, and all seven affected audio/Blink browser checks passed on rerun. The three final viewport checks, three new shortcut/army browser cases, 40 related input/logistics checks and 56 simulation/input/audio checks also passed. No local failures remain after these targeted reruns. Economic AI and campaign mechanics are unchanged by this follow-up, so the long economic matrix was not repeated.

The final dynamic-eligibility review fix passed four focused browser checks and 54 related command, tunnel, group-ability, profile/snapshot and simulation-preservation checks. These cover unchanged button identity through real unload and remote boarding, current Stop/Hold payloads and disabled passenger-only commands.

The production-card focus check now refocuses the enabled card before testing stable queue updates. Native disabled buttons legitimately lose focus in Linux Chromium, so resource-disable availability and enabled-state focus stability are tested separately.

The existing sprite-tint alpha-edge test has an unrelated Linux Chromium failure: [earlier main CI](https://github.com/Sean-Pereksta/Magic/actions/runs/40143763190/job/121020707905) and PR #1839 report identical pixel differences with unchanged renderer/test blobs. This update does not change tint rendering or loosen that test. The installed local Chrome passes it. Physical-device sizing and live multi-device play remain release checks.
