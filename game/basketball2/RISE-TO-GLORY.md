# Rise to Glory

Build a franchise from three rookies, recruit specialists, develop a captain, and compete through Street, City, Regional, National, and Elite divisions. Open **Rise to Glory** from Basketball 2's main menu. National Tour and exhibition basketball remain separate modes.

## The franchise loop

- Create a club name, two colors, original emblem, uniform, offensive identity and defensive philosophy. Choose three of fifteen rookies; the first is the captain, with a custom name, hairstyle, skin tone and number.
- The clubhouse shows the captain's existing animated GLB model, team overall, chemistry, record, available points, next opponent, standings and recent results.
- Each division has seven league opponents, 1v1 rivalry and 2v2 tournament fixtures, standings, playoff qualification, a semifinal and a championship. Winning promotes the club; losing permits another season. Elite clubs can defend their title in subsequent seasons.
- Match results award team credits and contribution XP. Wins, assists, good shots, screens, stops, rebounds, steals and blocks all support development. A player does not need to lead scoring to develop effectively.
- The initial generated pool contains 96 distinct athletes, with more prospects arriving each offseason. Availability expands by division. Sign specialists, compare strengths and weaknesses, choose three starters and maintain up to ten athletes. Releases retain the athlete's career history and provide a partial contract refund. No purchases or backend service are involved.

## Development and on-court effects

The six categories contain 32 ratings, including screening and rebounding. Upgrade previews show the current value, next value, cap, point cost and remaining investment. Archetype, potential, height and weight constrain development; a finite investment budget and two specialization branches prevent perfect players.

All campaign athletes, including CPU opponents and AI teammates, use their own profiles in the ordinary character, collision, input and fixed-step simulation systems. Ratings affect shot dispersion and timing, finishing, acceleration, speed, lateral movement, stamina, pass speed/accuracy/leading, ball security, reach, rebounding and tactical reads. A complementary roster changes the existing playbook's destinations and player selection. Chemistry does not add a hidden basket-success multiplier.

The eighteen ability paths include the nine shooting signatures, five finishing signatures, Floor General, Rim Guardian, Pickpocket and Screen Setter. Their descriptions identify the actual physics or timing changes. Requirements and signature potential remain enforced. A third-tier training mastery provides compatible signature tuition; physical and prerequisite requirements still apply.

Power, aerial, reverse, windmill, contact, standing and poster finishes use the existing rigs and hand IK. Height, build, vertical reach, strength, approach angle, stamina and momentum determine eligibility and animation. A well-positioned defender can still block an elite poster attempt.

Clean, well-timed midrange releases receive more control when the shooter is settled, fresh and open, including both wings and baselines. The extra control falls away with pressure, movement, fatigue and timing error; specialist ratings and skills still add their own benefits. The player's ground position determines two- versus three-point attempts and the applicable shooting rating/signatures, while the real hand position remains the ball's launch origin.

For running layups, a close finishing attempt stays a layup while the same Shoot hold continues and the player settles inside the existing finish range. The gather controls travel after takeoff, including when the movement direction stays held. Open short-release touch blends smoothly into meter timing as the hold lengthens, and pressure removes that help. The meter's green strip now shows the player's actual grading window, including Quick Release and current coverage, so an old fixed green patch cannot suggest an early release.

## Match preparation and training

Set offensive identity, defensive focus, scoring priorities, roles, matchups and substitution policy in **Game plan**. The existing Motion, Pick & Roll, Pick & Pop, Drive & Kick, Isolation, Give & Go, Post Up and Fast Break styles remain available. Midrange specialists seek elbows; perimeter shooters provide arc/corner outlets; passers, screeners and interior finishers fill complementary jobs. Difficult opponents use shared coverage, switches, help and observed tendencies with the same physical movement and shooting rules.

Assignments and signature development affect how AI teammates use those opportunities. Shooters look to settle and release open catches, especially with Catch & Shoot, Corner Specialist or Quick Release; an open three is preferred to a long drive for a two. A defender in the shooter's face or poor range prevents that quick attempt. A close open finish can still be better. Interior builds seal and back down a viable matchup and pass out when help arrives. Playmakers favor useful passing windows and slashers favor clear driving lanes. Balanced players read their actual skills. Human-controlled players retain every decision, and all attempts use the ordinary shot, contact and ball-flight systems.

The court's **Substitutions** button pauses play and lets a bench athlete replace an active slot. Shots and passes must finish before a substitution can occur. Automatic substitutions happen at dead balls according to the selected policy, stamina and matchup fit. Contribution box scores follow each actual athlete across substitutions.

Seven drills use the live court: midrange, three-point shooting, dunking, passing, dribbling cones, defensive footwork and team offense. Complete both the attempt count and the successful-action target in two minutes. Increasing tiers tighten the target. Rewards are modest and capped at three rewarded sessions per athlete per campaign game week. Ending a drill early grants no completion reward.

## Saves and compatibility

Rise to Glory uses `basketballRiseToGloryV3` and a last-valid backup, with bounded normalization, older campaign migration and recovery from malformed data. National Tour's `basketballNationalTourV2` and legacy Tour save are untouched. Results are tied to a fixture ID to reject duplicate rewards. Current progression, prospects, appearance, career statistics and season history survive reloads.

Use **Export save** to download a portable JSON copy of your franchise. **Import save** reads a selected file and shows a team preview before replacing the current franchise. Canceling or rejecting a file keeps your current team; successful replacement retains the previous valid save as a backup. Export includes the roster, upgraded ratings, signature ranks, assignments, appearance and campaign progress. Only compatible Rise to Glory saves are accepted.

Returning to Quick Play restores the previous profiles, equipment, team size, local configuration, rules and gameplay preset. You can explicitly select franchise athletes in Quick Play's OVR-capped custom roster builder; their current development travels into that exhibition without awarding campaign results. Standard Quick Play and National Tour keep their ordinary rosters.

## Repeatable validation

Run `npm run test:basketball2` for the deterministic tests. Browser tests require Playwright/Chromium, with optional `BASKETBALL_CHROME` and `THREE_BUNDLE` paths for installed Chrome/Edge and the game's Three 0.160 UMD bundle. Run `npm run test:basketball2:campaign-browser` for franchise creation, upgrading, recruitment, GLB match lineups, substitutions, rewards, reloads, Tour isolation, live drills, all management panes at desktop/phone/landscape sizes, and standard solo/local game configurations.

The shooting regression steps 900 seeded attempts per condition through release, gravity, rim, backboard and swept hoop scoring. Current midrange calibration at 5.8 meters measures:

| Midrange condition | Node physical solver |
| --- | ---: |
| Average, open and well timed | 65.4% |
| Specialist, open and well timed | 76.4% |
| Average, moderate contest | 36.7% |
| Average, heavy contest | 22.0% |
| Average, moving release | 36.0% |
| Average, fatigued | 39.2% |

The side-midrange browser regression holds and releases the ordinary keyboard shot through the actual GLB charge pose, from the front, both wings and both baselines across all three builds. It checks skill improvement and retains separate contested, moving and fatigue controls. The role-playstyle browser regression completes twelve real passes to an open arc shooter, verifies prompt three-point launches with active signature effects, and also checks guarded catches and interior backdowns. Custom-roster and save-transfer browser regressions exercise both caps, imported athlete identities, local/co-op slots, downloads, import review, malformed files and responsive layouts.

Side-layup regressions use real held-key approaches, sprinting and ordinary releases, including raw franchise rookies. Input checks cover modern/classic controls, stopping during a charge, entering and leaving finish range, retained shot selection and exactly one physical attempt. Meter pixel checks compare the painted green region and overlay against the actual release grade for rookie, Quick Release, moving and contested cases. Historical shot types cannot widen the next shot's contest preview.

These are aggregate tuning results from 900 attempts per condition, not guaranteed makes. Every jump shot is resolved by the physical ball/hoop solver; there is no scoring roll that bypasses collision. `dunk-finish-browser.cjs` additionally tests eighteen rig/procedural and signature finish cases, a timed Posterizer block, and actual GLB hand release positions (300 attempts per condition by default; set `BASKETBALL_SHOT_SAMPLES=900` to reproduce the full calibration). Existing deterministic and browser regressions cover team AI, defense, all offensive styles, dribble/drive behavior, input/controller isolation, equipment, models, split screens and National Tour. The Basketball intelligence workflow runs the campaign acceptance alongside those existing checks.

Optional CPU trades are not included. Recruitment and releases provide the complete roster economy.
