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

## Match preparation and training

Set offensive identity, defensive focus, scoring priorities, roles, matchups and substitution policy in **Game plan**. The existing Motion, Pick & Roll, Pick & Pop, Drive & Kick, Isolation, Give & Go, Post Up and Fast Break styles remain available. Midrange specialists seek elbows; perimeter shooters provide arc/corner outlets; passers, screeners and interior finishers fill complementary jobs. Difficult opponents use shared coverage, switches, help and observed tendencies with the same physical movement and shooting rules.

The court's **Substitutions** button pauses play and lets a bench athlete replace an active slot. Shots and passes must finish before a substitution can occur. Automatic substitutions happen at dead balls according to the selected policy, stamina and matchup fit. Contribution box scores follow each actual athlete across substitutions.

Seven drills use the live court: midrange, three-point shooting, dunking, passing, dribbling cones, defensive footwork and team offense. Complete both the attempt count and the successful-action target in two minutes. Increasing tiers tighten the target. Rewards are modest and capped at three rewarded sessions per athlete per campaign game week. Ending a drill early grants no completion reward.

## Saves and compatibility

Rise to Glory uses `basketballRiseToGloryV3` and a last-valid backup, with bounded normalization, older campaign migration and recovery from malformed data. National Tour's `basketballNationalTourV2` and legacy Tour save are untouched. Results are tied to a fixture ID to reject duplicate rewards. Current progression, prospects, appearance, career statistics and season history survive reloads.

Campaign rosters are used only inside Rise to Glory and its practice sessions. Returning to Quick Play restores the previous profiles, equipment, team size, local configuration, rules and gameplay preset. Standard competitive modes never silently receive campaign upgrades.

## Repeatable validation

Run `npm run test:basketball2` for the deterministic tests. Browser tests require Playwright/Chromium, with optional `BASKETBALL_CHROME` and `THREE_BUNDLE` paths for installed Chrome/Edge and the game's Three 0.160 UMD bundle. Run `npm run test:basketball2:campaign-browser` for franchise creation, upgrading, recruitment, GLB match lineups, substitutions, rewards, reloads, Tour isolation, live drills, all management panes at desktop/phone/landscape sizes, and standard solo/local game configurations.

The shooting regression steps 900 seeded attempts per condition through release, gravity, rim, backboard and swept hoop scoring. Initial calibration measured:

| Midrange condition | Node physical solver | Browser with GLB hands |
| --- | ---: | ---: |
| Average, open and well timed | 52.2% | 55.3% |
| Specialist, open and well timed | 64.0% | 65.7% |
| Average, moderate contest | 35.0% | 39.9% |
| Average, heavy contest | 22.0% | 18.2% |
| Average, moving release | 36.0% | 34.0% |
| Average, fatigued | 39.2% | 40.6% |

These are aggregate tuning results from 900 attempts per condition, not guaranteed makes. Every jump shot is resolved by the physical ball/hoop solver; there is no scoring roll that bypasses collision. `dunk-finish-browser.cjs` additionally tests eighteen rig/procedural and signature finish cases, a timed Posterizer block, and actual GLB hand release positions (300 attempts per condition by default; set `BASKETBALL_SHOT_SAMPLES=900` to reproduce the full calibration). Existing deterministic and browser regressions cover team AI, defense, all offensive styles, dribble/drive behavior, input/controller isolation, equipment, models, split screens and National Tour. The Basketball intelligence workflow runs the campaign acceptance alongside those existing checks.

Optional CPU trades are not included. Recruitment and releases provide the complete roster economy.
