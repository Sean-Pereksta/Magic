# Receiver Window QB

Singleplayer franchise game at `/game/receiver-window-qb.html`, registered in the Catnmice lobby. Based on the supplied `receiver_window_qb_infinite_franchise_audibles_strength(1).html`.

## Play

Choose a play (1–8), optionally audible a receiver (X/H/Y/Z, then the route letter), then Space or Start Play to snap. Hold/release Space to throw; W/S adjusts loft. On touch during a live play, the screen is split into two independent control zones: drag the **left half** to look/aim, and hold the **right half** to charge the pass. While holding the right half, slide up for more loft or down for a faster bullet, then release the right side to throw. Left-side aiming never starts or releases a throw, and horizontal motion on the right side does not steer the camera. Receivers run automatically after a catch; J or the Juke button attempts a move when a defender is nearby. They also use moves automatically. Menu pauses play. R resets only before the snap.

## Progression and saves

Every completed best-of-five pays cash: losses start at $100 plus $25 per touchdown, victories start at $750 plus touchdown bonuses. Wins add $75 per round after round one, capped at $1,500 extra; loss bonuses are unchanged. Each of nine attributes has five training sessions, with no 100-rating cap. Prestige starts at $7,500 and multiplies by 2.5 each time; after finishing five sessions in any attribute, it renews five sessions per attribute without reducing ratings. Recruits roll 85% OVR 60–79, 10% 80–89, 4% 90–96, and 1% 97–105, with increasingly expensive signing prices: 90 OVR costs $3,500, 100 costs $8,950, and 105 costs $12,200. Existing market listings receive the lower prices when loaded, preserving any cheaper legacy bargains. Physical gains above 100 taper. Defense speed, acceleration and turning continue growing gradually with rounds; athleticism and ball skills improve slowly, and some defenders protect depth using delayed observed receiver movement. Athleticism increases jump height and rescue-dive reach; catch improves hands and pursuit burst; size increases the model, contact leverage and high catch reach; tricks improves jukes/head fakes. Jukes cost momentum, have a cooldown, and can briefly fool nearby defenders; better defenses resist them. Low rescue dives require the same physical hand contact as standing catches.

Three local slots use `receiverWindowQB_slots_v2`; the selected slot survives reload. The original v1 local franchise is imported when no slot exists. Checkpoints resume at the next pre-snap state, with score, down, spot, roster, market and money. Reload during a play restarts that snap. Menus and replays pause the simulation. Local auto-save is separate from the explicit **Save to Cloud** button.

Cloud saves mirror `../arcane-wilds/cloud-save.js`: Firebase 10.7.1, project `bible-game-246c0`, anonymous auth, and the authenticated `/lobbies/{gameId}` rules. Football uses app `receiver-window-qb-cloud`, game type `receiver-window-qb-cloud-save`, and document prefix `receiver-window-qb-save--`. Names are normalized and hashed, and the payload uses AES-GCM with PBKDF2-SHA256 (210,000 iterations), fresh salt/IV, and optional gzip. The password is never stored. Existing names require successful decryption before overwrite. No rules change or deployment is needed relative to Arcane Wilds' existing rules surface. Like Arcane Wilds, these are named encrypted saves, not UID-owned account slots; the shared existing rules allow authenticated writes.

## Replays and graphics

Fourth-down throws, contested/high-point catches, gains of 10+ total yards (including YAC), and every touchdown qualify. Replays run after the live play ends at 72% speed, following the throw and then the carrier through the final tackle or score. Recording starts at the snap. A bounded 360-frame history starts at 30 Hz and progressively thins older samples across the whole timeline, reusing their buffers while retaining the beginning and ending. Timestamp-based interpolation preserves playback timing. Skip (button, Space or Escape) restores the scene and continues exactly once. Playback runs no physics, rewards or catch checks. Four subtle round-based venues vary turf, end-zone color, sky and sunlight. Field numbers, mowing stripes, stands and goalposts share persistent geometry. Three.js 0.160.0 is vendored with its MIT license so the game does not require a graphics CDN.

## Verify

`node --test game/receiver-window-qb/*.test.cjs`

With Playwright installed: `node game/receiver-window-qb/browser-smoke.cjs`. Optionally set `QB_CHROMIUM_PATH` to a Chromium executable. This serves the actual game with a test-only introspection hook, checks the menu/slots/drive reload, pause guards, loss rewards, replay isolation, jukes, catch variants, interceptions and mobile viewport overflow. It generates no preview files.

Cloud tests use real Web Crypto with mocked Firebase transport; they do not write to production. Deployment still needs an end-to-end cloud save/load check against the live project's anonymous-auth settings and deployed rules.


## Directional hands and full-body moves

Jukes animate a shared visual rig so the helmet, jersey, limbs and secured ball turn together. Successful fakes add a short wrong-way impulse and visible balance recovery to the defender; cooldowns and defender resistance still apply. A thrown ball cancels the receiver juke pose so pursuit takes priority.

Receivers and aware defenders aim their hands toward a nearby interception over the next 0.24 seconds, including side and low reaches. Targets are converted to model-local coordinates and limited to 0.89 model units from each shoulder. Athleticism controls reaching speed. Two-bone elbows and knees keep the limbs connected while running, jumping, reaching and landing. The complete visual rig is included in replay transform capture.

## Replayability and gameplay variety

The original QB camera, independent mobile look/throw gestures, charge, loft, gravity, four-down drives and franchise saves remain the main game. The audible menu now includes **Fade (A), Curl (U), and Double Move (M)** alongside the ten existing routes. Curl receivers settle after the break; route endpoints stay inside the field. Speed has a wider effect on separation, cutting controls acceleration and cut losses, and turning controls redirection. Route progress follows actual movement rather than a fixed clock.

Eight opponent identities rotate across rounds: Conservative, Fast Secondary, Physical Secondary, Aggressive Man, Zone Heavy, Ball Hawks, Short Route Hunters and Deep Ball Patrol. Each has several possible coverages and small personnel differences. New off-man, underneath/deep zones, safety brackets and a rotating two-high shell extend the existing defenses. Coverage stays fixed while changing a presnap play. Defenders use delayed observed movement, never the current route path, and must see the ball before chasing its predicted destination.

The last 12 resolved pass attempts inform gradual adjustments. The first two attempts cause none; repeated deep/short throws move safety depth by at most three/two world units. Repeated targets can earn safety help, and repeated routes modestly increase observed-movement anticipation. These tendencies save with the franchise and clear at the end of each matchup. They never instantly counter the current play.

Catch placement is measured at the actual contact point. In-stride completions keep momentum; low, behind-body and contested catches require gathering. Placing the ball away from a nearby defender helps hands. Catch feedback and short rig poses distinguish high points, low catches, back/outside shoulders, over-the-shoulder catches, catch-and-turns and sideline toe taps. Failed control, arm impacts and contested touches produce live deflections; another receiver or defender can make the next touch. Sideline catches end at the receiver's position. Low rescue dives start before contact and cannot award a catch from proximity.

YAC speed and cuts use receiver attributes. Defenders pursue a bounded intercept of delayed carrier movement with **no post-catch speed multiplier**. Strong receivers can survive one glancing tackle when moving fast enough, while direct tackles still stop them. Completing five sessions in an attribute, or prestiging, can unlock one signature when its supporting rating is at least 85: Deep Threat, Route Artist, Sure Hands, Contact Balance, Sideline Specialist or YAC Specialist. Each adds a small bonus to an existing physical capability.

Matchups choose Day, Sunset, Night, Overcast or Light Rain and retain that appearance through the match and save/load. Weather is visual only. Venue turf, end zones, seats, instanced background buildings and sideline boards also vary. Rain is capped at 96 points. The menu's **Graphics** button cycles Auto, Low and High; Auto lowers resolution/shadows after sustained slow active frames, and Low removes rain and dynamic shadows. Shared player geometry, a fixed trajectory buffer, bounded receiver history and packed/reused replay transforms reduce allocation. Replays alternate ball and sideline angles; big-play feedback stays brief.

After the catch, the nearest defender closes while support defenders take containment angles. Close pursuers use visible carrier movement; distant pursuers retain delayed observations. Swept contact checks catch tackles between frames, including from behind, and resolve contact before goal-line or sideline crossings. Defenders can commit to short diving tackles with limited reach, no midair steering, a cooldown and recovery after a miss. Technique gradually improves across rounds. A juke can fool the nearest defender while support remains active; physical receivers can still break an isolated glancing standing tackle. Within five yards of the goal line, carriers take an open lane directly into the end zone, retaining normal evasion when defenders block it.

The test suite includes full-runtime simulations with real Three.js math and substituted DOM/GPU I/O. These verify coverage/route movement, save continuity, fair defensive observations, placement momentum, bobbles, sideline resolution, rear and diving tackles, support pursuit, goal-line choices, full-play replay isolation and bounded buffers. The Playwright smoke test remains necessary for actual rendering and device interaction checks.

## Physical catching and athletic poses

Possession starts with a swept collision against a rendered glove. Upper arms and forearms can deflect the ball, but cannot directly award possession. Chest/head proximity and the old oversized catch/rescue radii are removed. Contact order is chronological for both teams; opposing touches within 6 ms produce a contested tip. A 45–75 ms control window permits a second player's actual hand/arm contact to break up a provisional catch. Nearby defenders cannot cause invisible swats.

The football uses three small lobes following its visible orientation. Moving limb capsules are sampled from the rig, including model scale, body rotation and jump offset. Deflection direction and speed depend on surface normal, incoming velocity and limb velocity. Overlapping touches do not repeatedly add impulses; the actor becomes eligible again after separation. Loose balls stay live until controlled, grounded, out of bounds or the flight timeout. Laces spin with the ball, and tips introduce tumble.

Athleticism increases real jump velocity, reach speed and control. High-point jumps anticipate arrival near the jump apex; size scales actual limb reach. Bent knees, asymmetric airborne legs, takeoff/landing compression and a catch-to-tuck blend supplement the jointed model. All geometry is shared, with no external assets or physics engine. Live-flight physics uses at most five substeps per capped render frame; spatial rejection avoids detailed checks for distant players. Existing controls, drive rules, saves, tackling and replay timing are retained.

Regression coverage includes no-contact chest/head passes, fast and moving-limb contacts, contact ordering, contested tips, provisional-catch breakups, tip-to-interception recovery, scaled/rotated hands, ground precedence, full high-point completions, ordinary moving catches at 30/60/120 Hz and joint replay restoration.

## Tackling, falling and situational dives

Tackles carry bounded momentum into a short jointed finish animation. Contact angle, relative speed, strength and nearby support select wrap, drag-down, side-fall, hard-hit or trip poses. Defenders reach around the carrier; knees fold, the free arm braces, and the body settles into a short roll or slide. A broken glancing tackle adds a brief stumble and recovery. Existing shared geometry is reused; no ragdoll engine or new assets are required.

Receivers with at least 65 athleticism can consider an automatic effort dive while running forward near an attainable first-down marker or goal line, with a real tackle predicted within 0.28 seconds. There is one probability roll per possession, only after those conditions are met. Speed, athleticism, strength and body control (existing turning/evasion ratings) affect the decision, launch and resistance to contact. An open field, distant marker, low speed, sideline risk or already-established tackle prevents the dive. A defender can still stop an airborne receiver.

The held football follows the extending gloves. During tackles and dives, its forward progress is tracked until the down or boundary event; later cosmetic sliding cannot add yards. These finishes use the actual spot without the older minimum-one-yard gain. Goal-line and sideline events resolve in order. Touchdown accounting waits for the landing animation, happens once, and is included in the existing skippable replay. All attempted effort dives qualify for replay. Controls and save formats remain unchanged.

Tests cover launch gating and one-roll rarity, reachable first downs, goal dives after passing the first-down marker, head-on dive stops, momentum and fall variants, exact down spots, boundary order, stable outcomes at 30/60/120 Hz, glove attachment, geometry reuse and full landing replay.
