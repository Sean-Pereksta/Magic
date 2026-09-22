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

## Expanded playbook and roster identity

The playbook has 72 concepts across seven tabs: Quick Game, Intermediate,
Deep Shots, Screens, Bunch / Stack, Motion and Trick Plays. Original 1–8
shortcuts remain; use arrows and Enter, mouse, or touch for every play and tab.
New concepts have formation spacing, release depths, option reads, motion or
screen assignments. Seven trick concepts use bubble/wheel, tunnel/go, sluggo,
fake quick screen and motion reversal routes; they keep the standard QB throw.

Screens identify one intended receiver and two lead blockers. Blockers begin
positioning before the catch, claim separate threats, and approach the shoulder
between defender and carrier. Other receivers also help during YAC. Strength,
size, leverage and closing momentum determine contact effectiveness. Holds last
0.12–0.85 seconds, followed by a defender recovery window; weak blocks can shed
almost immediately. A Blocking Receiver prioritizes useful seals.

The designated screen target receives 1.8 seconds of +45% acceleration, +22%
maximum speed and +28 percentage points of juke success. The runner evaluates the blocker's protected shoulder, nearby pursuit and open
forward lanes. An early, useful developing block can prompt a brief 14% slowdown;
that patience ends by 1.1 seconds, under immediate pressure, or in open space.
Screen runners have a tighter 0.8-yard voluntary retreat limit. Before the throw,
the receiver adjusts the outlet within a shallow window to avoid coverage and
clutter in the throwing lane. Leads retain their assignment during a screen pass,
while still reacting to a ball arriving directly at their hands. Changing that target's route with
an audible removes screen eligibility. Repeated screen use improves recognition
of observed shallow movement; defenders cannot see hidden route paths.

Post-catch movement scores forward and diagonal corridors against projected
pursuit. Lateral movement is available, while backward cuts are penalized and
voluntary retreat is limited to 1.6 yards from the best run position, or 0.6 yards
within four yards of a first-down/goal marker. Physical contact impulses remain
separate. A clear goal lane takes the runner straight into the end zone.

Roster playstyles are derived from existing archetypes, preserving save data:
Deep Threat acquires deep tracking windows earlier; Route Technician settles
away from nearby coverage; YAC Specialist uses wider lanes/more moves; Power
and Possession receivers favor forward progress over avoidance; Blocking Receiver
prioritizes blocking leverage. The new Blocking archetype can appear in recruitment.

Designated Option, Choice, Seam Read and Screen Choice routes make one coverage
read before the throw. Cutting/turning and Football IQ affect timing and reliability.
Choice settles or breaks away from leverage; Seam Read continues, bends inside or
breaks outside against the visible cap; Screen Choice takes the bubble or tunnel
based on edge coverage. The route panel reports the read. Option stems are run once,
and ordinary routes retain their assignment. Whip and Pivot add opposite double
breaks. All five new route types are available through the existing audible menu.

Press **F** or tap **PUMP** during the eight-second QB window. It aims at the
receiver nearest the camera direction and can cause at most one nearby defender
to take a short false step. A 1.1-second cooldown and persisted matchup memory
reduce success for repeat pumps, targets and concepts. Veterans bite less often;
Gamblers/Ball Hawks jump observed routes more aggressively. Bullies, Track Team
and Heavy Hitters vary physical strength, speed and coverage pools. Memories reset
when the matchup finishes or changes, not when another play is called.

Select a receiver pre-snap to **Widen**, **Tighten** (up to two yards from the
concept's alignment), or set **Motion** on Motion/Bunch/Trick concepts. Both QB
look controls now reach 90 degrees left/right; receiver facing follows movement.

Light Rain reduces receiver and defender turning 4% and catch-control probability 2.5 percentage points.
Windy adds at most 0.32 world units/s² of crosswind to elevated passes. Cold adds
3.5% defender contact strength. Existing weather indices and neutral conditions
retain their meanings; gameplay variation stays small.

Speed, cutting acceleration and turning have wider rating differences, with
prestige benefits still bounded. Player dimensions derive from SIZE and STR;
large builds, taller players and compact players have different equipment.
Roster cards use matching dimensions and distinguish star, elite and generational
ratings. Existing local/cloud saves need no reset.

Arrow keys navigate the top visible menu spatially, Enter activates, and Tab
cycles its controls. This covers the main menu, roster/training/recruitment,
saves, audibles, play selection and replay skip. Text fields keep native editing
keys; live aiming and mobile split controls remain separate.

## Watch Replay and camera controls

Every completed play, including short gains, incompletions, interceptions and
expired throw clocks, makes its replay available through **Watch Replay** on the
field toolbar, main menu and team screen. It retains the most recent completed
play for repeated viewing during the current session; the next completed play
replaces it. Existing automatic highlight replays remain skippable.

During either replay type, Right/Down cycles QB view → sideline → angled overhead;
Left/Up cycles backward. On-screen view buttons provide the same controls for
mouse/touch. Space, Escape or Skip Replay exits. The selected view persists for
subsequent replays. Manual replays pause the simulation and restore the exact
field/menu state without advancing downs, payouts or time. Replay data is not
part of local/cloud saves and is cleared when switching franchises. One bounded
recording and one detached actor set are retained, with owned resources disposed
when replaced.

## Opponent tour and rematches

Open **Team & Opponents** from the main menu or **Team** between rounds. Previous /
Next cycles through every defeated opponent and the current campaign opponent;
select **Play Rematch**, then continue to the field. The card shows team number,
defensive identity, recorded wins/losses and the three-touchdown win prize.
Changing opponents is allowed only between matchups, never mid-drive or mid-series.

Rematches use the earlier opponent's round for coverage identity, physical
ratings, uniform and cash calculation. All rematch cash, including participation
and touchdown bonuses, is 20% of that opponent's normal reward. They do not
advance or erase campaign progress. After a rematch, the next campaign opponent
is selected again; another rematch can be chosen in the room.

The first eight opponents remain intact. The generated tour now combines 32
names with 16 mascots for 512 additional teams: **520 unique teams**, followed by
numbered leagues with continuing difficulty progression. This fixes the former
16-name repetition. Existing saves infer defeated teams from campaign progress;
new per-opponent results, selected rematch and match-in-progress state survive
local/cloud saves. Historical losses before this update cannot be reconstructed.


## Stiff arms and hurdles

Ball carriers automatically attempt stiff arms against close front/side upright
tacklers and hurdles against approaching diving tacklers. Strength controls stiff-arm
execution; athleticism controls hurdle execution and launch height. Evasion controls
recognizing the right moment. Each threat gets one timing evaluation per move, with a
shared 1.35-second move cooldown, so frame rate cannot turn failures into guaranteed wins.

A stiff arm extends one hand, keeps the football tucked in the other, and physically
pushes the defender away. A hurdle launches the runner and tucks the knees; the runner
must actually be high enough at impact to clear the selected low defender. Upright
support can still tackle, and another tackler remains dangerous after a stiff arm.

For the first 1.8 seconds after a designated screen catch, move recognition gains 20
percentage points and execution gains 28 points (bounded below 100%). Early automatic
juke opportunities also increase. Rating differences still matter, and the burst ends
normally; audibles that remove screen eligibility remove these bonuses too.

## Visual identity, animation and franchise dashboard

The dashboard centers the next matchup, record, cash, last result and receiving
corps. Team management includes side-by-side uniform previews and previous
meetings; roster cards expose build, standout traits and a career/chemistry page.
Arrow keys, Enter and Tab navigate all menus, including the new modal screens;
Escape closes a franchise screen and restores focus. Text/color/select fields
keep their native keyboard behavior.

- **Uniform Designer:** independent Home and Away kits, with helmet, jersey,
  secondary, pants, socks, shoes, numbers and accent colors; pick the active kit.
  Choose a football emblem, open the larger collection, or paste an emoji.
  Uniforms and franchise name travel with local slots and existing cloud saves.
  Small emblems appear on both helmet sides and the chest, as well as menus,
  scoreboard and opponent history. Jersey numbers stay distinct by roster slot.
- **Opponent identity:** deterministic colors, emblems, pants, helmets, venue
  templates and end-zone treatment keyed by opponent round. Rematches retain
  identity and original difficulty. Six venue layouts include outdoor, bowl,
  night, urban, classic and arena settings. Shared stadium props include benches,
  sideline personnel, chain markers, photographers, tunnels and a live scoreboard.
  The crowd is one instanced draw; low graphics disables crowd animation.
- **Animation:** acceleration/deceleration lean, speed-driven strides, cut plants,
  torso/shoulder rotation and ball-tracking heads build on the jointed rig.
  Stiff-arm reactions reflect the strength difference; hurdles tuck and extend
  through flight, and contact recoveries brace the runner. Evasion shapes juke
  timing. Hand targeting runs after the new body pose so real glove contact
  remains authoritative; the established move-success rules still resolve contact.
- **Playbook:** all seven categories and all original 44 concepts remain, with 28 additions. Suggested adds
  six situation-ranked options for short yardage, long downs and the red zone.
  Cards use actual route geometry and indicate the first read, blocks, options
  and motion. A larger diagram includes guidance; suggestions never restrict
  selection or audibles.
- **Personality:** derived Football IQ complements Evasion in option reads,
  lane/sideline decisions, settling, block approach and marker awareness. Deep
  threats release more aggressively, technicians/possession receivers seek
  settling space, and the established YAC/power/blocking preferences remain.
  Chemistry accumulates from completed concepts, targets, catches, contested
  catches and touchdowns. Timing/tracking benefits cap at 2%; no training category
  or additional control is introduced.
- **Statistics:** QB attempts, completions, percentage, yards, TD, INT, Y/A and
  longest completion; receiver targets, catches, yards, TD, YAC, Y/catch, drops,
  contested catches, broken tackles, successful stiff arms/hurdles/jukes and
  effective blocks. Outcomes commit once at the end of a play, then use the
  existing between-snap checkpoint. Abandoned snaps and replay viewing do not
  accumulate stats. Postgame summaries and career records retain released
  receivers' contributions. Career records begin with this upgrade; prior wins
  and campaign progress are preserved without inventing historical box scores.

`franchise.js` owns normalization and statistics; `franchise-ui.js` renders the
screens, and `presentation.js` owns procedural identity details and stadium props.
Decal textures use reference counts, including detached replays, and release when
neither a live model nor its saved replay uses them.

Validation:

```sh
node --test game/receiver-window-qb/*.test.cjs
node game/receiver-window-qb/browser-smoke.cjs
node game/receiver-window-qb/franchise-browser.cjs
```

Browser checks use Playwright and Chromium (or `QB_CHROMIUM_PATH`). They cover
existing controls/contact/replays, uniform persistence, real-play stat commits,
emoji decals, keyboard focus, bounded replay textures and phone/landscape layouts.


## Motion, catches and evasion expansion

The 28 appended plays preserve the original play indices and saved selection. New
concepts include read screens on both sides, a stack slip, tunnel convoy, mesh/sit
choices, seam reads, whip/pivot spacing, and motion fakes. Jet, return and orbit
paths can use waypoints and staggered paired shifts. Longer motion gets enough
countdown time to keep speed bounded; every route starts at its actual release
point. Man corners shade observed motion slowly while zone defenders retain their
landmarks. Field lines and playbook diagrams show the full motion path.

Successful catches select nine contextual gather animations: snatch/tuck,
high-point clamp, low scoop, sideline drag, body shield, one-hand reach/tuck,
screen turn-up, back-shoulder spin/secure and over-shoulder basket. These poses
start after the existing real glove-contact and securing checks. Hand targeting
and joint constraints still run after the body pose; catch odds, contact volumes,
sideline spots and tackle rules remain authoritative.

Evasion now chooses among spin, hesitation, hard cut, speed cut, shoulder dip,
dead leg, stutter-go and rocker step. Distance, closing speed, lateral pursuit,
sideline position, congestion, playstyle and recent technique determine the move.
Each has a distinct plant/body sequence, speed retention and lateral impulse.
Exit shoulders are checked against support defenders. Existing cooldowns, strength,
hurdles and stiff arms still apply; a juke can briefly fool at most one defender.
Screen targets and leads stay on task before the catch instead of starting an
unrelated automatic juke.

Regression coverage includes low-rated screen reads, one-time options, complete
motion trajectories, saved play 72, screen outlet and seal decisions, lead-blocker
pass discipline, forward progress at 30/60/120 Hz, eight evasion contexts, nine
catch poses, and desktop/phone diagrams for orbit and paired motion.


### Forgiving receiver catches

Routine receiver glove contacts are substantially more reliable without increasing the
collision shapes or awarding chest/helmet/proximity catches. In clear conditions, a
70-effective-CATCH/70-effective-ATH receiver with a perfect spiral and no other
modifiers has 98.85%, 98.55%, and 97.05% initial one-hand control at relative contact
speeds of 24, 40, and 50 world units/second. These are conditional control probabilities,
not overall completion rates; placement, real contact, coverage and securing still matter.

- Receiver-only control uses a smaller bullet penalty above 38 units/second; weather,
  staggering and underthrows remain relevant but less punitive. The ordinary cap is
  99.5%; contested control is capped at 94%. Defender interception odds are unchanged.
- Hand tracking moves faster within the same physical reach. A 160 ms simulation-time
  awareness grace bridges short pursuit-planner gaps and resets on every throw.
- An aware receiver may cushion one real wrist-side forearm impact per throw for up to
  75 ms. This only damps relative velocity; it never attracts, teleports or awards the
  ball. A rendered glove still has to touch it; expired/uncompleted gathers stay loose.
- A later second-glove contact can join the same securing attempt. Receivers secure in
  20 ms with two hands or 40 ms with one; defense retains its original timing.
- Receiver-first contested hand contacts can succeed or be broken up. A successful
  contest is evaluated once until the touching opponent separates, not every frame.
  Defense-first contact, genuine tips, ground and boundary authority remain intact.
- Uncontested receiver hand bobbles have softer rebounds and less tumble, making a real
  second attempt practical. No extra per-frame retry or automatic recovery is added.

Validation: `node --test game/receiver-window-qb/*.test.cjs`. The added regressions
cover conditional odds, reset/expiration, real wrist and glove contacts, both outcomes
of contact contests, second-hand arrivals, and moving catches at 30/60/120 Hz.
