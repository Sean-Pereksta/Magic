# Receiver Window QB

Singleplayer franchise game at `/game/receiver-window-qb.html`, registered in the Catnmice lobby. Based on the supplied `receiver_window_qb_infinite_franchise_audibles_strength(1).html`.

## Play

Choose a play (1–8), optionally audible a receiver (X/H/Y/Z, then the route letter), then Space or Start Play to snap. Hold/release Space to throw; W/S adjusts loft. On touch during a live play, the screen is split into two independent control zones: drag the **left half** to look/aim, and hold the **right half** to charge the pass. While holding the right half, slide up for more loft or down for a faster bullet, then release the right side to throw. Left-side aiming never starts or releases a throw, and horizontal motion on the right side does not steer the camera. Receivers run automatically after a catch; J or the Juke button attempts a move when a defender is nearby. They also use moves automatically. Menu pauses play. R resets only before the snap.

## Progression and saves

Every completed best-of-five pays cash: losses start at $100 plus $25 per touchdown, victories start at $250 plus touchdown bonuses. Round bonuses are capped. Each of nine attributes has five training sessions, with no 100-rating cap. Prestige starts at $7,500 and multiplies by 2.5 each time; after finishing five sessions in any attribute, it renews five sessions per attribute without reducing ratings. Recruits roll 85% OVR 60–79, 10% 80–89, 4% 90–96, and 1% 97–105, with increasingly expensive signing prices. Physical gains above 100 taper. Defense speed, acceleration and turning continue growing gradually with rounds; athleticism and ball skills improve slowly, and some defenders protect depth using delayed observed receiver movement. Athleticism increases jump height and rescue-dive reach; catch improves hands and pursuit burst; size increases the model, contact leverage and high catch reach; tricks improves jukes/head fakes. Jukes cost momentum, have a cooldown, and can briefly fool nearby defenders; better defenses resist them. Normal catches remain ahead of rescue-only dives.

Three local slots use `receiverWindowQB_slots_v2`; the selected slot survives reload. The original v1 local franchise is imported when no slot exists. Checkpoints resume at the next pre-snap state, with score, down, spot, roster, market and money. Reload during a play restarts that snap. Menus and replays pause the simulation. Local auto-save is separate from the explicit **Save to Cloud** button.

Cloud saves mirror `../arcane-wilds/cloud-save.js`: Firebase 10.7.1, project `bible-game-246c0`, anonymous auth, and the authenticated `/lobbies/{gameId}` rules. Football uses app `receiver-window-qb-cloud`, game type `receiver-window-qb-cloud-save`, and document prefix `receiver-window-qb-save--`. Names are normalized and hashed, and the payload uses AES-GCM with PBKDF2-SHA256 (210,000 iterations), fresh salt/IV, and optional gzip. The password is never stored. Existing names require successful decryption before overwrite. No rules change or deployment is needed relative to Arcane Wilds' existing rules surface. Like Arcane Wilds, these are named encrypted saves, not UID-owned account slots; the shared existing rules allow authenticated writes.

## Replays and graphics

Fourth-down throws, 10+ air-yard catches and touchdown catches qualify. Replays run after the live play ends and follow the recorded ball from close behind, at 72% speed. Recording stops shortly after the catch so a long run cannot evict the throw. Skip restores the scene and continues exactly once. A bounded 360-frame, 30 Hz history records transforms only; playback runs no physics, rewards or catch checks. Four subtle round-based venues vary turf, end-zone color, sky and sunlight. Field numbers, mowing stripes, stands and goalposts share persistent geometry. Three.js 0.160.0 is vendored with its MIT license so the game does not require a graphics CDN.

## Verify

`node --test game/receiver-window-qb/*.test.cjs`

With Playwright installed: `node game/receiver-window-qb/browser-smoke.cjs`. Optionally set `QB_CHROMIUM_PATH` to a Chromium executable. This serves the actual game with a test-only introspection hook, checks the menu/slots/drive reload, pause guards, loss rewards, replay isolation, jukes, catch variants, interceptions and mobile viewport overflow. It generates no preview files.

Cloud tests use real Web Crypto with mocked Firebase transport; they do not write to production. Deployment still needs an end-to-end cloud save/load check against the live project's anonymous-auth settings and deployed rules.


## Directional hands and full-body moves

Jukes animate a shared visual rig so the helmet, jersey, limbs and secured ball turn together. Successful fakes add a short wrong-way impulse and visible balance recovery to the defender; cooldowns and defender resistance still apply. A thrown ball cancels the receiver juke pose so pursuit takes priority.

Receivers aim both hands toward a nearby predicted interception over the next 0.24 seconds, including side and low reaches. Targets are converted to model-local coordinates, limited to 0.95 model units from each shoulder and approached smoothly. Existing hand-contact checks use those actual hand positions; normal catches remain ahead of rescue-only dives. The visual rig is included in the existing replay transform capture.
