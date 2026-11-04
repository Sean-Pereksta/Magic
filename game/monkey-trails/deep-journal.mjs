import { beginWalkSession } from './walk-sessions.mjs';
import { REEF, LOOM, GLASS, deepSpecies, hosts } from './deep-roster.mjs';
const FOOT = 0.3048;
const action = (text, choices, answer = null, kind = 'act') => ({ kind, text, choices, answer });
const camp = (text, choices, answer) => action(text, choices, answer, 'camp');
const step = (kind, text, goal = 1) => ({ kind, text, goal });
const find = (text, n) => step('find', text, n),
  walk = (kind, n) =>
    step(
      kind,
      `Walk ${n} ft ${{ E: 'east', W: 'west', N: 'north', S: 'south', carry: 'with the same conscious lead', feet: 'along the fourth trail' }[kind]}.`,
      n,
    );
export const quests = [
  [
    'The Last Bell Run',
    'thunder-highlands',
    [
      step('pace', 'Start the bell trial. Average 6 mph for three uninterrupted minutes.', 1584),
      camp('Ring the charged bell.', ['Ring bell']),
    ],
    'The keeper returns with a storm forecast.',
  ],
  [
    'Eastbound Mail',
    'emerald-jungle',
    [
      action('The courier offers a sealed satchel.', ['Take satchel']),
      walk('E', 1200),
      camp('Hand the satchel to the post monkey.', ['Deliver mail']),
    ],
    'The courier brings a reply and a barter offer.',
  ],
  [
    'Two Ends of the Ferry',
    'whispering-marsh',
    [
      find('Inspect the broken mooring.'),
      walk('W', 700),
      walk('E', 450),
      action('The restored line reaches the dock.', ['Ring dock bell']),
    ],
    'The ferrymonkey tends the repaired crossing.',
  ],
  [
    'The Empty Nest',
    'emerald-jungle',
    [
      step('basket', 'Collect a fruit basket and inspect its handprints.'),
      find('Find the youngster beneath a fallen tree.'),
      walk('carry', 500),
      action('Where should the family live?', ['Rebuild nest', 'Invite family home']),
    ],
    'The youngster remembers its escort.',
  ],
  [
    'Ice Underfoot',
    'frostpeak-mountains',
    [
      find('Find two cracked Frostpeak markers.', 2),
      step('block', 'Guard against the guardian’s announced strike and win.'),
      walk('N', 900),
      action('Light the shelter.', ['Hang lantern']),
    ],
    'The shelter glows. Its keeper visits camp.',
  ],
  [
    'Borrowed Fire',
    'ember-volcano',
    [
      find('Recover quest embers at separate discoveries.', 2),
      camp('Forge the bell clasp.', ['Forge clasp']),
      step('interrupt', 'Interrupt the guardian’s charging blow and win.'),
      action('Return the clasp to the smith.', ['Install clasp']),
    ],
    'The smith installs the clasp and opens a trade.',
  ],
  [
    'Bellreef Song',
    REEF,
    [
      find('Find tuned shells: Low, High, Middle.', 3),
      camp(
        'Play their notes in discovery order.',
        ['High · Middle · Low', 'Low · High · Middle', 'Middle · Low · High'],
        1,
      ),
      step('block', 'Block the reef guardian’s announced strike and win.'),
    ],
    'The ferry opens. The pilot awards the Reef Seal.',
  ],
  [
    'Three Knocks Below',
    REEF,
    [
      action('Listen from the ferry: short, long, short.', ['Board ferry']),
      walk('S', 800),
      walk('W', 600),
      action('Answer the knocks.', ['Long · Short · Short', 'Short · Long · Short'], 1),
    ],
    'The pearl diver brings a shell-drum recipe.',
    6,
  ],
  [
    'First Bridge',
    LOOM,
    [
      find('Recover two frost cable spools.', 2),
      walk('carry', 1000),
      camp('Anchor the first span.', ['Western colony', 'Eastern colony']),
      camp('The first visitor steps onto the new bridge. Receive the Loom Seal.', ['Greet the visitor']),
    ],
    'The first visitor crosses with the Loom Seal.',
  ],
  [
    'A Thread Between Homes',
    LOOM,
    [
      action('Hear the messenger from the other colony.', ['Meet messenger']),
      walk('N', 600),
      find('Recover a frayed silken knot.'),
      walk('S', 600),
      camp('Weave the knot into the second span.', ['Repair bridge']),
    ],
    'Both colonies cross and visit together.',
    8,
  ],
  [
    'Water Writes Back',
    GLASS,
    [
      find('Inspect three prints by the glass spring.', 3),
      action(
        'One track dims in shade. Which will you follow?',
        ['Bright glass', 'Dim print', 'Doubled print'],
        1,
      ),
      step('block', 'Guard through the glass guardian’s flare and win.'),
      action('Choose where the spring flows.', ['Nursery', 'Caravan']),
    ],
    'Fresh water reaches your chosen settlement. Its keeper awards the Glass Seal.',
  ],
  [
    'The Fourth Footprint',
    GLASS,
    [
      camp('Hear the ferry pilot’s testimony.', ['Hear pilot']),
      camp('Hear the bridge keeper’s testimony.', ['Hear keeper']),
      step('technique', 'Win a wild battle with techniques from two different lineup monkeys.'),
      walk('feet', 1500),
    ],
    'The buried court opens beneath Saltglass.',
    -1,
  ],
].map(([name, land, steps, ending, requires], id) => ({ id, name, land, steps, ending, requires }));
export function journal(s) {
  return (s.meta.deep ??= { quests: {}, hunts: {}, pairs: {}, pinned: null });
}
export function page(s, id) {
  const j = journal(s);
  return (j.quests[id] ??= { stage: 0, count: 0 });
}
export function done(s, id) {
  return page(s, id).stage >= quests[id].steps.length;
}
export function unlockedQuest(s, q) {
  return (
    s.world.unlocked.includes(q.land) &&
    (q.requires === undefined ||
      (q.requires === -1 ? [6, 8, 10].every((id) => done(s, id)) : done(s, q.requires)))
  );
}
export function pin(s, id) {
  const q = quests[id];
  if (!q || !unlockedQuest(s, q)) throw Error('Unlock the region and its earlier story first.');
  if (journal(s).pinned !== id) delete s.run.pace;
  journal(s).pinned = id;
}
export function activeQuest(s) {
  const q = quests[journal(s).pinned];
  if (!q || q.land !== s.world.activeLand || done(s, q.id) || !unlockedQuest(s, q)) return null;
  const p = page(s, q.id);
  return { q, p, step: q.steps[p.stage] };
}
function credit(s, x, amount) {
  if (!x || amount <= 0) return;
  const { q, p, step } = x;
  p.count += amount;
  if (p.count + 1e-7 < (step.goal || 1)) return;
  p.stage++;
  p.count = 0;
  if (done(s, q.id)) {
    s.log.push(q.ending);
    const session = beginWalkSession(s);
    (session.deepRecap ??= []).push(q.ending);
    p.visitToken = session.id;
  }
}
export function questChoice(s, index) {
  const x = activeQuest(s);
  if (
    !x?.step.choices ||
    (x.step.kind === 'camp' && s.run.phase !== 'camp') ||
    !['camp', 'trail'].includes(s.run.phase)
  )
    throw Error('Reach the required handoff first.');
  const { step, p } = x;
  if (!Number.isInteger(index) || !step.choices[index]) throw Error('Select a response.');
  if (step.answer != null && index !== step.answer)
    throw Error('Listen to the clue and try the sequence again.');
  if (step.choices.length > 1 && step.answer == null) p.choice = step.choices[index];
  credit(s, x, 1);
}
export function event(s, kind) {
  const x = activeQuest(s);
  if (x?.step.kind !== kind) return;
  credit(s, x, 1);
  if (kind === 'find' && x.q.steps[x.p.stage]?.kind === 'carry') x.p.lead = s.run.explorerId;
}
export function startPace(s, now = Date.now()) {
  if (activeQuest(s)?.step.kind !== 'pace' || s.run.phase !== 'trail')
    throw Error('Start the keeper’s trial on the trail.');
  s.run.pace = { start: now, last: now, feet: 0 };
}
export function paceSample(s, feet, now = Date.now()) {
  const p = s.run.pace;
  if (!p) return;
  if (now - p.last > 15000 || !activeQuest(s) || s.run.phase !== 'trail') {
    delete s.run.pace;
    return;
  }
  const end = p.start + 180000;
  p.feet += feet * Math.max(0, Math.min(1, (end - p.last) / Math.max(1, now - p.last)));
  p.last = now;
  if (now >= end) {
    if (p.feet >= 1584 - 1e-6) eventPace(s);
    else s.log.push('The bell fell quiet. Start a fresh three-minute attempt.');
    delete s.run.pace;
  }
}
function eventPace(s) {
  const x = activeQuest(s);
  if (x?.step.kind === 'pace') credit(s, x, 1584);
}
export function deepWalk(s, meters, east, north, roll) {
  const x = activeQuest(s);
  if (x) {
    const { p, step } = x;
    const len = Math.hypot(east, north),
      scale = len > meters ? meters / len : 1;
    const delta = {
      E: east * scale,
      W: -east * scale,
      N: north * scale,
      S: -north * scale,
      feet: meters,
      carry: 0,
    };
    if (step.kind === 'carry') {
      p.lead ??= s.run.explorerId;
      if (s.run.explorerId === p.lead && s.collection[p.lead]?.hp > 0) delta.carry = meters;
    }
    credit(s, x, (delta[step.kind] || 0) / FOOT);
  }
  const j = journal(s),
    row = deepSpecies.find((p) => p.id === j.target);
  if (!row || !huntReady(s, row)) return;
  const h = hunt(s, row.id);
  if (h.sign) return;
  h.feet += Math.max(0, Math.min(meters / FOOT, (s.run.distance + meters) / FOOT - row.minFeet));
  while (h.feet >= 2000 - 1e-7 && !h.sign) {
    h.feet -= 2000;
    h.checks++;
    h.sign = h.checks === 20 || roll(s) < (h.checks <= 10 ? 0.02 : 0.05);
  }
}
export function hunt(s, id) {
  return (journal(s).hunts[id] ??= { checks: 0, feet: 0, sign: false, trial: 0 });
}
export function huntReady(s, row) {
  return (
    row.rarity === 'mythic' &&
    row.region === s.world.activeLand &&
    done(s, { [REEF]: 6, [LOOM]: 8, [GLASS]: 10 }[row.region]) &&
    (row.id !== 'orrakai' || done(s, 11)) &&
    (s.run.deepWins ?? s.run.ordinaryWins) >= 2
  );
}
export const trialLessons = {
  nacreon: 'Interrupt the choir’s charged strike before the third bell, then win.',
  vesperloom: 'Guard against the shadow’s cutting strike, then win.',
  orrakai: 'Survive the announced falling crown, then win.',
};
export const trailsigns = {
  nacreon: 'A submerged shell answers the ferry with a fourth note.',
  vesperloom: 'A second shadow climbs down toward your lantern.',
  vitreus: 'Three mandrills approach, but only two cast shadows.',
  orrakai: 'A carved throne stirs below the royal seals.',
};
export const trials = {
  nacreon: [
    ['Replay the tide song.', ['High · Low · Middle', 'Low · High · Middle'], 1],
    ['The choir draws breath before its third bell.', ['Wait', 'Interrupt the choir'], 1],
  ],
  vesperloom: [
    ['The first knot hangs below the roots.', ['Follow the root knot', 'Climb toward the crown'], 0],
    ['The second swings in the wind.', ['Lower your lantern', 'Follow the span knot'], 1],
    ['Follow the knots from root to lantern.', ['Root · Span · Lantern', 'Lantern · Span · Root'], 0],
    ['Read the cutting strike.', ['Watch the body', 'Watch the second shadow'], 1],
  ],
  vitreus: [
    ['Identify the true body.', ['Bright reflection', 'Body without a shadow'], 1],
    ['Mirrors flare amber, rose, white. Break the first.', ['Amber', 'White', 'Rose'], 0],
    ['Break the second mirror.', ['White', 'Rose', 'Amber'], 1],
    ['Break the final mirror.', ['White', 'Amber', 'Rose'], 0],
  ],
  orrakai: [
    [
      'Arrange seals in the order the witnesses named them.',
      ['Reef · Loom · Glass', 'Glass · Reef · Loom'],
      0,
    ],
    ['The falling crown blots out the sky.', ['Brace under the throne', 'Chase the jewels'], 0],
  ],
};
export function trial(s, id, index) {
  const row = deepSpecies.find((r) => r.id === id),
    h = hunt(s, id);
  if (
    !row ||
    !h.sign ||
    !huntReady(s, row) ||
    s.run.distance / FOOT + 1e-6 < row.minFeet ||
    s.run.phase !== 'trail'
  )
    throw Error('Return to the hunt depth in its home region.');
  const cue = trials[id][h.trial];
  if (!cue || cue[2] !== index) throw Error('Read the sign and try again.');
  h.trial++;
}
export function attach(s, host, id) {
  if (
    !(s.run.phase === 'camp' || (s.run.phase === 'trail' && s.run.distance === 0)) ||
    !s.run.party.includes(host) ||
    !hosts.includes(s.collection[host]?.species)
  )
    throw Error('Attach a sidekick at camp or expedition setup.');
  const pairs = journal(s).pairs;
  if (!id) {
    delete pairs[host];
    return;
  }
  if (
    s.collection[host].hp <= 0 ||
    !(s.collection[id]?.hp > 0) ||
    s.run.party.includes(id) ||
    Object.entries(pairs).some(([h, r]) => h !== host && r === id)
  )
    throw Error('Choose a conscious, unassigned Sanctuary monkey.');
  pairs[host] = id;
}
export function rider(s, host) {
  const id = s.meta.deep?.pairs?.[host?.id];
  return hosts.includes(host?.species) &&
    s.run.party.includes(host.id) &&
    !s.run.party.includes(id) &&
    s.collection[id]?.hp > 0
    ? s.collection[id]
    : null;
}
export function travelers(s) {
  return s.run.party
    .flatMap((id) => {
      const h = s.collection[id];
      const r = h?.hp > 0 && rider(s, h);
      return r ? [h, r] : [h];
    })
    .filter(Boolean);
}
