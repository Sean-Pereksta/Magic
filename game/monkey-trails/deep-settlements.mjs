import { quests, page, done } from './deep-journal.mjs';
import { GLASS } from './deep-roster.mjs';
const encounters = [
  ['The keeper listens for thunder beyond the stones.', 'Listen for the next storm', null],
  [
    'A red seal hangs from the returning courier’s satchel.',
    'Trade 2 fruit for a Bond Charm',
    { price: 2, item: 'bondCharm' },
  ],
  ['The ferrymonkey waves from the repaired crossing.', 'Ride the ferry', null],
  ['The youngster calls to the monkey that carried it.', 'Follow to a hidden basket', { fruit: 3 }],
  ['Warm lanterns mark the north shelter.', 'Rest at the shelter', { heal: true }],
  [
    'The smith holds up the bell clasp you forged.',
    'Trade 2 fruit for charm material',
    { price: 2, item: 'charmMaterial' },
  ],
  ['The ferry pilot listens for a fourth note.', 'Listen below the reef', null],
  ['The pearl diver stretches a shell across a drum frame.', 'Learn to craft the shell drum', null],
  ['A visitor from across the first span unpacks a gift.', 'Welcome the visitor', { item: 'berry' }],
  ['Monkeys from both colonies share a meal.', 'Join the bridge supper', { heal: true }],
  ['Water shines in the new channel.', 'Visit the spring', null],
  ['Four long tracks lead down the stairs of the buried court.', 'Read the seals', null],
];
export function visits(s) {
  return quests
    .filter((q) => q.land === s.world.activeLand && done(s, q.id))
    .map((q) => {
      const p = page(s, q.id),
        [scene, label, reward] = encounters[q.id],
        caravan = q.land === GLASS && p.choice === 'Caravan';
      return {
        id: q.id,
        scene: scene + (p.choice ? ' ' + p.choice + '.' : ''),
        label: caravan ? 'Barter 3 fruit for a tonic' : label,
        reward: caravan ? { price: 3, item: 'tonic' } : q.id === 10 ? { fruit: 1 } : reward,
        ready:
          (![0, 1, 4, 5, 7, 8, 9].includes(q.id) || s.run.phase === 'camp') &&
          (q.id !== 3 || s.run.explorerId === p.lead) &&
          p.visitToken !== visitToken(s),
      };
    });
}
const visitToken = (s) => s.meta.walkSessions?.active?.id || s.run.id + ':' + s.run.camp?.id;
export function visit(s, id, forecast) {
  const row = visits(s).find((r) => r.id === id);
  if (!row?.ready || !['camp', 'trail'].includes(s.run.phase))
    throw Error('Return on a later walk with the required companion or meet the visitor at camp.');
  const p = page(s, id),
    reward = row.reward;
  if (reward?.price > s.run.fruit) throw Error('Gather enough fruit for the trade.');
  if (reward) {
    s.run.fruit += (reward.fruit || 0) - (reward.price || 0);
    if (reward.item) s.run.items[reward.item] = (s.run.items[reward.item] || 0) + 1;
    if (reward.heal) s.run.items.salve++;
  }
  if (id === 7) p.recipe = true;
  p.visitToken = visitToken(s);
  const line =
    id === 0
      ? 'Bellkeeper’s forecast · ' + forecast
      : id === 6
        ? 'The pilot hears a fourth note beneath the hull.'
        : id === 7
          ? 'Shell drum recipe learned. At camp, bind two River Pearls to charm material.'
          : row.scene;
  s.log.push(line);
  return line;
}
export function craftDrum(s) {
  if (
    s.run.phase !== 'camp' ||
    !page(s, 7).recipe ||
    s.run.essence.water < 2 ||
    s.run.items.charmMaterial < 1
  )
    throw Error('At camp, use two Water essence and one charm material to craft a shell drum.');
  s.run.essence.water -= 2;
  s.run.items.charmMaterial--;
  s.run.items.scoutFeather++;
  return 'The shell drum reveals the next Ultra like a Scout Feather.';
}
