const {test} = require('node:test');
const assert = require('node:assert/strict');
const {createHash} = require('node:crypto');
const {simulation} = require('./helpers/expansion-harness.cjs');

// Reference values recorded before the graphics/control upgrade. These protect
// the complete roster, economy, cooldowns, research, difficulty and victory rules
// from accidental balance changes in presentation work.
const reference = {
  units: [40, '7f350270573d72899405422058c967155d383b5d853333c7a894dedf9c1d78ee'],
  structures: [34, 'fb284da05098c835865efee668299760b9fbe05047857cba47bf8aca23027c57'],
  abilities: [54, '25fadb91b75def13156a8da32a8fe7c07a15afbfea434c409c610e65d91adcf0'],
  research: [22, '8c31a09bb56c82b75565343540a4144ce90596d2b32776211c38320ba4be21e9'],
  difficulty: [4, 'bae316c148d7d5a5950173bef5aff8521c7604deb2e4a7093a4fc36b1d531a31'],
  factions: [3, 'd481a66cb55a107a5980a102e82d15fea06d307d296f1bfd8761eb6945adf815'],
  modes: [3, 'd57bae01843fdd3f6e15d4a54d80f281ac45e193f4cbffd51271ed76a19b4e2c'],
  constants: [5, 'a769a5fab983cf1d1785bba0122b642ab3a04cddcadb738b38923347a8cc8903']
};

test('the visual upgrade preserves every original simulation definition and balance value', () => {
  const run = simulation();
  const definitions = run.plain(`({
    units: UNITS, structures: STRUCTURES, abilities: ABILITIES,
    research: RESEARCH_DEFS, difficulty: DIFFICULTY, factions: FACTIONS,
    modes: GAME_MODES,
    constants: {BOARD_W, BOARD_H, TILE, BASE_COLLECTION_RADIUS, AI_WORKER_HARD_CAP}
  })`);
  for (const [category, [count, hash]] of Object.entries(reference)) {
    assert.equal(Object.keys(definitions[category]).length, count, category + ' coverage');
    assert.equal(createHash('sha256').update(JSON.stringify(definitions[category])).digest('hex'), hash,
      category + ' must retain its original stats and behavior');
  }
});
