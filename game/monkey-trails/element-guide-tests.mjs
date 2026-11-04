import assert from 'node:assert/strict';
import test from 'node:test';
import { ELEMENTS, TYPE_CHART, effectiveness } from './core.mjs';
import { counterList, elementGuide } from './element-guide.mjs';
test('the help table matches every combat matchup in attack-row order', () => {
  const ids = Object.keys(ELEMENTS),
    html = elementGuide();
  assert.equal((html.match(/<td /g) || []).length, 64);
  for (const d of ids) {
    const rows = counterList(d);
    for (const a of ids) assert.equal(rows.find((row) => row.id === a).value, TYPE_CHART[a][d]);
    assert.ok(rows.every((r, i) => !i || r.value <= rows[i - 1].value));
  }
  assert.match(html, /Fire against Leaf \/ Water.*0.9×/);
  assert.match(html, /1.325×/);
});
test('dual defenders match the combat caps and repeated elements count once', () => {
  for (const a of Object.keys(ELEMENTS))
    for (const d of Object.keys(ELEMENTS))
      for (const row of counterList(a, d)) assert.equal(row.value, effectiveness(row.id, [a, d]));
  assert.equal(counterList('leaf', 'ice').find((r) => r.id === 'fire').value, 4);
  assert.equal(counterList('earth', 'water').find((r) => r.id === 'fire').value, 0.25);
});
