'use strict';
const assert = require('node:assert/strict');
const path = require('node:path');
const { bootUI } = require('./command-ui-test.cjs');
const root = process.argv[2] || path.resolve(__dirname, '..');
const ui = bootUI(root, { menu: true });
const D = ui.window.GameData, battles = D.PRESETS.filter(p => p.historical && typeof p.historical === 'object');
const el = id => ui.document.getElementById(id);
assert(battles.length >= 30);
el('menuHistory').click();
assert.equal(el('historyBrowser').hidden, false);
assert.equal(el('menuMapGrid').hidden, true);
assert.equal(ui.document.querySelectorAll('[data-history-battle]').length, battles.length);
const target = battles.find(p => /Waterloo/i.test(p.name)) || battles.at(-1);
el('historySearch').value = target.name;
el('historySearch').dispatch('input');
const visible = ui.document.querySelectorAll('[data-history-battle]').filter(b => !b.hidden);
assert.equal(visible.length, 1);
assert.equal(el('menuMapTitle').textContent, target.name);
assert.equal(el('historyDeployment').textContent, target.historical.deployment);
assert.match(el('historySources').innerHTML, /https:\/\//);
el('historySearch').value = 'no-such-battle';
el('historySearch').dispatch('input');
assert.equal(el('historyEmpty').hidden, false);
el('historySearch').value = '';
el('historyEra').value = target.historical.era;
el('historyEra').dispatch('change');
const eraCards = ui.document.querySelectorAll('[data-history-battle]').filter(b => !b.hidden);
assert(eraCards.length > 0);
assert(eraCards.every(b => battles.find(p => p.id === b.dataset.historyBattle).historical.era === target.historical.era));
el('historyGrid').dispatch('click', { target: ui.document.querySelector('[data-history-battle="' + target.id + '"]') });
el('menuDeploy').click();
assert.equal(ui.window.IRONFIELD.menuOpen, false);
assert.equal(ui.engine.phase, 'deployment');
assert.equal(ui.engine.map.name, target.customMap.name);
assert.equal(ui.engine.regiments.length, target.formations.length);
for (let i = 0; i < target.formations.length; i++) {
  const expected = target.formations[i], r = ui.engine.regiments[i];
  assert.equal(r.type, expected.type);
  assert.equal(r.team, expected.team);
  assert.equal(r.initial, expected.count);
  assert(Math.hypot(r.x - expected.x, r.y - expected.y) < 2, 'Opening deployment moved');
}
for (const factor of [.5, 2, 4]) {
  ui.window.IRONFIELD.loadPreset(target.id, factor);
  const cfg = ui.window.IRONFIELD.config;
  assert.equal(cfg.formations.reduce((n, f) => n + f.count, 0), ui.engine.units.length, 'Scaling created an auto-deployed reserve');
  for (const f of cfg.formations) {
    assert(target.formations.some(g => g.team === f.team && g.type === f.type && g.x === f.x && g.y === f.y), 'Scaling changed the historical formation layout');
  }
}
const muskets = battles.find(p => p.formations.some(f => f.type === 'musketeer'));
ui.window.IRONFIELD.loadPreset(muskets.id);
const gun = ui.engine.regiments.find(r => r.type === 'musketeer');
ui.select(gun);
for (const formation of ['volley', 'column']) {
  el('formationSelect').value = formation;el('formationSelect').dispatch('change');
  assert.equal(ui.engine.getRegiment(gun.id).formation, formation);
  assert(ui.window.IRONFIELD.config.formations.some(f => f.type === 'musketeer' && f.formation === formation));
  assert.match(el('selectedFormationDetails').textContent,formation==='volley'?/Three ranks.*fire by rank/:/Three soldiers abreast.*fire by row/);
}
el('scenarioName').value = 'Historical column saved';el('saveScenario').click();
const saved = JSON.parse(ui.storage.get('ironfield.scenarios.v1'))[0];
assert(saved.config.formations.some(f => f.type === 'musketeer' && f.formation === 'column'));
el('menuButton').click();el('menuFieldsTab').click();
assert.equal(el('historyBrowser').hidden, true);
assert.equal(el('menuMapGrid').hidden, false);
console.log('PASS Historical search, eras, sources, exact deployment, army scaling, formation choices, persistence, and collection navigation');
