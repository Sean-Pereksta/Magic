'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const F = require('../game/neon-arena/neon-factions.js'), C = require('../game/neon-arena/neon-campaign.js'), Q = require('../game/neon-arena/neon-conquest.js'), M = require('../game/neon-arena/neon-war-maps.js');
const gameDir = path.resolve(__dirname, '../game/neon-arena');
const source = fs.readFileSync(path.join(gameDir, 'neon-war-ui.js'), 'utf8');
const decode = text => String(text).replace(/&(?:amp|lt|gt|quot|#39);/g, entity => ({ '&amp;': '&', '&lt;': '<', '&gt;': '>', '&quot;': '"', '&#39;': "'" }[entity]));

// A small DOM adapter exercises the actual menu code without starting a browser.
// Rendering and touch actionability remain the responsibility of browser QA.
class Element {
  constructor(tag = 'div') { this.tagName = tag.toLowerCase(); this.children = []; this.attributes = {}; this.dataset = {}; this.listeners = {}; this.hidden = false; this.disabled = false; this.checked = false; this._text = ''; }
  get id() { return this.attributes.id || ''; }
  set id(value) { this.attributes.id = value; }
  setAttribute(key, value) { this.attributes[key] = String(value); if (key.startsWith('data-')) this.dataset[key.slice(5)] = String(value); if (['hidden', 'disabled', 'checked'].includes(key)) this[key] = true; }
  getAttribute(key) { return this.attributes[key] ?? null; }
  appendChild(child) { child.parent = this; this.children.push(child); return child; }
  before(child) { child.parent = this.parent; this.parent.children.splice(this.parent.children.indexOf(this), 0, child); }
  get options() { return this.children.filter(child => child.tagName === 'option'); }
  get value() {
    if (this.tagName === 'select') return (this.options.find(o => o.selected) || this.options.find(o => o.attributes.selected !== undefined) || this.options[0])?.value || '';
    return this._value ?? this.attributes.value ?? (this.tagName === 'option' ? this.textContent : '');
  }
  set value(value) {
    if (this.tagName === 'select') { for (const option of this.options) { option.selected = option.value === String(value); delete option.attributes.selected; } }
    else this._value = String(value);
  }
  get textContent() { return this._text + this.children.map(child => child.textContent).join(''); }
  set textContent(value) { this._text = String(value); this.children = []; }
  set innerHTML(html) {
    this.children = []; this._text = ''; this._html = html;
    const stack = [this];
    for (const token of html.match(/<[^>]+>|[^<]+/g) || []) {
      if (token.startsWith('</')) { stack.pop(); continue; }
      if (token.startsWith('<')) {
        const tag = token.match(/^<([\w-]+)/)?.[1]; if (!tag) continue;
        const child = new Element(tag);
        for (const attribute of token.slice(tag.length + 1, -1).matchAll(/([\w:-]+)(?:="([^"]*)")?/g)) child.setAttribute(attribute[1], decode(attribute[2] ?? ''));
        stack.at(-1).appendChild(child);
        if (!['img', 'input', 'link', 'br'].includes(tag)) stack.push(child);
      } else { const child = new Element('#text'); child._text = decode(token); stack.at(-1).appendChild(child); }
    }
  }
  get innerHTML() { return this._html || ''; }
  matches(selector) {
    const attribute = selector.match(/^\[([^=\]]+)(?:="([^"]*)")?\]$/);
    if (attribute) { const value = attribute[1].startsWith('data-') ? this.dataset[attribute[1].slice(5)] : this.getAttribute(attribute[1]); return value != null && (attribute[2] === undefined || value === attribute[2]); }
    return selector.startsWith('#') ? this.id === selector.slice(1) : this.tagName === selector;
  }
  querySelectorAll(selector) { return this.children.flatMap(child => [...(child.matches(selector) ? [child] : []), ...child.querySelectorAll(selector)]); }
  querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
  closest(selector) { return this.matches(selector) ? this : this.parent?.closest(selector) || null; }
  addEventListener(type, callback) { (this.listeners[type] ||= []).push(callback); }
  dispatchEvent(event) { event.target ||= this; for (const callback of this.listeners[event.type] || []) callback(event); if (event.bubbles) this.parent?.dispatchEvent(event); }
  click() { if (!this.disabled) this.dispatchEvent({ type: 'click', bubbles: true }); }
}

function harness(saved = {}, initialProgress = C.freshProgress(), mode = 'conquest') {
  const document = new Element('document'), head = document.appendChild(new Element('head')), body = document.appendChild(new Element('body'));
  document.head = head; document.baseURI = 'http://localhost/game/neon-arena/';
  document.currentScript = { src: document.baseURI + 'neon-war-ui.js' };
  document.createElement = tag => new Element(tag);
  document.getElementById = id => document.querySelector('#' + id);
  body.innerHTML = '<select id="mode"><option>dm</option><option>tdm</option><option>campaign</option><option>conquest</option></select><select id="map">'
    + ['grid', ...M.IDS].map(id => '<option>' + id + '</option>').join('') + '</select><select id="secondPlayer"><option>off</option><option>keyboard</option></select><div id="arenaCards">'
    + ['grid', ...M.IDS].map(id => '<button data-map="' + id + '"></button>').join('') + '</div><div id="characterLocker"></div><div id="advancedOptions"></div>';
  const $ = id => document.getElementById(id); $('mode').value = mode;
  let progress = initialProgress;
  const writes = [], events = [], mapEvents = [];
  document.addEventListener('neon-war-selection', event => events.push(event.detail));
  $('map').addEventListener('change', event => mapEvents.push(event.target.value));
  const root = { NeonFactions: F, NeonCampaign: C, NeonWarMaps: M, NeonSurvivalProgress: { store: { getCampaign: () => progress } } };
  const Event = class { constructor(type, options = {}) { this.type = type; Object.assign(this, options); } };
  const context = vm.createContext({ window: root, document, URL, Event, CustomEvent: Event,
    localStorage: { getItem: () => typeof saved === 'string' ? saved : JSON.stringify(saved), setItem: (key, value) => writes.push({ key, value: JSON.parse(value) }) } });
  vm.runInContext(source, context); root.NeonWarUI.mount();
  return { $, root, document, writes, events, mapEvents, change(id, value) { $(id).value = value; $(id).dispatchEvent(new Event('change', { bubbles: true })); },
    mode(value) { $('mode').value = value; root.NeonWarUI.refresh(); }, progress(value) { progress = value; root.NeonWarUI.refreshProgress(); } };
}

test('mount is idempotent, loads its own stylesheet, and preserves existing select values', () => {
  const h = harness({ faction: 'cats', character: 'cat_heavy', weapon: 'thunderpaw', rule: 'score', budget: 12, base: 6, perFlag: 0, resume: false });
  h.root.NeonWarUI.mount();
  assert.equal(h.document.querySelectorAll('#warSetup').length, 1);
  assert.equal(h.document.querySelectorAll('#neonWarMenuStyles').length, 1);
  assert.equal(h.$('neonWarMenuStyles').href, 'http://localhost/game/neon-arena/neon-war-menu.css');
  const choice = h.root.NeonWarUI.selection();
  assert.deepEqual(JSON.parse(JSON.stringify(choice)), { faction: 'cats', character: 'cat_heavy', weapon: 'thunderpaw', rule: 'score', budget: 12, base: 6, perFlag: 0, resume: false, livery: false, mission: 'cats_silent_outpost' });
  assert.deepEqual(h.$('warBudget').options.map(o => o.value), ['8', '12', '16']);
  assert.deepEqual(h.$('warBase').options.map(o => o.value), ['3', '4', '5', '6']);
  assert.deepEqual(h.$('warPerFlag').options.map(o => o.value), ['0', '1', '2']);
  assert.equal(h.$('warWeapon').options.length, 6);
});

test('faction cards update native selection and events while remembering each faction appearance', () => {
  const h = harness(); h.change('warCharacter', 'mouse_sniper');
  const cats = h.$('warFactionCards').querySelector('[data-faction="cats"]'), mice = h.$('warFactionCards').querySelector('[data-faction="mice"]');
  cats.querySelector('strong').click();
  assert.equal(h.root.NeonWarUI.selection().faction, 'cats');
  assert.ok(h.$('warCharacter').options.every(o => o.value.startsWith('cat_')));
  assert.equal(cats.getAttribute('aria-pressed'), 'true');
  assert.equal(mice.getAttribute('aria-pressed'), 'false');
  h.change('warCharacter', 'cat_officer'); mice.click();
  assert.equal(h.root.NeonWarUI.selection().character, 'mouse_sniper');
  assert.equal(h.root.NeonWarUI.selection().weapon, 'burrower');
  cats.click(); assert.equal(h.root.NeonWarUI.selection().character, 'cat_officer');
  assert.equal(h.events.length, 5); assert.equal(h.mapEvents.length, 5);
  assert.equal(h.writes.at(-1).key, 'neonBreachWarSelectionV1');
  assert.equal(h.writes.at(-1).value.character, 'cat_officer');
});

test('Conquest presets preserve victory choice and roster explanations match the actual simulation', () => {
  const h = harness({ rule: 'score' });
  for (const [preset, budget, base] of [['skirmish', 8, 3], ['balanced', 12, 4], ['large', 16, 4]]) {
    h.$('warPresets').querySelector('[data-preset="' + preset + '"]').querySelector('strong').click();
    const choice = h.root.NeonWarUI.selection(), state = Q.create(M.worldMetadata('cold_storage'), { ...choice, botBudget: choice.budget });
    assert.equal(choice.rule, 'score'); assert.equal(choice.budget, budget); assert.equal(choice.base, base);
    assert.equal(Number(h.$('warRosterBase').textContent), Q.rosterTarget(state, 1));
    state.sectors.forEach(sector => { sector.owner = 1; });
    assert.equal(Number(h.$('warRosterFull').textContent), Q.rosterTarget(state, 1));
    assert.equal(Number(h.$('warRosterBudget').textContent), state.config.botBudget);
    assert.match(h.$('warRuleHelp').textContent, /1 point per second/);
  }
  h.change('warPerFlag', '0');
  assert.equal(h.root.NeonWarUI.selection().perFlag, 0);
  assert.equal(h.writes.at(-1).value.perFlag, 0);
  assert.equal(h.$('warRosterFull').textContent, h.$('warRosterBase').textContent);
  assert.equal(h.$('warPresetStatus').textContent, 'Custom roster');
  assert.match(h.$('warRosterHelp').textContent, /rosters stay fixed/);
});

test('campaign chapters enforce unlocks and refresh progress, next operation and checkpoint details', () => {
  const h = harness({}, C.freshProgress(), 'campaign'), chapters = h.$('warChapters').querySelectorAll('[data-chapter]');
  chapters[1].click(); assert.equal(h.root.NeonWarUI.selection().mission, 'mice_broken_convoy');
  assert.equal(chapters.filter(b => !b.disabled).length, 1); assert.equal(h.$('warResume').disabled, true);
  const progress = C.freshProgress();
  progress.completed.mice_broken_convoy = { stars: 3, bestTime: 83, completions: 1, difficulty: 'normal' };
  progress.checkpoints.mice_behind_enemy_lines = { index: 2 };
  h.progress(progress);
  assert.equal(chapters[1].disabled, false); assert.equal(chapters[2].disabled, true);
  assert.match(h.$('warCampaignProgress').textContent, /1 \/ 5.*3 \/ 15/);
  assert.equal(h.$('warContinue').hidden, false);
  assert.match(h.$('warCheckpointStatus').textContent, /best result ★★★ · 1:23/);
  h.$('warContinue').click();
  assert.equal(h.root.NeonWarUI.selection().mission, 'mice_behind_enemy_lines');
  assert.equal(h.$('map').value, 'rail_yard_nine');
  assert.equal(h.$('warResume').disabled, false);
  assert.match(h.$('warCheckpointStatus').textContent, /Resume at objective 3: Copy/);
  assert.equal(h.$('warObjectives').querySelectorAll('li').length, 4);
  assert.equal(h.$('warObjectives').querySelectorAll('li').filter(li => li.getAttribute('class') === 'complete').length, 2);
  h.$('warResume').checked = false; h.$('warResume').dispatchEvent({ type: 'change', bubbles: true });
  assert.equal(h.writes.at(-1).value.resume, false);
  assert.match(h.$('warCheckpointStatus').textContent, /Fresh start selected/);
  assert.equal(h.$('warObjectives').querySelectorAll('li').filter(li => li.getAttribute('class') === 'complete').length, 0);
});

test('campaign rewards stay cosmetic and locked until a mission completion exists', () => {
  const h = harness({ livery: true }, C.freshProgress(), 'campaign');
  assert.equal(h.$('warLivery').disabled, true); assert.equal(h.root.NeonWarUI.selection().livery, false);
  const progress = C.freshProgress(); progress.completed.cats_silent_outpost = { stars: 1, bestTime: 60 };
  h.root.NeonWarUI.setProfile(() => progress);
  assert.equal(h.$('warLivery').disabled, false); assert.equal(h.root.NeonWarUI.selection().livery, true);
  assert.match(h.$('warMedals').textContent, /1 mission medal earned/);
});

test('all character, weapon and mission previews resolve to shipped local assets', () => {
  const progress = C.freshProgress(); for (const faction of ['cats', 'mice']) for (const m of C.catalog(faction)) progress.completed[m.id] = { stars: 1, bestTime: 30 };
  const h = harness({}, progress, 'campaign');
  const exists = id => assert.ok(fs.existsSync(path.resolve(gameDir, h.$(id).src)), h.$(id).src);
  for (const faction of ['cats', 'mice']) {
    h.change('warFaction', faction);
    for (const character of F.characters.filter(c => c.faction === faction)) { h.change('warCharacter', character.id); exists('warPortrait'); assert.equal(h.$('warPortrait').alt, character.name); }
    for (const mission of C.catalog(faction)) { h.change('warMission', mission.id); exists('warMissionPreview'); assert.equal(h.$('warMissionTitle').textContent, mission.name); }
  }
  for (const weapon of F.weapons) { h.change('warWeapon', weapon.id); exists('warWeaponPreview'); assert.equal(h.$('warWeaponMagazine').textContent, String(weapon.mag)); assert.equal(h.$('warWeaponReload').textContent, weapon.reload + 's'); }
});

test('returning to original modes restores their map visibility and character locker', () => {
  const h = harness({}, C.freshProgress(), 'campaign');
  assert.equal(h.$('secondPlayer').disabled, true);
  assert.equal(h.$('arenaCards').querySelectorAll('[data-map]').filter(card => !card.hidden).length, 1);
  h.mode('conquest'); assert.equal(h.$('arenaCards').querySelectorAll('[data-map]').filter(card => !card.hidden).length, 6);
  assert.equal(h.$('conquestSetup').hidden, false); assert.equal(h.$('campaignSetup').hidden, true);
  h.mode('dm');
  assert.equal(h.$('warSetup').hidden, true); assert.equal(h.$('characterLocker').hidden, false); assert.equal(h.$('secondPlayer').disabled, false);
  assert.ok(h.$('arenaCards').querySelectorAll('[data-map]').every(card => !card.hidden));
});

test('missing or malformed local preferences fall back to a playable selection', () => {
  for (const saved of ['null', '{invalid', { faction: 'bad', character: 'bad', weapon: 'bad', mission: 'bad' }]) {
    const h = harness(saved), selected = h.root.NeonWarUI.selection();
    assert.ok(F.characters.some(c => c.id === selected.character && c.faction === selected.faction));
    assert.ok(F.weapons.some(w => w.id === selected.weapon));
    assert.ok(C.isUnlocked(C.freshProgress(), selected.mission));
  }
});
