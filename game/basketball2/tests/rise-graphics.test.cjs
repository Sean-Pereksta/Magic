const test=require('node:test');
const assert=require('node:assert/strict');
const vm=require('node:vm');
const fs=require('node:fs');
const Rise=require('../js/campaign/rise-to-glory.js');
const Graphics=require('../js/campaign/rise-graphics.js');

test('the vector library covers the campaign catalog without repeating ability or drill artwork',()=>{
  assert.deepEqual(Graphics.keys.abilities,Object.keys(Rise.ABILITIES));
  assert.deepEqual(Graphics.keys.training,Rise.TRAINING.map(drill=>drill.id));
  assert.deepEqual(Graphics.keys.categories,Object.keys(Rise.ATTRIBUTES));
  assert.deepEqual(Graphics.keys.archetypes,Object.keys(Rise.ARCHETYPES));
  assert.deepEqual(Graphics.keys.divisions,Rise.DIVISIONS.map(division=>division.id));
  for(const tab of ['hub','roster','develop','market','schedule','strategy','training','stats'])assert.ok(Graphics.keys.icons.includes(tab));
  for(const [method,keys]of [['ability',Graphics.keys.abilities],['training',Graphics.keys.training],['division',Graphics.keys.divisions]]){
    const artwork=keys.map(key=>Graphics[method](key).replace(/class="[^"]*"/g,''));
    assert.equal(new Set(artwork).size,keys.length,`${method} glyphs should be visually distinct`);
  }
});

test('repeated inline marks need no shared IDs or external assets and default to decorative accessibility',()=>{
  const marks=[...Graphics.keys.icons.map(key=>Graphics.icon(key)),...Graphics.keys.abilities.map(key=>Graphics.ability(key)),...Graphics.keys.training.map(key=>Graphics.training(key)),...Graphics.keys.divisions.map(key=>Graphics.division(key)),Graphics.court()];
  for(const mark of marks){
    assert.match(mark,/^<svg\b/);
    assert.match(mark,/<\/svg>$/);
    assert.match(mark,/aria-hidden="true"/);
    assert.match(mark,/focusable="false"/);
    assert.doesNotMatch(mark,/\b(?:id|href|src|onload|onclick)\s*=/i);
    assert.doesNotMatch(mark,/<(?:script|image|foreignObject)\b|url\(/i);
  }
});

test('accessible marks expose escaped descriptions and filter user-provided SVG attributes',()=>{
  const svg=Graphics.ability('deadeye',{label:'Eye <screen> "focus" & aim',className:'is-owned test-badge bad" onload="evil',size:32});
  assert.match(svg,/role="img"/);
  assert.match(svg,/aria-label="Eye &lt;screen&gt; &quot;focus&quot; &amp; aim"/);
  assert.match(svg,/<title>Eye &lt;screen&gt; &quot;focus&quot; &amp; aim<\/title>/);
  assert.match(svg,/width="32" height="32"/);
  assert.match(svg,/is-owned test-badge/);
  assert.doesNotMatch(svg,/aria-hidden|onload|<screen>/);
  assert.match(Graphics.division(4,{decorative:false}),/aria-label="Elite Championship crest"/);
  assert.doesNotMatch(Graphics.icon('hub',{size:'44" onload="evil'}),/\swidth=|\sonload=/);
});

test('unknown catalog values safely fall back and division IDs match numeric tiers',()=>{
  assert.equal(Graphics.division('elite'),Graphics.division(4));
  assert.equal(Graphics.division(99),Graphics.division(0));
  assert.match(Graphics.icon('__proto__'),/rise-graphic-basketball/);
  assert.match(Graphics.ability('<bad>'),/rise-graphic-basketball/);
  assert.match(Graphics.training('<bad>'),/rise-graphic-whistle/);
  assert.match(Graphics.category('<bad>'),/rise-graphic-basketball/);
  assert.match(Graphics.archetype('<bad>'),/rise-graphic-allRounder/);
  assert.match(Graphics.court({decorative:false}),/aria-label="Three-player halfcourt spacing and movement diagram"/);
});

test('the same library loads directly in a browser without CommonJS or dependencies',()=>{
  const browser={};
  vm.runInNewContext(fs.readFileSync(require.resolve('../js/campaign/rise-graphics.js'),'utf8'),browser);
  assert.equal(typeof browser.RiseGraphics.ability,'function');
  assert.equal(browser.RiseGraphics.icon('hub'),Graphics.icon('hub'));
});
