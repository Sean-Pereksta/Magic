import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

const html = fs.readFileSync(new URL('./chess_warlord.html', import.meta.url), 'utf8');
const moduleMatch = html.match(/<script type="module">([\s\S]*?)<\/script>/);
assert.ok(moduleMatch, 'ChessWarlords module script should exist');
const source = moduleMatch[1];
const compilableSource = source.replace(/import\s+[\s\S]*?\s+from\s+["'][^"']+["'];/g, '');

function extractFunction(name) {
  const start = source.indexOf(`function ${name}`);
  assert.notEqual(start, -1, `${name} should exist`);
  const bodyStart = source.indexOf('{', start);
  let depth = 0;
  for (let index = bodyStart; index < source.length; index++) {
    if (source[index] === '{') depth++;
    if (source[index] === '}' && --depth === 0) return source.slice(start, index + 1);
  }
  throw new Error(`Could not extract ${name}`);
}

test('the module remains syntactically valid', () => {
  assert.doesNotThrow(() => new Function(compilableSource));
});

test('Shadow special movement cannot capture a King', () => {
  const helpers = new Function(`${extractFunction('isShadowSpecialMoveMeta')}\n${extractFunction('canShadowSpecialCaptureTarget')}\nreturn {isShadowSpecialMoveMeta,canShadowSpecialCaptureTarget};`)();
  for (const meta of [{phase:true},{shadowTeleport:true},{mirror:true},{markedAssassination:true}]) {
    assert.equal(helpers.isShadowSpecialMoveMeta(meta), true);
    assert.equal(helpers.canShadowSpecialCaptureTarget(meta, {type:'king'}), false);
    assert.equal(helpers.canShadowSpecialCaptureTarget(meta, {type:'queen'}), true);
  }
  assert.equal(helpers.canShadowSpecialCaptureTarget({}, {type:'king'}), true, 'normal combat against Kings stays legal');
});

test('every Shadow displacement path carries special-move metadata', () => {
  for (const pattern of ['shadepawn','blinkpawn','assassin','shadowstepper','wraith','umbralbishop','voidstrider']) {
    const patternStart = source.indexOf(`pattern==='${pattern}'`);
    assert.notEqual(patternStart, -1, `${pattern} movement block should exist`);
    const nextPattern = source.indexOf("pattern==='", patternStart + 12);
    const block = source.slice(patternStart, nextPattern === -1 ? patternStart + 2200 : nextPattern);
    assert.match(block, /(shadowTeleport|phase|markedAssassination)/, `${pattern} must identify its special movement`);
  }
  assert.match(source, /target\.type\s*===\s*'king'\s*\)\s*return false;/, 'Shadow Marks reject Kings');
  assert.match(source, /target\.type\s*!==\s*'king'[\s\S]{0,180}shadowMarkedUntil/, 'Wraith execution rejects Kings');
});

test('Assassin AI values surgical targets without the former King chase bonus', () => {
  assert.match(source, /function shadowTargetPriority/);
  assert.match(source, /shadowMarkedUntil[\s\S]{0,120}score\+=210/);
  assert.match(source, /target\.hp<target\.maxHp/);
  assert.match(source, /nearbyFriendlyCount\(target,2\)/);
  assert.match(source, /style==='assassin'[\s\S]{0,100}Math\.max\(0,6-d\*\.45\)/);
  assert.doesNotMatch(source, /target\.type==='king'\?400/);
});

test('motion and effects retain performance fallbacks', () => {
  assert.match(source, /const MOVE_ANIMATION_MS = 150/);
  assert.match(source, /const PHASE_ANIMATION_MS = 245/);
  assert.match(source, /if\(reduceMotion\) return 0/);
  assert.match(source, /graphicsQualityMode==='performance'\) return 0/);
  assert.match(source, /if\(tier===0 && !special\) return/);
  assert.match(source, /function topKByScore/);
});
