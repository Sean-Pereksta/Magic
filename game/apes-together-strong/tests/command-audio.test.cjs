'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const { loadEngine } = require('./performance-harness.cjs');
const root = path.resolve(__dirname, '..');

function fixture(roll = .5) {
  let first = true, seed = 93;
  const math = Object.create(Math);
  math.random = () => { if (first) { first = false; return roll; } seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 4294967296; };
  const c = vm.createContext({ console, Math: math, Set, Promise, Uint8Array, document: { hidden: false }, atob: s => Buffer.from(s, 'base64').toString('binary') });
  c.window = c;
  vm.runInContext(fs.readFileSync(path.join(root, 'audio.js'), 'utf8'), c);
  const param = () => ({ value: 0, peaks: [], setValueAtTime(v) { this.value = v; }, linearRampToValueAtTime(v) { this.peaks.push(v); } });
  const node = () => ({ disconnected: false, connect() {}, disconnect() { this.disconnected = true; } });
  const created = [], gains = [];
  const ctx = { currentTime: 10, state: 'running', close() { return Promise.resolve(); },
    createGain() { const n = { ...node(), gain: param() }; gains.push(n); return n; },
    createStereoPanner() { return { ...node(), pan: param() }; },
    createBufferSource() { const n = { ...node(), start(time, offset) { this.started = { time, offset }; }, stop() { this.stopped = true; this.onended?.(); } }; created.push(n); return n; }
  };
  const a = new c.ATSAudio(); assert.equal(a.ctx, null, 'construction stays silent');
  a.ctx = ctx; a.master = node();
  a.commandBuffers = Array.from({ length: 9 }, (_, i) => ({ id: 'clip-' + i, buffer: { duration: 3 + i / 10 } }));
  return { a, c, ctx, created, gains, roll(value) { roll = value; first = true; } };
}

test('commands choose distinct solo/duet/trio samples, start together, and vary selections', () => {
  const f = fixture(), seen = new Set();
  for (let batch = 0; batch < 30; batch++) {
    const count = batch % 3 + 1; f.roll([.1, .5, .9][count - 1]);
    f.a.play('call', 1.4, -.2);
    const voices = [...f.a.commandSources]; assert.equal(voices.length, count);
    assert.equal(new Set(voices.map(s => s.buffer)).size, count);
    assert.equal(new Set(voices.map(s => s.started.time)).size, 1, 'chorus starts simultaneously');
    for (const s of voices) { assert.equal(s.loop, false); assert.equal(s.started.offset, undefined, 'original clip starts at its beginning'); seen.add(s.buffer); }
    for (const s of voices) s.stop();
    f.ctx.currentTime += 1;
  }
  assert.equal(seen.size, 9, 'the whole library participates');
  assert.ok(f.gains.every(g => g.gain.peaks.every(v => v <= 1)));
});

test('a shared chorus budget keeps rapid and overlapping command sounds soft and bounded', () => {
  const f = fixture(.9); f.a.play('call'); assert.equal(f.a.commandSources.size, 3);
  f.a.play('charge'); assert.equal(f.a.commandSources.size, 3, 'different commands share a throttle');
  f.ctx.currentTime += .7; f.roll(.9); f.a.play('recall'); assert.equal(f.a.commandSources.size, 6);
  assert.ok(f.a.commandGain.gain.value * f.a.commandSources.size <= .18);
  f.ctx.currentTime += .7; f.a.play('hold'); assert.equal(f.a.commandSources.size, 6, 'long samples cannot pile up');
  const ended = [...f.a.commandSources][0]; ended.stop(); assert.ok(ended.disconnected);
  assert.equal(f.a.sources.size, 5); assert.ok(f.a.commandGain.gain.value * f.a.commandSources.size <= .18);
  f.ctx.currentTime += .7; f.roll(.9); f.a.play('patrol'); assert.equal(f.a.commandSources.size, 6);
  const busy = fixture(.9); for (let i = 0; i < busy.a.maxVoices - 1; i++) busy.a.sources.add({});
  busy.a.play('charge'); assert.equal(busy.a.sources.size, busy.a.maxVoices, 'battle voice limit also applies');
});

test('sampled calls respect mute, zero volume, pause, hidden tabs and disposal', () => {
  for (const setting of [{ enabled: false }, { volume: 0 }]) {
    const f = fixture(); f.a.play('call'); assert.ok(f.a.commandSources.size > 0);
    f.a.setSettings(setting); assert.equal(f.a.commandSources.size, 0); f.ctx.currentTime += 1;
    f.a.play('charge'); assert.equal(f.a.commandSources.size, 0);
  }
  const f = fixture(); f.c.document.hidden = true; f.a.play('call'); assert.equal(f.a.commandSources.size, 0);
  f.c.document.hidden = false; f.ctx.currentTime += 1; f.a.play('recall'); assert.ok(f.a.commandSources.size);
  f.a.pause(); assert.equal(f.a.commandSources.size, 0); f.a.play('hold'); assert.equal(f.a.commandSources.size, 0);
  f.a.pause(false); f.a.play('hold'); assert.ok(f.a.commandSources.size);
  f.a.dispose(); assert.equal(f.a.ctx, null); assert.equal(f.a.sources.size, 0); assert.equal(f.a.commandBuffers.length, 0);
});

test('each clip decodes once, a bad clip is isolated, and a disposed context cannot refill the cache', async () => {
  const f = fixture(); f.a.commandBuffers = []; let decodes = 0;
  f.c.ATS_COMMAND_SOUNDS = [{ id: 'good', src: 'data:audio/wav;base64,AQ==' }, { id: 'bad', src: 'data:audio/mpeg;base64,Ag==' }];
  f.ctx.decodeAudioData = async bytes => { decodes++; if (new Uint8Array(bytes)[0] === 2) throw Error('unsupported'); return { duration: 3 }; };
  f.a._loadCommandSounds(); const loading = f.a.commandLoading; f.a._loadCommandSounds(); await loading;
  assert.equal(decodes, 2); assert.equal(f.a.commandBuffers.length, 1); f.a._loadCommandSounds(); assert.equal(decodes, 2);
  const stale = fixture(); stale.c.ATS_COMMAND_SOUNDS = f.c.ATS_COMMAND_SOUNDS.slice(0, 1); let finish;
  stale.ctx.decodeAudioData = () => new Promise(resolve => { finish = resolve; });
  stale.a._loadCommandSounds(); const pending = stale.a.commandLoading; stale.a.dispose(); finish({ duration: 3 }); await pending;
  assert.equal(stale.a.commandBuffers.length, 0);
});

test('commands retain procedural feedback until the sample library is available', () => {
  const f = fixture(); f.a.commandBuffers = []; const heard = []; f.a._roar = (...args) => heard.push(args); f.a._tone = () => {};
  for (const name of ['call', 'charge', 'recall', 'hold', 'patrol', 'command']) f.a.play(name);
  assert.equal(heard.length, 7); // Patrol has two procedural calls.
});

test('selected army commands acknowledge real members, with no calls for rejected orders', () => {
  const c = loadEngine(), g = new c.ATSGame('COMMAND-AUDIO'); const heard = []; g.hooks.sound = (...v) => heard.push(v);
  const a = g.makeApe(40, 0, 'follow'); a.species = 'gibbon'; g.siege.balance(a, true); g.siege.select('gibbon');
  assert.equal(g.siege.immediate('hold', { x: 40, y: 0 }), true); assert.deepEqual(heard.map(v => v[0]), ['command']);
  heard.length = 0; assert.equal(g.siege.issue(['gibbon'], [{ type: 'invalid', x: 0, y: 0 }]), false); assert.equal(heard.length, 0);
  g.siege.select('gorilla'); g.siege.immediate('hold', { x: 40, y: 0 }); assert.equal(heard.length, 0, 'no members receive an order');
});

test('the standalone HTML embeds every original command file byte for byte', () => {
  const html = fs.readFileSync(path.resolve(root, '../apes-together-strong.html'), 'utf8');
  const clips = JSON.parse(html.match(/window\.ATS_COMMAND_SOUNDS=(\[[^\n]+\]);/)[1]);
  const files = JSON.parse(fs.readFileSync(path.join(root, 'assets/command-sounds/manifest.json'), 'utf8'));
  assert.equal(clips.length, 9); assert.equal(files.length, 9);
  files.forEach((file, i) => { assert.equal(clips[i].id, path.parse(file).name); assert.deepEqual(Buffer.from(clips[i].src.split(',')[1], 'base64'), fs.readFileSync(path.join(root, 'assets/command-sounds', file))); });
});
