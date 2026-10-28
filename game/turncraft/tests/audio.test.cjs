const {test} = require('node:test');
const assert = require('node:assert/strict');
const {createAudioSystem} = require('../audio.js');

function environment(saved) {
  const events = {}, storage = new Map(saved ? [['turncraft.audio.v1', JSON.stringify(saved)]] : []);
  let plays = 0, blocked = false;
  class Audio {
    paused = true; currentTime = 12; volume = 1;
    constructor(src) {this.src = src;}
    addEventListener() {}
    play() {plays++; if (blocked) return Promise.reject(Error('autoplay')); this.paused = false; return Promise.resolve();}
    pause() {this.paused = true;}
  }
  function node() {const param = () => ({value: 0, setValueAtTime() {}, exponentialRampToValueAtTime() {}}); return {gain: param(), frequency: param(), pan: param(), threshold: param(), knee: param(), ratio: param(), connect() {}, disconnect() {}, start() {}, stop() {}};}
  class AudioContext {
    currentTime = 0; sampleRate = 1000; state = 'running'; destination = {};
    createGain = node; createStereoPanner = node; createDynamicsCompressor = node;
    createOscillator = node; createBufferSource = node; createBiquadFilter = node;
    createBuffer(channels, length) {return {getChannelData: () => new Float32Array(length)};}
    resume() {this.state = 'running';return Promise.resolve();}
    suspend() {this.state = 'suspended';return Promise.resolve();}
    close() {return Promise.resolve();}
  }
  const env = {Audio, AudioContext, document: {hidden: false, addEventListener(name, fn) {events[name] = fn;}, removeEventListener() {}}, localStorage: {getItem: key => storage.get(key), setItem: (key, value) => storage.set(key, value)}};
  return {env, storage, events, get plays() {return plays;}, block(value) {blocked = value;}};
}
const flush = () => new Promise(resolve => setImmediate(resolve));

test('provided theme starts on a gesture at reduced volume and loops without restarting', async () => {
  const f = environment(), audio = createAudioSystem(f.env, '/theme.mp3');
  assert.equal(audio.debug().music, undefined);
  assert.equal(audio.play('release'), false);
  audio.unlock(); await flush();
  assert.equal(f.plays, 1);
  const track = audio.debug().music;
  assert.equal(track.src, '/theme.mp3'); assert.equal(track.loop, true); assert.equal(track.volume, .35);
  audio.unlock(); audio.setSettings({effects: .2}); await flush();
  assert.equal(f.plays, 1); assert.equal(track.currentTime, 12);
});
test('rejected autoplay retries on a later gesture and has no unhandled rejection', async () => {
  const f = environment(); f.block(true); const audio = createAudioSystem(f.env);
  audio.unlock(); await flush(); assert.equal(f.plays, 1); assert.equal(audio.debug().music.paused, true);
  f.block(false); audio.unlock(); await flush(); assert.equal(f.plays, 2); assert.equal(audio.debug().music.paused, false);
});
test('music/effects/mute persist independently, clamp invalid values and work without storage', async () => {
  const f = environment({music: .23, effects: .8, muted: true}), audio = createAudioSystem(f.env);
  audio.unlock(); await flush(); assert.equal(f.plays, 0);
  audio.setSettings({music: 4, effects: -2, muted: false}); await flush();
  assert.deepEqual(audio.settings, {music: 1, effects: 0, muted: false});
  assert.deepEqual(JSON.parse(f.storage.get('turncraft.audio.v1')), audio.settings);
  assert.equal(audio.play('order'), false);
  audio.setSettings({music: NaN, effects: Infinity}); assert.deepEqual(audio.settings, {music: 1, effects: 0, muted: false});
  f.env.localStorage.setItem = () => {throw Error('blocked');}; audio.setSettings({music: .4}); assert.equal(audio.settings.music, .4);
});
test('switching tabs pauses music and all effects; returning resumes the same track position', async () => {
  const f = environment(), audio = createAudioSystem(f.env); audio.unlock(); await flush();
  assert.equal(audio.play('release', {family: 'artillery'}), true);
  f.env.document.hidden = true; f.events.visibilitychange(); assert.equal(audio.debug().music.paused, true);
  assert.equal(audio.debug().activeVoices, 0); assert.equal(audio.play('impact'), false);
  f.env.document.hidden = false; f.events.visibilitychange(); await flush();
  assert.equal(audio.debug().music.paused, false); assert.equal(audio.debug().music.currentTime, 12);
});
test('distinct action/weapon/impact/ability cues are bounded to twelve concurrent voices', () => {
  const f = environment(), audio = createAudioSystem(f.env); audio.unlock();
  assert.equal(audio.play('prepare'), false, 'visual windups do not beep over weapon releases');
  for (const [kind, family] of [['select',''], ['order',''], ['build',''], ['train',''], ['research',''], ['release','ballistic'], ['release','artillery'], ['release','psionic'], ['release','claw'], ['release','acid'], ['impact','shield'], ['cast','']]) assert.equal(audio.play(kind, {family}), true, kind + family);
  assert.equal(audio.debug().activeVoices, 12); assert.equal(audio.play('death'), false);
  audio.debug().context.currentTime = 1; assert.equal(audio.play('death'), true);
  assert.equal(audio.play('death'), false); audio.setSettings({muted: true}); assert.equal(audio.debug().activeVoices, 0);
});
test('no Web Audio support still permits looping theme and mute', async () => {
  const f = environment(); delete f.env.AudioContext; const audio = createAudioSystem(f.env); audio.unlock(); await flush();
  assert.equal(audio.debug().music.paused, false); assert.equal(audio.play('release'), false);
  audio.setSettings({muted: true}); assert.equal(audio.debug().music.paused, true); audio.dispose();
});
