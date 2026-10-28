(function (root) {
  'use strict';
  const DEFAULTS = Object.freeze({music: .35, effects: .65, muted: false});
  const KEY = 'turncraft.audio.v1';
  const CUES = new Set(['ui','select','order','build','train','research','error','release','impact','death','detonation','teleportOut','teleportIn','heal','link','cast','deploy']);
  function createAudioSystem(env = root, source = 'turncraft/assets/audio/conquest-signal.mp3') {
    let settings = {...DEFAULTS}, music, context, output, noise, unlocked = false, disposed = false;
    let lastMusicAttempt = null;
    const cooldowns = new Map(), voices = [], listeners = new Set();
    try {
      const saved = JSON.parse(env.localStorage?.getItem(KEY) || 'null');
      for (const name of ['music', 'effects']) if (typeof saved?.[name] === 'number' && Number.isFinite(saved[name])) settings[name] = Math.max(0, Math.min(1, saved[name]));
      if (typeof saved?.muted === 'boolean') settings.muted = saved.muted;
    } catch { /* Audio preferences are optional when storage is unavailable. */ }
    const hidden = () => !!env.document?.hidden;
    function publish() { for (const listener of listeners) listener({...settings, unlocked, musicPlaying: !!music && !music.paused, musicUnavailable: !!music?.error}); }
    function ensureMusic() {
      if (music || !env.Audio) return music;
      music = new env.Audio(source);
      music.loop = true;
      music.preload = 'none';
      music.addEventListener?.('error', publish);
      return music;
    }
    function sync() {
      if (disposed) return;
      if (music) music.volume = settings.muted ? 0 : settings.music;
      if (output && context) output.gain.setValueAtTime(settings.muted ? 0 : settings.effects * .42, context.currentTime);
      if (!unlocked || settings.muted || !settings.music || hidden()) {
        music?.pause();
      } else {
        const track = ensureMusic();
        if (track) {
          track.volume = settings.music;
          // Rejection is expected under autoplay restrictions. Another real gesture retries.
          if (track.paused && !lastMusicAttempt) {
            try {
              lastMusicAttempt = Promise.resolve(track.play()).catch(() => {}).finally(() => {lastMusicAttempt = null; publish();});
            } catch {lastMusicAttempt = null;}
          }
        }
      }
      publish();
    }
    function ensureContext() {
      if (context) return context;
      const AudioContext = env.AudioContext || env.webkitAudioContext;
      if (!AudioContext) return null;
      try {
        context = new AudioContext();
        output = context.createGain();
        output.gain.value = settings.muted ? 0 : settings.effects * .42;
        const limiter = context.createDynamicsCompressor();
        limiter.threshold.value = -16; limiter.knee.value = 12; limiter.ratio.value = 6;
        output.connect(limiter); limiter.connect(context.destination);
        noise = context.createBuffer(1, Math.ceil(context.sampleRate * .8), context.sampleRate);
        const samples = noise.getChannelData(0);
        for (let i = 0; i < samples.length; i++) samples[i] = Math.random() * 2 - 1;
      } catch { context = null; }
      return context;
    }
    function unlock() {
      if (disposed) return;
      unlocked = true;
      const ctx = ensureContext();
      if (!hidden() && ctx?.state === 'suspended') Promise.resolve(ctx.resume()).catch(() => {});
      sync();
    }
    function setSettings(update) {
      for (const name of ['music', 'effects']) if (typeof update[name] === 'number' && Number.isFinite(update[name])) settings[name] = Math.max(0, Math.min(1, update[name]));
      if (typeof update.muted === 'boolean') settings.muted = update.muted;
      try { env.localStorage?.setItem(KEY, JSON.stringify(settings)); } catch { /* Keep session controls usable. */ }
      if (settings.muted || !settings.effects) stopVoices();
      sync();
    }
    function stopVoices() {
      for (const voice of voices.splice(0)) for (const node of voice.nodes) {try {node.stop(); node.disconnect();} catch {}}
    }
    function play(kind, {pan = 0, gain = 1, family = '', defense = ''} = {}) {
      if (!CUES.has(kind)) return false;
      if (!unlocked || disposed || settings.muted || !settings.effects || hidden()) return false;
      const ctx = ensureContext();
      if (!ctx || ctx.state !== 'running') return false;
      const t = ctx.currentTime;
      for (let i = voices.length - 1; i >= 0; i--) if (voices[i].end <= t) voices.splice(i, 1);
      const bucket = kind + ':' + family + ':' + defense;
      const interval = ['select', 'order', 'ui'].includes(kind) ? .075 : kind === 'impact' ? .085 : .06;
      if (t - (cooldowns.get(bucket) ?? -Infinity) < interval || voices.length >= 12) return false;
      cooldowns.set(bucket, t);
      const bus = ctx.createGain(); bus.gain.value = Math.max(0, Math.min(1, gain));
      let tail = bus;
      if (ctx.createStereoPanner) {const p = ctx.createStereoPanner(); p.pan.value = Math.max(-1, Math.min(1, pan)); bus.connect(p); tail = p;}
      tail.connect(output);
      const voice = {nodes: [], end: t + .8}; voices.push(voice);
      let nodeCount = 0, maxDuration = .03;
      function tone(frequency, end, duration, type = 'sine', volume = .28, delay = 0) {
        const oscillator = ctx.createOscillator(), envelope = ctx.createGain(), start = t + delay;
        oscillator.type = type;
        oscillator.frequency.setValueAtTime(frequency, start);
        oscillator.frequency.exponentialRampToValueAtTime(Math.max(20, end), start + duration);
        envelope.gain.setValueAtTime(.0001, start);
        envelope.gain.exponentialRampToValueAtTime(Math.max(.0002, volume), start + .005);
        envelope.gain.exponentialRampToValueAtTime(.0001, start + duration);
        oscillator.connect(envelope); envelope.connect(bus);
        oscillator.start(start); oscillator.stop(start + duration + .01);
        voice.nodes.push(oscillator); maxDuration = Math.max(maxDuration, delay + duration + .01); nodeCount++;
        oscillator.onended = () => cleanup(oscillator, envelope);
      }
      function hiss(duration, cutoff, volume = .45, type = 'lowpass') {
        const sample = ctx.createBufferSource(), filter = ctx.createBiquadFilter(), envelope = ctx.createGain();
        sample.buffer = noise; filter.type = type; filter.frequency.value = cutoff;
        envelope.gain.setValueAtTime(volume, t); envelope.gain.exponentialRampToValueAtTime(.0001, t + duration);
        sample.connect(filter); filter.connect(envelope); envelope.connect(bus);
        sample.start(t); sample.stop(t + duration + .01); voice.nodes.push(sample); nodeCount++;
        maxDuration = Math.max(maxDuration, duration + .01);
        sample.onended = () => {filter.disconnect(); cleanup(sample, envelope);};
      }
      function cleanup(sourceNode, envelope) {sourceNode.disconnect(); envelope.disconnect(); if (--nodeCount <= 0) {bus.disconnect(); if (tail !== bus) tail.disconnect();}}
      if (kind === 'release') {
        if (['artillery', 'explosive', 'missile', 'heavy', 'subterranean'].includes(family)) {hiss(.36, 850, .62); tone(110, 32, .38, 'sine', .55);}
        else if (['claw', 'blade', 'spine', 'glaive'].includes(family)) {hiss(.12, 2600, .4, 'bandpass'); tone(260, 95, .1, 'triangle', .22);}
        else if (['acid', 'spores', 'flame'].includes(family)) {hiss(.24, 1500, .38, 'bandpass'); tone(190, 65, .19, 'triangle', .18);}
        else if (family === 'ballistic') {hiss(.105, 2100, .58); tone(140, 45, .09, 'triangle', .28);}
        else {tone(920, 150, .19, 'sawtooth', .15); tone(460, 110, .22, 'sine', .3);}
      } else if (kind === 'impact') {
        if (defense === 'shield') {tone(1200, 350, .16, 'sine', .25); tone(1800, 550, .13, 'triangle', .1);}
        else {hiss(.12, defense === 'armor' ? 3200 : 600, .36); tone(defense === 'armor' ? 620 : 90, 45, .11, 'triangle', .2);}
      } else if (kind === 'death' || kind === 'detonation') {hiss(.48, 680, .58); tone(95, 26, .5, 'sine', .5);}
      else if (kind === 'teleportOut' || kind === 'teleportIn') {tone(kind === 'teleportOut' ? 700 : 120, kind === 'teleportOut' ? 100 : 950, .26, 'sine', .26); hiss(.2, 1600, .12, 'bandpass');}
      else if (kind === 'heal' || kind === 'link') {tone(440, 660, .24, 'sine', .15); tone(660, 880, .2, 'sine', .11, .07);}
      else if (kind === 'cast' || kind === 'deploy') {tone(220, 880, .21, 'triangle', .24); tone(440, 1320, .26, 'sine', .15, .035);}
      else if (kind === 'build' || kind === 'train' || kind === 'research') {tone(380, 440, .08, 'triangle', .22); tone(570, 660, .13, 'sine', .22, .07);}
      else if (kind === 'error') {tone(160, 100, .15, 'triangle', .22);}
      else if (kind === 'select') {tone(660, 880, .065, 'sine', .19);}
      else {tone(440, 540, .07, 'sine', .2); tone(660, 700, .055, 'sine', .12, .035);}
      voice.end = t + maxDuration;
      return true;
    }
    function visibility() {
      if (hidden()) {stopVoices(); if (context?.state === 'running') Promise.resolve(context.suspend()).catch(() => {});}
      else if (unlocked && context?.state === 'suspended') Promise.resolve(context.resume()).catch(() => {});
      sync();
    }
    function dispose() {disposed = true; music?.pause(); stopVoices(); Promise.resolve(context?.close()).catch(() => {}); listeners.clear(); env.document?.removeEventListener?.('visibilitychange', visibility);}
    env.document?.addEventListener?.('visibilitychange', visibility);
    return {unlock, play, setSettings, dispose, get settings() {return {...settings};}, subscribe(fn) {listeners.add(fn); publish(); return () => listeners.delete(fn);}, debug() {return {unlocked, activeVoices: voices.length, music, context};}};
  }
  if (typeof module !== 'undefined' && module.exports) module.exports = {createAudioSystem, DEFAULTS};
  if (!root.document) return;
  const script = root.document.currentScript;
  const audio = createAudioSystem(root, new URL('assets/audio/conquest-signal.mp3', script?.src || root.location.href).href);
  root.TurncraftAudio = audio;
  const gesture = () => audio.unlock();
  root.document.addEventListener('pointerdown', gesture, {capture: true});
  root.document.addEventListener('keydown', event => {if (!event.repeat) gesture();}, {capture: true});
  function mount() {
    const panel = root.document.createElement('details'); panel.id = 'tcAudio';
    panel.innerHTML = '<summary aria-label="Music and sound settings">Sound</summary><div class="tcAudioControls"><strong>Turncraft audio</strong><label>Theme music <input type="range" min="0" max="100" step="1" data-audio="music" aria-label="Theme music volume"><output data-level="music"></output></label><label>Sound effects <input type="range" min="0" max="100" step="1" data-audio="effects" aria-label="Sound effects volume"><output data-level="effects"></output></label><label class="tcAudioMute"><input type="checkbox" data-audio="muted"> Mute all sound</label><small data-audio-status>Interact to start Conquest Signal</small></div>';
    // Keep the audio panel outside clipped command docks and clear of the minimap.
    root.document.body.append(panel);
    function placeControls() {
      const menu = root.document.getElementById('sfMenu'), setup = root.document.getElementById('overlay');
      const inMenu = (menu && !menu.hidden) || (setup && root.getComputedStyle(setup).display !== 'none');
      const compact = root.innerWidth <= 920;
      panel.dataset.menu = String(!!inMenu);
      panel.dataset.compact = String(compact);
      if (compact && !inMenu) {
        const minimap = root.document.getElementById('minimapDock')?.getBoundingClientRect();
        const top = minimap && minimap.width > 0 && minimap.bottom < root.innerHeight - 100 ? Math.max(92, minimap.bottom + 8) : 92;
        panel.style.top = `${Math.round(top)}px`;
      } else panel.style.removeProperty('top');
    }
    if (root.MutationObserver) {
      let watchedMenu;
      const observer = new root.MutationObserver(() => {const menu = root.document.getElementById('sfMenu'); if (menu && menu !== watchedMenu) {watchedMenu = menu; observer.observe(menu, {attributes: true, attributeFilter: ['hidden']});} placeControls();});
      observer.observe(root.document.body, {childList: true});
      const setup = root.document.getElementById('overlay');
      if (setup) observer.observe(setup, {attributes: true, attributeFilter: ['style']});
      const minimap = root.document.getElementById('minimapDock');
      if (minimap) observer.observe(minimap, {attributes: true, attributeFilter: ['class']});
    }
    placeControls();
    root.addEventListener('resize', placeControls);
    panel.addEventListener('toggle', placeControls);
    root.requestAnimationFrame?.(placeControls);
    for (const input of panel.querySelectorAll('[data-audio]')) input.addEventListener('input', () => {const update = {[input.dataset.audio]: input.type === 'checkbox' ? input.checked : Number(input.value) / 100}; audio.setSettings(update); audio.unlock();});
    audio.subscribe(state => {
      for (const name of ['music', 'effects']) {panel.querySelector(`[data-audio="${name}"]`).value = Math.round(state[name] * 100); panel.querySelector(`[data-level="${name}"]`).textContent = `${Math.round(state[name] * 100)}%`;}
      panel.querySelector('[data-audio="muted"]').checked = state.muted;
      panel.querySelector('[data-audio-status]').textContent = state.musicUnavailable ? 'Theme unavailable. Sound effects remain available.' : state.muted ? 'Sound muted' : !state.music ? 'Theme music off' : state.musicPlaying ? 'Conquest Signal · looping' : 'Interact to start Conquest Signal';
    });
    panel.addEventListener('keydown', event => event.stopPropagation());
    root.document.addEventListener('pointerdown', event => {if (panel.open && !panel.contains(event.target)) panel.open = false;});
  }
  if (root.document.readyState === 'loading') root.document.addEventListener('DOMContentLoaded', mount, {once: true}); else mount();
})(typeof window === 'undefined' ? globalThis : window);
