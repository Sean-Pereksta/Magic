/* Apes Together Strong — procedural audio, no downloads or autoplay. */
(function () {
  'use strict';

  class ATSAudio {
    constructor() {
      this.enabled = true;
      this.volume = 0.45;
      this.ctx = null;
      this.master = null;
      this.paused = false;
      this.sources = new Set();
      this.last = Object.create(null);
      this.noise = null;
      this.wind = null;
      this.windGain = null;
      this.windFilter = null;
      this.beatTime = 0;
      this.insectTime = 0;
      this.rotorTime = 0;
      this.stepTime = 0;
      this.natureTime = 0;
      this.step = 0;
      this.threat = 0;
      this.maxVoices = 34;
      this.unlocking = null;
    }

    // Call this from a click, tap, or key event. Construction remains silent.
    async unlock() {
      if (this.unlocking) return this.unlocking;
      this.unlocking = (async () => {
        try {
          if (!this.ctx) {
            const AudioContextClass = window.AudioContext || window.webkitAudioContext;
            if (!AudioContextClass) return false;
            this.ctx = new AudioContextClass();
            this.master = this.ctx.createGain();
            this.master.gain.value = this.enabled && !this.paused ? this.volume : 0;
            this.master.connect(this.ctx.destination);
            const frames = Math.ceil(this.ctx.sampleRate * 2);
            this.noise = this.ctx.createBuffer(1, frames, this.ctx.sampleRate);
            const data = this.noise.getChannelData(0);
            // Pinkish noise keeps the wind and roars soft at low volume.
            let smooth = 0;
            for (let i = 0; i < frames; i++) {
              smooth = 0.72 * smooth + 0.28 * (Math.random() * 2 - 1);
              data[i] = smooth * 1.7;
            }
            this._createWind();
          }
          if (this.ctx.state === 'suspended') await this.ctx.resume();
          return this.ctx.state === 'running';
        } catch (_) {
          return false;
        } finally {
          this.unlocking = null;
        }
      })();
      const result = await this.unlocking;
      this.unlocking = null;
      return result;
    }

    setSettings(settings = {}) {
      if (typeof settings.enabled === 'boolean') this.enabled = settings.enabled;
      if (Number.isFinite(settings.volume)) this.volume = this._clamp(settings.volume, 0, 1);
      this._masterLevel();
    }

    pause(paused = true) {
      this.paused = !!paused;
      this._masterLevel();
      if (this.paused) {
        for (const source of Array.from(this.sources)) {
          try { source.stop(this.ctx.currentTime + 0.03); } catch (_) {}
        }
        this.last = Object.create(null);
      }
    }

    _masterLevel() {
      if (!this.master || !this.ctx) return;
      try {
        this.master.gain.setTargetAtTime(this.enabled && !this.paused ? this.volume : 0,
          this.ctx.currentTime, 0.04);
      } catch (_) {}
    }

    _clamp(value, min, max) { return Math.max(min, Math.min(max, value)); }

    _createWind() {
      const ctx = this.ctx;
      this.wind = ctx.createBufferSource();
      this.wind.buffer = this.noise;
      this.wind.loop = true;
      this.windFilter = ctx.createBiquadFilter();
      this.windFilter.type = 'lowpass';
      this.windFilter.frequency.value = 380;
      this.windGain = ctx.createGain();
      this.windGain.gain.value = 0;
      this.wind.connect(this.windFilter);
      this.windFilter.connect(this.windGain);
      this.windGain.connect(this.master);
      this.wind.start();
    }

    _ready() {
      return this.enabled && !this.paused && this.ctx && this.ctx.state === 'running';
    }

    _voice(source, settings) {
      if (!this._ready() || this.sources.size >= this.maxVoices) {
        try { source.disconnect(); } catch (_) {}
        return;
      }
      const ctx = this.ctx;
      const t = ctx.currentTime + (settings.delay || 0);
      const duration = Math.max(0.025, settings.duration || 0.2);
      const gain = ctx.createGain();
      const nodes = [source, gain];
      let tail = source;
      if (settings.filter) {
        const filter = ctx.createBiquadFilter();
        filter.type = settings.filter;
        filter.frequency.value = settings.cutoff || 1000;
        filter.Q.value = settings.q || 0.7;
        tail.connect(filter);
        tail = filter;
        nodes.push(filter);
        if (settings.endCutoff) filter.frequency.exponentialRampToValueAtTime(settings.endCutoff, t + duration);
      }
      tail.connect(gain);
      if (ctx.createStereoPanner) {
        const panner = ctx.createStereoPanner();
        panner.pan.value = this._clamp(settings.pan || 0, -1, 1);
        gain.connect(panner);
        panner.connect(this.master);
        nodes.push(panner);
      } else gain.connect(this.master);
      const peak = Math.max(0.0001, Math.min(0.8, settings.gain || 0.1));
      const attack = Math.min(duration * 0.3, settings.attack || 0.009);
      gain.gain.setValueAtTime(0.0001, t);
      gain.gain.exponentialRampToValueAtTime(peak, t + attack);
      gain.gain.exponentialRampToValueAtTime(0.0001, t + duration);
      this.sources.add(source);
      source.onended = () => {
        this.sources.delete(source);
        for (const node of nodes) { try { node.disconnect(); } catch (_) {} }
      };
      if (source.buffer) {
        source.loop = true;
        source.start(t, Math.random() * this.noise.duration);
      } else source.start(t);
      source.stop(t + duration + 0.015);
    }

    _tone(frequency, endFrequency, settings = {}) {
      if (!this._ready() || this.sources.size >= this.maxVoices) return;
      const oscillator = this.ctx.createOscillator();
      oscillator.type = settings.wave || 'sine';
      const t = this.ctx.currentTime + (settings.delay || 0);
      oscillator.frequency.setValueAtTime(Math.max(15, frequency), t);
      if (endFrequency) oscillator.frequency.exponentialRampToValueAtTime(Math.max(15, endFrequency), t + (settings.duration || 0.2));
      this._voice(oscillator, settings);
    }

    _noise(settings = {}) {
      if (!this._ready() || this.sources.size >= this.maxVoices) return;
      const source = this.ctx.createBufferSource();
      source.buffer = this.noise;
      this._voice(source, settings);
    }

    _roar(pitch, strength, pan, duration = 0.55) {
      this._tone(pitch, pitch * 0.64, { duration, gain: 0.28 * strength, wave: 'sawtooth', filter: 'lowpass', cutoff: 550, endCutoff: 200, pan, attack: 0.045 });
      this._tone(pitch * 1.49, pitch * 0.88, { duration: duration * 0.85, gain: 0.11 * strength, wave: 'triangle', pan, attack: 0.05 });
      this._noise({ duration, gain: 0.24 * strength, filter: 'bandpass', cutoff: 470, q: 1.4, pan, attack: 0.025 });
    }

    play(name, strength = 1, pan = 0) {
      if (!this._ready()) return;
      strength = Number.isFinite(strength) ? this._clamp(strength, 0.08, 1.4) : 1;
      pan = Number.isFinite(pan) ? this._clamp(pan, -1, 1) : 0;
      const now = this.ctx.currentTime;
      const intervals = {
        gun: 0.055, attack: 0.07, hit: 0.11, death: 0.25, smash: 0.2,
        alarm: 1.7, radio: 0.9, heli: 0.38, rescue: 0.25, birth: 0.3,
        food: 0.18, call: 0.65, charge: 0.6, recall: 0.65,
        hold: 0.5, settle: 0.5, patrol: 0.5, fall: 0.16, warning: 0.45, grenade: 0.3
      };
      if (!Object.prototype.hasOwnProperty.call(intervals, name)) return;
      if (now - (this.last[name] ?? -100) < intervals[name]) return;
      this.last[name] = now;
      const s = strength;
      const at = (duration, gain, extra = {}) => ({ duration, gain: gain * s, pan, ...extra });
      try {
        switch (name) {
          case 'call': this._roar(155, s, pan, 0.72); break;
          case 'charge':
            this._roar(135, s, pan, 0.68);
            this._tone(78, 35, at(0.28, 0.3));
            break;
          case 'recall':
            this._roar(190, s * 0.8, pan, 0.38);
            this._tone(220, 165, at(0.26, 0.15, { delay: 0.25, wave: 'triangle' }));
            break;
          case 'hold': this._roar(104, s * 0.7, pan, 0.32); break;
          case 'settle':
            this._tone(185, 140, at(0.25, 0.14, { wave: 'triangle' }));
            this._tone(140, 105, at(0.35, 0.12, { delay: 0.2, wave: 'triangle' }));
            break;
          case 'patrol':
            this._roar(172, s * 0.65, pan, 0.25);
            this._roar(148, s * 0.42, pan, 0.35);
            break;
          case 'attack':
            this._tone(88 + Math.random() * 40, 35 + Math.random() * 20, at(0.14, 0.28));
            this._noise(at(0.07 + Math.random() * .05, 0.26, { filter: 'lowpass', cutoff: 950 + Math.random() * 650 }));
            break;
          case 'smash':
            this._noise(at(0.3, 0.48, { filter: 'bandpass', cutoff: 1250, endCutoff: 450 }));
            this._tone(95, 27, at(0.3, 0.28));
            this._noise(at(0.09, 0.19, { delay: 0.08, filter: 'highpass', cutoff: 2100 }));
            break;
          case 'rescue':
            this._noise(at(0.16, 0.2, { filter: 'highpass', cutoff: 1700 }));
            [220, 277, 330].forEach((frequency, i) => this._tone(frequency, frequency * 1.02, at(0.45, 0.12, { delay: i * 0.11, wave: 'triangle' })));
            break;
          case 'gun':
            this._noise(at(0.075, 0.7, { filter: 'highpass', cutoff: 1300, attack: 0.002 }));
            this._noise(at(0.24, 0.3, { filter: 'lowpass', cutoff: 1050, attack: 0.002 }));
            this._tone(125, 36, at(0.16, 0.25, { attack: 0.002 }));
            break;
          case 'alarm':
            for (let i = 0; i < 3; i++) this._tone(i % 2 ? 820 : 610, i % 2 ? 610 : 820,
              at(0.49, 0.18, { delay: i * 0.42, wave: 'sawtooth', filter: 'lowpass', cutoff: 1400, attack: 0.02 }));
            break;
          case 'radio':
            this._noise(at(0.42, 0.2, { filter: 'bandpass', cutoff: 1600, q: 1.7 }));
            this._tone(1080, 1080, at(0.1, 0.1, { wave: 'square', filter: 'lowpass', cutoff: 1600 }));
            this._tone(790, 790, at(0.12, 0.07, { delay: 0.27, wave: 'square', filter: 'lowpass', cutoff: 1600 }));
            break;
          case 'hit':
            this._noise(at(0.17, 0.26, { filter: 'lowpass', cutoff: 700 }));
            this._tone(110, 55, at(0.22, 0.19, { wave: 'triangle' }));
            break;
          case 'death': this._roar(115, s * 0.45, pan, 0.52); break;
          case 'fall':
            this._tone(65 + Math.random() * 35, 24, at(.24, .18));
            this._noise(at(.28, .17, { filter: 'lowpass', cutoff: 380 + Math.random() * 400 }));
            break;
          case 'warning':
            this._tone(720, 1250, at(.17, .09, { wave: 'triangle' }));
            this._tone(1040, 1450, at(.12, .065, { delay: .18, wave: 'triangle' }));
            break;
          case 'grenade':
            this._noise(at(.12, .15, { filter: 'highpass', cutoff: 2300 }));
            this._tone(330, 170, at(.13, .1, { delay: .08, wave: 'triangle' }));
            break;
          case 'birth':
            this._tone(360, 490, at(0.27, 0.12, { wave: 'triangle' }));
            this._tone(480, 370, at(0.3, 0.08, { delay: 0.14, wave: 'triangle' }));
            break;
          case 'food':
            this._tone(270, 195, at(0.1, 0.1, { wave: 'triangle' }));
            this._noise(at(0.08, 0.1, { filter: 'lowpass', cutoff: 800 }));
            break;
          case 'heli':
            for (let i = 0; i < 4; i++) {
              this._noise(at(0.075, 0.18, { delay: i * 0.075, filter: 'lowpass', cutoff: 350, attack: 0.006 }));
              this._tone(43, 35, at(0.085, 0.06, { delay: i * 0.075 }));
            }
            break;
        }
      } catch (_) { /* Audio must never interrupt gameplay. */ }
    }

    update(game, dt) {
      if (!this._ready()) return;
      try {
        const now = this.ctx.currentTime;
        if (!game) {
          this.windGain.gain.setTargetAtTime(0, now, 0.8);
          return;
        }
        dt = Number.isFinite(dt) ? this._clamp(dt, 0, 0.25) : 0.016;
        let danger = 0;
        for (const field of ['threat', 'threatLevel', 'heat', 'alertLevel', 'wanted']) {
          const value = game[field];
          if (Number.isFinite(value)) danger = Math.max(danger, value > 1 ? value / 100 : value);
        }
        if (game.inCombat || game.detected || game.hunted) danger = Math.max(danger, 0.7);
        const humans = game.humans || game.enemies;
        if (Array.isArray(humans)) {
          // Sampling bounds the audio work even when a horde fills the map.
          for (let i = 0; i < Math.min(24, humans.length); i++) {
            const human = humans[i];
            if (human && (human.alerted || human.state === 'attack' || human.state === 'chase' || human.state === 'combat')) danger = Math.max(danger, 0.65);
          }
        }
        danger = this._clamp(danger, 0, 1);
        this.threat += (danger - this.threat) * Math.min(1, dt * 2);
        const terrain = game.world?.terrain(game.king?.x || 0, game.king?.y || 0);
        const windLevel = (terrain?.biome === 'rocky' ? .11 : .065) + Math.sin(now * 0.19) * 0.014;
        this.windGain.gain.setTargetAtTime(windLevel, now, 0.7);
        this.windFilter.frequency.setTargetAtTime(300 + Math.sin(now * 0.12) * 80 + this.threat * 130, now, 1);

        if (game.king?.moving && now >= this.stepTime) {
          this.stepTime = now + .31 + Math.random() * .045;
          const wet = terrain?.biome === 'wetland';
          this._noise({ duration: wet ? .17 : .09, gain: .055, filter: wet ? 'bandpass' : 'lowpass', cutoff: wet ? 1350 : terrain?.road ? 780 : 450 });
          this._tone(50 + Math.random() * 20, 28, { duration: .08, gain: .04 });
        }
        if (terrain?.river && now >= this.natureTime) {
          this.natureTime = now + 1.8;
          this._noise({ duration: 2, gain: .026, attack: .3, filter: 'bandpass', cutoff: 1450, q: .45, pan: -.3 });
        }

        if (this.threat > 0.12 && now >= this.beatTime) {
          const beat = this.step++;
          this.beatTime = now + (0.7 - this.threat * 0.24);
          this._tone(beat % 4 === 0 ? 82 : 67, 29, { duration: 0.24, gain: 0.04 + this.threat * 0.11 });
          if (beat % 2 === 1) this._noise({ duration: 0.12, gain: 0.025 + this.threat * 0.015, filter: 'bandpass', cutoff: 740 });
        }
        if (this.threat < 0.55 && now >= this.insectTime) {
          this.insectTime = now + 3.5 + Math.random() * 4;
          const pitch = 2850 + Math.random() * 1200;
          const pan = Math.random() * 1.6 - 0.8;
          this._tone(pitch, pitch * 0.98, { duration: 0.07, gain: 0.015, wave: 'sine', pan });
          this._tone(pitch, pitch * 0.98, { duration: 0.07, gain: 0.013, wave: 'sine', pan, delay: 0.1 });
        }
        if ((game.helicopterActive || game.heliActive) && now >= this.rotorTime) {
          this.rotorTime = now + 0.55;
          this.play('heli', 0.35);
        }
      } catch (_) { /* Stale game objects and unavailable audio are harmless. */ }
    }

    // Optional teardown for restarting or removing the game from a page.
    dispose() {
      this.paused = true;
      for (const source of this.sources) { try { source.stop(); } catch (_) {} }
      this.sources.clear();
      if (this.wind) { try { this.wind.stop(); this.wind.disconnect(); } catch (_) {} }
      if (this.ctx) { try { const closing = this.ctx.close(); if (closing && closing.catch) closing.catch(() => {}); } catch (_) {} }
      this.ctx = null;
      this.master = null;
      this.wind = null;
      this.noise = null;
    }
  }

  window.ATSAudio = ATSAudio;
})();
