/* Procedural effects and the user-provided, embedded Primal Roar soundtrack. */
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
      this.engineTime = 0;
      this.warTime = 0;
      this.warCursor = 0;
      this.engineCursor = 0;
      this.stepTime = 0;
      this.natureTime = 0;
      this.step = 0;
      this.threat = 0;
      this.maxVoices = 34;
      this.unlocking = null;
      this.music = window.document?.getElementById?.('campaignMusic') || null;
      this.musicEnabled = true;
      this.musicVolume = 0.22;
      this.musicPending = false;
      this.musicArmed = false;
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
      this._syncMusic();
      return result;
    }

    setSettings(settings = {}) {
      if (typeof settings.enabled === 'boolean') this.enabled = settings.enabled;
      if (Number.isFinite(settings.volume)) this.volume = this._clamp(settings.volume, 0, 1);
      if (typeof settings.musicEnabled === 'boolean') this.musicEnabled = settings.musicEnabled;
      if (Number.isFinite(settings.musicVolume)) this.musicVolume = this._clamp(settings.musicVolume, 0, 1);
      this._masterLevel();
      this._syncMusic();
    }

    pause(paused = true) {
      this.paused = !!paused;
      if (!this.paused) this.musicArmed = true;
      this._masterLevel();
      this._syncMusic();
      if (this.paused) {
        for (const source of Array.from(this.sources)) {
          try { source.stop(this.ctx.currentTime + 0.03); } catch (_) {}
        }
        this.last = Object.create(null);
      }
    }

    _musicReady() {
      return this.musicArmed && this.enabled && this.musicEnabled && !this.paused && this.volume > 0 &&
        this.musicVolume > 0 && !window.document?.hidden && this.ctx?.state === 'running';
    }

    _syncMusic() {
      const music = this.music;
      if (!music) return;
      music.volume = this.volume * this.musicVolume;
      music.loop = true;
      if (!this._musicReady()) { music.pause(); return; }
      if (!music.paused || this.musicPending) return;
      this.musicPending = true;
      try {
        Promise.resolve(music.play()).then(() => {
          if (!this._musicReady()) music.pause();
        }).catch(() => { /* A later player gesture retries a browser-blocked start. */ })
          .finally(() => { this.musicPending = false; });
      } catch (_) { this.musicPending = false; }
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

    _voiceLimit() { return this._voiceBudget ?? Math.max(1, this.maxVoices - 8); }

    _voice(source, settings) {
      if (!this._ready() || this.sources.size >= this._voiceLimit()) {
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
      if (!this._ready() || this.sources.size >= this._voiceLimit()) return;
      const oscillator = this.ctx.createOscillator();
      oscillator.type = settings.wave || 'sine';
      const t = this.ctx.currentTime + (settings.delay || 0);
      oscillator.frequency.setValueAtTime(Math.max(15, frequency), t);
      if (endFrequency) oscillator.frequency.exponentialRampToValueAtTime(Math.max(15, endFrequency), t + (settings.duration || 0.2));
      this._voice(oscillator, settings);
    }

    _noise(settings = {}) {
      if (!this._ready() || this.sources.size >= this._voiceLimit()) return;
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
        hold: 0.5, settle: 0.5, patrol: 0.5, fall: 0.16, warning: 0.45, grenade: 0.3,
        cannon: 0.35, tank: 0.65, apc: 0.8, truck: 0.85, military: 2.4, mobilization: 2.4,
        rumble: 0.65, recon: 0.4, scout: 0.4, gunship: 0.4, overrun: 0.8,
        mortar: 0.45, mortarImpact: 0.45, distantGun: 1.15,
        converge: 3.5, offensive: 4, sirens: 4
      };
      if (!Object.prototype.hasOwnProperty.call(intervals, name)) return;
      if (now - (this.last[name] ?? -100) < intervals[name]) return;
      this.last[name] = now;
      // Gunfire and ambience leave voices for danger tells and commands, so a
      // mass battle cannot drown out a mortar launch or the King's recall.
      const previousBudget = this._voiceBudget;
      this._voiceBudget = ['warning', 'mortar', 'mortarImpact', 'cannon', 'offensive', 'call', 'charge', 'recall', 'hit', 'death'].includes(name) ? this.maxVoices : Math.max(1, this.maxVoices - 8);
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
          case 'mortar':
            // A low launch thump and rising whistle precede the warning circle.
            this._tone(118, 38, at(.24, .23, { attack: .004 }));
            this._noise(at(.28, .2, { filter: 'lowpass', cutoff: 680, endCutoff: 180 }));
            this._tone(510, 1450, at(.65, .055, { delay: .16, wave: 'triangle', attack: .08 }));
            break;
          case 'mortarImpact':
            this._noise(at(.16, .56, { filter: 'lowpass', cutoff: 1350, attack: .002 }));
            this._tone(76, 20, at(.72, .34, { attack: .003 }));
            this._noise(at(.95, .24, { delay: .05, filter: 'lowpass', cutoff: 420, endCutoff: 80, attack: .03 }));
            break;
          case 'distantGun':
            for (let i = 0; i < 4; i++) {
              this._noise(at(.09, .15, { delay: i * .09, filter: 'lowpass', cutoff: 540, attack: .002 }));
              this._tone(83, 28, at(.12, .055, { delay: i * .09, attack: .003 }));
            }
            break;
          case 'converge':
          case 'offensive':
            this._tone(47, 30, at(1.15, .15, { wave: 'sawtooth', filter: 'lowpass', cutoff: 180, attack: .12 }));
            this._noise(at(.52, .14, { delay: .12, filter: 'bandpass', cutoff: 1400, q: 1.8 }));
            for (let i = 0; i < (name === 'offensive' ? 3 : 2); i++) this._tone(760 - i * 110, 530, at(.16, .07, { delay: .14 + i * .2, wave: 'square', filter: 'lowpass', cutoff: 1100 }));
            break;
          case 'sirens':
            for (let i = 0; i < 3; i++) this._tone(i % 2 ? 580 : 420, i % 2 ? 420 : 580, at(.72, .075, { delay: i * .6, wave: 'sawtooth', filter: 'lowpass', cutoff: 780, attack: .06 }));
            break;
          case 'cannon':
            this._noise(at(.11, .72, { filter: 'lowpass', cutoff: 1700, endCutoff: 420, attack: .002 }));
            this._tone(92, 24, at(.7, .44, { attack: .003 }));
            this._noise(at(1.05, .32, { filter: 'lowpass', cutoff: 480, endCutoff: 90, delay: .04, attack: .03 }));
            this._noise(at(.22, .15, { filter: 'highpass', cutoff: 2100, delay: .09 }));
            break;
          case 'tank':
            this._tone(48, 37, at(.76, .18, { wave: 'sawtooth', filter: 'lowpass', cutoff: 150, attack: .09 }));
            this._noise(at(.65, .16, { filter: 'bandpass', cutoff: 220, q: .7, attack: .06 }));
            for (let i = 0; i < 3; i++) this._noise(at(.075, .075, { delay: i * .17, filter: 'bandpass', cutoff: 940, q: 1.4 }));
            break;
          case 'apc':
            this._tone(67, 49, at(.75, .14, { wave: 'sawtooth', filter: 'lowpass', cutoff: 260, attack: .07 }));
            this._noise(at(.54, .1, { filter: 'bandpass', cutoff: 390, q: .7, attack: .05 }));
            break;
          case 'truck':
            this._tone(78, 55, at(.8, .12, { wave: 'sawtooth', filter: 'lowpass', cutoff: 310, attack: .1 }));
            this._noise(at(.65, .075, { filter: 'lowpass', cutoff: 590, attack: .08 }));
            break;
          case 'rumble':
            this._tone(44, 22, at(.95, .18, { attack: .06 }));
            this._noise(at(.8, .17, { filter: 'lowpass', cutoff: 330, endCutoff: 90, attack: .04 }));
            break;
          case 'military':
          case 'mobilization':
            // Escalation adds a low command pulse and radio acknowledgement.
            // Strength encodes the response stage without extra audio assets.
            for (let i = 0; i < (s > 1 ? 3 : 2); i++) {
              this._tone(82 - i * 9, 37, at(.4, .21, { delay: i * .25, attack: .016 }));
              this._noise(at(.12, .065, { delay: i * .25, filter: 'bandpass', cutoff: 920 }));
            }
            this._noise(at(.28, .12, { delay: .68, filter: 'bandpass', cutoff: 1650, q: 2 }));
            this._tone(s > 1 ? 680 : 890, 540, at(.19, .09, { delay: .73, wave: 'triangle' }));
            break;
          case 'overrun':
            this._noise(at(.25, .28, { filter: 'bandpass', cutoff: 1850, endCutoff: 450 }));
            this._tone(105, 36, at(.42, .22));
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
          case 'recon':
          case 'scout':
          case 'gunship':
            for (let i = 0; i < 4; i++) {
              const heavy = name === 'gunship', interval = heavy ? .095 : .075;
              this._noise(at(heavy ? .105 : .075, heavy ? .24 : .18, { delay: i * interval, filter: 'lowpass', cutoff: name === 'recon' ? 480 : heavy ? 280 : 350, attack: .006 }));
              this._tone(heavy ? 34 : 43, heavy ? 26 : 35, at(.095, heavy ? .095 : .06, { delay: i * interval }));
            }
            break;
        }
      } catch (_) { /* Audio must never interrupt gameplay. */ }
      finally { this._voiceBudget = previousBudget; }
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
        const king = game.king;
        // Sample bounded actor lists and sound only the nearest engine/rotor.
        // A distant armored column never consumes the combat voice budget.
        if (king && now >= this.engineTime) {
          this.engineTime = now + .78;
          let nearest = null, distance = 1450 * 1450;
          const vehicles = game.vehicles || [];
          for (let i = 0; i < Math.min(24, vehicles.length); i++) {
            const vehicle = vehicles[(this.engineCursor + i) % vehicles.length];
            if (!vehicle || vehicle.hp <= 0 || vehicle.engineDamage >= 100) continue;
            const kind = vehicle.vehicleClass || vehicle.kind;
            if (!['tank', 'apc', 'ifv', 'truck'].includes(kind)) continue;
            const d = (vehicle.x - king.x) ** 2 + (vehicle.y - king.y) ** 2;
            if (d < distance) { distance = d; nearest = vehicle; }
          }
          if (vehicles.length) this.engineCursor = (this.engineCursor + 24) % vehicles.length;
          if (nearest) {
            const kind = nearest.vehicleClass || nearest.kind;
            this.play(kind === 'ifv' ? 'apc' : kind, .1 + (1 - Math.sqrt(distance) / 1450) * .55,
              this._clamp((nearest.x - king.x - nearest.y + king.y) / 1450, -.85, .85));
          }
        }
        if (king && now >= this.rotorTime) {
          this.rotorTime = now + .55;
          let nearest = null, distance = 1000 * 1000;
          const helicopters = game.helis || [];
          for (let i = 0; i < Math.min(12, helicopters.length); i++) {
            const heli = helicopters[i];
            if (!heli || heli.hp <= 0) continue;
            const d = (heli.x - king.x) ** 2 + (heli.y - king.y) ** 2;
            if (d < distance) { distance = d; nearest = heli; }
          }
          if (nearest) this.play(['recon', 'scout', 'gunship'].includes(nearest.kind) ? nearest.kind : 'heli',
            .18 + (1 - Math.sqrt(distance) / 1000) * .45, this._clamp((nearest.x - king.x - nearest.y + king.y) / 950, -.8, .8));
          else if (game.helicopterActive || game.heliActive) this.play('heli', .35);
        }
        if (king && now >= this.warTime) {
          this.warTime = now + 2.2 + Math.random() * .7;
          const troops = game.humans || [], intensity = game.warIntensity || 0;
          let gunner = null, radio = null, gunDistance = 1900 * 1900, radioDistance = 1900 * 1900;
          // Rotate through a fixed-size sample; new reinforcement squads get
          // heard without inspecting hundreds of actors on each audio tick.
          for (let i = 0; i < Math.min(32, troops.length); i++) {
            const h = troops[(this.warCursor + i) % troops.length];
            if (!h || h.hp <= 0) continue;
            const d = (h.x - king.x) ** 2 + (h.y - king.y) ** 2;
            if (d > 420 ** 2 && d < gunDistance && h.state === 'combat') { gunner = h; gunDistance = d; }
            if (d < radioDistance && h.reported && h.hasRadio && !game.world?.sites?.get(h.siteId)?.radioDown && ['combat', 'search', 'radio'].includes(h.state)) { radio = h; radioDistance = d; }
          }
          if (troops.length) this.warCursor = (this.warCursor + 32) % troops.length;
          if (gunner) this.play('distantGun', .14 + (1 - Math.sqrt(gunDistance) / 1900) * .32, this._clamp((gunner.x - king.x - gunner.y + king.y) / 1900, -.8, .8));
          if (radio && intensity > 0) this.play('converge', .15 + intensity * .04, this._clamp((radio.x - king.x - radio.y + king.y) / 1900, -.7, .7));
          let checked = 0;
          for (const site of game.world?.sites?.values?.() || []) {
            if (checked++ >= 24) break;
            const d = (site.x - king.x) ** 2 + (site.y - king.y) ** 2;
            if (site.alarm && d > 350 ** 2 && d < 2200 ** 2) { this.play('sirens', .12 + (1 - Math.sqrt(d) / 2200) * .18, this._clamp((site.x - king.x - site.y + king.y) / 2200, -.8, .8)); break; }
          }
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
