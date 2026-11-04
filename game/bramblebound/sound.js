/* Original little mallet and chime cues, synthesized on the player's device. */
(() => {
  'use strict';
  let enabled = false, context, master;
  const voices = new Set();
  const last = new Map();
  const tone = {
    leaf:   ['triangle', 438, .82, .10],
    fire:   ['triangle', 308, .41, .13],
    frost:  ['sine',     916, 1.50,.14],
    storm:  ['square',   533, .31, .07],
    stone:  ['sine',     201, .60, .12],
    spirit: ['sine',     648, 1.25,.20]
  };
  // [onset, pitch in Hz, length, end/start pitch ratio]. Motifs use an open pentatonic tuning.
  const motifs = {
    enemy:     [[0,148,.10,.64],[.035,98,.13,.76]],
    stagger:   [[0,392,.075,.60],[.058,311,.12,.65]],
    crit:      [[0,659,.10,.82],[.042,988,.13,1]],
    kill:      [[0,294,.09,.86],[.055,392,.12,.95]],
    loot:      [[0,392,.13,1],[.075,494,.14,1],[.15,659,.26,1]],
    dead:      [[0,392,.20,.98],[.135,330,.21,.98],[.28,247,.26,.95],[.46,165,.38,.94]],
    ability:   [[0,392,.09,1.18],[.055,659,.16,.96]],
    level:     [[0,392,.12,1],[.07,494,.13,1],[.14,659,.15,1],[.21,784,.23,1]],
    milestone: [[0,784,.08,1.02],[.055,1175,.14,1]],
    combine:   [[0,330,.15,1],[.055,494,.17,1],[.12,659,.19,1],[.185,988,.23,1]],
    equip:     [[0,330,.07,.91],[.046,494,.11,.92]],
    start:     [[0,294,.13,1],[.105,392,.16,1],[.22,494,.26,1]]
  };
  function setEnabled(value, unlock = false) {
    enabled = !!value;
    try {
      if (enabled && unlock) {
        const Api = window.AudioContext || window.webkitAudioContext;
        if (!Api) return;
        if (!context || context.state === 'closed') {
          context = new Api(); master = context.createGain(); master.gain.value = .26;
          master.connect(context.destination);
        }
        if (context.state !== 'running') context.resume()?.catch(() => {});
      }
      if (master) master.gain.setTargetAtTime(enabled ? .26 : 0, context.currentTime, .008);
      if (!enabled) {
        for (const voice of voices) { try { voice.stop(); } catch (_) {} }
        last.clear();
      }
    } catch (_) { /* The game remains playable when a device withholds audio. */ }
  }
  function note(at, frequency, duration, slide, wave, volume) {
    if (voices.size >= 12) return;
    const osc = context.createOscillator(), gain = context.createGain();
    osc.type = wave;
    osc.frequency.setValueAtTime(frequency, at);
    osc.frequency.exponentialRampToValueAtTime(frequency * slide, at + duration);
    gain.gain.setValueAtTime(.0001, at);
    gain.gain.exponentialRampToValueAtTime(volume, at + .006);
    gain.gain.exponentialRampToValueAtTime(.0001, at + duration);
    osc.connect(gain); gain.connect(master); voices.add(osc);
    osc.onended = () => { voices.delete(osc); osc.disconnect(); gain.disconnect(); };
    osc.start(at); osc.stop(at + duration + .01);
  }
  function play(kind, element = 'leaf') {
    if (!enabled || !context || context.state !== 'running') return;
    if (kind !== 'attack' && !Object.hasOwn(motifs,kind)) return;
    const now = context.currentTime;
    const gap = kind === 'attack' ? .09 : kind === 'milestone' ? .25 : .065;
    if (now - (last.get(kind) ?? -Infinity) < gap) return;
    last.set(kind, now);
    if (kind === 'attack') {
      if (voices.size >= 7) return;
      const [wave, hz, slide, length] = Object.hasOwn(tone,element) ? tone[element] : tone.leaf;
      note(now,hz,length,slide,wave,wave === 'square' ? .13 : .37);
      // A very short overtone gives the rounded pitched note a wooden attack.
      note(now,hz*2.72,.037,.77,'sine',.12);
    } else {
      const wave = ['dead','milestone','loot'].includes(kind) ? 'sine' : 'triangle';
      for (const [when,hz,length,slide] of motifs[kind]) note(now+when,hz,length,slide,wave,.34);
    }
  }
  window.TrailSound = {setEnabled, play};
})();
