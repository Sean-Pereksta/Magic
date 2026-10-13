export const MILESTONES = [5, 10, 15, 20, 30, 50, 75, 100];
export const MODIFIERS = [
  {
    id: 'haste',
    name: 'Rapid reinforcements',
    description: 'Enemy movement cooldowns are reduced.'
  },
  {
    id: 'armor',
    name: 'Hardened armor',
    description: 'Wave troops gain additional health.'
  },
  {
    id: 'flanks',
    name: 'Open flank',
    description: 'An additional approach route opens.'
  },
  {
    id: 'siege',
    name: 'Heavy siege',
    description: 'Every army brings siege support.'
  },
  {
    id: 'disruption',
    name: 'Resource disruption',
    description: 'A supply disruption removes a small resource reserve.'
  },
  {
    id: 'hazard',
    name: 'Unstable ground',
    description: 'Telegraphed Fracture eruptions damage exposed units.'
  },
  {
    id: 'dual-army',
    name: 'Twin armies',
    description: 'Coordinated attacks arrive from opposing directions.'
  },
  {
    id: 'elite',
    name: 'Veteran command',
    description: 'Elite formations gain attack speed and shields.'
  }
];
export const ROSTERS = {
  frontier: {
    scout: ['reaper', 'rifleman'],
    swarm: ['rifleman', 'medic'],
    flank: ['reaper', 'hellion'],
    armor: ['marauder', 'goliath'],
    siege: ['siegeTank', 'rifleman'],
    elite: ['battlecruiser', 'goliath'],
    antiAir: ['goliath', 'rifleman']
  },
  ascendant: {
    scout: ['zealot', 'sentinel'],
    swarm: ['zealot', 'dragoon'],
    flank: ['sentinel', 'phoenix'],
    armor: ['immortal', 'dragoon'],
    siege: ['disruptor', 'dragoon'],
    elite: ['carrier', 'immortal'],
    antiAir: ['dragoon', 'phoenix']
  },
  brood: {
    scout: ['zergling', 'roach'],
    swarm: ['zergling', 'hydralisk'],
    flank: ['mutalisk', 'zergling'],
    armor: ['roach', 'ravager'],
    siege: ['lurker', 'ravager'],
    elite: ['ultralisk', 'defiler'],
    antiAir: ['hydralisk', 'mutalisk']
  }
};
export function seededRandom(seed) {
  let value = seed >>> 0;
  return () => {
    value = (Math.imul(value, 1664525) + 1013904223) >>> 0;
    return value / 4294967296;
  };
}
const strategies = [
  'scout',
  'swarm',
  'flank',
  'armor',
  'siege',
  'ambush',
  'coordinated',
  'elite'
];
export function modifiersFor(wave) {
  const count = Math.floor(Math.max(0, wave) / 5);
  return Array.from({ length: Math.min(count, MODIFIERS.length) }, (_, i) => ({
    ...MODIFIERS[i],
    level: 1 + Math.floor((count - 1 - i) / MODIFIERS.length)
  }));
}
export function buildWave({
  wave = 1,
  seed = 1,
  regionIndex = 0,
  difficulty = 'medium',
  enemyFaction = 'brood',
  strategy,
  waves = null,
  army = {}
} = {}) {
  wave = Math.max(1, Math.floor(wave));
  const random = seededRandom((seed ^ Math.imul(wave, 2654435761)) >>> 0),
    roster = ROSTERS[enemyFaction] || ROSTERS.brood;
  const elite = wave % 10 === 0 || (!!waves && wave === waves),
    modifiers = modifiersFor(wave),
    ids = new Set(modifiers.map((m) => m.id));
  const purpose = elite
    ? regionIndex < 2 && wave < 10
      ? 'armor'
      : 'elite'
    : strategy === 'finale'
      ? wave > 10
        ? 'coordinated'
        : wave > 5
          ? 'siege'
          : 'armor'
      : strategy && wave % 4 === 0
        ? strategy
        : strategies[(wave - 1 + Math.floor(random() * 3)) % strategies.length];
  const difficultyScale =
    { easy: 0.72, medium: 1, hard: 1.22, pro: 1.45 }[difficulty] || 1;
  const budget = Math.min(
      72,
      Math.round((3 + wave * 1.35 + regionIndex * 1.7) * difficultyScale)
    ),
    composition = [];
  const pool = roster[purpose] || roster.flank;
  for (let n = 0; n < budget; n++)
    composition.push(pool[Math.floor(random() * pool.length)]);
  // Counter what the player fields, using the actual faction roster.
  if ((army.flying || 0) > Math.max(2, (army.total || 0) * 0.22))
    for (let n = 0; n < Math.ceil(budget * 0.3); n++)
      composition[n] = roster.antiAir[n % roster.antiAir.length];
  if ((army.structures || 0) > 7 || ids.has('siege'))
    composition.splice(
      0,
      Math.max(1, Math.floor(budget / 7)),
      ...Array.from(
        { length: Math.max(1, Math.floor(budget / 7)) },
        (_, i) => roster.siege[i % roster.siege.length]
      )
    );
  const directions =
    purpose === 'flank' || purpose === 'ambush'
      ? 2
      : purpose === 'coordinated' || ids.has('dual-army')
        ? 3
        : 1;
  const approaches = [0, 1, 2, 3, 4];
  for (let n = approaches.length - 1; n > 0; n--) {
    const i = Math.floor(random() * (n + 1));
    [approaches[n], approaches[i]] = [approaches[i], approaches[n]];
  }
  const routes = approaches.slice(
    0,
    Math.min(5, directions + (ids.has('flanks') ? 1 : 0))
  );
  return {
    number: wave,
    purpose,
    label: elite
      ? 'Elite command assault'
      : `${purpose[0].toUpperCase() + purpose.slice(1)} assault`,
    composition,
    routes,
    elite,
    modifiers,
    healthScale: Math.min(
      6,
      1 + Math.max(0, wave - 12) * 0.025 + (ids.has('armor') ? 0.22 : 0)
    ),
    moveScale: ids.has('haste') ? 0.83 : 1,
    attackScale: ids.has('elite') ? 0.87 : 1,
    spawnInterval: Math.max(0.25, 0.9 - wave * 0.008),
    target:
      purpose === 'flank' || purpose === 'ambush'
        ? 'economy'
        : purpose === 'siege'
          ? 'production'
          : 'objective'
  };
}

export class WaveDirector {
  constructor({
    waves = null,
    preparationSeconds = 50,
    betweenWavesSeconds = 40,
    seed = 1,
    regionIndex = 0,
    difficulty = 'medium',
    strategy = 'scout',
    enemyFaction = 'brood'
  } = {}) {
    this.config = {
      waves,
      preparationSeconds,
      betweenWavesSeconds,
      seed: seed >>> 0,
      regionIndex,
      difficulty,
      strategy,
      enemyFaction
    };
    this.state = {
      version: 1,
      phase: 'preparation',
      wave: 0,
      remaining: preparationSeconds,
      elapsed: 0,
      cleared: 0,
      enemiesDefeated: 0,
      pending: [],
      activeIds: [],
      spawnClock: 0,
      modifiers: [],
      milestones: [],
      wavePlan: null
    };
  }
  nextWave(army = {}) {
    const s = this.state;
    if (s.phase !== 'preparation') return null;
    s.wave++;
    s.phase = 'assault';
    s.remaining = 0;
    s.wavePlan = buildWave({ ...this.config, wave: s.wave, army });
    s.pending = [...s.wavePlan.composition];
    s.activeIds = [];
    s.spawnClock = 0;
    s.modifiers = s.wavePlan.modifiers;
    return s.wavePlan;
  }
  tick(dt, { alive = (id) => false, spawn = () => null, army = {} } = {}) {
    const s = this.state,
      events = [];
    if (['victory', 'defeat'].includes(s.phase)) return events;
    dt = Math.max(0, Math.min(1, Number(dt) || 0));
    s.elapsed += dt;
    if (s.phase === 'preparation') {
      s.remaining = Math.max(0, s.remaining - dt);
      if (s.remaining === 0) {
        const plan = this.nextWave(army);
        events.push({ type: 'wave', plan });
      }
    }
    if (s.phase === 'assault') {
      const still = s.activeIds.filter(alive);
      s.enemiesDefeated += s.activeIds.length - still.length;
      s.activeIds = still;
      s.spawnClock -= dt;
      // Keep pathfinding bounded; delayed troops remain in a reproducible reinforcement queue.
      if (s.pending.length && s.activeIds.length < 54 && s.spawnClock <= 0) {
        const id = spawn(
          s.pending[0],
          s.wavePlan,
          s.wavePlan.composition.length - s.pending.length
        );
        if (id != null) {
          s.pending.shift();
          s.activeIds.push(id);
          s.spawnClock = s.wavePlan.spawnInterval;
        } else s.spawnClock = 0.35;
      }
      if (!s.pending.length && !s.activeIds.length) {
        s.cleared = s.wave;
        if (
          MILESTONES.includes(s.wave) ||
          (s.wave > 100 && s.wave % 50 === 0)
        ) {
          s.milestones.push(s.wave);
          events.push({ type: 'milestone', wave: s.wave });
        }
        events.push({ type: 'cleared', wave: s.wave });
        if (this.config.waves && s.wave >= this.config.waves) {
          s.phase = 'objectives';
          events.push({ type: 'waves-complete' });
        } else {
          s.phase = 'preparation';
          s.remaining = this.config.betweenWavesSeconds;
        }
      }
    }
    return events;
  }
  finish(won) {
    this.state.phase = won ? 'victory' : 'defeat';
  }
  serialize() {
    return JSON.parse(
      JSON.stringify({ version: 1, config: this.config, state: this.state })
    );
  }
  static restore(snapshot) {
    if (
      snapshot?.version !== 1 ||
      snapshot.state?.version !== 1 ||
      !Array.isArray(snapshot.state.pending) ||
      !Array.isArray(snapshot.state.activeIds) ||
      !Number.isFinite(snapshot.state.elapsed) ||
      snapshot.state.wave < 0
    )
      throw new Error('Unsupported or invalid wave checkpoint.');
    const d = new WaveDirector(snapshot.config);
    d.state = JSON.parse(JSON.stringify(snapshot.state));
    return d;
  }
}
