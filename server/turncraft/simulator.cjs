'use strict';
const fs = require('node:fs'),
  path = require('node:path'),
  vm = require('node:vm');
const { gzipSync, gunzipSync } = require('node:zlib');
const D = require('./domain.cjs');
const GAME = path.resolve(__dirname, '../../game/turncraft.html'),
  MAP = path.resolve(__dirname, '../../game/turncraft/maps.js');
let compiled = null;
const noop = () => {};
function program() {
  if (compiled) return compiled;
  const html = fs.readFileSync(GAME, 'utf8'),
    module = html.match(/<script type="module">([\s\S]*?)<\/script>/);
  D.fail(
    module && module[1].includes('\nboot();'),
    'ENGINE_UNAVAILABLE',
    'The authoritative TurnCraft engine could not load.',
    503
  );
  let source = module[1].replace(/^import .*;$/gm, '');
  source = source.replace(
    '\nboot();',
    `\nrefreshUI=()=>{};showCommandMarker=()=>{};pushBattleAlert=()=>{};toast=()=>{};showMatchResult=()=>{};updateMapPresentation=()=>{};focusOwnBase=()=>{};
nowMs=()=>globalThis.authorityClock;
globalThis.authorityEvaluate=source=>eval(source);`
  );
  compiled = {
    map: new vm.Script(fs.readFileSync(MAP, 'utf8'), {
      filename: 'turncraft/maps.js'
    }),
    game: new vm.Script(source, {
      filename: 'turncraft/authoritative-engine.js'
    })
  };
  return compiled;
}
function createSimulator({ clock, seed = 1, checkpoint } = {}) {
  const paint = new Proxy(
    {
      measureText: (text) => ({ width: String(text).length * 6 }),
      createLinearGradient: () => ({ addColorStop: noop }),
      createRadialGradient: () => ({ addColorStop: noop }),
      getImageData: () => ({ data: new Uint8ClampedArray(4) })
    },
    { get: (target, key) => target[key] || noop }
  );
  function element() {
    return {
      style: { setProperty: noop },
      dataset: {},
      children: [],
      classList: {
        add: noop,
        remove: noop,
        toggle: noop,
        contains: () => false
      },
      appendChild: noop,
      append: noop,
      remove: noop,
      setAttribute: noop,
      getAttribute: () => null,
      replaceChildren: noop,
      addEventListener: noop,
      removeEventListener: noop,
      querySelector: () => null,
      querySelectorAll: () => [],
      getContext: () => paint,
      getBoundingClientRect: () => ({
        left: 0,
        top: 700,
        width: 1280,
        height: 100,
        right: 1280,
        bottom: 800
      }),
      insertAdjacentHTML: noop,
      focus: noop,
      click: noop,
      textContent: '',
      innerHTML: '',
      scrollTop: 0,
      scrollLeft: 0
    };
  }
  const elements = new Map(),
    document = {
      getElementById: (id) => {
        if (!elements.has(id)) elements.set(id, element());
        return elements.get(id);
      },
      createElement: element,
      querySelector: () => null,
      querySelectorAll: () => [],
      addEventListener: noop,
      body: element(),
      head: element(),
      documentElement: element()
    },
    microtasks = [];
  let randomState = seed >>> 0 || 1;
  const math = Object.create(Math);
  math.random = () => {
    randomState ^= randomState << 13;
    randomState ^= randomState >>> 17;
    randomState ^= randomState << 5;
    return (randomState >>> 0) / 4294967296;
  };
  const sandbox = {
    console,
    document,
    Math: math,
    URL,
    URLSearchParams,
    location: {
      search: '',
      href: 'http://turncraft.server/game/turncraft.html'
    },
    localStorage: { getItem: () => null, setItem: noop },
    innerWidth: 1280,
    innerHeight: 900,
    devicePixelRatio: 1,
    performance: { now: () => sandbox.authorityClock },
    setTimeout: () => 0,
    clearTimeout: noop,
    setInterval: () => 0,
    clearInterval: noop,
    requestAnimationFrame: noop,
    queueMicrotask: (callback) => microtasks.push(callback),
    matchMedia: () => ({ matches: false, addEventListener: noop }),
    HTMLInputElement: class {},
    HTMLSelectElement: class {},
    Image: class {},
    initializeApp: () => ({}),
    getAuth: () => ({}),
    getFirestore: () => ({}),
    navigator: {},
    screen: {},
    addEventListener: noop,
    authorityClock: Number(clock) || Date.now()
  };
  sandbox.window = sandbox;
  const context = vm.createContext(sandbox),
    code = program();
  code.map.runInContext(context, { timeout: 5000 });
  code.game.runInContext(context, { timeout: 5000 });
  function run(source) {
    const value = vm.runInContext(
      `authorityEvaluate(${JSON.stringify(source)})`,
      context,
      { timeout: 5000 }
    );
    while (microtasks.length) microtasks.shift()();
    return value;
  }
  const plain = (source) => JSON.parse(JSON.stringify(run(source)));
  if (checkpoint) {
    const value = JSON.parse(
      gunzipSync(Buffer.from(checkpoint, 'base64')).toString('utf8')
    );
    sandbox.authorityClock = value.clock;
    randomState = value.randomState;
    run(
      `applyRemoteState(${JSON.stringify(value.state)});lobbyId='';isController=false;gameStarted=true;paused=false;nextEntityId=${value.nextEntityId};nextNodeId=${value.nextNodeId};simAccumulator=${value.accumulator || 0};`
    );
  }
  function start({ players, mapId = 'classic', gameMode = 'annihilation' }) {
    D.fail(
      players?.length === 2 &&
        players.every((p) => D.FACTIONS.includes(p.factionId)),
      'INVALID_ARGUMENT',
      'Ranked play requires two existing factions.'
    );
    run(
      `lobbyId='';chosenMapId=${JSON.stringify(mapId)};chosenGameMode=${JSON.stringify(gameMode)};chosenDifficulty='medium';chosenAiStyle='balanced';createGame(${JSON.stringify(players.map((p) => ({ name: p.name, factionId: p.factionId, ai: false })))});`
    );
    return snapshot();
  }
  function advance(to) {
    const target = Math.min(Number(to), sandbox.authorityClock + 5000);
    D.fail(
      Number.isFinite(target),
      'INVALID_ARGUMENT',
      'Invalid simulation time.'
    );
    run(
      `(()=>{const end=${target};while(authorityClock<end&&!gameOver){const dt=Math.min(.05,(end-authorityClock)/1000);authorityClock+=dt*1000;simAccumulator+=dt;updatePersistentOrders();updateCombat(dt);resolveProjectileImpacts();updateProduction(dt);updateRegeneration(dt);updateAreaEffects();updateNeutralObjectives(dt);updateObjectives(dt);if(simAccumulator>=.25){simAccumulator=0;updateMining();updateLastSeenIntel();checkBaseStates()}checkBaseStates();}return authorityClock})()`
    );
    return snapshot();
  }
  function command(slot, command) {
    sandbox.authorityCommand = command;
    run(
      `(()=>{const slot=${Number(slot)},command=globalThis.authorityCommand;PLAYER=slot;const actors=[...(command.entityIds||[]),...(command.entityId!==undefined?[command.entityId]:[])].map(entityById);if(actors.some(e=>!e||e.alive===false||e.faction!==slot))return false;const target=entityById(command.targetId);if(target&&target.faction!==slot&&!tileVisibleToFaction(slot,target.x,target.y))return false;applyCommand({...command,slot,by:players[slot].name});return true;})()`
    );
    delete sandbox.authorityCommand;
    return snapshot();
  }
  function finish(winnerIdx, reason, at) {
    if (Number.isFinite(at)) sandbox.authorityClock = at;
    run(
      `finishMatch(${winnerIdx === null ? 'null' : `players[${Number(winnerIdx)}]`},${JSON.stringify(reason)});`
    );
    return snapshot();
  }
  function snapshot() {
    return plain('serializeState()');
  }
  function publicSnapshot(slot) {
    const state = snapshot();
    if (state.gameOver) return state;
    const visible = plain(
        `(()=>{rebuildVisionCache(true);return entities.filter(e=>e.alive!==false&&(e.faction===${Number(slot)}||tileVisibleToFaction(${Number(slot)},e.x,e.y))).map(e=>e.id)})()`
      ),
      ids = new Set(visible),
      visibleTiles = new Set(
        plain(`Array.from(visionCache.get(${Number(slot)})||[])`)
      );
    const canSee = (value) =>
      Number.isFinite(value.x) &&
      Number.isFinite(value.y) &&
      visibleTiles.has(`${Math.floor(value.x)},${Math.floor(value.y)}`);
    state.entities = state.entities
      .filter((e) => ids.has(e.id))
      .map((e) => {
        if (e.faction === slot) return e;
        const publicEntity = { ...e };
        for (const key of [
          'queue',
          'pendingBuild',
          'moveDestination',
          'attackMoveDestination',
          'targetId',
          'miningNodeId',
          'abilityReadyAt',
          'attackReadyAt',
          'garrisonedIds',
          'passengers',
          'research',
          'logisticsChannel',
          'directTargetId'
        ])
          delete publicEntity[key];
        return publicEntity;
      });
    state.players = state.players.map((p, i) =>
      i === slot
        ? p
        : {
            idx: p.idx,
            name: p.name,
            factionId: p.factionId,
            ai: false,
            eliminated: p.eliminated,
            ready: true,
            minerals: 0,
            gas: 0,
            research: {},
            strategy: { lastSeen: { units: {}, structures: {} } }
          }
    );
    state.resourceNodes = state.resourceNodes.map((n) =>
      canSee(n) ? n : { ...n, amount: 0 }
    );
    state.areaEffects = state.areaEffects.filter(
      (e) => e.faction === slot || canSee(e)
    );
    state.scanEffects = state.scanEffects.filter((e) => e.faction === slot);
    state.temporaryObstacles = state.temporaryObstacles.filter(
      (e) => e.faction === slot || canSee(e)
    );
    state.pendingImpacts = state.pendingImpacts.filter((e) => canSee(e));
    return state;
  }
  function save() {
    const state = snapshot(),
      metadata = plain(
        '({nextEntityId,nextNodeId,accumulator:simAccumulator})'
      );
    return gzipSync(
      JSON.stringify({
        state,
        clock: sandbox.authorityClock,
        randomState: randomState >>> 0,
        ...metadata
      })
    ).toString('base64');
  }
  return {
    start,
    advance,
    command,
    finish,
    snapshot,
    publicSnapshot,
    save,
    get clock() {
      return sandbox.authorityClock;
    }
  };
}
const COMMANDS = Object.freeze([
  'move',
  'groupMove',
  'attack',
  'groupAttack',
  'build',
  'train',
  'research',
  'orbitalScan',
  'massRecall',
  'morph',
  'ability',
  'groupAbility',
  'garrison',
  'unloadBunker',
  'unloadDropship',
  'cancelQueue',
  'stop',
  'groupStop',
  'hold',
  'groupHold',
  'attackMove',
  'groupAttackMove',
  'setRally',
  'mine',
  'doctrine',
  'assistBuild',
  'tunnelTravel',
  'groupTunnelTravel'
]);
function validateCommand(input) {
  D.exact(input, [
    'type',
    'entityId',
    'entityIds',
    'targetId',
    'nodeId',
    'bunkerId',
    'mawId',
    'buildType',
    'unitType',
    'researchId',
    'abilityId',
    'doctrineId',
    'x',
    'y',
    'mode'
  ]);
  D.fail(
    COMMANDS.includes(input.type),
    'INVALID_ARGUMENT',
    'That battlefield command is unsupported.'
  );
  for (const key of ['entityId', 'targetId', 'nodeId', 'bunkerId', 'mawId'])
    if (input[key] !== undefined && input[key] !== null)
      D.integer(input[key], 1, 10000000);
  if (input.entityIds !== undefined)
    D.fail(
      Array.isArray(input.entityIds) &&
        input.entityIds.length <= 200 &&
        input.entityIds.every(
          (id) => Number.isInteger(id) && id > 0 && id < 10000000
        ),
      'INVALID_ARGUMENT',
      'Select a supported unit group.'
    );
  for (const [key, max] of [
    ['x', 42],
    ['y', 30]
  ])
    if (input[key] !== undefined)
      D.fail(
        Number.isInteger(input[key]) && input[key] >= 0 && input[key] < max,
        'INVALID_ARGUMENT',
        'That command points outside the battlefield.'
      );
  for (const key of [
    'buildType',
    'unitType',
    'researchId',
    'abilityId',
    'doctrineId',
    'mode'
  ])
    if (input[key] !== undefined)
      D.fail(
        typeof input[key] === 'string' &&
          /^[a-zA-Z0-9_-]{1,60}$/.test(input[key]),
        'INVALID_ARGUMENT',
        'That command option is unsupported.'
      );
  return input;
}
module.exports = { createSimulator, validateCommand, COMMANDS };
