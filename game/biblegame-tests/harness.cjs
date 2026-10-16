const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const sourceFile = path.join(__dirname, '..', 'biblegame-multiplayer.html');
const html = fs.readFileSync(sourceFile, 'utf8');
const script = html.match(/<script\s+type="module">([\s\S]*?)<\/script>/)[1]
  .replace(/^import\s+.*;\s*$/gm, '');

function copy(value) {
  if (!value || typeof value !== 'object') return value;
  if (typeof value.toMillis === 'function') return timestamp(value.toMillis());
  if (Array.isArray(value)) return value.map(copy);
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, copy(item)]));
}

function timestamp(ms) { return { toMillis: () => ms }; }

class Element {
  constructor(tagName = 'DIV', id = '') {
    this.tagName = tagName.toUpperCase();
    this.id = id;
    this.dataset = {};
    this.children = [];
    this.value = '';
    this.disabled = false;
    this.textContent = '';
    this.className = '';
    this.style = {};
    this.listeners = new Map();
    const classes = new Set();
    this.classList = {
      add: (...items) => items.forEach(item => classes.add(item)),
      remove: (...items) => items.forEach(item => classes.delete(item)),
      contains: item => classes.has(item),
      toggle: (item, enabled = !classes.has(item)) => {
        enabled ? classes.add(item) : classes.delete(item);
        return enabled;
      }
    };
  }
  set innerHTML(value) {
    this._innerHTML = value;
    this.children = [];
    // Rebuilding a real select destroys its selected option.
    if (this.tagName === 'SELECT') this.value = '';
    for (const match of value.matchAll(/<button[^>]*class="([^"]+)"[^>]*data-id="([^"]+)"[^>]*>/g)) {
      const element = new Element('button');
      element.className = match[1];
      element.dataset.id = match[2];
      this.children.push(element);
    }
  }
  get innerHTML() { return this._innerHTML || ''; }
  get options() { return this.children; }
  get selectedOptions() { return this.options.filter(option => option.selected); }
  appendChild(child) { this.children.push(child); return child; }
  append(...children) { this.children.push(...children); }
  addEventListener(name, listener) { this.listeners.set(name, listener); }
  remove() {}
  focus() { this.focused = true; }
  setAttribute(name, value) { this[name] = value; }
  removeAttribute(name) { delete this[name]; }
}

function createHarness({ now = 1_000_000 } = {}) {
  let clock = now;
  const elements = new Map();
  for (const match of html.matchAll(/<([a-z]+)[^>]*\bid="([^"]+)"/gi)) {
    elements.set(match[2], new Element(match[1], match[2]));
  }
  const document = {
    getElementById(id) {
      if (!elements.has(id)) elements.set(id, new Element('div', id));
      return elements.get(id);
    },
    createElement: tag => new Element(tag),
    querySelectorAll(selector) {
      const all = [...elements.values()].flatMap(element => [element, ...element.children]);
      if (selector === '.mc-btn') return all.filter(element => element.className.split(' ').includes('mc-btn'));
      return [];
    },
    addEventListener() {},
    documentElement: new Element(),
    body: new Element('body')
  };
  const docs = new Map();
  const listeners = new Map();
  const writes = [];
  const transactions = [];
  const intervals = new Map();
  let intervalId = 0;
  let beforeTransaction;
  let transactionTail = Promise.resolve();
  const ref = (...parts) => ({ path: parts.flatMap(part => part?.path ? [part.path] : typeof part === 'string' ? [part] : []).join('/') });
  const snap = (value, metadata = {}) => ({
    exists: () => value !== undefined,
    data: () => copy(value),
    metadata: { hasPendingWrites: false, fromCache: false, ...metadata }
  });
  const write = (type, target, data, options = {}) => {
    const value = copy(data);
    if (type === 'delete') docs.delete(target.path);
    else if (type === 'update') {
      const next = copy(docs.get(target.path)) || {};
      for (const [field, item] of Object.entries(value)) {
        const parts = field.split('.');
        let parent = next;
        for (const part of parts.slice(0, -1)) {
          if (!parent[part] || typeof parent[part] !== 'object') parent[part] = {};
          parent = parent[part];
        }
        parent[parts.at(-1)] = item;
      }
      docs.set(target.path, next);
    } else docs.set(target.path, options.merge ? { ...copy(docs.get(target.path)), ...value } : value);
    writes.push({ type, path: target.path, data: value });
  };
  const context = vm.createContext({
    console,
    URL,
    Map,
    Set,
    Promise,
    Date: class extends Date { static now() { return clock; } },
    crypto: { randomUUID: () => 'test-connection' },
    location: { href: 'https://example.test/game/biblegame-multiplayer.html?gameId=test&username=Alice' },
    navigator: { onLine: true },
    document,
    window: { addEventListener() {}, removeEventListener() {} },
    initializeApp: () => ({}), getFirestore: () => ({}), getAuth: () => ({}),
    doc: ref, collection: ref, query: target => target,
    orderBy: () => ({}), where: () => ({}), limit: () => ({}),
    serverTimestamp: () => timestamp(clock),
    getDoc: async target => snap(docs.get(target.path)),
    getDocs: async target => {
      const children = [...docs.entries()]
        .filter(([key]) => key.startsWith(`${target.path}/`) && !key.slice(target.path.length + 1).includes('/'))
        .map(([key, value]) => ({ id: key.split('/').at(-1), ref: ref(key), data: () => copy(value) }));
      return { docs: children, empty: children.length === 0, size: children.length, forEach: callback => children.forEach(callback) };
    },
    setDoc: async (target, data, options) => write('set', target, data, options),
    updateDoc: async (target, data) => write('update', target, data),
    addDoc: async (target, data) => write('set', ref(target, `message-${writes.length}`), data),
    onSnapshot(target, ...args) {
      const callback = args.find(arg => typeof arg === 'function');
      if (!listeners.has(target.path)) listeners.set(target.path, new Set());
      listeners.get(target.path).add(callback);
      return () => listeners.get(target.path).delete(callback);
    },
    runTransaction(_db, callback) {
      const run = transactionTail.then(async () => {
        if (beforeTransaction) {
          const hook = beforeTransaction;
          beforeTransaction = undefined;
          await hook();
        }
        const pending = [];
        const tx = {
          get: async target => snap(docs.get(target.path)),
          set: (target, data, options) => pending.push(['set', target, data, options]),
          update: (target, data) => pending.push(['update', target, data]),
          delete: target => pending.push(['delete', target])
        };
        const result = await callback(tx);
        pending.forEach(args => write(...args));
        transactions.push(pending.map(([type, target, data]) => ({ type, path: target.path, data: copy(data) })));
        return result;
      });
      transactionTail = run.catch(() => {});
      return run;
    },
    // Keep authentication startup dormant: tests exercise the real production
    // methods and snapshots without fetching the external Bible dataset.
    onAuthStateChanged() {}, signInAnonymously: async () => {},
    setTimeout: () => 1, clearTimeout() {},
    setInterval(callback) { intervals.set(++intervalId, callback); return intervalId; },
    clearInterval(id) { intervals.delete(id); },
    MULTIPLAYER_CATEGORY_KEYS: ['faith'],
    conceptLabel: key => key, conceptHint: () => 'faith',
    matchVerseConcept: text => /faith/i.test(text) ? { term: 'faith' } : null,
    fetch: async () => { throw new Error('Unexpected network request in regression test'); }
  });
  vm.runInContext(script, context, { filename: sourceFile });
  const run = code => vm.runInContext(code, context, { timeout: 1000 });
  const playerId = run("stablePlayerUid('Alice')");
  const sessionPath = 'lobbies/test/bibleSessions/test-session';
  const playerPath = `${sessionPath}/players/${playerId}`;
  const verse = { id: 1, book: 'Genesis', chapter: 1, verse: 1, text: 'A verse about faith.' };
  const verse2 = { id: 2, book: 'John', chapter: 3, verse: 16, text: 'Another verse about faith.' };
  const baseSession = {
    sessionId: 'test-session', status: 'playing', mode: 'traditional',
    hostUid: playerId, hostName: 'Alice', round: 1, roundKey: 1,
    phaseStartedAt: timestamp(now - 10_000), durationMs: 40_000,
    currentQuestion: verse, chosenIds: [verse.id], allowedBooks: ['Genesis', 'John'],
    participantUids: [playerId], teamA: [playerId], teamB: ['bob'],
    teamTotals: { A: 0, B: 0 }, teamLocks: { A: false, B: false },
    sharedLives: 3, coopGoal: 12, coopCorrectCount: 0, coopLocked: false,
    options: [verse, verse2], bookPoolMode: 'all', categoryKey: 'faith'
  };
  context.__fixture = { playerId, sessionPath, baseSession, verses: [verse, verse2] };
  run(`
    playerUid=__fixture.playerId;
    currentUser={uid:'auth-alice'};
    LOBBY={host:'Alice',players:['Alice','Bob']};
    CONTROL={activeSessionId:'test-session',hostName:'Alice'};
    currentSessionId='test-session';
    sessionRef=doc(db,__fixture.sessionPath);
    playersCol=collection(sessionRef,'players');
    votesCol=collection(sessionRef,'votes');
    bookChoicesCol=collection(sessionRef,'bookChoices');
    submissionsCol=collection(sessionRef,'submissions');
    activePlayerRef=doc(playersCol,playerUid);
    SESSION=__fixture.baseSession;
    sessionSyncReady=playerSyncReady=true;
    sessionFromCache=playerFromCache=false;
    books=['Genesis','John'];
    verses=__fixture.verses;
    verseByRef=new Map(verses.map(v=>[normalizedBookKey(v.book)+' '+v.chapter+':'+v.verse,v]));
    PLAYERS=new Map([[playerUid,{uid:playerUid,username:'Alice',profileKey:'Alice',authUid:currentUser.uid,score:0,online:true,connectionId,lastSeenAt:serverTimestamp(),currentQuestion:verses[0],questionKey:1}]]);
  `);
  docs.set(sessionPath, copy(baseSession));
  docs.set('lobbies/test/bibleGame/control', { activeSessionId: 'test-session', hostUid: playerId, hostName: 'Alice' });
  docs.set(playerPath, copy(run('PLAYERS.get(playerUid)')));
  docs.set('users/Alice', { username: 'Alice', chatAuthUid: 'auth-alice', bibleGame: { elo: 0 } });

  return {
    run, context, elements, document, docs, writes, transactions, intervals,
    playerId, sessionPath, playerPath, verse, verse2,
    timestamp,
    el: id => document.getElementById(id),
    now: () => clock,
    setNow: value => { clock = value; },
    beforeTransaction: callback => { beforeTransaction = callback; },
    session: () => copy(docs.get(sessionPath)),
    player: () => copy(docs.get(playerPath)),
    setSession(patch, { local = true, stored = true } = {}) {
      const value = { ...copy(docs.get(sessionPath)), ...copy(patch) };
      if (stored) docs.set(sessionPath, value);
      if (local) { context.__nextSession = copy(value); run('SESSION=__nextSession'); }
      return value;
    },
    setPlayer(patch, { local = true, stored = true } = {}) {
      const value = { ...copy(docs.get(playerPath)), ...copy(patch) };
      if (stored) docs.set(playerPath, value);
      if (local) { context.__nextPlayer = copy(value); run('PLAYERS.set(playerUid,__nextPlayer)'); }
      return value;
    },
    emitDoc(target, data = docs.get(target), metadata = {}) {
      for (const callback of listeners.get(target) || []) callback(snap(data, metadata));
    },
    emitCollection(target, rows, metadata = {}) {
      const snapshot = {
        metadata: { hasPendingWrites: false, fromCache: false, ...metadata },
        forEach(callback) {
          for (const [id, data] of rows) callback({ id, data: () => copy(data), metadata: snapshot.metadata });
        }
      };
      for (const callback of listeners.get(target) || []) callback(snapshot);
    },
    snapshotListenerCount(target) { return listeners.get(target)?.size || 0; },
    captureDocListener(target) {
      const callback = [...(listeners.get(target) || [])][0];
      return (data, metadata = {}) => callback(snap(data, metadata));
    }
  };
}

module.exports = { createHarness, timestamp };
