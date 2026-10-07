/* Persistence helpers shared by the game and Node tests. */
(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.TinyTroopsSaves = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';
  const PREFIX = 'tinyTroops.save.v2.', BACKUP_PREFIX = 'tinyTroops.backup.v3.';
  const isRecord = value => !!value && typeof value === 'object' && !Array.isArray(value);
  function valid(payload) {
    return isRecord(payload) && typeof payload.password === 'string' && payload.password.length > 0 &&
      isRecord(payload.state) && Array.isArray(payload.state.squad);
  }
  function parse(text) { try { const data = JSON.parse(text); return valid(data) ? data : null; } catch { return null; } }
  function stamp(data) { const value = Number(data?.updatedAtMs) || Number(data?.state?.savedAt) || 0; return Number.isFinite(value) ? Math.max(0, value) : 0; }
  function createStore(storage) {
    const get = key => { try { return storage.getItem(key); } catch { return null; } };
    function read(clean) {
      const raw = get(PREFIX + clean), backup = parse(get(BACKUP_PREFIX + clean)), data = parse(raw);
      return { data: data || backup, recovered: !data && !!backup, corrupt: !!raw && !data, hasBackup: !!backup };
    }
    function write(clean, payload) {
      if (!valid(payload)) return { ok: false, error: 'The checkpoint could not be validated.' };
      try {
        const serialized = JSON.stringify(payload), previous = get(PREFIX + clean);
        // A damaged file must never replace the last readable backup.
        if (parse(previous) && previous !== serialized) {
          try { storage.setItem(BACKUP_PREFIX + clean, previous); } catch { /* Primary write can still fit. */ }
        }
        storage.setItem(PREFIX + clean, serialized);
        if (get(PREFIX + clean) !== serialized) throw new Error('Browser storage did not retain the checkpoint.');
        return { ok: true };
      } catch (error) { return { ok: false, error: error.message || 'Browser storage is unavailable or full.' }; }
    }
    function list() {
      const names = new Set();
      try {
        for (let i = 0; i < storage.length; i++) {
          const key = storage.key(i) || '';
          for (const prefix of [PREFIX, BACKUP_PREFIX]) if (key.startsWith(prefix)) names.add(key.slice(prefix.length));
        }
      } catch { return []; }
      return [...names].map(clean => {
        const saved = read(clean), data = saved.data, state = data?.state, army = (state?.squad || []).filter(Boolean);
        return { clean, name: String(data?.username || clean), readable: !!data, recovered: saved.recovered,
          hasBackup: saved.hasBackup, updatedAtMs: stamp(data), wave: Number(state?.round) || 1,
          coins: Number(state?.coins) || 0, size: army.length, army: army.slice(0, 8).map(u => String(u.e || '⚔️')).join(' '),
          completed: !!state?.runEnded || !!state?.tt?.finished || ['dead', 'won'].includes(state?.phase) };
      }).sort((a, b) => b.updatedAtMs - a.updatedAtMs || a.name.localeCompare(b.name));
    }
    function available() { try { storage.getItem(PREFIX + '__availability__'); return Number.isFinite(storage.length); } catch { return false; } }
    return { read, write, list, available };
  }
  function withTimeout(operation, milliseconds = 8000) {
    let timer;
    const timeout = new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('Cloud connection timed out.')), milliseconds); });
    return Promise.race([Promise.resolve().then(operation), timeout]).finally(() => clearTimeout(timer));
  }
  function choose(candidates, password, validate = state => state) {
    const errors = [], matching = candidates.filter(c => valid(c.data) && c.data.password === password);
    matching.sort((a, b) => stamp(b.data) - stamp(a.data) || (Number(b.data.sequence) || 0) - (Number(a.data.sequence) || 0));
    for (const candidate of matching) {
      try {
        const state = validate(candidate.data.state);
        if (!isRecord(state) || !Array.isArray(state.squad)) throw new Error('Invalid checkpoint.');
        return { ...candidate, data: { ...candidate.data, state } };
      } catch (error) { errors.push(error.message); }
    }
    if (errors.length) throw new Error('The saved checkpoint could not be loaded. Your save files were kept.');
    if (candidates.some(c => valid(c.data))) throw new Error('Save name/code did not match.');
    throw new Error('No readable saved run found for that save name.');
  }
  function createWriter({ store, writeCloud, notify = () => {}, timeoutMs = 8000 }) {
    const pending = new Map(), latest = new Map(), generations = new Map();
    let running = false;
    function report(job, phase, error) {
      if (latest.get(job.payload.cleanUsername) === job.payload) notify({ phase, local: job.local.ok, error: error?.message || error || '', code: String(error?.code || ''), payload: job.payload });
    }
    async function drain() {
      if (running) return;
      running = true;
      try {
        while (pending.size) {
          const [clean, job] = pending.entries().next().value; pending.delete(clean);
          try {
            await withTimeout(() => writeCloud(job.payload, () => job.generation === (generations.get(clean) || 0)), timeoutMs);
            report(job, 'saved'); job.resolve({ local: job.local.ok, cloud: true });
          } catch (error) {
            report(job, 'failed', error); job.resolve({ local: job.local.ok, cloud: false, error: error.message });
          } finally { if (latest.get(clean) === job.payload) latest.delete(clean); }
        }
      } finally { running = false; }
    }
    function save(payload, online) {
      // Detach both the checkpoint and profile identity before any asynchronous work.
      payload = JSON.parse(JSON.stringify(payload));
      const local = store.write(payload.cleanUsername, payload), job = { payload, local, generation: generations.get(payload.cleanUsername) || 0 };
      latest.set(payload.cleanUsername, payload);
      if (!valid(payload)) { report(job, 'failed', local.error); latest.delete(payload.cleanUsername); return Promise.resolve({ local: false, cloud: false, error: local.error }); }
      report(job, online ? 'syncing' : local.ok ? 'local' : 'failed', local.error);
      if (!online) { latest.delete(payload.cleanUsername); return Promise.resolve({ local: local.ok, cloud: false, error: local.error }); }
      const promise = new Promise(resolve => { job.resolve = resolve; });
      const replaced = pending.get(payload.cleanUsername);
      if (replaced) replaced.resolve({ local: replaced.local.ok, cloud: false, superseded: true });
      pending.set(payload.cleanUsername, job); void drain();
      return promise;
    }
    function invalidate(clean) {
      generations.set(clean, (generations.get(clean) || 0) + 1);
      const job = pending.get(clean); if (job) { pending.delete(clean); job.resolve({ local: job.local.ok, cloud: false, superseded: true }); }
      latest.delete(clean);
    }
    return { save, invalidate, pendingCount: () => pending.size + (running ? 1 : 0) };
  }
  function unusedName(name, entries, cleanName) {
    const used = new Set(entries.map(entry => entry.clean));
    if (!used.has(cleanName(name))) return name;
    for (let number = 2; number < 10000; number++) {
      const suffix = ' ' + number, candidate = name.slice(0, 28 - suffix.length) + suffix;
      if (!used.has(cleanName(candidate))) return candidate;
    }
    throw new Error('Choose another save name.');
  }
  return { PREFIX, BACKUP_PREFIX, valid, stamp, createStore, choose, withTimeout, createWriter, unusedName };
});
