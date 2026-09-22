// Only idempotent field assignments belong in this queue. Purchases, increments,
// deletes and combat transactions must retain their own commit semantics.
const clone = value => JSON.parse(JSON.stringify(value));
const equal = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const transientCodes = new Set(['unavailable', 'deadline-exceeded', 'aborted', 'resource-exhausted', 'unknown']);

export function stateFieldPatch(data) {
  const patch = {};
  for (const [key, value] of Object.entries(data)) {
    if ((key === 'cat' || key === 'objectives') && value && typeof value === 'object') {
      // Each objective is atomic; replacing supplyDrop must clear the previous
      // drop's coordinates. Cat fields must not erase facing/health siblings.
      for (const [field, child] of Object.entries(value)) patch[`${key}.${field}`] = clone(child);
    } else {
      patch[key] = clone(value);
    }
  }
  return patch;
}

export function createPatchWriter({
  write, minIntervalMs = 120, enabled = () => true,
  now = Date.now, schedule = setTimeout, unschedule = clearTimeout,
  onError = () => {}, maxRetries = 5,
  bookkeeping = ['updatedAt', 'lastSeen', 'clientSeq']
}) {
  let pending = {}, confirmed = {}, flight = null, timer = null, completion = null;
  let lastStartedAt = -Infinity, retryAt = 0, failures = 0, generation = 0;
  const ignored = new Set(bookkeeping);
  const stats = { writes:0, skipped:0, retries:0 };

  function settle(error, value = true) {
    const current = completion;
    completion = null;
    if (error) current?.reject(error); else current?.resolve(value);
  }

  function kick() {
    if (timer !== null || flight || !Object.keys(pending).length || !enabled()) return;
    const delay = Math.max(0, lastStartedAt + minIntervalMs - now(), retryAt - now());
    timer = schedule(() => { timer = null; void flush(); }, delay);
  }

  async function flush() {
    if (flight || !enabled() || !Object.keys(pending).length) return;
    const patch = pending;
    pending = {};
    const changed = Object.keys(patch).some(key => !ignored.has(key) && !equal(patch[key], confirmed[key]));
    if (!changed) {
      stats.skipped++;
      settle(null);
      return;
    }
    // Send only changed gameplay fields, retaining the bookkeeping for real writes.
    const delta = Object.fromEntries(Object.entries(patch).filter(([key, value]) => ignored.has(key) || !equal(value, confirmed[key])));
    const currentGeneration = generation;
    flight = delta;
    lastStartedAt = now(); // Network RTT already consumes this interval.
    stats.writes++;
    try {
      await write(delta);
      if (currentGeneration === generation) {
        Object.assign(confirmed, delta);
        failures = 0;
        retryAt = 0;
      }
    } catch (error) {
      if (currentGeneration === generation) {
        const code = String(error?.code || '').replace(/^firestore\//, '');
        if (transientCodes.has(code) && failures < maxRetries) {
          // A newer value always wins, but unrelated failed fields survive.
          pending = { ...patch, ...pending };
          failures++;
          stats.retries++;
          retryAt = now() + Math.min(8000, 500 * 2 ** (failures - 1));
        } else {
          pending = {};
          failures = 0;
          retryAt = 0;
          settle(error);
          onError(error);
        }
      }
    } finally {
      flight = null;
      if (currentGeneration === generation && !Object.keys(pending).length) settle(null);
      kick();
    }
  }

  return {
    enqueue(patch) {
      pending = { ...pending, ...clone(patch) };
      if (!completion) {
        let resolve, reject;
        const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
        // Fire-and-forget callers still report errors through onError.
        promise.catch(() => {});
        completion = { promise, resolve, reject };
      }
      const promise = completion.promise;
      kick();
      return promise;
    },
    // Use server-confirmed snapshots only. Pending local echoes are not acks.
    observe(patch) { Object.assign(confirmed, clone(patch)); },
    resume: kick,
    cancel() {
      generation++;
      pending = {};
      confirmed = {};
      failures = 0;
      retryAt = 0;
      if (timer !== null) unschedule(timer);
      timer = null;
      settle(null, false);
      // An SDK write already in flight cannot be cancelled. Its completion is
      // ignored; do not overlap a new generation with it.
    },
    get stats() { return { ...stats, pendingFields:Object.keys(pending).length, inFlight:!!flight }; }
  };
}

export function eligibleHostCandidate(players, isActive, excludedUid = null) {
  return Object.entries(players).filter(([uid, player]) => uid !== excludedUid && isActive(player))
    .sort((a, b) => (a[1].joinedAt || 0) - (b[1].joinedAt || 0) || a[0].localeCompare(b[0]))[0]?.[0] || null;
}

// Rendering cache is intentionally local. No DOM/animation state is synced.
export function reconcileKeyedNodes(cache, layer, entries, keyOf, signatureOf, create) {
  const seen = new Set();
  for (const entry of entries) {
    const key = keyOf(entry), signature = signatureOf(entry);
    seen.add(key);
    let cached = cache.get(key);
    if (cached?.signature === signature && cached.node.parentNode === layer) continue;
    const node = create(entry);
    if (cached) cached.node.remove();
    cache.delete(key);
    if (node) {
      layer.appendChild(node);
      cache.set(key, { node, signature });
    }
  }
  for (const [key, cached] of cache) {
    if (!seen.has(key)) { cached.node.remove(); cache.delete(key); }
  }
}
