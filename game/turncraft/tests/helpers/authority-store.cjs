'use strict';
const clone = (value) =>
  value === undefined ? undefined : structuredClone(value);
function merge(a, b) {
  const value = clone(a) || {};
  for (const [key, item] of Object.entries(b))
    value[key] =
      item && typeof item === 'object' && !Array.isArray(item)
        ? merge(value[key], item)
        : clone(item);
  return value;
}
function createStore(initial = {}) {
  const records = new Map(
    Object.entries(initial).map(([key, value]) => [key, clone(value)])
  );
  let serial = Promise.resolve();
  function snapshot(path) {
    const value = records.get(path);
    return {
      id: path.split('/').at(-1),
      exists: value !== undefined,
      data: () => clone(value)
    };
  }
  function doc(path) {
    return {
      path,
      id: path.split('/').at(-1),
      get: async () => snapshot(path),
      delete: async () => records.delete(path)
    };
  }
  function collection(name, filters = [], sort = null, cap = Infinity) {
    return {
      where: (key, op, value) =>
        collection(name, [...filters, [key, op, value]], sort, cap),
      orderBy: (key, direction) =>
        collection(name, filters, [key, direction], cap),
      limit: (n) => collection(name, filters, sort, n),
      get: async () => {
        let rows = [...records.keys()]
          .filter(
            (path) =>
              path.startsWith(name + '/') && path.split('/').length === 2
          )
          .map(snapshot)
          .filter((s) =>
            filters.every(([key, op, value]) =>
              op === '=='
                ? s.data()[key] === value
                : op === '>'
                  ? s.data()[key] > value
                  : false
            )
          );
        if (sort)
          rows.sort(
            (a, b) =>
              (a.data()[sort[0]] - b.data()[sort[0]]) *
              (sort[1] === 'desc' ? -1 : 1)
          );
        return { docs: rows.slice(0, cap), empty: rows.length === 0 };
      }
    };
  }
  const db = {
    doc,
    collection,
    runTransaction(callback) {
      const task = serial.then(async () => {
        const writes = [];
        let writing = false;
        const tx = {
          get: async (ref) => {
            if (writing)
              throw new Error(
                'Transactions must read every document before writing.'
              );
            return snapshot(ref.path);
          },
          create(ref, data) {
            if (records.has(ref.path))
              throw new Error('Document already exists.');
            writing = true;
            writes.push(['set', ref.path, clone(data)]);
          },
          set(ref, data, options) {
            writing = true;
            writes.push([
              'set',
              ref.path,
              options?.merge ? merge(records.get(ref.path), data) : clone(data)
            ]);
          },
          update(ref, data) {
            if (!records.has(ref.path))
              throw new Error('Missing update target.');
            writing = true;
            writes.push(['set', ref.path, merge(records.get(ref.path), data)]);
          },
          delete(ref) {
            writing = true;
            writes.push(['delete', ref.path]);
          }
        };
        const result = await callback(tx);
        for (const [op, key, value] of writes)
          if (op === 'delete') records.delete(key);
          else records.set(key, value);
        return result;
      });
      serial = task.catch(() => {});
      return task;
    }
  };
  return { db, records };
}
module.exports = { createStore };
