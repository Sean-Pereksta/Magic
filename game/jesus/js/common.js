export const clamp = (value, min, max) => Math.max(min, Math.min(max, value));
export const lerp = (a, b, t) => a + (b - a) * t;
export const approach = (value, target, amount) => value < target
  ? Math.min(target, value + amount)
  : Math.max(target, value - amount);

export function overlaps(a, b) {
  return a.x < b.x + b.w && a.x + a.w > b.x &&
    a.y < b.y + b.h && a.y + a.h > b.y;
}

export function seededRandom(seed = 1) {
  let state = Math.max(1, Math.floor(Math.abs(seed))) >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 4294967296;
  };
}

export function choose(list, random = Math.random) {
  return list[Math.floor(random() * list.length)];
}

export function uid(prefix = "id") {
  uid.next = (uid.next || 0) + 1;
  return `${prefix}-${uid.next}`;
}

