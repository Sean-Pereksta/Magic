import { clamp } from "./data.mjs";
export class MinHeap {
  constructor() {
    this.items = [];
  }
  push(id, score) {
    const a = this.items;
    let i = a.length;
    a.push({ id, score });
    while (i > 0) {
      const p = (i - 1) >> 1;
      if (a[p].score <= score) break;
      a[i] = a[p];
      i = p;
    }
    a[i] = { id, score };
  }
  pop() {
    const a = this.items,
      first = a[0],
      last = a.pop();
    if (a.length) {
      let i = 0;
      while (i * 2 + 1 < a.length) {
        let j = i * 2 + 1;
        if (j + 1 < a.length && a[j + 1].score < a[j].score) j++;
        if (last.score <= a[j].score) break;
        a[i] = a[j];
        i = j;
      }
      a[i] = last;
    }
    return first;
  }
}
export class Navigator {
  constructor(map) {
    this.map = map;
    const n = map.width * map.height;
    this.cost = new Float32Array(n);
    this.parents = new Int32Array(n);
    this.seen = new Uint32Array(n);
    this.closed = new Uint32Array(n);
    this.stamp = 0;
    this.cache = new Map();
    this.maxVisits = Math.min(8192, n * 2);
    this.lastVisits = 0;
    this.backbone = map.roads?.length ? map.settlements.map(() => []) : null;
    if (this.backbone) for (const road of map.roads) {
      this.backbone[road.a].push({ to: road.b, points: road.points, reverse: false });
      this.backbone[road.b].push({ to: road.a, points: road.points, reverse: true });
    }
  }
  walkable(x, y) {
    return (
      x >= 0 &&
      y >= 0 &&
      x < this.map.width &&
      y < this.map.height &&
      this.map.costs[y * this.map.width + x] > 0
    );
  }
  nearest(x, y) {
    x = clamp(Math.floor(x), 0, this.map.width - 1);
    y = clamp(Math.floor(y), 0, this.map.height - 1);
    if (this.walkable(x, y)) return { x, y };
    for (let r = 1; r < 10; r++)
      for (let dy = -r; dy <= r; dy++)
        for (let dx = -r; dx <= r; dx++) {
          if (Math.max(Math.abs(dx), Math.abs(dy)) !== r) continue;
          if (this.walkable(x + dx, y + dy)) return { x: x + dx, y: y + dy };
        }
    return null;
  }
  path(start, end) {
    this.lastVisits = 0;
    const a = this.nearest(start.x, start.y),
      b = this.nearest(end.x, end.y);
    if (!a || !b) return [];
    const w = this.map.width,
      s = a.y * w + a.x,
      t = b.y * w + b.x,
      key = `${s}:${t}:${this.map.version || 0}`;
    if (this.cache.has(key)) return this.cache.get(key).map((p) => ({ ...p }));
    if (s === t) return [];
    let path = null;
    if (this.backbone && Math.hypot(a.x - b.x, a.y - b.y) > 48)
      path = this.backbonePath(a, b);
    path ||= this.search(a, b, this.maxVisits);
    if (!path?.length) return [];
    if (this.cache.size >= 192) this.cache.delete(this.cache.keys().next().value);
    this.cache.set(key, path);
    return path.map((p) => ({ ...p }));
  }
  backbonePath(a, b) {
    const sites = this.map.settlements;
    const candidates = (p) => sites.slice(0, this.backbone.length).map((s, id) => ({ id, d: Math.hypot(s.x - p.x, s.y - p.y) }))
      .sort((x, y) => x.d - y.d || x.id - y.id).slice(0, 3);
    let first, last, enter, leave;
    for (const { id } of candidates(a)) {
      const site = { x: Math.floor(sites[id].x), y: Math.floor(sites[id].y) };
      const route = site.x === a.x && site.y === a.y ? [] : this.search(a, site, 3072);
      if (route) { first = id; enter = route; break; }
    }
    for (const { id } of candidates(b)) {
      const site = { x: Math.floor(sites[id].x), y: Math.floor(sites[id].y) };
      const route = site.x === b.x && site.y === b.y ? [] : this.search(site, b, 3072);
      if (route) { last = id; leave = route; break; }
    }
    if (first == null || last == null) return null;
    const heap = new MinHeap(), distance = new Float32Array(sites.length).fill(Infinity), previous = [];
    distance[first] = 0; heap.push(first, 0);
    while (heap.items.length) {
      const { id, score } = heap.pop();
      if (score > distance[id] + .001) continue;
      if (id === last) break;
      for (const edge of this.backbone[id]) {
        const next = score + edge.points.length;
        if (next < distance[edge.to]) { distance[edge.to] = next; previous[edge.to] = { from: id, edge }; heap.push(edge.to, next); }
      }
    }
    if (!Number.isFinite(distance[last])) return null;
    const edges = [];
    for (let at = last; at !== first;) {
      const item = previous[at]; if (!item) return null;
      edges.push(item.edge); at = item.from;
    }
    const path = [...enter];
    for (const edge of edges.reverse()) {
      const points = edge.reverse ? [...edge.points].reverse() : edge.points;
      for (const p of points) if (!path.length || p.x !== path.at(-1).x || p.y !== path.at(-1).y) path.push({ ...p });
    }
    path.push(...leave);
    return path;
  }
  search(a, b, maxVisits) {
    const w = this.map.width, s = a.y * w + a.x, t = b.y * w + b.x;
    const stamp = ++this.stamp,
      heap = new MinHeap();
    this.seen[s] = stamp;
    this.cost[s] = 0;
    this.parents[s] = -1;
    heap.push(s, 0);
    let visits = 0,
      found = false;
    const dirs = [
      [-1, 0],
      [1, 0],
      [0, -1],
      [0, 1],
      [-1, -1],
      [1, -1],
      [-1, 1],
      [1, 1],
    ];
    while (heap.items.length && visits++ < maxVisits) {
      const { id } = heap.pop();
      if (this.closed[id] === stamp) continue;
      this.closed[id] = stamp;
      if (id === t) {
        found = true;
        break;
      }
      const x = id % w,
        y = Math.floor(id / w);
      for (const [dx, dy] of dirs) {
        const nx = x + dx,
          ny = y + dy;
        if (!this.walkable(nx, ny)) continue;
        if (
          dx &&
          dy &&
          (!this.walkable(x + dx, y) || !this.walkable(x, y + dy))
        )
          continue;
        const next = ny * w + nx;
        if (this.closed[next] === stamp) continue;
        const cost =
          this.cost[id] + (dx && dy ? 1.4142 : 1) * this.map.costs[next];
        if (this.seen[next] !== stamp || cost < this.cost[next]) {
          this.seen[next] = stamp;
          this.cost[next] = cost;
          this.parents[next] = id;
          heap.push(next, cost + Math.hypot(nx - b.x, ny - b.y) * 0.7);
        }
      }
    }
    this.lastVisits += visits;
    if (!found) return null;
    const path = [];
    for (let p = t; p !== s && p !== -1; p = this.parents[p])
      path.push({ x: (p % w) + 0.5, y: Math.floor(p / w) + 0.5 });
    path.reverse();
    return path;
  }
  clearLine(a, b) {
    const steps = Math.ceil(Math.hypot(b.x - a.x, b.y - a.y) * 3);
    for (let i = 0; i <= steps; i++) {
      const f = steps ? i / steps : 0;
      if (
        !this.walkable(
          Math.floor(a.x + (b.x - a.x) * f),
          Math.floor(a.y + (b.y - a.y) * f),
        )
      )
        return false;
    }
    return true;
  }
}
export class SpatialHash {
  constructor(size = 3) {
    this.size = size;
    this.buckets = new Map();
    this.pool = [];
  }
  rebuild(units) {
    for (const b of this.buckets.values()) {
      b.length = 0;
      this.pool.push(b);
    }
    this.buckets.clear();
    for (const u of units) {
      if (u.hp <= 0 || u.routed) continue;
      const key = `${Math.floor(u.x / this.size)},${Math.floor(u.y / this.size)}`;
      let b = this.buckets.get(key);
      if (!b) {
        b = this.pool.pop() || [];
        this.buckets.set(key, b);
      }
      b.push(u);
    }
  }
  each(x, y, r, fn) {
    const n = this.size;
    for (let by = Math.floor((y - r) / n); by <= Math.floor((y + r) / n); by++)
      for (
        let bx = Math.floor((x - r) / n);
        bx <= Math.floor((x + r) / n);
        bx++
      ) {
        const bucket = this.buckets.get(`${bx},${by}`);
        if (bucket)
          for (const u of bucket) {
            const d = (u.x - x) ** 2 + (u.y - y) ** 2;
            if (d <= r * r) fn(u, d);
          }
      }
  }
}
