export const DIRECTIONS = Object.freeze([[1, 0], [0, 1], [-1, 0], [0, -1]]);

export function tileAt(map, x, y) {
  return map.tiles[Math.round(y)]?.[Math.round(x)] ?? '#';
}

export function distanceSquared(a, b) {
  return (a.x - b.x) ** 2 + (a.y - b.y) ** 2;
}

export function findPath(start, goal, size, walkable, maxSteps = Infinity) {
  if (!walkable(goal.x, goal.y)) return null;
  const first = Math.round(start.y) * size + Math.round(start.x);
  const last = goal.y * size + goal.x;
  const visited = new Int32Array(size * size).fill(-1);
  const queue = new Int32Array(visited.length);
  let head = 0;
  let tail = 1;
  let layerEnd = tail;
  let steps = 0;
  queue[0] = first;
  visited[first] = first;

  while (head < tail && visited[last] === -1 && steps < maxSteps) {
    const current = queue[head++];
    const x = current % size;
    const y = Math.floor(current / size);
    for (const [dx, dy] of DIRECTIONS) {
      const nextX = x + dx;
      const nextY = y + dy;
      const next = nextY * size + nextX;
      if (!walkable(nextX, nextY) || visited[next] !== -1) continue;
      visited[next] = current;
      queue[tail++] = next;
    }
    if (head === layerEnd) {
      layerEnd = tail;
      steps++;
    }
  }

  if (visited[last] === -1) return null;
  const path = [];
  for (let node = last; node !== first; node = visited[node]) {
    path.push({ x: node % size, y: Math.floor(node / size) });
  }
  return path.reverse();
}

function push(queue, cell, cost) {
  let child = queue.length;
  queue.push({ cell, cost });
  while (child > 0) {
    const parent = Math.floor((child - 1) / 2);
    if (queue[parent].cost <= cost) break;
    queue[child] = queue[parent];
    child = parent;
  }
  queue[child] = { cell, cost };
}

function pop(queue) {
  const head = queue[0];
  const last = queue.pop();
  if (queue.length === 0) return head;
  let parent = 0;
  while (parent * 2 + 1 < queue.length) {
    let child = parent * 2 + 1;
    if (child + 1 < queue.length && queue[child + 1].cost < queue[child].cost) child++;
    if (queue[child].cost >= last.cost) break;
    queue[parent] = queue[child];
    parent = child;
  }
  queue[parent] = last;
  return head;
}

// Paths favor open lanes while retaining routes through structures the rats can breach.
export function buildEnemyField(map, goals, structures) {
  const { size } = map;
  const nextCell = new Int32Array(size * size).fill(-1);
  const costs = new Float64Array(nextCell.length).fill(Infinity);
  const queue = [];
  for (const goal of goals) {
    if (goal.hp === 0) continue;
    const cell = goal.y * size + goal.x;
    costs[cell] = 0;
    nextCell[cell] = cell;
    push(queue, cell, 0);
  }

  while (queue.length) {
    const { cell, cost } = pop(queue);
    if (cost !== costs[cell]) continue;
    const x = cell % size;
    const y = Math.floor(cell / size);
    const structure = structures.get(cell);
    const movementCost = structure ? 6 + structure.maxHp * 0.08 : 1;
    for (const [dx, dy] of DIRECTIONS) {
      const nextX = x + dx;
      const nextY = y + dy;
      const tile = tileAt(map, nextX, nextY);
      if (tile === '#' || tile === '~') continue;
      const neighbor = nextY * size + nextX;
      const nextCost = cost + movementCost + (tile === ':' ? 0 : 0.025);
      if (nextCost >= costs[neighbor]) continue;
      costs[neighbor] = nextCost;
      nextCell[neighbor] = cell;
      push(queue, neighbor, nextCost);
    }
  }
  return nextCell;
}

export function terrainHit(map, x, y, endX, endY, blocked = (tx, ty) => tileAt(map, tx, ty) === '#') {
  const dx = endX - x;
  const dy = endY - y;
  let tx = Math.round(x);
  let ty = Math.round(y);
  if (blocked(tx, ty)) return 0;
  const stepX = Math.sign(dx);
  const stepY = Math.sign(dy);
  const stepTimeX = dx ? 1 / Math.abs(dx) : Infinity;
  const stepTimeY = dy ? 1 / Math.abs(dy) : Infinity;
  let hitX = dx ? (tx + stepX / 2 - x) / dx : Infinity;
  let hitY = dy ? (ty + stepY / 2 - y) / dy : Infinity;

  while (Math.min(hitX, hitY) <= 1) {
    if (hitX === hitY) {
      if (blocked(tx + stepX, ty) || blocked(tx, ty + stepY)) return hitX;
      tx += stepX;
      ty += stepY;
      if (blocked(tx, ty)) return hitX;
      hitX += stepTimeX;
      hitY += stepTimeY;
    } else if (hitX < hitY) {
      tx += stepX;
      if (blocked(tx, ty)) return hitX;
      hitX += stepTimeX;
    } else {
      ty += stepY;
      if (blocked(tx, ty)) return hitY;
      hitY += stepTimeY;
    }
  }
  return null;
}

// Collide in relative space so a moving target can cross a shot between ticks.
export function movingCircleHit(x, y, dx, dy, target, radius) {
  const previousX = target.previousX ?? target.x;
  const previousY = target.previousY ?? target.y;
  const rx = x - previousX;
  const ry = y - previousY;
  const vx = dx - (target.x - previousX);
  const vy = dy - (target.y - previousY);
  const c = rx * rx + ry * ry - radius * radius;
  if (c <= 0) return 0;
  const a = vx * vx + vy * vy;
  const b = rx * vx + ry * vy;
  const determinant = b * b - a * c;
  if (a === 0 || b >= 0 || determinant < 0) return null;
  const impact = (-b - Math.sqrt(determinant)) / a;
  return impact <= 1 ? impact : null;
}

export function interceptPoint(shooter, target, speed, lifetime) {
  const dx = target.x - shooter.x;
  const dy = target.y - shooter.y;
  const vx = target.vx || 0;
  const vy = target.vy || 0;
  const a = vx * vx + vy * vy - speed * speed;
  const b = dx * vx + dy * vy;
  const c = dx * dx + dy * dy;
  const determinant = b * b - a * c;
  if (determinant < 0) return { x: target.x, y: target.y };
  const denominator = Math.sqrt(determinant) - b;
  const time = denominator > 0 ? Math.min(c / denominator, lifetime / 1000) : 0;
  return { x: target.x + vx * time, y: target.y + vy * time };
}
