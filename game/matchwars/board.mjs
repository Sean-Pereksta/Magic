/** Pure, DOM-free match rules. All snapshots are detached from the live board. */
export const ELEMENTS = Object.freeze(['ember', 'tide', 'grove', 'storm', 'sun', 'void']);
const SPECIALS = new Set(['row', 'column', 'nova', 'wild']);
const RELICS = ['fury', 'aegis', 'haste', 'vitality'];
const cloneTile = tile => ({ ...tile });
const at = ({ r, c }) => `${r},${c}`;

export class Board {
  constructor({ size = 7, elements = ELEMENTS, rng = Math.random } = {}) {
    if (!Number.isInteger(size) || size < 3 || size > 10) throw new RangeError('Board size must be 3–10.');
    if (!Array.isArray(elements) || new Set(elements).size !== elements.length || elements.length < 3 || elements.some(e => typeof e !== 'string')) {
      throw new TypeError('Use at least three distinct element names.');
    }
    if (typeof rng !== 'function') throw new TypeError('rng must be a function.');
    this.size = size;
    this.elements = [...elements];
    this.rng = rng;
    this.nextId = 1;
    this.grid = [];
    this._fill();
    if (!this.hint()) this._fill(true);
  }

  _random(length) {
    const value = Number(this.rng());
    return Math.min(length - 1, Math.max(0, Math.floor((Number.isFinite(value) ? value : 0) * length)));
  }

  _tile(key = this.elements[this._random(this.elements.length)], relics = true) {
    const relic = relics && this._random(1000) < 60 ? RELICS[this._random(RELICS.length)] : null;
    return { id: this.nextId++, key, special: null, relic };
  }

  _fill(forceMove = false) {
    this.grid = Array.from({ length: this.size }, () => Array(this.size).fill(null));
    const [a, b, c] = this.elements;
    const pattern = [[a, b, a], [c, a, c]];
    for (let r = 0; r < this.size; r++) {
      for (let col = 0; col < this.size; col++) {
        const available = this.elements.filter(key =>
          !(col >= 2 && this.grid[r][col - 1]?.key === key && this.grid[r][col - 2]?.key === key) &&
          !(r >= 2 && this.grid[r - 1][col]?.key === key && this.grid[r - 2][col]?.key === key));
        const key = forceMove && r < 2 && col < 3 ? pattern[r][col] : available[this._random(available.length)];
        this.grid[r][col] = this._tile(key);
      }
    }
  }

  snapshot() { return this.grid.map(row => row.map(cloneTile)); }

  /** Restore a validated snapshot; deliberately permits matches for authored scenarios. */
  restore(grid) {
    const ids = new Set();
    if (!Array.isArray(grid) || grid.length !== this.size || grid.some(row => !Array.isArray(row) || row.length !== this.size)) {
      throw new TypeError('Invalid board dimensions.');
    }
    for (const row of grid) for (const tile of row) {
      if (!tile || !this.elements.includes(tile.key) || (tile.special != null && !SPECIALS.has(tile.special)) ||
          (tile.relic != null && !RELICS.includes(tile.relic)) ||
          !((typeof tile.id === 'number' && Number.isSafeInteger(tile.id) && tile.id >= 0) || (typeof tile.id === 'string' && tile.id.length > 0)) || ids.has(tile.id)) {
        throw new TypeError('Invalid or duplicate tile.');
      }
      ids.add(tile.id);
    }
    this.grid = grid.map(row => row.map(tile => ({ id: tile.id, key: tile.key, special: tile.special || null, relic: tile.relic || null })));
    this.nextId = Math.max(this.nextId, ...[...ids].filter(id => typeof id === 'number').map(id => id + 1));
    return this.snapshot();
  }

  _inside(p) { return p && Number.isInteger(p.r) && Number.isInteger(p.c) && p.r >= 0 && p.c >= 0 && p.r < this.size && p.c < this.size; }
  _swap(a, b) { [this.grid[a.r][a.c], this.grid[b.r][b.c]] = [this.grid[b.r][b.c], this.grid[a.r][a.c]]; }

  _matches() {
    const lines = [];
    for (const dir of ['h', 'v']) for (let outer = 0; outer < this.size; outer++) {
      let start = 0;
      while (start < this.size) {
        const point = i => dir === 'h' ? { r: outer, c: i } : { r: i, c: outer };
        const p = point(start);
        const key = this.grid[p.r][p.c]?.key;
        let end = start + 1;
        while (key && end < this.size) {
          const q = point(end);
          if (this.grid[q.r][q.c]?.key !== key) break;
          end++;
        }
        if (key && end - start >= 3) lines.push({ key, dir, cells: Array.from({ length: end - start }, (_, i) => point(start + i)) });
        start = end;
      }
    }
    // Merge intersecting runs before counting, so T/L shapes summon only once per tile.
    const groups = [];
    for (const line of lines) {
      let merged = { key: line.key, cells: new Map(line.cells.map(p => [at(p), p])), lines: [line] };
      for (let i = groups.length - 1; i >= 0; i--) {
        const group = groups[i];
        if (group.key === line.key && [...merged.cells.keys()].some(key => group.cells.has(key))) {
          for (const [key, p] of group.cells) merged.cells.set(key, p);
          merged.lines.push(...group.lines);
          groups.splice(i, 1);
        }
      }
      groups.push(merged);
    }
    return groups.map(group => ({ ...group, cells: [...group.cells.values()] }));
  }

  _productive(a, b) {
    if (this.grid[a.r][a.c].special || this.grid[b.r][b.c].special) return true;
    this._swap(a, b);
    const valid = this._matches().some(group => group.cells.some(p => at(p) === at(a) || at(p) === at(b)));
    this._swap(a, b);
    return valid;
  }

  legalMoves() {
    const moves = [];
    for (let r = 0; r < this.size; r++) for (let c = 0; c < this.size; c++) {
      const from = { r, c };
      for (const to of [{ r, c: c + 1 }, { r: r + 1, c }]) {
        if (this._inside(to) && this._productive(from, to)) moves.push({ from, to });
      }
    }
    return moves;
  }

  hint() { return this.legalMoves()[0] || null; }

  /** Reorders existing tiles first, preserving relics/specials. A safe fresh fill is the bounded fallback. */
  shuffle() {
    const tiles = this.grid.flat().map(cloneTile);
    for (let attempt = 0; attempt < 80; attempt++) {
      for (let i = tiles.length - 1; i > 0; i--) {
        const j = this._random(i + 1);
        [tiles[i], tiles[j]] = [tiles[j], tiles[i]];
      }
      this.grid = Array.from({ length: this.size }, (_, r) => tiles.slice(r * this.size, (r + 1) * this.size));
      if (!this._matches().length && this.hint()) return this.snapshot();
    }
    this._fill(true);
    return this.snapshot();
  }

  trySwap(from, to) {
    const result = { valid: false, steps: [], reshuffled: false };
    if (!this._inside(from) || !this._inside(to) || Math.abs(from.r - to.r) + Math.abs(from.c - to.c) !== 1) return result;
    if (!this._productive(from, to)) return result;
    result.valid = true;
    this._swap(from, to);
    const a = this.grid[from.r][from.c], b = this.grid[to.r][to.c];
    let extra = [];
    const wildTargets = new Map();
    if (a.special || b.special) {
      extra = [from, to].filter(p => this.grid[p.r][p.c].special);
      if (a.special === 'wild' || b.special === 'wild') {
        const all = a.special === 'wild' && b.special === 'wild';
        const target = a.special === 'wild' ? b.key : a.key;
        for (const tile of [a, b]) if (tile.special === 'wild') wildTargets.set(tile.id, all ? null : target);
        for (let r = 0; r < this.size; r++) for (let c = 0; c < this.size; c++) {
          if (all || this.grid[r][c].key === target) extra.push({ r, c });
        }
      }
    }
    for (let chain = 1; chain <= 24; chain++) {
      const matches = this._matches();
      if (!matches.length && !extra.length) break;
      const step = this._clear(matches, extra, chain, chain === 1 ? [to, from] : [], chain === 1 ? wildTargets : new Map());
      result.steps.push(step);
      extra = [];
    }
    if (this._matches().length || !this.hint()) { this.shuffle(); result.reshuffled = true; }
    return result;
  }

  _clear(matches, extra, chain, preferred, wildTargets = new Map()) {
    const before = this.snapshot();
    const marked = new Map(extra.map(p => [at(p), p]));
    const created = [];
    for (const group of matches) {
      for (const p of group.cells) marked.set(at(p), p);
      const crossing = group.lines.some(line => line.dir === 'h') && group.lines.some(line => line.dir === 'v');
      const long = group.lines.some(line => line.cells.length >= 5);
      const special = crossing ? 'nova' : long ? 'wild' : group.cells.length >= 4 ? (group.lines[0].dir === 'h' ? 'row' : 'column') : null;
      if (special) {
        const normal = group.cells.filter(p => !this.grid[p.r][p.c].special);
        const anchor = preferred.find(p => normal.some(q => at(p) === at(q))) || normal[Math.floor(normal.length / 2)];
        if (anchor) created.push({ ...anchor, tile: { ...this.grid[anchor.r][anchor.c], special, relic: null } });
      }
    }
    // A queue makes special-to-special detonations finite, even when their areas overlap.
    const queue = [...marked.values()];
    const activated = new Set();
    for (let i = 0; i < queue.length; i++) {
      const p = queue[i], tile = this.grid[p.r][p.c];
      if (!tile.special || activated.has(tile.id)) continue;
      activated.add(tile.id);
      const add = (r, c) => {
        const q = { r, c }, key = at(q);
        if (this._inside(q) && !marked.has(key)) { marked.set(key, q); queue.push(q); }
      };
      if (tile.special === 'row') for (let c = 0; c < this.size; c++) add(p.r, c);
      if (tile.special === 'column') for (let r = 0; r < this.size; r++) add(r, p.c);
      if (tile.special === 'nova') for (let r = p.r - 1; r <= p.r + 1; r++) for (let c = p.c - 1; c <= p.c + 1; c++) add(r, c);
      if (tile.special === 'wild') {
        const key = wildTargets.has(tile.id) ? wildTargets.get(tile.id) : tile.key;
        for (let r = 0; r < this.size; r++) for (let c = 0; c < this.size; c++) if (key === null || this.grid[r][c].key === key) add(r, c);
      }
    }
    // Aggregate by element across the clear; every tile contributes to exactly one troop group.
    const byElement = new Map();
    for (const p of marked.values()) {
      const tile = this.grid[p.r][p.c];
      if (!byElement.has(tile.key)) byElement.set(tile.key, { key: tile.key, count: 0, cells: [], buffs: [], elite: chain > 1 });
      const group = byElement.get(tile.key);
      group.count++;
      group.cells.push({ ...p });
      if (tile.relic) group.buffs.push(tile.relic);
      if (tile.special) group.elite = true;
    }
    for (const group of byElement.values()) if (group.count >= 4) group.elite = true;
    const kept = new Set(created.map(at));
    const cleared = [];
    for (const p of marked.values()) if (!kept.has(at(p))) {
      cleared.push({ ...p, tile: cloneTile(this.grid[p.r][p.c]) });
      this.grid[p.r][p.c] = null;
    }
    for (const creation of created) this.grid[creation.r][creation.c] = cloneTile(creation.tile);
    for (let c = 0; c < this.size; c++) {
      const stack = [];
      for (let r = this.size - 1; r >= 0; r--) if (this.grid[r][c]) stack.push(this.grid[r][c]);
      for (let r = this.size - 1; r >= 0; r--) this.grid[r][c] = stack[this.size - 1 - r] || this._tile();
    }
    return { before, cleared, groups: [...byElement.values()], created, after: this.snapshot(), chain };
  }
}
