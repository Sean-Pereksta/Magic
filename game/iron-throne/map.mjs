import { HOUSES, TERRAINS } from './data.mjs';
import { PLAYER, kingdom, settlements, sizeOf, tileId } from './core.mjs';

const RADIUS = 25, SQRT3 = Math.sqrt(3);
export const hexPixel = t => ({ x: RADIUS * SQRT3 * (t.q + t.r / 2), y: RADIUS * 1.5 * t.r });
export function pixelHex(x, y) {
  const q = (SQRT3 / 3 * x - y / 3) / RADIUS, r = 2 / 3 * y / RADIUS;
  let rq = Math.round(q), rr = Math.round(r), rs = Math.round(-q - r);
  const dq = Math.abs(rq - q), dr = Math.abs(rr - r), ds = Math.abs(rs + q + r);
  if (dq > dr && dq > ds) rq = -rr - rs; else if (dr > ds) rr = -rq - rs;
  return tileId(rq, rr);
}
export class WorldMap {
  constructor(canvas, { getState, onSelect }) {
    this.canvas = canvas; this.ctx = canvas.getContext('2d'); this.getState = getState; this.onSelect = onSelect;
    this.zoom = 1; this.x = 0; this.y = 0; this.selected = '5,6'; this.armyId = null; this.pointers = new Map(); this.drag = null; this.moved = false; this.frame = null;
    this.observer = new ResizeObserver(() => this.resize()); this.observer.observe(canvas);
    canvas.addEventListener('pointerdown', e => {
      canvas.setPointerCapture(e.pointerId); this.pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
      this.drag = { x: e.clientX, y: e.clientY, startX: e.clientX, startY: e.clientY };
      this.moved = this.pointers.size > 1; this.pinch = this.pinchDistance();
    });
    canvas.addEventListener('pointermove', e => {
      if (!this.pointers.has(e.pointerId)) return;
      const old = this.pointers.get(e.pointerId); this.pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
      if (this.pointers.size === 2) {
        const d = this.pinchDistance(); if (this.pinch > 0) this.setZoom(this.zoom * d / this.pinch); this.pinch = d; this.moved = true;
      } else if (this.drag) {
        this.x -= (e.clientX - old.x) / this.zoom; this.y -= (e.clientY - old.y) / this.zoom;
        if (Math.hypot(e.clientX - this.drag.startX, e.clientY - this.drag.startY) > 6) this.moved = true;
        this.draw();
      }
    });
    const release = e => {
      if (!this.pointers.has(e.pointerId)) return;
      const click = e.type === 'pointerup' && !this.moved && this.pointers.size === 1;
      this.pointers.delete(e.pointerId);
      if (click) {
        const rect = canvas.getBoundingClientRect();
        const id = pixelHex((e.clientX - rect.left - this.width / 2) / this.zoom + this.x, (e.clientY - rect.top - this.height / 2) / this.zoom + this.y);
        if (this.getState().tiles[id]) this.onSelect(id);
      }
      if (!this.pointers.size) this.drag = null;
      else { const p = [...this.pointers.values()][0]; this.drag = { ...p, startX: p.x, startY: p.y }; this.moved = true; }
    };
    canvas.addEventListener('pointerup', release); canvas.addEventListener('pointercancel', release); canvas.addEventListener('lostpointercapture', release);
    canvas.addEventListener('wheel', e => { e.preventDefault(); this.setZoom(this.zoom * Math.exp(-e.deltaY * .001)); }, { passive: false });
    canvas.addEventListener('keydown', e => {
      const delta = { ArrowRight: [1, 0], ArrowLeft: [-1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1] }[e.key];
      if (delta) { e.preventDefault(); const s = this.getState(), t = s.tiles[this.selected], next = s.tiles[tileId(t.q + delta[0], t.r + delta[1])]; if (next) { this.selected = next.id; this.center(next.id); this.onSelect(next.id); } }
      if (e.key === 'Enter') { e.preventDefault(); this.onSelect(this.selected); }
      if (e.key === '+' || e.key === '=') this.setZoom(this.zoom * 1.2);
      if (e.key === '-') this.setZoom(this.zoom / 1.2);
      if (e.key === 'Home') { e.preventDefault(); this.home(); }
    });
    this.home();
  }
  pinchDistance() { const p = [...this.pointers.values()]; return p.length === 2 ? Math.hypot(p[0].x - p[1].x, p[0].y - p[1].y) : 0; }
  resize() {
    const rect = this.canvas.getBoundingClientRect(); this.width = rect.width; this.height = rect.height;
    this.dpr = Math.min(2, globalThis.devicePixelRatio || 1);
    this.canvas.width = Math.round(this.width * this.dpr); this.canvas.height = Math.round(this.height * this.dpr); this.draw();
  }
  setZoom(zoom) { this.zoom = Math.min(2.8, Math.max(.22, zoom)); this.draw(); }
  center(id) { const t = this.getState().tiles[id]; if (!t) return; const p = hexPixel(t); this.x = p.x; this.y = p.y; this.draw(); }
  home() { this.zoom = this.width < 600 ? .85 : 1.25; this.center(settlements(this.getState(), PLAYER)[0]?.id || '5,6'); }
  fit() { const bottom = hexPixel({ q: 39, r: 29 }); this.x = bottom.x / 2; this.y = bottom.y / 2; this.setZoom(Math.min(this.width / (bottom.x + 100), this.height / (bottom.y + 100))); }
  draw() { if (!this.frame) this.frame = requestAnimationFrame(() => { this.frame = null; this.render(); }); }
  hex(x, y, radius = RADIUS) {
    const c = this.ctx; c.beginPath();
    for (let i = 0; i < 6; i++) { const angle = (60 * i - 30) * Math.PI / 180; const px = x + Math.cos(angle) * radius, py = y + Math.sin(angle) * radius; if (i) c.lineTo(px, py); else c.moveTo(px, py); } c.closePath();
  }
  render() {
    if (!this.width || !this.height) return;
    const c = this.ctx, s = this.getState();
    c.setTransform(this.dpr, 0, 0, this.dpr, 0, 0); c.fillStyle = '#1d323f'; c.fillRect(0, 0, this.width, this.height);
    c.translate(this.width / 2, this.height / 2); c.scale(this.zoom, this.zoom); c.translate(-this.x, -this.y);
    const colors = Object.fromEntries(HOUSES.map(h => [h.id, h.color]));
    const visible = Object.values(s.tiles).filter(t => { const p = hexPixel(t); return Math.abs(p.x - this.x) < this.width / this.zoom / 2 + 60 && Math.abs(p.y - this.y) < this.height / this.zoom / 2 + 60; });
    for (const t of visible) {
      const p = hexPixel(t); this.hex(p.x, p.y);
      c.fillStyle = TERRAINS[t.terrain].color; c.fill(); c.strokeStyle = '#13262755'; c.lineWidth = .7; c.stroke();
      if (t.owner) { c.fillStyle = `${colors[t.owner]}30`; c.fill(); c.lineWidth = 1.4; c.strokeStyle = `${colors[t.owner]}95`; c.stroke(); }
      if (t.river && t.terrain !== 'water') { c.strokeStyle = '#76a4b8'; c.lineWidth = t.road ? 2 : 4; c.beginPath(); c.moveTo(p.x - 5, p.y - 21); c.bezierCurveTo(p.x + 10, p.y - 8, p.x - 10, p.y + 10, p.x + 5, p.y + 21); c.stroke(); }
      if (t.road) {
        c.strokeStyle = '#c8b9949c'; c.lineWidth = 2.2;
        for (const [dq, dr] of [[1, 0], [0, 1], [-1, 1]]) {
          const n = s.tiles[tileId(t.q + dq, t.r + dr)]; if (!n?.road) continue;
          const v = hexPixel(n); c.beginPath(); c.moveTo(p.x, p.y); c.lineTo(v.x, v.y); c.stroke();
        }
      }
      if (this.zoom > .4) this.terrain(t, p.x, p.y);
    }
    const a = s.armies.find(a => a.id === this.armyId);
    if (a?.path.length) {
      c.beginPath(); const from = hexPixel(s.tiles[a.tile]); c.moveTo(from.x, from.y);
      for (const id of a.path) { const p = hexPixel(s.tiles[id]); c.lineTo(p.x, p.y); }
      c.setLineDash([5, 4]); c.lineWidth = 2.5; c.strokeStyle = '#ffe0a0'; c.stroke(); c.setLineDash([]);
      const end = hexPixel(s.tiles[a.path.at(-1)]); c.strokeRect(end.x - 6, end.y - 6, 12, 12);
    }
    for (const t of visible) {
      const p = hexPixel(t);
      if (t.building) this.building(t, p.x, p.y, colors[t.owner] || '#ddd');
      if (t.project) { c.strokeStyle = '#ffdf98'; c.lineWidth = 1.5; c.setLineDash([3, 3]); this.hex(p.x, p.y, 20); c.stroke(); c.setLineDash([]); }
    }
    const grouped = new Map();
    for (const army of s.armies) { const key = `${army.tile}:${army.owner}`; if (!grouped.has(key)) grouped.set(key, []); grouped.get(key).push(army); }
    for (const group of grouped.values()) {
      const army = group[0], p = hexPixel(s.tiles[army.tile]), selected = group.some(a => a.id === this.armyId);
      const total = group.reduce((n, a) => n + sizeOf(a), 0);
      c.fillStyle = '#111d24'; c.strokeStyle = selected ? '#fff0b5' : colors[army.owner]; c.lineWidth = selected ? 2.5 : 1.7;
      c.beginPath(); c.roundRect(p.x - 16, p.y + 2, 32, 20, 3); c.fill(); c.stroke();
      c.fillStyle = colors[army.owner]; c.font = 'bold 11px system-ui'; c.textAlign = 'center'; c.fillText(`⚑${total}`, p.x, p.y + 16);
    }
    const selected = s.tiles[this.selected];
    if (selected) { const p = hexPixel(selected); this.hex(p.x, p.y, 24); c.lineWidth = 2.5; c.strokeStyle = '#ffe9af'; c.stroke(); }
    for (const t of visible.filter(t => ['city', 'town'].includes(t.building))) {
      if (this.zoom < .55 && !t.capital) continue;
      const p = hexPixel(t), label = t.name || 'Town';
      c.font = t.capital ? 'bold 11px Georgia' : '10px Georgia'; c.textAlign = 'center';
      const w = c.measureText(label).width; c.fillStyle = '#14222bea'; c.fillRect(p.x - w / 2 - 5, p.y - 38, w + 10, 15);
      c.fillStyle = colors[t.owner] || '#f3e3c3'; c.fillText(label, p.x, p.y - 27);
    }
    c.setTransform(this.dpr, 0, 0, this.dpr, 0, 0);
    const gradient = c.createRadialGradient(this.width / 2, this.height / 2, this.height * .3, this.width / 2, this.height / 2, Math.max(this.width, this.height) * .7);
    gradient.addColorStop(0, '#061b2400'); gradient.addColorStop(1, '#06111a66'); c.fillStyle = gradient; c.fillRect(0, 0, this.width, this.height);
  }
  terrain(t, x, y) {
    const c = this.ctx;
    if (t.terrain === 'forest') {
      for (const [dx, dy] of [[-8, 0], [6, 4], [0, -6]]) { c.fillStyle = '#1f4136'; c.beginPath(); c.moveTo(x + dx, y + dy - 12); c.lineTo(x + dx - 7, y + dy + 3); c.lineTo(x + dx + 7, y + dy + 3); c.fill(); c.strokeStyle = '#a8bea52b'; c.lineWidth = 1; c.stroke(); }
    } else if (['mountain', 'hills'].includes(t.terrain)) {
      c.fillStyle = t.terrain === 'mountain' ? '#46545c' : '#646b50'; c.beginPath(); c.moveTo(x - 16, y + 9); c.lineTo(x - 2, y - (t.terrain === 'mountain' ? 18 : 7)); c.lineTo(x + 14, y + 9); c.fill();
      if (t.terrain === 'mountain') { c.fillStyle = '#c6c8b9'; c.beginPath(); c.moveTo(x - 2, y - 18); c.lineTo(x - 7, y - 8); c.lineTo(x, y - 10); c.lineTo(x + 5, y - 6); c.fill(); }
    } else if (t.terrain === 'water') { c.strokeStyle = '#688d9b35'; c.lineWidth = 1; c.beginPath(); c.moveTo(x - 10, y); c.quadraticCurveTo(x, y + 5, x + 10, y); c.stroke(); }
    if (t.resource && !t.building && this.zoom > .65) { c.fillStyle = t.resource === 'iron' ? '#bccbd0' : t.resource === 'food' ? '#c8c979' : '#c1bb9b'; c.font = '11px Georgia'; c.textAlign = 'center'; c.fillText({ iron: '⚒', stone: '◆', food: 'ˇˇˇ', wood: '' }[t.resource], x, y + 14); }
  }
  building(t, x, y, color) {
    const c = this.ctx;
    if (['city', 'town', 'fort'].includes(t.building)) {
      c.fillStyle = '#202a2b'; c.fillRect(x - 11, y - 9, 22, 13);
      c.fillStyle = '#b6b09a'; c.fillRect(x - 9, y - 11, 18, 13);
      c.fillStyle = '#dfd2ac'; c.fillRect(x - 12, y - 16, 6, 19); c.fillRect(x + 6, y - 16, 6, 19);
      c.fillStyle = '#576565'; c.fillRect(x - 3, y - 4, 6, 8);
      c.fillStyle = color; c.beginPath(); c.moveTo(x - 1, y - 21); c.lineTo(x + 10, y - 18); c.lineTo(x - 1, y - 15); c.fill();
      c.strokeStyle = '#e4d8b8'; c.beginPath(); c.moveTo(x - 1, y - 23); c.lineTo(x - 1, y - 10); c.stroke();
      if (t.walls > 0) { c.strokeStyle = '#dddbcb'; c.lineWidth = 3; c.strokeRect(x - 16, y - 15, 32, 23); }
    } else if (this.zoom > .6) {
      c.fillStyle = '#e6d8aa'; c.font = '16px Georgia'; c.textAlign = 'center'; c.shadowColor = '#17251e'; c.shadowBlur = 3;
      c.fillText({ farm: '♧', lumber: '▥', quarry: '◆', mine: '⚒' }[t.building] || '', x, y + 2); c.shadowBlur = 0;
    }
  }
}
