import { HOUSES } from './data.mjs';
import { localHouseId } from './house-control.mjs';
const color = id => HOUSES.find(h => h.id === id)?.color || '#eee2c4';
const name = id => HOUSES.find(h => h.id === id)?.name.replace('House ', '') || id;
const eventKey = (e, index) => e.id || `${e.turn}:${e.attacker}:${e.defender}:${e.tile}:${e.action}:${index}`;
export function eventTroopLosses(e) {
  if(e.action==='battle'&&e.before?.length===2&&e.after?.length===2)
    return e.before.map((n,i)=>Math.max(0,n-e.after[i]));
  // Siege before/after contains wall strength, never treat that as soldiers.
  if(e.troopLosses?.length===2)return e.troopLosses.map(n=>Math.max(0,n));
  return null;
}

// Presentation consumes completed simulation events. It never rolls combat,
// changes unit positions, or blocks input. Each clash lives for two seconds.
export class BattleEffects {
  constructor() { this.localHouseId='ashen'; this.seen = new Set(); this.active = []; this.results = []; this.seed = null; this.turn = 0; }
  ingest(s, now) {
    this.localHouseId=localHouseId(s);
    const events = s.militaryEvents.slice(-100);
    if (this.seed !== s.seed || s.turn < this.turn) {
      this.seed = s.seed; this.seen = new Set(events.map(eventKey)); this.active = []; this.results = [];
    } else {
      const fresh = events.filter((e, i) => !this.seen.has(eventKey(e, i)) && e.turn >= s.turn - 1).slice(-6);
      for (const e of fresh) {
        const seed = Number(e.id) || e.turn * 13;
        const particles = Array.from({ length: 12 }, (_, i) => ({ angle: (seed * .7 + i * 2.4) % (Math.PI * 2), speed: 5 + (i * 7 % 18), size: 1 + i % 3, kind: i % 4 }));
        this.active.push({ event: e, start: now, particles }); this.results.push({ event: e, expires: now + 5500 });
      }
    }
    this.turn = s.turn; this.seen = new Set(events.map(eventKey));
    this.active = this.active.filter(e => now - e.start < 2000).slice(-6);
    this.results = this.results.filter(e => e.expires > now).slice(-3);
  }
  drawWorld(c, s, pixel, inView, now, reduced, zoom) {
    const rows=new Map();
    for (const effect of this.active) {
      const e = effect.event, t = s.tiles[e.tile], p = t && pixel(t);
      if (!p || !inView(p)) continue;
      const progress = Math.min(1, (now - effect.start) / 2000);
      const row=rows.get(e.tile)||0;rows.set(e.tile,row+1);
      c.save(); c.translate(p.x, p.y);
      if (reduced || zoom < .45) {
        c.strokeStyle = '#f1af74'; c.lineWidth = 2; c.beginPath(); c.arc(0, 0, 22, 0, Math.PI * 2); c.stroke(); this.drawLosses(c,e,progress,zoom,reduced,row);c.restore(); continue;
      }
      const impact = Math.sin(Math.min(1, progress * 3) * Math.PI / 2), retreat = progress > .7 && e.retreat ? (progress - .7) * 40 : 0;
      // Opposed ranks close, collide, and recoil; their banners retain House colors.
      for (let side = -1; side <= 1; side += 2) {
        c.fillStyle = color(side === -1 ? e.attacker : e.defender);
        const x = side * (24 - impact * 15 + (side === 1 ? retreat : 0));
        for (let i = 0; i < 4; i++) {
          c.globalAlpha = progress > .55 && i === 3 ? Math.max(0, 1 - (progress - .55) * 3) : .9;
          c.beginPath(); c.arc(x, i * 5 - 8, 2, 0, Math.PI * 2); c.fill(); c.fillRect(x - 2, i * 5 - 6, 4, 4);
        }
        c.globalAlpha = 1; c.strokeStyle = '#efe4c2'; c.lineWidth = 1;
        c.beginPath(); c.moveTo(x - 1, -13); c.lineTo(x - 1, -30); c.stroke();
        c.beginPath(); c.moveTo(x, -29); c.lineTo(x + side * (9 + Math.sin(progress * 42) * 1.3), -27); c.lineTo(x, -22); c.fill();
      }
      const siege = e.action === 'siege' || e.action === 'capture' || e.action === 'structure';
      const arrows = siege || (e.composition||[]).some(a=>(a.archer||0)+(a.veteranArcher||0)+(a.crossbow||0)>0);
      for (const particle of effect.particles) {
        const drift = progress * particle.speed;
        c.globalAlpha = (1 - progress) * .7;
        c.fillStyle = particle.kind === 0 ? '#efca8b' : particle.kind === 1 ? '#d78150' : siege ? '#736b64' : '#c2b295';
        const x = Math.cos(particle.angle) * drift, y = Math.sin(particle.angle) * drift - (siege ? progress * 19 : 0);
        c.beginPath(); c.arc(x, y, particle.size + progress * 3, 0, Math.PI * 2); c.fill();
      }
      c.globalAlpha = Math.max(0, 1 - progress);
      // Bounded arrows / siege projectile arcs and brief weapon flashes.
      for (let i = 0; i < (siege ? 2 : arrows ? 4 : 0); i++) {
        const flight = (progress * 2 + i * .22) % 1;
        c.strokeStyle = '#f4d49a'; c.lineWidth = siege ? 2 : 1;
        c.beginPath(); c.moveTo(-22 + flight * 44, -5 - Math.sin(flight * Math.PI) * (siege ? 35 : 17)); c.lineTo(-18 + flight * 44, -7 - Math.sin(flight * Math.PI) * (siege ? 35 : 17)); c.stroke();
      }
      if (progress > .2 && progress < .65) {
        c.strokeStyle = '#ffe8b6'; c.lineWidth = 1.6;
        c.beginPath(); c.moveTo(-5, -7); c.lineTo(5, 4); c.moveTo(5, -7); c.lineTo(-5, 4); c.stroke();
        if (siege) { c.strokeStyle = '#3f3530'; c.lineWidth = 2; c.beginPath(); c.moveTo(3, -18); c.lineTo(-2, -10); c.lineTo(3, -7); c.lineTo(0, 3); c.stroke(); }
      }
      this.drawLosses(c,e,progress,zoom,reduced,row);
      c.restore();
    }
  }
  drawLosses(c,e,progress,zoom,reduced,row) {
    const losses=eventTroopLosses(e);if(!losses)return;
    c.save();c.scale(Math.max(1,.85/zoom),Math.max(1,.85/zoom));
    c.globalAlpha=Math.max(0,1-progress);c.font='bold 12px system-ui';c.textAlign='center';
    c.lineJoin='round';c.lineWidth=4;c.strokeStyle='#081923';
    const y=-43-(reduced?0:progress*25)-row*34;
    [e.attacker,e.defender].forEach((owner,i)=>{
      const label=`${owner===this.localHouseId?'You':name(owner)} −${losses[i]}`;
      c.fillStyle=color(owner);c.strokeText(label,0,y+i*15);c.fillText(label,0,y+i*15);
    });
    c.restore();
  }
  drawResults(c, s, width, height, now) {
    const result = this.results.at(-1); if (!result) return;
    const e = result.event, tile = s.tiles[e.tile], boxWidth = Math.min(320, width - 24), y = Math.max(65, height - 136);
    c.save(); c.translate(12, y); c.fillStyle = '#112532f2'; c.strokeStyle = color(e.attacker); c.lineWidth = 1;
    c.beginPath(); c.roundRect(0, 0, boxWidth, 101, 5); c.fill(); c.stroke();
    c.textAlign = 'left'; c.fillStyle = '#f0d8a3'; c.font = 'bold 10px system-ui';
    const fit = (text, max = boxWidth - 20) => { while (c.measureText(text).width > max && text.length) text = text.slice(0, -1); return text; };
    c.fillText(fit(`${e.action === 'structure' ? 'STRUCTURE ATTACK' : e.action === 'siege' ? 'SIEGE' : 'BATTLE'} OF ${(tile?.name || e.tile).toUpperCase()}`), 10, 18);
    c.font = '11px system-ui'; c.fillStyle = '#eceddf';
    c.fillText(fit(e.winner ? `${name(e.winner)} ${e.action === 'capture' ? 'takes the settlement' : 'wins the clash'}` : e.action === 'structure' ? (e.destroyed ? 'Structure destroyed' : 'Structure damaged') : 'The siege continues'), 10, 37);
    if (e.before && e.after) {
      c.fillText(fit(`${name(e.attacker)}: ${e.before[0]} → ${e.after[0]}`), 10, 54);
      c.fillText(fit(`${e.action === 'siege' ? 'Walls' : name(e.defender)}: ${e.before[1]} → ${e.after[1]}`), 10, 70);
    }
    c.fillStyle = '#b1bec0'; c.font = '10px system-ui';
    c.fillText(fit(e.retreat ? `${name(e.retreatOwner||e.defender)} retreats to ${s.tiles[e.retreat]?.name || e.retreat}.` : this.results.length > 1 ? `${this.results.length} recent encounters · full report in Realm` : 'Orders are available.'), 10, 88);
    c.restore();
  }
  animating(now, reduced) { return this.active.some(e => now - e.start < 2000&&(!reduced||eventTroopLosses(e.event))); }
}
