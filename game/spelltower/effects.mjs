const COLORS = { fire: '#ffac62', water: '#8ce7f6', nature: '#b4ec87', lightning: '#fff1a0', ice: '#dcfaff', earth: '#e5c798', wind: '#d6f4e7', arcane: '#d6bcff', shadow: '#bca7dd' };

/** Short-lived local presentation. Simulation never depends on these particles. */
export class SpellEffects {
  constructor(canvas, board) {
    this.canvas = canvas; this.board = board; this.ctx = canvas.getContext('2d');
    this.particles = []; this.frame = 0; this.audio = null; this.sound = false;
    this.reduced = globalThis.matchMedia?.('(prefers-reduced-motion: reduce)').matches || false;
    this.resize = () => {
      const box = canvas.getBoundingClientRect(), ratio = Math.min(devicePixelRatio || 1, 2);
      canvas.width = Math.round(box.width * ratio); canvas.height = Math.round(box.height * ratio);
      this.width = box.width; this.height = box.height; this.ctx?.setTransform(ratio, 0, 0, ratio, 0, 0);
    };
    this.observer = new ResizeObserver(this.resize); this.observer.observe(canvas);
  }
  point(x, y) {
    const tile = this.board.querySelector(`[data-x="${x}"][data-y="${y}"]`);
    if (!tile) return { x: this.width / 2, y: this.height / 2 };
    const r = tile.getBoundingClientRect(), c = this.canvas.getBoundingClientRect();
    return { x: r.x - c.x + r.width / 2, y: r.y - c.y + r.height / 2 };
  }
  enableSound(enabled) {
    this.sound = enabled;
    if (enabled) {
      const Context = globalThis.AudioContext || globalThis.webkitAudioContext;
      if (Context) this.audio ||= new Context();
      this.audio?.resume().catch(() => {});
    }
  }
  chime(element, combo = false) {
    if (!this.sound || !this.audio || this.audio.state !== 'running') return;
    const ac = this.audio, at = ac.currentTime;
    const note = { fire: 196, water: 440, nature: 329.6, lightning: 659.3, ice: 880, earth: 130.8, wind: 523.3 }[element] || 440;
    for (let i = 0; i < (combo ? 3 : 1); i++) {
      const oscillator = ac.createOscillator(), volume = ac.createGain();
      oscillator.type = element === 'lightning' ? 'triangle' : 'sine';
      oscillator.frequency.setValueAtTime(note * [1, 1.25, 1.5][i], at + i * .045);
      oscillator.frequency.exponentialRampToValueAtTime(note * 1.1, at + .18 + i * .045);
      volume.gain.setValueAtTime(.0001, at); volume.gain.exponentialRampToValueAtTime(.045, at + .018 + i * .045);
      volume.gain.exponentialRampToValueAtTime(.0001, at + .32 + i * .045);
      oscillator.connect(volume); volume.connect(ac.destination);
      oscillator.start(at + i * .045); oscillator.stop(at + .36 + i * .045);
    }
  }
  play(events = []) {
    if (!this.ctx) return;
    const time = performance.now();
    const cast = events.find(event => event.type === 'cast');
    if (cast) this.chime(cast.element, events.some(event => event.type === 'combo'));
    const presented = events.filter(event => event.type !== 'cast');
    if (cast) {
      for (const cell of (cast.cells || [cast]).slice(0, 36)) presented.unshift({ ...cast, x: cell.x, y: cell.y });
    }
    for (const event of presented.slice(-80)) {
      if (!Number.isFinite(event.x) || !Number.isFinite(event.y) || event.x < 0 || event.y < 0) continue;
      const p = this.point(event.x, event.y), element = event.element || 'arcane', color = COLORS[element] || COLORS.arcane;
      if (['cast', 'combo', 'reaction', 'damage', 'kill', 'death', 'tower', 'heal', 'freeze', 'push', 'move'].includes(event.type)) {
        if (event.amount || ['combo', 'reaction'].includes(event.type)) this.particles.push({ ...p, type: 'text', text: ['combo', 'reaction', 'move'].includes(event.type) ? event.text : event.amount ? `${event.type === 'heal' ? '+' : '−'}${event.amount}` : event.text, color, at: time, life: this.reduced ? 380 : 1000 });
        if (this.reduced) { this.particles.push({ ...p, type: 'ring', color, at: time, life: 220 }); continue; }
        if (element === 'lightning') this.particles.push({ ...p, type: 'bolt', color, at: time, life: 350, points: Array.from({ length: 9 }, (_, i) => ({ x: p.x + (Math.random() - .5) * (i === 8 ? 0 : 32), y: p.y - 100 + i * 12.5 })) });
        this.particles.push({ ...p, type: element === 'wind' ? 'wind' : 'ring', color, at: time, life: 550 });
        const count = event.type === 'cast' || event.type === 'combo' ? 13 : 5;
        for (let i = 0; i < count; i++) {
          const a = Math.PI * 2 * i / count, speed = 18 + Math.random() * 45;
          this.particles.push({ ...p, type: element === 'ice' ? 'shard' : element === 'nature' ? 'leaf' : element === 'earth' ? 'rock' : 'spark', color, at: time, life: 450 + Math.random() * 400, vx: Math.cos(a) * speed, vy: Math.sin(a) * speed - (element === 'fire' ? 35 : 10), size: 2 + Math.random() * 3, a });
        }
      }
    }
    this.particles = this.particles.slice(-360);
    if (!this.frame && this.particles.length) this.frame = requestAnimationFrame(now => this.draw(now));
  }
  draw(now) {
    this.frame = 0;
    const ctx = this.ctx; ctx.clearRect(0, 0, this.width, this.height);
    this.particles = this.particles.filter(p => now - p.at < p.life);
    for (const p of this.particles) {
      const t = Math.max(0, (now - p.at) / p.life), seconds = (now - p.at) / 1000;
      ctx.save(); ctx.globalAlpha = (1 - t) * .95; ctx.strokeStyle = p.color; ctx.fillStyle = p.color;
      if (p.type === 'text') {
        ctx.font = 'bold 14px Trebuchet MS, sans-serif'; ctx.textAlign = 'center'; ctx.lineWidth = 3; ctx.strokeStyle = '#16332b';
        const text = String(p.text || '').slice(0, 32), y = p.y - 10 - (this.reduced ? 0 : t * 30);
        ctx.strokeText(text, p.x, y); ctx.fillText(text, p.x, y);
      } else if (p.type === 'bolt') {
        ctx.lineWidth = 2.5; ctx.shadowColor = p.color; ctx.shadowBlur = 9; ctx.beginPath();
        p.points.forEach((point, i) => i ? ctx.lineTo(point.x, point.y) : ctx.moveTo(point.x, point.y)); ctx.stroke();
      } else if (p.type === 'ring' || p.type === 'wind') {
        const radius = this.reduced ? 18 : 8 + t * 35; ctx.lineWidth = 2 * (1 - t) + .5;
        ctx.beginPath(); ctx.ellipse(p.x, p.y, radius, radius * .55, p.type === 'wind' ? t * 4 : 0, 0, p.type === 'wind' ? Math.PI * 1.5 : Math.PI * 2); ctx.stroke();
      } else {
        ctx.translate(p.x + p.vx * seconds, p.y + p.vy * seconds + seconds * seconds * 20); ctx.rotate(p.a + t * 3);
        const size = p.size * (1 - t * .5);
        if (p.type === 'shard') { ctx.beginPath(); ctx.moveTo(0, -size * 2); ctx.lineTo(size, 0); ctx.lineTo(0, size * 2); ctx.lineTo(-size, 0); ctx.closePath(); ctx.fill(); }
        else if (p.type === 'leaf') { ctx.beginPath(); ctx.ellipse(0, 0, size * 2, size, 0, 0, Math.PI * 2); ctx.fill(); }
        else if (p.type === 'rock') ctx.fillRect(-size, -size, size * 2, size * 2);
        else { ctx.beginPath(); ctx.arc(0, 0, size, 0, Math.PI * 2); ctx.fill(); }
      }
      ctx.restore();
    }
    if (this.particles.length) this.frame = requestAnimationFrame(time => this.draw(time));
  }
  clear() { cancelAnimationFrame(this.frame); this.frame = 0; this.particles = []; this.ctx?.clearRect(0, 0, this.width, this.height); }
  destroy() { this.clear(); this.observer.disconnect(); this.audio?.close().catch(() => {}); }
}
