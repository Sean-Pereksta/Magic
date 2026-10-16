import { TILE, UNITS, armySize, clamp, hash } from "./data.mjs";
import { battleMap, formationSlots } from "./battle.mjs";
export class Renderer {
  constructor(canvas, mini, assets) {
    this.canvas = canvas;
    this.ctx = canvas.getContext("2d", { alpha: false, desynchronized: true });
    this.mini = mini;
    this.mctx = mini.getContext("2d", { alpha: false });
    this.assets = assets;
    this.cam = { x: 15, y: 44, zoom: 1 };
    this.quality = "auto";
    this.chunks = new Map();
    this.selected = new Set();
    this.preview = null;
    this.marquee = null;
    this.follow = true;
    this.frameMs = 16.7;
    this.low = false;
    this.lastMini = 0;
    this.width = 0;
    this.height = 0;
    this.resize();
  }
  resize() {
    const r = this.canvas.getBoundingClientRect();
    this.width = r.width;
    this.height = r.height;
    const mobile = matchMedia("(pointer:coarse)").matches;
    this.dpr =
      this.quality === "low" || mobile
        ? 1
        : this.quality === "high"
          ? Math.min(2, devicePixelRatio || 1)
          : Math.min(1.5, devicePixelRatio || 1);
    this.canvas.width = Math.round(r.width * this.dpr);
    this.canvas.height = Math.round(r.height * this.dpr);
    this.cam.zoom = clamp(this.cam.zoom, 0.35, 1.8);
  }
  toWorld(x, y) {
    return {
      x: (x - this.width / 2) / (TILE * this.cam.zoom) + this.cam.x,
      y: (y - this.height / 2) / (TILE * this.cam.zoom) + this.cam.y,
    };
  }
  toScreen(x, y) {
    return {
      x: (x - this.cam.x) * TILE * this.cam.zoom + this.width / 2,
      y: (y - this.cam.y) * TILE * this.cam.zoom + this.height / 2,
    };
  }
  visible(x, y, pad = 3) {
    return (
      Math.abs(x - this.cam.x) <
        this.width / (TILE * this.cam.zoom * 2) + pad &&
      Math.abs(y - this.cam.y) < this.height / (TILE * this.cam.zoom * 2) + pad
    );
  }
  atlas(index, frame, x, y, w, h) {
    const c = this.ctx,
      a = this.assets.manifest.atlases[index],
      inset = 2,
      col = frame % a.columns,
      row = Math.floor(frame / a.columns);
    c.drawImage(
      this.assets.images[index],
      col * a.frameWidth + inset,
      row * a.frameHeight + inset,
      a.frameWidth - inset * 2,
      a.frameHeight - inset * 2,
      x,
      y,
      w,
      h,
    );
  }
  sprite(key, state, x, y, time, facing = 1, size = 1) {
    const c = this.ctx,
      t = UNITS[key] || UNITS.recruits,
      sequence = this.assets.manifest.animation[state] || [0],
      frame =
        sequence[
          Math.floor(time * (state === "move" ? 7 : 4)) % sequence.length
        ],
      sz = 66 * size,
      breath =
        state === "idle" || state === "defend"
          ? Math.sin(time * 2.2) * 0.55
          : 0;
    c.save();
    c.translate(x * TILE, y * TILE + breath);
    c.scale(facing, 1);
    this.atlas(t.sheet, t.row * 8 + frame, -sz / 2, -sz * 0.84, sz, sz);
    c.restore();
  }
  chunk(map, cx, cy) {
    const key = `${map.seed}:${map.siege || false}:${cx}:${cy}:${map.version}`,
      hit = this.chunks.get(key);
    if (hit) return hit;
    const s = 12,
      canvas = document.createElement("canvas");
    canvas.width = s * TILE;
    canvas.height = s * TILE;
    const c = canvas.getContext("2d", { alpha: false });
    c.fillStyle = "#485132";
    c.fillRect(0, 0, canvas.width, canvas.height);
    const a = this.assets.manifest.atlases[2];
    for (let y = 0; y < s; y++)
      for (let x = 0; x < s; x++) {
        const wx = cx * s + x,
          wy = cy * s + y;
        if (wx >= map.width || wy >= map.height) continue;
        const frame = map.tiles[wy * map.width + wx];
        c.drawImage(
          this.assets.images[2],
          (frame % 4) * a.frameWidth + 2,
          Math.floor(frame / 4) * a.frameHeight + 2,
          a.frameWidth - 4,
          a.frameHeight - 4,
          x * TILE,
          y * TILE,
          TILE,
          TILE,
        );
      }
    if (this.chunks.size > 20) {
      const first = this.chunks.keys().next().value;
      this.chunks.delete(first);
    }
    this.chunks.set(key, canvas);
    return canvas;
  }
  terrain(map) {
    const c = this.ctx,
      s = 12,
      left = Math.max(
        0,
        Math.floor((this.cam.x - this.width / TILE / this.cam.zoom / 2) / s),
      ),
      right = Math.min(
        Math.ceil(map.width / s),
        Math.ceil((this.cam.x + this.width / TILE / this.cam.zoom / 2) / s),
      ),
      top = Math.max(
        0,
        Math.floor((this.cam.y - this.height / TILE / this.cam.zoom / 2) / s),
      ),
      bottom = Math.min(
        Math.ceil(map.height / s),
        Math.ceil((this.cam.y + this.height / TILE / this.cam.zoom / 2) / s),
      );
    for (let cy = top; cy < bottom; cy++)
      for (let cx = left; cx < right; cx++)
        c.drawImage(this.chunk(map, cx, cy), cx * s * TILE, cy * s * TILE);
  }
  label(text, x, y, color = "#f4e8cf", small = false) {
    const c = this.ctx;
    c.font = `${small ? 10 : 12}px system-ui`;
    c.textAlign = "center";
    const w = c.measureText(text).width;
    c.fillStyle = "#101b19dc";
    c.fillRect(x * TILE - w / 2 - 6, y * TILE - 11, w + 12, 17);
    c.fillStyle = color;
    c.fillText(text, x * TILE, y * TILE + 2);
  }
  pennon(x, y, color = "#d5b26c", scale = 1) {
    const c = this.ctx;
    c.strokeStyle = "#e2c78b";
    c.lineWidth = 1.5;
    c.beginPath();
    c.moveTo(x * TILE, y * TILE);
    c.lineTo(x * TILE, y * TILE - 22 * scale);
    c.stroke();
    c.fillStyle = color;
    c.beginPath();
    c.moveTo(x * TILE, y * TILE - 22 * scale);
    c.lineTo(x * TILE + 14 * scale, y * TILE - 19 * scale);
    c.lineTo(x * TILE + 10 * scale, y * TILE - 13 * scale);
    c.lineTo(x * TILE, y * TILE - 15 * scale);
    c.fill();
  }
  path(points, color = "#ebca7e", time = 0) {
    if (points.length < 2) return;
    const c = this.ctx;
    c.strokeStyle = color;
    c.lineWidth = 2 / this.cam.zoom;
    c.setLineDash([9 / this.cam.zoom, 6 / this.cam.zoom]);
    c.lineDashOffset = -time * 15;
    c.beginPath();
    c.moveTo(points[0].x * TILE, points[0].y * TILE);
    for (let i = 1; i < points.length; i++)
      c.lineTo(points[i].x * TILE, points[i].y * TILE);
    c.stroke();
    c.setLineDash([]);
    const last = points.at(-1),
      prev = points.at(-2),
      angle = Math.atan2(last.y - prev.y, last.x - prev.x);
    c.save();
    c.translate(last.x * TILE, last.y * TILE);
    c.rotate(angle);
    c.fillStyle = color;
    c.beginPath();
    c.moveTo(9, 0);
    c.lineTo(-8, -6);
    c.lineTo(-4, 0);
    c.lineTo(-8, 6);
    c.fill();
    c.restore();
  }
  world(state, map, time) {
    const c = this.ctx,
      p = state.profile;
    this.terrain(map);
    if (p.route?.length > p.routeIndex)
      this.path([p, ...p.route.slice(p.routeIndex)], "#ebcc89", time);
    if (!this.low) {
      c.strokeStyle = "#a7d1d04a";
      c.lineWidth = 1;
      let strokes = 0;
      for (
        let y = Math.max(
          0,
          Math.floor(this.cam.y - this.height / (TILE * this.cam.zoom * 2)),
        );
        y <
        Math.min(
          map.height,
          this.cam.y + this.height / (TILE * this.cam.zoom * 2),
        );
        y++
      )
        for (
          let x = Math.max(
            0,
            Math.floor(this.cam.x - this.width / (TILE * this.cam.zoom * 2)),
          );
          x <
          Math.min(
            map.width,
            this.cam.x + this.width / (TILE * this.cam.zoom * 2),
          );
          x++
        )
          if (map.tiles[y * map.width + x] === 3 && strokes++ < 80) {
            const offset = (time * 9 + (hash(x, y) % 32)) % 38;
            c.beginPath();
            c.moveTo(x * TILE + 4, y * TILE + offset);
            c.lineTo(x * TILE + 23, y * TILE + offset + 2);
            c.stroke();
          }
    }
    const objects = [];
    for (const prop of map.props)
      if (this.visible(prop.x, prop.y)) objects.push({ ...prop, sort: prop.y });
    for (const s of state.settlements)
      if (this.visible(s.x, s.y))
        objects.push({
          ...s,
          type:
            s.kind === "village"
              ? 10
              : s.level > 2 || s.faction === "ember"
                ? 12
                : 11,
          sort: s.y + 0.4,
          settlement: true,
        });
    objects.sort((a, b) => a.sort - b.sort);
    for (const o of objects) {
      const size =
          o.type === 8
            ? 90
            : o.type === 9
              ? 115
              : o.kind === "castle"
                ? 110
                : 84,
        sway = o.type === 8 && !this.low ? Math.sin(time + o.x) * 1.5 : 0;
      this.atlas(
        2,
        o.type,
        o.x * TILE - size / 2 + sway,
        o.y * TILE - size * 0.8,
        size,
        size,
      );
      if (o.settlement) {
        const mine = o.owner === p.uid;
        this.label(o.name, o.x, o.y + 1, mine ? "#f1ce82" : "#f3e9d4");
        if (o.kind === "castle") {
          this.pennon(
            o.x + 0.5,
            o.y - 0.8,
            mine ? "#dabb76" : o.faction === "ember" ? "#d78367" : "#72b4d5",
          );
          this.label(
            o.ownerName || `${o.garrison} defenders`,
            o.x,
            o.y + 1.4,
            mine ? "#e5c47e" : "#c4cdbd",
            true,
          );
        }
      }
    }
    for (const n of state.npcs) {
      if (n.battleId || !this.visible(n.x, n.y)) continue;
      if (n.kind === "merchant")
        this.atlas(2, 14, n.x * TILE - 28, n.y * TILE - 35, 62, 62);
      else {
        this.sprite(
          n.kind === "bandit" ? "footmen" : "cavalry",
          n.renderMoving === false ? "idle" : "move",
          n.x,
          n.y,
          time,
          n.renderFacing || 1,
          0.85,
        );
        this.pennon(
          n.x - 0.4,
          n.y,
          n.kind === "bandit" ? "#d5765c" : "#8fc0c3",
          0.7,
        );
      }
      this.label(
        n.kind === "merchant"
          ? "Caravan"
          : `${n.kind === "bandit" ? "Outlaws" : "Patrol"} · ${armySize(n.army)}`,
        n.x,
        n.y + 0.8,
        n.kind === "bandit" ? "#f4b095" : "#c9d6c4",
        true,
      );
    }
    for (const o of state.others) {
      if (!this.visible(o.x, o.y)) continue;
      this.sprite(
        "cavalry",
        o.activeBattle ? "attack" : o.renderMoving === false ? "idle" : "move",
        o.x,
        o.y,
        time,
        o.renderFacing || 1,
        0.95,
      );
      this.label(
        o.username,
        o.x,
        o.y + 0.9,
        o.allianceId && o.allianceId === p.allianceId ? "#9bd6bc" : "#aac6dd",
      );
    }
    const moving = p.renderMoving ?? p.route?.length > p.routeIndex;
    const retinue = Object.keys(p.army).filter((k) => p.army[k] > 0);
    for (let i = Math.min(6, retinue.length * 2) - 1; i >= 0; i--) {
      const key = retinue[i % retinue.length],
        x = p.x - (i % 3) * 0.45 - 0.4,
        y = p.y + Math.floor(i / 3) * 0.45 - 0.6;
      this.sprite(
        key,
        moving ? "move" : "idle",
        x,
        y,
        time + (i % 3) * 0.12,
        p.renderFacing || 1,
        0.65,
      );
    }
    this.sprite(
      "cavalry",
      moving ? "move" : "idle",
      p.x,
      p.y,
      time,
      p.renderFacing || 1,
      1.05,
    );
    this.pennon(p.x + 0.55, p.y - 0.25, "#efd393");
    this.label(
      `${p.username} · ${armySize(p.army)}`,
      p.x,
      p.y + 1.1,
      "#f0d495",
    );
    if (!this.low) {
      c.fillStyle = `rgba(7,18,29,${0.03 + (Math.sin(time / 60) + 1) * 0.025})`;
      c.fillRect(0, 0, map.width * TILE, map.height * TILE);
    }
  }
  battle(b, time, alpha, team) {
    const c = this.ctx,
      map = battleMap(b);
    map.seed = b.seed;
    map.siege = b.siege;
    this.terrain(map);
    if (b.siege) {
      for (let y = 1; y < 40; y += 3) {
        if (y > 17 && y < 23) continue;
        this.atlas(2, 12, 42 * TILE - 42, y * TILE - 50, 84, 94);
      }
      if (b.gate?.hp > 0) {
        this.atlas(2, 11, 42 * TILE - 48, 20 * TILE - 72, 96, 96);
        c.fillStyle = "#1e2524";
        c.fillRect(42 * TILE - 34, 20 * TILE - 78, 68, 6);
        c.fillStyle = "#e6bc6c";
        c.fillRect(
          42 * TILE - 34,
          20 * TILE - 78,
          (68 * b.gate.hp) / b.gate.max,
          6,
        );
      }
    }
    const units = b.units
      .filter(
        (u) =>
          this.visible(u.x, u.y, 2) &&
          !u.routed &&
          (u.hp > 0 || b.time - u.deathAt < 6),
      )
      .sort((a, d) => a.y - d.y);
    for (const u of units) {
      const x = u.px + (u.x - u.px) * alpha,
        y = u.py + (u.y - u.py) * alpha;
      if (this.selected.has(u.id) && u.hp > 0) {
        c.strokeStyle = "#f4d38a";
        c.lineWidth = 2;
        c.beginPath();
        for (const [dx, dy] of [
          [-14, -5],
          [14, -5],
          [-14, 7],
          [14, 7],
        ]) {
          c.moveTo(x * TILE + dx, y * TILE + dy);
          c.lineTo(x * TILE + dx + Math.sign(-dx) * 6, y * TILE + dy);
        }
        c.stroke();
      }
      const state =
        u.hp <= 0 ? (b.time - u.deathAt > 0.4 ? "dead" : "dying") : u.state;
      this.sprite(
        u.key,
        state === "dead" ? "dying" : state,
        x,
        y,
        state === "dead" ? 1.75 : time + u.id * 0.075,
        u.facing,
        0.8,
      );
      if (u.hp > 0) {
        c.fillStyle = u.team === team ? "#75b3ce" : "#db8068";
        c.fillRect(x * TILE - 8, y * TILE + 8, 16, 2);
        if (u.hp < u.maxHp || this.selected.has(u.id)) {
          c.fillStyle = "#17221d";
          c.fillRect(x * TILE - 11, y * TILE - 32, 22, 3);
          c.fillStyle = u.team === team ? "#a7d7a5" : "#df9a7f";
          c.fillRect(x * TILE - 11, y * TILE - 32, (22 * u.hp) / u.maxHp, 3);
        }
      }
    }
    for (const p of b.projectiles) {
      c.save();
      c.translate(p.x * TILE, p.y * TILE - 15);
      c.rotate(Math.atan2(p.ty - p.y, p.tx - p.x));
      if (p.kind === "magic") {
        const g = c.createRadialGradient(0, 0, 0, 0, 0, 12);
        g.addColorStop(0, "#fff0ac");
        g.addColorStop(0.3, "#f5b34b");
        g.addColorStop(1, "#ed622300");
        c.fillStyle = g;
        c.fillRect(-12, -12, 24, 24);
      } else {
        c.strokeStyle = "#e9d6ac";
        c.lineWidth = 2;
        c.beginPath();
        c.moveTo(-10, 0);
        c.lineTo(7, 0);
        c.lineTo(3, -3);
        c.moveTo(7, 0);
        c.lineTo(3, 3);
        c.stroke();
      }
      c.restore();
    }
    for (const e of b.effects) {
      const age = b.time - e.time;
      if (e.kind === "order") {
        if (e.team === team) {
          this.pennon(e.x, e.y, "#f1d28a", 1 + Math.sin(age * 5) * 0.08);
        }
        continue;
      }
      const duration = 1.1;
      if (age > duration) continue;
      c.save();
      c.translate(e.x * TILE, e.y * TILE - 14);
      c.globalAlpha = 1 - age / duration;
      const heal = e.kind === "heal" || e.kind === "rally";
      c.strokeStyle = heal
        ? "#bfe8af"
        : e.kind === "fire" || e.kind === "firestorm"
          ? "#ffd383"
          : "#f1dfae";
      c.lineWidth = heal ? 3 : 2;
      for (let i = 0; i < (this.low ? 4 : 8); i++) {
        const angle = (i * Math.PI) / 4 + e.x;
        const distance = 5 + age * (e.kind === "firestorm" ? 100 : 28),
          x = Math.cos(angle) * distance,
          y = Math.sin(angle) * distance;
        if (heal) {
          c.beginPath();
          c.moveTo(x - 3, y - age * 15);
          c.lineTo(x + 3, y - age * 15);
          c.moveTo(x, y - 3 - age * 15);
          c.lineTo(x, y + 3 - age * 15);
          c.stroke();
        } else {
          c.beginPath();
          c.moveTo(x, y);
          c.lineTo(x + Math.cos(angle) * 7, y + Math.sin(angle) * 7);
          c.stroke();
        }
      }
      c.restore();
    }
    if (this.preview) {
      const { point, formation, count, center } = this.preview;
      this.path([center, point], "#ead39a", time);
      const slots = formationSlots(
        Math.min(count, 96),
        formation,
        point,
        team === 0 ? 1 : -1,
      );
      c.strokeStyle = "#f0d28c";
      c.globalAlpha = 0.65;
      c.lineWidth = 1.3;
      for (const s of slots) {
        c.strokeRect(s.x * TILE - 8, s.y * TILE - 3, 16, 7);
      }
      c.globalAlpha = 1;
    }
  }
  draw(state, map, b, team, alpha = 1, now = performance.now()) {
    const c = this.ctx,
      time = now / 1000;
    this.low =
      this.quality === "low" || (this.quality === "auto" && this.frameMs > 24);
    c.setTransform(1, 0, 0, 1, 0, 0);
    c.fillStyle = "#1b2827";
    c.fillRect(0, 0, this.canvas.width, this.canvas.height);
    const activeMap = b ? battleMap(b) : map,
      halfW = this.width / (TILE * this.cam.zoom * 2),
      halfH = this.height / (TILE * this.cam.zoom * 2);
    this.cam.x =
      activeMap.width < halfW * 2
        ? activeMap.width / 2
        : clamp(this.cam.x, halfW, activeMap.width - halfW);
    this.cam.y =
      activeMap.height < halfH * 2
        ? activeMap.height / 2
        : clamp(this.cam.y, halfH, activeMap.height - halfH);
    c.setTransform(
      this.dpr * this.cam.zoom,
      0,
      0,
      this.dpr * this.cam.zoom,
      this.dpr * (this.width / 2 - this.cam.x * TILE * this.cam.zoom),
      this.dpr * (this.height / 2 - this.cam.y * TILE * this.cam.zoom),
    );
    if (b) this.battle(b, time, alpha, team);
    else this.world(state, map, time);
    c.setTransform(this.dpr, 0, 0, this.dpr, 0, 0);
    if (this.marquee) {
      const { a, d } = this.marquee;
      c.fillStyle = "#e1c99020";
      c.strokeStyle = "#e3c585";
      c.fillRect(a.x, a.y, d.x - a.x, d.y - a.y);
      c.strokeRect(a.x, a.y, d.x - a.x, d.y - a.y);
    }
    if (now - this.lastMini > 350) {
      this.drawMini(state, map, b, team);
      this.lastMini = now;
    }
  }
  drawMini(state, map, b, team) {
    const c = this.mctx,
      w = this.mini.width,
      h = this.mini.height,
      m = b ? battleMap(b) : map;
    c.fillStyle = "#243629";
    c.fillRect(0, 0, w, h);
    const colors = [
      "#647747",
      "#38553c",
      "#73736b",
      "#3e697c",
      "#a08b5f",
      "#a9935d",
      "#c1c6b2",
      "#bb9d72",
    ];
    for (let y = 0; y < m.height; y += 2)
      for (let x = 0; x < m.width; x += 2) {
        c.fillStyle = colors[m.tiles[y * m.width + x]];
        c.fillRect(
          (x / m.width) * w,
          (y / m.height) * h,
          (2 / m.width) * w + 1,
          (2 / m.height) * h + 1,
        );
      }
    const marker = (x, y, color, size = 3) => {
      c.fillStyle = color;
      c.fillRect(
        (x / m.width) * w - size / 2,
        (y / m.height) * h - size / 2,
        size,
        size,
      );
    };
    if (b) {
      for (const u of b.units)
        if (u.hp > 0 && !u.routed)
          marker(u.x, u.y, u.team === team ? "#a8d9e5" : "#ff9579", 2);
    } else {
      for (const s of state.settlements)
        marker(
          s.x,
          s.y,
          s.owner === state.profile.uid ? "#f0d08b" : "#ded9c8",
          4,
        );
      for (const n of state.npcs)
        if (n.kind === "bandit") marker(n.x, n.y, "#df9576", 2);
      marker(state.profile.x, state.profile.y, "#f8db8c", 5);
    }
    c.strokeStyle = "#e8dfc6";
    c.lineWidth = 1;
    const vw = (this.width / TILE / this.cam.zoom / m.width) * w,
      vh = (this.height / TILE / this.cam.zoom / m.height) * h;
    c.strokeRect(
      (this.cam.x / m.width) * w - vw / 2,
      (this.cam.y / m.height) * h - vh / 2,
      vw,
      vh,
    );
  }
}
