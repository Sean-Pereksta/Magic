/* Beneath the Foundry weapons, charge states and specialized attacks. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.CircuitFoundryCombat = factory();
})(globalThis, function () {
  'use strict';

  const CATALOG = {
    breachHammer: {
      name: 'Breach Hammer', color: '#ffbc83', icon: '⚒', damage: 106, range: 148, cool: .84, charge: .42,
      cost: { osmiumIngot: 3, steel: 6, precisionBearing: 1 }, site: 'Borer Test Gallery in the Osmium Basin',
      desc: 'Plant both feet and charge a slam that sends a shockwave along nearby terrain. The wave stops at gaps.'
    },
    shardScattergun: {
      name: 'Shard Scattergun', color: '#f8ca9b', icon: '≋', damage: 22, range: 170, cool: .61, ammo: 'ammo',
      cost: { osmiumIngot: 3, cobaltIngot: 3, steel: 5 }, site: 'Shard Foundry in the Osmium Basin',
      desc: 'Fire seven shards with wide spread and steep falloff. Uses one Scrap Ammunition per shot.'
    },
    anchorHarpoon: {
      name: 'Anchor Harpoon', color: '#eccc89', icon: '⇥', damage: 58, range: 370, cool: .93, energy: 9,
      cost: { osmiumIngot: 3, iridiumIngot: 2, iron: 5 }, site: 'Harpoon Drop Shaft in the Osmium Basin',
      desc: 'Pull a light enemy toward you or hold a heavy enemy in place. Uses 9 energy.'
    },
    repeaterCarbine: {
      name: 'Repeater Carbine', color: '#fbd99d', icon: '⋮', damage: 38, range: 470, cool: .36, ammo: 'ammo',
      cost: { osmiumIngot: 4, cobaltIngot: 4, precisionBearing: 2 }, site: 'Security Yard in the Osmium Basin',
      desc: 'Fire a controlled three-round burst. Standing still reduces spread. Uses three Scrap Ammunition.'
    },
    gravityMaul: {
      name: 'Gravity Maul', color: '#83eed5', icon: '⚒', damage: 135, range: 106, cool: .86, energy: 10,
      cost: { graviteIngot: 4, osmiumIngot: 2, steel: 6 }, site: 'Lift Test Chamber in the Gravity Hollows',
      desc: 'Launch a cluster of nearby targets upward. Heavy enemies travel a shorter distance. Uses 10 energy.'
    },
    orbitDisc: {
      name: 'Orbit Disc', color: '#88fbee', icon: '◉', damage: 69, range: 310, cool: .76, energy: 8,
      cost: { graviteIngot: 3, precisionBearing: 2, copperIngot: 4 }, site: 'Flywheel Workshop in the Gravity Hollows',
      desc: 'Throw a disc that slices through foes on the way out and steers back toward you for a second pass. Uses 8 energy.'
    },
    compressionProjector: {
      name: 'Compression Projector', color: '#9ed4ff', icon: '⊛', damage: 18, range: 350, cool: 1.2, energy: 14,
      cost: { graviteIngot: 4, osmiumIngot: 2, fluxCell: 3 }, site: 'Compression Lab in the Gravity Hollows',
      desc: 'Pull exposed enemies toward your aim point in a 105-pixel radius. Switch weapons to follow up. Uses 14 energy.'
    },
    kineticBow: {
      name: 'Kinetic Bow', color: '#b1f8c8', icon: '➶', damage: 130, range: 545, cool: .56, charge: .56, ammo: 'ammo',
      cost: { graviteIngot: 4, osmiumIngot: 2, wood: 4 }, site: 'Kinetic Firing Alley in the Gravity Hollows',
      desc: 'Draw and fire a bolt that pins an enemy if there is a wall close behind it. Uses one Scrap Ammunition.'
    },
    prismRifle: {
      name: 'Prism Rifle', color: '#d5b6ff', icon: '⌁', damage: 150, range: 650, cool: .72, energy: 14,
      cost: { nullQuartz: 4, graviteIngot: 2, phaseLens: 2 }, site: 'Prism Vault in the Null Crystal Vaults',
      desc: 'Bank one shot off a floor, wall or ceiling to reach an enemy around cover. Uses 14 energy.'
    },
    phaseSabers: {
      name: 'Phase Sabers', color: '#ecc4ff', icon: '⚔', damage: 104, range: 92, cool: .41,
      cost: { nullQuartz: 3, osmiumIngot: 3, phaseLens: 1 }, site: 'Phase Annex in the Null Crystal Vaults',
      desc: 'Dodge an imminent attack with K or the Dodge button, then attack before the counter window closes.'
    },
    interferenceWand: {
      name: 'Interference Wand', color: '#bc95ff', icon: 'ϟ', damage: 72, range: 320, cool: .66, energy: 11,
      cost: { nullQuartz: 3, copperIngot: 4, quantumController: 1 }, site: 'Signal Vault in the Null Crystal Vaults',
      desc: 'Interrupt a machine’s charged attack and jam it briefly. Jammed machines can still move. Uses 11 energy.'
    },
    echoMortar: {
      name: 'Echo Mortar', color: '#e2a3fa', icon: '◒', damage: 190, range: 480, cool: 1.28, ammo: 'ammo',
      cost: { nullQuartz: 4, graviteIngot: 2, roboticFrame: 1 }, site: 'Echo Loading Bay in the Null Crystal Vaults',
      desc: 'Lob a shell over low cover. Its marked landing point detonates after a delay. Uses two Scrap Ammunition.'
    },
    starcoreLance: {
      name: 'Starcore Lance', color: '#ffe79d', icon: '➤', damage: 245, range: 800, cool: .96, charge: .9, energy: 26,
      cost: { starcoreIngot: 4, nullQuartz: 3, superAlloy: 2 }, site: 'Mantle Rail Gallery in the Starcore Mantle',
      desc: 'Plant your feet during a long windup, then pierce up to six targets in a line. Uses 26 energy.'
    },
    coronaCaster: {
      name: 'Corona Caster', color: '#ffa761', icon: '☼', damage: 34, range: 90, cool: 1.48, energy: 22,
      cost: { starcoreIngot: 4, graviteIngot: 2, fluxCell: 4 }, site: 'Corona Shield Room in the Starcore Mantle',
      desc: 'Carry a ring of heat with you for one second. It repeatedly burns nearby exposed enemies. Uses 22 energy.'
    },
    singularityLauncher: {
      name: 'Singularity Launcher', color: '#ffa5b3', icon: '⊙', damage: 224, range: 405, cool: 1.74, energy: 30,
      cost: { starcoreIngot: 5, graviteIngot: 4, quantumController: 2 }, site: 'Gravity Furnace in the Starcore Mantle',
      desc: 'Lob a slow orb that gathers exposed enemies around it before bursting. Uses 30 energy.'
    },
    mantleBreaker: {
      name: 'Mantle Breaker', color: '#ffc66d', icon: '◐', damage: 232, range: 120, cool: .92, charge: .64, energy: 13,
      cost: { starcoreIngot: 4, osmiumIngot: 3, superAlloy: 2 }, site: 'Mantle Guard Post in the Starcore Mantle',
      desc: 'Block most frontal damage during a short charge, then deliver a heavy strike at close range. Uses 13 energy.'
    }
  };
  const STATIONARY = new Set(['breachHammer', 'starcoreLance']);
  const AMMO_COUNTS = { repeaterCarbine: 3, echoMortar: 2 };
  const TAU = Math.PI * 2;

  function install(items, recipes) {
    for (const [key, weapon] of Object.entries(CATALOG)) {
      const { name, icon, color, cost, desc, site } = weapon;
      if (!items.some(item => item.key === key)) {
        items.push({ key, name, icon, color, tile: -2, weapon: true, equipment: true });
      }
      if (!recipes.some(recipe => recipe.key === key)) recipes.push({
        key, name, icon, cost, desc, unlockSource: site, out: 1, category: 'weapons', station: 'workbench',
        schematic: key, schematicLabel: name, requiresSchematic: true, equipment: true,
        attackProfile: {
          range: weapon.range, cooldown: weapon.cool, charge: weapon.charge,
          energy: weapon.energy, ammo: weapon.ammo && (AMMO_COUNTS[key] || 1)
        }
      });
    }
  }

  function reset(survival) {
    survival.foundryCharge = null;
    survival.foundryBurst = null;
    survival.foundryShots = [];
    survival.foundryCounter = 0;
    survival.foundryDodge = 0;
  }

  function face(survival, angle) {
    survival.player.facing = Math.cos(angle) >= 0 ? 1 : -1;
    survival.attackAngle = angle;
  }

  function attack(survival, key, origin, angle, aimDistance) {
    const weapon = CATALOG[key];
    const ammo = AMMO_COUNTS[key] || 1;
    if (weapon.ammo && (survival.inventory[weapon.ammo] || 0) < ammo) {
      survival.notify(`Needs ${ammo} Scrap Ammunition`);
      return false;
    }
    if (!survival.canFire(weapon)) return false;
    if (STATIONARY.has(key) && (!survival.player.onGround || !survival.standingStill())) {
      survival.notify('Stand still on solid ground to charge the ' + weapon.name);
      return false;
    }
    face(survival, angle);
    if (weapon.charge) {
      survival.foundryCharge = { key, angle, aimDistance, remaining: weapon.charge };
      survival.attackAnimation = weapon.charge + .14;
    } else {
      fire(survival, key, origin, angle, aimDistance);
    }
    return true;
  }

  function effect(survival, kind, origin, color, duration, extra = {}) {
    survival.effects.push({ ...origin, life: duration, max: duration, kind, color, ...extra });
  }

  // Tile DDA preserves the exact face normal needed by the Prism Rifle.
  function wallContact(survival, origin, dx, dy, limit) {
    const size = survival.tile;
    let x = Math.floor(origin.x / size), y = Math.floor(origin.y / size);
    const stepX = Math.sign(dx), stepY = Math.sign(dy);
    const deltaX = stepX ? Math.abs(size / dx) : Infinity;
    const deltaY = stepY ? Math.abs(size / dy) : Infinity;
    let nextX = stepX ? ((x + (stepX > 0 ? 1 : 0)) * size - origin.x) / dx : Infinity;
    let nextY = stepY ? ((y + (stepY > 0 ? 1 : 0)) * size - origin.y) / dy : Infinity;
    while (Math.min(nextX, nextY) <= limit) {
      const distance = Math.min(nextX, nextY);
      const crossX = Math.abs(nextX - distance) < 1e-7;
      const crossY = Math.abs(nextY - distance) < 1e-7;
      if (crossX && crossY) {
        const blockX = survival.solid(x + stepX, y), blockY = survival.solid(x, y + stepY);
        if (blockX || blockY) return { distance, crossX: blockX, crossY: blockY };
      }
      if (crossX) { x += stepX; nextX += deltaX; }
      if (crossY) { y += stepY; nextY += deltaY; }
      if (survival.solid(x, y)) return { distance, crossX, crossY };
    }
    return null;
  }

  function fireRay(survival, origin, angle, range, color, width = 6, margin = 3, piercing = false) {
    const dx = Math.cos(angle), dy = Math.sin(angle);
    const wall = wallContact(survival, origin, dx, dy, range);
    const limit = wall ? Math.max(0, wall.distance - .1) : range;
    const hits = survival.traceRay(origin, angle, limit, margin).hits.filter(({ enemy }) => survival.canHit(enemy));
    const distance = piercing ? limit : (hits[0]?.distance ?? limit);
    effect(survival, 'ray', origin, color, .2, { dx: dx * distance, dy: dy * distance, width });
    return { dx, dy, wall, hits, distance };
  }

  function exposed(survival, origin, radius) {
    return survival.query(origin.x, origin.y, radius)
      .filter(enemy => survival.canHit(enemy) && survival.lineOfSight(origin.x, origin.y, enemy.x, enemy.y))
      .sort((a, b) => Math.hypot(a.x - origin.x, a.y - origin.y) - Math.hypot(b.x - origin.x, b.y - origin.y));
  }

  function cone(survival, origin, angle, weapon, arc = 1.25) {
    return exposed(survival, origin, weapon.range).filter(enemy => {
      const delta = Math.atan2(enemy.y - origin.y, enemy.x - origin.x) - angle;
      return Math.abs(Math.atan2(Math.sin(delta), Math.cos(delta))) <= arc;
    }).slice(0, 7);
  }

  function heavy(enemy) {
    return enemy.w >= 28 || (enemy.maxHealth || enemy.hp) >= 140;
  }

  function displace(enemy, vx, vy, time, gravity = 0) {
    enemy.foundryMotion = { vx, vy, time, gravity };
    enemy.wind = 0;
  }

  function pull(enemy, point, speed, time) {
    const dx = point.x - enemy.x, dy = point.y - enemy.y;
    const travel = Math.hypot(dx, dy);
    if (travel < 5) return;
    const capped = Math.min(speed, (travel - 4) / time) * (heavy(enemy) ? .55 : 1);
    displace(enemy, dx / travel * capped, dy / travel * capped, time);
  }

  function launch(survival, kind, origin, angle, weapon, extras = {}) {
    survival.foundryShots.push({
      kind, x: origin.x, y: origin.y, angle, damage: weapon.damage, color: weapon.color,
      range: weapon.range, age: 0, ...extras
    });
  }

  function shockwave(survival, origin, angle, weapon) {
    const feet = survival.player.y + survival.player.h;
    launch(survival, 'wave', { x: origin.x, y: feet - 3 }, angle, weapon, {
      direction: Math.cos(angle) >= 0 ? 1 : -1, distance: 0, struck: new Set()
    });
  }

  function fire(survival, key, origin, angle, aimDistance) {
    const weapon = CATALOG[key];
    survival.spendShot(weapon);
    if (AMMO_COUNTS[key]) survival.inventory[weapon.ammo] -= AMMO_COUNTS[key] - 1;
    face(survival, angle);
    survival.attackCooldown = weapon.cool;
    survival.attackAnimation = .23;
    const hit = (enemy, damage = weapon.damage) => survival.hit(enemy, damage, 'player');
    const ray = (width, margin, piercing = false) => fireRay(survival, origin, angle, weapon.range, weapon.color, width, margin, piercing);
    const slash = () => effect(survival, 'slash', {
      x: origin.x + Math.cos(angle) * 42, y: origin.y + Math.sin(angle) * 42
    }, weapon.color, .23, { angle });
    switch (key) {
      case 'breachHammer':
        shockwave(survival, origin, angle, weapon);
        break;
      case 'shardScattergun':
        for (let i = -3; i <= 3; i++) {
          const { hits: [target] } = fireRay(survival, origin, angle + i * .083, weapon.range, weapon.color, 3, 3);
          if (target) hit(target.enemy, weapon.damage * (1 - .68 * target.distance / weapon.range));
        }
        break;
      case 'anchorHarpoon': {
        const { hits: [target] } = ray(6, 5);
        if (!target || !hit(target.enemy)) break;
        if (heavy(target.enemy)) target.enemy.foundryPin = .95;
        else pull(target.enemy, origin, 520, .43);
        break;
      }
      case 'repeaterCarbine':
        survival.foundryBurst = { angle, shots: 3, next: 0 };
        updateBurst(survival, 0);
        break;
      case 'gravityMaul':
        for (const enemy of cone(survival, origin, angle, weapon, 1.65)) {
          if (hit(enemy)) displace(enemy, Math.sign(enemy.x - origin.x) * 58, heavy(enemy) ? -170 : -330, .55, 700);
        }
        effect(survival, 'pulse', origin, weapon.color, .32, { radius: weapon.range });
        break;
      case 'orbitDisc':
        launch(survival, 'disc', origin, angle, weapon, { speed: 510, distance: 0, returning: false, struck: new Set() });
        break;
      case 'compressionProjector': {
        const distance = Math.min(weapon.range, aimDistance);
        const dx = Math.cos(angle), dy = Math.sin(angle), wall = wallContact(survival, origin, dx, dy, distance);
        const length = wall ? Math.max(0, wall.distance - 8) : distance;
        const center = { x: origin.x + dx * length, y: origin.y + dy * length };
        for (const enemy of exposed(survival, center, 105)) if (hit(enemy)) pull(enemy, center, 295, .46);
        effect(survival, 'pulse', center, weapon.color, .43, { radius: 105 });
        break;
      }
      case 'kineticBow': {
        const { hits: [target], dx, dy } = ray(6, 4);
        if (!target || !hit(target.enemy)) break;
        const enemy = target.enemy;
        const wall = wallContact(survival, enemy, dx, dy, 74);
        if (wall && wall.distance > 2) {
          const clearance = Math.abs(dx) * enemy.w / 2 + Math.abs(dy) * enemy.h / 2 + 1;
          const travel = Math.max(0, wall.distance - clearance);
          survival.move(enemy, dx * travel, dy * travel);
          enemy.foundryPin = heavy(enemy) ? .55 : 1.5;
        }
        break;
      }
      case 'prismRifle': {
        const first = ray(7, 3);
        if (first.hits.length) { hit(first.hits[0].enemy); break; }
        if (!first.wall || first.distance >= weapon.range - 3) break;
        const dx = first.wall.crossX ? -first.dx : first.dx;
        const dy = first.wall.crossY ? -first.dy : first.dy;
        const next = { x: origin.x + first.dx * first.distance + dx * .6, y: origin.y + first.dy * first.distance + dy * .6 };
        const { hits: [target] } = fireRay(survival, next, Math.atan2(dy, dx), weapon.range - first.distance, '#fff0b6', 6, 3);
        if (target) hit(target.enemy, weapon.damage * .82);
        break;
      }
      case 'phaseSabers': {
        const counter = survival.foundryCounter > 0;
        survival.foundryCounter = 0;
        for (const enemy of cone(survival, origin, angle, weapon, counter ? Math.PI : 1.27)) {
          if (hit(enemy, weapon.damage * (counter ? 2.05 : 1)) && counter) enemy.stun = Math.max(enemy.stun, .45);
        }
        slash();
        if (counter) effect(survival, 'pulse', origin, weapon.color, .3, { radius: weapon.range });
        break;
      }
      case 'interferenceWand': {
        const { hits: [target] } = ray(7, 5);
        if (!target || !hit(target.enemy)) break;
        if (['drone', 'wisp', 'leech', 'siege', 'sentinel', 'guardian'].includes(target.enemy.type)) {
          target.enemy.foundryJam = target.enemy.type === 'guardian' ? 1.35 : 3.2;
          target.enemy.wind = 0;
          effect(survival, 'pulse', { x: target.enemy.x, y: target.enemy.y }, weapon.color, .56, { radius: 28 });
        }
        break;
      }
      case 'echoMortar': {
        const distance = Math.max(45, Math.min(weapon.range, aimDistance));
        const landing = { x: origin.x + Math.cos(angle) * distance, y: origin.y + Math.sin(angle) * distance };
        launch(survival, 'mortar', origin, angle, weapon, {
          origin, landing, travel: .52 + distance / 800, loft: Math.max(38, Math.min(145, distance * .36)), fuse: .66
        });
        break;
      }
      case 'starcoreLance':
        for (const [index, { enemy }] of ray(14, 7, true).hits.slice(0, 6).entries()) hit(enemy, weapon.damage * (1 - index * .1));
        break;
      case 'coronaCaster':
        launch(survival, 'corona', origin, angle, weapon, { nextPulse: 0 });
        break;
      case 'singularityLauncher':
        launch(survival, 'singularity', origin, angle, weapon, {
          vx: Math.cos(angle) * 190, vy: Math.sin(angle) * 190 - 52,
          range: Math.min(weapon.range, Math.max(45, aimDistance)), distance: 0
        });
        break;
      case 'mantleBreaker':
        for (const enemy of cone(survival, origin, angle, weapon)) {
          if (hit(enemy)) displace(enemy, Math.cos(angle) * (heavy(enemy) ? 90 : 235), -75, .24, 460);
        }
        slash();
        break;
    }
  }

  function updateBurst(survival, dt) {
    const burst = survival.foundryBurst;
    if (!burst) return;
    const weapon = CATALOG.repeaterCarbine;
    burst.next -= dt;
    while (burst.shots > 0 && burst.next <= 0) {
      const spread = survival.standingStill() ? .016 : .085;
      const angle = burst.angle + (burst.shots - 2) * spread;
      const { hits: [target] } = fireRay(survival, survival.center(), angle, weapon.range, weapon.color, 4);
      if (target) survival.hit(target.enemy, weapon.damage, 'player');
      survival.attackAnimation = .15;
      burst.shots--;
      burst.next += .068;
    }
    if (!burst.shots) survival.foundryBurst = null;
  }

  function advance(survival, shot, x, y, onStep) {
    const dx = x - shot.x, dy = y - shot.y;
    const length = Math.hypot(dx, dy), steps = Math.max(1, Math.ceil(length / (survival.tile / 5)));
    for (let i = 0; i < steps; i++) {
      const nextX = shot.x + dx / steps, nextY = shot.y + dy / steps;
      if (survival.solid(Math.floor(nextX / survival.tile), Math.floor(nextY / survival.tile))) return false;
      shot.x = nextX;
      shot.y = nextY;
      onStep?.();
    }
    return true;
  }

  function explode(survival, shot, radius) {
    for (const enemy of exposed(survival, shot, radius)) {
      const distance = Math.hypot(enemy.x - shot.x, enemy.y - shot.y);
      const falloff = Math.max(.24, 1 - .73 * distance / radius);
      survival.hit(enemy, shot.damage * falloff, 'player');
    }
    effect(survival, 'pulse', { x: shot.x, y: shot.y }, shot.color, .55, { radius });
  }

  function advanceWave(survival, shot, dt) {
    const size = survival.tile;
    for (let count = 0; count < Math.ceil(370 * dt / 5); count++) {
      const x = shot.x + 5 * shot.direction;
      const col = Math.floor(x / size), row = Math.floor(shot.y / size);
      const floor = [row + 1, row, row + 2].find(y => survival.solid(col, y) && !survival.solid(col, y - 1));
      if (floor === undefined) return false;
      shot.x = x;
      shot.y = floor * size - 3;
      shot.distance += 5;
      const area = { x, y: shot.y - 13 };
      for (const enemy of exposed(survival, area, 19)) {
        if (shot.struck.has(enemy.id)) continue;
        shot.struck.add(enemy.id);
        if (survival.hit(enemy, shot.damage, 'player')) displace(enemy, shot.direction * 110, -105, .2, 650);
      }
      if (shot.distance >= shot.range) return false;
    }
    return true;
  }

  function updateShot(survival, shot, dt) {
    shot.age += dt;
    if (shot.kind === 'wave') return advanceWave(survival, shot, dt);
    if (shot.kind === 'disc') {
      const center = survival.center();
      const angle = shot.returning ? Math.atan2(center.y - shot.y, center.x - shot.x) : shot.angle;
      const travel = shot.speed * dt;
      const reached = shot.returning && Math.hypot(center.x - shot.x, center.y - shot.y) <= travel;
      const passed = advance(survival, shot, shot.x + Math.cos(angle) * travel, shot.y + Math.sin(angle) * travel, () => {
        for (const enemy of exposed(survival, shot, 9)) {
          if (shot.struck.has(enemy.id)) continue;
          shot.struck.add(enemy.id);
          survival.hit(enemy, shot.damage, 'player');
        }
      });
      if (shot.returning) return passed && !reached && shot.age < 2.4;
      shot.distance += travel;
      if (!passed || shot.distance >= shot.range) {
        shot.returning = true;
        shot.struck.clear();
      }
      return true;
    }
    if (shot.kind === 'mortar') {
      if (!shot.armed) {
        const t = Math.min(1, shot.age / shot.travel);
        const x = shot.origin.x + (shot.landing.x - shot.origin.x) * t;
        const y = shot.origin.y + (shot.landing.y - shot.origin.y) * t - 4 * shot.loft * t * (1 - t);
        if (!advance(survival, shot, x, y) || t >= 1) shot.armed = true;
        return true;
      }
      shot.fuse -= dt;
      if (shot.fuse > 0) return true;
      explode(survival, shot, 103);
      return false;
    }
    if (shot.kind === 'corona') {
      Object.assign(shot, survival.center());
      if (shot.age >= 1.05) return false;
      if (shot.age >= shot.nextPulse) {
        shot.nextPulse = shot.age + .2;
        for (const enemy of exposed(survival, shot, shot.range)) survival.hit(enemy, shot.damage, 'player');
      }
      return true;
    }
    if (shot.kind === 'singularity') {
      if (!shot.settled) {
        shot.vy += 115 * dt;
        shot.distance += Math.hypot(shot.vx, shot.vy) * dt;
        if (!advance(survival, shot, shot.x + shot.vx * dt, shot.y + shot.vy * dt) || shot.distance >= shot.range) {
          shot.settled = true;
        }
      }
      for (const enemy of exposed(survival, shot, 120)) pull(enemy, shot, 162, .22);
      if (shot.age < 2.25) return true;
      explode(survival, shot, 130);
      return false;
    }
    return false;
  }

  function tick(survival, dt) {
    survival.foundryCounter = Math.max(0, survival.foundryCounter - dt);
    survival.foundryDodge = Math.max(0, survival.foundryDodge - dt);
    const charge = survival.foundryCharge;
    if (charge) {
      if (survival.weapon !== charge.key || !survival.inventory[charge.key] ||
          (STATIONARY.has(charge.key) && (!survival.standingStill() || !survival.player.onGround))) {
        survival.foundryCharge = null;
        survival.attackAnimation = 0;
        survival.notify('Charge cancelled');
      } else {
        charge.remaining -= dt;
        if (charge.remaining <= 0) {
          survival.foundryCharge = null;
          if (survival.canFire(CATALOG[charge.key])) fire(survival, charge.key, survival.center(), charge.angle, charge.aimDistance);
        }
      }
    }
    updateBurst(survival, dt);
    survival.foundryShots = survival.foundryShots.filter(shot => updateShot(survival, shot, dt));
  }

  function dodge(survival, direction) {
    if (survival.weapon !== 'phaseSabers' || !survival.inventory.phaseSabers || survival.foundryDodge > 0) return false;
    const origin = survival.center();
    const threat = survival.query(origin.x, origin.y, 200).some(enemy => enemy.wind > 0 && enemy.wind < .4 && enemy.attackMachine == null &&
      Math.hypot(enemy.attackX - origin.x, enemy.attackY - origin.y) < 44 && survival.lineOfSight(enemy.x, enemy.y, origin.x, origin.y)) ||
      survival.projectiles.some(shot => shot.owner === 'enemy' && Math.hypot(
        shot.x + shot.vx * .18 - origin.x, shot.y + shot.vy * .18 - origin.y) < 36);
    const player = survival.player;
    const sign = Math.sign(direction || -player.facing) || -1;
    let moved = 0;
    for (let step = 1; step <= 12; step++) {
      if (survival.blocked(origin.x + sign * step * 4, origin.y, player.w, player.h)) break;
      moved = step * 4;
    }
    if (!moved) return false;
    player.x += sign * moved;
    survival.foundryDodge = 1.25;
    survival.invulnerable = Math.max(survival.invulnerable, .28);
    if (threat) {
      survival.foundryCounter = .72;
      survival.notify('Phase counter ready — attack now');
    }
    effect(survival, 'ray', origin, CATALOG.phaseSabers.color, .3, { dx: sign * moved, dy: 0, width: 11 });
    return true;
  }

  function controlEnemy(survival, enemy, dt) {
    enemy.foundryJam = Math.max(0, (enemy.foundryJam || 0) - dt);
    if (enemy.foundryPin > 0) {
      enemy.foundryPin -= dt;
      enemy.wind = 0;
      enemy.vx = 0;
      enemy.vy = 0;
      return true;
    }
    const motion = enemy.foundryMotion;
    if (!motion) return false;
    motion.time -= dt;
    motion.vy += motion.gravity * dt;
    survival.move(enemy, motion.vx * dt, motion.vy * dt);
    enemy.vy = motion.vy;
    if (motion.time <= 0 || (enemy.ground && motion.vy > 0)) enemy.foundryMotion = null;
    return true;
  }

  function guardFactor(survival, threat) {
    if (!threat || survival.foundryCharge?.key !== 'mantleBreaker') return 1;
    const origin = survival.center(), angle = survival.foundryCharge.angle;
    const direction = Math.atan2(threat.y - origin.y, threat.x - origin.x);
    return Math.cos(direction - angle) >= .38 ? .24 : 1;
  }

  function draw(survival, ctx, camera, width, height) {
    const cx = camera.x || 0, cy = camera.y || 0;
    for (const shot of survival.foundryShots) {
      const x = shot.x - cx, y = shot.y - cy;
      if (x < -140 || y < -140 || x > width + 140 || y > height + 140) continue;
      ctx.save(); ctx.translate(x, y);
      ctx.strokeStyle = shot.color; ctx.fillStyle = shot.color; ctx.lineWidth = 2;
      ctx.beginPath();
      if (shot.kind === 'disc') {
        ctx.rotate(shot.age * 18);
        ctx.ellipse(0, 0, 11, 4, 0, 0, TAU); ctx.stroke();
      } else if (shot.kind === 'wave') {
        ctx.moveTo(-11, 4); ctx.lineTo(-3, -12); ctx.lineTo(2, 2); ctx.lineTo(7, -8); ctx.lineTo(13, 4); ctx.stroke();
      } else if (shot.kind === 'corona') {
        ctx.lineWidth = 3 + Math.sin(shot.age * 24) * 1.3;
        ctx.globalAlpha = Math.min(1, (1.1 - shot.age) * 1.9);
        ctx.arc(0, 0, shot.range, 0, TAU); ctx.stroke();
      } else {
        ctx.arc(0, 0, shot.kind === 'singularity' ? 10 : 5, 0, TAU);
        ctx.fill();
        if (shot.armed || shot.kind === 'singularity') {
          ctx.globalAlpha = .42;
          ctx.setLineDash([5, 5]); ctx.beginPath();
          ctx.arc(0, 0, shot.kind === 'singularity' ? 120 : 103, 0, TAU); ctx.stroke();
        }
      }
      ctx.restore();
    }
    if (!survival.foundryCharge && !survival.foundryCounter) return;
    const { x, y } = survival.center(), charge = survival.foundryCharge;
    ctx.save(); ctx.translate(x - cx, y - cy); ctx.lineWidth = 3;
    ctx.strokeStyle = CATALOG[charge?.key || 'phaseSabers'].color; ctx.beginPath();
    if (charge?.key === 'mantleBreaker') ctx.arc(0, 0, 26, charge.angle - 1.18, charge.angle + 1.18);
    else ctx.arc(0, 0, 22, -Math.PI / 2, -Math.PI / 2 + TAU * (charge ? 1 - charge.remaining / CATALOG[charge.key].charge : survival.foundryCounter / .72));
    ctx.stroke(); ctx.restore();
  }

  return { CATALOG, install, reset, attack, tick, dodge, controlEnemy, guardFactor, draw };
});
