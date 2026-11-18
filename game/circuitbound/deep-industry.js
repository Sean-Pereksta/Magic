(function (root, factory) {
  'use strict';
  const industry = factory();
  if (typeof module === 'object' && module.exports) module.exports = industry;
  else root.CircuitDeepIndustry = industry;
})(typeof globalThis === 'undefined' ? this : globalThis, function () {
  'use strict';

  const COOLANT = 'coolantCanister';
  const EMPTY = 'emptyCoolantCanister';
  const FRAGMENT = 'nullQuartzFragment';
  const BUFFER = 16;
  const port = (side, type, mode, name) => ({ side, type, mode, name });
  const power = port(3, 'power', 'in', 'PWR');
  const drive = port(2, 'mechanical', 'in', 'DRIVE');
  const product = side => port(side, 'item', 'out', 'ORE');
  const ores = [
    { symbol: 'OSMIUM_ORE', id: 160, key: 'osmiumOre', name: 'Osmium Ore', refined: 'osmiumIngot', good: 'Osmium Ingot', color: '#9cbacf' },
    { symbol: 'GRAVITE_ORE', id: 161, key: 'graviteOre', name: 'Gravite Ore', refined: 'graviteIngot', good: 'Gravite Ingot', color: '#9d8ad6' },
    { symbol: 'NULL_QUARTZ_ORE', id: 162, key: 'nullQuartzOre', name: 'Null Quartz Deposit', refined: 'nullQuartz', good: 'Null Quartz', color: '#b1eedb' },
    { symbol: 'STARCORE_ORE', id: 163, key: 'starcoreOre', name: 'Starcore Ore', refined: 'starcoreIngot', good: 'Starcore Ingot', color: '#ffb666' }
  ];
  const rigs = [
    {
      symbol: 'DEEPCORE_BORER', id: 164, key: 'deepcoreBorer', name: 'Deepcore Borer', color: '#bd965b',
      cost: { steel: 6, cobaltIngot: 3, iridiumIngot: 1, aetheriumIngot: 1, precisionBearing: 2 },
      load: 10, torque: 12, seconds: 2.75, reach: 8,
      description: 'Extracts Osmium from a vertical seam. Set it just above the deposit, with solid ground beneath both feet. Feed power from above, drive from the left, and connect a bin to the lower right outlet.'
    },
    {
      symbol: 'SEISMIC_STRIP_MINER', id: 165, key: 'seismicStripMiner', name: 'Seismic Strip Miner', color: '#a69bd9',
      cost: { steel: 8, osmiumIngot: 3, cobaltIngot: 2, roboticFrame: 1, servoCore: 2 },
      load: 14, torque: 16, seconds: 1.8, reach: 9,
      description: 'Extracts Gravite with a moving cutting head. Span its travel with guide rails two tiles above the control head. Choose both travel limits and the side where the rig unloads.'
    },
    {
      symbol: 'RESONANCE_DRILL', id: 166, key: 'resonanceDrill', name: 'Resonance Drill', color: '#86d5bc',
      cost: { steel: 6, graviteIngot: 3, osmiumIngot: 1, iridiumIngot: 1, phaseLens: 1, circuit: 3 },
      load: 16, torque: 20, seconds: 2.3, reach: 10,
      description: 'Extracts Null Quartz. Precision recovers two intact chunks per cut. Fast mode recovers three fragments in less time. Refine intact quartz at an Induction Smelter or use fragments for bulk circuits in a Robotics Foundry.'
    },
    {
      symbol: 'MANTLE_EXTRACTOR', id: 167, key: 'mantleExtractor', name: 'Mantle Extractor', color: '#ee9c66',
      cost: { steel: 10, nullQuartz: 3, graviteIngot: 1, aetheriumIngot: 2, quantumController: 1, servoCore: 2 },
      load: 22, torque: 0, seconds: 2.9, reach: 10,
      description: 'Extracts Starcore with separate power, coolant and product lines. Load canisters into the upper blue intake and connect storage to the lower right outlet. The rig stops safely when either supply or storage runs out.'
    }
  ];
  rigs.forEach((rig, index) => { rig.ore = ores[index]; });

  const parts = [
    { id: 168, symbol: 'DEEP_RIG_FOOT', name: 'Rig anchor', icon: '╨' },
    { id: 169, symbol: 'DEEP_RIG_OUTLET', name: 'Ore outlet', icon: '▸', ports: [product(0)] },
    { id: 170, symbol: 'DEEP_RIG_COOLANT', name: 'Coolant inlet', icon: '●', ports: [port(0, 'item', 'in', 'COOLANT')], transport: true, inventoryCapacity: 12, accepts: [COOLANT], retainItems: true },
    { id: 171, symbol: 'DEEP_RIG_GANTRY', name: 'Rig gantry', icon: '╦' },
    { id: 172, symbol: 'DEEP_RIG_LEFT_OUTLET', name: 'Left ore outlet', icon: '◂', ports: [product(2)] }
  ];

  function install(T, defs, items, recipes) {
    for (const ore of ores) {
      T[ore.symbol] = ore.id;
      defs[ore.id] = { name: ore.name, kind: 'ore', drop: ore.key, solid: true, hard: 8, miningTier: 5, deepOre: true, weight: 8, strength: 18 };
      items.push({ key: ore.key, name: ore.name, tile: -2, resource: true, icon: '◆', color: ore.color, rarity: 'relic' });
      items.push({ key: ore.refined, name: ore.good, tile: -2, resource: true, icon: '▰', color: ore.color, rarity: 'relic' });
    }
    for (const [key, name, icon, color] of [
      [FRAGMENT, 'Null Quartz Fragment', '◈', '#abd3d0'],
      [COOLANT, 'Coolant Canister', '●', '#86daff'],
      [EMPTY, 'Empty Coolant Canister', '○', '#a6bdc5']
    ]) items.push({ key, name, tile: -2, resource: true, icon, color });

    for (const rig of rigs) {
      T[rig.symbol] = rig.id;
      const coolant = rig.symbol === 'MANTLE_EXTRACTOR';
      defs[rig.id] = {
        name: rig.name, icon: '▣', kind: 'item', foundryRig: rig, drop: rig.key, solid: true,
        electric: true, load: rig.load, mechanical: !!rig.torque, hard: 1.6, weight: 12, strength: 40,
        capacity: 75, ports: [power, ...(rig.torque ? [drive] : [])],
        ...(coolant ? { transport: true, accepts: [COOLANT], inventoryCapacity: 12, retainItems: true } : {})
      };
      items.push({ key: rig.key, name: rig.name, tile: rig.id, icon: '▣', color: rig.color, rarity: 'relic' });
      recipes.push({ key: rig.key, name: rig.name, icon: '▣', out: 1, cost: rig.cost, desc: rig.description, category: 'deep mining' });
    }
    for (const part of parts) {
      T[part.symbol] = part.id;
      defs[part.id] = { kind: 'item', solid: true, hard: 1.6, weight: 4, strength: 32, foundryPart: true, ...part };
    }
    recipes.push({
      key: COOLANT, name: 'Coolant Canister', icon: '●', out: 2, cost: { ironIngot: 1, crystal: 2 },
      desc: 'Keeps the Mantle Extractor cooled for four cuts. Feed canisters into the separate blue coolant inlet.',
      category: 'deep mining'
    });
    const smelter = defs[T.INDUCTION_SMELTER]?.machine;
    if (smelter) {
      smelter.recipes.push(...ores.map(ore => ({ key: ore.refined, cost: { [ore.key]: 2 }, out: { [ore.refined]: 1 } })));
      smelter.recipes.push(
        { key: 'compressNullQuartz', cost: { [FRAGMENT]: 6 }, out: { nullQuartz: 1 } },
        { key: 'refillCoolant', cost: { [EMPTY]: 1, crystal: 1 }, out: { [COOLANT]: 1 } }
      );
    }
    defs[T.ROBOTICS_FOUNDRY]?.machine?.recipes.push({
      key: 'bulkQuartzCircuits', cost: { [FRAGMENT]: 3, copperIngot: 1 }, out: { circuit: 4 }
    });
  }

  function create({ T, defs, get, set, engine, inventory, stats, player, tile, inside, placementBlock, notify }) {
    const controllers = new Set();
    const data = (x, y) => engine.getMeta().get(engine.key(x, y));
    const strip = rig => rig.id === T.SEISMIC_STRIP_MINER;
    const resonance = rig => rig.id === T.RESONANCE_DRILL;
    const mantle = rig => rig.id === T.MANTLE_EXTRACTOR;

    function footprint(x, y, rig) {
      return [
        { x, y, id: rig.id },
        { x: x - 1, y: y + 1, id: T.DEEP_RIG_FOOT },
        { x: x + 1, y: y + 1, id: T.DEEP_RIG_OUTLET },
        { x: x - 1, y: y - 1, id: strip(rig) ? T.DEEP_RIG_LEFT_OUTLET : T.DEEP_RIG_GANTRY },
        { x: x + 1, y: y - 1, id: mantle(rig) ? T.DEEP_RIG_COOLANT : T.DEEP_RIG_GANTRY }
      ];
    }

    function placement(x, y, item) {
      const rig = defs[item?.tile]?.foundryRig;
      if (!rig) return null;
      const cells = footprint(x, y, rig).map(cell => ({ ...cell, rot: 0, reason: placementBlock(cell.x, cell.y, { tile: cell.id }) }));
      const reason = cells.find(cell => cell.reason)?.reason ||
        (!(inventory[item.key] > 0) ? 'Out of ' + item.name + '.' : '');
      return { cells, reason, count: reason ? 0 : cells.length, item };
    }

    function place(x, y, item) {
      const plan = placement(x, y, item);
      if (!plan) return false;
      if (plan.reason) { notify(plan.reason); return false; }
      for (const cell of plan.cells) {
        set(cell.x, cell.y, cell.id, { rot: 0, foundryX: x, foundryY: y });
      }
      inventory[item.key]--;
      stats.built = (stats.built || 0) + plan.cells.length;
      controllers.add(engine.key(x, y));
      notify('Rig placed. Inspect it to anchor the feet and connect its ports.');
      return true;
    }

    function controller(x, y) {
      let id = get(x, y);
      if (defs[id]?.foundryPart) {
        const m = data(x, y);
        if (!Number.isInteger(m?.foundryX) || !Number.isInteger(m?.foundryY)) return null;
        x = m.foundryX; y = m.foundryY; id = get(x, y);
      }
      const rig = defs[id]?.foundryRig;
      if (!rig) return null;
      return { x, y, id, rig, m: engine.data(engine.key(x, y)) };
    }

    function reset() {
      controllers.clear();
      for (const k of engine.nodes) if (defs[engine.id(k)]?.foundryRig) controllers.add(k);
    }

    function anchorSupported(x, y) {
      const id = get(x, y);
      return inside(x, y) && defs[id]?.solid && !defs[id]?.platform && !defs[id]?.door;
    }

    function targetAt(x, y, rig, underFoot = false) {
      for (let down = underFoot ? 3 : 2; down <= rig.reach && inside(x, y + down); down++) {
        const id = get(x, y + down);
        if (id === rig.ore.id) return { x, y: y + down };
        // Overburden is bored through visually. Structures, protected ore and other parts stop the head.
        if (id && ![T.GRASS, T.DIRT, T.STONE, T.OBSIDIAN].includes(id)) break;
      }
      return null;
    }

    function railLimit(m, side) { return Math.max(2, Math.min(6, Number(m[side]) || 2)); }

    function railGap({ x, y, m }) {
      for (let i = -railLimit(m, 'foundryRailLeft'); i <= railLimit(m, 'foundryRailRight'); i++) {
        if (![T.RAIL, T.MAGNETIC_RAIL, T.FREIGHT_RAIL].filter(Number.isInteger).includes(get(x + i, y - 2))) return x + i;
      }
      return null;
    }

    function receiver({ x, y, rig, m }, key, amount = 1) {
      const left = strip(rig) && m.foundryUnload === 'left';
      const px = x + (left ? -1 : 1), py = y + (left ? -1 : 1);
      const outlet = get(px, py), side = left ? 2 : 0;
      if (outlet !== (left ? T.DEEP_RIG_LEFT_OUTLET : T.DEEP_RIG_OUTLET)) return null;
      const pk = engine.key(px, py), k = engine.neighbor(pk, side);
      if (k === null) return null;
      const def = defs[engine.id(k)];
      if (!def?.transport || !engine.pp(k, 'item').some(p => p.side === (side + 2) % 4 && p.mode !== 'out')) return null;
      const cargo = engine.getMeta().get(k);
      if (engine.id(k) === T.FILTER && (cargo?.filter || 'iron') !== key || def.ammo && def.ammo !== key ||
          def.accepts && !def.accepts.includes(key) || def.producer && !def.machine?.recipes) return null;
      return { k, fits: engine.capacity(k) - engine.stored(k) >= amount };
    }

    function stop(m, fault, detail, stage) {
      m.working = false;
      m.foundryFault = fault;
      m.foundryDetail = detail;
      m.foundryStage = stage;
    }

    function collectPortCoolant({ x, y, m }) {
      if (get(x + 1, y - 1) !== T.DEEP_RIG_COOLANT) return;
      const portData = data(x + 1, y - 1);
      if (portData?.contents?.[COOLANT] > 0) {
        const n = engine.insert(engine.key(x, y), COOLANT, portData.contents[COOLANT]);
        portData.contents[COOLANT] -= n;
        if (!portData.contents[COOLANT]) delete portData.contents[COOLANT];
      }
    }

    function flushOutput(c, dt) {
      const { m } = c;
      m.foundryShipping = Math.min(4, (m.foundryShipping || 0) + dt * 5);
      const stock = m.outputs || {};
      for (const key of Object.keys(stock)) {
        if (key === EMPTY) continue; // The tender or player retrieves these from the coolant loop.
        const count = Math.min(stock[key], Math.floor(m.foundryShipping));
        if (count < 1) continue;
        const next = receiver(c, key);
        if (!next?.fits) continue;
        const moved = engine.insert(next.k, key, count);
        m.foundryShipping -= moved;
        stock[key] -= moved;
        if (!stock[key]) delete stock[key];
      }
    }

    function cutter(c, dt) {
      const { x, y, rig, m } = c;
      if (!strip(rig)) {
        if (!resonance(rig)) return targetAt(x, y, rig);
        return [0, 1, -1, 2, -2].map(offset => targetAt(x + offset, y, rig, Math.abs(offset) === 1)).find(Boolean) || null;
      }
      const left = railLimit(m, 'foundryRailLeft'), right = railLimit(m, 'foundryRailRight');
      const head = Math.max(-left, Math.min(right, Number(m.foundryHead) || 0));
      let direction = m.foundryDirection === -1 ? -1 : 1;
      const available = Array.from({ length: left + right + 1 }, (_, index) => {
        const offset = index - left;
        return targetAt(x + offset, y, rig, Math.abs(offset) === 1);
      }).filter(Boolean);
      if (!available.length) return null;
      const next = () => direction > 0
        ? available.find(target => target.x - x >= head)
        : available.findLast(target => target.x - x <= head);
      let target = next();
      if (!target) { direction *= -1; target = next(); }
      const delta = target.x - x - head;
      m.foundryDirection = direction;
      m.foundryHead = head + Math.sign(delta) * Math.min(Math.abs(delta), dt * 1.4);
      return Math.abs(delta) > .01 ? { moving: true } : target;
    }

    function process(c, dt) {
      const { x, y, rig, m } = c;
      flushOutput(c, dt);
      if (mantle(rig)) collectPortCoolant(c);
      if (m.disabled || m.ruin) {
        return stop(m, 'Maintenance required', 'Use a Repair Kit from this panel or assign a Rig Tender.', 5);
      }
      if (!anchorSupported(x - 1, y + 2) || !anchorSupported(x + 1, y + 2)) {
        return stop(m, 'Anchor the rig', 'Put solid supporting blocks under both feet, one tile below each outer corner.', 1);
      }
      if (!m.powered || m.overload) {
        return stop(m, m.overload ? 'Power cable overloaded' : 'Connect power above',
          m.overload ? 'Use a higher-capacity cable on this circuit.' : `Supply ${rig.load} power to the PWR connection above the core.`, 2);
      }
      if (rig.torque && (!m.running || m.torque < rig.torque)) {
        return stop(m, 'Connect mechanical drive', `Supply ${rig.torque} torque at the left socket. A powered motor and matching shaft can drive it.`, 3);
      }
      const needsCoolant = mantle(rig) && !(m.foundryCoolant > 0);
      if (needsCoolant && !(m.contents?.[COOLANT] > 0)) {
        return stop(m, 'Coolant empty', 'Load a Coolant Canister at the upper blue port or connect a hopper to it.', 3);
      }
      if (strip(rig)) {
        const gap = railGap(c);
        if (gap !== null) return stop(m, 'Complete the gantry rail', `Place guide rail at column ${gap}, two tiles above the core, or shorten the travel limit.`, 3);
      }
      const fast = resonance(rig) && m.foundryMode === 'fast';
      const key = fast ? FRAGMENT : rig.ore.key;
      const amount = fast ? 3 : 2;
      const side = strip(rig) && m.foundryUnload === 'left' ? 'upper left' : 'lower right';
      const next = receiver(c, key, amount);
      if (!next) return stop(m, 'Connect an ore bin', `Attach a compatible storage bin or hopper to the ${side} output.`, 4);
      const queued = Object.values(m.outputs || {}).reduce((sum, count) => sum + count, 0);
      if (!next.fits || BUFFER - queued < amount + (needsCoolant ? 1 : 0)) {
        return stop(m, 'Output bin full', `Empty the ${side} receiver, or collect the rig's output from this panel.`, 4);
      }
      const target = cutter(c, dt);
      if (!target) return stop(m, 'No ore seam in reach', `Look for a deposit of ${rig.ore.name} no more than ${rig.reach} tiles below the head.`, 5);
      m.foundryFault = '';
      m.foundryDetail = target.moving ? 'The cutting head is traveling along the gantry.' : 'Extracting ore and feeding the attached receiver.';
      m.foundryStage = 5;
      m.working = true;
      if (target.moving) { m.foundryCut = null; m.progress = 0; return; }
      const targetKey = engine.key(target.x, target.y);
      if (m.foundryCut?.x !== target.x || m.foundryCut?.y !== target.y) m.progress = 0;
      m.foundryCut = target;
      if (engine.getMeta().get(targetKey)?.assembly) {
        return stop(m, 'Ore cannot be removed', 'Disconnect the seam from its assembly to free up the cut.', 5);
      }
      const depthSurge = mantle(rig) && !m.foundryStabilized && Math.sin(engine.elapsed * .085 + x * .13) > .85;
      if (depthSurge) return stop(m, 'Heat surge', 'The safety interlock will restart the rig when it cools. An anchored Stabilizer Walker keeps it working through the surge.', 5);
      m.progress = Math.min(1, (m.progress || 0) + dt / (rig.seconds * (fast ? .48 : 1)));
      if (m.progress < 1) return;
      if (engine.id(targetKey) !== rig.ore.id) { m.progress = 0; return; }
      set(target.x, target.y, T.AIR);
      if (get(target.x, target.y) !== T.AIR) return stop(m, 'Ore cannot be removed', 'The seam is protected by a connected assembly.', 5);
      m.outputs ||= {};
      if (needsCoolant) {
        if (--m.contents[COOLANT] === 0) delete m.contents[COOLANT];
        m.foundryCoolant = 4;
        m.outputs[EMPTY] = (m.outputs[EMPTY] || 0) + 1;
      }
      if (mantle(rig)) m.foundryCoolant--;
      m.outputs[key] = (m.outputs[key] || 0) + amount;
      m.progress = 0;
      m.foundryLastOre = key;
      m.totalMined = (m.totalMined || 0) + amount;
      stats.automined = (stats.automined || 0) + amount;
      engine.metrics.mined += amount;
      if (strip(rig) && !m.foundryStabilized) {
        m.foundryWear = (m.foundryWear || 0) + 1;
        if (m.foundryWear >= 12) {
          m.foundryWear = 0; m.maxHp = 100; m.hp = Math.max(0, (m.hp ?? 100) - 36);
          if (!m.hp) { m.ruin = true; m.disabled = true; }
        }
      }
    }

    function tick(dt = 0) {
      const playerX = player.x / tile, playerY = player.y / tile;
      for (const k of controllers) {
        const [x, y] = engine.xy(k), c = controller(x, y);
        if (!c) { controllers.delete(k); continue; }
        const near = Math.abs(x - playerX) < 36 && Math.abs(y - playerY) < 26;
        const horizon = near ? .15 : 1;
        c.m.foundryClock = (c.m.foundryClock || 0) + Math.max(0, dt);
        if (dt && c.m.foundryClock + 1e-8 < horizon) continue;
        const elapsed = Math.min(1.5, c.m.foundryClock);
        c.m.foundryClock = 0;
        process(c, elapsed);
      }
    }

    function describe(x, y) {
      const c = controller(x, y);
      if (!c) return null;
      const { rig, m } = c;
      const stage = m.foundryStage || 1;
      const status = m.foundryFault || (m.working ? 'Extracting ' + rig.ore.name : 'Rig awaiting connections');
      const detail = m.foundryDetail || 'Inspect the anchored footprint and supply its marked sockets.';
      const left = strip(rig) && m.foundryUnload === 'left';
      const missingRail = strip(rig) && m.foundryFault === 'Complete the gantry rail' ? railGap(c) : null;
      const focus = [
        { cells: [[c.x - 1, c.y + 2], [c.x + 1, c.y + 2]], color: '#dbacdc' },
        { cells: [[c.x, c.y - 1]], color: '#ffe892' },
        { cells: [missingRail !== null ? [missingRail, c.y - 2] : mantle(rig) ? [c.x + 1, c.y - 1] : [c.x - 1, c.y]], color: '#84ddfa' },
        { cells: [[c.x + (left ? -2 : 2), c.y + (left ? -1 : 1)]], color: '#91f1b8' }
      ][stage - 1];
      return {
        ...c, status, detail, focus,
        steps: [
          ['Anchor', stage > 1],
          ['Power', stage > 2],
          [mantle(rig) ? 'Coolant' : 'Drive', stage > 3],
          ['Output', stage > 4]
        ],
        options: strip(rig) ? {
          left: railLimit(m, 'foundryRailLeft'), right: railLimit(m, 'foundryRailRight'), unload: m.foundryUnload === 'left' ? 'left' : 'right'
        } : resonance(rig) ? { mode: m.foundryMode === 'fast' ? 'fast' : 'precision' } : null,
        coolant: mantle(rig) ? { loaded: (m.contents?.[COOLANT] || 0) + (data(c.x+1,c.y-1)?.contents?.[COOLANT] || 0), cuts: m.foundryCoolant || 0 } : null,
        integrity: Number.isFinite(m.hp) ? m.hp : 100
      };
    }

    function configure(x, y, action) {
      const c = controller(x, y);
      if (!c) return false;
      const { m, rig } = c;
      if (action === 'repair' && (m.hp < 100 || m.ruin || m.disabled)) {
        if (!(inventory.repairKit > 0)) { notify('A Repair Kit is required.'); return false; }
        inventory.repairKit--; m.hp = Math.min(100, (m.hp || 0) + 60); m.ruin = false; m.disabled = false;
        notify('Repair Kit installed.');
        return true;
      }
      if (action === 'coolant' && mantle(rig)) {
        const port = engine.key(c.x + 1, c.y - 1);
        const n = get(c.x + 1, c.y - 1) === T.DEEP_RIG_COOLANT ? engine.insert(port, COOLANT, inventory[COOLANT] || 0) : 0;
        if (n) inventory[COOLANT] -= n;
        notify(n ? `Loaded ${n} coolant canister${n === 1 ? '' : 's'} into the blue port.` : 'No canisters in your pack or the input is full.');
        return !!n;
      }
      if (action === 'mode' && resonance(rig)) {
        m.foundryMode = m.foundryMode === 'fast' ? 'precision' : 'fast';
      } else if (strip(rig) && action === 'unload') {
        m.foundryUnload = m.foundryUnload === 'left' ? 'right' : 'left';
      } else if (strip(rig) && (action === 'left' || action === 'right')) {
        const key = action === 'left' ? 'foundryRailLeft' : 'foundryRailRight';
        const next = railLimit(m, key) + 2;
        m[key] = next > 6 ? 2 : next;
      } else return false;
      m.progress = 0;
      m.foundryCut = null;
      return true;
    }

    function dismantle(x, y) {
      const c = controller(x, y);
      if (!c) return null;
      const cells = footprint(c.x, c.y, c.rig);
      const cargo = { [c.rig.key]: 1 };
      for (const cell of cells) {
        const m = data(cell.x, cell.y);
        if (m?.foundryX !== c.x || m?.foundryY !== c.y) continue;
        for (const stock of [m.contents, m.outputs]) {
          for (const [key, n] of Object.entries(stock || {})) if (n > 0) cargo[key] = (cargo[key] || 0) + n;
        }
      }
      if (engine.drops.length + Object.keys(cargo).length > 512) return { ok: false, message: 'Collect loose items before dismantling the rig.' };
      for (const [key, n] of Object.entries(cargo)) engine.spawn(key, n, c.x + .5, c.y + .5);
      for (const cell of cells) {
        const m = data(cell.x, cell.y);
        if (m?.foundryX === c.x && m?.foundryY === c.y) set(cell.x, cell.y, T.AIR);
      }
      controllers.delete(engine.key(c.x, c.y));
      return { ok: true, message: c.rig.name + ' recovered with its cargo.' };
    }

    function visible(left, top, right, bottom, draw) {
      for (const k of controllers) {
        const [x, y] = engine.xy(k);
        if (x < left - 7 || y < top - 10 || x > right + 7 || y > bottom + 2) continue;
        const c = controller(x, y);
        if (c) draw(x, y, c.rig, c.m);
      }
    }

    reset();
    return { placement, place, controller, describe, configure, dismantle, reset, tick, visible };
  }

  return { install, create, ores, rigs };
});
