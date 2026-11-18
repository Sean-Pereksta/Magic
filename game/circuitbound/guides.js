(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.CircuitGuides = factory();
})(typeof globalThis === 'object' ? globalThis : this, function () {
  'use strict';

  const directions = ['right', 'down', 'left', 'up'];
  const vectors = [[1, 0], [0, 1], [-1, 0], [0, -1]];
  const structure = new Set(['platform', 'ladder']);
  const movingParts = new Set(['platform', 'counterweight']);
  const orientationMatters = new Set(['motor', 'drill', 'hopper', 'storage', 'sensor', 'autoDoor', 'winch', 'switch']);
  const starterStone = [[6, 26], [5, 26], [6, 25], [5, 25]];
  const legacyStarterStone = [[9, 26], [11, 26], [9, 25], [11, 25]];
  const explanations = {
    generator: 'Supplies power to the adjacent machine.',
    motor: 'Turns electrical power into drive at its front port.',
    drill: 'Takes power and drive, then sends mined resources through its item port.',
    hopper: 'Carries mined resources into storage.',
    storage: 'Receives mined resources. Collect its contents to free space.',
    sensor: 'Sends an open signal when a player approaches.',
    signal: 'Carries the sensor signal to the doorway.',
    autoDoor: 'Receives power and a signal at its open input.',
    winch: 'Takes drive from behind and controls the deck below.',
    switch: 'Changes the lift between raising and lowering.',
    platform: 'Makes up the moving deck.',
    counterweight: 'Moves opposite the deck and offsets its load.',
    rail: 'Keeps the deck and weight on their tracks.'
  };
  const lessons = {
    'automatic-miner': { title: 'Drill to storage', site: 'Aim at open space with mineable rock within six tiles below the drill.' },
    'sensor-door': { title: 'Sensor-operated door', site: 'Aim at a clear column four tiles tall.' },
    elevator: { title: 'Working winch lift', site: 'Aim at a clear shaft with room above and below the deck.' },
    'counterweight-lift': { title: 'Counterweighted lift', site: 'Aim at a wide clear shaft with room for the deck and counterweight.' }
  };

  function create({ T, items, inventory, get, inside, getMeta, canPlace, place, link, notify, save,
    blueprints = globalThis.CircuitInterface?.blueprints || [] }) {
    const catalog = new Map(blueprints.filter(plan => lessons[plan.id]).map(plan => [plan.id, plan]));
    const item = key => items.find(entry => entry.key === key);
    const fresh = legacy => ({ starter: { done: !!legacy, benchOpened: false, structurePlaced: false, bench: null }, activeId: null, records: {} });
    const inBounds = point => Array.isArray(point) && point.length === 2 && point.every(Number.isInteger) && inside(...point);
    const readMeta = (x, y) => getMeta(x, y) || {};
    let progress = fresh(false);

    function model(id = progress.activeId) {
      const plan = catalog.get(id), record = plan && progress.records[id];
      return record ? { plan, record, lesson: lessons[id] } : null;
    }

    function cells(plan, x, y) {
      return plan.placements.map(part => ({ ...part, x: x + part.x, y: y + part.y }));
    }

    function matches(part) {
      return get(part.x, part.y) === item(part.key)?.tile &&
        (!orientationMatters.has(part.key) || (readMeta(part.x, part.y).rot || 0) === part.rot);
    }

    function nextPart(plan, record) {
      if (!record.anchor) return null;
      return cells(plan, ...record.anchor).find(part => !(record.built && movingParts.has(part.key)) && !matches(part)) || null;
    }

    function starterObjective() {
      const starter = progress.starter;
      if (starter.done) return null;
      if (!starter.benchOpened) {
        const title = 'Build a Field Workbench';
        if (starter.bench && get(...starter.bench) === T.WORKBENCH) {
          return { title, message: 'Open the placed Workbench with Interact.', markers: [{ at: starter.bench, label: 'Open' }] };
        }
        if ((inventory.workbench || 0) > 0) return { title, message: 'Place the Workbench from your hotbar or Backpack.',
          markers: get(10, 28) === T.AIR ? [{ at: [10, 28], label: 'Place the Workbench' }] : [] };
        const wood = Math.max(0, 6 - (inventory.wood || 0)), stone = Math.max(0, 4 - (inventory.stone || 0));
        if (!wood && !stone) return { title, message: 'Craft the Field Workbench in Crafting (E). It costs 6 wood and 4 stone.', action: 'craft-workbench' };
        const markers = [];
        for (const [missing, id, label, spots] of [
          [wood, T.WOOD, 'Mine wood', [[7, 26], [8, 26], [7, 25], [8, 25], [7, 24], [8, 24]]],
          [stone, T.STONE, 'Mine stone', [...starterStone, ...legacyStarterStone]]
        ]) {
          const at = missing && spots.find(([x, y]) => get(x, y) === id);
          if (at) markers.push({ at, label });
        }
        const need = [wood && `${wood} wood`, stone && `${stone} stone`].filter(Boolean).join(' and ');
        return { title, message: markers.length ? `Mine and collect ${need} by the outpost.` : `Walk close to the loose materials to collect ${need}.`, markers };
      }
      if (!starter.structurePlaced) {
        const woodAvailable = (inventory.wood || 0) >= 2;
        const woodInStorage = get(14, 28) === T.STORAGE && readMeta(14, 28).contents?.wood >= 2;
        const useStorage = !woodAvailable && woodInStorage;
        return { title: 'Build a ladder or platform', action: 'craft-ladder',
          message: useStorage ? 'Interact with the marked Storage Bin and take its wood, then craft and place a Ladder.' : 'Craft a Ladder from 2 wood and place it from your hotbar.',
          markers: useStorage ? [{ at: [14, 28], label: 'Collect wood' }]
            : get(13, 27) === T.AIR ? [{ at: [13, 27], label: 'Place a ladder' }] : [] };
      }
      const title = 'Repair the outpost';
      if (get(10, 23) !== T.CORE) return null;
      if (get(9, 27) !== T.WIRE) return {
        title,
        message: `${inventory.wire ? 'Place' : 'Craft and place'} 1 Copper Wire in the marked gap to power the Foundry Core.`,
        markers: [{ at: [9, 27], label: 'Copper Wire' }]
      };
      if (!readMeta(10, 23).powered) return { title, message: 'Inspect the Foundry Core and reconnect its power line.', markers: [{ at: [10, 23], label: 'Check power' }] };
      if (get(11, 28) !== T.GENERATOR) return {
        title,
        message: `${inventory.generator ? 'Place' : 'Craft and place'} 1 Hand Generator at the marker to drive the motor.`,
        markers: [{ at: [11, 28], label: 'Hand Generator' }]
      };
      if (!readMeta(12, 28).running) return { title, message: 'Inspect the motor and check its rear power input.', markers: [{ at: [12, 28], label: 'Check drive' }] };
      return null;
    }

    function guideStatus(plan, record, lesson) {
      const result = { id: plan.id, title: lesson.title, active: !record.paused && !record.done, paused: record.paused, complete: record.done };
      if (record.done) return { ...result, message: 'Build demonstrated.' };
      if (!record.anchor) return { ...result, message: lesson.site, placing: true };
      const part = nextPart(plan, record);
      if (part) {
        const entry = item(part.key), count = inventory[part.key] || 0, direction = orientationMatters.has(part.key) ? ` facing ${directions[part.rot]}` : '';
        const problem = get(part.x, part.y) === entry.tile && orientationMatters.has(part.key)
          ? `Rotate the ${entry.name} ${directions[part.rot]}.`
          : canPlace(part.x, part.y, entry);
        const message = `${count ? 'Place' : 'Craft'} ${entry.name}${direction} · 1 needed, ${count} owned. ${problem || explanations[part.key]}`;
        return { ...result, message, placing: true, part, problem };
      }
      return { ...result, ...demonstration(plan, record) };
    }

    function demonstration(plan, record) {
      const placed = cells(plan, ...record.anchor);
      const part = key => placed.find(entry => entry.key === key);
      const data = key => { const p = part(key); return readMeta(p.x, p.y); };
      const marker = (key, label) => { const p = part(key); return [{ at: [p.x, p.y], label }]; };
      if (plan.id === 'automatic-miner') {
        const drill = data('drill'), storage = data('storage');
        return {
          passed: drill.totalMined > 0 && Object.values(storage.contents || {}).some(count => count > 0),
          message: drill.machineFault ? `Drill: ${formatFault(drill.machineFault)}.` : 'Watch the drill mine and send its first resources into storage.',
          markers: marker(drill.machineFault ? 'drill' : 'storage', drill.machineFault ? 'Inspect drill' : 'Ore arrives here')
        };
      }
      if (plan.id === 'sensor-door') {
        const door = data('autoDoor');
        return { passed: door.powered && door.signalInputs?.EN && door.open,
          message: door.powered ? 'Approach the sensor and watch the powered door open.' : 'Connect the generator to the bottom power port.',
          markers: marker('autoDoor', 'Approach door') };
      }
      const winch = data('winch'), deck = placed.filter(entry => entry.key === 'platform');
      const moved = record.deck && winch.liftAssembly === record.deck && deck.every(p => readMeta(p.x, p.y).assembly !== record.deck);
      return { passed: !!moved, message: winch.fault && winch.fault !== 'NO LOAD BELOW'
        ? `Lift: ${formatFault(winch.fault)}. Interact with the switch or clear the shaft.`
        : 'Interact with the switch to reverse the lift. Keep the linked deck and shaft clear.', markers: marker('switch', 'Lift direction') };
    }

    function formatFault(fault) {
      return fault[0].toUpperCase() + fault.slice(1).toLowerCase();
    }

    function anchorProblem(plan, x, y) {
      const layout = cells(plan, x, y);
      if (!layout.every(part => inside(part.x, part.y))) return 'Move the guide away from the edge of the world.';
      if (layout.some(part => get(part.x, part.y) !== T.AIR)) return 'Choose open space or clear the highlighted tiles.';
      if (plan.id === 'automatic-miner') {
        const drill = layout.find(part => part.key === 'drill');
        const mineable = [T.GRASS, T.DIRT, T.STONE, T.WOOD, T.COPPER, T.IRON, T.CRYSTAL, T.OBSIDIAN];
        for (let yy = drill.y + 1; yy <= drill.y + 6 && inside(drill.x, yy); yy++) {
          const tile = get(drill.x, yy);
          if (mineable.includes(tile)) return '';
          if (tile !== T.AIR) break;
        }
        return 'Aim with mineable terrain within six tiles under the drill.';
      }
      return '';
    }

    function current() {
      const active = model();
      return active ? guideStatus(active.plan, active.record, active.lesson) : null;
    }

    function objective() {
      const active = current();
      return active?.active ? active : starterObjective();
    }

    function start(plan) {
      const id = typeof plan === 'string' ? plan : plan?.id;
      if (!catalog.has(id)) return false;
      const previous = model();
      if (previous && previous.plan.id !== id) previous.record.paused = true;
      let record = progress.records[id];
      if (!record || record.done) record = progress.records[id] = { anchor: null, paused: false, built: false, done: false, deck: null };
      progress.activeId = id;
      record.paused = false;
      save();
      return true;
    }

    function pause(value = true) {
      const active = model();
      if (!active || active.record.done || active.record.paused === value) return;
      active.record.paused = value;
      save();
    }

    function benchOpened() {
      if (progress.starter.done || progress.starter.benchOpened) return;
      progress.starter.benchOpened = true;
      save();
    }

    function placed(key, x, y) {
      const starter = progress.starter;
      if (starter.done) return;
      if (key === 'workbench' && !starter.benchOpened && inBounds([x, y])) starter.bench = [x, y];
      else if (starter.benchOpened && !starter.structurePlaced && structure.has(key)) starter.structurePlaced = true;
      else return;
      save();
    }

    function act(x, y) {
      const active = model();
      if (!active || active.record.paused || active.record.done || !inside(x, y)) return false;
      const { plan, record } = active;
      if (!record.anchor) {
        const reason = anchorProblem(plan, x, y);
        if (reason) { notify(reason); return true; }
        record.anchor = [x, y];
        save();
        return true;
      }
      const part = nextPart(plan, record);
      if (!part || part.x !== x || part.y !== y) return false;
      const entry = item(part.key);
      if ((inventory[part.key] || 0) < 1) { notify(`Craft 1 ${entry.name} before placing this step.`); return true; }
      const blockage = canPlace(x, y, entry);
      if (blockage) { notify(blockage); return true; }
      const extra = part.key === 'winch' ? { ...part.meta, manualPaused: !record.built } : part.meta;
      if (place(x, y, entry, part.rot, extra)) { tick(); save(); }
      return true;
    }

    function tick() {
      const starter = progress.starter;
      if (!starter.done && starter.benchOpened && starter.structurePlaced &&
        get(9, 27) === T.WIRE && get(11, 28) === T.GENERATOR && readMeta(10, 23).powered && readMeta(12, 28).running) {
        starter.done = true;
        notify('Outpost working. Open Blueprints for guided engineering builds.');
        save();
      }
      const active = model();
      if (!active || active.record.paused || active.record.done || !active.record.anchor) return;
      const { plan, record, lesson } = active;
      if (nextPart(plan, record)) return;
      if (!record.built) {
        const [x, y] = record.anchor;
        for (const group of plan.links || []) {
          const id = link(group.map(([gx, gy]) => [x + gx, y + gy]));
          if (!record.deck) record.deck = id;
        }
        const winch = cells(plan, x, y).find(part => part.key === 'winch');
        if (winch) readMeta(winch.x, winch.y).manualPaused = false;
        record.built = true;
        notify(plan.links?.length ? 'Deck linked. Test the lift with its direction switch.' : `${lesson.title} assembled. Test it now.`);
        save();
      }
      if (demonstration(plan, record).passed) {
        record.done = true;
        notify(plan.id === 'automatic-miner' ? 'Ore reached storage. Collect it when full to resume drilling.' : `${lesson.title} works.`);
        save();
      }
    }

    function draw(ctx, tileRect, pointer) {
      const active = model();
      const selected = active && !active.record.paused && !active.record.done;
      const targets = [];
      if (selected) {
        const { plan, record } = active;
        if (!record.anchor && pointer && inside(pointer.tx, pointer.ty)) {
          const { tx, ty } = pointer;
          const color = anchorProblem(plan, tx, ty) ? '#ff9077' : '#99d9ce';
          for (const part of cells(plan, tx, ty)) targets.push({ at: [part.x, part.y], color, muted: true });
        } else {
          const part = nextPart(plan, record);
          if (part) {
            const entry = item(part.key), blocked = canPlace(part.x, part.y, entry) || !inventory[part.key];
            targets.push({ at: [part.x, part.y], color: blocked ? '#ff9077' : '#a6ffdc', label: entry.name, rot: part.rot });
          } else if (record.anchor) targets.push(...demonstration(plan, record).markers);
        }
      } else targets.push(...(starterObjective()?.markers || []));
      if (!targets.length) return;
      ctx.save();
      ctx.font = '600 11px system-ui';
      ctx.textAlign = 'center';
      for (const { at, label, rot, muted, color = '#ffce84' } of targets) {
        const { x, y } = tileRect(...at);
        ctx.strokeStyle = color;
        ctx.globalAlpha = muted ? .55 : 1;
        ctx.lineWidth = muted ? 1 : 2;
        ctx.setLineDash(muted ? [3, 3] : [5, 3]);
        ctx.strokeRect(x + 2, y + 2, 20, 20);
        ctx.setLineDash([]);
        if (label) {
          ctx.lineWidth = 3;
          ctx.strokeStyle = '#102129';
          ctx.strokeText(label, x + 12, y - 4);
          ctx.fillStyle = color;
          ctx.fillText(label, x + 12, y - 4);
        }
        if (rot !== undefined) {
          const [dx, dy] = vectors[rot];
          const ax = x + 12 + dx * 7, ay = y + 12 + dy * 7;
          ctx.strokeStyle = color;
          ctx.lineWidth = 2;
          ctx.beginPath();
          ctx.moveTo(x + 12 - dx * 6, y + 12 - dy * 6);
          ctx.lineTo(ax, ay);
          ctx.moveTo(ax - dx * 5 - dy * 4, ay - dy * 5 + dx * 4);
          ctx.lineTo(ax, ay);
          ctx.lineTo(ax - dx * 5 + dy * 4, ay - dy * 5 - dx * 4);
          ctx.stroke();
        }
      }
      ctx.restore();
    }

    function restore(data, { legacy = false } = {}) {
      progress = fresh(legacy && !data);
      if (!data || typeof data !== 'object') return;
      const starter = data.starter || {};
      progress.starter = { done: !!starter.done, benchOpened: !!starter.benchOpened, structurePlaced: !!starter.structurePlaced,
        bench: inBounds(starter.bench) ? [...starter.bench] : null };
      for (const id of catalog.keys()) {
        const record = data.records?.[id];
        if (!record || typeof record !== 'object') continue;
        const anchor = inBounds(record.anchor) && cells(catalog.get(id), ...record.anchor).every(part => inside(part.x, part.y)) ? [...record.anchor] : null;
        progress.records[id] = { anchor, paused: !!record.paused, built: !!(anchor && record.built), done: !!(anchor && record.built && record.done),
          deck: Number.isSafeInteger(record.deck) && record.deck > 0 ? record.deck : null };
      }
      if (progress.records[data.activeId] && catalog.has(data.activeId)) progress.activeId = data.activeId;
    }

    return { start, pause, resume: () => pause(false), current, objective, act, tick, draw, benchOpened, placed,
      isPlacing: () => { const active = current(); return !!(active?.active && active.placing); },
      reset: () => { progress = fresh(false); }, export: () => structuredClone(progress), restore };
  }

  return { create, starterStone };
});
