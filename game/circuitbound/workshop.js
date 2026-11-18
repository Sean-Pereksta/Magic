(function (root, factory) {
  const workshop = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = workshop;
  else root.CircuitWorkshop = workshop;
})(globalThis, function () {
  'use strict';

  const REACH = 6;
  const STATIONS = { hand: 'Hand crafting', workbench: 'Field Workbench' };
  const HAND_RECIPES = new Set(['workbench', 'platform', 'ladder', 'ledge', 'rope', 'ropeLadder', 'scaffold', 'bridge', 'medkit']);
  const MINING_TOOLS = new Set(['reinforcedPickaxe', 'steelDrillTool', 'poweredMiningTool']);
  const WEAPONS = new Set(['hammer', 'scrapLauncher', 'stunTool', 'energyRifle']);
  const SOURCES = {
    dirt: 'Mine surface soil.',
    stone: 'Mine grey stone at the outpost or below the surface.',
    wood: 'Mine the timber by the outpost or the trees on the surface.',
    copper: 'Mine the orange-veined ore in the tunnel east of the outpost.',
    iron: 'Mine the pale metallic ore below the copper tunnel.',
    crystal: 'Mine glowing deposits deeper underground, or recover crystals from Spark Wisps.',
    obsidian: 'Mine dark bedrock near the bottom of the world.',
    scrap: 'Collect the scrap left behind by defeated scrapyard enemies.',
    cobaltOre: 'Mine a blue cobalt vein with a steel drill or an advanced mining rig. Refine the ore in an Induction Smelter.',
    iridiumOre: 'Mine a violet iridium vein with the charged powered mining tool or an Ion Bore. Refine the ore in an Induction Smelter.',
    aetheriumOre: 'Use a driven Plasma Excavator to cut deep aetherium veins. Refine the ore in an Induction Smelter.'
  };

  function install(T, defs, items, recipes) {
    // Keep previously assigned world tile IDs stable for loaded expeditions.
    T.WORKBENCH = 100;
    defs[T.WORKBENCH] = { name: 'Field Workbench', kind: 'structure', solid: true, hard: .55, drop: 'workbench', strength: 4, weight: 2 };
    items.push(
      { key: 'workbench', tile: T.WORKBENCH, name: 'Field Workbench', icon: '▤', color: '#db9f62' },
      { key: 'pickaxe', tile: -2, name: 'Basic Pickaxe', icon: '⛏', color: '#db9f62', tool: 'mining', permanent: true },
      { key: 'tool', tile: -2, name: 'Field Tool', icon: '⚔', color: '#b8cad0', weapon: true, permanent: true },
      { key: 'scrap', tile: -2, name: 'Scrap', icon: '◆', color: '#af9b77', resource: true },
      { key: 'reactorCell', tile: -2, name: 'Reactor Cell', icon: '◆', color: '#52dbcb', resource: true }
    );
    const link = items.find(item => item.key === 'link');
    if (link) link.permanent = true;
    recipes.unshift({
      key: 'workbench', name: 'Field Workbench', icon: '▤', out: 1, cost: { wood: 6, stone: 4 }, category: 'structure',
      desc: 'Place and open an unpowered bench to fabricate tools and engineering parts.'
    });
    const itemKeys = new Set(items.map(item => item.key));
    for (const recipe of recipes) {
      recipe.station = HAND_RECIPES.has(recipe.key) ? 'hand' : 'workbench';
      if (recipe.key === 'gear') recipe.desc = 'Takes rotation → changes speed or torque → drives a shaft or machine.';
      if (recipe.key === 'door') recipe.desc = 'Plugs a wall opening. Use the interact action to open or close it.';
      const weapon = WEAPONS.has(recipe.key) || recipe.weapon === true || !!globalThis.CircuitSurvival?.WEAPONS?.[recipe.key];
      if (itemKeys.has(recipe.key)) {
        if (weapon) items.find(item => item.key === recipe.key).weapon = true;
        continue;
      }
      items.push({
        key: recipe.key, tile: -2, name: recipe.name, icon: recipe.icon, color: '#d4be8b',
        tool: MINING_TOOLS.has(recipe.key) ? 'mining' : undefined,
        weapon, equipment: recipe.equipment
      });
    }
  }

  function migrate(inventory, stats, version) {
    if (version >= 4 || stats.workshopIntroduced) return;
    // A returning player can place a bench without having to spend existing materials.
    inventory.workbench = Math.max(1, inventory.workbench || 0);
    stats.workshopIntroduced = true;
  }

  function itemName(items, key) {
    const name = typeof items?.get === 'function' ? items.get(key)?.name : items?.find(item => item.key === key)?.name;
    if (name) return name;
    return key.replace(/([a-z])([A-Z])/g, '$1 $2').replace(/^./, first => first.toUpperCase());
  }

  function quantityLabel(items, contents) {
    return Object.entries(contents).map(([key, quantity]) => `${quantity} ${itemName(items, key)}`).join(' + ');
  }

  function technologyRequirement(recipe) {
    const chapter = recipe.chapter > 1 ? `Chapter ${recipe.chapter}` : '';
    const schematic = recipe.schematic ? `${recipe.schematicLabel || itemName(null, recipe.schematic)} schematic` : '';
    return [chapter, schematic].filter(Boolean).join(recipe.requiresSchematic ? ' and ' : ' or ');
  }

  function element(tag, className, description) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (description) node.textContent = description;
    return node;
  }

  function button(label, onClick, className) {
    const node = element('button', className, label);
    node.type = 'button';
    node.addEventListener('click', onClick);
    return node;
  }

  function machineRecipes(choices, activeRecipe, items, onSelect) {
    if (!choices?.length) return null;
    const label = element('label', 'machine-recipe-select', 'Recipe');
    const select = element('select');
    for (const recipe of choices) {
      const option = element('option', '', `${quantityLabel(items, recipe.out)} from ${quantityLabel(items, recipe.cost)}`);
      option.value = recipe.key;
      select.append(option);
    }
    select.value = activeRecipe?.key || choices[0].key;
    select.disabled = choices.length === 1;
    select.addEventListener('change', () => onSelect(select.value));
    label.append(select);
    return label;
  }

  function loadableInput(engine, k, cost, key) {
    const capacity = engine.capacity(k);
    const batchSize = Object.values(cost).reduce((sum, count) => sum + count, 0);
    const reserve = Math.floor(capacity / batchSize) * (cost[key] || 0);
    const current = engine.data(k).contents?.[key] || 0;
    return Math.max(0, Math.min(capacity - engine.stored(k), reserve - current));
  }

  function create(options) {
    const { inventory, items, recipes, defs, T, player, tile, get, distanceToTile, getSurvival, inventoryRoom,
      stats, assignToHotbar, sound, notify, refreshHotbar, openCraft, save } = options;
    const panel = document.getElementById('craftPanel');
    const title = document.getElementById('workshopTitle') || panel.querySelector('h2');
    const intro = document.getElementById('workshopIntro') || panel.querySelector('.modal-head p');
    const resources = document.getElementById('resourceChips');
    const contents = document.getElementById('recipes');
    const itemByKey = new Map(items.map(item => [item.key, item]));
    const formatName = key => itemName(itemByKey, key);
    const carried = key => inventory[key] || 0;
    const knownRecipes = recipes.map(recipe => ({ ...recipe, id: recipe.key }));
    for (const [tileId, def] of Object.entries(defs)) {
      for (const recipe of def.machine?.recipes || []) {
        const output = Object.keys(recipe.out)[0];
        knownRecipes.push({
          ...recipe, id: `powered:${tileId}:${recipe.key}`, output, tileId: Number(tileId), station: 'powered',
          name: formatName(output), stationName: def.name, icon: itemByKey.get(output)?.icon,
          desc: `${def.machine.time} s per batch · ${def.machine.load} power${def.machine.torque ? ` · ${def.machine.torque} torque` : ''}`
        });
      }
    }
    const recipesById = new Map(knownRecipes.map(recipe => [recipe.id, recipe]));
    const state = { view: 'craft', station: 'hand', showAll: false, selection: 'workbench', query: '', ingredient: null };

    const toolbar = element('div', 'workshop-controls craft-controls');
    const tabs = element('div', 'workshop-tabs craft-tabs');
    const craftTab = button('Crafting', () => show('craft'));
    const backpackTab = button('Backpack', () => show('backpack'));
    tabs.append(craftTab, backpackTab);
    const search = element('input');
    search.id = 'craftSearch';
    search.type = 'search';
    search.autocomplete = 'off';
    search.addEventListener('input', () => {
      state.query = search.value.trim().toLowerCase();
      state.selection = null;
      state.ingredient = null;
      render();
    });
    const filter = element('label', 'craft-affordable');
    const filterBox = element('input');
    filterBox.type = 'checkbox';
    filterBox.addEventListener('change', () => {
      state.showAll = filterBox.checked;
      state.selection = null;
      state.ingredient = null;
      render();
    });
    filter.append(filterBox, ' Show all recipes');
    toolbar.append(tabs, filter, search);
    contents.before(toolbar);
    contents.classList.add('workshop-content');

    function locateStations() {
      const found = new Map();
      const px = Math.floor((player.x + player.w / 2) / tile);
      const py = Math.floor((player.y + player.h / 2) / tile);
      for (let y = py - REACH; y <= py + REACH; y++) {
        for (let x = px - REACH; x <= px + REACH; x++) {
          const id = get(x, y);
          if (id !== T.WORKBENCH && !defs[id]?.machine?.recipes?.length) continue;
          const distance = distanceToTile(x, y);
          if (distance <= REACH && (found.get(id)?.distance ?? Infinity) > distance) found.set(id, { x, y, distance });
        }
      }
      return found;
    }

    function craftLimit(recipe, stations) {
      if (recipe.station === 'powered') return 0;
      if (recipe.station === 'workbench' && !stations.has(T.WORKBENCH)) return 0;
      if (getSurvival()?.canCraft?.(recipe) === false) return 0;
      const costs = Object.entries(recipe.cost);
      const batches = Math.min(...costs.map(([key, quantity]) => Math.floor(carried(key) / quantity)));
      const growth = recipe.out - costs.reduce((sum, [, quantity]) => sum + quantity, 0);
      const space = growth > 0 ? Math.floor(inventoryRoom() / growth) : Infinity;
      return Math.max(0, Math.min(batches, space, recipe.equipment ? 1 : Infinity));
    }

    function craft(recipe, maximum = false) {
      if (!recipe) return false;
      const count = Math.min(maximum ? Infinity : 1, craftLimit(recipe, locateStations()));
      if (count <= 0) return false;
      for (const [key, quantity] of Object.entries(recipe.cost)) inventory[key] -= quantity * count;
      inventory[recipe.key] = carried(recipe.key) + recipe.out * count;
      stats.crafted = (stats.crafted || 0) + count;
      if (itemByKey.get(recipe.key)?.weapon) getSurvival()?.selectWeapon?.(recipe.key);
      sound('craft');
      notify(`${recipe.name} crafted ×${recipe.out * count}`);
      refreshHotbar();
      save();
      if (state.view === 'craft') state.selection = recipe.id || recipe.key;
      render();
      const placement = contents.querySelector('[data-workshop-place]');
      (placement || contents.querySelector('[data-workshop-craft]:not(:disabled)') ||
        contents.querySelector('.workshop-detail h3'))?.focus({ preventScroll: true });
      if (!globalThis.matchMedia?.('(prefers-reduced-motion: reduce)').matches) {
        placement?.animate?.([{ transform: 'translateY(4px)', opacity: .55 }, { transform: 'translateY(0)', opacity: 1 }], { duration: 170, easing: 'ease-out' });
      }
      return true;
    }

    function itemIcon(key, fallback) {
      const view = element('span', 'recipe-icon');
      const makeIcon = options.icon || globalThis.CircuitArt?.icon;
      if (makeIcon) view.append(makeIcon(key, fallback || '◆'));
      else view.textContent = fallback || '◆';
      view.setAttribute('aria-hidden', 'true');
      return view;
    }

    function selectRecipe(id, focus = false) {
      state.selection = id;
      state.ingredient = null;
      render();
      if (focus) contents.querySelector('.workshop-detail h3')?.focus({ preventScroll: true });
    }

    function addPlacement(target, key) {
      const item = itemByKey.get(key);
      if (!options.readyToPlace || !item || item.tile < 0 || carried(key) <= 0) return false;
      const place = button(`Place ${item.name}`, () => options.readyToPlace(key), 'primary');
      place.setAttribute('data-workshop-place', key);
      target.append(place);
      return true;
    }

    function addAssignment(target, key) {
      if (!assignToHotbar || !itemByKey.has(key) || !(carried(key) || itemByKey.get(key)?.permanent)) return;
      const form = element('div', 'workshop-assignment');
      const label = element('label', '', 'Hotbar slot');
      const slots = element('select');
      const current = options.getHotbar?.() || Array(10).fill(null);
      for (let index = 0; index < current.length; index++) {
        const value = current[index];
        const slotItem = typeof value === 'string' ? value : value?.key;
        const option = element('option', '', `${(index + 1) % 10} · ${slotItem ? formatName(slotItem) : 'Empty'}`);
        option.value = index;
        slots.append(option);
      }
      const matching = current.findIndex(slot => slot === key || slot?.key === key);
      const empty = current.findIndex(slot => !slot);
      slots.value = String(matching >= 0 ? matching : empty >= 0 ? empty : Math.max(0, current.length - 1));
      label.append(slots);
      form.append(label, button('Assign to hotbar', () => {
        if (assignToHotbar(key, Number(slots.value)) === false) return;
        refreshHotbar();
        save();
        notify(`${formatName(key)} assigned to slot ${(Number(slots.value) + 1) % 10}`);
        render();
      }));
      target.append(form);
    }

    function addIngredients(target, recipe, counts) {
      const ingredients = element('ul', 'workshop-ingredients');
      for (const [key, quantity] of Object.entries(recipe.cost)) {
        const row = element('li');
        const owned = counts[key] || 0;
        row.classList.toggle('available', owned >= quantity);
        const label = button(`${formatName(key)} ${owned} / ${quantity}`, () => {
          state.ingredient = state.ingredient === key ? null : key;
          render();
          contents.querySelector('.workshop-source button')?.focus({ preventScroll: true });
        }, 'workshop-ingredient');
        label.setAttribute('aria-expanded', String(state.ingredient === key));
        row.append(label);
        ingredients.append(row);
      }
      target.append(ingredients);
      if (!state.ingredient) return;
      const key = state.ingredient;
      const source = SOURCES[key];
      const other = knownRecipes.find(entry => entry.id !== recipe.id && (entry.output || entry.key) === key);
      const note = element('div', 'workshop-source');
      note.append(element('p', '', source || (other ? `Make ${formatName(key)} at ${other.stationName || STATIONS[other.station]}.` : 'Recover this material during exploration.')));
      if (other) note.append(button('Show ingredient recipe', () => {
        state.showAll = true;
        state.query = '';
        search.value = '';
        selectRecipe(other.id, true);
      }));
      target.append(note);
    }

    function requirement(recipe, stations) {
      if (recipe.station === 'workbench' && !stations.has(T.WORKBENCH)) return 'Build or approach a Field Workbench.';
      if (getSurvival()?.canCraft?.(recipe) === false) return `Requires ${technologyRequirement(recipe)}.`;
      const missing = Object.entries(recipe.cost).filter(([key, quantity]) => carried(key) < quantity);
      if (missing.length) return `Need ${missing.map(([key, quantity]) => `${quantity - carried(key)} more ${formatName(key).toLowerCase()}`).join(' and ')}.`;
      return craftLimit(recipe, stations) ? 'Ready to craft.' : 'Make room in your backpack.';
    }

    function machineTarget(recipe, stations) {
      const engine = options.getEngine?.();
      const station = stations.get(recipe.tileId);
      if (!engine || !station) return null;
      return { engine, k: engine.key(station.x, station.y) };
    }

    function machineReadiness(recipe, { engine, k }, includeCarried = false) {
      const data = engine.data(k);
      const machine = defs[recipe.tileId].machine;
      const needs = Object.entries(recipe.cost).map(([key, quantity]) => [key, Math.max(0, quantity - (data.contents?.[key] || 0))]);
      const missing = needs.find(([key, amount]) => amount > (includeCarried ? carried(key) : 0));
      const spaceNeeded = needs.reduce((sum, [, count]) => sum + count, 0);
      const batch = Object.values(recipe.out).reduce((sum, count) => sum + count, 0);
      if (data.disabled || data.signalOK === false) return 'Stopped by its control signal. Enable the machine.';
      if (data.overload) return `Wire overloaded · ${data.load || 0} load / ${data.capacity || 0} capacity. Use a higher-capacity conductor.`;
      if (!data.powered) return `Needs ${machine.load} power. Connect a generator to the power input.`;
      if (machine.torque && (!data.running || data.torque < machine.torque)) return `Needs ${machine.torque} torque. Connect a motor to the drive input.`;
      if (spaceNeeded > engine.capacity(k) - engine.stored(k)) return 'Input buffer is full. Recover existing inputs to make room.';
      if (missing) return `Waiting for ${formatName(missing[0]).toLowerCase()}. Load ingredients or connect a hopper.`;
      if (engine.outputRoom(k) < batch) return 'Output storage full. Collect finished goods or add a receiver.';
      return '';
    }

    function loadMachine(recipe, key) {
      const target = machineTarget(recipe, locateStations());
      if (target?.engine.recipe(target.k)?.key !== recipe.key) return render();
      const want = Math.min(carried(key), loadableInput(target.engine, target.k, recipe.cost, key));
      const amount = target.engine.insert(target.k, key, want);
      inventory[key] = carried(key) - amount;
      notify(amount ? `Loaded ${amount} ${formatName(key)}` : 'The input buffer has no room.');
      refreshHotbar();
      save();
      render();
    }

    function collectMachine(recipe, bufferName) {
      const target = machineTarget(recipe, locateStations());
      if (!target) return render();
      const buffer = target.engine.data(target.k)[bufferName] || {};
      let collected = 0;
      for (const [key, amount] of Object.entries(buffer)) {
        const take = Math.min(amount, inventoryRoom());
        if (!take) continue;
        inventory[key] = carried(key) + take;
        buffer[key] -= take;
        if (!buffer[key]) delete buffer[key];
        collected += take;
      }
      notify(collected ? `Collected ${collected} items` : 'Make room in your backpack.');
      refreshHotbar();
      save();
      render();
    }

    function addPoweredActions(target, recipe, stations) {
      const found = machineTarget(recipe, stations);
      if (!found) {
        addIngredients(target, recipe, inventory);
        target.append(element('p', 'workshop-status', `Build or approach a ${recipe.stationName} to produce this batch.`));
        return;
      }
      const { engine, k } = found;
      const data = engine.data(k);
      const active = engine.recipe(k)?.key === recipe.key;
      addIngredients(target, recipe, data.contents || {});
      const status = !active ? 'Select this recipe to load it into the machine.' : machineReadiness(recipe, found) ||
        (data.working && data.activeRecipe === recipe.key ? 'Running' : 'Ready to run');
      target.append(element('p', 'workshop-status', status));
      const output = quantityLabel(itemByKey, data.outputs || {});
      if (output) target.append(element('p', '', `Finished goods · ${output}`));
      const controls = element('div', 'workshop-actions');
      if (!active) controls.append(button('Select machine recipe', () => {
        const next = machineTarget(recipe, locateStations());
        if (!next) return render();
        Object.assign(next.engine.data(next.k), { recipe: recipe.key, progress: 0 });
        save();
        render();
      }));
      if (active) {
        for (const key of Object.keys(recipe.cost)) {
          const load = button(`Load ${formatName(key)} (${carried(key)})`, () => loadMachine(recipe, key));
          load.disabled = carried(key) <= 0 || loadableInput(engine, k, recipe.cost, key) <= 0;
          controls.append(load);
        }
      }
      const collect = button('Collect finished goods', () => collectMachine(recipe, 'outputs'));
      collect.disabled = !Object.values(data.outputs || {}).some(amount => amount > 0);
      controls.append(collect);
      if (engine.stored(k) > 0) controls.append(button('Recover inputs', () => collectMachine(recipe, 'contents')));
      target.append(controls);
    }

    function recipeDetail(recipe, stations) {
      const detail = element('section', 'workshop-detail');
      const heading = element('h3', '', recipe.name);
      heading.tabIndex = -1;
      const yieldLabel = recipe.station === 'powered' ? quantityLabel(itemByKey, recipe.out) : `${recipe.out} per craft`;
      const technology = technologyRequirement(recipe);
      detail.append(
        heading,
        element('p', 'workshop-requirement', `${recipe.stationName || STATIONS[recipe.station]} · ${yieldLabel}${technology ? ` · ${technology}` : ''}`),
        element('p', 'workshop-description', recipe.desc)
      );
      if (recipe.station === 'powered') {
        addPoweredActions(detail, recipe, stations);
        addPlacement(detail, recipe.output);
        addAssignment(detail, recipe.output);
        return detail;
      }
      addIngredients(detail, recipe, inventory);
      const max = craftLimit(recipe, stations);
      const controls = element('div', 'workshop-actions');
      const canPlace = addPlacement(controls, recipe.key);
      const reason = requirement(recipe, stations);
      detail.append(element('p', 'workshop-status', canPlace ? `${carried(recipe.key)} ready to place. ${max ? reason : `For more, ${reason[0].toLowerCase()}${reason.slice(1)}`}` : reason));
      const one = button('Craft one', () => craft(recipe, false), canPlace ? '' : 'primary');
      one.setAttribute('data-workshop-craft', '');
      one.disabled = max < 1;
      controls.append(one);
      if (!recipe.equipment) {
        const many = button(max > 1 ? `Craft max (${max})` : 'Craft max', () => craft(recipe, true));
        many.disabled = max < 2;
        controls.append(many);
      }
      detail.append(controls);
      addAssignment(detail, recipe.key);
      return detail;
    }

    function renderInventory() {
      const list = element('div', 'workshop-inventory');
      const keys = new Set([...itemByKey.keys(), ...Object.keys(inventory)]);
      const owned = Array.from(keys).filter(key => (carried(key) > 0 || itemByKey.get(key)?.permanent || key === 'link') && formatName(key).toLowerCase().includes(state.query));
      owned.sort((a, b) => formatName(a).localeCompare(formatName(b)));
      for (const key of owned) {
        const item = itemByKey.get(key);
        const row = button('', () => selectRecipe(key), 'recipe workshop-recipe-option');
        row.setAttribute('aria-pressed', String(state.selection === key));
        row.append(itemIcon(key, item?.icon), element('b', '', formatName(key)), element('span', 'make', item?.permanent || key === 'link' ? '—' : `×${carried(key)}`));
        list.append(row);
      }
      const detail = element('section', 'workshop-detail');
      if (state.selection && owned.includes(state.selection)) {
        const selected = itemByKey.get(state.selection);
        const recipe = recipesById.get(state.selection);
        detail.append(element('h3', '', formatName(state.selection)));
        if (recipe || SOURCES[state.selection]) detail.append(element('p', '', recipe?.desc || SOURCES[state.selection]));
        addPlacement(detail, state.selection);
        addAssignment(detail, state.selection);
        if (recipe) detail.append(button('Show recipe', () => open('craft', 'hand', recipe.key)));
        if (selected?.resource && !recipe) {
          const production = knownRecipes.find(entry => entry.output === state.selection);
          if (production) detail.append(button('Show production recipe', () => open('craft', 'hand', production.id)));
        }
      } else detail.append(element('p', '', 'Choose an item to view it or assign a hotbar slot.'));
      if (!owned.length) list.append(element('p', 'craft-empty', 'No carried items match.'));
      contents.replaceChildren(detail, list);
    }

    function renderCraft(stations) {
      if (state.station === 'workbench' && !stations.has(T.WORKBENCH)) state.station = 'hand';
      const list = element('div', 'workshop-recipe-list');
      const visible = knownRecipes.filter(recipe => {
        const label = `${recipe.name} ${recipe.desc} ${recipe.category || ''} ${recipe.stationName || STATIONS[recipe.station]} ${Object.keys(recipe.cost).map(formatName).join(' ')}`.toLowerCase();
        if (state.query && !label.includes(state.query)) return false;
        if (state.showAll) return true;
        if (recipe.station !== 'powered') return craftLimit(recipe, stations) > 0;
        const found = machineTarget(recipe, stations);
        return found && !machineReadiness(recipe, found, true);
      });
      if (state.station === 'workbench') visible.sort((a, b) => Number(b.station === 'workbench') - Number(a.station === 'workbench'));
      const selected = recipesById.get(state.selection) || visible[0];
      for (const recipe of visible) {
        const row = button('', () => selectRecipe(recipe.id, true), 'recipe workshop-recipe-option');
        row.setAttribute('aria-pressed', String(selected?.id === recipe.id));
        const label = element('span');
        label.append(element('b', '', recipe.name), element('small', '', recipe.station === 'powered' ? `${recipe.stationName} · ${quantityLabel(itemByKey, recipe.cost)}` : STATIONS[recipe.station]));
        const output = recipe.station === 'powered' ? Object.values(recipe.out)[0] : recipe.out;
        row.append(itemIcon(recipe.output || recipe.key, recipe.icon), label, element('span', 'make', `×${output}`));
        list.append(row);
      }
      if (!visible.length) list.append(element('p', 'craft-empty', state.showAll ? 'No recipes match.' : 'No matching recipes are ready. Show all recipes to see their requirements.'));
      if (selected) contents.replaceChildren(recipeDetail(selected, stations), list);
      else contents.replaceChildren(list);
    }

    function render() {
      const craftView = state.view === 'craft';
      const stations = craftView && locateStations();
      title.textContent = craftView ? 'Crafting' : 'Backpack';
      if (intro) intro.textContent = craftView ? (stations.has(T.WORKBENCH) ? 'Field Workbench in reach' : 'Hand crafting') : `${Object.values(inventory).reduce((sum, amount) => sum + (amount || 0), 0)} / 2048 items`;
      search.placeholder = craftView ? 'Search recipes, ingredients or uses' : 'Search carried items';
      search.setAttribute('aria-label', search.placeholder);
      filter.hidden = !craftView;
      filterBox.checked = state.showAll;
      craftTab.classList.toggle('active', craftView);
      craftTab.setAttribute('aria-pressed', String(craftView));
      backpackTab.classList.toggle('active', !craftView);
      backpackTab.setAttribute('aria-pressed', String(!craftView));
      contents.classList.toggle('workshop-backpack', !craftView);
      if (resources) resources.hidden = true;
      if (craftView) renderCraft(stations); else renderInventory();
    }

    function show(view, station = 'hand', recipeId) {
      if (state.view !== view || state.station !== station || recipeId) {
        state.query = '';
        state.selection = recipeId || null;
        state.ingredient = null;
        search.value = '';
      }
      state.view = view;
      state.station = station;
      if (recipeId) state.showAll = true;
      render();
    }

    function open(view = 'craft', station = 'hand', recipeId) {
      show(view, station, recipeId);
      openCraft();
    }

    return { open, show, render, craft };
  }

  return { install, migrate, create, machineRecipes, loadableInput };
});
