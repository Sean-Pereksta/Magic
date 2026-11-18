(function (root, factory) {
  const expedition = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = expedition;
  else root.CircuitFoundryMap = expedition;
})(globalThis, function () {
  'use strict';

  const colors = {
    site: '#ffc77e', survey: '#f0a9e0', outpost: '#82f2c6', marker: '#edeee3',
    station: '#83d1ff', lift: '#c4aeff'
  };
  const terrainColors = ['#536868', '#48675c', '#65644c', '#645775', '#6a4a72', '#70403d'];
  const scopes = [112, 280, 2048];

  function create(options) {
    const { panel, getWorld, locate, stations, lifts, terrain, canTravel, travel, scan, canScan, save } = options;
    const canvas = panel.querySelector('canvas');
    const ctx = canvas.getContext('2d');
    const list = panel.querySelector('#foundryLocations');
    const details = panel.querySelector('#foundrySelection');
    const info = panel.querySelector('#foundryMapReadout');
    const status = panel.querySelector('#foundryMapStatus');
    const scanButton = panel.querySelector('#foundryScan');
    const travelButton = panel.querySelector('#foundryTravel');
    const pinButton = panel.querySelector('#foundryPin');
    const removeButton = panel.querySelector('#foundryRemove');
    const nameInput = panel.querySelector('#foundryOutpostName');
    const zoom = panel.querySelector('#foundryZoom');
    let selected = null;
    let center = null;
    let frame = null;

    function allLocations(world) {
      return [
        ...world.markers,
        ...world.surveyMarkers,
        ...world.sites.filter(site => site.kind !== 'deposit' && world.isExplored(site.x, site.y))
          .map(site => ({ ...site, kind: 'site' })),
        ...stations().filter(point => world.isExplored(point.x, point.y)).map(point => ({ ...point, id: `station-${point.x},${point.y}`, kind: 'station' })),
        ...lifts().filter(point => world.isExplored(point.x, point.y)).map(point => ({ ...point, id: `lift-${point.x},${point.y}`, kind: 'lift' }))
      ];
    }

    function draw(world, locations) {
      const position = locate();
      const anchor = center || position;
      const width = canvas.width;
      const height = canvas.height;
      const span = Math.min(scopes[Number(zoom.value)] || scopes[0], world.width);
      const scale = Math.min(width / span, height / Math.min(world.height, span * height / width));
      const left = Math.max(0, Math.min(world.width - span, anchor.x - span / 2));
      const rows = Math.min(world.height, height / scale);
      const top = Math.max(0, Math.min(world.height - rows, anchor.y - rows / 2));
      const point = (x, y) => [(x - left) * scale, (y - top) * scale];
      frame = { locations, point, scale, left, top };
      ctx.fillStyle = '#080e18';
      ctx.fillRect(0, 0, width, height);

      for (const [x, y] of world.exploredCells()) {
        if (x + 8 < left || x > left + span || y + 8 < top || y > top + rows) continue;
        const region = y < 84 ? 0 : y < 120 ? 1 : y < 196 ? 2 : y < 280 ? 3 : y < 368 ? 4 : 5;
        ctx.fillStyle = terrainColors[region];
        const [px, py] = point(x, y);
        ctx.fillRect(Math.floor(px), Math.floor(py), Math.ceil(8 * scale), Math.ceil(8 * scale));
        ctx.fillStyle = region ? '#151b26' : '#142934';
        for (let dy = 0; dy < 8; dy += 2) for (let dx = 0; dx < 8; dx += 2) {
          if (!terrain(x + dx, y + dy)) ctx.fillRect(px + dx * scale, py + dy * scale, 2 * scale + .4, 2 * scale + .4);
        }
      }

      const active = locations.find(entry => entry.id === world.pinned);
      if (active) {
        ctx.strokeStyle = '#82f2c680';
        ctx.setLineDash([8, 6]);
        ctx.beginPath();
        ctx.moveTo(...point(position.x, position.y));
        ctx.lineTo(...point(active.x, active.y));
        ctx.stroke();
        ctx.setLineDash([]);
      }
      for (const entry of locations) {
        const [x, y] = point(entry.x, entry.y);
        ctx.fillStyle = entry.powered === false ? '#66808c' : colors[entry.kind] || colors.site;
        ctx.beginPath();
        ctx.arc(x, y, entry.id === selected ? 7 : 4.5, 0, Math.PI * 2);
        ctx.fill();
        if (entry.id === selected) { ctx.strokeStyle = '#fff'; ctx.stroke(); }
      }
      const [heroX, heroY] = point(position.x, position.y);
      ctx.fillStyle = '#fff8c5';
      ctx.fillRect(heroX - 3, heroY - 3, 6, 6);
    }

    function refresh() {
      const world = getWorld();
      if (!world?.exploredCells) return;
      const position = locate();
      const locations = allLocations(world);
      const current = locations.find(entry => entry.id === selected);
      const sorted = locations.slice().sort((a, b) =>
        Math.hypot(a.x - position.x, a.y - position.y) - Math.hypot(b.x - position.x, b.y - position.y));
      info.textContent = `Column ${position.x} · Depth ${position.y}`;
      scanButton.disabled = !canScan();
      pinButton.disabled = !current;
      pinButton.textContent = current && world.pinned === current.id ? 'Unpin route' : 'Pin route';
      removeButton.hidden = !current || current.kind !== 'outpost';
      travelButton.hidden = current?.kind !== 'station';
      const route = current?.kind === 'station' ? canTravel(current) : null;
      travelButton.disabled = !route?.ok;
      details.textContent = current ? [current.name, `Depth ${current.y}`, route?.reason].filter(Boolean).join(' · ') : 'Select a location to pin a route.';
      list.replaceChildren(...sorted.map(entry => {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'icon-btn foundry-location';
        button.textContent = `${entry.name} · ${entry.y}`;
        button.setAttribute('aria-pressed', String(entry.id === selected));
        button.addEventListener('click', () => {
          select(entry);
          Array.from(list.children).find(node => node.getAttribute('aria-pressed') === 'true')?.focus({ preventScroll: true });
        });
        return button;
      }));
      draw(world, locations);
    }

    function select(entry) {
      selected = entry.id;
      center = entry;
      status.textContent = '';
      refresh();
    }

    canvas.addEventListener('click', event => {
      if (!frame) return;
      const rect = canvas.getBoundingClientRect();
      const px = (event.clientX - rect.left) * canvas.width / rect.width;
      const py = (event.clientY - rect.top) * canvas.height / rect.height;
      let nearest = null;
      let distance = 15;
      for (const entry of frame.locations) {
        const [x, y] = frame.point(entry.x, entry.y);
        const next = Math.hypot(x - px, y - py);
        if (next < distance) { nearest = entry; distance = next; }
      }
      if (nearest) select(nearest);
      else if (Number(zoom.value) === 2) {
        center = { x: frame.left + px / frame.scale, y: frame.top + py / frame.scale };
        zoom.value = '1';
        refresh();
      }
    });
    zoom.addEventListener('change', refresh);
    panel.querySelector('#foundryRecenter').addEventListener('click', () => { center = null; refresh(); });
    scanButton.addEventListener('click', () => { status.textContent = scan(); refresh(); });
    pinButton.addEventListener('click', () => {
      const world = getWorld();
      if (!world || !selected) return;
      world.pinned = world.pinned === selected ? null : selected;
      save();
      refresh();
    });
    travelButton.addEventListener('click', () => {
      const destination = frame?.locations.find(entry => entry.id === selected);
      if (destination) status.textContent = travel(destination) || '';
    });
    panel.querySelector('#foundryAddOutpost').addEventListener('click', () => {
      const name = nameInput.value.trim().slice(0, 36);
      const world = getWorld();
      if (!world || !name) { nameInput.focus(); return; }
      const { x, y } = locate();
      const id = `outpost-${x},${y}`;
      const existing = world.markers.find(entry => entry.id === id);
      const marker = { id, x, y, name, kind: 'outpost' };
      if (existing) existing.name = name;
      else if (world.markers.length < 64) world.markers.push(marker);
      else { status.textContent = 'Remove an outpost before adding another.'; return; }
      nameInput.value = '';
      save();
      select(marker);
    });
    removeButton.addEventListener('click', () => {
      const world = getWorld();
      const index = world.markers.findIndex(entry => entry.id === selected);
      if (index < 0) return;
      world.markers.splice(index, 1);
      if (world.pinned === selected) world.pinned = null;
      selected = null;
      save();
      refresh();
    });

    return {
      open() { center = null; selected = getWorld()?.pinned || null; status.textContent = ''; refresh(); },
      pinned() {
        const world = getWorld();
        return world?.pinned ? allLocations(world).find(entry => entry.id === world.pinned) : null;
      }
    };
  }

  return { create };
});
