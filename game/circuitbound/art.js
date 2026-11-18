(function () {
  'use strict';

  const assetRoot = new URL('.', document.currentScript.src);
  const cellSize = 128;
  const tileNames = [
    'grass', 'dirt', 'stone', 'wood', 'copper', 'iron',
    'crystal', 'obsidian', 'platform', 'copperBlock', 'ironBlock', 'ladder',
    'wire', 'signal', 'generator', 'switch', 'sensor', 'motor',
    'shaft', 'gear', 'piston', 'lamp', 'conveyor', 'winch',
    'door', 'doorOpen', 'core', 'workbench', 'drill', 'storage',
    'hopper', 'smelter', 'crusher', 'battery', 'autoDoor', 'turret'
  ];
  const actorNames = [
    'idle', 'walkA', 'walkB', 'climbA', 'climbB',
    'mineWindup', 'mineStrike', 'attackWindup', 'attackStrike', 'jump',
    'beetle', 'crawler', 'wisp', 'burrower', 'drone',
    'leech', 'siege', 'sentinel', 'guardian', 'cache',
    'pickaxe', 'tool', 'link', 'repairKit', 'energyCell'
  ];
  const partNames = [
    'and', 'or', 'not', 'timer', 'counter', 'circuit',
    'clutch', 'belt', 'pulley', 'bevel', 'smallGear', 'largeGear',
    'bearing', 'hinge', 'splitter', 'filter', 'frame', 'beam',
    'refiner', 'assembler', 'ammoFabricator', 'advancedFabricator', 'repairStation', 'recoveryStation',
    'arcTower', 'teslaCoil', 'barrier', 'trap', 'metalScaffold', 'rail',
    'steel', 'copperIngot', 'ironIngot', 'ammo', 'repairKit', 'pickaxe'
  ];
  const enemyNames = ['beetle', 'crawler', 'wisp', 'burrower', 'drone', 'leech', 'siege', 'sentinel', 'guardian'];
  const machineNames = ['motor', 'conveyor', 'drill', 'generator', 'winch', 'piston', 'smelter', 'crusher', 'gear'];
  const fixtureNames = ['shaft', 'belt', 'pulley', 'bearing', 'refiner', 'assembler', 'turret', 'sensor', 'lamp'];
  const robotNames = ['collectionDrone', 'miningDrone', 'defenseDrone', 'starship', 'ionBore', 'plasmaExcavator', 'reclaimerDock', 'roboticsFoundry', 'reactorGenerator'];
  const rareWeapons = ['phaseBlade', 'coilLance', 'voidPulser', 'siegeRailcannon', 'sunspikeLaser'];
  const robotPartNames = [
    'cobaltOre', 'iridiumOre', 'aetheriumOre', 'cobaltIngot', 'iridiumIngot', 'aetheriumIngot',
    'cobaltPlate', 'iridiumShell', 'aetheriumPanel', 'temperedGlass', 'refractoryBrick', 'honeycombFrame',
    'trussBeam', 'electricBus', 'superCable', 'fiberWire', 'armoredPlatform', 'gantryDeck',
    'serviceLadder', 'magneticRail', 'bulkhead', 'flightDeck', 'anchorPylon', 'ballastCore',
    'hangarGrate', 'armoredScaffold', 'miningDroneDock', 'defenseDroneDock', 'oreCollectionBin', 'salvageCollectionBin',
    'defenseSupplyBin', 'reactorBattery', 'servoShaft', 'starshipPad', 'cargoSilo', 'alloyRack'
  ];
  const machineIds = new Set(machineNames.concat(fixtureNames));
  const robotIds = new Set(robotNames);
  const robotComponentNames = [
    'inductionSmelter','servoMotor','droneBeacon','droneCharger','droneDepot',
    'precisionBearing','roboticFrame','servoCore','fluxCell','surveyLens',
    'diamondHead','superAlloy','phaseLens','quantumController','droneChassis',
    'flightController','plasmaThruster','fluxTank','starshipHull','flightCell',
    'railSlug','sunspikeCell','advancedComponent','reactorCell','shieldModule'
  ];
  const weaponPhases = ['idle', 'charge', 'fire', 'projectile', 'impact'];
  const frameKeys = (names, count) => names.flatMap(name => Array.from({length: count}, (_, i) => name + ':' + i));
  const aliases = {
    and: 'sensor', or: 'sensor', not: 'switch', xor: 'and', nand: 'not', nor: 'not', timer: 'switch',
    pulse: 'timer', toggle: 'switch', latch: 'timer', counter: 'sensor', signalSplit: 'splitter',
    piston: 'shaft', clutch: 'gear', belt: 'conveyor', pulley: 'winch', bevel: 'gear',
    smallGear: 'gear', largeGear: 'gear', bearing: 'gear', hinge: 'piston', splitter: 'hopper', filter: 'storage',
    frame: 'ladder', beam: 'ironBlock', excavator: 'drill', refiner: 'smelter', assembler: 'motor',
    ammoFabricator: 'assembler', advancedFabricator: 'refiner', repairStation: 'storage', recoveryStation: 'repairStation',
    arcTower: 'turret', teslaCoil: 'core', enemySensor: 'sensor', barrier: 'ironBlock', trap: 'gear',
    rope: 'shaft', mechanicalRope: 'shaft', ropeLadder: 'ladder', reinforcedLadder: 'ladder',
    scaffold: 'platform', metalScaffold: 'scaffold', bridge: 'platform', anchor: 'piston', rail: 'shaft',
    ledge: 'platform', counterweight: 'storage', crushedIron: 'iron', crushedCopper: 'copper',
    ironIngot: 'iron', copperIngot: 'copper', steel: 'ironBlock', circuit: 'sensor', advancedComponent: 'motor',
    ammo: 'copperBlock', repairKit: 'storage', medkit: 'repairKit', scrap: 'iron', reactorCell: 'core',
    pickaxe: 'piston', reinforcedPickaxe: 'pickaxe', steelDrillTool: 'drill', poweredMiningTool: 'drill',
    weapon: 'turret', tool: 'pickaxe', hammer: 'pickaxe', scrapLauncher: 'turret', stunTool: 'piston',
    energyRifle: 'drill', miningArmor: 'ironBlock', insulatedArmor: 'battery', reinforcedArmor: 'barrier',
    shieldModule: 'core', link: 'shaft'
  };
  const fullTiles = new Set(tileNames.slice(0, 8).concat('copperBlock','ironBlock','wire','signal',
    'cobaltOre','iridiumOre','aetheriumOre','cobaltPlate','iridiumShell','aetheriumPanel','temperedGlass','refractoryBrick','honeycombFrame','electricBus','superCable','fiberWire'));
  const uprightTiles = new Set(['door', 'autoDoor', 'ladder', 'ropeLadder', 'reinforcedLadder', 'workbench', 'generator', 'barrier']);
  const horizontalTiles = new Set(['platform', 'ledge', 'scaffold', 'metalScaffold', 'bridge', 'conveyor','armoredPlatform','gantryDeck','flightDeck','hangarGrate','armoredScaffold','starshipPad']);
  const narrowTiles = new Set(['rope', 'mechanicalRope', 'rail','magneticRail']);
  const spinTiles = new Set(['gear', 'motor', 'pulley', 'smallGear', 'largeGear', 'bevel', 'winch', 'drill', 'excavator','servoMotor','servoShaft']);
  const signalTiles = new Set(['signal', 'switch', 'sensor', 'enemySensor', 'and', 'or', 'not', 'xor', 'nand', 'nor', 'timer', 'pulse', 'toggle', 'latch', 'counter', 'signalSplit']);
  const colors = {
    grass: '#58492c', dirt: '#543b2b', stone: '#43474b', wood: '#573c2a',
    copper: '#694431', iron: '#454c52', crystal: '#304b50', obsidian: '#27283a',
    copperBlock: '#724934', ironBlock: '#414b50'
  };
  const zones = [[0, 42, 0], [42, 54, 1], [54, 65, 2], [65, 74, 1], [74, 84, 3]];
  const tileSprites = new Map(), partSprites = new Map();
  const actorSprites = new Map(), enemyMotion = new Map(), machineMotion = new Map();
  const robotSprites = new Map(), robotMotion = new Map(), weaponSprites = new Map();
  const itemLayers = [robotSprites, partSprites, tileSprites, actorSprites];
  const waitingIcons = new Set();
  const glowSprites = new Map();
  let sceneryBands = [];
  let industrialScenery = null;
  let pendingAtlases = 0;
  let artStarted = false;

  function canvas(w, h) {
    const node = document.createElement('canvas');
    node.width = w;
    node.height = h;
    return node;
  }

  function loadImage(name, types, onload, onerror) {
    const image = new Image();
    image.onload = () => onload(image);
    image.onerror = () => {
      if (types.length > 1) loadImage(name, types.slice(1), onload, onerror);
      else onerror();
    };
    image.src = new URL(name + '.' + types[0], assetRoot).href;
  }

  function isMatte(r, g, b) {
    return r > 167 && b > 153 && g < 142 && Math.min(r, b) - g > 66;
  }

  function cellBounds(pixels, width, left, top, right, bottom) {
    let x0 = right, x1 = left, y0 = bottom, y1 = top;
    for (let y = top; y < bottom; y++) {
      for (let x = left; x < right; x++) {
        if (pixels[(y * width + x) * 4 + 3] < 30) continue;
        if (x < x0) x0 = x;
        if (x > x1) x1 = x;
        if (y < y0) y0 = y;
        if (y > y1) y1 = y;
      }
    }
    return x0 <= x1 && y0 <= y1 ? [x0, y0, x1 - x0 + 1, y1 - y0 + 1] : null;
  }

  function readSheet(image, columns, names, target) {
    const rows = Math.ceil(names.length / columns);
    const width = image.naturalWidth, height = image.naturalHeight;
    const source = canvas(width, height);
    const raw = source.getContext('2d', {willReadFrequently: true});
    raw.drawImage(image, 0, 0);
    let data = null;
    try { data = raw.getImageData(0, 0, width, height); } catch { /* file: images can be origin restricted */ }
    if (data) {
      const pixels = data.data;
      let matteCorners = 0;
      // Crop both transparent and raw chroma-matte sprite sheets into the same packed format.
      for (let i = 0; i < names.length; i++) {
        const x = Math.floor((i % columns + .03) * width / columns);
        const y = Math.floor((Math.floor(i / columns) + .03) * height / rows);
        const p = (y * width + x) * 4;
        if (pixels[p + 3] && isMatte(pixels[p], pixels[p + 1], pixels[p + 2])) matteCorners++;
      }
      if (matteCorners > names.length / 2) {
        for (let p = 0; p < pixels.length; p += 4) {
          if (isMatte(pixels[p], pixels[p + 1], pixels[p + 2])) pixels[p + 3] = 0;
        }
        raw.putImageData(data, 0, 0);
      }
    }
    const cells=names.map((name,i)=>{
      const col=i%columns,row=Math.floor(i/columns);
      const left=Math.floor(col*width/columns),right=Math.floor((col+1)*width/columns);
      const top=Math.floor(row*height/rows),bottom=Math.floor((row+1)*height/rows);
      const rect=data?cellBounds(data.data,width,left,top,right,bottom):[left,top,right-left,bottom-top];
      return {left,top,rect};
    });
    const keepOrigin=target===enemyMotion||target===machineMotion||target===robotMotion;
    const groups=new Map();
    if(keepOrigin)for(const [i,{left,top,rect}] of cells.entries()){
      if(!rect)continue;
      const group=Math.floor(i/4),[x,y,w,h]=rect,localX=x-left,localY=y-top;
      const last=groups.get(group)||[Infinity,Infinity,0,0];
      groups.set(group,[Math.min(last[0],localX),Math.min(last[1],localY),Math.max(last[2],localX+w),Math.max(last[3],localY+h)]);
    }
    const atlas = canvas(columns * cellSize, rows * cellSize);
    const packed = atlas.getContext('2d');
    packed.imageSmoothingQuality = 'high';
    for (const [i, name] of names.entries()) {
      if (!name) continue;
      const col = i % columns, row = Math.floor(i / columns);
      const {left,top,rect}=cells[i];
      if (!rect) continue;
      const bounds=groups.get(Math.floor(i/4));
      const [sx,sy,sw,sh]=bounds?[left+bounds[0],top+bounds[1],bounds[2]-bounds[0],bounds[3]-bounds[1]]:rect;
      const scale = cellSize / Math.max(sw, sh);
      const w = Math.max(1, Math.round(sw * scale)), h = Math.max(1, Math.round(sh * scale));
      const x = col * cellSize, y = row * cellSize;
      packed.drawImage(source, sx, sy, sw, sh, x, y, w, h);
      target.set(name, {atlas, x, y, w, h});
    }
  }

  function imageFor(key) {
    while (key) {
      for (const layer of itemLayers) {
        const sprite = layer.get(key);
        if (sprite) return sprite;
      }
      const animated = robotMotion.get(key + ':0') || weaponSprites.get(key + ':0') || machineMotion.get(key + ':0');
      if (animated) return animated;
      key = Object.hasOwn(aliases, key) ? aliases[key] : null;
    }
    return null;
  }

  function draw(ctx, sprite, x, y, w, h) {
    ctx.drawImage(sprite.atlas, sprite.x, sprite.y, sprite.w, sprite.h, x, y, w, h);
  }

  function fit(ctx, sprite, x, y, w, h, bottom = false) {
    const scale = Math.min(w / sprite.w, h / sprite.h);
    const sw = sprite.w * scale, sh = sprite.h * scale;
    draw(ctx, sprite, x + (w - sw) / 2, y + (h - sh) / (bottom ? 1 : 2), sw, sh);
  }

  function item(ctx, key, x, y, size) {
    const sprite = imageFor(key);
    if (!sprite) return false;
    fit(ctx, sprite, x, y, size, size);
    return true;
  }

  function paintIcon(host, key) {
    const icon = canvas(48, 48);
    icon.style.cssText = 'width:100%;height:100%;display:block';
    item(icon.getContext('2d'), key, 0, 0, 48);
    host.replaceChildren(icon);
    host.dataset.mounted = 'true';
  }

  function refreshIcons() {
    for (const entry of waitingIcons) {
      if (imageFor(entry.key) && (entry.host.isConnected || !entry.host.dataset.mounted)) paintIcon(entry.host, entry.key);
    }
    if (!pendingAtlases) waitingIcons.clear();
  }

  function icon(key, fallback = '') {
    const foundryIcon = globalThis.CircuitboundFoundryArt?.icon?.(key, fallback);
    if (foundryIcon) return foundryIcon;
    const host = document.createElement('span');
    host.className = 'cb-art-icon';
    host.style.cssText = 'display:inline-flex;align-items:center;justify-content:center;width:1.35em;height:1.35em;flex:none;vertical-align:middle';
    host.setAttribute('aria-hidden', 'true');
    host.textContent = fallback;
    if (pendingAtlases) waitingIcons.add({host, key});
    if (imageFor(key)) paintIcon(host, key);
    return host;
  }

  function mirrorStrip(image, x, y, w, h) {
    const strip = canvas(w * 2, h);
    const ctx = strip.getContext('2d', {alpha: false});
    ctx.drawImage(image, x, y, w, h, 0, 0, w, h);
    ctx.translate(w * 2, 0);
    ctx.scale(-1, 1);
    ctx.drawImage(image, x, y, w, h, 0, 0, w, h);
    return strip;
  }

  function readScenery(image) {
    const w = Math.floor(image.naturalWidth / 2), h = Math.floor(image.naturalHeight / 2);
    sceneryBands = [0, 1, 2, 3].map(index => mirrorStrip(image, index % 2 * w, Math.floor(index / 2) * h, w, h));
  }

  function start() {
    if (artStarted) return;
    artStarted = true;
    const sheets = [
      ['art-tiles', 6, tileNames, tileSprites],
      ['art-actors', 5, actorNames, actorSprites],
      ['art-parts', 6, partNames, partSprites],
      ['art-enemy-motion', 6, frameKeys(enemyNames, 4), enemyMotion, ['webp', 'png']],
      ['art-machine-motion', 6, frameKeys(machineNames, 4), machineMotion, ['webp', 'png']],
      ['art-fixture-motion', 6, frameKeys(fixtureNames, 4), machineMotion],
      ['art-robot-motion', 6, frameKeys(robotNames, 4), robotMotion],
      ['art-rare-weapons', 5, frameKeys(rareWeapons, 5), weaponSprites],
      ['art-robot-parts', 6, robotPartNames, robotSprites],
      ['art-robot-components', 5, robotComponentNames, robotSprites]
    ];
    pendingAtlases = sheets.length + 2;
    const finished = () => { pendingAtlases--; refreshIcons(); };
    for (const [name, columns, names, sprites, types = ['png', 'webp']] of sheets) {
      loadImage(name, types, image => {
        try { readSheet(image, columns, names, sprites); } finally { finished(); }
      }, finished);
    }
    loadImage('art-scenery', ['png', 'webp'], image => {
      try { readScenery(image); } finally { finished(); }
    }, finished);
    loadImage('art-industrial-scenery', ['png', 'webp'], image => {
      try { industrialScenery = mirrorStrip(image, 0, 0, image.naturalWidth, image.naturalHeight); }
      finally { finished(); }
    }, finished);
  }

  function scenery(ctx, camera, width, height, tileSize, time) {
    if (!sceneryBands.length) return false;
    ctx.fillStyle = '#172329';
    ctx.fillRect(0, 0, width, height);
    for (const [start, end, index] of zones) {
      const y = Math.round(start * tileSize - camera.y), size = (end - start) * tileSize;
      if (y >= height || y + size <= 0) continue;
      const strip = start === 65 && industrialScenery ? industrialScenery : sceneryBands[index];
      const scaledWidth = Math.max(width, Math.ceil(size * strip.width / strip.height));
      const drift = Math.floor(camera.x * (index === 0 ? .18 : .27)) % scaledWidth;
      ctx.drawImage(strip, -drift, y, scaledWidth, size);
      if (scaledWidth - drift < width) ctx.drawImage(strip, scaledWidth - drift, y, scaledWidth, size);
      const warmth = index === 0 ? .1 + (1 - Math.sin(time * .012)) * .09 : .16;
      ctx.fillStyle = start === 65 && !industrialScenery ? '#402a1a59' : 'rgba(5,15,24,' + warmth + ')';
      ctx.fillRect(0, y, width, size);
    }
    return true;
  }

  function player(ctx, player, x, y, {time = 0, mining = false, climbing = false, attack = 0, miningProgress = null, weapon = null, attackAngle = 0, charging = false} = {}) {
    const foundry = globalThis.CircuitboundFoundryArt;
    const newWeapon = globalThis.CircuitFoundryCombat?.CATALOG[weapon] && foundry?.ready?.(weapon);
    const visibleWeapon = newWeapon || weaponSprites.has(weapon + ':0');
    const step = Math.sin(player.walk) < 0;
    let pose = 'idle';
    if (attack && !visibleWeapon) pose = attack < .095 ? 'attackStrike' : 'attackWindup';
    else if (mining) pose = (miningProgress == null ? (time * 4) % 1 : miningProgress) < .48 ? 'mineWindup' : 'mineStrike';
    else if (climbing) pose = Math.floor(time * (Math.abs(player.vy) > 3 ? 8 : 0)) % 2 ? 'climbB' : 'climbA';
    else if (!player.onGround && Math.abs(player.vy) > 25) pose = 'jump';
    else if (Math.abs(player.vx) > 8) pose = step ? 'walkA' : 'walkB';
    const sprite = actorSprites.get(pose) || actorSprites.get('idle');
    if (!sprite) return false;
    const actionPose = mining || attack && !visibleWeapon;
    const w = actionPose ? 48 : 30, h = player.h + 4;
    ctx.save();
    ctx.translate(x + player.w / 2, y - 4);
    ctx.scale(player.facing || 1, 1);
    if (player.hurt && Math.floor(time * 16) % 2) ctx.globalAlpha = .58;
    fit(ctx, sprite, -w / 2 + (actionPose ? 6 : 0), 0, w, h, true);
    if (visibleWeapon && !mining && !climbing) {
      const facing=player.facing||1;
      const localAngle=attack||charging?Math.atan2(Math.sin(attackAngle),Math.cos(attackAngle)*facing):0;
      const width=weapon==='sunspikeLaser'?40:weapon==='phaseBlade'?27:33;
      ctx.translate(3,h*.51);
      ctx.rotate(localAngle);
      if (newWeapon) {
        const recoil = attack ? Math.min(4, attack * 20) : 0;
        foundry.draw(ctx, weapon, -5 - recoil, -13, 32, 26, {time, working: !!attack || charging, phase: charging ? 'charge' : attack ? 'fire' : 'idle'});
      } else frame(ctx,weapon,charging?'charge':attack?'fire':'idle',-4,-7,width,14);
    }
    ctx.restore();
    return true;
  }

  function enemy(ctx, actor) {
    let index = 0;
    if (actor.wind > 0) index = 2 + Number(actor.wind < .105);
    else if (Math.abs(actor.vx) > 5 || Math.abs(actor.vy) > 5) index = Math.floor(actor.age * 8) % 2;
    const sprite = enemyMotion.get(actor.type + ':' + index) || actorSprites.get(actor.type);
    if (!sprite) return false;
    const width = actor.w + 8, height = actor.h + 6;
    const bob = Math.sin(actor.age * 10) * (Math.abs(actor.vx) > 5 ? .9 : .25);
    ctx.save();
    ctx.translate(0, bob);
    ctx.scale(actor.facing || 1, 1);
    if (actor.flash > 0) ctx.filter = 'brightness(1.45)';
    fit(ctx, sprite, -width / 2, -height / 2 - 2, width, height, true);
    ctx.restore();
    return true;
  }

  function cache(ctx, kind) {
    const sprite = actorSprites.get('cache');
    if (!sprite) return false;
    ctx.save();
    if (kind === 'recovery') ctx.filter = 'hue-rotate(110deg)';
    fit(ctx, sprite, -11, -9, 22, 18);
    ctx.restore();
    return true;
  }

  function glowFor(color) {
    let sprite = glowSprites.get(color);
    if (sprite) return sprite;
    sprite = canvas(48, 48);
    const ctx = sprite.getContext('2d');
    const gradient = ctx.createRadialGradient(24, 24, 0, 24, 24, 30);
    gradient.addColorStop(0, 'rgb(' + color + ')');
    gradient.addColorStop(1, 'rgba(' + color + ',0)');
    ctx.fillStyle = gradient;
    ctx.fillRect(0, 0, 48, 48);
    glowSprites.set(color, sprite);
    return sprite;
  }

  function paintTile(ctx, key, state, time, size) {
    const door = key === 'door' || key === 'autoDoor';
    const lit = state?.active || state?.on || state?.powered;
    const running = state?.running || state?.working || state?.firing || state?.extended
      || (['generator','sensor','lamp','reactorGenerator','reclaimerDock'].includes(key) && lit);
    const clock = Math.floor(time * 8 * Math.max(.5, Math.min(2, Math.abs(state?.speed || 1))));
    const steps=Number.isFinite(state?.animationTime)?Math.min(3,Math.max(0,Math.floor((time-state.animationTime)*22))):3;
    const cel=key==='piston'?(state?.extended?steps:3-steps):running?clock%4:0;
    const animated = robotIds.has(key) ? robotMotion.get(key + ':' + cel)
      : machineIds.has(key) ? machineMotion.get(key + ':' + cel) : null;
    const sprite = (door && state?.open ? imageFor('doorOpen') : null) || animated || imageFor(key);
    if (!sprite) return false;
    const angle = (state?.rot || 0) * Math.PI / 2;
    const rotated = angle && !uprightTiles.has(key) && !fullTiles.has(key) && !narrowTiles.has(key);
    if (rotated) {
      ctx.save();
      ctx.translate(size / 2, size / 2);
      ctx.rotate(angle);
      ctx.translate(-size / 2, -size / 2);
    }
    if (colors[key]) {
      ctx.fillStyle = colors[key];
      ctx.fillRect(0, 0, size, size);
    }
    if (door && state?.open && !imageFor('doorOpen')) {
      draw(ctx, sprite, size * .74, 0, size * .26, size);
    } else if (fullTiles.has(key)) {
      draw(ctx, sprite, 0, 0, size, size);
    } else if (horizontalTiles.has(key)) {
      draw(ctx, sprite, 0, size * .23, size, size * .55);
    } else if (narrowTiles.has(key)) {
      draw(ctx, sprite, size * .36, 0, size * .28, size);
    } else if (key === 'shaft') {
      draw(ctx, sprite, 0, size * .28, size, size * .44);
    } else if (door || key.endsWith('adder')) {
      draw(ctx, sprite, size * .08, 0, size * .84, size);
    } else {
      fit(ctx, sprite, 0, 0, size, size, true);
    }
    if (running && !animated && spinTiles.has(key)) {
      const wheel = imageFor('gear');
      if (wheel) {
        ctx.save();
        ctx.translate(size / 2, size / 2);
        ctx.rotate(time * 7 * (state.speed || 1));
        ctx.globalAlpha *= .86;
        fit(ctx, wheel, -size * .18, -size * .18, size * .36, size * .36);
        ctx.restore();
      }
    }
    if (running || (lit && (signalTiles.has(key) || key === 'wire' || key === 'lamp' || key === 'core'))) {
      const color = signalTiles.has(key) ? '255,109,114' : ['smelter', 'wire', 'lamp'].includes(key) ? '255,196,87' : '129,255,224';
      const alpha = ctx.globalAlpha;
      ctx.globalAlpha *= .22 + Math.sin(time * 12) * .07;
      ctx.drawImage(glowFor(color), 0, 0, size, size);
      ctx.globalAlpha = alpha;
    }
    if (rotated) ctx.restore();
    return true;
  }

  function frame(ctx, key, index, x, y, w, h) {
    const weaponIndex = typeof index === 'string' ? weaponPhases.indexOf(index) : index;
    const sprite = robotMotion.get(key + ':' + index) || weaponSprites.get(key + ':' + weaponIndex)
      || machineMotion.get(key + ':' + index) || enemyMotion.get(key + ':' + index) || imageFor(key);
    if (!sprite) return false;
    fit(ctx, sprite, x, y, w, h);
    return true;
  }

  function vehicle(ctx,key,x,y,w,h) {
    const frames={starship:0,starshipIdle:0,starshipThrust1:1,starshipThrust2:2,starshipBrake:3};
    return frame(ctx,'starship',frames[key]??0,x,y,w,h);
  }

  function drone(ctx,key,x,y,w,h,phase=0) {
    return frame(ctx,key,Math.floor(phase)%4,x,y,w,h);
  }

  function create(tiles, defs, items) {
    start();
    const keys = new Map(items.filter(part => part.tile >= 0).map(part => [part.tile, part.key]));
    for (const [id, def] of Object.entries(defs)) {
      if (!keys.has(Number(id)) && def.drop) keys.set(Number(id), def.drop);
    }
    for (const [id, key] of [[tiles.GRASS, 'grass'], [tiles.CORE, 'core'], [tiles.WORKBENCH, 'workbench']]) {
      if (id != null) keys.set(id, key);
    }
    return {
      tile: (ctx, id, state, time = 0, size = 24) => paintTile(ctx, keys.get(id) || defs[id]?.drop, state, time, size),
      item, icon, player, scenery, frame, vehicle, drone
    };
  }

  globalThis.CircuitArt = {create, item, icon, player, enemy, cache, scenery, frame};
})();
