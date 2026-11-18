(function () {
  'use strict';

  const root = new URL('./art/expansion/', document.currentScript.src);
  const objects = [
    'deepcoreBorer', 'seismicStripMiner', 'resonanceDrill', 'mantleExtractor', 'osmiumOre', 'graviteOre',
    'surveyDrone', 'rigTender', 'freightDrone', 'repairDrone', 'relayDrone', 'shieldDrone',
    'interceptorDrone', 'constructionDrone', 'railHauler', 'tunnelCrawler', 'stabilizerWalker', 'bastionWalker',
    'breachHammer', 'shardScattergun', 'anchorHarpoon', 'repeaterCarbine', 'gravityMaul', 'orbitDisc',
    'compressionProjector', 'kineticBow', 'prismRifle', 'phaseSabers', 'interferenceWand', 'echoMortar',
    'starcoreLance', 'coronaCaster', 'singularityLauncher', 'mantleBreaker', 'nullQuartzOre', 'starcoreOre'
  ];
  const regionKeys = ['faultGalleries', 'osmiumBasin', 'gravityHollows', 'nullCrystalVaults', 'starcoreMantle', 'freightYard'];
  const indexed = new Map(objects.map((key, i) => [key, i]));
  for (const [key, source] of Object.entries({
    osmiumIngot: 'osmiumOre', graviteIngot: 'graviteOre', nullQuartz: 'nullQuartzOre',
    nullQuartzFragment: 'nullQuartzOre', starcoreIngot: 'starcoreOre'
  })) indexed.set(key, indexed.get(source));
  const rock = new Map(['faultRock','basinRock','gravityRock','vaultRock','mantleRock'].map((key,i)=>[key,i]));

  function load(file) {
    const image = new Image();
    image.src = new URL(file, root).href;
    return image;
  }

  const atlas = load('foundry-objects.webp?view=front');
  const landscapes = load('foundry-regions.webp');
  const cell = 96;
  // Opaque bounds inside each atlas cell, in catalog order. Crop the padding before anchoring a sprite.
  const frames = [
    [7,2,82,92], [2,11,92,74], [4,2,88,92], [2,4,92,88], [2,5,92,86], [2,10,92,76],
    [2,5,92,86], [2,7,92,82], [2,13,92,69], [2,11,92,73], [13,2,69,92], [2,18,92,60],
    [2,15,92,65], [2,6,92,83], [2,13,92,69], [2,15,92,66], [2,9,92,77], [2,3,92,90],
    [2,16,92,64], [2,24,92,48], [2,26,92,44], [2,21,92,53], [2,18,92,60], [2,5,92,85],
    [2,20,92,55], [11,2,73,92], [2,21,92,53], [2,12,92,72], [3,2,90,92], [2,11,92,74],
    [2,31,92,33], [2,15,92,65], [2,20,92,55], [2,2,92,91], [4,2,87,92], [2,6,92,83]
  ];
  const loaded = image => !!image.naturalWidth;

  function ready(key) {
    return rock.has(key) ? loaded(landscapes) : indexed.has(key) && loaded(atlas);
  }

  function draw(ctx, key, x, y, w, h, { time = 0, working = false, facing = 1, phase, grounded = false } = {}) {
    if (!ready(key)) return false;
    if (rock.has(key)) {
      const region = rock.get(key);
      const sw = landscapes.naturalWidth / 2;
      const sh = landscapes.naturalHeight / 3;
      ctx.drawImage(landscapes, sw * (region % 2 + .56), sh * (Math.floor(region / 2) + .52), 42, 42, x, y, w, h);
      return true;
    }
    const index = indexed.get(key);
    const flying = index >= 6 && index <= 13;
    const driving = index >= 14 && index <= 17;
    const ore = index === 4 || index === 5 || index >= 34;
    const orePulse = ore && working ? .8 + .2 * Math.sin(time * 2.5 + index) : 1;
    const movement = flying && !grounded ? Math.sin(time * (working ? 13 : 5)) * h * .04 : 0;
    const [left, top, sourceWidth, sourceHeight] = frames[index];
    const scale = Math.min(w / (sourceWidth + 4), h / (sourceHeight + 4));
    const frameWidth = sourceWidth * scale;
    const frameHeight = sourceHeight * scale;
    const suspension = grounded && driving && working ? 1 + Math.sin(time * 20) * .028 : 1;
    ctx.save();
    ctx.translate(x + w / 2, grounded ? y + h : y + h / 2 + movement);
    ctx.scale(facing < 0 ? -1 : 1, suspension);
    if (flying && working && !grounded) ctx.rotate(Math.sin(time * 12) * .075);
    if (phase === 'charge') ctx.filter = `brightness(${1.18 + Math.sin(time * 21) * .18})`;
    else if (phase === 'fire') ctx.filter = 'brightness(1.35)';
    else if (orePulse !== 1) ctx.globalAlpha *= orePulse;
    ctx.drawImage(atlas, index % 6 * cell + left, Math.floor(index / 6) * cell + top, sourceWidth, sourceHeight,
      -frameWidth / 2, grounded ? -frameHeight : -frameHeight / 2, frameWidth, frameHeight);
    ctx.restore();
    return true;
  }

  function drawRegion(ctx, key, x, y, w, h, alpha = 1) {
    const index = regionKeys.indexOf(key);
    if (index < 0 || !loaded(landscapes)) return false;
    const sw = landscapes.naturalWidth / 2;
    const sh = landscapes.naturalHeight / 3;
    const scale = Math.max(w / sw, h / sh);
    const visibleWidth = w / scale;
    const visibleHeight = h / scale;
    const sx = index % 2 * sw + (sw - visibleWidth) / 2;
    const sy = Math.floor(index / 2) * sh + (sh - visibleHeight) / 2;
    ctx.save();
    ctx.globalAlpha *= alpha;
    ctx.imageSmoothingEnabled = true;
    ctx.drawImage(landscapes, sx, sy, visibleWidth, visibleHeight, x, y, w, h);
    ctx.restore();
    return true;
  }

  function icon(key, fallback) {
    if (!indexed.has(key)) return null;
    const graphic = document.createElement('span');
    graphic.className = 'cb-art-icon';
    graphic.setAttribute('aria-hidden', 'true');
    graphic.style.cssText = 'display:inline-grid;place-items:center;flex:none;width:1.4em;height:1.4em';
    graphic.textContent = fallback;
    const paint = () => {
      const stamp = document.createElement('canvas');
      stamp.width = stamp.height = 44;
      stamp.style.cssText = 'display:block;width:100%;height:100%';
      if (draw(stamp.getContext('2d'), key, 0, 0, 44, 44)) graphic.replaceChildren(stamp);
    };
    if (loaded(atlas)) paint();
    else atlas.addEventListener?.('load', paint, { once: true });
    return graphic;
  }

  globalThis.CircuitboundFoundryArt = { draw, drawRegion, icon, ready };
})();
