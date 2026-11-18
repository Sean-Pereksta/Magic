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

  const atlas = load('foundry-objects.webp');
  const landscapes = load('foundry-regions.webp');
  const cell = 96;
  const loaded = image => !!image.naturalWidth;

  function ready(key) {
    return rock.has(key) ? loaded(landscapes) : indexed.has(key) && loaded(atlas);
  }

  function draw(ctx, key, x, y, w, h, { time = 0, working = false, facing = 1, phase } = {}) {
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
    const weapon = index >= 18 && index <= 33;
    const ore = index === 4 || index === 5 || index >= 34;
    const orePulse = ore && working ? .8 + .2 * Math.sin(time * 2.5 + index) : 1;
    const movement = flying ? Math.sin(time * (working ? 13 : 5)) * h * .04 : driving && working ? Math.sin(time * 20) * h * .023 : 0;
    ctx.save();
    ctx.translate(x + w / 2, y + h / 2 + movement);
    ctx.scale(facing < 0 ? -1 : 1, 1);
    if (flying && working) ctx.rotate(Math.sin(time * 12) * .075);
    if (phase === 'charge') ctx.filter = `brightness(${1.18 + Math.sin(time * 21) * .18})`;
    else if (phase === 'fire') ctx.filter = 'brightness(1.35)';
    else if (orePulse !== 1) ctx.globalAlpha *= orePulse;
    const frameWidth = weapon ? Math.min(w, h * 1.5) : w;
    ctx.drawImage(atlas, index % 6 * cell, Math.floor(index / 6) * cell, cell, cell,
      -frameWidth / 2, -h / 2, frameWidth, h);
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
