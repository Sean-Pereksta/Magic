import { artwork, assetUrl, loadManifest } from './assets.mjs';
import {
  CHECKPOINT_KEY,
  readProgress,
  mergeProgress,
  recordResult,
  missionUnlocked,
  arenaUnlocked
} from './progress.mjs';

const params = new URLSearchParams(location.search);
const root = document.createElement('section');
root.id = 'sfMenu';
root.hidden = true;
root.setAttribute('aria-label', 'TurnCraft command center');
document.body.append(root);
let api,
  catalog,
  faction = localStorage.getItem('csf.faction') || 'frontier',
  difficulty = 'medium';
let identity = {
    displayName: localStorage.getItem('gh.username') || 'Local Commander'
  },
  profile = {},
  cloud;
let progressKey = 'turncraft.shattered-front.progress.local.v1',
  progress = readProgress(progressKey);
let activeView = 'home',
  returnView = 'home',
  selectedRegion = '',
  pendingLaunch = null,
  lastResult = null,
  busy = false;
let message = '',
  factionReturn = 'home',
  profileUnsub,
  initialized = false,
  syncing = false;
const fallbackArt = (id) =>
  new URL(`../assets/factions/${id}.webp`, import.meta.url).href;
const el = (tag, className, text) => {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text != null) node.textContent = text;
  return node;
};
const button = (label, action, className = 'sf-button') => {
  const node = el('button', className, label);
  node.type = 'button';
  node.onclick = action;
  return node;
};
const factionInfo = () => api.getFactions?.() || {};
function factions() {
  const source = factionInfo();
  return Array.isArray(source) ? source : Object.values(source);
}
const factionName = (id) =>
  factions().find((item) => item.id === id)?.name || id;
function rank() {
  return cloud?.rankFor
    ? cloud.rankFor(profile.mmr, profile.rankedMatches)
    : { name: 'Unranked', id: 'unranked' };
}
function rankName() {
  const value = rank();
  return typeof value === 'string'
    ? value
    : value.name || value.rank || 'Unranked';
}
function emblem() {
  const name = rankName().toLowerCase();
  return artwork(`rank.${name}`, `ranks/${name}.png`, {
    className: 'sf-rank',
    alt: `${rankName()} emblem`
  });
}
function saveProgress() {
  try {
    localStorage.setItem(progressKey, JSON.stringify(progress));
  } catch {
    message =
      'Browser storage is full. Account saves will retry when connected.';
  }
  api.setProfile?.({ ...profile, ...progress });
}
async function flushSaves() {
  if (syncing || !cloud || !identity.linked || !progress.pendingResults?.length)
    return;
  const startedFor = progressKey;
  syncing = true;
  try {
    while (progress.pendingResults?.length) {
      const result = progress.pendingResults[0],
        owner = progressKey;
      const account = { username: identity.username };
      if (result.mode === 'campaign') await cloud.saveCampaign(result, account);
      else await cloud.saveSurvival(result, account);
      if (owner !== progressKey) break;
      progress.pendingResults.shift();
      saveProgress();
    }
  } catch {
    status(
      'Saved on this device. Account sync will retry when your connection returns.'
    );
  } finally {
    syncing = false;
    if (startedFor !== progressKey) void flushSaves();
  }
}
function checkpoint() {
  try {
    return JSON.parse(localStorage.getItem(CHECKPOINT_KEY) || 'null');
  } catch {
    return null;
  }
}
function show(view = 'home', { focus = true } = {}) {
  if (view === 'home' && api?.getIdentity?.().lobbyId) view = 'session';
  activeView = view;
  root.hidden = false;
  document.body.classList.add('sf-menu-open');
  const legacy = document.getElementById('overlay');
  if (legacy) legacy.inert = true;
  render();
  if (focus) root.querySelector('h1,h2')?.focus({ preventScroll: true });
}
function hide() {
  root.hidden = true;
  document.body.classList.remove('sf-menu-open');
  const legacy = document.getElementById('overlay');
  if (legacy) legacy.inert = false;
}
function heading(title, eyebrow, body) {
  const wrap = el('header', 'sf-heading');
  wrap.append(el('span', 'sf-eyebrow', eyebrow));
  const h = el('h2', '', title);
  h.tabIndex = -1;
  wrap.append(h);
  if (body) wrap.append(el('p', '', body));
  return wrap;
}
function status(text) {
  message = text;
  const box = root.querySelector('#sfStatus');
  if (box) box.textContent = text;
}
function shell() {
  root.replaceChildren();
  root.dataset.faction = faction;
  for (const state of ['default', 'hover', 'selected', 'pressed'])
    root.style.setProperty(
      `--sf-button-${state}`,
      `url("${assetUrl(`ui.button.${state}`, `ui/main_menu/buttons/${state}.png`)}")`
    );
  root.style.setProperty(
    '--sf-smoke',
    `url("${assetUrl('effect.smoke', 'effects/smoke.png')}")`
  );
  const scene = artwork('menu.background', 'ui/main_menu/battlefield.webp', {
    className: 'sf-backdrop',
    fallback: fallbackArt(faction)
  });
  scene.loading = 'eager';
  root.append(scene, el('div', 'sf-atmosphere'));
  const effects = el('div', 'sf-menu-effects');
  effects.setAttribute('aria-hidden', 'true');
  for (let i = 0; i < 3; i++) effects.append(el('div', 'sf-smoke-cloud'));
  root.append(effects);
  const top = el('header', 'sf-topbar');
  top.append(button('TC / SHATTERED FRONT', () => show(), 'sf-wordmark'));
  const account = button('', () => show('profile'), 'sf-account');
  account.append(emblem());
  if (identity.avatar && /^https:\/\//.test(identity.avatar)) {
    const avatar = el('img', 'sf-avatar');
    avatar.src = identity.avatar;
    avatar.alt = '';
    avatar.onerror = () => {
      avatar.hidden = true;
    };
    account.append(avatar);
  }
  const names = el('span');
  names.append(
    el('b', '', identity.displayName || identity.username || 'Local Commander'),
    el(
      'small',
      '',
      `${rankName()}${profile.rankedMatches ? ` · ${Math.round(profile.mmr)} MMR` : ' · Personal progress'}`
    ),
    el(
      'small',
      'sf-account-progress',
      `${Object.keys(progress.campaign.completedMissions).length} / 24 campaign missions`
    )
  );
  account.append(names);
  top.append(
    account,
    button('Lobbies ↗', () => lobby('browse'), 'sf-link')
  );
  root.append(top);
  const main = el('main', `sf-main sf-view-${activeView}`);
  root.append(main);
  const footer = el('footer', 'sf-footer');
  footer.append(
    el('span', '', 'CATNMICE GAMES  /  TURNCRAFT'),
    el(
      'span',
      '',
      `${Object.keys(progress.campaign.completedMissions).length} / 24 MISSIONS COMPLETE`
    ),
    button('Settings', () => show('settings'), 'sf-link')
  );
  root.append(footer);
  const note = el('p', 'sf-status', message);
  note.id = 'sfStatus';
  note.setAttribute('role', 'status');
  root.append(note);
  return main;
}
function render() {
  if (!api || !catalog) return;
  const main = shell();
  if (!['home', 'session', 'loading'].includes(activeView))
    main.append(button('← Command center', () => show('home'), 'sf-back'));
  ({
    home,
    campaign,
    survival,
    factions: factionScreen,
    briefing,
    multiplayer,
    profile: profileScreen,
    settings,
    result: resultScreen,
    skirmish,
    loading,
    session
  })[activeView]?.(main);
}
function home(main) {
  const intro = el('div', 'sf-home-intro');
  intro.append(
    el('span', 'sf-eyebrow', 'A WORLD DIVIDED. A COALITION RISING.')
  );
  const title = el('h1', 'sf-title', 'TURNCRAFT');
  title.tabIndex = -1;
  const logo = artwork('menu.logo', 'ui/main_menu/turncraft-logo.png', {
    className: 'sf-logo'
  });
  logo.loading = 'eager';
  logo.onload = () => intro.classList.add('sf-has-logo');
  intro.append(
    logo,
    title,
    el('p', 'sf-subtitle', 'SHATTERED FRONT'),
    el(
      'p',
      'sf-home-copy',
      'The Fracture is opening. Build your defenses. Command the counterattack.'
    )
  );
  const nav = el('nav', 'sf-navigation');
  for (const [label, sub, view, icon] of [
    ['Campaign', 'The Shattered Front · 24 missions', 'campaign', '01'],
    ['Multiplayer', 'Rivals, allies, and ranked command', 'multiplayer', '02'],
    ['Single Player', 'Your battlefield. Your rules.', 'skirmish', '03'],
    ['Endless Survival', 'Hold the line. Set a new record.', 'survival', '04'],
    ['Armory / Factions', 'Know your army', 'factions', '05'],
    ['Player Profile', 'Your story so far', 'profile', '06']
  ]) {
    const b = button(
      '',
      () => {
        if (view === 'factions') {
          factionReturn = 'home';
          pendingLaunch = null;
        }
        show(view);
      },
      'sf-nav-item'
    );
    b.append(el('span', 'sf-nav-number', icon));
    const text = el('span');
    text.append(el('b', '', label), el('small', '', sub));
    b.append(text, el('span', 'sf-arrow', '↗'));
    nav.append(b);
  }
  const left = el('div', 'sf-home-left');
  left.append(intro, nav);
  main.append(left);
  const feature = el('aside', 'sf-feature');
  feature.append(
    el('span', 'sf-eyebrow', 'TRANSMISSION / COMMANDER VENN'),
    el('h2', '', 'One front worth fighting for.'),
    el(
      'p',
      '',
      '“The enemy has endless reinforcements. We have Kade’s inventions. I’m choosing to call that an advantage.”'
    )
  );
  const next =
    catalog.missions.find((m) => !progress.campaign.completedMissions[m.id]) ||
    catalog.missions.at(-1);
  feature.append(
    button(
      Object.keys(progress.campaign.completedMissions).length
        ? 'Continue campaign →'
        : 'Begin the campaign →',
      () => chooseMission(next),
      'sf-button sf-primary'
    )
  );
  if (checkpoint())
    feature.append(button('Resume saved battle', resume, 'sf-button'));
  feature.append(
    el(
      'small',
      'sf-feature-note',
      'Six regions. Three factions. A threat that learns.'
    )
  );
  main.append(feature);
}
function regionArt(region, className = 'sf-card-art') {
  const id = region.assetId || region.id.replaceAll('-', '_');
  return artwork(`region.${id}.thumbnail`, `regions/${id}/thumbnail.webp`, {
    className,
    fallback: fallbackArt(faction)
  });
}
function campaign(main) {
  main.append(
    heading(
      'The Shattered Front',
      'STORY CAMPAIGN',
      'Push through six regions and uncover the intelligence behind the invasion. Every victory opens the next mission.'
    )
  );
  const world = el('div', 'sf-world-map');
  world.append(
    artwork('campaign.worldMap', 'campaign/world_map/shattered-front.webp', {
      className: 'sf-world-image'
    })
  );
  const coordinates = [
    [13, 63],
    [30, 39],
    [47, 70],
    [61, 39],
    [78, 23],
    [88, 61]
  ];
  catalog.regions.forEach((region, i) => {
    const marker = button(
      `${String(i + 1).padStart(2, '0')} / ${region.name}`,
      () => {
        selectedRegion = region.id;
        render();
      },
      `sf-map-marker ${selectedRegion === region.id ? 'selected' : ''}`
    );
    marker.style.left = `${coordinates[i][0]}%`;
    marker.style.top = `${coordinates[i][1]}%`;
    world.append(marker);
  });
  main.append(world);
  const strip = el('div', 'sf-region-strip');
  for (const region of catalog.regions) {
    const first = catalog.missions.find((m) => m.regionId === region.id),
      unlocked = missionUnlocked(first, catalog, progress);
    const b = button(
      '',
      () => {
        selectedRegion = region.id;
        render();
      },
      `sf-region ${selectedRegion === region.id ? 'selected' : ''}`
    );
    b.append(
      regionArt(region),
      el('b', '', region.name),
      el(
        'small',
        '',
        unlocked
          ? `${catalog.missions.filter((m) => m.regionId === region.id && progress.campaign.completedMissions[m.id]).length} / 4 secured`
          : 'Complete the previous region'
      )
    );
    strip.append(b);
  }
  main.append(strip);
  const region =
    catalog.regions.find((r) => r.id === selectedRegion) || catalog.regions[0];
  selectedRegion = region.id;
  main.append(heading(region.name, 'REGIONAL OPERATIONS', region.description));
  const grid = el('div', 'sf-mission-grid');
  for (const mission of catalog.missions.filter(
    (m) => m.regionId === region.id
  )) {
    const unlocked = missionUnlocked(mission, catalog, progress),
      saved = progress.campaign.completedMissions[mission.id];
    const b = button('', () => chooseMission(mission), 'sf-mission');
    b.disabled = !unlocked;
    b.append(
      el(
        'small',
        'sf-eyebrow',
        `OPERATION ${String(mission.index + 1).padStart(2, '0')}`
      ),
      el('h3', '', mission.title || mission.name),
      el(
        'p',
        '',
        mission.objectives?.find((o) => !o.optional)?.label ||
          'Hold against the incursion.'
      ),
      el(
        'span',
        'sf-stars',
        saved
          ? '★'.repeat(saved.stars || 1) + '☆'.repeat(3 - (saved.stars || 1))
          : unlocked
            ? `${mission.waves} waves · Deploy →`
            : '🔒 Locked'
      )
    );
    grid.append(b);
  }
  main.append(grid);
}
function chooseMission(mission) {
  if (!mission || !missionUnlocked(mission, catalog, progress)) return;
  pendingLaunch = { mode: 'campaign', item: mission };
  show('briefing');
}
function difficultyPicker(container) {
  const field = el('label', 'sf-field', 'AI difficulty');
  const select = el('select');
  for (const id of ['easy', 'medium', 'hard', 'pro']) {
    const option = el('option', '', id[0].toUpperCase() + id.slice(1));
    option.value = id;
    option.selected = id === difficulty;
    select.append(option);
  }
  select.onchange = () => {
    difficulty = select.value;
  };
  field.append(select);
  container.append(field);
}
function briefing(main) {
  const mission = pendingLaunch?.item;
  if (!mission) return show('campaign');
  const region = catalog.regions.find((r) => r.id === mission.regionId);
  const panel = el('div', 'sf-briefing');
  const sceneId =
    mission.index === 0
      ? 'firstStand'
      : mission.index === 11
        ? 'discovery'
        : mission.index === 20
          ? 'finalAssault'
          : '';
  if (sceneId)
    panel.append(
      artwork(
        `campaign.scene.${sceneId}`,
        `campaign/cutscenes/${sceneId}.webp`,
        { className: 'sf-briefing-scene', fallback: fallbackArt(faction) }
      )
    );
  else panel.append(regionArt(region, 'sf-briefing-scene'));
  const content = el('div', 'sf-briefing-content');
  content.append(
    heading(
      mission.title || mission.name,
      region.name,
      'Prepare, defend, and strike back.'
    )
  );
  const speaker = mission.briefing?.speaker || 'elara_venn';
  const dialogue = el('div', 'sf-dialogue');
  dialogue.append(
    artwork(`portrait.${speaker}`, `characters/${speaker}/portrait.webp`, {
      className: 'sf-portrait'
    })
  );
  const words = el('div');
  words.append(
    el(
      'b',
      '',
      catalog.characters.find((c) => c.id === speaker)?.name ||
        speaker.replaceAll('_', ' ')
    ),
    el(
      'p',
      '',
      mission.briefing?.text || 'Coalition command is counting on you.'
    )
  );
  dialogue.append(words);
  content.append(dialogue);
  const objectives = el('ul', 'sf-objectives');
  for (const o of mission.objectives || [])
    objectives.append(
      el('li', '', `${o.optional ? 'Optional: ' : ''}${o.label}`)
    );
  content.append(objectives);
  difficultyPicker(content);
  content.append(
    button(
      'Choose your faction →',
      () => {
        factionReturn = 'briefing';
        show('factions');
      },
      'sf-button sf-primary'
    )
  );
  panel.append(content);
  main.append(panel);
}
function survival(main) {
  main.append(
    heading(
      'Endless Survival',
      'THERE IS NO FINAL WAVE',
      'New threats every five waves. Elite encounters every ten. Campaign victories unlock new defensive arenas.'
    )
  );
  difficultyPicker(main);
  const grid = el('div', 'sf-arena-grid');
  for (const arena of catalog.arenas) {
    const region = catalog.regions.find(
        (r) => r.id === (arena.regionId || arena.id)
      ),
      unlocked = arenaUnlocked(arena, catalog, progress);
    const card = el('article', 'sf-arena');
    card.append(
      regionArt(region),
      el('h3', '', arena.name),
      el('p', '', region.description)
    );
    const record = progress.survival.recordsByMap[`${arena.id}:${difficulty}`];
    card.append(
      el(
        'small',
        '',
        record
          ? `Personal best: wave ${record.wave} · ${record.factionId}`
          : 'No personal record yet'
      )
    );
    const b = button(
      unlocked ? 'Defend this arena →' : '🔒 Complete the previous region',
      () => {
        pendingLaunch = { mode: 'survival', item: arena };
        factionReturn = 'survival';
        show('factions');
      },
      'sf-button'
    );
    b.disabled = !unlocked;
    card.append(b);
    grid.append(card);
  }
  main.append(grid);
}
function factionScreen(main) {
  main.append(
    heading(
      'Choose your command',
      'THE COALITION ARMORY',
      'Three armies. Three philosophies. The same battlefield.'
    )
  );
  const layout = el('div', 'sf-faction-layout'),
    tabs = el('div', 'sf-faction-tabs');
  for (const f of factions()) {
    const b = button(
      '',
      () => {
        faction = f.id;
        api.selectFaction?.(faction);
        localStorage.setItem('csf.faction', faction);
        render();
      },
      `sf-faction-tab ${f.id === faction ? 'selected' : ''}`
    );
    const insignia = el('span', 'sf-faction-insignia', f.icon);
    const image = artwork(
      `faction.${f.id}.emblem`,
      `ui/faction_select/${f.id}/emblem.png`,
      { className: 'sf-faction-emblem' }
    );
    image.onload = () => insignia.classList.add('loaded');
    insignia.append(image);
    b.append(insignia, el('b', '', f.name));
    b.style.setProperty('--faction-color', f.color);
    tabs.append(b);
  }
  const f = factions().find((item) => item.id === faction) || factions()[0];
  const art = el('div', 'sf-faction-art');
  const image = el('img');
  image.src = fallbackArt(f.id);
  image.alt = f.name;
  image.decoding = 'async';
  art.append(image, el('span', 'sf-faction-banner', f.short));
  const detail = el('div', 'sf-faction-detail');
  detail.append(
    el('span', 'sf-eyebrow', f.playstyle),
    el('h3', '', f.name),
    el('p', '', f.summary)
  );
  const strengths = el('ul');
  for (const strength of f.strengths.split(' · '))
    strengths.append(el('li', '', strength));
  detail.append(
    strengths,
    el('small', '', `Watch your flank: ${f.weaknesses}`)
  );
  const units = api.getUnits?.() || {};
  const roster = Array.isArray(units)
    ? units
    : Object.entries(units).map(([id, unit]) => ({ id, ...unit }));
  const preview = el('div', 'sf-unit-list');
  for (const u of roster.filter((u) => u.f === f.id)) {
    const chip = el('details'),
      summary = el('summary');
    if (api.loadUnitArt) {
      const canvas = el('canvas', 'sf-unit-thumb');
      canvas.width = canvas.height = 64;
      canvas.setAttribute('aria-hidden', 'true');
      summary.append(canvas);
      void api
        .loadUnitArt(u.id)
        .then((image) => {
          if (image && canvas.isConnected) {
            const width = image.naturalWidth || image.width,
              height = image.naturalHeight || image.height,
              scale = Math.min(64 / width, 64 / height);
            canvas
              .getContext('2d')
              .drawImage(
                image,
                (64 - width * scale) / 2,
                (64 - height * scale) / 2,
                width * scale,
                height * scale
              );
          } else canvas.hidden = true;
        })
        .catch(() => {
          canvas.hidden = true;
        });
    } else if (u.imageUrl) {
      const unitArt = el('img', 'sf-unit-thumb');
      unitArt.src = u.imageUrl;
      unitArt.alt = '';
      unitArt.loading = 'lazy';
      unitArt.onerror = () => {
        unitArt.hidden = true;
      };
      summary.append(unitArt);
    }
    summary.append(document.createTextNode(`${u.icon || '◆'} ${u.name}`));
    chip.append(
      summary,
      el('p', '', u.desc || `${u.hp} health · ${u.supply || 1} supply`)
    );
    preview.append(chip);
  }
  detail.append(preview);
  detail.append(
    button(
      pendingLaunch ? 'Confirm & deploy →' : 'Confirm faction',
      launch,
      'sf-button sf-primary'
    ),
    button('Random faction', () => {
      faction = factions()[Math.floor(Math.random() * factions().length)].id;
      api.selectFaction?.(faction);
      render();
    }),
    button('← Back', () => show(factionReturn), 'sf-link')
  );
  layout.append(tabs, art, detail);
  main.append(layout);
}
async function launch() {
  if (busy) return;
  if (!pendingLaunch) {
    api.selectFaction?.(faction);
    show('home');
    return;
  }
  busy = true;
  status('Preparing your battlefield…');
  try {
    show('loading');
    await new Promise(requestAnimationFrame);
    api.selectFaction?.(faction);
    const launch =
      pendingLaunch.mode === 'campaign' ? api.startCampaign : api.startSurvival;
    await launch(pendingLaunch.item.id, { factionId: faction, difficulty });
    hide();
    pendingLaunch = null;
  } catch (error) {
    show('factions');
    status(error.message || 'The battlefield could not load. Try again.');
  } finally {
    busy = false;
  }
}
function loading(main) {
  const region = catalog.regions.find(
    (r) => r.id === pendingLaunch?.item?.regionId
  );
  const panel = el('div', 'sf-loading');
  panel.append(
    artwork(
      `faction.${faction}.background`,
      `ui/faction_select/${faction}/background.webp`,
      { className: 'sf-loading-scene', fallback: fallbackArt(faction) }
    )
  );
  panel.append(
    heading(
      'Preparing the battlefield',
      'DEPLOYMENT / COALITION COMMAND',
      `${pendingLaunch?.item?.title || pendingLaunch?.item?.name || 'Battle'} · ${region?.name || ''}`
    )
  );
  const player = el('div', 'sf-loading-player');
  player.append(
    emblem(),
    el('b', '', identity.displayName || 'Commander'),
    el('span', '', `${factionName(faction)} · ${rankName()}`)
  );
  panel.append(
    player,
    el('p', '', 'Establishing command. Your army is ready to deploy.')
  );
  const bar = el('progress');
  bar.setAttribute('aria-label', 'Preparing the battlefield');
  panel.append(bar);
  main.append(panel);
}
async function resume() {
  try {
    await api.resume(checkpoint());
    hide();
  } catch (error) {
    status(error.message || 'This saved battle could not be restored.');
  }
}
function skirmish(main) {
  pendingLaunch = null;
  main.append(
    heading(
      'Single Player',
      'CUSTOM SKIRMISH',
      'Configure your opponents, then choose a battlefield, difficulty, temperament, and victory condition.'
    )
  );
  const panel = el('div', 'sf-setup');
  const player = el('label', 'sf-field', 'Your faction');
  const own = el('select');
  for (const f of factions()) {
    const o = el('option', '', f.name);
    o.value = f.id;
    o.selected = f.id === faction;
    own.append(o);
  }
  player.append(own);
  panel.append(player);
  const picks = [];
  for (let i = 0; i < 2; i++) {
    const label = el('label', 'sf-field', `Opponent ${i + 1}`);
    const select = el('select');
    if (i) {
      const none = el('option', '', 'No second opponent');
      none.value = '';
      select.append(none);
    }
    for (const f of factions()) {
      const o = el('option', '', f.name);
      o.value = f.id;
      o.selected = f.id === factions()[(i + 1) % factions().length].id;
      select.append(o);
    }
    label.append(select);
    panel.append(label);
    picks.push(select);
  }
  panel.append(
    el(
      'p',
      '',
      'Standard rules: each commander begins with the established economy and army. Annihilation, Conquest, and Central Relic remain available.'
    )
  );
  panel.append(
    button(
      'Battlefield & match settings →',
      () => {
        faction = own.value;
        api.configureSolo?.({
          factionId: faction,
          opponentFactions: picks.map((p) => p.value).filter(Boolean)
        });
        api.selectFaction?.(faction);
        api.showSkirmish?.();
        hide();
        addSkirmishBack();
      },
      'sf-button sf-primary'
    )
  );
  main.append(panel);
}
function addSkirmishBack() {
  const card = document.getElementById('startCard');
  if (!card || document.getElementById('sfSkirmishBack')) return;
  const b = button('← Command center', () => show('home'), 'sf-back');
  b.id = 'sfSkirmishBack';
  card.prepend(b);
}
function lobby(mode) {
  if (api.getIdentity?.().lobbyId) {
    void api.exitToLobby?.();
    return;
  }
  const url = new URL('../../turncraft-lobby.html', import.meta.url);
  url.searchParams.set('mode', mode);
  location.href = url.href;
}
function session(main) {
  const current = api.getIdentity?.() || {};
  main.append(
    heading(
      'Battle command',
      'MULTIPLAYER IN PROGRESS',
      'The shared battlefield continues while this screen is open.'
    )
  );
  const panel = el('div', 'sf-setup');
  panel.append(
    button(
      'Return to battle',
      () => {
        api.resumeBattle?.();
        hide();
      },
      'sf-button sf-primary'
    ),
    button(
      current.isRanked && !current.isGameOver
        ? 'Forfeit & return to lobby'
        : 'Return to lobby',
      () => {
        void api.exitToLobby?.();
      }
    ),
    button('Player profile', () => show('profile'))
  );
  main.append(panel);
}
function multiplayer(main) {
  main.append(
    heading(
      'Multiplayer command',
      'CATNMICE NETWORK',
      'Create a room, join your friends, or find your next rival. Your CatnMice identity comes with you.'
    )
  );
  const grid = el('div', 'sf-multi-grid');
  for (const [title, copy, mode] of [
    ['Quick Play', 'Find an available compatible public room.', 'quick'],
    [
      'Ranked Match',
      'Ranked matches are currently unavailable.',
      'ranked'
    ],
    [
      'Create Lobby',
      'Set the battlefield and invite your commanders.',
      'create'
    ],
    ['Browse Lobbies', 'Public battles waiting for reinforcements.', 'browse'],
    ['Join by Code', 'Jump directly into a private invitation.', 'code']
  ]) {
    const b = button('', () => lobby(mode), 'sf-multi-card');
    b.append(
      el('h3', '', title),
      el('p', '', copy),
      el('span', 'sf-arrow', '↗')
    );
    if (mode === 'ranked') b.disabled = true;
    grid.append(b);
  }
  main.append(grid);
}
function profileScreen(main) {
  const complete = Object.values(progress.campaign.completedMissions),
    totalStars = complete.reduce((sum, m) => sum + (m.stars || 0), 0);
  main.append(
    heading(
      identity.displayName || identity.username || 'Local Commander',
      'PLAYER DOSSIER',
      identity.linked
        ? 'Connected to your CatnMice account.'
        : 'Personal progress is saved on this device. Linked CatnMice accounts also sync their progress.'
    )
  );
  const stats = el('div', 'sf-stat-grid');
  for (const [value, label] of [
    [rankName(), 'Competitive rank'],
    [profile.rankedMatches ? profile.mmr : '—', 'TurnCraft MMR'],
    [`${complete.length} / 24`, 'Campaign missions'],
    [`${totalStars} / 72`, 'Campaign stars'],
    [profile.rankedWins || 0, 'Ranked wins'],
    [profile.rankedLosses || 0, 'Ranked losses']
  ]) {
    const item = el('div', 'sf-stat');
    item.append(el('b', '', value), el('small', '', label));
    stats.append(item);
  }
  main.append(stats);
  const records = Object.values(progress.survival.recordsByMap).sort(
    (a, b) => b.wave - a.wave
  );
  main.append(
    heading(
      'Survival records',
      'PERSONAL OPERATIONS',
      'Your best runs are saved with your account.'
    )
  );
  const list = el('div', 'sf-records');
  for (const record of records)
    list.append(
      el(
        'p',
        '',
        `Wave ${record.wave} · ${catalog.arenas.find((a) => a.id === record.arenaId)?.name || record.arenaId || record.regionId} · ${record.difficulty} · ${factionName(record.factionId)} · ${Math.floor(record.durationSeconds || 0)} seconds · ${record.enemiesDefeated || 0} defeated`
      )
    );
  if (!records.length)
    list.append(
      el(
        'p',
        '',
        'Deploy into Endless Survival to establish your first record.'
      )
    );
  main.append(list);
  const badges = el('div', 'sf-badges');
  for (const id of Object.keys(progress.achievements))
    if (progress.achievements[id]) {
      const badge = el('span', '', id.replaceAll('-', ' '));
      const wave = /^survival-(\d+)$/.exec(id)?.[1];
      if (wave)
        badge.prepend(
          artwork(`achievement.wave${wave}`, `achievements/wave-${wave}.png`, {
            className: 'sf-achievement-art'
          })
        );
      badges.append(badge);
    }
  main.append(
    badges
  );
}
function settings(main) {
  main.append(
    heading(
      'Command settings',
      'PRESENTATION & ACCESSIBILITY',
      'Preferences apply to this device.'
    )
  );
  const panel = el('div', 'sf-setup');
  const motion = el('label', 'sf-field');
  const check = el('input');
  check.type = 'checkbox';
  check.checked = localStorage.getItem('turncraft.reducedMotion') === 'true';
  check.onchange = () => {
    localStorage.setItem('turncraft.reducedMotion', String(check.checked));
    root.classList.toggle('sf-reduced-motion', check.checked);
  };
  motion.append(check, document.createTextNode(' Reduce menu motion'));
  panel.append(motion);
  const graphics = el('label', 'sf-field', 'Expansion artwork location');
  const input = el('input');
  input.type = 'url';
  input.placeholder = 'https://your-bucket.example/shattered-front/';
  input.value = localStorage.getItem('turncraft.expansionAssetBase') || '';
  graphics.append(input);
  panel.append(
    graphics,
    button('Save artwork location', () => {
      try {
        if (input.value && !/^https?:$/.test(new URL(input.value).protocol))
          throw new Error();
        if (input.value)
          localStorage.setItem('turncraft.expansionAssetBase', input.value);
        else localStorage.removeItem('turncraft.expansionAssetBase');
        status('Artwork location saved.');
        render();
      } catch {
        status('Enter a valid HTTPS artwork location.');
      }
    })
  );
  panel.append(
    button('Gameplay controls', () => {
      hide();
      document.getElementById('helpBtn')?.click();
    }),
    el(
      'p',
      '',
      'Battlefield controls, unit movement, abilities, zoom, and touch selection use TurnCraft’s existing settings.'
    )
  );
  main.append(panel);
}
function resultScreen(main) {
  const r = lastResult;
  if (!r) return show('home');
  const mission = catalog.missions.find((m) => m.id === r.missionId),
    banner = el('div', 'sf-result-banner');
  banner.append(
    artwork(
      r.won ? 'result.victory' : 'result.defeat',
      r.won ? 'ui/victory/victory.webp' : 'ui/victory/defeat.webp',
      { className: 'sf-result-art', fallback: fallbackArt(faction) }
    ),
    heading(
      r.mode === 'campaign'
        ? r.won
          ? 'Operation secured'
          : 'The line has fallen'
        : 'A stand to remember',
      'AFTER ACTION REPORT',
      mission?.title || catalog.arenas.find((a) => a.id === r.arenaId)?.name
    )
  );
  main.append(banner);
  const stats = el('div', 'sf-stat-grid');
  for (const [value, label] of [
    [
      r.mode === 'campaign'
        ? '★'.repeat(r.stars || 0) + '☆'.repeat(3 - (r.stars || 0))
        : r.wave,
      r.mode === 'campaign' ? 'Mission stars' : 'Waves reached'
    ],
    [r.enemiesDefeated || 0, 'Enemies defeated'],
    [r.unitsLost || 0, 'Units lost'],
    [Math.floor(r.durationSeconds || 0), 'Seconds survived']
  ]) {
    const item = el('div', 'sf-stat');
    item.append(el('b', '', value), el('small', '', label));
    stats.append(item);
  }
  main.append(stats);
  const content = el('div', 'sf-setup');
  for (const o of r.optionalObjectives || [])
    content.append(
      el(
        'p',
        '',
        typeof o === 'string'
          ? `✓ ${mission?.objectives?.find((item) => item.id === o)?.label || o}`
          : `${o.label || o.id}: ${o.completed ? 'Complete' : 'Incomplete'}`
      )
    );
  if (r.mostEffectiveUnit) {
    const units = api.getUnits?.() || [];
    const unit = (Array.isArray(units) ? units : Object.values(units)).find(
      (u) => u.id === r.mostEffectiveUnit
    );
    content.append(
      el('p', '', `Most effective unit: ${unit?.name || r.mostEffectiveUnit}`)
    );
  }
  if (r.mode === 'campaign') {
    content.append(
      el(
        'p',
        '',
        r.won
          ? 'Venn: “A clean victory. Try not to look surprised, Kade.”'
          : 'Kade: “I have identified a few improvements. Most of them involve more walls.”'
      )
    );
    const next =
      catalog.missions[
        catalog.missions.findIndex((m) => m.id === r.missionId) + 1
      ];
    if (r.won && next) {
      if (next.regionId !== r.regionId)
        content.append(
          el(
            'p',
            '',
            `New region and survival arena unlocked: ${catalog.regions.find((region) => region.id === next.regionId)?.name}.`
          )
        );
      content.append(
        button(
          'Continue to next mission →',
          () => chooseMission(next),
          'sf-button sf-primary'
        )
      );
    }
    if (r.won && !next)
      content.append(
        el(
          'p',
          '',
          'The Conductor falls silent. Across six regions, the gateways close. Beyond the broken network, one signal remains unanswered. The coalition will be ready.'
        )
      );
    content.append(button('Retry mission', () => chooseMission(mission)));
  } else {
    content.append(
      el(
        'p',
        '',
        `Previous personal best: wave ${r.previousBest || 0}. Milestones reached: ${(r.milestones || []).join(', ') || 'Keep fighting for wave 5.'}`
      ),
      button(
        'Defend again',
        () => {
          pendingLaunch = {
            mode: 'survival',
            item: catalog.arenas.find((a) => a.id === r.arenaId)
          };
          factionReturn = 'survival';
          show('factions');
        },
        'sf-button sf-primary'
      )
    );
  }
  content.append(button('Return to command center', () => show()));
  main.append(content);
}
async function initialize() {
  api = window.TurncraftExpansion;
  if (!api || initialized) return;
  initialized = true;
  if (api.ready) await api.ready;
  catalog = api.catalog;
  if (!catalog?.missions?.length) return;
  root.classList.toggle(
    'sf-reduced-motion',
    localStorage.getItem('turncraft.reducedMotion') === 'true'
  );
  if (!params.get('gameId') && !params.get('lobbyId'))
    show(params.get('view') === 'profile' ? 'profile' : 'home');
  loadManifest().then(() => {
    if (!root.hidden) render();
  });
  try {
    cloud = await import('./profile.mjs');
    profileUnsub = await cloud.watchProfile((data) => {
      {
        identity = data.identity || { displayName: 'Local Commander' };
        const key = `turncraft.shattered-front.progress.${encodeURIComponent(identity.username || identity.uid || 'local')}.v1`;
        if (key !== progressKey) {
          const local = data.identity && progressKey === 'turncraft.shattered-front.progress.local.v1' ? progress : null;
          progressKey = key;
          progress = readProgress(key);
          if (local) {
            progress = mergeProgress(progress, local);
            progress.pendingResults = [...(progress.pendingResults || []), ...(local.pendingResults || [])];
          }
        }
      }
      profile = data.profile || {};
      progress = mergeProgress(progress, profile);
      saveProgress();
      if (data.error)
        message =
          'Account sync is unavailable. Progress continues to save on this device.';
      if (!root.hidden) render();
      void flushSaves();
    });
  } catch {
    /* Local campaign and survival remain playable during a connection outage. */
  }
}
window.addEventListener('turncraft:expansion-ready', initialize, {
  once: true
});
window.addEventListener('sf:ready', initialize, { once: true });
if (window.TurncraftExpansion) initialize();
window.addEventListener('sf:menu', () => show());
window.addEventListener('turncraft:expansion-result', async (event) => {
  lastResult = { ...event.detail };
  if (lastResult.mode === 'survival')
    lastResult.wave = lastResult.highestWave || lastResult.wave;
  const key = `${lastResult.arenaId}:${lastResult.difficulty || 'medium'}`;
  lastResult.previousBest = progress.survival.recordsByMap[key]?.wave || 0;
  progress = recordResult(progress, lastResult);
  if (lastResult.mode !== 'campaign' || lastResult.won) {
    progress.pendingResults ||= [];
    progress.pendingResults.push({ ...lastResult });
  }
  saveProgress();
  show('result');
  await flushSaves();
});
window.addEventListener('online', flushSaves);
window.addEventListener('pagehide', () => profileUnsub?.());
window.addEventListener('keydown', (event) => {
  if (!root.hidden && event.key === 'Escape' && activeView !== 'home') {
    event.preventDefault();
    show();
  }
});
root.addEventListener('keydown', (event) => {
  if (event.key !== 'Tab' || root.hidden) return;
  const nodes = [
    ...root.querySelectorAll('button,input,select,a,summary')
  ].filter((n) => !n.disabled && n.getClientRects().length);
  const first = nodes[0],
    last = nodes.at(-1);
  if (!first) return;
  if (
    event.shiftKey &&
    (document.activeElement === first || !root.contains(document.activeElement))
  ) {
    event.preventDefault();
    last.focus();
  } else if (
    !event.shiftKey &&
    (document.activeElement === last || !root.contains(document.activeElement))
  ) {
    event.preventDefault();
    first.focus();
  }
});
