import { artwork, assetUrl, loadManifest } from './assets.mjs';
import { firebaseError } from './firebase-connection.mjs';
import {
  CHECKPOINT_KEY,
  checkpointKey,
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
    username: localStorage.getItem('gh.username') || params.get('username') || null,
    displayName: localStorage.getItem('gh.username') || params.get('username') || 'Local Commander'
  },
  profile = {},
  cloud;
let progressKey = `turncraft.shattered-front.progress.${encodeURIComponent(identity.username || 'local')}.v1`,
  progress = readProgress(progressKey);
let historyName = null, matchHistory = [], checkpointLoadedFor = null;
const pendingCheckpointUploads = new Set();
let checkpointSyncBusy = false;
let activeView = 'home',
  returnView = 'home',
  selectedRegion = '',
  pendingLaunch = null,
  lastResult = null,
  busy = false;
let message = '', syncError = '',
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
      if (result.resultId) await cloud.saveCheckpoint(null, account.username, result.resultId);
      if (result.resultId) {
        const key = checkpointKey(account.username);
        try { if (JSON.parse(localStorage.getItem(key) || 'null')?.resultId === result.resultId) localStorage.removeItem(key); } catch {}
      }
      if (owner !== progressKey) break;
      progress.pendingResults.shift();
      historyName = null;
      saveProgress();
    }
    if (startedFor === progressKey && !progress.pendingResults?.length && !pendingCheckpointUploads.has(identity.username) && syncError) {
      syncError = ''; status('Progress saved to Firestore.'); if (!root.hidden) render();
    }
  } catch (error) {
    if (startedFor === progressKey) reportSyncFailure(error);
  } finally {
    syncing = false;
    if (startedFor !== progressKey) void flushSaves();
  }
}
function checkpoint() {
  try {
    return JSON.parse(localStorage.getItem(checkpointKey(identity.username || api?.getIdentity?.()?.username)) || 'null');
  } catch {
    return null;
  }
}
function show(view = 'home', { focus = true } = {}) {
  if (view === 'profile') historyName = null;
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
function reportSyncFailure(error) {
  syncError = firebaseError(error).message;
  status(`Progress sync pending. ${syncError}`);
  if (!root.hidden) render();
}
async function retryAccountSync() {
  status('Reconnecting your Firestore profile…');
  try {
    await cloud.retryConnection();
    await flushSaves();
    const current = checkpoint();
    if (identity.linked && current?.username === identity.username) {pendingCheckpointUploads.add(identity.username);await flushCheckpointSaves();}
    if (!syncError) status('Profile connected to Firestore.');
  } catch (error) { reportSyncFailure(error); }
}
async function flushCheckpointSaves() {
  const owner = identity.username;
  if (checkpointSyncBusy || !cloud || !identity.linked || !pendingCheckpointUploads.has(owner)) return;
  const saved = checkpoint();
  if (!saved || saved.username !== owner) {pendingCheckpointUploads.delete(owner);return;}
  checkpointSyncBusy = true;
  let succeeded = false;
  try {
    await cloud.saveCheckpoint(saved, owner);succeeded = true;
    if (identity.username === owner && checkpoint()?.savedAt === saved.savedAt) pendingCheckpointUploads.delete(owner);
    if (identity.username === owner && !pendingCheckpointUploads.has(owner) && !progress.pendingResults?.length) {syncError = '';status('Battle checkpoint saved to Firestore.');if (!root.hidden) render();}
  } catch (error) {if (identity.username === owner) reportSyncFailure(error);}
  finally {
    checkpointSyncBusy = false;
    if (identity.username !== owner || succeeded && pendingCheckpointUploads.has(owner)) void flushCheckpointSaves();
  }
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
  if (syncError && cloud?.retryConnection) footer.append(button('Retry account sync', retryAccountSync, 'sf-button sf-sync-retry'));
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
            ? `${mission.missionStyle || 'Tactical operation'} · Deploy →`
            : '🔒 Locked'
      )
    );
    if (saved) {
      const remaining=(mission.objectives||[]).filter(o=>o.optional&&!(saved.optionalObjectives||[]).includes(o.id));
      b.append(el('small','',`${mission.missionStyle||'Campaign operation'}${saved.bestDurationSeconds?` · Best ${Math.floor(saved.bestDurationSeconds/60)}m ${saved.bestDurationSeconds%60}s`:''} · ${remaining.length} optional remaining`));
    }
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
      mission.missionStyle || 'Prepare, defend, and strike back.'
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
  content.append(el('p','sf-starting-conditions',mission.startingConditions||'Establish a base and complete the coalition objectives.'));
  if(mission.operation?.stages?.length) {
    const stages=el('ol','sf-objectives');
    for(const stage of mission.operation.stages)stages.append(el('li','',stage.title));
    content.append(el('b','','Tactical stages'),stages);
  }
  if(mission.campaignReward)content.append(el('p','',`Campaign reward: ${mission.campaignReward.label}. Optional objectives recover modest resources, intelligence or reinforcements for later operations.`));
  difficultyPicker(content);
  content.append(button('Launch Mission',launch,'sf-button sf-primary'));
  content.append(
    button(
      'Change faction →',
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
    await launch(pendingLaunch.item.id, { factionId: faction, difficulty, username: identity.username });
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
  const legacy = document.getElementById('overlay');
  if (legacy) legacy.style.display = 'none';
  main.append(heading('Single Player', 'CUSTOM SKIRMISH', 'Choose your army, battlefield and opponents, then start the battle.'));
  let saved = {};
  try { saved = JSON.parse(localStorage.getItem('csf.skirmishSetup.v1') || '{}') || {}; } catch {}
  const initial = { factionId: faction, difficulty: 'medium', style: 'balanced', aiCount: 2, ...(api.getSoloSettings?.() || {}), ...saved },
    allFactions = factions(), maps = api.getMaps?.() || [],
    config = { ...initial, opponents: Array.isArray(initial.opponents) ? initial.opponents.map(p => ({ ...p })) : [] };
  config.aiCount = Math.max(1, Math.min(7, Number(config.aiCount) || 2));
  const persist = () => { faction = config.factionId; try { localStorage.setItem('csf.skirmishSetup.v1', JSON.stringify(config)); } catch {} };
  const grid = el('div', 'sf-skirmish-layout'), army = el('section', 'sf-skirmish-army'), options = el('section', 'sf-skirmish-options');
  army.append(el('h3', '', 'Your faction'));
  const cards = el('div', 'sf-skirmish-factions');
  const markFaction = () => { for (const b of cards.children) { const active = b.dataset.faction === config.factionId; b.classList.toggle('selected', active); b.setAttribute('aria-pressed', String(active)); } };
  for (const f of allFactions) {
    const card = button('', () => { config.factionId = f.id; markFaction(); persist(); }, 'sf-skirmish-faction');
    card.dataset.faction = f.id; card.setAttribute('aria-label', `Play as ${f.name}`);
    const img = el('img'); img.src = fallbackArt(f.id); img.alt = ''; img.loading = 'lazy';
    img.onerror = () => { img.hidden = true; };
    const text = el('span', 'sf-skirmish-faction-text');
    text.append(el('b', '', f.name), el('small', '', f.playstyle || f.summary), el('small', '', `Strengths: ${f.strengths || 'Combined arms'}`), el('small', '', `Tradeoffs: ${f.weaknesses || 'Protect your economy'}`));
    card.append(img, text); cards.append(card);
  }
  markFaction(); army.append(cards); grid.append(army, options); main.append(grid);
  function selectField(host, label, entries, value, onChange) {
    const wrap = el('label', 'sf-field', label), select = el('select'); select.setAttribute('aria-label', label);
    for (const [id, name] of entries) { const o = el('option', '', name); o.value = id; select.append(o); }
    select.value = value; select.onchange = () => { onChange(select.value); persist(); }; wrap.append(select); host.append(wrap); return select;
  }
  const labels = ids => ids.map(id => [id, id[0].toUpperCase() + id.slice(1)]), difficultyOptions = labels(['easy', 'medium', 'hard', 'pro']), styleOptions = labels(['cautious', 'balanced', 'aggressive']);
  options.append(el('h3', '', 'Battle settings'));
  const standard = el('div', 'sf-skirmish-fields'); options.append(standard);
  selectField(standard, 'AI opponents', Array.from({ length: 7 }, (_, i) => [String(i + 1), String(i + 1)]), String(config.aiCount), value => { config.aiCount = Number(value); renderOpponents(); updateMaps(); });
  selectField(standard, 'Battle difficulty', difficultyOptions, config.difficulty, value => { config.difficulty = value; });
  selectField(standard, 'AI temperament', styleOptions, config.style, value => { config.style = value; });
  selectField(standard, 'Victory condition', [['annihilation', 'Annihilation'], ['conquest', 'Conquest'], ['relic', 'Central Relic']], config.gameMode || 'annihilation', value => { config.gameMode = value; });
  const advanced = el('details', 'sf-skirmish-advanced'); advanced.append(el('summary', '', 'Configure each opponent')); const opponentRows = el('div', 'sf-skirmish-opponents'); advanced.append(opponentRows); options.append(advanced);
  function renderOpponents() {
    opponentRows.replaceChildren();
    for (let i = 0; i < config.aiCount; i++) {
      const settings = config.opponents[i] ||= { factionId: allFactions[(i + 1) % allFactions.length]?.id || config.factionId, difficulty: '', style: '' }, row = el('div', 'sf-skirmish-opponent');
      row.append(el('b', '', `AI ${i + 1}`));
      selectField(row, `Opponent ${i + 1} faction`, allFactions.map(f => [f.id, f.name]), settings.factionId, value => { settings.factionId = value; });
      selectField(row, `Opponent ${i + 1} difficulty`, [['', 'Use battle difficulty'], ...difficultyOptions], settings.difficulty || '', value => { settings.difficulty = value; });
      selectField(row, `Opponent ${i + 1} temperament`, [['', 'Use battle temperament'], ...styleOptions], settings.style || '', value => { settings.style = value; });
      opponentRows.append(row);
    }
  }
  renderOpponents();
  const battlefields = el('section', 'sf-skirmish-battlefields'); battlefields.append(el('h3', '', 'Battlefield'));
  const note = el('p', 'sf-skirmish-map-note'), gallery = el('div', 'sf-skirmish-maps'); battlefields.append(note, gallery); main.append(battlefields);
  for (const map of maps) {
    const b = button('', () => { config.mapId = map.id; updateMaps(); persist(); }, 'sf-skirmish-map'); b.dataset.map = map.id;
    const preview = el('canvas'); preview.width = 252; preview.height = 156; preview.setAttribute('aria-hidden', 'true');
    b.append(preview, el('b', '', map.name), el('small', '', `${map.capacity || map.playerCount || 3} commanders · ${map.width || 42} × ${map.height || map.width || 30}`), el('small', '', map.description || map.subtitle || ''));
    b.setAttribute('aria-label', `Battlefield: ${map.name}`); gallery.append(b); api.drawMapPreview?.(preview, map.id);
  }
  function updateMaps() {
    const required = config.aiCount + 1, compatible = maps.filter(m => (m.capacity || m.playerCount || 3) >= required);
    if (!compatible.some(m => m.id === config.mapId)) config.mapId = compatible[0]?.id || config.mapId;
    for (const b of gallery.children) { const map = maps.find(m => m.id === b.dataset.map), active = map.id === config.mapId; b.disabled = (map.capacity || map.playerCount || 3) < required; b.classList.toggle('selected', active); b.setAttribute('aria-pressed', String(active)); }
    note.textContent = `${required} commanders. Choose from every supported battlefield; maps too small for this army are disabled.`;
  }
  updateMaps(); persist();
  const launch = button('Start Battle', async () => {
    const selected = { ...config, opponents: config.opponents.slice(0, config.aiCount).map(p => ({ ...p })) };
    if (!selected.mapId || !api.startSkirmish) { status('The battlefield is still preparing. Please try again.'); return; }
    launch.disabled = true;
    try { await api.startSkirmish(selected); hide(); } catch (error) { status(error.message || 'This battle could not start.'); launch.disabled = false; }
  }, 'sf-button sf-primary'); launch.classList.add('sf-skirmish-start'); main.append(launch);
}
function lobby(mode) {
  if (api.getIdentity?.().lobbyId) {
    void api.exitToLobby?.();
    return;
  }
  const url = new URL('../../turncraft-lobby.html', import.meta.url);
  url.searchParams.set('mode', mode);
  if (identity.username) url.searchParams.set('username', identity.username);
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
      'Play a human rival in a ranked duel. Results and MMR save to your profile.',
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
    grid.append(b);
  }
  main.append(grid);
}
function profileScreen(main) {
  if (cloud && identity.linked && historyName !== identity.username) {
    const name = identity.username;
    historyName = name;
    matchHistory = [];
    void cloud.getHistory(name).then(rows => {
      if (identity.username !== name) return;
      matchHistory = rows;
      if (activeView === 'profile' && !root.hidden) render();
    }).catch(() => { historyName = null; });
  }
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
  main.append(heading('Game history', 'RECENT BATTLES', 'Completed games are stored with the profile that played them.'));
  const historyList = el('div', 'sf-records');
  for (const record of matchHistory) historyList.append(el('p', '', `${record.mode} · ${record.outcome} · ${record.opponents?.join(', ') || record.missionId || record.arenaId || 'Solo operation'} · ${Math.floor(record.durationSeconds || 0)} seconds${record.mode === 'ranked' ? ` · ${record.mmrDelta > 0 ? '+' : ''}${record.mmrDelta} MMR` : ''}`));
  if (!matchHistory.length) historyList.append(el('p', '', 'Your completed battles will appear here after syncing.'));
  main.append(historyList);
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
  const radio=el('label','sf-field'),radioCheck=el('input');radioCheck.type='checkbox';radioCheck.checked=localStorage.getItem('turncraft.radioMessages')!=='false';radioCheck.onchange=()=>localStorage.setItem('turncraft.radioMessages',String(radioCheck.checked));radio.append(radioCheck,document.createTextNode(' Show ordinary NPC radio messages (objectives remain visible)'));panel.append(radio);
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
  if(r.reason)content.append(el('p','',r.reason));
  for(const reason of r.starReasons||[])content.append(el('p','',`${reason.earned?'★':'☆'} ${reason.label}`));
  for(const id of r.completedObjectives||[])content.append(el('p','',`✓ ${mission?.objectives?.find(o=>o.id===id)?.label||id}`));
  for(const reward of r.campaignRewards||[])content.append(el('p','',`Campaign reward earned: ${reward.label}`));
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
    content.append(button(r.won?'Replay mission':'Retry mission', () => chooseMission(mission)),button('Campaign Map',()=>show('campaign')));
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
  if (window.TurncraftStartup?.ready) await window.TurncraftStartup.ready;
  if (api.ready) await api.ready;
  catalog = api.catalog;
  if (!catalog?.missions?.length) return;
  root.classList.toggle(
    'sf-reduced-motion',
    localStorage.getItem('turncraft.reducedMotion') === 'true'
  );
  if (params.get('mode') === 'solo' || (!params.get('gameId') && !params.get('lobbyId')))
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
          progressKey = key;
          progress = readProgress(key);
          historyName = null;
        }
      }
      profile = data.profile || {};
      progress = mergeProgress(progress, profile);
      saveProgress();
      if (identity.linked && checkpointLoadedFor !== identity.username) {
        const name = identity.username;
        checkpointLoadedFor = name;
        void cloud.getCheckpoint(name).then(saved => {
          if (name !== identity.username) return;
          const current = checkpoint();
          if (saved?.cleared) {
            if (current && saved.savedAt >= current.savedAt && (!saved.completedRunId || saved.completedRunId === current.resultId)) localStorage.removeItem(checkpointKey(name));
            if (checkpoint()?.savedAt > saved.savedAt) pendingCheckpointUploads.add(name);
            void flushCheckpointSaves();
            if (!root.hidden) render();
            return;
          }
          if (saved && (!current || saved.savedAt > current.savedAt)) localStorage.setItem(checkpointKey(name), JSON.stringify(saved));
          if (current?.username === name && (!saved || current.savedAt > saved.savedAt)) pendingCheckpointUploads.add(name);
          void flushCheckpointSaves();
          if (!root.hidden) render();
        }).catch(error => { checkpointLoadedFor = null; reportSyncFailure(error); });
      }
      if (data.error) { syncError = data.error; message = `Progress sync pending. ${data.error}`; }
      else if (data.connectionStatus === 'connected' && syncError && !progress.pendingResults?.length && !pendingCheckpointUploads.has(identity.username)) { syncError = ''; message = 'Profile connected to Firestore.'; }
      else if (data.connectionStatus === 'cached') message = 'Using your downloaded profile. Pending progress will sync to Firestore when connected.';
      if (!root.hidden) render();
      void flushSaves();
      if (data.connectionStatus === 'connected') void flushCheckpointSaves();
    });
  } catch (error) {
    reportSyncFailure(error);
  }
}
window.addEventListener('turncraft:expansion-ready', initialize, {
  once: true
});
window.addEventListener('sf:ready', initialize, { once: true });
if (window.TurncraftExpansion) initialize();
window.addEventListener('sf:menu', () => show());
window.addEventListener('turncraft:expansion-result', async (event) => {
  historyName = null;
  lastResult = { ...event.detail };
  if (lastResult.mode === 'survival')
    lastResult.wave = lastResult.highestWave || lastResult.wave;
  lastResult.resultId ||= crypto.randomUUID();
  if (lastResult.username && lastResult.username !== identity.username) {
    const ownerKey = `turncraft.shattered-front.progress.${encodeURIComponent(lastResult.username)}.v1`;
    const owned = recordResult(readProgress(ownerKey), lastResult);
    owned.pendingResults ||= [];
    owned.pendingResults.push({...lastResult});
    localStorage.setItem(ownerKey, JSON.stringify(owned));
    status('This run is saved for the profile that started it. Reopen that profile to sync.');
    return;
  }
  const key = `${lastResult.arenaId}:${lastResult.difficulty || 'medium'}`;
  lastResult.previousBest = progress.survival.recordsByMap[key]?.wave || 0;
  progress = recordResult(progress, lastResult);
  {
    progress.pendingResults ||= [];
    progress.pendingResults.push({ ...lastResult });
  }
  saveProgress();
  show('result');
  await flushSaves();
});
window.addEventListener('turncraft:checkpoint', event => {
  const saved = event.detail?.checkpoint;
  if (saved?.username) pendingCheckpointUploads.add(saved.username);
  if (saved?.username === identity.username) void flushCheckpointSaves();
});
window.addEventListener('online', flushSaves);
window.addEventListener('online', flushCheckpointSaves);
window.addEventListener('pagehide', event => { if (!event.persisted) profileUnsub?.(); });
window.addEventListener('keydown', (event) => {
  if (!root.hidden && event.key === 'Escape' && activeView !== 'home') {
    event.preventDefault();
    show();
  }
});
root.addEventListener('keydown', (event) => {
  if (event.key !== 'Tab' || root.hidden || document.getElementById('graphicsWarnings')?.hidden === false) return;
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
