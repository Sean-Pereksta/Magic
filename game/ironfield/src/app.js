/* IRONFIELD — offline interface, camera input, scenario persistence, and audio.
 * Source is deliberately dependency-free and works when opened with file://.
 */
(function () {
  'use strict';
  const $ = id => document.getElementById(id);
  const all = selector => Array.from(document.querySelectorAll(selector));
  const clone = value => JSON.parse(JSON.stringify(value));
  const fmt = value => Math.round(Number(value) || 0).toLocaleString('en-US');
  const esc = value => String(value).replace(/[&<>"']/g, s => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[s]));
  const clamp = (x, lo, hi) => Math.max(lo, Math.min(hi, x));
  const clock = t => `${Math.floor(t / 60).toString().padStart(2, '0')}:${Math.floor(t % 60).toString().padStart(2, '0')}`;
  const ICONS = {
    menu:'<path d="M4 6h16M4 12h16M4 18h16"/>',
    folder:'<path d="M3 7h6l2-3h9v15H3z"/><path d="M3 7V4h5l3 3"/>',
    mute:'<path d="M10 5 5 9H2v6h3l5 4zM16 9l6 6m0-6-6 6"/>',
    sound:'<path d="M10 5 5 9H2v6h3l5 4zM15 8a6 6 0 0 1 0 8m3-11a10 10 0 0 1 0 14"/>',
    settings:'<path d="m9 3-.7 3-2.5 1.5L3 7l-1 3 2.3 2-1 3 2.5 2 2.7-.4L10 20h4l1.5-3.4 2.7.4 2.5-2-1-3L22 10l-1-3-2.8.5L15.7 6 15 3z"/><circle cx="12" cy="11.5" r="3"/>',
    help:'<circle cx="12" cy="12" r="9"/><path d="M9 9a3 3 0 0 1 6 0c0 2-3 2-3 5m0 3h.01"/>',
    expand:'<path d="M8 3H3v5m13-5h5v5M3 16v5h5m13-5v5h-5"/>',
    close:'<path d="m6 6 12 12M18 6 6 18"/>',
    swords:'<path d="m4 3 2 6 11 11m-2-3 4-4M3 21l4-4m-3-4 7 7M20 3l-2 6-6 6M3 3l6 2 12 16"/>',
    army:'<path d="M4 11h5v10H4zM15 11h5v10h-5zM9 14h6v7H9z"/><circle cx="6.5" cy="7" r="2.5"/><circle cx="17.5" cy="7" r="2.5"/><circle cx="12" cy="10" r="2.5"/>',
    fit:'<path d="M8 3H3v5m13-5h5v5M3 16v5h5m13-5v5h-5"/><path d="M8 8h8v8H8z"/>',
    focus:'<circle cx="12" cy="12" r="6"/><path d="M12 2v5m0 10v5M2 12h5m10 0h5"/>',
    move:'<path d="M3 18h6V8h11m-5-5 5 5-5 5"/>',
    face:'<path d="M4 17a8 8 0 0 1 13-9m0-5v5h-5"/><path d="m9 20 3-8 3 8-3-2z"/>',
    shield:'<path d="m12 2 8 4v6c0 5-8 10-8 10S4 17 4 12V6zM12 7v8"/>',
    bolt:'<path d="m14 2-10 12h7l-1 8L21 9h-8z"/>',
    flag:'<path d="M5 22V3m0 1c5-5 9 5 15 0v10c-6 5-10-5-15 0"/>',
    retreat:'<path d="M21 18h-7V7H3m5-5L3 7l5 5"/>',
    split:'<path d="M12 21v-8M12 13 5 6m7 7 7-7M5 12V6h6m2 0h6v6"/>',
    play:'<path d="m7 4 13 8-13 8z" fill="currentColor" stroke="none"/>',
    pause:'<path d="M7 5v14M17 5v14" stroke-width="4"/>',
    reset:'<path d="M3 3v6h6M3 9a9 9 0 1 1 .4 8"/>',
    camera:'<path d="M3 7h4l2-3h6l2 3h4v13H3z"/><circle cx="12" cy="13" r="4"/>',
    trash:'<path d="M3 6h18M6 6l1 15h10l1-15M9 6V3h6v3m-5 4v7m4-7v7"/>',
    crown:'<path d="m3 6 4 4 5-7 5 7 4-4-2 12H5zM6 21h12"/><circle cx="3" cy="5" r="1"/><circle cx="21" cy="5" r="1"/>'
  };
  const icon = name => `<svg viewBox="0 0 24 24" aria-hidden="true">${ICONS[name] || ICONS.flag}</svg>`;
  all('[data-icon]').forEach(el => { el.innerHTML = icon(el.dataset.icon); });
  function setIcon(el, name) { el.innerHTML = icon(name); }
  function toast(message, duration = 3600) {
    const div = document.createElement('div'); div.className = 'toast'; div.textContent = message;
    $('toastArea').appendChild(div);
    while ($('toastArea').children.length > 3) $('toastArea').firstElementChild.remove();
    window.setTimeout(() => div.remove(), duration);
  }
  function safeRead(key, fallback) { try { return JSON.parse(localStorage.getItem(key)) || fallback; } catch (_) { return fallback; } }
  function safeWrite(key, value) { try { localStorage.setItem(key, JSON.stringify(value)); return true; } catch (_) { return false; } }
  const STORAGE = 'ironfield.scenarios.v1';
  const PREFS = 'ironfield.preferences.v1';
  let data, engine, renderer, config, chosenPreset, customScaleBase = null, deployment = [];
  const armyLimit=()=>Math.floor((data.MAX_UNITS||40000)/2);
  const terrainLimit=()=>data.MAX_TERRAIN_REGIONS||192;
  const structureKey=value=>typeof value==='string'&&/^fort-\d+$/.test(value)&&Number(value.slice(5))<terrainLimit()?value:Number.isInteger(value)&&value>=0&&value<terrainLimit()?'fort-'+value:undefined;
  const STRUCTURE_KINDS=['stone','palisade','earthwork','house','watchtower','barricade','keep'];
  let rampartDraft={ranks:2,spacing:'normal'};
  let editorTool = null, terrainHistory = [];
  let activeTeam = 0, control = '0', speed = 1, selection = new Set(), commandMode = null;
  let troopCategory = 'all';
  let commanderCursor = -1;
  let accumulator = 0, lastFrame = 0, lastUI = 0, lastMinimap = 0, fps = 60;
  let simulationRatio = 1, observedTime = 0, simulatedTime = 0, ratioTimer = 0;
  let endedShown = false, rebuildTimer = 0, resizeTimer = 0, cinematic = false;
  let visibleEventSignature = '', mouseWorld = {x:3200,y:2200}, mouseScreen = null;
  let menuOpen = true, enteredField = false, menuResumeRunning = false, menuMapId = null, menuFilter = 'all';
  let menuCollection = 'fields', historicalPresetId = null;
  let lastMiniSignature = '', lastRendered = 0;
  let cancelBattlefieldGesture = () => {};
  const keys = new Set();
  const preferences = Object.assign({quality:'auto', showHealth:false, showLabels:true, showOrders:true, showBodies:true, corpseMode:'brief', animations:'full', sound:false, volume:35}, safeRead(PREFS, {}));
  if(!['brief','long','battle'].includes(preferences.corpseMode))preferences.corpseMode='brief';
  if(!['full','reduced'].includes(preferences.animations))preferences.animations='full';
  const canvas = $('battlefield');

  class BattlefieldAudio {
    constructor() { this.ctx = null; this.volume = .35; this.enabled = false; this.lastHit = 0; this.lastCount = 0; }
    enable(value) {
      this.enabled = value;
      if (!value) return;
      try {
        const Ctx = window.AudioContext || window.webkitAudioContext;
        if (!Ctx) { this.enabled = false; return; }
        if (!this.ctx) { this.ctx = new Ctx(); this.master = this.ctx.createGain(); this.master.connect(this.ctx.destination); }
        this.master.gain.value = this.volume * .25;
        if (this.ctx.state === 'suspended') this.ctx.resume().catch(() => {});
      } catch (_) { this.enabled = false; }
    }
    tone(freq, duration, type = 'sine', gain = .2, slide = 0) {
      if (!this.enabled || !this.ctx || this.ctx.state !== 'running') return;
      const t = this.ctx.currentTime, o = this.ctx.createOscillator(), g = this.ctx.createGain();
      o.type = type; o.frequency.setValueAtTime(freq, t);
      if (slide) o.frequency.exponentialRampToValueAtTime(Math.max(12, freq + slide), t + duration);
      g.gain.setValueAtTime(gain, t); g.gain.exponentialRampToValueAtTime(.001, t + duration);
      o.connect(g); g.connect(this.master); o.start(t); o.stop(t + duration);
    }
    horn(victory) {
      if (!this.enabled) return;
      const notes = victory ? [164.8,207.65,246.94,329.63] : [110,146.83,164.81];
      notes.forEach((n, i) => window.setTimeout(() => this.tone(n, .8, 'sawtooth', .2), i * 170));
    }
    update(now, stats) {
      const dead = stats.teams[0].dead + stats.teams[1].dead;
      if (dead > this.lastCount && now - this.lastHit > .17 && engine.phase === 'running') {
        this.lastHit = now; this.tone(90 + (dead % 7) * 47, .07 + (dead % 3) * .025, 'triangle', .12, -50);
        if (dead % 9 === 0) this.tone(64, .22, 'sine', .3, -30);
      }
      this.lastCount = dead;
    }
  }
  const audio = new BattlefieldAudio();

  function mapKind(map){
    if(map.fortress&&data.FORTRESS_TYPES?.[map.fortress.kind])return data.FORTRESS_TYPES[map.fortress.kind].name;
    const regions=map.regions||[];
    if(regions.some(r=>r.castleGate))return regions.filter(r=>r.castleTower).length>=4?'Walled castle':'Fortified siege';
    if(regions.some(r=>r.structureKind==='house'))return 'Village theater';
    return regions.some(r=>r.type==='wall'||r.type==='gate')?'Fortified ground':map.biome==='desert'?'Desert theater':map.biome==='tundra'?'Winter theater':map.biome==='wetland'?'River & marsh':map.biome==='woodland'?'Woodland theater':map.biome==='highland'?'Mountain theater':'Open battlefield';
  }
  function mapArea(map){return(map.width*map.height/(6400*4400)).toFixed(2).replace(/\.00$/,'');}
  function mapSizeName(map){const area=map.width*map.height;return area>=16000*11000?'Colossal':area>=12800*8800?'Huge':area>6400*4400?'Large':'Standard';}
  function mapSizeLabel(map){return`${mapSizeName(map)} · ${mapArea(map)}× area`;}
  function largestMapsFirst(a,b){return b.width*b.height-a.width*a.height;}
  function mapPreview(map,scenario){
    const scaleX=640/map.width,scaleY=440/map.height,colors={grassland:'#52674b',woodland:'#405b45',desert:'#8c7951',tundra:'#7c8d89',wetland:'#415e53',highland:'#626b56',ashland:'#665f4e'};
    const terrain=(map.regions||[]).map(r=>{
      const x=r.x*scaleX,y=r.y*scaleY,w=r.w*scaleX,h=r.h*scaleY;
      if(r.type==='hill')return`<ellipse cx="${x+w/2}" cy="${y+h/2}" rx="${w/2}" ry="${h/2}" fill="#c3c28c14" stroke="#bdc7a230"/><ellipse cx="${x+w/2}" cy="${y+h/2}" rx="${w*.37}" ry="${h*.37}" fill="none" stroke="#bdc7a225"/>`;
      const fill={forest:'#223d2b',rock:'#7f836c',water:'#315a65',bridge:'#c6ad78',wall:r.structureKind==='palisade'?'#96714b':r.structureKind==='earthwork'?'#8c8563':r.structureKind==='house'?'#a36f52':'#d6cdb1',gate:'#ad7744',road:'#b8a17a',mud:'#66503b',marsh:'#52786d',snow:'#d3ded5',trench:'#3c3829'}[r.type]||'#70846a';
      return`<rect x="${x}" y="${y}" width="${w}" height="${h}" rx="${r.type==='forest'?Math.min(w,h)*.2:r.type==='rock'?5:0}" fill="${fill}" fill-opacity="${r.type==='forest'?.8:1}" stroke="#17241c55" stroke-width="1"/>`;
    }).join('');
    let ranks='';
    const siege=scenario?.formations;
    if(siege)for(const group of siege){
      const color=group.team?'#ec947b':'#8dd2e3',facing=Number.isFinite(group.facing)?group.facing:group.team?Math.PI:0;
      const type=data.TYPES[group.type],form=data.FORMATIONS[group.formation]||{},spacing=(type.siege?48:type.mounted?28:17)*(form.spacing||1);
      const columns=form.files?Math.min(group.count,form.files):form.ranks?Math.ceil(group.count/form.ranks):Math.ceil(Math.sqrt(group.count*1.6));
      let depth=Math.ceil(group.count/columns)*spacing,front=columns*spacing;
      if(group.offsets?.length){
        const nx=Math.cos(facing),ny=Math.sin(facing),points=group.offsets.map(p=>({x:p.x*nx+p.y*ny,y:-p.x*ny+p.y*nx}));
        depth=Math.max(...points.map(p=>p.x))-Math.min(...points.map(p=>p.x))+spacing;
        front=Math.max(...points.map(p=>p.y))-Math.min(...points.map(p=>p.y))+spacing;
      }
      const w=Math.max(3,depth*scaleX),h=Math.max(3,front*scaleY);
      const shape=group.formation==='wedge'?`<path d="M${w/2} 0 ${-w/2} ${-h/2} ${-w/2} ${h/2}Z"/>`:`<rect x="${-w/2}" y="${-h/2}" width="${w}" height="${h}" rx="1"/>`;
      ranks+=`<g transform="translate(${group.x*scaleX} ${group.y*scaleY}) rotate(${facing*180/Math.PI})" fill="${color}" fill-opacity=".9" stroke="#1d302b" stroke-width=".7">${shape}<path d="M${w/2+2} -2 ${w/2+5} 0 ${w/2+2} 2" stroke="${color}" fill="none" stroke-width="1"/></g>`;
    }
    else for(let team=0;team<2;team++)for(let group=0;group<4;group++){
      const x=team?520:100,y=95+group*79,color=team?'#ec947b':'#8dd2e3';
      for(let row=0;row<3;row++)for(let col=0;col<8;col++)ranks+=`<rect x="${x+row*5}" y="${y+col*4}" width="2.6" height="2.6" fill="${color}" opacity=".9"/>`;
      ranks+=`<path d="M${x-7} ${y-5}v25m0-25h13l-3 4 3 4h-13" stroke="${color}" stroke-width="1.4" fill="${color}"/>`;
    }
    const orientation=historicalInfo(scenario)?'<text x="29" y="35" fill="#ead9abbb" font-size="10" font-family="Arial" letter-spacing="1">OPENING DEPLOYMENT · SCHEMATIC</text>':'<path d="M321 35v-21m-5 8 5-8 5 8" stroke="#e6d8a1aa" fill="none"/><text x="320" y="53" text-anchor="middle" fill="#ead9abbb" font-size="10" font-family="Georgia">N</text>';
    return`<svg viewBox="0 0 640 440" xmlns="http://www.w3.org/2000/svg"><rect x="1" y="1" width="638" height="438" rx="7" fill="${colors[map.biome]||colors.grassland}" stroke="#c7c29377" stroke-width="2"/><path d="M160 0v440M320 0v440M480 0v440M0 110h640M0 220h640M0 330h640" stroke="#d6ddaa12" fill="none"/>${terrain}${ranks}${orientation}<rect x="13" y="13" width="614" height="414" rx="3" fill="none" stroke="#e0d2a522"/></svg>`;
  }
  function updateMenuMap(id){
    const map=data.MAPS.find(m=>m.id===id&&m.id!=='custom')||data.MAPS[0];menuMapId=map.id;
    $('menuMapTitle').textContent=map.name;$('menuMapTag').textContent=`${mapSizeName(map)} MAP · ${mapKind(map)}`.toUpperCase();
    $('menuMapDescription').textContent=map.description;
    $('menuMapDimensions').textContent=`${mapSizeLabel(map)} · ${fmt(map.width)} × ${fmt(map.height)}`;
    const scenarios=data.PRESETS.filter(p=>p.map===map.id);
    $('menuMapPreview').innerHTML=mapPreview(map,scenarios[0]);
    $('menuScenarioSelect').innerHTML=scenarios.map(p=>`<option value="${esc(p.id)}">${esc(mapSizeLabel(map))} — ${esc(p.name)}</option>`).join('');
    $('menuScenarioSelect').value=scenarios[0]?scenarios[0].id:'';
    $('menuScenarioSelect').hidden=scenarios.length<2;
    all('[data-menu-map]').forEach(b=>{const active=b.dataset.menuMap===map.id;b.classList.toggle('active',active);b.setAttribute('aria-pressed',String(active));});
  }
  function historicalInfo(preset){return preset&&(typeof preset.historical==='object'?preset.historical:preset.history);}
  function historicalBattles(){return data.PRESETS.filter(p=>historicalInfo(p));}
  function historicalYear(year){return Number(year)<0?`${Math.abs(Number(year))} BCE`:`${year} CE`;}
  function showHistoricalBattle(id){
    const preset=historicalBattles().find(p=>p.id===id)||historicalBattles()[0];if(!preset)return;
    historicalPresetId=preset.id;const h=historicalInfo(preset),map=preset.map==='custom'?preset.customMap:data.MAPS.find(m=>m.id===preset.map);
    $('menuMapTitle').textContent=preset.name;
    $('menuMapTag').textContent=`${h.era} · ${historicalYear(h.year)}`.toUpperCase();
    $('menuMapDescription').textContent=preset.description||h.summary;
    const counts=preset.armies.map(totalRoster);
    $('menuMapDimensions').textContent=`${h.location} · ${fmt(counts[0]+counts[1])} soldiers`;
    $('menuMapPreview').innerHTML=mapPreview(map,preset);
    $('menuScenarioSelect').hidden=true;
    $('historyDeployment').textContent=h.deployment||h.summary||'';
    $('historyScaleNote').textContent=h.scaleNote||'A scaled recreation of the opening deployment. The battle can unfold differently under your command.';
    $('historyUnitNotes').textContent=Array.isArray(h.unitNotes)?h.unitNotes.join(' '):h.unitNotes||'';
    $('historySources').innerHTML=(h.sources||[]).filter(s=>/^https:\/\//.test(s.url)).map(s=>`<a href="${esc(s.url)}" target="_blank" rel="noopener noreferrer">${esc(s.title)} ↗</a>`).join('');
    all('[data-history-battle]').forEach(b=>{const active=b.dataset.historyBattle===preset.id;b.classList.toggle('active',active);b.setAttribute('aria-pressed',String(active));});
  }
  function filterHistoricalBattles(){
    const query=$('historySearch').value.trim().toLowerCase(),era=$('historyEra').value;
    const visible=historicalBattles().filter(p=>{const h=historicalInfo(p);return(era==='all'||h.era===era)&&(!query||`${p.name} ${h.location} ${h.year} ${h.era} ${p.armies.map(a=>a.name).join(' ')}`.toLowerCase().includes(query));});
    const ids=new Set(visible.map(p=>p.id));all('[data-history-battle]').forEach(b=>{b.hidden=!ids.has(b.dataset.historyBattle);});
    $('historyResults').textContent=`${visible.length} of ${historicalBattles().length} battles · Opening deployments`;
    $('historyEmpty').hidden=visible.length>0;
    if(visible.length&&!ids.has(historicalPresetId))showHistoricalBattle(visible[0].id);
  }
  function buildHistoricalBrowser(){
    const battles=historicalBattles().slice().sort((a,b)=>Number(historicalInfo(a).year)-Number(historicalInfo(b).year));
    $('historyCount').textContent=battles.length;
    $('historyEra').innerHTML='<option value="all">Every era</option>'+Array.from(new Set(battles.map(p=>historicalInfo(p).era))).map(era=>`<option value="${esc(era)}">${esc(era)}</option>`).join('');
    $('historyEra').value='all';
    $('historyGrid').innerHTML=battles.map(p=>{const h=historicalInfo(p);return`<button class="history-card" data-history-battle="${esc(p.id)}" aria-pressed="false"><span class="history-card-top"><span>${esc(h.era)}</span><b>${esc(historicalYear(h.year))}</b></span><strong>${esc(p.name)}</strong><span class="history-card-place">${esc(h.location)}</span><small>${esc(p.armies[0].name)}<span>vs</span>${esc(p.armies[1].name)}</small></button>`;}).join('');
    filterHistoricalBattles();
  }
  function setMenuCollection(collection){
    menuCollection=collection;
    const history=collection==='history';
    $('historyBrowser').hidden=$('historyBriefing').hidden=!history;
    $('menuMapGrid').hidden=$('battlefieldToolbar').hidden=history;
    $('menuFieldsTab').classList.toggle('active',!history);$('menuFieldsTab').setAttribute('aria-pressed',String(!history));
    $('menuHistoryTab').classList.toggle('active',history);$('menuHistoryTab').setAttribute('aria-pressed',String(history));
    $('menuBrowseTitle').textContent=history?'Great battles. Your command.':'A field worth fighting for.';
    $('menuMapCount').textContent=history?`${historicalBattles().length} HISTORICAL BATTLES`:`${data.MAPS.filter(m=>m.id!=='custom').length} BATTLEFIELDS`;
    if(history)showHistoricalBattle(historicalPresetId);else updateMenuMap(menuMapId||config.map);
  }
  function buildMenuMaps(){
    const maps=data.MAPS.filter(m=>m.id!=='custom').sort(largestMapsFirst);
    $('menuTroopCount').textContent=data.TYPE_ORDER.length;
    $('menuMapCount').textContent=`${maps.length} BATTLEFIELDS`;
    $('menuPlanCount').textContent=`${Object.keys(data.DOCTRINES).length} BATTLE PLANS`;
    $('menuMapGrid').innerHTML=maps.map(m=>`<button class="menu-map-card" data-menu-map="${esc(m.id)}" aria-pressed="false" aria-label="${esc(mapSizeLabel(m))} — ${esc(m.name)}"><span class="map-size-badge" data-map-size="${mapSizeName(m).toLowerCase()}">${esc(mapSizeLabel(m))}</span><strong>${esc(m.name)}</strong><span class="map-card-kind">${esc(mapKind(m))}</span><small>${fmt(m.width)} × ${fmt(m.height)}<br>${esc(m.subtitle||'Room to maneuver')}</small></button>`).join('');
    const largeFilter=document.querySelector('[data-map-filter="large"]');
    if(largeFilter)largeFilter.textContent=`Large maps · ${maps.filter(m=>m.width*m.height>6400*4400).length}`;
    updateMenuMap(menuMapId||config.map);
  }
  function filterMenuMaps(filter){
    menuFilter=filter;
    all('[data-map-filter]').forEach(b=>{const active=b.dataset.mapFilter===filter;b.classList.toggle('active',active);b.setAttribute('aria-pressed',String(active));});
    const cards=all('[data-menu-map]');
    const additions=new Set((data.EXPANSION_MAPS||[]).map(m=>typeof m==='string'?m:m.id));
    cards.forEach(b=>{const map=data.MAPS.find(m=>m.id===b.dataset.menuMap);b.hidden=filter==='new'?!additions.has(map.id):filter==='large'?map.width*map.height<=6400*4400:filter==='siege'?!map.regions.some(r=>r.type==='wall'||r.type==='gate'):false;});
    const selected=cards.find(b=>b.dataset.menuMap===menuMapId);
    if(!selected||selected.hidden){const first=cards.find(b=>!b.hidden);if(first)updateMenuMap(first.dataset.menuMap);}
    $('menuMapGrid').scrollTop=0;
  }
  function openMainMenu(){
    if(!menuOpen)menuResumeRunning=engine.phase==='running';
    if(engine.phase==='running')engine.pause();
    menuOpen=true;keys.clear();cancelBattlefieldGesture();accumulator=0;lastFrame=0;commandMode=null;mouseScreen=null;
    $('selectionBox').style.display='none';$('mainMenu').hidden=false;
    $('app').inert=true;$('app').setAttribute('aria-hidden','true');document.body.classList.add('menu-open');
    $('menuResume').hidden=!enteredField;
    $('menuCurrent').hidden=!enteredField;
    $('menuCurrent').textContent=`${config.name} · ${mapSizeLabel(engine.map)} · ${clock(engine.time)} · ${engine.phase==='deployment'?'Deployment ready':engine.phase==='ended'?'Battle concluded':'Battle paused'}`;
    updatePhase();(enteredField?$('menuResume'):$('menuQuick')).focus({preventScroll:true});
  }
  function closeMainMenu(resume){
    menuOpen=false;enteredField=true;keys.clear();accumulator=0;lastFrame=0;lastRendered=0;lastMiniSignature='';
    $('mainMenu').hidden=true;$('app').inert=false;$('app').setAttribute('aria-hidden','false');document.body.classList.remove('menu-open');
    if(resume&&menuResumeRunning&&engine.phase==='paused')engine.resume();
    menuResumeRunning=false;updatePhase();resize();canvas.focus({preventScroll:true});
  }
  function enterMenuBattle(quick){
    const selected=menuCollection==='history'?data.PRESETS.find(p=>p.id===historicalPresetId):data.PRESETS.find(p=>p.id===$('menuScenarioSelect').value)||data.PRESETS.find(p=>p.map===menuMapId);
    closeMainMenu(false);
    if(selected)loadPreset(selected.id);
    else{const raw=clone(data.PRESETS[0]);raw.map=menuMapId;raw.name=data.MAPS.find(m=>m.id===menuMapId).name;delete raw.formations;newBattle(raw);setBuilder(true);}
    if(quick)startBattle();
    else toast('Deployment ready. Arrange the armies, choose battle plans, then press Space.',4200);
  }
  function bindMenu(){
    buildMenuMaps();buildHistoricalBrowser();setMenuCollection('fields');
    $('menuHistory').addEventListener('click',()=>{setMenuCollection('history');$('menuHistoryTab').focus({preventScroll:true});});
    $('menuHistoryTab').addEventListener('click',()=>setMenuCollection('history'));
    $('menuFieldsTab').addEventListener('click',()=>setMenuCollection('fields'));
    $('historySearch').addEventListener('input',filterHistoricalBattles);
    $('historyEra').addEventListener('change',filterHistoricalBattles);
    $('historyGrid').addEventListener('click',e=>{const b=e.target.closest('[data-history-battle]');if(b)showHistoricalBattle(b.dataset.historyBattle);});
    $('menuButton').addEventListener('click',openMainMenu);
    $('menuResume').addEventListener('click',()=>closeMainMenu(true));
    $('menuQuick').addEventListener('click',()=>enterMenuBattle(true));
    $('menuDeploy').addEventListener('click',()=>enterMenuBattle(false));
    $('menuScenarioSelect').addEventListener('change',()=>{
      const scene=data.PRESETS.find(p=>p.id===$('menuScenarioSelect').value);
      if(!scene)return;
      $('menuMapDescription').textContent=scene.description;
      $('menuMapPreview').innerHTML=mapPreview(data.MAPS.find(m=>m.id===scene.map),scene);
    });
    $('menuCustom').addEventListener('click',()=>{closeMainMenu(false);newEmptyEncounter();});
    $('menuSettings').addEventListener('click',()=>openDialog('settingsDialog'));
    $('menuManual').addEventListener('click',()=>openDialog('helpDialog'));
    $('menuSaved').addEventListener('click',()=>openDialog('scenariosDialog'));
    $('menuMapGrid').addEventListener('click',e=>{const b=e.target.closest('[data-menu-map]');if(b)updateMenuMap(b.dataset.menuMap);});
    all('[data-map-filter]').forEach(b=>b.addEventListener('click',()=>filterMenuMaps(b.dataset.mapFilter)));
    openMainMenu();
  }

  function setBuilder(open) {
    if (open && engine.phase === 'running') { engine.pause(); accumulator = 0; }
    $('app').classList.toggle('builder-open', open);
    $('builder').inert = !open;
    $('builder').setAttribute('aria-hidden',String(!open));
    if(!open && $('builder').contains(document.activeElement))canvas.focus({preventScroll:true});
    updatePhase();
  }
  function isDeployment() { return engine && engine.phase === 'deployment'; }
  function totalRoster(army) { return data.TYPE_ORDER.reduce((sum, id) => sum + (Number(army.roster[id]) || 0), 0); }
  function armyValue(army) { return data.TYPE_ORDER.reduce((sum, id) => sum + (Number(army.roster[id]) || 0) * data.TYPES[id].cost, 0); }
  const armorOrder = ['light','medium','heavy'];
  function armorLabel(type) { return data.ARMOR_CLASSES[type.armorClass].label; }
  function armorBadge(type) { return `<span class="armor-badge armor-${type.armorClass}" title="${esc(data.ARMOR_CLASSES[type.armorClass].description)}">${esc(armorLabel(type))} armor</span>`; }
  function counterNames(entries, limit = 2) { return entries.slice(0,limit).map(item=>data.TYPES[item.type].shortName).join(', '); }
  function armorProfileText(type) { return armorOrder.map(id=>`${data.ARMOR_CLASSES[id].label} ×${data.armorMultiplier(type.id,id).toFixed(2)}`).join(' · '); }
  function counterDescription(type) {
    const profile=data.ATTACK_PROFILES[type.attackProfile];
    return `${armorLabel(type)} armor · ${profile.label}\n${armorProfileText(type)}\n${type.matchups.roleNote}\nGood targets: ${type.matchups.strongAgainst.map(item=>data.TYPES[item.type].name+': '+item.reason).join(' · ')}\nThreats: ${type.matchups.weakAgainst.map(item=>data.TYPES[item.type].name+': '+item.reason).join(' · ')}`;
  }
  function cleanConfig(raw) {
    if (!raw || typeof raw !== 'object' || !Array.isArray(raw.armies) || raw.armies.length !== 2) throw new Error('A scenario needs two armies.');
    let map = data.MAPS.find(m => m.id === raw.map) || data.MAPS[0];
    if(map.id==='custom'){
      const source=raw.customMap&&typeof raw.customMap==='object'?raw.customMap:{};
      map=Object.assign({},map,data.normalizeWorldSize(source.width,source.height));
    }
    const result = {name:typeof raw.name === 'string' ? raw.name.slice(0,64) : 'Custom battle',map:map.id,seed:clamp(Math.floor(Number(raw.seed) || 142857),1,2147483647),armies:[]};
    raw.armies.forEach((army, team) => {
      if (!army || typeof army !== 'object') throw new Error('Invalid army data.');
      let remaining = armyLimit();
      const roster = {};
      data.TYPE_ORDER.forEach(id => {
        let n = Number(army.roster && army.roster[id]);
        n = Number.isFinite(n) ? clamp(Math.floor(n),0,Math.min(remaining,data.TYPES[id].maxCount||remaining)) : 0;
        roster[id] = n; remaining -= n;
      });
      result.armies.push({name:typeof army.name === 'string' ? army.name.slice(0,40) : team === 0 ? 'Azure Crown' : 'Ember Dominion',doctrine:Object.hasOwn(data.DOCTRINES,army.doctrine) ? army.doctrine : 'balanced',roster});
    });
    if(map.id==='custom'){
      const source=raw.customMap&&typeof raw.customMap==='object'?raw.customMap:{};
      const biomes=['grassland','wetland','desert','tundra','woodland','highland','ashland'];
      result.customMap={name:typeof source.name==='string'?source.name.slice(0,64):'Custom Battlefield',biome:biomes.includes(source.biome)?source.biome:'grassland',width:map.width,height:map.height,regions:[]};
      if(Array.isArray(source.regions))for(const r of source.regions.slice(0,terrainLimit())){
        if(!r||!['forest','hill','rock','water','bridge','wall','gate','road','mud','marsh','snow','trench'].includes(r.type)||![r.x,r.y,r.w,r.h].every(Number.isFinite)||r.w<=0||r.h<=0)continue;
        const x=clamp(r.x,0,map.width-20),y=clamp(r.y,0,map.height-20);
        const region={type:r.type,x,y,w:clamp(r.w,20,map.width-x),h:clamp(r.h,20,map.height-y)};
        if(r.type==='wall'||r.type==='gate'){region.team=r.team===0?0:1;region.hp=clamp(Number(r.hp)||(r.type==='gate'?1000:2200),200,10000);}
        if((r.type==='wall'||r.type==='gate')&&STRUCTURE_KINDS.includes(r.structureKind))region.structureKind=r.structureKind;
        if(typeof r.name==='string')region.name=r.name.slice(0,64);
        if((r.type==='wall'||r.type==='gate')&&r.walkable===true){
          region.walkable=true;region.deckHeight=clamp(Number(r.deckHeight)||36,8,80);
          if(['north','south','east','west'].includes(r.rampartSide))region.rampartSide=r.rampartSide;
          if(Number.isFinite(r.courtyardX)&&Number.isFinite(r.courtyardY)){region.courtyardX=clamp(r.courtyardX,20,map.width-20);region.courtyardY=clamp(r.courtyardY,20,map.height-20);}
          if(Array.isArray(r.stairs))region.stairs=r.stairs.slice(0,8).filter(s=>s&&['x','y'].includes(s.axis)&&[s.x,s.y,s.w,s.h,s.groundX,s.groundY,s.deckX,s.deckY].every(Number.isFinite)&&s.w>0&&s.h>0&&s.x>=0&&s.y>=0&&s.x+s.w<=map.width&&s.y+s.h<=map.height&&[s.groundX,s.deckX].every(n=>n>=0&&n<=map.width)&&[s.groundY,s.deckY].every(n=>n>=0&&n<=map.height)).map(s=>({id:String(s.id||'stair').slice(0,64),x:s.x,y:s.y,w:s.w,h:s.h,axis:s.axis,dir:s.dir===-1?-1:1,width:clamp(Number(s.width)||64,24,160),height:clamp(Number(s.height)||region.deckHeight,8,80),groundX:s.groundX,groundY:s.groundY,deckX:s.deckX,deckY:s.deckY}));
        }
        if(r.type==='wall'&&r.castleTower===true)region.castleTower=true;
        if(r.type==='gate'&&r.castleGate===true)region.castleGate=true;
        result.customMap.regions.push(region);
      }
    }
    if(Array.isArray(raw.formations)){
      const remaining=result.armies.map(army=>Object.assign({},army.roster));
      let offsetBudget=data.MAX_UNITS;
      result.formations=[];
      for(const f of raw.formations.slice(0,512)){
        if(!f||![0,1].includes(f.team)||!data.TYPES[f.type]||![f.count,f.x,f.y].every(Number.isFinite)||f.count<1)continue;
        const count=Math.min(Math.floor(f.count),remaining[f.team][f.type],data.TYPES[f.type].commander?1:armyLimit());
        if(!count)continue;
        const group={team:f.team,type:f.type,count,x:clamp(f.x,35,map.width-35),y:clamp(f.y,35,map.height-35),formation:Object.hasOwn(data.FORMATIONS,f.formation)?f.formation:data.TYPES[f.type].id==='musketeer'?'volley':'line'};
        if(typeof f.name==='string')group.name=f.name.slice(0,64);
        if(f.rampart&&typeof f.rampart==='object'){
          group.rampart={ranks:clamp(Math.floor(Number(f.rampart.ranks)||2),1,3),spacing:['compact','normal','wide'].includes(f.rampart.spacing)?f.rampart.spacing:'normal'};
          const id=structureKey(f.rampart.structureId);if(id!==undefined)group.rampart.structureId=id;
        }
        if(Number.isFinite(f.facing))group.facing=Math.atan2(Math.sin(f.facing),Math.cos(f.facing));
        // A split keeps the soldiers' actual footprint. Rebuilding a standard
        // rectangle at each half's center can otherwise put both halves atop
        // one another. Preserve bounded per-soldier offsets in scenario saves.
        if(Array.isArray(f.offsets)&&f.offsets.length>=count&&count<=offsetBudget){
          const offsets=f.offsets.slice(0,count);
          if(offsets.every(p=>p&&Number.isFinite(p.x)&&Number.isFinite(p.y)&&Math.abs(p.x)<=map.width&&Math.abs(p.y)<=map.height)){
            group.offsets=offsets.map(p=>({x:p.x,y:p.y}));offsetBudget-=count;
          }
        }
        remaining[f.team][f.type]-=count;
        result.formations.push(group);
      }
    }
    return result;
  }
  function applyDeployment(positions) {
    if (!Array.isArray(positions)) return;
    const seen = new Set();
    const facings = [];
    positions.slice(0,512).forEach(p => {
      if (!p || !Number.isInteger(p.id) || seen.has(p.id) || !Number.isFinite(p.x) || !Number.isFinite(p.y)) return;
      if (!engine.getRegiment(p.id)) return;
      if(p.formation && engine.setFormation)engine.setFormation([p.id],p.formation);
      seen.add(p.id); engine.relocateRegiments([p.id], clamp(p.x,30,engine.width-30), clamp(p.y,30,engine.height-30));
      if(Number.isFinite(p.facing))facings.push(p);
    });
    // Move every legacy group before validating its turn, so a temporary
    // default deployment cannot obstruct another group's saved orientation.
    if(engine.faceRegiments)for(const p of facings){
      const r=engine.getRegiment(p.id),members=r.unitIds.map(id=>engine.units[id]).filter(u=>u.alive&&!u.routed);
      if(!members.length)continue;
      const center=members.reduce((c,u)=>({x:c.x+u.x/members.length,y:c.y+u.y/members.length}),{x:0,y:0});
      engine.faceRegiments([p.id],center.x+Math.cos(p.facing)*1000,center.y+Math.sin(p.facing)*1000);
    }
  }
  function captureDeployment() { return engine.regiments.map(r => ({id:r.id,x:r.x,y:r.y,formation:r.formation||'line',facing:r.facing})); }
  function rememberDeployment(){
    if(!isDeployment())return;
    deployment=captureDeployment();
    if(engine.captureFormations)config.formations=engine.captureFormations();
  }
  function newBattle(raw, options = {}) {
    clearTimeout(rebuildTimer); rebuildTimer = 0;
    cancelBattlefieldGesture();
    const previousCamera = renderer ? Object.assign({}, renderer.camera) : null;
    config = cleanConfig(raw);
    engine = new BattleEngine(config);
    if(!config.formations||!config.formations.length)applyDeployment(options.deployment || []);
    rememberDeployment();
    if(!options.preserveEditor)editorTool=null;
    if (!renderer) renderer = new BattleRenderer(canvas,engine,preferences);
    else renderer.setEngine(engine);
    selection.clear(); commandMode = null; endedShown = false; commanderCursor=-1;
    accumulator = 0; visibleEventSignature = ''; audio.lastCount = 0;lastMiniSignature='';lastMinimap=0;lastRendered=0;
    ratioTimer = 0; observedTime = 0; simulatedTime = 0; simulationRatio = 1;
    resize();
    if (options.preserveCamera && previousCamera) renderer.camera = previousCamera;
    else renderer.fit();
    $('mapSelect').value = config.map;
    const fieldLabel=`${mapSizeLabel(engine.map)} — ${engine.map.name}`;
    $('mapEyebrow').textContent = fieldLabel.toUpperCase();
    $('mapEyebrow').title = fieldLabel;
    $('mapSizeDetails').textContent = `${mapSizeLabel(engine.map)} · ${fmt(engine.width)} × ${fmt(engine.height)}`;
    const customMapOption=$('mapSelect').querySelector('option[value="custom"]');
    if(customMapOption)customMapOption.textContent=config.map==='custom'?fieldLabel:'Custom Map · Choose battlefield size';
    $('battleName').textContent = config.name;
    $('blueName').textContent = config.armies[0].name.toUpperCase();
    $('redName').textContent = config.armies[1].name.toUpperCase();
    $('editBlue').textContent = config.armies[0].name;
    $('editRed').textContent = config.armies[1].name;
    for(let team=0;team<2;team++){
      const option=$('controlSelect').querySelector(`option[value="${team}"]`);
      if(option)option.textContent=`Command ${config.armies[team].name}`;
    }
    $('seedInput').value = config.seed;
    $('scenarioName').value = config.name;
    $('fieldFeed').replaceChildren();
    populateRoster(); updatePhase(); updateUI(true); updateEncounterEditor();
  }
  function scheduleRebuild() {
    if (!isDeployment()) return;
    rememberDeployment();
    customScaleBase = null;
    config.name = 'Custom battle';
    $('battleName').textContent = config.name;
    $('presetSelect').value = 'custom'; $('scaleSelect').value = '1';
    $('presetDescription').textContent = config.map==='custom'?'Build your encounter: place terrain and formations on the empty field.':'Your armies, your plan. Adjust both sides and create a battle.';
    updateTally();
    clearTimeout(rebuildTimer);
    rebuildTimer = window.setTimeout(() => {rebuildTimer=0;newBattle(config,{preserveCamera:true});},160);
  }
  function loadPreset(id, factor = 1) {
    if(id==='custom'){newEmptyEncounter();return;}
    const preset = data.PRESETS.find(p => p.id === id) || data.PRESETS[0];
    chosenPreset = preset.id;
    customScaleBase = null; terrainHistory=[];
    const raw = clone(preset);
    raw.armies.forEach(a => {
      const total = totalRoster(a), scale = Math.min(factor,armyLimit() / Math.max(1,total));
      data.TYPE_ORDER.forEach(t => { a.roster[t] = Array.isArray(raw.formations)&&data.TYPES[t].commander?a.roster[t]:Math.round(a.roster[t] * scale); });
    });
    // Scale every authored formation proportionally, preserving its place in
    // the historical deployment instead of auto-deploying extra soldiers.
    if(factor!==1&&Array.isArray(raw.formations))for(let team=0;team<2;team++)for(const type of data.TYPE_ORDER){
      const groups=raw.formations.filter(f=>f.team===team&&f.type===type),original=groups.reduce((n,f)=>n+f.count,0);
      let assigned=0,cumulative=0;for(const group of groups){
        cumulative+=group.count;const end=Math.round(cumulative/Math.max(1,original)*raw.armies[team].roster[type]);
        const count=end-assigned;assigned=end;
        if(count!==group.count&&group.offsets?.length){
          // Keep a deep phalanx deep and a broad firing line broad when the
          // army scale changes. Coordinates remain relative to its facing.
          const a=group.facing||0,nx=Math.cos(a),ny=Math.sin(a);
          const axes=group.offsets.map(p=>({forward:p.x*nx+p.y*ny,cross:-p.x*ny+p.y*nx}));
          const unique=key=>Array.from(new Set(axes.map(p=>Math.round(p[key]*100)/100))).sort((x,y)=>x-y);
          const files=unique('cross'),ranks=unique('forward'),spacing=list=>list.length>1?Math.min(...list.slice(1).map((n,i)=>n-list[i])):17;
          const cols=Math.max(1,Math.round(Math.sqrt(count*files.length/Math.max(1,ranks.length)))),rows=Math.ceil(count/cols),sx=spacing(ranks),sy=spacing(files);
          group.offsets=Array.from({length:count},(_,i)=>{const x=((rows-1)/2-Math.floor(i/cols))*sx,y=(i%cols-(cols-1)/2)*sy;return{x:x*nx-y*ny,y:x*ny+y*nx};});
        }
        group.count=count;
      }
    }
    if (factor !== 1) raw.name += ` · ${factor}×`;
    newBattle(raw);
    $('presetSelect').value = preset.id; $('scaleSelect').value = String(factor);
    $('presetDescription').textContent = preset.description;
    setBuilder(true);
  }
  function newEmptyEncounter(){
    const armies=[0,1].map(team=>({name:team===0?'Azure Crown':'Ember Dominion',doctrine:'balanced',roster:Object.fromEntries(data.TYPE_ORDER.map(id=>[id,0]))}));
    const raw={name:'Custom encounter',map:'custom',seed:config?config.seed:142857,customMap:{name:'Custom Battlefield',biome:'grassland',width:6400,height:4400,regions:[]},armies,formations:[]};
    customScaleBase=null;chosenPreset=null;terrainHistory=[];activeTeam=0;
    newBattle(raw);control='both';$('controlSelect').value='both';
    $('presetSelect').value='custom';$('scaleSelect').value='1';
    $('presetDescription').textContent='An empty battlefield. Paint terrain, place formations for each army, then begin.';
    setBuilder(true);updatePhase();
    toast('Empty encounter ready. Both armies and all terrain have been cleared.',3600);
  }
  function updateEncounterEditor(){
    const custom=config.map==='custom',locked=!isDeployment();
    $('encounterEditor').hidden=!custom;
    $('formationPlacement').hidden=!custom;
    if(!custom)editorTool=null;
    $('terrainBiome').value=custom&&config.customMap?config.customMap.biome:'grassland';
    const worldSize=data.WORLD_SIZES.find(size=>size.width===engine.width&&size.height===engine.height);
    $('customWorldSize').value=worldSize?worldSize.id:'standard';
    $('customWorldSize').disabled=locked;
    $('customSizeHelp').textContent=`${mapSizeLabel(engine.map)} · ${fmt(engine.width)} × ${fmt(engine.height)}. Enlarging keeps your troops and terrain in place.`;
    $('placeArmyLabel').textContent=config.armies[activeTeam].name;
    $('terrainCount').textContent=`${custom&&config.customMap?config.customMap.regions.length:0} / ${terrainLimit()} terrain areas`;
    all('[data-terrain-tool]').forEach(b=>{b.disabled=locked;b.classList.toggle('active',editorTool===b.dataset.terrainTool);b.setAttribute('aria-pressed',String(editorTool===b.dataset.terrainTool));});
    ['terrainBiome','terrainDefender','fortressStyle','wallStairSide','placeTroop','placeCount','placeFormationButton','finishTerrain','clearTerrain'].forEach(id=>{$(id).disabled=locked;});
    $('wallAccessOptions').hidden=editorTool!=='wall'&&editorTool!=='gate';
    const fortress=(data.FORTRESS_TYPES||{})[$('fortressStyle').value];
    $('fortressDescription').textContent=fortress?fortress.description:'Choose a castle plan, then use the Castle brush to place its walls and gates.';
    const defender=$('terrainDefender');
    for(let i=0;i<2;i++)if(defender.options&&defender.options[i])defender.options[i].textContent=config.armies[i].name;
    $('placeCount').max=data.TYPES[$('placeTroop').value||'swordsman'].groupSize||400;
    $('undoTerrain').disabled=locked||!terrainHistory.length;
    const label={forest:'Forest',hill:'Hill',rock:'Rocks',water:'Water',bridge:'Bridge',road:'Road',mud:'Mud',marsh:'Marsh',snow:'Snow',trench:'Trench',wall:'Defensive wall',gate:'Defensive gate',house:'House',watchtower:'Watchtower',barricade:'Barricade',keep:'Keep',erase:'Erase terrain'}[editorTool];
    const fortHelp=editorTool==='wall'||editorTool==='gate'?` Defended by ${config.armies[Number(defender.value)===0?0:1].name}.${editorTool==='gate'?' Place across a wall to cut a real gate opening.':''}`:'';
    const text=editorTool==='castle'?`${fortress?fortress.name:'Castle'}: drag its footprint or click for the standard plan. Gates and crossings are included. Set the defender below.`:editorTool==='troops'?`Click open ground to place ${$('placeCount').value||160} ${data.TYPES[$('placeTroop').value||'swordsman'].shortName} for ${config.armies[activeTeam].name}.`:label?`${label}: drag an area on the field, or click for a standard size.${fortHelp} Escape finishes editing.`:'Choose a terrain or building brush and drag on the field. Castle plans include their gates and crossings.';
    $('editorHelp').textContent=locked?'Reset the battle to edit this encounter.':text;
    if(editorTool){$('commandHint').textContent=text;$('commandHint').hidden=false;}
    canvas.style.cursor=editorTool||commandMode?'crosshair':'';
  }
  function chooseEditorTool(tool){
    if(!isDeployment()||config.map!=='custom')return;
    if(rebuildTimer){clearTimeout(rebuildTimer);newBattle(config,{preserveCamera:true});}
    editorTool=tool;commandMode=null;selection.clear();updateSelection();updatePhase();canvas.focus({preventScroll:true});
  }
  function resizeCustomWorld(id){
    if(!isDeployment()||config.map!=='custom')return;
    const size=data.WORLD_SIZES.find(s=>s.id===id);if(!size)return;
    const outsideTerrain=config.customMap.regions.some(r=>r.x+r.w>size.width||r.y+r.h>size.height);
    const outsideTroops=engine.units.some(u=>u.alive&&(u.x+data.TYPES[u.type].radius+12>size.width||u.y+data.TYPES[u.type].radius+12>size.height));
    if(outsideTerrain||outsideTroops){updateEncounterEditor();toast('Move troops and terrain inside the smaller field before reducing its size. Your current layout has been kept.',5000);return;}
    rememberDeployment();config.customMap.width=size.width;config.customMap.height=size.height;terrainHistory=[];
    newBattle(config,{preserveEditor:true});
    toast(`${mapSizeLabel(size)} battlefield ready: ${fmt(size.width)} × ${fmt(size.height)}. Existing formations and terrain kept in place.`,3500);
  }
  function changeBattlefield(id){
    if(!isDeployment())return;
    if(id==='custom'){newEmptyEncounter();return;}
    const map=data.MAPS.find(m=>m.id===id);if(!map)return;
    if(rebuildTimer){clearTimeout(rebuildTimer);newBattle(config,{preserveCamera:true});}
    rememberDeployment();
    const scaleX=map.width/engine.width,scaleY=map.height/engine.height;
    // Preserve the relative placement of formations on a different theater,
    // but keep body spacing and facing at their actual world scale.
    for(const group of config.formations||[]){group.x*=scaleX;group.y*=scaleY;}
    config.map=map.id;config.name='Custom battle';customScaleBase=null;terrainHistory=[];editorTool=null;
    newBattle(config);$('presetSelect').value='custom';$('scaleSelect').value='1';
    $('presetDescription').textContent='Your armies on a new battlefield. Check the terrain, refine their positions, and give each side a plan.';
    toast(`${map.name} ready. Formation centers follow the new field's dimensions; troop spacing stays the same.`,4200);
  }
  function bodyBlockedByTerrain(x,y,radius,regions){
    return regions.some(r=>{
      if(!['rock','water','wall','gate'].includes(r.type)||x<r.x-radius-1||x>r.x+r.w+radius+1||y<r.y-radius-1||y>r.y+r.h+radius+1)return false;
      if(r.type!=='water')return true;
      return !regions.some(b=>b.type==='bridge'&&x>=b.x+radius&&x<=b.x+b.w-radius&&y>=b.y+radius&&y<=b.y+b.h-radius);
    });
  }
  function applyTerrain(regions,remember=true){
    if(!isDeployment()||config.map!=='custom')return false;
    if(engine.units.some(u=>u.alive&&bodyBlockedByTerrain(u.x,u.y,data.TYPES[u.type].radius,regions)&&!bodyBlockedByTerrain(u.x,u.y,data.TYPES[u.type].radius,engine.map.regions||[]))){toast('Move the formations clear before changing the ground beneath them.');return false;}
    rememberDeployment();
    if(remember){terrainHistory.push(clone(config.customMap.regions));if(terrainHistory.length>20)terrainHistory.shift();}
    config.customMap.regions=regions;
    newBattle(config,{preserveCamera:true,preserveEditor:true});
    return true;
  }
  function editTerrain(start,end,tool){
    if(!isDeployment()||config.map!=='custom')return;
    const fortress=(data.FORTRESS_TYPES||{})[$('fortressStyle').value];
    const defaults={forest:[360,300],hill:[440,360],rock:[180,180],water:[240,520],bridge:[420,200],road:[900,180],mud:[420,320],marsh:[480,360],snow:[500,360],trench:[100,640],wall:[80,480],gate:[80,240],castle:[fortress?.defaultWidth||1600,fortress?.defaultHeight||1600],house:[180,140],watchtower:[120,120],barricade:[70,400],keep:[760,760],erase:[180,180]};
    const dragged=Math.hypot(end.x-start.x,end.y-start.y)>45;
    const sizes=defaults[tool]||defaults.forest;
    let w=dragged?Math.max(60,Math.abs(end.x-start.x)):sizes[0],h=dragged?Math.max(60,Math.abs(end.y-start.y)):sizes[1];
    let x=dragged?Math.min(start.x,end.x):end.x-w/2,y=dragged?Math.min(start.y,end.y):end.y-h/2;
    x=clamp(x,12,engine.width-72);y=clamp(y,12,engine.height-72);w=Math.min(w,engine.width-12-x);h=Math.min(h,engine.height-12-y);
    let rect={type:tool,x:Math.round(x),y:Math.round(y),w:Math.round(w),h:Math.round(h)};
    const overlap=(a,b)=>a.x<b.x+b.w&&a.x+a.w>b.x&&a.y<b.y+b.h&&a.y+a.h>b.y;
    let regions=clone(config.customMap.regions);
    if(tool==='erase'){
      const kept=regions.filter(r=>!overlap(rect,r));
      if(kept.length===regions.length){toast('No terrain areas intersect that selection.');return;}
      if(applyTerrain(kept))toast(`${regions.length-kept.length} terrain area${regions.length-kept.length===1?'':'s'} removed.`);return;
    }
    if(regions.length>=terrainLimit()){toast(`This encounter has ${terrainLimit()} terrain areas. Erase an area before adding another.`);return;}
    if(tool==='castle'){
      const minSize=fortress?.minSize||800;
      if(rect.w<minSize||rect.h<minSize){toast(`This castle needs at least ${minSize} units on each side. Click to use its standard plan.`);return;}
      const parts=data.createFortress?data.createFortress($('fortressStyle').value,rect.x,rect.y,rect.w,rect.h,{team:Number($('terrainDefender').value)===0?0:1}):data.castleWalls(rect.x,rect.y,rect.w,rect.h,{team:Number($('terrainDefender').value)===0?0:1});
      if(!parts.length){toast('Give this castle more room or choose its standard plan.');return;}
      if(parts.some(p=>p.x<12||p.y<12||p.x+p.w>engine.width-12||p.y+p.h>engine.height-12)){toast('Leave more room around this castle for its walls, moat, and crossings.');return;}
      if(regions.length+parts.length>terrainLimit()){toast(`This castle needs ${parts.length} terrain areas. Erase some terrain first.`);return;}
      if(parts.some(part=>regions.some(r=>['wall','gate','rock'].includes(r.type)&&overlap(part,r)))){
        toast('Place the perimeter clear of existing walls, gates and rocks.');return;
      }
      const entranceGround=regions.concat(parts.filter(r=>r.type==='bridge'));
      if(parts.filter(r=>r.type==='gate').some(gate=>bodyBlockedByTerrain(gate.x+gate.w/2,gate.y+gate.h/2,12,entranceGround))){
        toast('Every entrance needs passable ground. Place a bridge first or move the castle.');return;
      }
      if(applyTerrain(regions.concat(parts)))toast('Castle built. Place the garrison inside the courtyard.');
      return;
    }
    if(['house','watchtower','barricade','keep'].includes(tool)){
      const parts=data.createBattlefieldStructure(tool,rect.x,rect.y,rect.w,rect.h,{team:Number($('terrainDefender').value)===0?0:1});
      if(!parts.length||parts.some(p=>p.x<12||p.y<12||p.x+p.w>engine.width-12||p.y+p.h>engine.height-12)){toast('Give this building more clear ground inside the battlefield.');return;}
      if(regions.length+parts.length>terrainLimit()){toast('Erase an area before adding this building.');return;}
      if(parts.some(part=>regions.some(r=>['wall','gate','rock','water'].includes(r.type)&&overlap(part,r)))){toast('Place buildings on clear ground, with room around their walls.');return;}
      if(applyTerrain(regions.concat(parts)))toast(`${{house:'House',watchtower:'Watchtower',barricade:'Barricade',keep:'Keep'}[tool]} built. Troops find their way around its footprint.`,2600);
      return;
    }
    if(tool==='wall'||tool==='gate'){
      rect.team=Number($('terrainDefender').value)===0?0:1;rect.hp=tool==='gate'?1000:2200;
      if(data.createRampartWall){
        const positive=$('wallStairSide').value!=='negative',side=rect.h>=rect.w?(positive?'east':'west'):(positive?'south':'north');
        rect=data.createRampartWall(rect.x,rect.y,rect.w,rect.h,{team:rect.team,type:tool,hp:rect.hp,courtyardSide:side,worldWidth:engine.width,worldHeight:engine.height});
        if(!rect||!rect.walkable||(rect.stairs||[]).some(s=>s.x<0||s.y<0||s.x+s.w>engine.width||s.y+s.h>engine.height||bodyBlockedByTerrain(s.groundX,s.groundY,12,regions))){toast('Leave clear ground on the chosen stair side, or choose the opposite side.');return;}
      }
      if(tool==='gate'){
        // A gate painted across a wall replaces the intersecting masonry.
        // Keeping a hidden wall beneath a gate would make its breach useless.
        const wall=regions.find(r=>r.type==='wall'&&overlap(rect,r));
        if(wall){
          if(wall.h>=wall.w){const right=Math.max(rect.x+rect.w,wall.x+wall.w);rect.x=Math.min(rect.x,wall.x);rect.w=right-rect.x;}
          else{const bottom=Math.max(rect.y+rect.h,wall.y+wall.h);rect.y=Math.min(rect.y,wall.y);rect.h=bottom-rect.y;}
        }
        const carved=[];
        for(const region of regions){
          if(region.type!=='wall'||!overlap(rect,region)){carved.push(region);continue;}
          const x1=Math.max(rect.x,region.x),x2=Math.min(rect.x+rect.w,region.x+region.w),y1=Math.max(rect.y,region.y),y2=Math.min(rect.y+rect.h,region.y+region.h);
          for(const part of [{x:region.x,y:region.y,w:region.w,h:y1-region.y},{x:region.x,y:y2,w:region.w,h:region.y+region.h-y2},{x:region.x,y:y1,w:x1-region.x,h:y2-y1},{x:x2,y:y1,w:region.x+region.w-x2,h:y2-y1}]){
            if(part.w>=20&&part.h>=20){const section=Object.assign({},region,part);if(section.stairs)section.stairs=section.stairs.filter(s=>s.deckX>=section.x&&s.deckX<=section.x+section.w&&s.deckY>=section.y&&s.deckY<=section.y+section.h);carved.push(section);}
          }
        }
        regions=carved;
        if(regions.length>=terrainLimit()){toast('This gate would exceed the terrain limit. Erase a nearby area first.');return;}
      }
    }
    if(tool==='bridge'){
      const water=regions.find(r=>r.type==='water'&&overlap(rect,r));
      if(!water){toast('Place the bridge across a water area.');return;}
      if(water.h>=water.w){const right=Math.max(rect.x+rect.w,water.x+water.w+45);rect.x=Math.max(12,Math.min(rect.x,water.x-45));rect.w=Math.min(engine.width-12,right)-rect.x;rect.h=Math.max(140,rect.h);}
      else{const bottom=Math.max(rect.y+rect.h,water.y+water.h+45);rect.y=Math.max(12,Math.min(rect.y,water.y-45));rect.h=Math.min(engine.height-12,bottom)-rect.y;rect.w=Math.max(140,rect.w);}
    }
    if(['rock','water','wall','gate'].includes(tool)&&engine.units.some(u=>u.alive&&overlap(rect,{x:u.x-data.TYPES[u.type].radius,y:u.y-data.TYPES[u.type].radius,w:data.TYPES[u.type].radius*2,h:data.TYPES[u.type].radius*2}))){
      toast('Move the formations out of this area before placing impassable terrain.');return;
    }
    regions.push(rect);if(applyTerrain(regions))toast(`${{forest:'Forest',hill:'High ground',rock:'Rocks',water:'Water',bridge:'Bridge',road:'Road',mud:'Mud',marsh:'Marsh',snow:'Snow',trench:'Trench',wall:'Wall',gate:'Gate'}[tool]} placed.`,1800);
  }
  function placeFormationAt(x,y){
    if(!isDeployment()||config.map!=='custom')return;
    const type=$('placeTroop').value,t=data.TYPES[type];if(!t)return;
    const army=config.armies[activeTeam],remaining=Math.min(armyLimit()-totalRoster(army),(t.maxCount||armyLimit())-army.roster[type]);
    const requested=Math.floor(Number($('placeCount').value)||1),count=clamp(requested,1,Math.min(400,t.groupSize|| (t.commander?1:400)));
    if(count>remaining){toast('This formation would exceed the army or troop limit. Reduce its size first.');return;}
    const formation=type==='musketeer'?'volley':'line';
    const spacing=(t.siege?48:type==='elephant'?56:t.mounted?28:17)*(data.FORMATIONS[formation].spacing||1),columns=type==='musketeer'?Math.ceil(count/3):Math.ceil(Math.sqrt(count*1.6)),rows=Math.ceil(count/columns);
    const radius=t.radius,regions=engine.map.regions||[];
    for(let slot=0;slot<count;slot++){
      const row=Math.floor(slot/columns),col=slot%columns,side=activeTeam===0?1:-1,px=x+(((rows-1)/2-row)*spacing)*side,py=y+(col-(columns-1)/2)*spacing*side;
      const blocked=bodyBlockedByTerrain(px,py,radius,regions);
      if(px<radius+12||py<radius+12||px>engine.width-radius-12||py>engine.height-radius-12||blocked){toast('The whole formation needs open ground. Choose a wider clearing.');return;}
    }
    const halfX=rows*spacing/2+radius,halfY=columns*spacing/2+radius;
    if(engine.units.some(u=>u.alive&&Math.abs(u.x-x)<halfX+data.TYPES[u.type].radius&&Math.abs(u.y-y)<halfY+data.TYPES[u.type].radius)){toast('Leave space between formations. Choose clear ground beside the existing ranks.');return;}
    if(engine.regiments.length>=512){toast('This encounter already has 512 formations.');return;}
    rememberDeployment();const forms=config.formations||[];
    forms.push({team:activeTeam,type,count,x,y,formation,facing:activeTeam===0?0:Math.PI});config.formations=forms;army.roster[type]+=count;
    editorTool=null;newBattle(config,{preserveCamera:true});$('presetSelect').value='custom';$('scaleSelect').value='1';
    const placed=engine.regiments.filter(r=>r.team===activeTeam&&r.type===type).sort((a,b)=>Math.hypot(a.x-x,a.y-y)-Math.hypot(b.x-x,b.y-y))[0];
    if(placed)selectRegiments([placed.id]);
    toast(`${count} ${t.shortName} placed for ${army.name}. Right-click to reposition; T then click to face a direction.`);
  }
  function splitSelectedFormations(){
    if(control==='spectate'||engine.phase==='ended'||!engine.splitRegiments)return;
    if(rebuildTimer){clearTimeout(rebuildTimer);newBattle(config,{preserveCamera:true});toast('Roster updated. Select the formation to split.');return;}
    const source=selectedRegiments().filter(r=>r.alive>=2&&r.type!=='commander'&&r.status!=='routing');
    if(!source.length){toast('Select a formation with at least two soldiers to split.');return;}
    const ids=engine.splitRegiments(source.map(r=>r.id));
    if(!ids.length){toast(engine.regiments.length>=512?'The field has reached its 512-formation limit.':'These formations cannot split in their current state.');return;}
    editorTool=null;commandMode=null;selection=new Set([...source.map(r=>r.id),...ids]);rememberDeployment();
    updateSelection();updatePhase();canvas.focus({preventScroll:true});
    toast(`${ids.length} formation${ids.length===1?'':'s'} split into separate commands. Click either group to give it its own orders.`,4200);
  }
  function categoryOf(type) {
    if(type.siege)return 'siege';
    if(type.mounted)return 'cavalry';
    return type.ranged?'ranged':'infantry';
  }
  function updatePlaceCount() {
    const t=data.TYPES[$('placeTroop').value];
    $('placeCount').value=t.groupSize||(t.commander?1:t.siege?12:t.id==='elephant'?16:t.mounted?80:t.id==='musketeer'?90:160);
  }
  function chooseTroopCategory(tab) {
    troopCategory=tab.dataset.troopCategory;
    all('[data-troop-category]').forEach(button=>{
      button.setAttribute('aria-selected',String(button===tab));
      button.tabIndex=button===tab?0:-1;
    });
    $('armyList').setAttribute('aria-labelledby',tab.id);
    populateRoster();
  }
  function populateRoster() {
    const army = config.armies[activeTeam];
    $('doctrineSelect').value = army.doctrine;
    updateDoctrineDescription();
    $('editBlue').classList.toggle('active', activeTeam === 0); $('editRed').classList.toggle('active', activeTeam === 1);
    $('editBlue').setAttribute('aria-selected', String(activeTeam === 0)); $('editRed').setAttribute('aria-selected', String(activeTeam === 1));
    const displayTypes=['commander',...data.TYPE_ORDER.filter(id=>id!=='commander')]
      .filter(id=>troopCategory==='all'||categoryOf(data.TYPES[id])===troopCategory);
    const picker=$('placeTroop'),previous=picker.value||'swordsman';
    picker.innerHTML=displayTypes.map(id=>`<option value="${id}">${esc(data.TYPES[id].name)}</option>`).join('');
    picker.value=displayTypes.includes(previous)?previous:displayTypes[0];
    if(picker.value!==previous)updatePlaceCount();
    $('armyList').innerHTML = displayTypes.map(id => {
      const t = data.TYPES[id], description = `${t.description}\n${counterDescription(t)}\nHealth ${t.hp} | Armor points ${t.armor} | Attack ${t.damage} | Range ${t.range}`;
      return `<article class="troop-card ${id==='commander'?'commander-card':''}" title="${esc(description)}"><div class="troop-visual"><span class="troop-role">${esc(t.role)}</span><canvas data-portrait="${id}" width="88" height="88" aria-label="${esc(t.name)}"></canvas></div><h3>${esc(t.shortName)}</h3>${armorBadge(t)}${t.missile?`<p class="troop-behavior" title="${esc(t.missile.description)}">${esc(t.missile.label)}</p>`:''}<p class="troop-counter-summary"><span>Good targets:</span> ${esc(counterNames(t.matchups.strongAgainst))}</p><button class="counter-link" data-counter-type="${id}" aria-label="Compare ${esc(t.name)} matchups">Matchups &amp; counters</button><span class="troop-cost">${fmt(t.cost)} POINTS / ${t.siege?'ENGINE':id==='elephant'?'ELEPHANT':id==='commander'?'LEADER':'SOLDIER'}</span><div class="troop-stepper"><button data-count-type="${id}" data-delta="-1" aria-label="Remove ${esc(t.shortName)}">−</button><input type="number" data-roster="${id}" aria-label="${esc(t.name)} count" min="0" max="${t.maxCount||armyLimit()}" step="1" value="${army.roster[id]}"><button data-count-type="${id}" data-delta="1" aria-label="Add ${esc(t.shortName)}">+</button></div></article>`;
    }).join('');
    $('armyList').querySelectorAll('[data-portrait]').forEach(c => renderer.drawUnitIcon(c,c.dataset.portrait,activeTeam));
    updateTally();
    setBuilderDisabled(!isDeployment());updateEncounterEditor();
  }
  function setBuilderDisabled(locked) {
    ['presetSelect','mapSelect','scaleSelect','mirrorArmy','clearArmy'].forEach(id => { $(id).disabled = locked; });
    $('doctrineSelect').disabled = !canChangePlan(activeTeam);
    all('[data-roster], [data-count-type]').forEach(el => {el.disabled = locked;});
    $('startButton').querySelector('span:nth-child(2)').textContent = locked ? 'RESET TO EDIT ARMIES' : 'UNLEASH THE ARMIES';
    $('deployHint').textContent = locked ? 'The armies are committed. Reset to change the roster.' : config.map==='custom'?'Place both armies, then select and right-click to position. T sets facing; B splits.':'Select and right-click to position. T then click sets facing; B splits.';
  }
  function updateTally() {
    $('armyCount').textContent = fmt(totalRoster(config.armies[activeTeam]));
    $('armyCost').textContent = fmt(armyValue(config.armies[activeTeam]));
  }
  function canChangePlan(team) {
    return engine.phase !== 'ended' && (isDeployment() || control === 'both' || control === String(team));
  }
  function doctrineOptions() {
    return Object.values(data.DOCTRINES).map(plan=>`<option value="${esc(plan.id)}">${esc(plan.name)} · ${esc(plan.subtitle)}</option>`).join('');
  }
  function updateDoctrineDescription() {
    const plan=data.DOCTRINES[$('doctrineSelect').value];
    $('doctrineDescription').textContent=plan?plan.description:'';
  }
  function updateBattlePlans() {
    for(let team=0;team<2;team++){
      const army=config.armies[team],select=$('plan'+team+'Select');
      $('plan'+team+'Name').textContent=army.name;
      select.value=army.doctrine;
      select.disabled=!canChangePlan(team);$('applyPlan'+team).disabled=!canChangePlan(team);
      $('plan'+team+'Description').textContent=data.DOCTRINES[army.doctrine].description;
      const regs=engine.regiments.filter(r=>r.team===team&&r.alive>0&&!r._routed);
      const recovering=regs.filter(r=>r.fallingBack||r._rallyTime>0).length;
      const automatic=regs.filter(r=>r.order==='auto'&&!r.fallingBack&&r._rallyTime<=0).length;
      const manual=regs.length-automatic-recovering;
      $('plan'+team+'Status').textContent=isDeployment()?`${regs.length} formations ready to follow this plan.`:
        `${automatic} on Auto · ${manual} under direct orders · ${recovering} regrouping`;
      $('plan'+team+'Access').textContent=canChangePlan(team)?'':'Choose this army or Command both in the command bar to change its plan.';
    }
  }
  function applyBattlePlan(team,id) {
    if(!canChangePlan(team)||!Object.hasOwn(data.DOCTRINES,id))return false;
    if(!engine.setDoctrine(team,id))return false;
    config.armies[team].doctrine=id;
    if(customScaleBase)customScaleBase.armies[team].doctrine=id;
    if(activeTeam===team){$('doctrineSelect').value=id;updateDoctrineDescription();}
    updateBattlePlans();updateSelection();
    const plan=data.DOCTRINES[id];
    toast(`${config.armies[team].name}: ${plan.name}. ${isDeployment()?'Ready for the advance.':'Automatic formations will follow the plan; press G to return selected troops to Auto.'}`,5000);
    return true;
  }
  function setRosterCount(type, value) {
    if (!isDeployment()) return;
    const army = config.armies[activeTeam];
    const maximum = Math.min(armyLimit()-totalRoster(army)+army.roster[type],data.TYPES[type].maxCount||armyLimit());
    const count = clamp(Math.floor(Number(value) || 0),0,maximum);
    if (Number(value) > maximum) toast(data.TYPES[type].maxCount&&Number(value)>data.TYPES[type].maxCount?`Each army can field up to ${data.TYPES[type].maxCount} ${type==='commander'?'commanders':data.TYPES[type].shortName.toLowerCase()}.`:`Each army can field up to ${fmt(armyLimit())} combatants.`);
    army.roster[type] = count;
    const input = document.querySelector(`[data-roster="${type}"]`); if (input) input.value = count;
    scheduleRebuild();
  }
  function selectedRegiments() {
    return Array.from(selection).map(id=>engine.getRegiment(id)).filter(r=>r&&r.alive>0);
  }
  function updateCommandButtons(regs = selectedRegiments()) {
    const orders = new Set(regs.map(r=>r.order));
    const current = orders.size===1 ? regs[0].order : null;
    const armed = commandMode==='charge' ? 'chargeTarget' : commandMode;
    all('[data-command]').forEach(b=>{
      const isArmed = b.dataset.command===armed;
      const active = isArmed || (!commandMode && !isDeployment() && b.dataset.command===current);
      b.classList.toggle('active',active); b.classList.toggle('armed',isArmed);
      b.setAttribute('aria-pressed',String(active));
      b.disabled=!regs.length||control==='spectate'||engine.phase==='ended';
    });
    $('splitFormation').disabled=!regs.some(r=>r.alive>=2&&r.type!=='commander'&&r.status!=='routing')||regs.some(r=>r._rampartJob)||control==='spectate'||engine.phase==='ended'||engine.regiments.length>=512;
    canvas.style.cursor=commandMode||editorTool?'crosshair':'';
    if(renderer)renderer.pendingFacing=commandMode==='face'?{x:mouseWorld.x,y:mouseWorld.y}:null;
    updateRampartPreview();
  }
  function rampartOptions(){return{ranks:clamp(Number($('rampartRanks').value)||rampartDraft.ranks,1,3),spacing:['compact','normal','wide'].includes($('rampartSpacing').value)?$('rampartSpacing').value:rampartDraft.spacing};}
  function hasRangedFoot(regs){return regs.some(r=>{const t=data.TYPES[r.type];return t.ranged&&!t.mounted&&!t.siege;});}
  function rememberRampartIntent(regs,options,structureId,clear=false){
    if(isDeployment()){rememberDeployment();return;}
    for(const r of regs){const f=config.formations&&config.formations[r.id];if(!f||f.team!==r.team||f.type!==r.type||f.count!==r.initial)continue;
      if(clear)delete f.rampart;else{f.rampart=Object.assign({},f.rampart||{},options);const id=structureKey(structureId);if(id!==undefined)f.rampart.structureId=id;}
    }
  }
  function updateRampartPreview(){
    if(!renderer)return;
    const regs=selectedRegiments(),contextual=!editorTool&&hasRangedFoot(regs);
    const s=commandMode==='rampart'||contextual?structureAtPointer(mouseWorld.x,mouseWorld.y,true):null;
    renderer.pendingRampart=s&&s.walkable&&!s.breached&&regs.some(r=>r.team===s.team)&&(commandMode==='rampart'||s.type!=='gate')?Object.assign({structureId:s.id,regimentIds:Array.from(selection)},rampartOptions()):null;
  }
  function updateRampartControls(regs){
    const foot=regs.filter(r=>!data.TYPES[r.type].mounted&&!data.TYPES[r.type].siege&&!data.TYPES[r.type].commander);
    const active=foot.filter(r=>r._rampartJob),hasWall=(engine.structures||[]).some(s=>s.walkable&&!s.breached&&foot.some(r=>r.team===s.team));
    $('rampartControls').hidden=!foot.length||(!hasWall&&!active.length);
    const locked=control==='spectate'||engine.phase==='ended';
    $('manRampartButton').disabled=locked||!hasWall;$('leaveRampartButton').disabled=locked||!active.length;
    $('rampartRanks').disabled=$('rampartSpacing').disabled=locked;
    const ranks=new Set(active.map(r=>r.rampartRanks||2)),spaces=new Set(active.map(r=>r.rampartSpacing||'normal'));
    if(document.activeElement!==$('rampartRanks'))$('rampartRanks').value=active.length?(ranks.size===1?String(ranks.values().next().value):'mixed'):String(rampartDraft.ranks);
    if(document.activeElement!==$('rampartSpacing'))$('rampartSpacing').value=active.length?(spaces.size===1?spaces.values().next().value:'mixed'):rampartDraft.spacing;
    let onWall=0,climbing=0,total=0;for(const r of active)for(const id of r.unitIds){const u=engine.units[id];if(!u.alive||u.routed)continue;total++;if(u._onRampart)onWall++;else if(u._rampartClimb)climbing++;}
    const labels={approaching:'Approaching the stairs',climbing:'Climbing the stairs',ascending:'Climbing the stairs',forming:'Forming on the wall',guarding:'Guarding the rampart',courtyard:'Preparing in the courtyard',reforming:'Reforming in the courtyard',descending:'Descending to the courtyard',falling:'Falling from the wall'};
    const status=active.length===1?(labels[active[0].rampartStatus]||active[0].rampartStatus||'Rampart duty'):active.length?'Rampart duty':foot.some(r=>r.rampartStatus==='Courtyard reorganized')?'Courtyard defense ready':'Choose a friendly wall';
    $('rampartStatus').textContent=active.length?`${status} · ${onWall}/${total} on wall${climbing?' · '+climbing+' climbing':''}`:status;
    $('rampartStatus').title='Troops use real stairs and form into the available wall width. Surplus troops wait in the courtyard. Wall or gate breaches trigger a courtyard defense.';
  }
  function updateSelectedOrder(regs) {
    const el=$('selectedOrder');
    el.hidden=!regs.length||isDeployment()||engine.phase==='ended';
    if(el.hidden)return;
    const names={auto:'Auto',move:'Move',attack:'Attack-move',hold:'Hold',charge:'Charge',retreat:'Retreat',face:'Face'};
    const states={automatic:'Following battle plan',moving:'Moving',advancing:'Advancing',charging:'Closing on target',engaging:'Engaging',firing:'Firing',holding:'Holding here',regrouping:'Regrouping',turning:'Turning to withdraw',facing:'Turning to face',withdrawing:'Withdrawing',blocked:'Blocked', 'no-target':'No reachable target'};
    const orders=new Set(regs.map(r=>r.order)),statuses=new Set(regs.map(r=>r.commandStatus).filter(Boolean));
    const targets=new Map(regs.filter(r=>Number.isInteger(r.commandTargetId)&&r.commandTargetId>=0&&r.commandTargetName).map(r=>[r.commandTargetId,r.commandTargetName]));
    let label=orders.size===1?(names[regs[0].order]||'Following orders'):'Mixed orders';
    if(orders.size===1&&regs[0].order==='auto'){
      const plans=new Set(regs.map(r=>config.armies[r.team].doctrine));
      if(plans.size===1)label+=' · '+data.DOCTRINES[plans.values().next().value].name;
    }
    if(regs.length>1)label+=` · ${regs.length} formations`;
    if(orders.size===1&&regs[0].order==='charge'&&targets.size)label+=targets.size===1?` → ${targets.values().next().value}`:` → ${targets.size} enemy formations`;
    if(statuses.size===1)label+=` · ${states[statuses.values().next().value]||'Following orders'}`;
    else if(statuses.has('turning'))label+=' · Turning and withdrawing';
    else if(statuses.has('facing'))label+=' · Turning to face';
    else if(statuses.has('regrouping')||statuses.has('withdrawing'))label+=' · Some troops are recovering';
    if(regs.length===1&&regs[0].order==='auto'&&regs[0].siegeStatus)label='Siege · '+regs[0].siegeStatus;
    if(engine.phase==='paused')label='Paused · '+label;
    if(el.textContent!==label)el.textContent=label;
    el.title=engine.phase==='paused'?'Orders are stored immediately. Resume the battle to execute them.':statuses.has('turning')?'Soldiers turn as they withdraw. Repeating Retreat keeps their withdrawal in progress.':statuses.has('facing')?'Turn the ranks toward the chosen direction, then hold this position. Soldiers use the available room to reform.':'Troops march from their current positions and reform at the destination. Walls, enemy bodies and narrow passages can delay individual troops.';
  }
  function updatePhase() {
    if (!engine) return;
    const p = engine.phase;
    $('phaseLabel').textContent = p === 'deployment' ? 'DEPLOYMENT' : p === 'running' ? 'BATTLE LIVE' : p === 'paused' ? 'PAUSED' : 'BATTLE ENDED';
    $('phaseLabel').style.color = p === 'running' ? '#c4dca6' : '';
    setIcon($('playPauseButton'),p === 'running' ? 'pause' : 'play');
    $('playPauseButton').setAttribute('aria-label',p === 'running' ? 'Pause battle' : p === 'deployment' ? 'Start battle' : p === 'ended' ? 'Battle report' : 'Resume battle');
    $('fieldInstruction').textContent = p === 'deployment' ? 'SELECT · RIGHT CLICK TO POSITION · T THEN CLICK TO FACE' : 'RIGHT CLICK TO MARCH OR ATTACK · A FOR ATTACK-MOVE';
    $('statusText').textContent = p === 'deployment' ? engine.units.length?'Armies assembled. The field is yours.':'Empty field. Place troops for both armies to build your encounter.' : p === 'running' ? 'The battle is underway.' : p === 'paused' ? 'Battle paused. Orders are still available.' : 'The field is silent. Inspect your battle report.';
    setBuilderDisabled(p !== 'deployment');
    $('commandHint').hidden = !commandMode&&!editorTool;
    updateCommandButtons();
    updateSelectedOrder(selectedRegiments());
    $('formationSelect').disabled=!selection.size||control==='spectate'||engine.phase==='ended'||Array.from(selection).every(id=>{const r=engine.getRegiment(id);return r&&r.type==='commander';});
    if(selectedRegiments().some(r=>r._rampartJob))$('formationSelect').disabled=true;
    updateEncounterEditor();
  }
  function startBattle() {
    if(menuOpen)return;
    if (!isDeployment()) { newBattle(config,{deployment}); setBuilder(true); return; }
    if (!config.armies.every(a => totalRoster(a) > 0)) { toast('Add at least one combatant to each army before starting.'); return; }
    if (rebuildTimer) { clearTimeout(rebuildTimer); newBattle(config,{preserveCamera:true}); }
    rememberDeployment();editorTool=null;
    engine.start(); setBuilder(false); accumulator = 0;
    if (preferences.sound) audio.enable(true);
    audio.horn(false); updatePhase();
    toast('The battle has begun. Watch it unfold, or select formations and take command.',4200);
  }
  function playPause() {
    if(menuOpen)return;
    if (engine.phase === 'deployment') return startBattle();
    if (engine.phase === 'ended') { showResults(); return; }
    if (engine.phase === 'running') engine.pause();
    else { engine.resume(); setBuilder(false); }
    accumulator = 0; updatePhase();
  }
  function resetBattle(openBuilder = true) { newBattle(config,{deployment}); setBuilder(openBuilder); toast('Armies returned to their deployment.'); }

  function selectRegiments(ids, additive = false, toggle = false) {
    if (control === 'spectate') return;
    if (!additive) selection.clear();
    for (const id of ids) {
      const r = engine.getRegiment(id);
      if (!r || r.alive <= 0 || (control !== 'both' && r.team !== Number(control))) continue;
      if (toggle && selection.has(id)) selection.delete(id); else selection.add(id);
    }
    commandMode = null; updateSelection(); updatePhase();
  }
  function updateSelection() {
    const regs = Array.from(selection).map(id=>engine.getRegiment(id)).filter(r=>r && r.alive>0);
    selection = new Set(regs.map(r=>r.id));
    $('selectionPanel').hidden = !regs.length;
    updateRampartControls(regs);
    if (!regs.length) {commandMode=null;$('commandHint').hidden=true;$('formationSelect').disabled=true;$('rallyLabel').textContent='Rally';updateCommandButtons(regs);return;}
    updateCommandButtons(regs);updateSelectedOrder(regs);
    const alive = regs.reduce((s,r)=>s+r.alive,0);
    const morale = regs.reduce((s,r)=>s+(r.morale||0)*r.alive,0)/Math.max(1,alive);
    const cohesion = regs.reduce((s,r)=>s+(Number.isFinite(r.cohesion)?r.cohesion:100)*r.alive,0)/Math.max(1,alive);
    const types = new Set(regs.map(r=>r.type));
    $('selectedTeam').textContent = regs.every(r=>r.team===regs[0].team) ? config.armies[regs[0].team].name.toUpperCase() : 'COMBINED SELECTION';
    $('selectedName').textContent = regs.length === 1 ? regs[0].name||data.TYPES[regs[0].type].shortName : types.size === 1 ? `${regs.length} ${data.TYPES[regs[0].type].shortName} regiments` : `${regs.length} formations selected`;
    const compass=['E','SE','S','SW','W','NW','N','NE'];
    const directions=new Set(regs.map(r=>compass[(Math.round((Number.isFinite(r.facing)?r.facing:0)/(Math.PI/4))+8)%8]));
    $('selectedDetails').textContent = `${regs.length===1?data.TYPES[regs[0].type].shortName+' · ':''}${fmt(alive)} in army · ${Math.round(morale)}% morale · ${directions.size===1?'Facing '+directions.values().next().value:'Mixed facing'}`;
    const armorClasses=new Set(regs.map(r=>data.TYPES[r.type].armorClass)), selectedType=data.TYPES[regs[0].type];
    $('selectedMatchups').textContent=(armorClasses.size===1?armorLabel(selectedType)+' armor':'Mixed armor')+' · Matchup guide';
    $('selectedMatchups').title=types.size===1?counterDescription(selectedType):'Compare armor, weapon modifiers, useful targets, and threats for every troop type.';
    $('selectedMorale').style.width = `${clamp(morale,0,100)}%`;
    $('selectedMorale').style.background = morale < 30 ? '#d88269' : '#90b99a';
    const formations=new Set(regs.map(r=>r.formation||'line'));
    const formation=formations.size===1?(regs[0].formation||'line'):'mixed';
    if(document.activeElement!==$('formationSelect'))$('formationSelect').value=formation;
    $('formationSelect').disabled=control==='spectate'||engine.phase==='ended'||regs.every(r=>r.type==='commander');
    if(regs.some(r=>r._rampartJob))$('formationSelect').disabled=true;
    const recovering=regs.filter(r=>r.fallingBack||r.status==='rallying');
    const statusNames={steady:'Steady',shaken:'Shaken',fallingBack:'Falling back',rallying:'Reforming',routing:'Routing'};
    const turning=regs.filter(r=>r.commandStatus==='turning');
    const facing=regs.filter(r=>r.commandStatus==='facing');
    const status=regs.length===1?(turning.length?'Turning to withdraw':facing.length?'Turning to face':statusNames[regs[0].status]||'Steady'):turning.length?`${turning.length} turning to withdraw`:facing.length?`${facing.length} turning to face`:recovering.length?`${recovering.length} regrouping`:'Formations ready';
    const regroup=regs.length===1&&regs[0].fallingBack&&regs[0].regroupWith?` → ${regs[0].regroupWith}`:'';
    $('selectedStatus').textContent=`${status}${regroup} · ${Math.round(cohesion)}% cohesion`;
    $('selectedStatus').style.color=recovering.length?'#e8bb78':'';
    $('selectedCohesion').style.width=clamp(cohesion,0,100)+'%';
    const tactics=$('selectedTactics');
    tactics.hidden=engine.phase==='deployment'||engine.phase==='ended';
    if(!tactics.hidden){
      const roleNames={advance:'Advancing',line:'Pressing the line',flank:'Working the flank',support:'Ranged support',reserve:'Reforming after rally',hold:'Holding position',withdraw:'Withdrawing',command:'Supporting the army',charge:'Charging'};
      const roles=new Set(regs.map(r=>r.tacticalRole||'advance'));
      const engaged=regs.reduce((sum,r)=>sum+(Number(r.engagedSoldiers)||0),0);
      const pushing=regs.reduce((sum,r)=>sum+(Number(r.pushingSoldiers)||0),0);
      const role=roles.size===1?(roleNames[regs[0].tacticalRole]||'Following orders'):'Coordinating formations';
      const profile=regs.length===1&&data.TYPES[regs[0].type].missile;
      const plan=regs.length===1&&regs[0].order==='auto'&&!regs[0].fallingBack&&regs[0]._rallyTime<=0?regs[0]._tactic:null;
      const scope=plan&&plan.scopeLabel?` · ${plan.scopeLabel}${plan.targetCount>1?' ('+plan.targetCount+' squads)':''}`:'';
      tactics.textContent=`${plan?plan.phaseName:profile&&regs[0].tacticalRole==='support'?profile.label:role}${scope} · ${fmt(engaged)} active attackers${pushing?' · '+fmt(pushing)+' pushing':''}`;
      tactics.title=(plan?plan.detail+' ':'')+(profile?profile.description+' ':'')+'Living soldiers that attacked during the last 1.5 simulated seconds. Pushing counts hoplites that physically displaced an opponent. Counts update twice per simulated second; troops reloading can be between attacks.';
    }
    const riders=regs.filter(r=>['cavalry','lancer','horseArcher'].includes(r.type)),momentum=$('selectedMomentum'),momentumBar=$('selectedMomentumBar');
    momentum.hidden=momentumBar.hidden=!riders.length||engine.phase==='ended';
    if(!momentum.hidden){
      let count=0,speed=0;
      for(const r of riders)for(const id of r.unitIds){const u=engine.units[id];if(u&&u.alive){speed+=clamp((u._mountSpeed||0)/data.TYPES[u.type].speed,0,1.2);count++;}}
      const percent=Math.round(clamp(speed/Math.max(1,count),0,1)*100),shock=riders.some(r=>r.type!=='horseArcher');
      momentum.textContent=`${shock?'Charge':'Riding'} speed · ${percent}%${percent>=85?' · Full momentum':percent>=15?' · Building speed':' · Needs a run-up'}`;
      momentum.title='Actual riding speed. A straight run-up builds acceleration. Friendly horses sidestep to keep the charge moving. Sharp turns, enemy impacts, and blocked terrain remove momentum; impact strength scales with actual speed squared. Hoplites, pikes and supporting ranks resist the plunge.';
      $('selectedMomentumFill').style.width=percent+'%';momentumBar.setAttribute('aria-valuenow',String(percent));
    }
    const reload=$('selectedReload');
    reload.hidden=types.size!==1||!['musketeer','cannon','trebuchet'].includes(selectedType.id)||isDeployment()||engine.phase==='ended';
    if(!reload.hidden){
      let loaded=0,total=0,next=Infinity;
      for(const r of regs)for(const id of r.unitIds){
        const u=engine.units[id];
        if(!u.alive||u.routed)continue;
        total++;
        if(u._cooldown<=0)loaded++;
        else next=Math.min(next,u._cooldown);
      }
      reload.textContent=`${loaded}/${total} loaded${Number.isFinite(next)?' · Next ready in '+next.toFixed(1)+' s':''}`;
      if(selectedType.id==='musketeer'){
        const r=regs[0],status={melee:'Line broken · Close combat',reforming:'Reforming the firing line',marching:'Marching · Hold to form ranks',forming:'Forming ranks',ready:'Waiting for targets in range',aiming:'Aiming the company volley',reloading:'Reloading the company volley'}[r.volleyStatus]||'Forming ranks';
        const wait=Math.max(0,(r.nextVolleyAt||0)-engine.time);
        reload.textContent=`${status}${wait>.1&&r.volleyStatus!=='melee'?' · '+wait.toFixed(1)+' s':''} · ${loaded}/${total} loaded`;
      }
    }
    const siegeRegs=regs.filter(r=>r.siegeStatus||data.TYPES[r.type].equipment),siegeText=$('selectedSiege');
    siegeText.hidden=!siegeRegs.length||engine.phase==='ended';
    if(!siegeText.hidden){
      const r=siegeRegs[0],u=r.unitIds.map(id=>engine.units[id]).find(u=>u&&u.alive),target=u&&(engine.structures||[]).find(s=>s.id===u._siegeTarget);
      const progress=u&&u._siegeStage==='deploying'?` · Deploying ${Math.round(clamp(u._siegeProgress||0,0,1)*100)}%`:'';
      const health=target&&!target.breached?` · ${target.type==='gate'?'Gate':'Wall'} ${Math.ceil(target.hp)} / ${target.maxHp}`:'';
      siegeText.textContent=siegeRegs.length>1?`${siegeRegs.length} siege formations · ${siegeRegs.filter(r=>r.siegeStatus).length} working`:`${r.siegeStatus||'Ready for an assault'}${progress}${health}`;
      siegeText.title='Right-click an enemy gate or wall to assign a target. Auto chooses a useful approach. Towers deploy an assault ramp after docking; rams break gates. Move, Hold, Face and Retreat take priority.';
    }
    const maneuvering=regs.filter(r=>r.type!=='commander'&&((r.maneuver&&r.maneuver!=='formed')||Number(r.flexingSoldiers)>0));
    const maneuver=$('selectedManeuver');
    maneuver.hidden=!maneuvering.length;
    if(maneuvering.length){
      const descriptions={
        narrowing:'Narrowing for terrain',
        threading:'Weaving through allied ranks',
        reforming:'Re-forming the ranks'
      };
      const r=maneuvering[0];
      const localFlex=r.maneuver==='formed'&&Number(r.flexingSoldiers)>0;
      const frontage=Math.round(clamp((Number(r.effectiveWidth)||r.width)/Math.max(1,r.width),0,1)*100);
      maneuver.textContent=regs.length===1?(localFlex?`Ranks adjusting locally · ${fmt(r.flexingSoldiers)} soldiers`:`${descriptions[r.maneuver]||'Adapting the ranks'}${r.maneuver==='threading'?' · slowed':` · ${frontage}% frontage`}`):`${maneuvering.length} formations adapting their ranks`;
      maneuver.title=localFlex?'Soldiers use nearby space around crowded positions, then recover the chosen shape. Formation bonuses still depend on physical cohesion.':r.maneuver==='threading'?'Soldiers open gaps and pass between allied ranks. Crowding slows movement and disrupts cohesion.':r.maneuver==='reforming'?'Soldiers are walking back into the chosen formation after clearing an obstruction.':'The regiment narrows to fit the terrain. Soldiers follow the passage and spread out after clearing it.';
    }
    const commanders=regs.filter(r=>r.type==='commander');
    const cooldown=commanders.length?Math.min(...commanders.map(r=>Number(r.rallyCooldown)||0)):0;
    $('rallyLabel').textContent=cooldown>0?`${Math.ceil(cooldown)}s`:'Rally';
    const info=data.FORMATIONS&&data.FORMATIONS[formation];
    $('selectedFormationDetails').textContent=commanders.length===regs.length?(cooldown>0?`Rally in ${Math.ceil(cooldown)}s. Keep this commander behind your recovering troops.`:'Rally ready. Shaken allies in the gold circle can reform.'):info?formationSummary(info,cohesion):'Several formations selected. Choose a formation to organize their ranks.';
    $('selectedFormationDetails').title=info?info.description||'':'';
    const portrait = $('selectedPortrait'), portraitKey = regs[0].type+':'+regs[0].team;
    if (portrait.dataset.key !== portraitKey) {renderer.drawUnitIcon(portrait,regs[0].type,regs[0].team);portrait.dataset.key=portraitKey;}
  }
  function formationSummary(info,cohesion){
    const scaled=key=>Math.round(((Number(info[key])||1)-1)*clamp(cohesion,0,100));
    const percent=n=>(n>0?'+':n<0?'−':'')+Math.abs(n)+'%';
    if(info.id==='volley')return`Three ranks · Company volley · ${percent(scaled('speed'))} speed`;
    if(info.id==='column')return`Three soldiers abreast · Company volley · ${percent(scaled('incomingSplash'))} splash taken`;
    if(info.id==='shieldwall')return`${percent(scaled('frontalMelee'))} frontal melee taken · ${percent(scaled('frontalRanged'))} frontal missiles · ${percent(scaled('speed'))} speed`;
    if(info.id==='wedge')return`${percent(scaled('charge'))} charge · ${percent(scaled('speed'))} speed · ${percent(scaled('incomingMelee'))} melee taken`;
    if(info.id==='loose')return`${percent(scaled('incomingRanged'))} missiles · ${percent(scaled('incomingSplash'))} splash · ${percent(scaled('incomingMelee'))} melee taken`;
    return`${percent(scaled('frontalMelee'))} frontal melee damage taken. Keep the line together.`;
  }
  function acknowledgeOrder(command, accepted, regs, target) {
    const count=regs.length;
    const paused=engine.phase==='paused'?' Resume the battle to execute.':'';
    if(!accepted){
      const recovering=regs.some(r=>r.fallingBack||r.commandStatus==='regrouping');
      toast(recovering?'These troops are regrouping. Bring a commander close and rally them before giving combat orders.':command==='face'&&isDeployment()?'This turn needs clear space. Move the selected troops away from obstacles and other formations, then choose a direction.':target?'That formation is not a valid enemy for the selected troops.':'No formation could accept that order.',3600);
      return;
    }
    const scope=accepted===count?`${accepted} formation${accepted===1?'':'s'}`:`${accepted} of ${count} formations`;
    const names={hold:'holding position',move:'moving to the marked position',attack:'advancing toward the marked position',charge:'charging',auto:'following the army battle plan',face:isDeployment()?'facing set':'turning to face, then holding position',retreat:'withdrawing toward friendly support'};
    const targetNames=new Set(regs.filter(r=>r.order===command&&r.commandTargetName).map(r=>r.commandTargetName));
    const toward=command==='charge'?(target?` → ${target.name}`:targetNames.size===1?` → ${targetNames.values().next().value}`:' toward nearby enemies'):'';
    const refusalReasons=[];
    if(regs.some(r=>r.fallingBack||r.status==='rallying'||r.commandStatus==='regrouping'||r.commandStatus==='withdrawing'))refusalReasons.push('withdrawing or regrouping');
    if(target&&regs.some(r=>r.team===target.team))refusalReasons.push('allied to the chosen target');
    const rejected=accepted<count?(refusalReasons.length?` Other formations are ${refusalReasons.join(' or ')} and could not accept this order.`:' Other formations could not accept this order.'):'';
    const noTarget=command==='charge'&&regs.filter(r=>r.order==='charge').every(r=>r.commandStatus==='no-target');
    const action=noTarget?'no reachable enemy for the charge':`${names[command]||'orders accepted'}${toward}`;
    toast(`${engine.phase==='paused'?'Queued · ':''}${scope}: ${action}.${rejected}${paused}`,engine.phase==='paused'||accepted<count?4200:2800);
  }
  function setCommand(command) {
    if(menuOpen)return;
    if (!selection.size) {toast('Select one or more formations first.');return;}
    if (control === 'spectate' || engine.phase === 'ended') return;
    editorTool=null;canvas.focus({preventScroll:true});
    if (command === 'move' || command === 'attack' || command === 'chargeTarget' || command === 'face' || command==='rampart') {
      if(command==='chargeTarget'&&isDeployment()){toast('Targeted charges are available after the battle starts.');return;}
      commandMode = command==='chargeTarget'?'charge':command;
      $('commandHint').textContent = command==='rampart'?'Man ramparts: click or tap a friendly wall. Troops climb the stairs and form on its deck. Escape cancels.':command==='chargeTarget'?'Charge target: click or tap an enemy formation. Escape cancels.':command==='face'?'Face: click or tap the direction to face. The formation stays here. Left or right click confirms. Escape cancels.':`${command === 'move' ? 'Move' : 'Attack-move'}: click or tap a destination. Left or right click confirms. Escape cancels.`;
      updatePhase(); return;
    }
    if (isDeployment()) {toast('Use Move to position your troops and Face / T to set their direction. Combat orders are available after the battle starts.');return;}
    if(command==='rally'){
      const commanders=Array.from(selection).map(id=>engine.getRegiment(id)).filter(r=>r&&r.type==='commander'&&r.alive>0);
      if(!commanders.length){toast('Select a commander to rally troops. Press J or click Commander to find one.');return;}
      const ready=commanders.filter(r=>(Number(r.rallyCooldown)||0)<=0);
      if(!ready.length){toast(`Rally is recovering. Ready in ${Math.ceil(Math.min(...commanders.map(r=>r.rallyCooldown)))} seconds.`);return;}
      const rallied=engine.command(ready.map(r=>r.id),'rally');
      if(!rallied){toast('This commander must recover before sounding a rally.');return;}
      commandMode=null;updateSelection();updatePhase();
      toast('Rally sounded. Nearby shaken and retreating formations gather under their commander.',4200);return;
    }
    const regs=selectedRegiments(),accepted=engine.command(regs.map(r=>r.id),command);
    commandMode = null; updateSelection();updatePhase();
    acknowledgeOrder(command,accepted,regs);
  }
  function orderAt(x,y,kind,contextualRightClick=false) {
    if(menuOpen)return;
    if (!selection.size || control === 'spectate' || engine.phase === 'ended') return;
    let order=kind||commandMode;
    const regs=selectedRegiments();
    if(!Number.isFinite(x)||!Number.isFinite(y))return;
    if(order!=='face'){x=clamp(x,25,engine.width-25);y=clamp(y,25,engine.height-25);}
    const rangedWallClick=contextualRightClick&&hasRangedFoot(regs);
    const wall=structureAtPointer(x,y,order==='rampart'||rangedWallClick);
    if(order==='rampart'||((rangedWallClick||!order&&!isDeployment())&&wall&&wall.walkable&&!wall.breached&&wall.type!=='gate'&&regs.some(r=>r.team===wall.team))){
      if(!wall||!wall.walkable||wall.breached){toast('Choose an intact friendly wall with stairs.');return;}
      const options=rampartOptions(),accepted=engine.siege.manRampart(regs.map(r=>r.id),wall.id,options);
      if(accepted){rememberRampartIntent(regs.filter(r=>r.team===wall.team),options,wall.id);commandMode=null;updateSelection();updatePhase();toast(`${accepted} formation${accepted===1?'':'s'} assigned to the rampart. Troops climb its stairs; surplus soldiers wait in the courtyard.`,4500);}
      else toast('Choose foot troops and a friendly rampart with room and a safe stair approach.');
      return;
    }
    if(!isDeployment()&&(!order||order==='attack'||order==='charge')&&engine.siege&&engine.siege.structureAt){
      const structure=wall;
      if(!order&&structure&&structure.type==='gate'&&regs.every(r=>r.team===structure.team)){
        const opened=!structure.open;
        if(engine.siege.setGateOpen(structure.id,opened,structure.team)){
          toast(opened?'Gate opened. Both armies can pass.':'Gate closed.',2200);
        }else toast('Clear the doorway before closing the gate.');
        return;
      }
      if(structure&&!structure.breached&&!structure.open){
        const accepted=engine.siege.command(regs.map(r=>r.id),structure.id);
        if(accepted){toast(`${accepted} formation${accepted===1?'':'s'} ordered to assault the ${structure.type}.`,2600);commandMode=null;updateSelection();updatePhase();canvas.focus({preventScroll:true});}
        else toast('Choose a hostile gate for rams or infantry, or a hostile wall for a siege tower. Artillery can bombard both.');
        return;
      }
    }
    if(!order)order=!isDeployment()&&pickRegiment({x,y},true)!==null?'attack':'move';
    let target=null;
    if(order==='charge'&&!isDeployment()){
      const id=pickRegiment({x,y},true);
      target=id===null?null:engine.getRegiment(id);
      if(!target){toast('Choose an enemy soldier or standard for the charge. Escape cancels.');return;}
    }
    const terrain = engine.terrainAt(x,y);
    if (order!=='face'&&!target&&terrain.blocked) {toast('That ground is impassable. Choose a clearing or a bridge.');return;}
    if(order==='face'){
      const accepted=engine.faceRegiments(regs.map(r=>r.id),x,y);
      if(isDeployment())rememberDeployment();
      acknowledgeOrder('face',accepted,regs);
    }
    else if (isDeployment()) {engine.relocateRegiments(regs.map(r=>r.id),x,y);rememberDeployment();toast(`${regs.length} formation${regs.length===1?'':'s'} repositioned.`,2200);}
    else {
      const accepted=engine.command(regs.map(r=>r.id),order,x,y,target?{targetRegimentId:target.id}:undefined);
      acknowledgeOrder(order,accepted,regs,target);
    }
    commandMode=null;updateSelection();updatePhase();canvas.focus({preventScroll:true});
  }
  function focusSelection() {
    const regs = Array.from(selection).map(id=>engine.getRegiment(id)).filter(Boolean);
    if (!regs.length) return;
    renderer.focus(regs.reduce((s,r)=>s+r.x,0)/regs.length,regs.reduce((s,r)=>s+r.y,0)/regs.length,regs.length>3?.45:.95);
  }
  function setControl(value) {
    control = value; commanderCursor=-1; $('controlSelect').value = value; selection.clear(); commandMode=null;updateSelection();updatePhase();
    toast(value === 'spectate' ? 'Spectator mode. Drag to pan and enjoy the battle.' : value==='both' ? 'You can now command either army.' : `Commanding ${config.armies[Number(value)].name}.`,2200);
  }
  function findCommander(){
    if(control==='spectate'){toast('Choose an army in the command menu to direct its commanders.');return;}
    const commanders=engine.regiments.filter(r=>r.type==='commander'&&r.alive>0&&(control==='both'||r.team===Number(control)));
    if(!commanders.length){toast('This army has no surviving commander. Add commanders to its roster before battle.');return;}
    commanderCursor=(commanderCursor+1)%commanders.length;
    const r=commanders[commanderCursor];selectRegiments([r.id]);setBuilder(false);renderer.focus(r.x,r.y,.65);
    toast(`Commander selected. Move near shaken troops; press V to rally them.`,3000);
  }
  function toggleCinema(force) {
    cinematic = typeof force === 'boolean' ? force : !cinematic;
    $('app').classList.toggle('cinematic',cinematic);$('exitCinema').hidden=!cinematic;
    $('cinematicButton').setAttribute('aria-pressed',String(cinematic));renderer.options.cinematic=cinematic;
    requestAnimationFrame(resize);
  }
  function resize() {
    if (!renderer) return;
    const rect=$('stage').getBoundingClientRect();renderer.resize(rect.width,rect.height,Math.min(window.devicePixelRatio||1,2));
  }

  function updateUI(force) {
    if (!engine) return;
    const stats=engine.getStats();
    ['blue','red'].forEach((side,i)=>{
      const t=stats.teams[i];$(side+'Alive').textContent=fmt(t.alive);$(side+'Lost').textContent=fmt(t.dead);$(side+'Routed').textContent=fmt(t.routed);
      $(side+'FallingBack').textContent=fmt(t.fallingBack||0);$(side+'Rallied').textContent=fmt(t.rallied||0);
      $(side+'Strength').style.width=`${100*t.alive/Math.max(1,t.initial)}%`;
    });
    $('clock').textContent=clock(stats.time);$('unitTotal').textContent=`${fmt(stats.teams[0].alive+stats.teams[1].alive)} in army`;
    $('zoomLabel').textContent=`${Math.round(renderer.camera.zoom*100)}%`;
    $('performance').textContent=`${Math.round(fps)} FPS${engine.phase==='running' ? ' · '+Math.min(speed,simulationRatio).toFixed(1)+'×' : ''}`;
    const a=stats.teams[0].power||stats.teams[0].alive,b=stats.teams[1].power||stats.teams[1].alive;
    $('odds').textContent=engine.phase==='deployment'?'AWAITING ORDERS':engine.phase==='ended'?'BATTLE CONCLUDED':a>b*1.25?'AZURE ADVANTAGE':b>a*1.25?'EMBER ADVANTAGE':'CONTESTED FIELD';
    const events=(engine.events||[]).slice(-3), signature=events.map(e=>e.time+':'+e.text).join('|');
    if(signature!==visibleEventSignature){visibleEventSignature=signature;$('fieldFeed').innerHTML=events.map(e=>`<div class="feed-item ${e.team===0?'blue':e.team===1?'red':''}"><time>${clock(e.time||0)}</time>${esc(e.text)}</div>`).join('');}
    updateSelection();audio.update(performance.now()/1000,stats);
    if(engine.phase==='ended'&&!endedShown){endedShown=true;updatePhase();const endedEngine=engine;setTimeout(()=>{if(engine!==endedEngine)return;if(menuOpen){endedShown=false;return;}audio.horn(true);showResults();},700);}
    if(force)updatePhase();
  }

  function frame(now) {
    if (!renderer) return;
    // The opaque main menu needs no simulation, world redraw, minimap, or HUD.
    // Reset the clock so returning never simulates the time spent in the menu.
    if(menuOpen){lastFrame=0;accumulator=0;requestAnimationFrame(frame);return;}
    const rawDt=lastFrame?Math.max(.001,(now-lastFrame)/1000):1/60;
    const dt=Math.min(rawDt,.1);lastFrame=now;
    fps=fps*.95+(1/rawDt)*.05;
    const modalOpen=!!document.querySelector('dialog[open]');
    if(!modalOpen){
      let dx=0,dy=0;
      if(keys.has('arrowleft')||(!selection.size&&keys.has('a')))dx-=1;
      if(keys.has('arrowright')||(!selection.size&&keys.has('d')))dx+=1;
      if(keys.has('arrowup')||(!selection.size&&keys.has('w')))dy-=1;
      if(keys.has('arrowdown')||(!selection.size&&keys.has('s')))dy+=1;
      if(dx||dy)renderer.pan(-dx*650*dt,-dy*650*dt);
    }
    if(engine.phase==='running'&&!document.hidden){
      accumulator=Math.min(accumulator+dt*speed,.5);
      const budgetStart=performance.now();let steps=0;
      while(accumulator>=1/30&&steps<12&&engine.phase==='running'){
        engine.step(1/30);accumulator-=1/30;steps++;simulatedTime+=1/30;
        if(performance.now()-budgetStart>12)break;
      }
      observedTime+=rawDt;ratioTimer+=rawDt;
      if(ratioTimer>=1){simulationRatio=simulatedTime/Math.max(.01,observedTime);simulatedTime=0;observedTime=0;ratioTimer=0;}
    }else accumulator=0;
    if(mouseScreen)mouseWorld=renderer.screenToWorld(mouseScreen.x,mouseScreen.y);
    renderer.pendingFacing=commandMode==='face'?{x:mouseWorld.x,y:mouseWorld.y}:null;
    updateRampartPreview();
    if(engine.phase==='running'||now-lastRendered>=32){renderer.render(now/1000,selection);lastRendered=now;}
    const miniInterval=engine.units.length>8000?350:220;
    if(now-lastMinimap>miniInterval){
      const camera=renderer.camera,signature=[engine.time,engine.terrainRevision||0,camera.x,camera.y,camera.zoom,...selection].join(':');
      if(signature!==lastMiniSignature&&!$('minimapPanel').classList.contains('collapsed')&&!cinematic){renderer.renderMinimap($('minimap'),selection);lastMiniSignature=signature;}
      lastMinimap=now;
    }
    if(now-lastUI>200){updateUI();lastUI=now;}
    requestAnimationFrame(frame);
  }

  function pointFromEvent(e, element=canvas) {
    const rect=element.getBoundingClientRect();return{x:e.clientX-rect.left,y:e.clientY-rect.top};
  }
  function structureAtPointer(x,y,preferDeck=false){
    if(!engine.siege||!engine.siege.structureAt)return null;
    const ground=engine.siege.structureAt(x,y);if(ground&&!preferDeck)return ground;
    const structures=engine.structures||[];for(let i=structures.length-1;i>=0;i--){const s=structures[i];if(!s.walkable||s.breached)continue;const height=Number(s.deckHeight)||36;
      if(x>=s.x&&x<=s.x+s.w&&y+height>=s.y&&y+height<=s.y+s.h)return s;
    }return ground;
  }
  function regimentsInProjectedRect(x1,y1,x2,y2){
    const result=new Set();for(const u of engine.units){if(!u.alive||u.routed||(control!=='both'&&u.team!==Number(control)))continue;
      const y=u.y-(u._rampartZ||0);if(u.x>=x1&&u.x<=x2&&y>=y1&&y<=y2)result.add(u.regiment);
    }return Array.from(result);
  }
  function pickRegiment(world, hostile = false) {
    const threshold=Math.max(26,15/renderer.camera.zoom);
    const teams=hostile?new Set(selectedRegiments().map(r=>r.team)):null;
    const eligible=team=>hostile?teams.has(1-team):control==='both'||team===Number(control);
    let best=threshold*threshold,result=null;
    for(const u of engine.units){
      if(!u.alive||u.routed||!eligible(u.team))continue;
      const d=(u.x-world.x)**2+(u.y-(u._rampartZ||0)-world.y)**2;
      if(d<best){best=d;result=u.regiment;}
    }
    // Standards remain clickable when their formation has stretched out.
    if(result===null)for(const r of engine.regiments){
      if(r.alive<=0||!eligible(r.team))continue;
      const d=(r.x-world.x)**2+(r.y-world.y)**2;
      if(d<best){best=d;result=r.id;}
    }
    return result;
  }
  function bindBattlefieldInput() {
    const pointers=new Map();let drag=null,gesture=null,hadGesture=false;
    cancelBattlefieldGesture=()=>{
      for(const id of pointers.keys())try{canvas.releasePointerCapture(id);}catch(_){}
      pointers.clear();drag=null;gesture=null;hadGesture=false;miniDrag=false;
      $('selectionBox').style.display='none';$('selectionBox').classList.remove('terrain-selection');
    };
    function updateGesture(){
      const pts=Array.from(pointers.values());if(pts.length<2)return null;
      return{x:(pts[0].x+pts[1].x)/2,y:(pts[0].y+pts[1].y)/2,d:Math.hypot(pts[0].x-pts[1].x,pts[0].y-pts[1].y)};
    }
    canvas.addEventListener('contextmenu',e=>e.preventDefault());
    canvas.addEventListener('pointerdown',e=>{
      if(menuOpen||document.querySelector('dialog[open]'))return;
      if(e.button>2)return;e.preventDefault();canvas.focus({preventScroll:true});
      const p=pointFromEvent(e);mouseScreen=p;mouseWorld=renderer.screenToWorld(p.x,p.y);pointers.set(e.pointerId,p);
      try{canvas.setPointerCapture(e.pointerId);}catch(_){}
      if(pointers.size>1){gesture=updateGesture();hadGesture=true;drag=null;$('selectionBox').style.display='none';return;}
      const pan=e.button===1||e.altKey||control==='spectate';
      drag={pointer:e.pointerId,button:e.button,start:p,last:p,world:renderer.screenToWorld(p.x,p.y),moved:false,pan,touch:e.pointerType==='touch',additive:e.shiftKey,editor:!pan&&e.button===0&&isDeployment()?editorTool:null};
      $('selectionBox').classList.toggle('terrain-selection',!!drag.editor);
    });
    canvas.addEventListener('pointermove',e=>{
      if(menuOpen)return;
      const p=pointFromEvent(e);mouseScreen=p;mouseWorld=renderer.screenToWorld(p.x,p.y);
      if(pointers.has(e.pointerId))pointers.set(e.pointerId,p);
      if(pointers.size>1){
        const next=updateGesture();
        if(gesture&&next){renderer.pan(next.x-gesture.x,next.y-gesture.y);if(gesture.d>3)renderer.zoomAt(next.d/gesture.d,next.x,next.y);}
        gesture=next;return;
      }
      if(drag&&drag.pointer===e.pointerId){
        const distance=Math.hypot(p.x-drag.start.x,p.y-drag.start.y);
        if(distance>5)drag.moved=true;
        if(drag.pan||drag.button===2||(drag.touch&&!drag.editor)){
          if(drag.moved)renderer.pan(p.x-drag.last.x,p.y-drag.last.y);
        }else if(drag.moved){
          const b=$('selectionBox');b.style.display='block';b.style.left=Math.min(p.x,drag.start.x)+'px';b.style.top=Math.min(p.y,drag.start.y)+'px';b.style.width=Math.abs(p.x-drag.start.x)+'px';b.style.height=Math.abs(p.y-drag.start.y)+'px';
        }
        drag.last=p;
      }
      const terrain=engine.terrainAt(mouseWorld.x,mouseWorld.y);
      const descriptions={forest:'Woodland · cover',hill:'High ground',water:'River · impassable',bridge:'Bridge crossing',rock:'Rock · impassable',road:'Road · faster marching',mud:'Mud · slow ground',marsh:'Marsh · slow ground and cover',snow:'Deep snow · slow ground',trench:'Trench · cover and slow movement',wall:'Defensive wall',gate:'Defensive gate',ramp:'Assault ramp · open',rubble:'Breached fortification · open',plain:'Open ground',ground:'Open ground'};
      const structure=structureAtPointer(mouseWorld.x,mouseWorld.y);
      $('terrainName').textContent=terrain.type==='ramp'?descriptions.ramp:structure&&structure.castleGate?`${structure.open?'Open':'Closed'} ${structure.name||'gate'} · ${Math.ceil(structure.hp)} HP · Select defenders and right-click or tap to ${structure.open?'close':'open'}`:structure?`${structure.breached?'Breach':structure.name||data.STRUCTURE_TYPES?.[structure.structureKind]?.name||(structure.type==='gate'?'Gate':structure.castleTower?'Corner tower':'Wall')}${structure.breached?' · open':` · ${Math.ceil(structure.hp)} / ${structure.maxHp}`}`:descriptions[terrain.type]||'Open ground';
      if(!editorTool&&structure&&structure.walkable&&!structure.breached&&structure.type!=='gate'&&hasRangedFoot(selectedRegiments().filter(r=>r.team===structure.team)))$('terrainName').textContent+=' · Right-click to man ramparts';
    });
    function finishPointer(e,cancelled){
      if(menuOpen){pointers.clear();drag=null;gesture=null;hadGesture=false;return;}
      pointers.delete(e.pointerId);
      if(hadGesture){if(!pointers.size){hadGesture=false;gesture=null;}drag=null;return;}
      if(!drag||drag.pointer!==e.pointerId)return;
      const d=drag;drag=null;$('selectionBox').style.display='none';$('selectionBox').classList.remove('terrain-selection');if(cancelled)return;
      const p=pointFromEvent(e),world=renderer.screenToWorld(p.x,p.y);
      if(d.editor){if(d.editor==='troops')placeFormationAt(world.x,world.y);else editTerrain(d.world,world,d.editor);return;}
      if(d.pan||(d.touch&&d.moved))return;
      if(d.button===2){if(!d.moved)orderAt(world.x,world.y,commandMode,true);return;}
      if(d.button!==0)return;
      if(d.moved){
        const first=renderer.screenToWorld(d.start.x,d.start.y);
        const ids=regimentsInProjectedRect(Math.min(first.x,world.x),Math.min(first.y,world.y),Math.max(first.x,world.x),Math.max(first.y,world.y));
        selectRegiments(ids,d.additive);return;
      }
      if(commandMode){orderAt(world.x,world.y,commandMode);return;}
      if(d.touch&&!isDeployment()&&selection.size&&engine.siege){
        const gate=structureAtPointer(world.x,world.y);
        if(gate&&gate.type==='gate'&&selectedRegiments().every(r=>r.team===gate.team)){orderAt(world.x,world.y);return;}
      }
      const id=pickRegiment(world);selectRegiments(id===null?[]:[id],d.additive,d.additive);
    }
    canvas.addEventListener('pointerup',e=>finishPointer(e,false));
    canvas.addEventListener('pointercancel',e=>finishPointer(e,true));
    canvas.addEventListener('wheel',e=>{e.preventDefault();if(menuOpen)return;const p=pointFromEvent(e);renderer.zoomAt(Math.exp(-clamp(e.deltaY,-200,200)*.0017),p.x,p.y);},{passive:false});
    canvas.addEventListener('dblclick',e=>{e.preventDefault();if(!menuOpen&&!editorTool)focusSelection();});
    const mini=$('minimap');let miniDrag=false;
    function seek(e){if(menuOpen)return;const r=mini.getBoundingClientRect();renderer.focus(clamp((e.clientX-r.left)/r.width,0,1)*engine.width,clamp((e.clientY-r.top)/r.height,0,1)*engine.height);}
    mini.addEventListener('pointerdown',e=>{e.preventDefault();miniDrag=true;mini.setPointerCapture(e.pointerId);seek(e);});
    mini.addEventListener('pointermove',e=>{if(miniDrag)seek(e);});
    mini.addEventListener('pointerup',()=>{miniDrag=false;});mini.addEventListener('pointercancel',()=>{miniDrag=false;});
    mini.addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();renderer.fit();}});
    window.addEventListener('keydown',e=>{
      if(menuOpen){if(e.key==='Escape'&&enteredField&&!document.querySelector('dialog[open]')){e.preventDefault();closeMainMenu(true);}return;}
      const tag=e.target.tagName;
      if((['INPUT','SELECT','TEXTAREA'].includes(tag)||e.target.dataset?.troopCategory)&&e.key!=='Escape')return;
      if(document.querySelector('dialog[open]'))return;
      const k=e.key.toLowerCase();keys.add(k);
      if([' ','tab','arrowleft','arrowright','arrowup','arrowdown'].includes(k))e.preventDefault();
      if(e.repeat)return;
      if(k===' '){playPause();return;}
      if(k==='f'){renderer.fit();return;}
      if(k==='j'){findCommander();return;}
      if(k==='tab'){toggleCinema();return;}
      if(k==='escape'){if(editorTool){editorTool=null;updatePhase();toast('Encounter editing finished. Select formations to command them.',2000);return;}if(commandMode){commandMode=null;updatePhase();toast('Pending order cancelled. Formations remain selected.',2000);return;}selection.clear();updateSelection();updatePhase();if(cinematic)toggleCinema(false);return;}
      if(k==='?'){openDialog('helpDialog');return;}
      if(['1','2','3'].includes(k)){setControl(k==='1'?'0':k==='2'?'1':'spectate');return;}
      const commandKeys={m:'move',t:'face',a:'attack',h:'hold',c:'charge',g:'auto',r:'retreat',v:'rally'};
      if(selection.size&&k==='b'){e.preventDefault();splitSelectedFormations();return;}
      if(selection.size&&k==='c'&&e.shiftKey){e.preventDefault();setCommand('chargeTarget');return;}
      if(selection.size&&commandKeys[k]){e.preventDefault();setCommand(commandKeys[k]);}
    });
    window.addEventListener('keyup',e=>keys.delete(e.key.toLowerCase()));
    window.addEventListener('blur',()=>{keys.clear();cancelBattlefieldGesture();});
    document.addEventListener('visibilitychange',()=>{
      if(document.hidden&&engine.phase==='running'){engine.pause();updatePhase();toast('Battle paused while the game was in the background.');}
      lastFrame=0;accumulator=0;keys.clear();if(document.hidden)cancelBattlefieldGesture();
    });
  }

  function openDialog(id) {
    keys.clear();cancelBattlefieldGesture();
    if(engine.phase==='running'){engine.pause();updatePhase();}
    if(id==='scenariosDialog')renderSavedScenarios();
    if(id==='battlePlansDialog')updateBattlePlans();
    if(id==='helpDialog'&&!$('manualRoster').children.length)buildManual();
    const dialog=$(id);if(!dialog.open)dialog.showModal();
  }
  function closeDialog(id){const d=$(id);if(d.open)d.close();(menuOpen?(enteredField?$('menuResume'):$('menuQuick')):canvas).focus({preventScroll:true});}
  function buildManual(){
    $('manualRoster').innerHTML=data.TYPE_ORDER.map(id=>{const t=data.TYPES[id];return `<article class="manual-unit"><div class="unit-title"><canvas data-manual-portrait="${id}" width="76" height="76"></canvas><strong>${esc(t.name)}</strong></div>${armorBadge(t)}<p>${esc(t.description)}</p><p class="unit-stats">HP ${t.hp} · ARMOR POINTS ${t.armor} · DAMAGE ${t.damage} · COST ${t.cost}</p><p class="manual-counter"><strong>Good targets:</strong> ${esc(counterNames(t.matchups.strongAgainst,3))}<br><strong>Threats:</strong> ${esc(counterNames(t.matchups.weakAgainst,3))}</p><button class="counter-link" data-counter-type="${id}">Compare matchups</button></article>`;}).join('');
    all('[data-manual-portrait]').forEach(c=>renderer.drawUnitIcon(c,c.dataset.manualPortrait,0));
    $('manualFormations').innerHTML=Object.values(data.FORMATIONS||{}).map(f=>`<article><h4>${esc(f.name)}</h4><p>${esc(f.description||f.summary||'')}</p></article>`).join('');
    $('manualPlans').innerHTML=Object.values(data.DOCTRINES).map(p=>`<article><h4>${esc(p.name)}</h4><p>${esc(p.description)}</p></article>`).join('');
    $('armorLegend').innerHTML=armorOrder.map(id=>`<div><span class="armor-badge armor-${id}">${esc(data.ARMOR_CLASSES[id].label)} armor</span><p>${esc(data.ARMOR_CLASSES[id].description)}</p></div>`).join('');
    const options=data.TYPE_ORDER.map(id=>`<option value="${id}">${esc(data.TYPES[id].name)}</option>`).join('');
    $('matchupAttacker').innerHTML=options;$('matchupDefender').innerHTML=options;
    $('matchupAttacker').value='pikeman';$('matchupDefender').value='cavalry';
    $('armorMatchupTable').innerHTML=data.TYPE_ORDER.map(id=>{const t=data.TYPES[id];return `<tr data-matchup-row="${id}"><th scope="row">${esc(t.shortName)}</th><td>${armorBadge(t)}</td>${armorOrder.map(armor=>{const n=data.armorMultiplier(id,armor);return `<td class="modifier ${n>1?'modifier-up':n<1?'modifier-down':'modifier-even'}" data-vs-armor="${armor}">×${n.toFixed(2)}</td>`;}).join('')}</tr>`;}).join('');
    updateMatchupGuide();
  }
  function updateMatchupGuide(){
    const attacker=data.TYPES[$('matchupAttacker').value],defender=data.TYPES[$('matchupDefender').value];
    if(!attacker||!defender)return;
    const m=data.getMatchup(attacker.id,defender.id),profile=data.ATTACK_PROFILES[attacker.attackProfile];
    const edge=attacker.matchups.strongAgainst.find(item=>item.type===defender.id),threat=attacker.matchups.weakAgainst.find(item=>item.type===defender.id);
    const context=[edge?`<strong>Good target:</strong> ${esc(edge.reason)}`:'',threat?`<strong>Threat:</strong> ${esc(threat.reason)}`:''].filter(Boolean).join('<br>')||esc(attacker.matchups.roleNote);
    const list=entries=>`<ul>${entries.map(item=>`<li><strong>${esc(data.TYPES[item.type].name)}:</strong> ${esc(item.reason)}</li>`).join('')}</ul>`;
    const modifier=m.armorMultiplier,change=Math.round((modifier-1)*100);
    const changeText=change===0?'Neutral armor matchup':`${Math.abs(change)}% ${change>0?'more':'less'} damage from this armor matchup`;
    $('matchupResult').innerHTML=`<div class="matchup-pair"><div><span class="eyebrow">ATTACKER</span><strong>${esc(attacker.name)}</strong><small>${esc(profile.label)}</small></div><div class="matchup-factor ${modifier>1?'modifier-up':modifier<1?'modifier-down':'modifier-even'}">×${modifier.toFixed(2)}<small>vs armor</small></div><div><span class="eyebrow">DEFENDER</span><strong>${esc(defender.name)}</strong>${armorBadge(defender)}</div></div><p class="matchup-statline">${esc(changeText)} · Pierces ${Math.round(m.armorPierce*100)}% of armor points${m.antiMountedBonus>0?` · +${m.antiMountedBonus} base damage against this mounted target`:''}.</p><p class="matchup-context">${context}</p><div class="matchup-notes"><section><h4>Good targets for ${esc(attacker.shortName)}</h4>${list(attacker.matchups.strongAgainst)}</section><section><h4>Threats to ${esc(attacker.shortName)}</h4>${list(attacker.matchups.weakAgainst)}</section></div><p class="matchup-role-note">${esc(attacker.matchups.roleNote)}</p>`;
  }
  function showCounterGuide(type){
    openDialog('helpDialog');
    if(data.TYPES[type]){
      $('matchupAttacker').value=type;
      const first=data.TYPES[type].matchups.strongAgainst[0];
      if(first)$('matchupDefender').value=first.type;
      updateMatchupGuide();
    }
    const section=$('matchupSection');if(section.scrollIntoView)section.scrollIntoView({behavior:'smooth',block:'start'});
    $('matchupAttacker').focus({preventScroll:true});
  }
  function savePreferences(){safeWrite(PREFS,preferences);}
  function setSound(value){
    preferences.sound=value;audio.volume=preferences.volume/100;audio.enable(value);
    if(value&&!audio.enabled){preferences.sound=false;toast('Audio is not available in this browser.');}
    $('soundEnabled').checked=preferences.sound;$('soundButton').setAttribute('aria-pressed',String(preferences.sound));
    $('soundButton').setAttribute('aria-label',preferences.sound?'Mute battlefield sound':'Enable battlefield sound');
    setIcon($('soundButton'),preferences.sound?'sound':'mute');savePreferences();
  }
  function scenarioSnapshot(){
    const name=($('scenarioName').value||config.name||'Custom battle').trim().slice(0,64);
    if(isDeployment()){if(rebuildTimer){clearTimeout(rebuildTimer);newBattle(config,{preserveCamera:true});}rememberDeployment();$('scenarioName').value=name;}
    return{format:'ironfield-scenario',version:2,name,savedAt:new Date().toISOString(),config:clone(config),deployment:clone(deployment)};
  }
  function download(name,content,type='application/json'){
    const blob=new Blob([content],{type}),url=URL.createObjectURL(blob),link=document.createElement('a');link.href=url;link.download=name;document.body.appendChild(link);link.click();link.remove();setTimeout(()=>URL.revokeObjectURL(url),10000);
  }
  function filename(value){return String(value).replace(/[^a-z0-9_-]+/gi,'-').replace(/^-|-$/g,'').slice(0,64)||'ironfield-battle';}
  function loadScenario(snapshot){
    if(!snapshot||snapshot.format!=='ironfield-scenario'||![1,2].includes(snapshot.version))throw new Error('This is not a supported IRONFIELD scenario.');
    const checked=cleanConfig(snapshot.config);checked.name=typeof snapshot.name==='string'?snapshot.name.slice(0,64):checked.name;
    if(menuOpen)closeMainMenu(false);
    customScaleBase=null;terrainHistory=[];newBattle(checked,{deployment:snapshot.deployment});$('presetSelect').value='custom';$('scaleSelect').value='1';
    $('presetDescription').textContent='A saved custom battle, ready to command.';setBuilder(true);closeDialog('scenariosDialog');toast('Scenario loaded at deployment.');
  }
  function renderSavedScenarios(){
    const saved=safeRead(STORAGE,[]);const list=Array.isArray(saved)?saved.slice(0,24):[];
    if(!list.length){$('savedScenarios').innerHTML='<div class="empty-state">No saved battles yet. Assemble your armies and save your first scenario.</div>';return;}
    $('savedScenarios').innerHTML=list.map((s,i)=>{
      let count=0,map=null;try{const savedConfig=cleanConfig(s.config);count=savedConfig.armies.reduce((n,a)=>n+totalRoster(a),0);const template=data.MAPS.find(m=>m.id===savedConfig.map);map=savedConfig.map==='custom'?Object.assign({},template,savedConfig.customMap):template;}catch(_){}
      return`<div class="saved-scenario"><div class="saved-scenario-info"><strong>${esc(s.name||'Saved battle')}</strong><small>${fmt(count)} combatants · ${esc(map?`${mapSizeLabel(map)} — ${map.name}`:'Battlefield')}</small></div><button class="quiet-button" data-load-scenario="${i}">Load</button><button class="icon-button" data-delete-scenario="${i}" aria-label="Delete ${esc(s.name||'saved battle')}">${icon('trash')}</button></div>`;
    }).join('');
  }
  function showResults(){
    if(engine.phase!=='ended')return;
    const stats=engine.getStats(),winner=stats.winner;
    $('resultTitle').textContent=winner===0||winner===1?`${config.armies[winner].name} prevails.`:'The battle ends in a draw.';
    $('resultEyebrow').textContent=winner===0?'VICTORY / AZURE':winner===1?'VICTORY / EMBER':'THE BATTLE IS OVER';
    const total=stats.teams.reduce((n,t)=>n+t.initial,0),dead=stats.teams.reduce((n,t)=>n+t.dead,0);
    $('resultSummary').textContent=`${fmt(total)} combatants took the field. After ${clock(stats.time)}, ${fmt(dead)} had fallen. ${stats.reason||'The opposing army could no longer hold the field.'}`;
    $('resultCards').innerHTML=stats.teams.map((t,i)=>`<section class="result-card ${i===1?'red':''}"><h3>${esc(config.armies[i].name.toUpperCase())}</h3><div class="result-numbers"><div><strong>${fmt(t.alive)}</strong><span>remaining</span></div><div><strong>${fmt(t.dead)}</strong><span>fallen</span></div><div><strong>${fmt(t.routed)}</strong><span>routed</span></div></div><div class="result-strip"><i style="width:${t.alive/Math.max(1,t.initial)*100}%;background:${i?'#dd8674':'#72b6d1'}"></i><i style="width:${t.routed/Math.max(1,t.initial)*100}%;background:#bca26a"></i><i style="width:${t.dead/Math.max(1,t.initial)*100}%;background:#5b665d"></i></div><small>${fmt(t.initial)} fielded · ${Math.round(t.alive/Math.max(1,t.initial)*100)}% held the field</small></section>`).join('');
    const breakdown=troopBreakdown();
    $('resultTable').innerHTML=data.TYPE_ORDER.filter(id=>breakdown[0][id].initial+breakdown[1][id].initial>0).map(id=>{
      const a=breakdown[0][id],b=breakdown[1][id];return`<tr><td>${esc(data.TYPES[id].name)}</td><td>${fmt(a.alive)} / ${fmt(a.initial)}</td><td>${fmt(a.dead)} / ${fmt(a.routed)}</td><td>${fmt(b.alive)} / ${fmt(b.initial)}</td><td>${fmt(b.dead)} / ${fmt(b.routed)}</td></tr>`;
    }).join('');
    openDialog('resultsDialog');
  }
  function troopBreakdown(){
    const result=[{},{}];for(let team=0;team<2;team++)data.TYPE_ORDER.forEach(id=>{result[team][id]={initial:0,alive:0,dead:0,routed:0};});
    for(const u of engine.units){const t=result[u.team][u.type];t.initial++;if(u.routed)t.routed++;else if(u.alive)t.alive++;else t.dead++;}return result;
  }

  function bindControls(){
    $('startButton').addEventListener('click',startBattle);$('playPauseButton').addEventListener('click',playPause);
    $('resetButton').addEventListener('click',()=>resetBattle(true));
    $('builderButton').addEventListener('click',()=>setBuilder(!$('app').classList.contains('builder-open')));
    $('closeBuilder').addEventListener('click',()=>setBuilder(false));
    $('fitButton').addEventListener('click',()=>renderer.fit());
    $('commanderButton').addEventListener('click',findCommander);
    $('battlePlansButton').addEventListener('click',()=>openDialog('battlePlansDialog'));
    for(let team=0;team<2;team++){
      $('plan'+team+'Select').addEventListener('change',e=>{
        const plan=data.DOCTRINES[e.target.value];
        $('plan'+team+'Description').textContent=plan?plan.description:'';
      });
      $('applyPlan'+team).addEventListener('click',()=>applyBattlePlan(team,$('plan'+team+'Select').value));
    }
    $('splitFormation').addEventListener('click',splitSelectedFormations);
    $('newEncounter').addEventListener('click',newEmptyEncounter);
    all('[data-terrain-tool]').forEach(b=>b.addEventListener('click',()=>chooseEditorTool(b.dataset.terrainTool)));
    $('finishTerrain').addEventListener('click',()=>{editorTool=null;updatePhase();canvas.focus({preventScroll:true});});
    $('undoTerrain').addEventListener('click',()=>{if(terrainHistory.length&&applyTerrain(terrainHistory[terrainHistory.length-1],false)){terrainHistory.pop();updateEncounterEditor();}});
    $('clearTerrain').addEventListener('click',()=>{if(config.map==='custom'&&config.customMap.regions.length)applyTerrain([]);});
    $('terrainBiome').addEventListener('change',e=>{if(!isDeployment()||config.map!=='custom')return;rememberDeployment();config.customMap.biome=e.target.value;newBattle(config,{preserveCamera:true,preserveEditor:true});});
    $('customWorldSize').addEventListener('change',e=>resizeCustomWorld(e.target.value));
    $('terrainDefender').addEventListener('change',updateEncounterEditor);
    $('fortressStyle').addEventListener('change',updateEncounterEditor);
    $('wallStairSide').addEventListener('change',updateEncounterEditor);
    $('placeFormationButton').addEventListener('click',()=>chooseEditorTool('troops'));
    $('placeTroop').addEventListener('change',()=>{updatePlaceCount();updateEncounterEditor();});
    $('placeCount').addEventListener('change',()=>{$('placeCount').value=clamp(Math.floor(Number($('placeCount').value)||1),1,data.TYPES[$('placeTroop').value||'swordsman'].groupSize||400);updateEncounterEditor();});
    $('formationSelect').addEventListener('change',e=>{
      if(!selection.size||!engine.setFormation)return;
      engine.setFormation(Array.from(selection),e.target.value);
      if(isDeployment())rememberDeployment();
      const info=data.FORMATIONS&&data.FORMATIONS[e.target.value];
      updateSelection();toast(`${info?info.name:'Formation'} ordered. Its bonuses depend on keeping the ranks cohesive.`,3500);
      canvas.focus({preventScroll:true});
    });
    $('manRampartButton').addEventListener('click',()=>setCommand('rampart'));
    $('leaveRampartButton').addEventListener('click',()=>{
      const regs=selectedRegiments(),accepted=engine.siege&&engine.siege.leaveRamparts?engine.siege.leaveRamparts(regs.map(r=>r.id)):0;
      if(accepted){rememberRampartIntent(regs,null,null,true);commandMode=null;updateSelection();updatePhase();toast('Rampart troops descend and form to defend the courtyard.',3500);}
    });
    for(const id of ['rampartRanks','rampartSpacing'])$(id).addEventListener('change',()=>{
      rampartDraft=rampartOptions();const regs=selectedRegiments();
      if(engine.siege&&engine.siege.setRampartFormation){engine.siege.setRampartFormation(regs.map(r=>r.id),rampartDraft);rememberRampartIntent(regs.filter(r=>r._rampartJob),rampartDraft);}
      updateSelection();updateRampartPreview();
    });
    $('zoomInButton').addEventListener('click',()=>{const r=canvas.getBoundingClientRect();renderer.zoomAt(1.5,r.width/2,r.height/2);});
    $('zoomOutButton').addEventListener('click',()=>{const r=canvas.getBoundingClientRect();renderer.zoomAt(1/1.5,r.width/2,r.height/2);});
    const categoryTabs=all('[data-troop-category]');
    categoryTabs.forEach((tab,index)=>{
      tab.addEventListener('click',()=>chooseTroopCategory(tab));
      tab.addEventListener('keydown',e=>{
        const count=categoryTabs.length;
        const destination={ArrowLeft:(index+count-1)%count,ArrowRight:(index+1)%count,Home:0,End:count-1}[e.key];
        if(destination===undefined)return;
        e.preventDefault();categoryTabs[destination].focus();chooseTroopCategory(categoryTabs[destination]);
      });
    });
    $('editBlue').addEventListener('click',()=>{activeTeam=0;populateRoster();});$('editRed').addEventListener('click',()=>{activeTeam=1;populateRoster();});
    $('presetSelect').addEventListener('change',e=>loadPreset(e.target.value));
    $('mapSelect').addEventListener('change',e=>changeBattlefield(e.target.value));
    $('scaleSelect').addEventListener('change',e=>{
      const factor=Number(e.target.value);
      if($('presetSelect').value!=='custom'){
        const plans=config.armies.map(army=>army.doctrine);
        loadPreset(chosenPreset,factor);
        plans.forEach((plan,team)=>{config.armies[team].doctrine=plan;engine.setDoctrine(team,plan);});
        populateRoster();return;
      }
      if(!customScaleBase)customScaleBase=clone(config);
      const raw=clone(customScaleBase);
      raw.armies.forEach((army,team)=>{
        const scale=Math.min(factor,armyLimit()/Math.max(1,totalRoster(army)));
        data.TYPE_ORDER.forEach(id=>{army.roster[id]=Array.isArray(raw.formations)&&data.TYPES[id].commander?army.roster[id]:Math.floor(army.roster[id]*scale);});
        if(raw.formations)raw.formations.filter(f=>f.team===team).forEach(f=>{
          // Keep arranged soldiers in place. Extra recruits form additional
          // groups; smaller armies retain a subset of each saved footprint.
          f.count=Math.min(f.count,Math.floor(f.count*scale));
          if(f.offsets)f.offsets=f.offsets.slice(0,f.count);
        });
      });
      if(raw.formations)raw.formations=raw.formations.filter(f=>f.count>0);
      if(factor!==1)raw.name+=` · ${factor}×`;
      newBattle(raw);$('presetSelect').value='custom';$('scaleSelect').value=String(factor);$('presetDescription').textContent='Your custom armies scaled to a new battle size.';
    });
    $('doctrineSelect').addEventListener('change',e=>{applyBattlePlan(activeTeam,e.target.value);canvas.focus({preventScroll:true});});
    $('mirrorArmy').addEventListener('click',()=>{config.armies[1-activeTeam].roster=clone(config.armies[activeTeam].roster);scheduleRebuild();toast('The opposing army now has the same troop composition.');});
    $('clearArmy').addEventListener('click',()=>{data.TYPE_ORDER.forEach(t=>{config.armies[activeTeam].roster[t]=0;});scheduleRebuild();populateRoster();});
    $('armyList').addEventListener('click',e=>{
      const guide=e.target.closest('[data-counter-type]');if(guide){showCounterGuide(guide.dataset.counterType);return;}
      const b=e.target.closest('[data-count-type]');if(!b||b.disabled)return;const type=b.dataset.countType,t=data.TYPES[type];
      const increment=e.shiftKey||type==='commander'?1:t.siege?4:type==='elephant'?2:t.mounted?20:50;
      setRosterCount(type,config.armies[activeTeam].roster[type]+Number(b.dataset.delta)*increment);
    });
    $('armyList').addEventListener('change',e=>{if(e.target.dataset.roster)setRosterCount(e.target.dataset.roster,e.target.value);});
    $('controlSelect').addEventListener('change',e=>setControl(e.target.value));
    all('[data-command]').forEach(b=>b.addEventListener('click',()=>setCommand(b.dataset.command)));
    all('[data-speed]').forEach(b=>b.addEventListener('click',()=>{speed=Number(b.dataset.speed);all('[data-speed]').forEach(x=>x.classList.toggle('active',x===b));}));
    $('focusSelection').addEventListener('click',focusSelection);$('clearSelection').addEventListener('click',()=>{selection.clear();commandMode=null;updateSelection();updatePhase();});
    $('cinematicButton').addEventListener('click',()=>toggleCinema());$('exitCinema').addEventListener('click',()=>toggleCinema(false));
    $('toggleMinimap').addEventListener('click',()=>{const collapsed=$('minimapPanel').classList.toggle('collapsed');$('toggleMinimap').textContent=collapsed?'+':'−';$('toggleMinimap').setAttribute('aria-label',collapsed?'Show minimap':'Hide minimap');});
    $('helpButton').addEventListener('click',()=>openDialog('helpDialog'));
    $('selectedMatchups').addEventListener('click',()=>showCounterGuide(selectedRegiments()[0]?.type));
    $('matchupAttacker').addEventListener('change',updateMatchupGuide);$('matchupDefender').addEventListener('change',updateMatchupGuide);
    $('manualRoster').addEventListener('click',e=>{const b=e.target.closest('[data-counter-type]');if(b)showCounterGuide(b.dataset.counterType);});
    $('brand').addEventListener('click',e=>{e.preventDefault();openMainMenu();});
    $('settingsButton').addEventListener('click',()=>openDialog('settingsDialog'));
    $('scenariosButton').addEventListener('click',()=>openDialog('scenariosDialog'));
    all('[data-close]').forEach(b=>b.addEventListener('click',()=>closeDialog(b.dataset.close)));
    all('dialog').forEach(d=>d.addEventListener('click',e=>{if(e.target===d){const r=d.getBoundingClientRect();if(e.clientX<r.left||e.clientX>r.right||e.clientY<r.top||e.clientY>r.bottom)closeDialog(d.id);}}));
    $('qualitySelect').value=preferences.quality;
    $('qualitySelect').addEventListener('change',e=>{preferences.quality=e.target.value;renderer.options.quality=preferences.quality;savePreferences();});
    for(const key of ['corpseMode','animations']){
      $(key).value=preferences[key];
      $(key).addEventListener('change',e=>{const allowed=key==='corpseMode'?['brief','long','battle']:['full','reduced'];if(!allowed.includes(e.target.value))return;preferences[key]=e.target.value;renderer.options[key]=e.target.value;savePreferences();});
    }
    ['showHealth','showLabels','showOrders','showBodies'].forEach(key=>{$(key).checked=!!preferences[key];$(key).addEventListener('change',e=>{preferences[key]=e.target.checked;renderer.options[key]=e.target.checked;savePreferences();});});
    $('soundButton').addEventListener('click',()=>setSound(!preferences.sound));
    $('soundEnabled').checked=preferences.sound;$('soundEnabled').addEventListener('change',e=>setSound(e.target.checked));
    $('volumeSlider').value=preferences.volume;$('volumeSlider').addEventListener('input',e=>{preferences.volume=Number(e.target.value);audio.volume=preferences.volume/100;if(audio.master)audio.master.gain.value=audio.volume*.25;savePreferences();});
    setIcon($('soundButton'),preferences.sound?'sound':'mute');$('soundButton').setAttribute('aria-pressed',String(preferences.sound));
    $('seedInput').addEventListener('change',e=>{config.seed=clamp(Math.floor(Number(e.target.value)||142857),1,2147483647);e.target.value=config.seed;if(isDeployment())newBattle(config,{deployment,preserveCamera:true});else toast('New seed saved. Reset the battle to use it.');});
    $('fullscreenButton').addEventListener('click',async()=>{try{if(document.fullscreenElement)await document.exitFullscreen();else if(document.documentElement.requestFullscreen)await document.documentElement.requestFullscreen();else toast('Use your browser’s fullscreen control.');}catch(_){toast('Fullscreen is unavailable in this browser window.');}});
    $('saveScenario').addEventListener('click',()=>{
      const snapshot=scenarioSnapshot();let saved=safeRead(STORAGE,[]);saved=Array.isArray(saved)?saved:[];
      const index=saved.findIndex(s=>s.name===snapshot.name);if(index>=0)saved.splice(index,1);saved.unshift(snapshot);saved=saved.slice(0,24);
      if(safeWrite(STORAGE,saved)){renderSavedScenarios();toast('Scenario saved on this device.');}else toast('Browser storage is unavailable. Use Export JSON to save this battle.');
    });
    $('exportScenario').addEventListener('click',()=>{const snapshot=scenarioSnapshot();download(filename(snapshot.name)+'.ironfield.json',JSON.stringify(snapshot,null,2));});
    $('importScenario').addEventListener('click',()=>$('scenarioFile').click());
    $('scenarioFile').addEventListener('change',async e=>{
      const file=e.target.files[0];if(!file)return;
      try{if(file.size>8*1024*1024)throw new Error('Scenario files must be smaller than 8 MB.');loadScenario(JSON.parse(await file.text()));}catch(error){toast('Could not import this scenario. '+error.message,6000);}finally{e.target.value='';}
    });
    $('savedScenarios').addEventListener('click',e=>{
      const load=e.target.closest('[data-load-scenario]'),remove=e.target.closest('[data-delete-scenario]');const saved=safeRead(STORAGE,[]);if(!Array.isArray(saved))return;
      if(load){try{loadScenario(saved[Number(load.dataset.loadScenario)]);}catch(error){toast('Could not load this scenario. '+error.message);}}
      if(remove){saved.splice(Number(remove.dataset.deleteScenario),1);if(safeWrite(STORAGE,saved))renderSavedScenarios();else toast('Could not update browser storage.');}
    });
    $('rematchButton').addEventListener('click',()=>{closeDialog('resultsDialog');if(menuOpen)closeMainMenu(false);newBattle(config,{deployment});startBattle();});
    $('editAfterBattle').addEventListener('click',()=>{closeDialog('resultsDialog');if(menuOpen)closeMainMenu(false);resetBattle(true);});
    $('exportReport').addEventListener('click',()=>download(filename(config.name)+'-battle-report.json',JSON.stringify({format:'ironfield-battle-report',version:1,config,stats:engine.getStats(),troops:troopBreakdown(),events:engine.events},null,2)));
    window.addEventListener('resize',()=>{clearTimeout(resizeTimer);resizeTimer=setTimeout(resize,50);});
    if(window.ResizeObserver)new ResizeObserver(()=>resize()).observe($('stage'));
  }

  function fail(error){
    console.error('IRONFIELD:',error);
    const layer=document.createElement('div');layer.className='error-overlay';
    layer.innerHTML=`<div><span class="eyebrow">IRONFIELD / STARTUP</span><h2>The battlefield could not load.</h2><p>If you are using the source version, keep index.html and the src folder together. You can also open the bundled IRONFIELD.html directly.</p><code>${esc(error.message||String(error))}</code><button class="primary-button" id="retryGame">RELOAD THE GAME</button></div>`;
    document.body.appendChild(layer);$('retryGame').addEventListener('click',()=>location.reload());
  }
  function boot(){
    data=window.GameData;
    if(!data||!window.BattleTactics||!window.BattleImpacts||!window.BattleSiege||!window.BattleRamparts||!window.BattleEngine||!window.BattleRenderer)throw new Error('A required game source file is missing.');
    for(const id of ['doctrineSelect','plan0Select','plan1Select'])$(id).innerHTML=doctrineOptions();
    const mapFor=p=>p.customMap||data.MAPS.find(m=>m.id===p.map)||data.MAPS[0];
    const scenarioOptions=presets=>presets.map(p=>`<option value="${p.id}">${historicalInfo(p)?esc(historicalYear(historicalInfo(p).year)):esc(mapSizeLabel(mapFor(p)))} — ${esc(p.name)}</option>`).join('');
    $('presetSelect').innerHTML='<optgroup label="Historical battles">'+scenarioOptions(historicalBattles())+'</optgroup><optgroup label="IRONFIELD battlefields">'+scenarioOptions(data.PRESETS.filter(p=>!historicalInfo(p)).slice().sort((a,b)=>largestMapsFirst(mapFor(a),mapFor(b))))+'</optgroup><option value="custom">Custom encounter · empty field</option>';
    $('formationSelect').innerHTML=Object.values(data.FORMATIONS).map(f=>`<option value="${f.id}">${esc(f.name)}</option>`).join('')+'<option value="mixed" disabled>Mixed formations</option>';
    $('mapSelect').innerHTML=data.MAPS.filter(m=>m.id!=='custom').sort(largestMapsFirst).map(m=>`<option value="${m.id}">${esc(mapSizeLabel(m))} — ${esc(m.name)}</option>`).join('')+'<option value="custom">Custom Map · Choose battlefield size</option>';
    $('customWorldSize').innerHTML=data.WORLD_SIZES.map(s=>`<option value="${s.id}">${esc(mapSizeLabel(s))} · ${fmt(s.width)} × ${fmt(s.height)}</option>`).join('');
    $('fortressStyle').innerHTML=Object.values(data.FORTRESS_TYPES||{}).map(f=>`<option value="${f.id}">${esc(f.name)}</option>`).join('');
    $('fortressStyle').value='classic';
    loadPreset(data.PRESETS[0].id);
    bindControls();bindBattlefieldInput();bindMenu();
    window.IRONFIELD=Object.freeze({version:'2.7.0',get engine(){return engine;},get renderer(){return renderer;},get config(){return clone(config);},get menuOpen(){return menuOpen;},loadPreset,showResults});
    requestAnimationFrame(frame);
    if(window.innerWidth<=760)setBuilder(false);
  }
  try{boot();}catch(error){fail(error);}
})();
