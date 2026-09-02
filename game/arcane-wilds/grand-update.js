'use strict';

/*
 * Arcane Wilds — Grand Catalog & Performance Update
 * Adds a large wave of spells, enemies and weapons, an in-game catalog,
 * stronger cast feedback, and progressive cosmetic budgeting as levels rise.
 * Gameplay entities are never culled by the performance layer.
 */
(function(){
  if(window.__arcaneWildsGrandUpdateLoaded)return;
  window.__arcaneWildsGrandUpdateLoaded=true;

  const NEW_SPELLS={
    solarLance:{name:'Solar Lance',icon:'☀️',rarity:'Uncommon',desc:'Fire a brilliant piercing lance that rewards precise lines through crowded rooms.',cooldown:4.7,damage:44,cast:'icelance',upgradeBase:'icelance'},
    stormSpear:{name:'Storm Spear',icon:'🌩️',rarity:'Rare',desc:'Launch a charged spear of stormlight with heavy impact and piercing pressure.',cooldown:5.1,damage:52,cast:'icelance',upgradeBase:'icelance'},
    emberComet:{name:'Ember Comet',icon:'🌋',rarity:'Uncommon',desc:'Hurl a dense ember core that bursts into a wide fiery impact.',cooldown:3.9,damage:38,cast:'firebolt',upgradeBase:'firebolt'},
    glassWinter:{name:'Glass Winter',icon:'🧊',rarity:'Rare',desc:'Detonate a sharp winter pulse that locks down dangerous close-range swarms.',cooldown:5.0,damage:31,cast:'frostnova',upgradeBase:'frostnova'},
    briarCrown:{name:'Briar Crown',icon:'🌹',rarity:'Uncommon',desc:'Unleash a rotating crown of living briars that punishes enemies surrounding you.',cooldown:4.2,damage:26,cast:'thorns',upgradeBase:'thorns'},
    novaSwarm:{name:'Nova Swarm',icon:'✨',rarity:'Rare',desc:'Release a flock of fast seeking motes that rapidly acquire nearby targets.',cooldown:3.8,damage:18,cast:'missiles',upgradeBase:'arcaneMissiles'},
    cycloneWall:{name:'Cyclone Wall',icon:'🌬️',rarity:'Uncommon',desc:'Drive a broad wall of force forward to clear space and interrupt advancing enemies.',cooldown:4.0,damage:23,cast:'gust',upgradeBase:'gust'},
    mirrorAegis:{name:'Mirror Aegis',icon:'🪞',rarity:'Rare',desc:'Raise a bright arcane barrier built for aggressive repositioning under pressure.',cooldown:8.2,damage:0,cast:'ward',upgradeBase:'ward'},
    heavensCircuit:{name:'Heaven Circuit',icon:'⚡',rarity:'Epic',desc:'Overcharge the room with chained skyfire that jumps rapidly through clustered foes.',cooldown:8.8,damage:34,cast:'chain',upgradeBase:'chain'},
    rotBloom:{name:'Rot Bloom',icon:'🍄',rarity:'Uncommon',desc:'Plant an aggressive spore garden that controls ground and punishes stationary enemies.',cooldown:5.7,damage:16,cast:'poison',upgradeBase:'poison'},
    eclipseDisc:{name:'Eclipse Disc',icon:'🌘',rarity:'Rare',desc:'Throw a heavy lunar disc that carves through enemies on the way out and back.',cooldown:4.1,damage:29,cast:'chakram',upgradeBase:'chakram'},
    ancestorChoir:{name:'Ancestor Choir',icon:'👻',rarity:'Epic',desc:'Call a chorus of spirits that orbit you before hunting enemies across the room.',cooldown:8.6,damage:16,cast:'spirits',upgradeBase:'spirits'}
  };

  for(const [id,spell] of Object.entries(NEW_SPELLS)){
    const {upgradeBase,...data}=spell;
    SPELLS[id]=data;
    const source=UPGRADE_POOLS[upgradeBase]||[];
    UPGRADE_POOLS[id]=source.map((u,index)=>[
      `${id}_${index+1}`,
      u[1],
      u[2],
      u[3],
      u[4]
    ]);
  }

  const NEW_WEAPONS=[
    {name:'Ember Recurve',icon:'🏹',type:'bow',damage:1.02,attack:1.42,range:10.5,speed:13,color:'#ff9d5f'},
    {name:'Frostbite Longbow',icon:'❄️',type:'bow',damage:.95,attack:1.56,range:11.4,speed:14,color:'#9de9ff'},
    {name:'Dawnedge',icon:'🗡️',type:'blade',damage:1.52,attack:.72,range:6.7,speed:8.5,color:'#ffd777'},
    {name:'Rift Needle',icon:'🪡',type:'blade',damage:1.4,attack:.84,range:7.1,speed:9.2,color:'#c48dff'},
    {name:'Tempest Pike',icon:'🔱',type:'spear',damage:1.26,attack:.98,range:11.8,speed:15,color:'#79e7ff'},
    {name:'Sunbreaker Spear',icon:'☀️',type:'spear',damage:1.34,attack:.9,range:10.9,speed:13.5,color:'#ffcf63'},
    {name:'Starwheel',icon:'🌟',type:'chakram',damage:1.19,attack:1.09,range:9.7,speed:11,color:'#b8b0ff'},
    {name:'Moonrazor',icon:'🌘',type:'chakram',damage:1.28,attack:.97,range:9.4,speed:10.5,color:'#a77cff'},
    {name:'Worldroot Staff',icon:'🌿',type:'staff',damage:1.08,attack:1.08,range:9.2,speed:9.5,color:'#72d88a'},
    {name:'Graveglass Wand',icon:'💀',type:'staff',damage:1.18,attack:1.0,range:8.8,speed:10.4,color:'#c09aff'},
    {name:'Thunder Rod',icon:'⚡',type:'scepter',damage:1.0,attack:1.28,range:8.9,speed:11.3,color:'#73ecff'},
    {name:'Void Scepter',icon:'🕳️',type:'scepter',damage:1.14,attack:1.12,range:8.6,speed:10.8,color:'#bb6cff'}
  ];
  WEAPON_BASES.push(...NEW_WEAPONS);

  Object.assign(ENEMY_TYPES,{
    awx_thornhound:{name:'Thorn Hound',icon:'🐺',biomes:['forest','meadow'],min:2,hp:42,speed:2.18,damage:11,r:.32,ai:'melee',color:'#74a75d',xp:14},
    awx_sporeling:{name:'Sporeling',icon:'🍄',biomes:['swamp','forest'],min:3,hp:36,speed:1.55,damage:10,r:.3,ai:'orbiter',color:'#a5cb68',proj:'#d6f08c',xp:15},
    awx_sunscout:{name:'Sunscar Scout',icon:'🏹',biomes:['desert','meadow'],min:4,hp:46,speed:1.42,damage:14,r:.31,ai:'sniper',range:9.4,color:'#c99458',proj:'#ffd47c',xp:20},
    awx_frostguard:{name:'Frostguard',icon:'🛡️',biomes:['frost','ruins'],min:5,hp:92,speed:.9,damage:14,r:.43,ai:'shield',color:'#7098ad',xp:27},
    awx_graveflame:{name:'Graveflame Seer',icon:'🕯️',biomes:['crypt','ruins'],min:6,hp:66,speed:1.02,damage:16,r:.34,ai:'beam',range:8,color:'#8f5aa6',proj:'#e48cff',xp:31},
    awx_stormcaller:{name:'Stormcaller Adept',icon:'🌩️',biomes:['frost','ruins'],min:7,hp:72,speed:1.2,damage:15,r:.35,ai:'spread',range:7.8,color:'#5f8399',proj:'#8cecff',xp:34},
    awx_cinderhorn:{name:'Cinderhorn',icon:'🐂',biomes:['volcanic','desert'],min:8,hp:128,speed:1.2,damage:23,r:.5,ai:'charger',color:'#aa5842',xp:41},
    awx_riftstalker:{name:'Riftstalker',icon:'👁️',biomes:['crypt','ruins'],min:9,hp:82,speed:2.22,damage:20,r:.32,ai:'blinker',color:'#7952a0',xp:43},
    awx_dunereaver:{name:'Dune Reaver',icon:'🦂',biomes:['desert'],min:10,hp:90,speed:1.92,damage:18,r:.34,ai:'skirmish',range:6.4,color:'#b77c48',proj:'#f0b56d',xp:45},
    awx_mosscolossus:{name:'Moss Colossus',icon:'🌳',biomes:['forest','swamp'],min:11,hp:205,speed:.58,damage:25,r:.6,ai:'shockwave',color:'#547a55',xp:58},
    awx_skyrazor:{name:'Skyrazor',icon:'🦅',biomes:['frost','desert'],min:12,hp:84,speed:2.08,damage:19,r:.33,ai:'diver',color:'#7898a8',proj:'#b9efff',xp:49},
    awx_voidbinder:{name:'Voidbinder',icon:'🧿',biomes:['crypt','volcanic'],min:13,hp:108,speed:.92,damage:13,r:.4,ai:'summoner',color:'#68428c',proj:'#cb7cff',xp:57}
  });

  const NEW_ENEMY_IDS=Object.keys(ENEMY_TYPES).filter(id=>id.startsWith('awx_'));
  const NEW_SPELL_IDS=Object.keys(NEW_SPELLS);
  const NEW_WEAPON_NAMES=new Set(NEW_WEAPONS.map(w=>w.name));

  /* Make spell presses read immediately without increasing gameplay entity counts. */
  const baseCastSpell=castSpell;
  castSpell=function(slot){
    const id=game.player?.activeSpells?.[slot];
    const before=id?(game.player.spellState[id]?.cd||0):0;
    baseCastSpell(slot);
    if(!id)return;
    const after=game.player.spellState[id]?.cd||0;
    if(after<=before+.02)return;
    const spell=SPELLS[id],rank=rarityRank[spell.rarity]||0;
    shake=Math.max(shake,2.2+rank*.8);
    fx('castRing',game.player.x,game.player.y,.18,rarityColors[spell.rarity],{r:.58+rank*.12});
    burst(game.player.x,game.player.y,rarityColors[spell.rarity],6+rank*2,.7+rank*.08,12);
    const button=$('spells')?.children?.[slot];
    if(button){
      button.animate([
        {transform:'translateY(0) scale(1)',filter:'brightness(1)'},
        {transform:'translateY(-3px) scale(1.1)',filter:'brightness(1.65)'},
        {transform:'translateY(0) scale(1)',filter:'brightness(1)'}
      ],{duration:145,easing:'ease-out'});
    }
  };

  /* Tone down constant weapon trails and muzzle flashes before the adaptive governor. */
  const baseMagicProjectile=magicProjectile;
  magicProjectile=function(opts={}){
    if(opts.trail==='weapon'){
      const level=Math.max(1,game.level||1);
      const keepChance=clamp(.55-(level-1)*.008,.28,.55);
      if(Math.random()>keepChance)opts={...opts,trail:''};
    }
    return baseMagicProjectile(opts);
  };

  const baseFx=fx;
  fx=function(kind,x,y,life,color,extra={}){
    if(kind==='muzzle'){
      const level=Math.max(1,game.level||1);
      const keepChance=clamp(.62-(level-1)*.006,.35,.62);
      if(Math.random()>keepChance)return;
    }
    baseFx(kind,x,y,life,color,extra);
  };

  /* Every level trims only cosmetic particle budget a little more. */
  const baseUpdate=update;
  update=function(dt){
    baseUpdate(dt);
    const level=Math.max(1,game.level||1);
    const scale=clamp(1-(level-1)*.012,.7,1);
    const cap=Math.round((isTouch?56:170)*scale);
    if(game.particles.length>cap)game.particles.splice(0,game.particles.length-cap);
    const perf=window.arcaneWildsPerformance;
    if(perf){
      perf.levelScale=scale;
      perf.particleCap=Math.round((isTouch?64:185)*scale);
      perf.dprCap=isTouch?1:1.55;
    }
  };

  function injectCatalogStyles(){
    if(document.getElementById('awGrandCatalogStyles'))return;
    const style=document.createElement('style');
    style.id='awGrandCatalogStyles';
    style.textContent=`
      #catalogOverlay .panel{max-width:1120px;max-height:88vh;overflow:hidden;display:flex;flex-direction:column}
      .aw-catalog-top{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:10px 0 14px}
      .aw-catalog-tabs{display:flex;gap:7px;flex-wrap:wrap}
      .aw-catalog-tab,.aw-catalog-search{border:1px solid rgba(185,214,244,.18);background:rgba(8,15,24,.7);color:#e9f3ff;border-radius:12px;padding:10px 12px;font:inherit}
      .aw-catalog-tab{cursor:pointer;font-weight:800;letter-spacing:.04em}
      .aw-catalog-tab.active{border-color:rgba(139,215,255,.65);box-shadow:0 0 18px rgba(95,193,255,.15);background:rgba(35,74,101,.52)}
      .aw-catalog-search{min-width:240px;flex:1;outline:none}
      .aw-catalog-grid{overflow:auto;display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px;padding:2px 4px 8px 0}
      .aw-catalog-card{position:relative;min-height:155px;border:1px solid rgba(177,210,240,.16);background:linear-gradient(145deg,rgba(15,26,39,.94),rgba(7,13,21,.95));border-radius:16px;padding:14px;overflow:hidden;text-align:left;color:#eaf4ff}
      .aw-catalog-card.new{border-color:rgba(126,226,255,.42)}
      .aw-catalog-card.new:after{content:'NEW';position:absolute;right:10px;top:10px;font-size:8px;font-weight:900;letter-spacing:.16em;color:#9ceaff;border:1px solid rgba(113,223,255,.35);padding:4px 6px;border-radius:999px;background:rgba(20,66,82,.45)}
      .aw-catalog-visual{width:52px;height:52px;border-radius:15px;display:grid;place-items:center;font-size:30px;margin-bottom:10px;border:1px solid rgba(255,255,255,.12);background:radial-gradient(circle at 35% 30%,rgba(255,255,255,.18),rgba(255,255,255,.03) 55%,rgba(0,0,0,.2))}
      .aw-catalog-name{font-size:15px;font-weight:900;margin-bottom:3px;padding-right:42px}
      .aw-catalog-meta{font-size:9px;text-transform:uppercase;letter-spacing:.12em;color:#89a8c1;margin-bottom:8px}
      .aw-catalog-desc{font-size:11px;line-height:1.45;color:#bfd0df}
      .aw-catalog-stats{display:flex;gap:6px;flex-wrap:wrap;margin-top:10px}
      .aw-catalog-chip{font-size:9px;border:1px solid rgba(174,207,235,.14);border-radius:999px;padding:4px 7px;color:#dceaf6;background:rgba(255,255,255,.035)}
      .aw-catalog-summary{font-size:11px;color:#93abc1;margin-left:auto}
      #catalogBtn{font-size:18px}
      @media(max-width:760px){#catalogOverlay .panel{max-height:92vh}.aw-catalog-grid{grid-template-columns:1fr}.aw-catalog-search{min-width:100%}.aw-catalog-summary{width:100%;margin-left:0}}
    `;
    document.head.appendChild(style);
  }

  function ensureCatalogUI(){
    injectCatalogStyles();
    if(!document.getElementById('catalogOverlay')){
      const overlay=document.createElement('div');
      overlay.className='overlay hidden';
      overlay.id='catalogOverlay';
      overlay.innerHTML=`<div class="panel"><div class="overlay-head"><div><h2>Arcane Wilds Catalog</h2><p>Browse the full living bestiary, arsenal, and spell library. New Grand Update entries are marked NEW.</p></div><button class="btn secondary" id="catalogCloseBtn">Close</button></div><div class="aw-catalog-top"><div class="aw-catalog-tabs"><button class="aw-catalog-tab active" data-catalog-tab="spells">Spells</button><button class="aw-catalog-tab" data-catalog-tab="weapons">Weapons</button><button class="aw-catalog-tab" data-catalog-tab="enemies">Enemies</button></div><input class="aw-catalog-search" id="catalogSearch" placeholder="Search the catalog..." autocomplete="off"><span class="aw-catalog-summary" id="catalogSummary"></span></div><div class="aw-catalog-grid" id="catalogGrid"></div></div>`;
      document.body.appendChild(overlay);
      $('catalogCloseBtn').onclick=()=>closeOverlay('catalogOverlay');
      $('catalogSearch').addEventListener('input',renderCatalog);
      overlay.querySelectorAll('[data-catalog-tab]').forEach(btn=>btn.onclick=()=>{
        overlay.querySelectorAll('[data-catalog-tab]').forEach(b=>b.classList.toggle('active',b===btn));
        overlay.dataset.tab=btn.dataset.catalogTab;
        renderCatalog();
      });
      overlay.dataset.tab='spells';
    }

    if(!document.getElementById('catalogBtn')){
      const btn=document.createElement('button');
      btn.className='round-btn';
      btn.id='catalogBtn';
      btn.title='Arcane Wilds Catalog';
      btn.textContent='📚';
      btn.onclick=openCatalog;
      const buttons=$('buttons');
      if(buttons)buttons.insertBefore(btn,buttons.firstChild);
    }

    if(!document.getElementById('startCatalogBtn')){
      const btn=document.createElement('button');
      btn.className='btn secondary';
      btn.id='startCatalogBtn';
      btn.textContent='Browse Catalog';
      btn.onclick=openCatalog;
      const actions=document.querySelector('#startOverlay .menu-actions');
      if(actions)actions.appendChild(btn);
    }
  }

  function catalogEntries(tab){
    if(tab==='weapons')return WEAPON_BASES.map(w=>({
      key:w.name,name:w.name,icon:w.icon||'⚔️',newEntry:NEW_WEAPON_NAMES.has(w.name),meta:`${capitalize(w.type)} weapon`,
      desc:`A ${w.type} profile tuned for ${w.range>=10?'long-range pressure':w.attack>=1.2?'rapid attacks':'high-impact attacks'}.`,
      chips:[`Damage ×${w.damage.toFixed(2)}`,`Attack ${w.attack.toFixed(2)}`,`Range ${w.range.toFixed(1)}`,`Speed ${w.speed.toFixed(1)}`]
    }));
    if(tab==='enemies')return Object.entries(ENEMY_TYPES).map(([id,e])=>({
      key:id,name:e.name,icon:e.icon||'◆',newEntry:NEW_ENEMY_IDS.includes(id),meta:`Threat ${e.min}+ • ${capitalize(e.ai)}`,
      desc:`Found in ${e.biomes.map(b=>biomePalette[b]?.name||capitalize(b)).join(', ')}.`,
      chips:[`HP ${e.hp}`,`Damage ${e.damage}`,`Speed ${e.speed.toFixed(2)}`,`XP ${e.xp}`]
    }));
    return Object.entries(SPELLS).map(([id,s])=>({
      key:id,name:s.name,icon:s.icon||'✨',newEntry:NEW_SPELL_IDS.includes(id),meta:`${s.rarity} spell`,
      desc:s.desc,
      chips:[`Cooldown ${s.cooldown.toFixed(1)}s`,s.damage?`Power ${s.damage}`:'Utility',`${(UPGRADE_POOLS[id]||[]).length} mutations`]
    }));
  }

  function renderCatalog(){
    const overlay=$('catalogOverlay');if(!overlay)return;
    const tab=overlay.dataset.tab||'spells';
    const q=($('catalogSearch')?.value||'').trim().toLowerCase();
    const all=catalogEntries(tab),items=q?all.filter(i=>`${i.name} ${i.meta} ${i.desc} ${i.chips.join(' ')}`.toLowerCase().includes(q)):all;
    $('catalogSummary').textContent=`${items.length} / ${all.length}`;
    $('catalogGrid').innerHTML=items.map(item=>`<article class="aw-catalog-card ${item.newEntry?'new':''}"><div class="aw-catalog-visual">${item.icon}</div><div class="aw-catalog-name">${item.name}</div><div class="aw-catalog-meta">${item.meta}</div><div class="aw-catalog-desc">${item.desc}</div><div class="aw-catalog-stats">${item.chips.map(c=>`<span class="aw-catalog-chip">${c}</span>`).join('')}</div></article>`).join('')||'<div class="feature"><b>No matches</b><span>Try a different search.</span></div>';
  }

  function openCatalog(){
    ensureCatalogUI();
    renderCatalog();
    showOverlay('catalogOverlay');
  }

  ensureCatalogUI();
  window.openArcaneWildsCatalog=openCatalog;
  window.ARCANE_WILDS_GRAND_UPDATE={
    spellsAdded:NEW_SPELL_IDS.length,
    weaponsAdded:NEW_WEAPONS.length,
    enemiesAdded:NEW_ENEMY_IDS.length,
    version:'grand-update-1'
  };
})();
