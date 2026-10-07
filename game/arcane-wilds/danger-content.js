'use strict';
/* Seeded content and bounded journey state. The existing encrypted campaign
 * bundle remains the only save; no new storage key or independent economy. */
(() => {
  const D=AWCampaignData;
  const affixes={
    Blazing:{color:'#ffa05d',counter:'Frost extinguishes trails; fire is resisted.'},
    Stormbound:{color:'#98eeff',counter:'Spread your summons; sidestep lightning marks.'},
    Frostborn:{color:'#c0f4ff',counter:'Fire melts ice walls; leave freezing ground.'},
    Vampiric:{color:'#f78fb0',counter:'Dodge hits to deny healing; punish recovery.'},
    Berserker:{color:'#ff8d80',counter:'Save interrupts for the final quarter of health.'},
    Mirror:{color:'#d9b7ff',counter:'Copies your last offensive school with an enemy warning.'},
    Commander:{color:'#ffe091',counter:'Defeat the commander to break the squad formation.'},
    Necromancer:{color:'#ccbadf',counter:'Interrupt the visible resurrection ritual.'},
    Juggernaut:{color:'#d2c3ac',counter:'Strike its rear or break frontal armor with Earth.'},
    Teleporter:{color:'#cba1ff',counter:'Watch the shadow, then dodge the emerging strike.'},
    Predator:{color:'#ffbb86',counter:'Change direction; sidestep the marked pursuit lanes.'}
  };
  const modifiers={
    longWinter:{name:'Long Winter',desc:'Frozen crossings and frost foes spread beyond the snowfields.'},
    goblinUprising:{name:'Goblin Uprising',desc:'Organized raiders replace ordinary patrols; commanders protect archers.'},
    arcaneStorm:{name:'Arcane Storm',desc:'Warned magical strikes interrupt exposed battles.'},
    monsterMigration:{name:'Monster Migration',desc:'Pursuit beasts appear beyond their native regions.'},
    undeadRising:{name:'Undead Rising',desc:'Crypt squads replace patrols during the visible night cycle.'},
    ageOfPlenty:{name:'Age of Plenty',desc:'Resource sites yield more materials and offer optional raider defenses.'}
  };
  const pursuits={
    dangerHunter:{name:'Dash Hunter',hp:38,speed:2.7,damage:10,r:.29,ai:'dangerHunter',color:'#e6a471',min:3,xp:16,combatTags:['BEAST'],biomes:['forest','desert','ruins']},
    dangerDasher:{name:'Chain Dasher',hp:42,speed:2.5,damage:9,r:.3,ai:'dangerDasher',color:'#d3a2ee',min:6,xp:19,combatTags:['ARCANE'],biomes:['crystal','gloam']},
    dangerBeast:{name:'Pouncing Beast',hp:52,speed:2.3,damage:12,r:.43,ai:'dangerBeast',color:'#aecc8a',min:4,xp:19,combatTags:['BEAST'],biomes:['forest','swamp','frost']},
    dangerAssassin:{name:'Shadow Assassin',hp:35,speed:2.65,damage:11,r:.27,ai:'dangerAssassin',color:'#c0a0e4',min:7,xp:20,combatTags:['SHADOW','HUMANOID'],biomes:['gloam','crypt']},
    dangerRaider:{name:'Mounted Raider',hp:48,speed:2.8,damage:12,r:.38,ai:'dangerRaider',color:'#d8b477',min:5,xp:19,combatTags:['HUMANOID','ARMORED'],biomes:['desert','ruins']}
  };
  Object.assign(ENEMY_TYPES,pursuits);
  const puzzleKinds=['runes','mirrors','plates','elements','memory','timing','combat'];
  const puzzles=Object.values(D.nodes).filter(n=>n.type==='puzzle');
  puzzles.forEach((n,i)=>{n.dangerPuzzle=puzzleKinds[i%puzzleKinds.length];});
  // The dungeon relic branch also contains a genuine optional puzzle.
  const dungeonPuzzles={verdant:'plates',meridian:'mirrors',gloam:'memory'};
  const legendary=[
    {id:'frostTroll',name:'Ancient Frost Troll',biomes:['frost','crystal'],continent:'meridian',base:'golem',ai:'bossGolem',family:'frost',reward:'winterRelic'},
    {id:'wyvern',name:'Crimson Wyvern',biomes:['volcanic'],continent:'meridian',base:'drake',ai:'bossDrake',family:'fire',reward:'emberRelic'},
    {id:'colossus',name:'Arcane Colossus',biomes:['gloam','celestial'],continent:'gloam',base:'golem',ai:'bossVoid',family:'void',reward:'mirrorRelic'},
    {id:'roc',name:'Storm Roc',biomes:['stormlands','celestial'],continent:'gloam',base:'harpy',ai:'bossOracle',family:'storm',reward:'stormRelic'},
    {id:'sandWorm',name:'Elder Sand Worm',biomes:['desert','ruins'],continent:'verdant',base:'charger',ai:'bossGolem',family:'sand',reward:'stoneRelic'},
    {id:'guardian',name:'Forest Guardian',biomes:['forest','thornwild'],continent:'verdant',base:'golem',ai:'bossGolem',family:'nature',reward:'groveRelic'}
  ];
  const relics={
    runeRelic:['Rune Compass',{ward:.12,cdr:.06}],
    mirrorRelic:['Mirrorheart Sigil',{ward:.08,spell:.12}],
    winterRelic:['Winterbreak Talisman',{move:.08,ward:.1}],
    emberRelic:['Crimson Wing Charm',{spell:.14}],
    stormRelic:['Rocfeather Charm',{move:.12,cdr:.04}],
    stoneRelic:['Wormstone Seal',{ward:.2}],
    groveRelic:['Guardian Seed',{hp:.08,spell:.06}]
  };
  for(const [id,[name,mods]] of Object.entries(relics))D.items[id]={name,slot:'trinket',icon:'🔮',rarity:'Rare',mods,color:'#b7d5ff',price:0,desc:'Earned by solving an interactive challenge or defeating a legendary creature.',source:'worldDanger'};
  function hash(seed,text){let n=seed>>>0;for(const c of String(text))n=Math.imul(n^c.charCodeAt(0),16777619)>>>0;return n;}
  function shuffled(values,seed){const a=[...values];let n=seed>>>0;for(let i=a.length-1;i>0;i--){n=(Math.imul(n,1664525)+1013904223)>>>0;const j=n%(i+1);[a[i],a[j]]=[a[j],a[i]];}return a;}
  function fresh(seed){return {version:1,seed:seed>>>0,modifiers:shuffled(Object.keys(modifiers),hash(seed,'conditions')).slice(0,2),puzzles:{},claims:[],legends:{},problems:{},visits:0};}
  const num=(v,max)=>Number.isFinite(v)?Math.max(0,Math.min(max,v)):0;
  function normalize(raw,seed){
    const s=fresh(seed);if(!raw||raw.version!==1||raw.seed!==s.seed)return s;
    s.modifiers=[...new Set((Array.isArray(raw.modifiers)?raw.modifiers:[]).filter(k=>modifiers[k]))].slice(0,3);
    if(!s.modifiers.length)s.modifiers=fresh(seed).modifiers;
    s.visits=Math.floor(num(raw.visits,1000000));
    const keys=new Set(Object.values(D.nodes).filter(n=>!n.shadow).flatMap(n=>Array.from({length:n.roomCount},(_,i)=>n.id+':'+i)));
    for(const [key,p] of Object.entries(raw.puzzles||{}))if(keys.has(key)&&p&&puzzleKinds.includes(p.kind)){
      s.puzzles[key]={kind:p.kind,solved:!!p.solved,bonus:!!p.bonus,step:Math.floor(num(p.step,8)),mistakes:Math.floor(num(p.mistakes,999)),rotation:Array.from({length:3},(_,i)=>Number.isFinite(p.rotation?.[i])?Math.floor(num(p.rotation[i],3)):[3,2,1][i]),crates:(Array.isArray(p.crates)?p.crates:[]).slice(0,2).filter(v=>Number.isFinite(v?.x)&&Number.isFinite(v?.y)).map(v=>({x:num(v.x,17),y:num(v.y,13)})),seals:Array.from({length:3},(_,i)=>p.seals?.[i]===true)};
    }
    for(const [key,p] of Object.entries(raw.problems||{}))if(D.nodes[key]&&!D.nodes[key].shadow&&p&&['fire','mine','nest','seal','flood','caravan'].includes(p.kind))s.problems[key]={kind:p.kind,done:!!p.done,steps:(Array.isArray(p.steps)?p.steps:[]).slice(0,5).map(Boolean)};
    for(const [key,p] of Object.entries(raw.legends||{}))if(D.nodes[key]&&legendary.some(l=>l.id===p?.id))s.legends[key]={id:p.id,discovered:!!p.discovered,defeated:!!p.defeated};
    s.claims=[...new Set((Array.isArray(raw.claims)?raw.claims:[]).filter(v=>typeof v==='string'&&v.length<100))].slice(-700);
    return s;
  }
  const base=D.normalize;D.normalize=raw=>{const s=base(raw);s.danger=normalize(raw?.danger,game.seed);return s;};
  function puzzleFor(n,room){return n?.dangerPuzzle||n?.type==='dungeon'&&room===2&&dungeonPuzzles[n.continent]||null;}
  function legendsFor(seed){
    const used=new Set(),out={};
    for(const l of legendary){
      let sites=Object.values(D.nodes).filter(n=>n.continent===l.continent&&!n.shadow&&!n.homestead&&['landmark','treasure','resource','danger'].includes(n.type)&&!used.has(n.id)&&l.biomes.includes(n.biome));
      if(!sites.length)sites=Object.values(D.nodes).filter(n=>n.continent===l.continent&&!n.shadow&&['landmark','treasure','resource'].includes(n.type)&&!used.has(n.id));
      const n=sites[hash(seed,l.id)%sites.length];if(n){used.add(n.id);out[n.id]=l.id;}
    }return out;
  }
  window.AWDangerContent={affixes,modifiers,pursuits,puzzleKinds,puzzleFor,legendary,relics,hash,shuffled,fresh,normalize,legendsFor};
})();
