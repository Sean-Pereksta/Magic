'use strict';
/* Campaign content and pure progression rules. No rendering or combat dependencies. */
(() => {
  const continents = [
    {id:'verdant',name:'The Verdant Reach',description:'Lantern roads, ancient forests and the roots of a sleeping king.',color:'#a9db85',range:[1,8],biomes:['meadow','forest','thornwild','swamp','ruins'],material:'hide',ruler:'The Thornheart Sovereign',base:'rootjuggernaut',ai:'bossNecro',reward:'heartwood',mount:'elk',spell:'groveGuardian',travel:'Board the dawnship for the Shattered Meridian'},
    {id:'meridian',name:'The Shattered Meridian',description:'Crystal cities endure beneath a sky split by storms.',color:'#80ddeb',range:[9,17],biomes:['desert','crystal','stormlands','frost','volcanic'],material:'frost',ruler:'Vaelith, Tempest Dragon',base:'drake',ai:'bossDrake',reward:'stormcaller',mount:'stormstag',spell:'stormBridge',travel:'Restore the ancient gate to the Astral Gloam'},
    {id:'gloam',name:'The Astral Gloam',description:'Sanctuaries of starlight on the edge of the Watching Dark.',color:'#d5a5ff',range:[18,27],biomes:['gloam','celestial','crypt','bloodroot','volcanic'],material:'bone',ruler:'The Watching Dark',base:'voideye',ai:'bossVoid',reward:'darkcrown',mount:'arcanebeast',spell:'mirrorBastion',travel:'The three continents are free. Explore the roads you left behind.'}
  ];
  const towns = {
    sunmere:{name:'Sunmere Haven',culture:'Lanternfolk',theme:'meadow',roles:['Merchant','Blacksmith','Alchemist','Spell Scribe','Stablemaster','Quest Keeper','Cartographer'],stock:['tonic','iron','starterstaff'],spells:['ward','runicNeedle'],mounts:['horse'],quest:'verdant-landmark',line:'Follow the lantern road. Briarwatch tends living wood; our forge works honest iron.'},
    briarwatch:{name:'Briarwatch',culture:'Grovekeepers',theme:'thornwild',roles:['Weaponsmith','Alchemist','Spell Scribe','Quest Keeper'],stock:['hide','thornstaff','hidearmor','naturecharm'],spells:['poison','spirits','groveGuardian'],mounts:[],quest:'verdant-dungeon',line:'The Heartwood grows for those who protect it. No city smith can work these living branches.'},
    greenharbor:{name:'Greenharbor',culture:'Dawnship Mariners',theme:'forest',roles:['Merchant','Stablemaster','Quest Keeper'],stock:['tonic','hide'],spells:[],mounts:['wolf'],quest:'verdant-boss2',line:'Our dawnship cannot sail until the Sovereign releases the sea winds.'},
    prismhold:{name:'Prismhold',culture:'Glasswrights',theme:'crystal',roles:['Arcanist','Enchanter','Relic Dealer','Cartographer','Quest Keeper'],stock:['dust','frost','prismwand','prismcrown','mirrorsigil'],spells:['icelance','timestop'],mounts:[],quest:'meridian-dungeon',line:'Clear the Crystal Vault and our Mirror Sigil is yours to purchase. Precision gives crystal its power.'},
    stormrest:{name:'Stormrest',culture:'Stormguard',theme:'stormlands',roles:['Weaponsmith','Armorer','Stablemaster','Quest Keeper'],stock:['iron','stormglass','stormcaller','stormboots'],spells:['chain','stormBridge'],mounts:['stormstag'],quest:'meridian-boss2',line:'Keep your feet moving. Our lightning rods have outlasted a hundred tempests.'},
    emberfall:{name:'Emberfall',culture:'Cinderforged',theme:'volcanic',roles:['Blacksmith','Armorer','Alchemist','Quest Keeper'],stock:['ember','firestaff','cinderplate','embercharm'],spells:['meteor','celestialFurnace'],mounts:[],quest:'meridian-boss4',line:'Only this furnace can bind Emberglass into Cinderplate. Bring ore and courage.'},
    astral:{name:'Astral Sanctuary',culture:'Starbound',theme:'celestial',roles:['Arcanist','Spell Scribe','Enchanter','Relic Dealer','Stablemaster','Quest Keeper'],stock:['dust','astralcodex','starrobe'],spells:['seraphicArray','eventideGate','mirrorBastion'],mounts:['arcanebeast'],quest:'gloam-dungeon',line:'Four seals hold the cathedral closed. Each fallen guardian returns a star to our sky.'},
    bonehaven:{name:'Bonehaven',culture:'Gravewardens',theme:'crypt',roles:['Armorer','Relic Dealer','Alchemist','Quest Keeper'],stock:['bone','boneward','tonic'],spells:['soulflame'],mounts:[],quest:'gloam-boss3',line:'Morrow still sits on his stolen throne. Lay him to rest, and the road will remember.'},
    redwatch:{name:'Redwatch Refuge',culture:'Ash Rangers',theme:'bloodroot',roles:['Weaponsmith','Stablemaster','Quest Keeper'],stock:['hide','ember','bloodfang'],spells:['voidrift'],mounts:['wolf'],quest:'gloam-boss4',line:'Watch the roots. The red forest follows travelers who walk alone.'}
  };
  const mounts = {
    horse:{name:'Roadrunner Horse',icon:'🐎',color:'#bb9475',speed:1.42,cost:110,desc:'A sure-footed companion for the first long road.'},
    elk:{name:'Great Elk',icon:'🦌',color:'#b7ca88',speed:1.52,cost:0,terrain:['forest','thornwild'],desc:'Reward for freeing the Verdant Reach; swiftest in the green lands.'},
    wolf:{name:'Dire Wolf',icon:'🐺',color:'#bbc7d3',speed:1.56,cost:320,desc:'A quick, low-running hunter from the coast and red woods.'},
    stormstag:{name:'Storm Stag',icon:'🦌',color:'#90eaff',speed:1.65,cost:700,terrain:['stormlands','frost'],desc:'Ignores the biting wind of the Meridian.'},
    arcanebeast:{name:'Arcane Beast',icon:'✦',color:'#dbb0ff',speed:1.75,cost:1400,terrain:['gloam','celestial'],desc:'A starlit traveler, at home on the last continent.'}
  };
  const items = {
    tonic:{name:'Healing Tonic',kind:'heal',price:22,desc:'Restore 50% maximum health.'},
    iron:{name:'Iron Shard bundle',kind:'material',material:'iron',price:35,count:5},
    hide:{name:'Beast Hide bundle',kind:'material',material:'hide',price:40,count:5},
    dust:{name:'Arcane Dust bundle',kind:'material',material:'dust',price:55,count:5},
    frost:{name:'Frost Crystal bundle',kind:'material',material:'frost',price:65,count:5},
    ember:{name:'Emberglass bundle',kind:'material',material:'ember',price:70,count:5},
    bone:{name:'Gravebone bundle',kind:'material',material:'bone',price:65,count:5},
    starterstaff:{name:'Sunmere Staff',slot:'weapon',rarity:'Uncommon',price:95,type:'staff',damage:1.08,attack:1.05,range:8,speed:10,color:'#e6cb88',desc:'A dependable staff from the lantern market.'},
    thornstaff:{name:'Thorn Staff',slot:'weapon',rarity:'Rare',price:230,type:'staff',damage:1.2,attack:1,range:9,speed:10,color:'#a6d786',suffix:'thorns',desc:'Living wood reflects a portion of incoming damage.'},
    hidearmor:{name:'Beast Hide Armor',slot:'armor',rarity:'Rare',price:210,hp:1.16,move:1.06,armor:.065,color:'#766448',trim:'#c4d091',helm:'hood',desc:'Flexible armor for the forest roads.'},
    naturecharm:{name:'Grovekeeper Knot',slot:'trinket',rarity:'Rare',price:260,mods:{spell:.10,ward:.12},special:'groveKin',desc:'Summons deal 20% more damage; stronger wards.'},
    heartwood:{name:'Heartwood Staff',slot:'weapon',rarity:'Legendary',type:'staff',damage:1.4,attack:1.08,range:10,speed:11,color:'#c4f49b',spellSlotBonus:1,suffix:'mending',desc:'The Sovereign’s living heart; +1 active spell slot and healing on kills.'},
    prismwand:{name:'Prism Wand',slot:'weapon',rarity:'Rare',price:460,type:'spear',damage:1.2,attack:1.22,range:11,speed:14,color:'#a0f4ff',desc:'Crystal needles pierce through enemy lines.'},
    prismcrown:{name:'Prism Crown',slot:'trinket',rarity:'Epic',price:590,mods:{cdr:.10,spell:.12},desc:'Crystal facets accelerate spell recovery.'},
    mirrorsigil:{name:'Mirror Sigil',slot:'trinket',rarity:'Epic',price:650,requires:'meridian-dungeon',mods:{ward:.18,spell:.10},spellSlotBonus:1,desc:'Vault-cleared commission. +1 active spell slot.'},
    stormglass:{name:'Stormglass Shard',slot:'trinket',rarity:'Rare',price:320,mods:{cdr:.07,move:.06},suffix:'storm',desc:'Weapon hits can arc lightning to a second foe.'},
    stormcaller:{name:'Stormcaller Ring',slot:'trinket',rarity:'Legendary',price:850,mods:{spell:.16,cdr:.09},suffix:'storm',desc:'The storm fortress’s signature lightning focus.'},
    stormboots:{name:'Stormguard Mantle',slot:'armor',rarity:'Epic',price:580,hp:1.1,move:1.14,armor:.08,color:'#435b6c',trim:'#91e9ff',helm:'hood',special:'stormShelter',desc:'Quick movement; reduces damage from stormland enemies by 15%.'},
    firestaff:{name:'Emberglass Scepter',slot:'weapon',rarity:'Epic',price:570,type:'scepter',damage:1.32,attack:1.12,range:10,speed:11,color:'#ffad72',suffix:'embers',desc:'Weapon hits can ignite enemies.'},
    cinderplate:{name:'Cinderplate',slot:'armor',rarity:'Legendary',price:890,cost:{ember:8,iron:6},hp:1.3,move:.98,armor:.14,color:'#403c3f',trim:'#ff9b59',helm:'crown',special:'cinderShelter',desc:'Emberfall forge commission; reduces volcanic damage by 20%.'},
    embercharm:{name:'Furnace Heart',slot:'trinket',rarity:'Epic',price:480,mods:{spell:.14,hp:.06},suffix:'embers',desc:'A burning focus from the Emberfall forge.'},
    astralcodex:{name:'Astral Codex',slot:'trinket',rarity:'Legendary',price:1800,cost:{dust:15},mods:{spell:.18,cdr:.07},spellSlotBonus:2,desc:'Starbound craft unlocks the fourth and fifth active spell slots.'},
    starrobe:{name:'Starbound Vestments',slot:'armor',rarity:'Legendary',price:1250,hp:1.22,move:1.08,armor:.09,color:'#554773',trim:'#fff0bb',helm:'crown',spellSlotBonus:1,desc:'A woven constellation grants +1 active spell slot.'},
    boneward:{name:'Gravewarden Seal',slot:'trinket',rarity:'Epic',price:700,mods:{hp:.15,armor:.03},suffix:'warding',desc:'A protective relic from Bonehaven.'},
    bloodfang:{name:'Bloodroot Fang',slot:'weapon',rarity:'Legendary',price:1100,type:'daggers',damage:1.05,attack:1.6,range:8,speed:14,color:'#e78794',suffix:'mending',desc:'The Ash Rangers’ blades restore health on kills.'},
    darkcrown:{name:'Crown of the Watching Dark',slot:'trinket',rarity:'Legendary',mods:{spell:.22,cdr:.12,hp:.10},spellSlotBonus:2,suffix:'warding',desc:'The final ruler’s crown channels five spells and shields its bearer.'}
  };
  const nodes={};
  const add=(c,key,name,type,biome,x,y,extra={})=>{
    const id=`${c.id}-${key}`;
    nodes[id]={id,continent:c.id,name,type,biome,x,y,connections:[],threat:c.range[0],...extra};return nodes[id];
  };
  const link=(c,a,b)=>{const x=nodes[`${c.id}-${a}`],y=nodes[`${c.id}-${b}`];x.connections.push(y.id);y.connections.push(x.id);};
  const names=[
    ['Sunmere Haven','Lantern Road','Whisperwood','Briarwatch','Rosefang Thicket','Briar Queen','Kingfall Ruins','Stonebell Colossus','Mire of Lanterns','Mire Sovereign','Greenharbor','Oldroot Caverns','Ancient Wyrm','Heartwood Hollow','Moonwell Shrine','The First Standing Stone','Elk Glade','Lost Lantern Caravan','Coastal Watch','Dawnship Pier','Thornheart Sanctum'],
    ['Prismhold','Sunscar Pass','Shiverglass Basin','Stormrest','Thundergrass Reach','Storm Knight Regent','Frozen Causeway','Rimeglass Matriarch','Ashen Rift','Vaelith the Cinder Sky','Emberfall','The Crystal Vault','Shard Crown Titan','Tempest Citadel','Skyglass Shrine','The Broken Orrery','Storm Stag Crag','Arcane Storm','Sundial Barrens','Ancient Arcane Gate','Eye of the Tempest'],
    ['Astral Sanctuary','Nightglass Road','Constellation Fields','Bonehaven','Moonless Graves','Morrow, Bone Regent','Bloodroot Wilds','Crimson Matriarch','Starless Chasm','Abyssal Executioner','Redwatch Refuge','The Sunken Observatory','The Last Sun Warden','Void Cathedral Approach','Starfall Shrine','The Forgotten Meridian','Astral Menagerie','Wandering Rift','Ashes of Heaven','Crownwatch Overlook','Cathedral of the Watching Dark']
  ];
  const settlements=[['sunmere','briarwatch','greenharbor'],['prismhold','stormrest','emberfall'],['astral','bonehaven','redwatch']];
  const bosses=[
    [['bloomhexer','bossNecro'],['golem','bossGolem'],['necro','bossNecro'],['drake','bossDrake']],
    [['stormknight','bossGolem'],['frostwitch','bossNecro'],['drake','bossDrake'],['shardram','bossGolem']],
    [['necro','bossNecro'],['crimsonwitch','bossNecro'],['executioner','bossGolem'],['sunwarden','bossVoid']]
  ];
  continents.forEach((c,i)=>{
    const b=c.biomes,n=names[i],s=settlements[i];
    const layout=[
      ['city','portalCity',b[0],105,340,{town:s[0]}],['road','wildland',b[0],260,340,{}],
      ['wood','wildland',b[1],410,220,{}],['town','town',towns[s[1]].theme,560,140,{town:s[1]}],
      ['danger','danger',b[2],720,210,{}],['boss1','boss',b[2],875,125,{boss:bosses[i][0]}],
      ['ruins','wildland',i===0?'ruins':b[3],430,415,{}],['boss2','boss',i===0?'ruins':b[3],625,385,{boss:bosses[i][1]}],
      ['mire','wildland',i===0?'swamp':b[4],410,620,{}],['boss3','boss',i===0?'swamp':b[4],615,690,{boss:bosses[i][2]}],
      ['village','village',towns[s[2]].theme,800,560,{town:s[2]}],
      ['dungeon','dungeon',i===0?'forest':i===1?'crystal':'celestial',770,370,{rooms:5}],
      ['boss4','boss',b[1],990,335,{boss:bosses[i][3]}],['approach','danger',b[2],1000,560,{}],
      ['shrine','shrine',b[1],565,40,{spell:['groveGuardian','stormBridge','mirrorBastion'][i]}],
      ['landmark','landmark',b[0],230,115,{}],['mount','mount',b[1],570,540,{mount:['elk','stormstag','arcanebeast'][i]}],
      ['event','event',b[0],230,575,{}],['camp','merchant',b[4],900,735,{}],
      ['passage','passage',b[1],1100,680,{}],['ruler','ruler',b[2],1110,80,{boss:[c.base,c.ai],reward:c.reward}]
    ];
    layout.forEach(([key,type,biome,x,y,extra],idx)=>add(c,key,n[idx],type,biome,x,y,{threat:c.range[0]+Math.min(c.range[1]-c.range[0],Math.floor(idx/3)),...extra}));
    for(const [a,b] of [['city','road'],['road','wood'],['road','ruins'],['road','mire'],['road','landmark'],['road','event'],['wood','town'],['town','danger'],['town','shrine'],['danger','boss1'],['ruins','boss2'],['ruins','dungeon'],['mire','boss3'],['mire','mount'],['mount','village'],['dungeon','boss4'],['dungeon','village'],['village','approach'],['village','camp'],['approach','passage'],['city','ruler']])link(c,a,b);
    c.start=`${c.id}-city`;c.portalCity=c.start;c.boss=`${c.id}-ruler`;c.requiredBosses=[1,2,3,4].map(j=>`${c.id}-boss${j}`);
    // Portal ruler is never an ordinary road destination; its only entrance is in the city.
    c.nodes=Object.values(nodes).filter(n=>n.continent===c.id).map(n=>n.id);
  });
  const dungeonLinks=[{N:1},{S:0,W:2,E:3},{E:1,N:4},{W:1,N:4},{S:2,E:3}];
  Object.values(nodes).forEach((n,index)=>{n.index=index;n.roomCount=n.rooms||1;});
  const continent=id=>continents.find(c=>c.id===id);
  const isTown=n=>!!n?.town;
  const fresh=()=>({version:1,current:'verdant-city',room:0,visited:['verdant-city'],scouted:[],cleared:[],defeated:[],unlocked:['verdant'],checkpoint:'verdant-city',mounts:[],activeMount:null,riding:false,claimed:[],celebrated:[],dungeonClears:{},questClaims:[],rewards:[]});
  const normalize=raw=>{
    const s=fresh();if(!raw||typeof raw!=='object')return s;
    for(const key of ['visited','scouted','cleared','defeated','claimed','questClaims','celebrated'])s[key]=[...new Set((Array.isArray(raw[key])?raw[key]:[]).filter(id=>nodes[id]))];
    s.visited=[...new Set(['verdant-city',...s.visited])];
    s.defeated=s.defeated.filter(id=>['boss','ruler'].includes(nodes[id].type));
    s.unlocked=['verdant'];for(let i=1;i<3;i++)if(s.unlocked.includes(continents[i-1].id)&&s.defeated.includes(continents[i-1].boss))s.unlocked.push(continents[i].id);
    s.current=nodes[raw.current]&&s.unlocked.includes(nodes[raw.current].continent)&&s.visited.includes(raw.current)?raw.current:s.current;
    s.room=Math.min(nodes[s.current].roomCount-1,Math.max(0,Math.floor(Number(raw.room)||0)));
    s.checkpoint=isTown(nodes[raw.checkpoint])&&s.visited.includes(raw.checkpoint)&&s.unlocked.includes(nodes[raw.checkpoint].continent)?raw.checkpoint:s.checkpoint;
    s.mounts=[...new Set((Array.isArray(raw.mounts)?raw.mounts:[]).filter(id=>mounts[id]))];
    s.activeMount=s.mounts.includes(raw.activeMount)?raw.activeMount:s.mounts[0]||null;s.riding=!!raw.riding&&!!s.activeMount;
    for(const [id,rooms] of Object.entries(raw.dungeonClears||{}))if(nodes[id]?.type==='dungeon'&&Array.isArray(rooms))s.dungeonClears[id]=[...new Set(rooms.filter(r=>Number.isInteger(r)&&r>=0&&r<nodes[id].roomCount))];
    s.rewards=(Array.isArray(raw.rewards)?raw.rewards:[]).filter(id=>items[id]).slice(0,10);
    return s;
  };
  const sealCount=(s,c)=>c.requiredBosses.filter(id=>s.defeated.includes(id)).length;
  const portalReady=(s,c)=>sealCount(s,c)===c.requiredBosses.length;
  const visible=(s,n)=>s.visited.includes(n.id)||s.scouted.includes(n.id)||n.connections.some(id=>s.visited.includes(id));
  const travelReason=(s,id,mode='road')=>{
    const to=nodes[id],from=nodes[s.current];if(!to||!from)return 'Unknown destination.';
    if(id===s.current)return 'You are here.';
    if(!s.unlocked.includes(to.continent))return 'Defeat the previous continent ruler first.';
    if(to.type==='ruler')return 'Enter the great portal inside the Portal City.';
    if(mode==='portal')return isTown(from)&&isTown(to)&&s.visited.includes(id)?'':'Town Portals connect settlements you have visited.';
    if(mode==='passage')return from.type==='passage'&&to.id===continent(to.continent).start&&Math.abs(continents.indexOf(continent(from.continent))-continents.indexOf(continent(to.continent)))===1?'':'Use an unlocked ship or Arcane gate.';
    if(!from.connections.includes(id))return 'Discover the world one connected location at a time.';
    if(!isTown(from)&&!s.cleared.includes(from.id))return 'Clear this location before continuing.';
    return '';
  };
  window.AWCampaignData={continents,nodes,towns,mounts,items,dungeonLinks,continent,isTown,fresh,normalize,sealCount,portalReady,visible,travelReason};
})();
