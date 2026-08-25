// Cheesehold sector economy + summoner progression expansion.
// Concatenated with cheesehold-elemental-progression.js and evaluated inside the core IIFE.

const SECTOR_GENERATOR_EFFICIENCY=[1,.60,.40,.28,.20];
const SECTOR_SAFE_ECONOMY=.25;
const SECTOR_BASE_REPRIEVE=15;
const SECTOR_SUMMON_TYPES=new Set(['tesla','turret','generator','superwall','phoenix','slow','poison']);
const SECTOR_SUMMON_MAJOR_IDS=new Set(['robotFoundry','mouseArmory','engineerWorkshop','gatehouse','emberNest','frostShrine','broodVat']);
const SECTOR_SUMMON_COLORS={sparkBot:0x93ecff,arcWalker:0x62d8ff,heavyMech:0xbceeff,shieldGuard:0xd8e1ef,spearGuard:0xf2df9c,repairBot:0xffd45a,phoenixling:0xff8a4f,greaterPhoenix:0xffc15d,rifleMouse:0xa9d9ff,marksman:0xe8f5ff,frostWisp:0xa8efff,frostGuardian:0x77dfff,venomCrawler:0x77df83,broodMother:0xb1ff75};

function sectorAddMajor(type,choice){ELEMENTAL_MILESTONES[type]??={major:[],ultra:[]};if(!ELEMENTAL_MILESTONES[type].major.some(x=>x.id===choice.id))ELEMENTAL_MILESTONES[type].major.push(choice);}
function sectorAddUltra(type,choice){ELEMENTAL_MILESTONES[type]??={major:[],ultra:[]};if(!ELEMENTAL_MILESTONES[type].ultra.some(x=>x.id===choice.id))ELEMENTAL_MILESTONES[type].ultra.push(choice);}

// Existing attack structures gain summoner alternatives at Level 10.
sectorAddMajor('tesla',{id:'robotFoundry',icon:'🤖',name:'Robot Foundry',desc:'Cuts direct lightning output to periodically construct fast Spark Bots.',mul:{damage:.62,fire:1.16,range:.92},element:'machine',summoner:true});
sectorAddUltra('tesla',{id:'arcAssembly',icon:'⚙',name:'Arc Assembly',desc:'Maintains more robots and adds ranged Arc Walkers.',mul:{damage:.86},element:'machine',requiresMajor:'robotFoundry',summoner:true});
sectorAddUltra('tesla',{id:'autonomousWarFactory',icon:'🦾',name:'Autonomous War Factory',desc:'Builds Arc Walkers and slow Heavy Mechs for brutal sector defense.',mul:{damage:.82},element:'machine',requiresMajor:'robotFoundry',summoner:true});
sectorAddMajor('turret',{id:'mouseArmory',icon:'🐭',name:'Mouse Armory',desc:'Trades much of the turret battery for mobile Rifle Mouse squads.',mul:{damage:.64,fire:1.15,range:.9},element:'militia',summoner:true});
sectorAddUltra('turret',{id:'rangerPost',icon:'⌖',name:'Ranger Post',desc:'Deploys Rifle Mice plus fragile long-range Marksmen.',mul:{damage:.9},element:'militia',requiresMajor:'mouseArmory',summoner:true});
sectorAddUltra('turret',{id:'mouseCommand',icon:'⚑',name:'Mouse Command',desc:'Fields a larger mixed squad and lets troops reinforce adjacent sectors.',mul:{damage:.82},element:'militia',requiresMajor:'mouseArmory',summoner:true});
sectorAddMajor('slow',{id:'frostShrine',icon:'❄',name:'Frost Shrine',desc:'Shrinks the static slow field to release mobile Frost Wisps.',mul:{slowRadius:.78,slowDuration:.92},element:'summonfrost',summoner:true});
sectorAddUltra('slow',{id:'winterCourt',icon:'♜',name:'Winter Court',desc:'Adds a Frost Guardian with a moving chill field.',mul:{slowRadius:.9},element:'summonfrost',requiresMajor:'frostShrine',summoner:true});
sectorAddUltra('slow',{id:'frozenLegion',icon:'✧',name:'Frozen Legion',desc:'Maintains a larger wisp screen and stronger Frost Guardians.',mul:{slowRadius:.82},element:'summonfrost',requiresMajor:'frostShrine',summoner:true});
sectorAddMajor('poison',{id:'broodVat',icon:'☣',name:'Brood Vat',desc:'Reduces tower poison output to breed capped Venom Crawlers.',mul:{poisonDamage:.68,range:.86},element:'brood',summoner:true});
sectorAddUltra('poison',{id:'plagueColony',icon:'◌',name:'Plague Colony',desc:'Produces a larger crawler pack with stronger poison.',mul:{poisonDamage:.82},element:'brood',requiresMajor:'broodVat',summoner:true});
sectorAddUltra('poison',{id:'hiveMind',icon:'♛',name:'Hive Mind',desc:'Adds a Brood Mother without allowing recursive uncapped spawning.',mul:{poisonDamage:.78},element:'brood',requiresMajor:'broodVat',summoner:true});

// Non-attack structures now receive Level 10/15 late-game paths too.
for(const type of ['generator','superwall','phoenix'])ELEMENTAL_ATTACK_TYPES.add(type);
Object.assign(ELEMENTAL_MILESTONES,{
 generator:{major:[
  {id:'industrialMint',icon:'♦',name:'Industrial Mint',desc:'A late economic specialization: larger yield, faster press.',mul:{genRate:.82},add:{yield:2},element:'economy'},
  {id:'engineerWorkshop',icon:'⚒',name:'Engineer Workshop',desc:'Sacrifices most cheese output to deploy autonomous Repair Bots.',mul:{hp:1.3,genRate:1.15},element:'machine',summoner:true},
  {id:'fortressForge',icon:'✚',name:'Fortress Forge',desc:'Becomes a durable local repair station instead of pure income.',mul:{hp:1.65},add:{repairAura:3},element:'support'}
 ],ultra:[
  {id:'crownMint',icon:'♛',name:'Crown Mint',desc:'Extreme yield per cycle, still subject to sector saturation.',mul:{genRate:.82},add:{yield:4},element:'economy',requiresMajor:'industrialMint'},
  {id:'compoundPress',icon:'≋',name:'Compound Press',desc:'Faster cycles with a smaller yield increase.',mul:{genRate:.58},add:{yield:2},element:'economy',requiresMajor:'industrialMint'},
  {id:'constructionYard',icon:'▦',name:'Construction Yard',desc:'Fields two Repair Bots; they may rebuild destroyed walls at a cheese cost.',mul:{hp:1.25},element:'machine',requiresMajor:'engineerWorkshop',summoner:true},
  {id:'mobileFoundry',icon:'⚙',name:'Mobile Foundry',desc:'Faster Repair Bot replacement and stronger repairs, but no autonomous new-wall construction.',mul:{hp:1.18},element:'machine',requiresMajor:'engineerWorkshop',summoner:true},
  {id:'bastionWorks',icon:'▣',name:'Bastion Works',desc:'Massive local repair aura and reinforced frame.',mul:{hp:1.5},add:{repairAura:4},element:'support',requiresMajor:'fortressForge'},
  {id:'ironworks',icon:'◇',name:'Ironworks',desc:'Turns the generator frame into a near-fortress with steady support repair.',mul:{hp:2},add:{repairAura:2},element:'support',requiresMajor:'fortressForge'}
 ]},
 superwall:{major:[
  {id:'worldwallCore',icon:'█',name:'Worldwall Core',desc:'Pure late-game durability with enormous health scaling.',mul:{hp:2.25},element:'fortress'},
  {id:'gatehouse',icon:'⚑',name:'Gatehouse',desc:'Trades some wall health to deploy Shield Guards into this sector.',mul:{hp:.78},element:'militia',summoner:true},
  {id:'masonCitadel',icon:'⚒',name:'Mason Citadel',desc:'A giant support wall that repairs surrounding structures.',mul:{hp:1.45},add:{repairAura:3.4},element:'support'}
 ],ultra:[
  {id:'eternalRampart',icon:'▰',name:'Eternal Rampart',desc:'Nearly immovable wall mass.',mul:{hp:2.1},element:'fortress',requiresMajor:'worldwallCore'},
  {id:'siegeMirrorPrime',icon:'↯',name:'Siege Mirror Prime',desc:'Huge health plus vicious retaliation.',mul:{hp:1.55},add:{retaliation:28},element:'fortress',requiresMajor:'worldwallCore'},
  {id:'fortressBarracks',icon:'♜',name:'Fortress Barracks',desc:'Fields Shield Guards and spear defenders from the gate.',mul:{hp:1.12},element:'militia',requiresMajor:'gatehouse',summoner:true},
  {id:'guardianGate',icon:'🛡',name:'Guardian Gate',desc:'Fewer but much tougher guards hold breaches near the wall.',mul:{hp:1.35},element:'militia',requiresMajor:'gatehouse',summoner:true},
  {id:'livingCitadel',icon:'✚',name:'Living Citadel',desc:'Extreme repair aura and additional wall health.',mul:{hp:1.65},add:{repairAura:5},element:'support',requiresMajor:'masonCitadel'}
 ]},
 phoenix:{major:[
  {id:'secondDawn',icon:'☀',name:'Second Dawn',desc:'Strengthens the Phoenix structure as a personal resurrection anchor.',mul:{hp:1.55},element:'rebirth'},
  {id:'emberNest',icon:'🔥',name:'Ember Nest',desc:'Becomes a roost that releases attacking Phoenixlings.',mul:{hp:1.18},element:'phoenix',summoner:true},
  {id:'hearthBeacon',icon:'✚',name:'Hearth Beacon',desc:'A support shrine that repairs nearby structures and protects the sector.',mul:{hp:1.4},add:{repairAura:2.8},element:'support'}
 ],ultra:[
  {id:'immortalDawn',icon:'✹',name:'Immortal Dawn',desc:'Massively reinforces the resurrection anchor.',mul:{hp:1.8},element:'rebirth',requiresMajor:'secondDawn'},
  {id:'eternalAviary',icon:'🪽',name:'Eternal Aviary',desc:'Maintains a flock and periodically fields a Greater Phoenix.',mul:{hp:1.18},element:'phoenix',requiresMajor:'emberNest',summoner:true},
  {id:'ashenCycle',icon:'♻',name:'Ashen Cycle',desc:'Phoenixlings are cheaper to replace and survive longer.',mul:{hp:1.15},element:'phoenix',requiresMajor:'emberNest',summoner:true},
  {id:'sunSanctuary',icon:'◇',name:'Sun Sanctuary',desc:'Huge repair beacon with a fortified shrine body.',mul:{hp:1.55},add:{repairAura:4.5},element:'support',requiresMajor:'hearthBeacon'}
 ]}
});

