export const RESOURCES = ['food', 'wood', 'stone', 'iron', 'gold', 'horses', 'tools', 'arms'];
export const RESOURCE_ICONS = { food: '◈', wood: '♣', stone: '◆', iron: '⚒', gold: '●', horses: '♞', tools: '⚙', arms: '⚔' };
export const HOUSES = [
  { id: 'ashen', name: 'House Ashen', ruler: 'The Crown Regent', color: '#dfa94f', sigil: '♛', motto: 'From embers, an empire.', aggression: .45, honor: .75, greed: .5, ambition: .7, paranoia: .4 },
  { id: 'wintermere', name: 'House Wintermere', ruler: 'Queen Ysella', color: '#90bfd7', sigil: '❄', motto: 'An oath outlasts the winter.', aggression: .25, honor: .95, greed: .25, ambition: .4, paranoia: .6 },
  { id: 'thornwall', name: 'House Thornwall', ruler: 'Lord Cassian', color: '#89b778', sigil: '♜', motto: 'Let them break against us.', aggression: .55, honor: .7, greed: .5, ambition: .65, paranoia: .8 },
  { id: 'sunspire', name: 'House Sunspire', ruler: 'Prince Dorian', color: '#dd9571', sigil: '☀', motto: 'Every crown has its price.', aggression: .35, honor: .45, greed: .95, ambition: .8, paranoia: .4 },
  { id: 'vesper', name: 'House Vesper', ruler: 'Duchess Nyra', color: '#b29cc9', sigil: '☾', motto: 'A whispered word can end a war.', aggression: .65, honor: .25, greed: .6, ambition: .95, paranoia: .75 },
  { id: 'redharbor', name: 'House Redharbor', ruler: 'King Oren', color: '#d67878', sigil: '⚑', motto: 'The tide bows to no throne.', aggression: .85, honor: .5, greed: .65, ambition: .8, paranoia: .35 }
];
export const TERRAINS = {
  plains: { name: 'Plains', cost: 1, defense: 1, color: '#5d7350' },
  forest: { name: 'Forest', cost: 2, defense: 1.25, color: '#345c4e' },
  hills: { name: 'Hills', cost: 2, defense: 1.35, color: '#7e7c60' },
  mountain: { name: 'Mountains', cost: Infinity, defense: 1.6, color: '#616b72' },
  water: { name: 'Water', cost: Infinity, defense: 1, color: '#243d50' },
  coast: { name: 'Coast', cost: 1, defense: 1, color: '#9a9673' }
};
export const BUILDINGS = {
  farm: { name: 'Farm', icon: '♧', cost: { wood: 20, gold: 10 }, terrain: ['plains'], yield: { food: 12 }, turns: 1, description: '+12 food. Fertile land produces +4 more.' },
  lumber: { name: 'Lumber camp', icon: '♣', cost: { wood: 10, gold: 15 }, terrain: ['forest'], yield: { wood: 9 }, turns: 1, description: '+9 wood from forests.' },
  quarry: { name: 'Quarry', icon: '◆', cost: { wood: 20, gold: 15 }, terrain: ['hills'], yield: { stone: 8 }, turns: 2, description: '+8 stone from hills.' },
  mine: { name: 'Iron mine', icon: '⚒', cost: { wood: 25, stone: 15, gold: 20 }, terrain: ['hills'], resource: 'iron', yield: { iron: 7 }, turns: 2, description: '+7 iron. Requires an iron deposit.' },
  road: { name: 'Road', icon: '═', cost: { stone: 5, gold: 3 }, turns: 1, description: 'Half-point movement. Connect settlements and trade partners for gold.' },
  fort: { name: 'Fort', icon: '♜', cost: { wood: 35, stone: 50, gold: 40 }, turns: 3, description: 'Integrity-scaled defender protection and control of nearby land. Troops are needed to stop enemy movement.' },
  town: { name: 'Town', icon: '♖', cost: { wood: 60, stone: 40, gold: 70 }, terrain: ['plains', 'coast'], turns: 3, description: 'Found a settlement at least 4 hexes from another settlement.' },
  city: { name: 'City', icon: '♛', cost: { wood: 70, stone: 85, gold: 100 }, turns: 4, description: 'Promote a town, then upgrade the city three times for more growth, food, gold, population capacity and recruitment orders.' },
  wall: { name: 'City walls', icon: '▥', cost: { stone: 60, gold: 35 }, turns: 3, description: '60 wall strength. Protects defending troops; bombardment reduces protection. Empty settlements can be occupied.' },
  market: { name: 'Market', icon: '⚖', cost: { wood: 40, stone: 25, gold: 40 }, turns: 2, description: '+12 gold each turn in this settlement.' },
  envoyOffice: { name: 'Envoy Office', icon: '✉', cost: { gold: 50, wood: 35 }, turns: 2, description: 'Four shared dispatches per turn and capacity for one ambassador.' },
  chancery: { name: 'Royal Chancery', icon: '⚜', cost: { gold: 100, wood: 50, stone: 30 }, turns: 3, description: 'Requires an Envoy Office here. Five shared dispatches, three ambassadors, faster travel.' },
  workshop: { name: 'Workshop', icon: '⚙', cost: { wood: 40, iron: 25, gold: 45 }, turns: 2, description: '+1 construction/recruitment order per turn, +3 iron.' }
};
export const UNITS = {
  levy: { name: 'Levies', icon: '⚔', attack: 1, defense: 1.2, count: 8, cost: { food: 12, gold: 14 }, description: 'Affordable infantry. Eight soldiers per muster.' },
  archer: { name: 'Archers', icon: '➶', attack: 1.35, defense: 1, count: 6, cost: { wood: 18, gold: 22 }, description: 'Six archers. Strong defending forts and walls.' },
  cavalry: { name: 'Cavalry', icon: '♞', attack: 2.7, defense: 1.8, count: 4, cost: { food: 16, iron: 12, gold: 32 }, description: 'Four riders. Fast alone; strong on open ground.' },
  siege: { name: 'Siege engines', icon: '♜', attack: .8, defense: .5, count: 2, cost: { wood: 30, iron: 16, gold: 35 }, description: 'Two engines. Break walls; slow army movement.' }
};
export const INTENT_TYPES = ['ALLIANCE', 'PEACE', 'TRADE', 'EXCHANGE', 'AID', 'JOINT_WAR', 'DEFEND', 'POSITION', 'WITHDRAW', 'BUILD_DEFENSES', 'TERRITORY', 'TRIBUTE', 'VASSALAGE', 'PROMISE', 'WAR', 'BETRAY', 'RECURRING', 'LOAN', 'NON_AGGRESSION', 'ACCESS', 'EMBARGO', 'GUARANTEE', 'PLEDGE_WAR', 'PLEDGE_ATTACK', 'PLEDGE_DEFEND', 'PLEDGE_WITHDRAW', 'PLEDGE_BUILD', 'PLEDGE_PEACE'];
export const SAVE_VERSION = 3;

// Shared catalogs drive construction, inspections, AI, manufacture and artwork.
export const RESOURCE_VALUES = { food: 1, wood: 1.2, stone: 1.5, iron: 2, gold: 1.5, horses: 3, tools: 3.5, arms: 4 };
export const QUALITY = { poor: .65, normal: 1, rich: 1.5, exceptional: 2 };
export const REGIONS = {
  ashen: { name: 'Central Crossroads', weights: [4, 3, 3, 2, 1], industry: 'market', troops: 'infantry', description: 'Flexible construction and commerce; few exceptional deposits.' },
  wintermere: { name: 'Northern Timberlands', weights: [5, 8, 2, 1, 1], industry: 'lumber', troops: 'archer', description: 'Rich timber and farmland; scarce iron and gold.' },
  thornwall: { name: 'Iron Highlands', weights: [1, 2, 7, 8, 1], industry: 'quarry', troops: 'infantry', description: 'Exceptional stone and iron; poor farmland.' },
  sunspire: { name: 'Golden Coast', weights: [3, 1, 2, 1, 2], industry: 'market', troops: 'cavalry', description: 'Coastal commerce and gold; limited heavy materials.' },
  vesper: { name: 'Veiled Cities', weights: [2, 2, 2, 2, 1], industry: 'workshop', troops: 'infantry', description: 'Efficient manufacturing and urban trade; modest raw deposits.' },
  redharbor: { name: 'Tidal Pastures', weights: [7, 3, 1, 1, 7], industry: 'ranch', troops: 'cavalry', description: 'Fertile coastal pastures, horses and shipping; scarce stone and iron.' }
};
const add = (name, category, cost, turns, extra = {}) => ({ name, category, cost, turns, icon: '◆', description: name, ...extra });
Object.assign(BUILDINGS, {
  intelligenceOffice: add('Whisper Office', 'Government', {gold:60,wood:35,stone:20}, 3, {settlement:true,description:'Supports 1 / 3 / 5 spies. Level II unlocks counterintelligence; Level III strengthens networks.'}),
  ranch: add('Horse Ranch', 'Economy', {wood: 30, gold: 30}, 2, {terrain: ['plains', 'coast'], yield: {horses: 4}}),
  storehouse: add('Storehouse', 'Economy', {wood: 30, stone: 20, gold: 20}, 2, {settlement: true}),
  tradeOutpost: add('Trading Post', 'Trade', {wood: 30, stone: 20, gold: 40}, 2, {yield: {gold: 5}}),
  merchantGuild: add('Merchant Guild Hall', 'Trade', {wood: 65, stone: 50, gold: 110, tools: 12}, 5, {settlement: true, requires: {market: 2}, yield: {gold: 15}}),
  harbor: add('Harbor', 'Trade', {wood: 65, stone: 40, gold: 75, tools: 10}, 4, {settlement: true, coastal: true, yield: {gold: 16}}),
  barracks: add('Barracks', 'Military', {wood: 35, stone: 25, gold: 35}, 2, {settlement: true}),
  range: add('Archery Range', 'Military', {wood: 40, gold: 30}, 2, {settlement: true}),
  stable: add('Military Stables', 'Military', {wood: 40, stone: 15, horses: 6, gold: 35}, 2, {settlement: true}),
  siegeWorks: add('Siege Workshop', 'Military', {wood: 55, iron: 20, tools: 10, gold: 45}, 3, {settlement: true, requires: {workshop: 1}}),
  armory: add('Armory', 'Military', {wood: 35, stone: 25, iron: 20, gold: 45}, 3, {settlement: true, recipe: {input: {iron: 4, wood: 2}, output: {arms: 4}}}),
  watchtower: add('Watchtower', 'Defense', {wood: 25, stone: 20, gold: 25}, 2),
  greatGranary: add('Great Granary', 'Great Projects', {wood: 100, stone: 100, tools: 35, gold: 100}, 6, {settlement: true, requires: {storehouse: 3}, maxLevel: 1}),
  royalArsenal: add('Royal Arsenal', 'Great Projects', {iron: 100, tools: 40, gold: 140}, 6, {settlement: true, requires: {armory: 3}, recipe: {input: {iron: 8, wood: 4}, output: {arms: 12}}, maxLevel: 1}),
  greatStable: add('Great Stable', 'Great Projects', {horses: 30, wood: 100, tools: 20, gold: 130}, 5, {settlement: true, requires: {stable: 3}, yield: {horses: 5}, maxLevel: 1}),
  siegeFoundry: add('Siege Foundry', 'Great Projects', {wood: 100, iron: 80, tools: 40, gold: 140}, 6, {settlement: true, requires: {siegeWorks: 3}, maxLevel: 1})
});
const tierNames = {
  intelligenceOffice: ['Whisper Office', 'Intelligence Bureau', 'Royal Whisper Network'],
  farm: ['Farmstead', 'Agricultural Estate', 'Great Estate'], lumber: ['Logging Camp', 'Sawmill', 'Royal Timberworks'], quarry: ['Quarry', 'Stoneworks', 'Grand Quarry'], mine: ['Iron Mine', 'Deep Mine', 'Royal Mine'], ranch: ['Horse Ranch', 'Horse Estate', 'Royal Stud'],
  city: ['City', 'Chartered City', 'Grand City', 'Royal City'],
  road: ['Road', 'Stone Road', 'Royal Highway'], fort: ['Fort', 'Stone Fortress', 'Great Fortress'], wall: ['City Walls', 'Reinforced Walls', 'Citadel Walls'],
  market: ['Market', 'Merchant Quarter', 'Grand Bazaar'], workshop: ['Workshop', 'Engineering Works', 'Royal Works'], storehouse: ['Storehouse', 'Warehouse', 'Royal Granary'],
  tradeOutpost: ['Trading Post', 'Merchant Outpost', 'Grand Exchange'], barracks: ['Barracks', 'Veteran Barracks', 'Royal Barracks'], range: ['Archery Range', 'Veteran Range', 'Royal Bowyer'], stable: ['Military Stables', 'Cavalry Stables', 'Knightly Hall'], siegeWorks: ['Siege Workshop', 'Siege Yard', 'Trebuchet Works'], armory: ['Armory', 'Arsenal', 'Grand Arsenal'],
  watchtower: ['Watchtower', 'Signal Tower', 'Beacon Keep'], envoyOffice: ['Envoy Office', 'Embassy', 'Grand Embassy'], chancery: ['Royal Chancery', 'High Chancery', 'Crown Secretariat'], merchantGuild: ['Merchant Guild Hall', 'Merchant League', 'Royal Trading Company'], harbor: ['Harbor', 'Merchant Port', 'Royal Shipyard']
};
const categories = {farm:'Economy', lumber:'Economy', quarry:'Economy', mine:'Economy', market:'Economy', workshop:'Economy', road:'Trade', fort:'Defense', wall:'Defense', town:'Government', city:'Government', envoyOffice:'Government', chancery:'Government'};
for (const [id, b] of Object.entries(BUILDINGS)) {
  b.category ||= categories[id]; b.maxLevel ||= tierNames[id]?.length || 1;
  if (['wall','market','workshop','envoyOffice','chancery'].includes(id)) b.settlement = true;
  if (id === 'workshop') { b.recipe = {input:{wood:3, iron:2}, output:{tools:5}}; delete b.yield; }
  if (id === 'market') b.yield = {gold:12};
  if (id === 'chancery') b.yield = {gold:3};
  b.levels = Array.from({length:b.maxLevel}, (_, n) => {
    const level = n+1, cost = Object.fromEntries(Object.entries(b.cost).map(([r,v]) => [r, Math.ceil(v*(1+n*.75))]));
    if (n && !['farm','lumber','town','city','envoyOffice'].includes(id)) cost.tools = (cost.tools || 0) + n * (id === 'road' ? 4 : 10);
    return {name: tierNames[id]?.[n] || b.name, cost, turns: Math.min(8, b.turns+n+(id === 'fort' && n ? 1 : 0)), level};
  });
}
// A base city plus three paid upgrades. These values also drive forecasts and UI.
BUILDINGS.city.levels.forEach((tier,n)=>Object.assign(tier,{growth:3+n,capacity:150+n*50,food:14+n*4,gold:8+n*4,orders:n}));
BUILDINGS.chancery.requires = {envoyOffice: 1};
BUILDINGS.fort.levels[2].turns = 6;
BUILDINGS.road.levels[2].cost.gold = 18;
BUILDINGS.road.levels[2].cost.stone = 18;
export const FORMATIONS = {
  balanced: {name:'Balanced', description:'Flexible ranks with no specialized bonus.'},
  defensive: {name:'Defensive Line', description:'25% stronger infantry defense; slower marching.'},
  spearWall: {name:'Spear Wall', description:'Spearmen stop mounted charges; exposed to volleys.'},
  charge: {name:'Aggressive Charge', description:'35% stronger opening charge; exposed in melee.'},
  skirmish: {name:'Skirmish', description:'25% stronger volleys and safer withdrawals.'},
  flanking: {name:'Flanking Formation', description:'Mounted troops exploit exposed flanks; requires cavalry.'}
};
const unit = (name, family, attack, defense, count, cost, requires, extra={}) => ({name, family, attack, defense, count, cost, requires, icon: family === 'mounted' ? '♞' : family === 'ranged' ? '➶' : family === 'siege' ? '♜' : '⚔', description:name, ...extra});
Object.assign(UNITS, {
  levy: unit('Levies','infantry',1,1.2,8,{food:12,gold:14},{barracks:1},{discipline:.7}),
  spearman: unit('Spearmen','infantry',1.4,1.8,6,{food:12,wood:12,iron:6,gold:20},{barracks:1},{antiCavalry:2.8}),
  menAtArms: unit('Men-at-Arms','infantry',2.4,2.6,6,{food:15,arms:6,gold:30},{barracks:2},{armor:.3}),
  heavyInfantry: unit('Heavy Infantry','infantry',3,4,4,{food:18,arms:8,iron:10,gold:40},{barracks:3},{armor:.5,discipline:1.3}),
  archer: unit('Archers','ranged',1.35,1,6,{wood:18,gold:22},{range:1},{ranged:2.5}),
  veteranArcher: unit('Veteran Archers','ranged',1.7,1.3,6,{wood:20,arms:3,gold:32},{range:2},{ranged:3.6}),
  crossbow: unit('Heavy Crossbowmen','ranged',2,1.8,4,{wood:18,arms:6,iron:8,gold:35},{range:3},{ranged:4.5,piercing:true}),
  scout: unit('Scouts','mounted',1.1,1,4,{horses:4,food:10,gold:18},{stable:1},{charge:1.2,pursuit:2}),
  lightCavalry: unit('Light Cavalry','mounted',1.8,1.5,4,{horses:4,arms:2,food:12,gold:24},{stable:1},{charge:3,pursuit:3}),
  cavalry: unit('Heavy Cavalry','mounted',2.7,1.8,4,{horses:4,food:16,iron:12,arms:4,gold:32},{stable:2},{charge:5,armor:.25,pursuit:2}),
  knight: unit('Mounted Knights','mounted',4.5,4,3,{horses:3,arms:9,iron:15,food:20,gold:55},{stable:3},{charge:8,armor:.5,pursuit:2.5,discipline:1.4}),
  ram: unit('Battering Rams','siege',.2,.5,2,{wood:30,tools:6,gold:25},{siegeWorks:1},{breach:7,bombardRange:1}),
  catapult: unit('Catapults','siege',.8,.5,2,{wood:35,iron:16,tools:10,gold:40},{siegeWorks:2},{breach:12,ranged:2,bombardRange:2}),
  trebuchet: unit('Trebuchets','siege',.6,.5,1,{wood:45,iron:20,tools:18,gold:50},{siegeWorks:3},{breach:24,ranged:2.5,bombardRange:3}),
  siege: unit('Legacy Siege Engines','siege',.8,.5,2,{wood:30,iron:16,gold:35},{siegeWorks:2},{breach:9,legacy:true,bombardRange:2})
});
for (const u of Object.values(UNITS)) u.description = `${u.count} per base muster. ${Object.entries(u.requires).map(([b,l])=>`${BUILDINGS[b].levels[l-1].name} required`).join(', ')}. ${u.family === 'siege' ? 'Slow; attacks walls over successive turns.' : u.antiCavalry ? 'Counters cavalry charges.' : u.piercing ? 'Armor-piercing volleys.' : u.family === 'mounted' ? 'Charge, flank and pursue; weak in forests and against spears.' : u.family === 'ranged' ? 'Volleys precede melee; protected by hills and walls.' : 'Holds the main battle line.'}`;
