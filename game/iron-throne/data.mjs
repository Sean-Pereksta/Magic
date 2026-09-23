export const RESOURCES = ['food', 'wood', 'stone', 'iron', 'gold'];
export const RESOURCE_ICONS = { food: '◈', wood: '♣', stone: '◆', iron: '⚒', gold: '●' };
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
  fort: { name: 'Fort', icon: '♜', cost: { wood: 35, stone: 50, gold: 40 }, turns: 3, description: '+60% defense, control nearby land, and stop adjacent enemy movement.' },
  town: { name: 'Town', icon: '♖', cost: { wood: 60, stone: 40, gold: 70 }, terrain: ['plains', 'coast'], turns: 3, description: 'Found a settlement at least 4 hexes from another settlement.' },
  city: { name: 'City upgrade', icon: '♛', cost: { wood: 70, stone: 85, gold: 100 }, turns: 4, description: 'Upgrade a town. More food, income, population capacity and defense.' },
  wall: { name: 'City walls', icon: '▥', cost: { stone: 60, gold: 35 }, turns: 3, description: '60 wall strength. Siege engines breach walls before an assault.' },
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
export const SAVE_VERSION = 2;
