/* Flag Capture: Riftbound. Deterministic cooldown strategy; no wall clocks or DOM. */
const ability = (name, description, cooldown, kind, range = 0) => ({ name, description, cooldown:cooldown*2+4, kind, range });
const troop = (name, hp, damage, move, range, cost, skill, description) => ({ name, hp, damage, move, range, cost, moveCooldown:move===3?2:hp>=12?3.5:hp>=10?3:2.8, ability: skill, description });
export const DRAFT_BUDGET = 24;
export const DEFAULT_ARMY = ['leader','scout','scout','brute','mystic'];
export const DRAFT_ROLES = Object.freeze(['leader','scout','brute','mystic','guardian','ranger','shaper']);
export const RECRUIT_ROLES = Object.freeze(DRAFT_ROLES.filter(role=>role!=='leader'));
const DRAFT_COSTS = {leader:6,scout:3,brute:5,mystic:4,guardian:5,ranger:4,shaper:5};
export function draftCost(role) {return Object.hasOwn(DRAFT_COSTS,role)?DRAFT_COSTS[role]:Infinity;}
export function armyCost(army) {return Array.isArray(army)?army.reduce((cost,role)=>cost+draftCost(role),0):Infinity;}
export function defaultArmyForBudget(money=DRAFT_BUDGET) {
  const army=[];for(const role of DEFAULT_ARMY)if(armyCost(army)+draftCost(role)<=money)army.push(role);return army;
}
export function defaultAIArmyForBudget(money=DRAFT_BUDGET,seed=1){
  if(money<18)return defaultArmyForBudget(money);
  const styles=[['leader','scout','ranger','guardian','scout'],['leader','ranger','scout','scout','shaper'],['leader','scout','scout','guardian','mystic']];
  const requested=styles[(Number(seed)>>>0)%styles.length],army=[];
  for(const role of requested)if(armyCost(army)+draftCost(role)<=money)army.push(role);
  return army;
}

export const FACTIONS = {
  bloom: {
    key: 'bloom', name: 'Lumen Brood', tagline: 'Life from another light.', color: '#55dfbd',
    lore: 'A migrating reef of curious space organisms. They plant living light wherever their many feet wander.',
    passive: 'Root network: start in a grove for +1 move range. Photosynthesis restores 1 HP in groves every 6 seconds; capturing a beacon restores 2 HP.',
    units: {
      leader: troop('Prism Matriarch', 10, 2, 2, 2, 0, ability('Bloomcall', 'Restore 3 HP to yourself and allies within 2 tiles.', 3, 'bloomcall', 2), 'A luminous hive mother who keeps the expedition growing.'),
      scout: troop('Skitterglow', 6, 2, 3, 1, 4, ability('Sporehop', 'Blink to an empty tile within 4; leave a healing grove behind.', 2, 'sporehop', 4), 'A six-legged seed courier, built for impossible shortcuts.'),
      brute: troop('Reefback Grazer', 12, 3, 2, 1, 6, ability('Gravity Belch', 'Deal 2 damage, pull a foe one tile closer, and root it for 2.5 seconds.', 3, 'belch', 3), 'A walking coral garden with a small black hole for a stomach.'),
      mystic: troop('Lantern Polyp', 7, 2, 2, 3, 5, ability('Sporeburst', 'Deal 3 damage to enemies within 1 tile of the target.', 3, 'sporeburst', 3), 'A floating lantern full of delightfully explosive pollen.')
    }
  },
  chaos: {
    key: 'chaos', name: 'Crooked Carnival', tagline: 'Reality has stage fright.', color: '#fb879e',
    lore: 'A parade of impossible beasts escaped from a broken dream. Their tricks are ridiculous. Their teeth are real.',
    passive: 'Wild temper: injured Carnival troops deal +1 damage with basic attacks.',
    units: {
      leader: troop('Mawring Maestro', 11, 3, 2, 1, 0, ability('Wrong Note', 'Deal 2 damage and root every enemy within 2 tiles for 2.5 seconds.', 3, 'wrongnote', 2), 'An accordion-mouthed conductor of glorious disorder.'),
      scout: troop('Blinkmutt', 6, 2, 3, 1, 4, ability('Sideways Bite', 'Warp beside a foe within 3 tiles and bite for 3 damage.', 2, 'blinkbite', 3), 'Three eyes, too many elbows, and absolutely no sense of direction.'),
      brute: troop('Mobius Ram', 12, 3, 2, 1, 6, ability('Foldhorn', 'Ram a foe within 2 for 4 damage and knock it back. A blocked knockback deals +1.', 3, 'foldhorn', 2), 'A woolly knot that charges through the folds of space.'),
      mystic: troop('Teacup Oracle', 7, 2, 2, 3, 5, ability('Bad Omen', 'Deal 2 damage to a foe within 3. Its next incoming hit deals +2.', 2, 'omen', 3), 'A many-eyed fortune teller who drinks tomorrow for breakfast.')
    }
  },
  choir: {
    key: 'choir', name: 'Aureate Choir', tagline: 'Make the impossible harmonious.', color: '#f8d77c',
    lore: 'Living bells, little comets, and watchful rings of light. These celestial beings build harmony one battlefield at a time.',
    passive: 'Resonance: gain 1 shield beside an ally every 6 seconds, up to 3.',
    units: {
      leader: troop('Halo Architect', 10, 2, 2, 2, 0, ability('Concordance', 'Other allies within 2 refresh their move/attack cooldown and gain 1 shield.', 3, 'concordance', 2), 'A winged blueprint dreaming a kinder geometry.'),
      scout: troop('Cherub Comet', 7, 2, 3, 1, 4, ability('Mercy Arc', 'Restore 4 HP to an ally within 3 and cleanse roots and curses.', 2, 'mercy', 3), 'A tiny star with huge wings and a habit of arriving just in time.'),
      brute: troop('Belltower Seraph', 13, 3, 2, 1, 6, ability('Sanctuary', 'Give yourself and allies within 1 tile 4 shield, up to 5.', 3, 'sanctuary', 1), 'A gentle walking bell tower that refuses to let its friends fall.'),
      mystic: troop('Ophanim Lens', 7, 2, 2, 3, 5, ability('Judgment Ray', 'Deal 3 damage to every enemy in a straight line up to 5 tiles.', 3, 'ray', 5), 'A wheel of watchful eyes that focuses dawn into a beam.')
    }
  }
};

const secondarySkills = {
  bloom:{leader:ability('Seedgrove','Plant a living grove on an empty ground tile within 3.',2,'seedgrove',3),mystic:ability('Nectar Mote','Restore 2 HP to an ally within 3 tiles.',2,'nectar',3)},
  chaos:{leader:ability('Laughing Hex','Curse a foe within 3 tiles. Its next incoming hit deals +2.',2,'laughinghex',3),mystic:ability('Crooked Swap','Swap places with a foe within 3 tiles. Both landing tiles must suit their movement.',3,'crookedswap',3)},
  choir:{leader:ability('Starstep','Teleport to an empty tile within 3 and gain 2 shield. May land on water, never rocks.',3,'starstep',3),mystic:ability('Ward Ray','Give an ally within 3 tiles 3 shield, up to 5.',2,'wardray',3)}
};
const summonSkills = {
  bloom:{leader:ability('Broodlings','Summon up to 2 tiny Glowgrubs on empty ground within 2 and an adjacent tile. Army limit 8.',4,'broodlings',2)},
  chaos:{mystic:ability('Pocket Maws','Summon up to 2 tiny Pocket Maws on empty ground within 2 and an adjacent tile. Army limit 8.',4,'pocketmaws',2)},
  choir:{mystic:ability('Bell Motes','Summon up to 2 tiny flying Bell Motes on empty ground within 2 and an adjacent tile. Army limit 8.',4,'bellmotes',2)}
};
const tacticalSkill=(name,description,cooldown,kind,range,target,color,groundKind,radius=0)=>Object.assign(ability(name,description,cooldown,kind,range),{target,color,...(groundKind?{groundKind}:{}),radius});
const newTroops={
  bloom:{
    guardian:[troop('Mossplate Bulwark',12,2,1,1,6,tacticalSkill('Rootguard','Give yourself and adjacent allies 2 shield, up to 5.',3,'rootguard',1,'self','support'),'A patient shell-garden that anchors a living formation.'),tacticalSkill('Briar Circle','Grow a thornfield within 2 for 12 seconds. Ground enemies take 1 damage every 3 seconds.',3,'thornfield',2,'ground','attack','thornfield',1)],
    ranger:[troop('Thornskate',6,2,3,2,5,tacticalSkill('Pinning Quill','Hit a foe within 3 for 2 damage and root it for 2.5 seconds.',2,'pinshot',3,'enemy','attack'),'A swift leaf-fin hunter that pins enemies in hostile gardens.'),tacticalSkill('Pollen Skate','Hop to empty ground within 3, leaving 12-second mycelium at both ends.',3,'pollenstep',3,'empty','move','mycelium')],
    shaper:[troop('Mycelium Weaver',8,1,2,2,6,tacticalSkill('Living Carpet','Grow mycelium within 3 for 12 seconds. Allies heal 1 every 3 seconds; enemy ground steps cost 2 movement.',3,'mycelium',3,'ground','support','mycelium',1),'A moon-jelly gardener that stitches safe paths through the battle.'),tacticalSkill('Germinate','Heal an ally within 3 for 3 HP. On friendly mycelium, also refresh its move/attack cooldown.',3,'germinate',3,'ally','heal')]
  },
  chaos:{
    guardian:[troop('Grinlock Bastion',11,2,2,1,6,tacticalSkill('Grinning Ward','Give yourself and adjacent allies 2 shield and root adjacent enemies for 1 second.',3,'grinward',1,'self','support'),'A grinning living gate that holds foes in a very bad place.'),tacticalSkill('Cinder Confetti','Scatter cinders within 2 for 10 seconds. Enemies take 1 damage on arrival and every 3 seconds.',3,'cinders',2,'ground','attack','cinders',1)],
    ranger:[troop('Rift Jackal',6,3,3,1,5,tacticalSkill('Crosscut','Strike a foe within 2 for 3 damage; deal +1 if an ally stands directly opposite you across that foe.',3,'crosscut',2,'enemy','attack'),'An angular dream-hound that hunts from both sides at once.'),tacticalSkill('Riftstep','Hop to empty ground within 3, leaving 12-second rifts at both ends.',3,'riftstep',3,'empty','move','rift')],
    shaper:[troop('Inkmaw Jester',7,1,2,3,6,tacticalSkill('Rift Ink','Paint a rift within 3 for 12 seconds. Allies starting there phase through foes and move +1; enemies are cursed on arrival and every 3 seconds.',3,'rift',3,'ground','support','rift',1),'A bottled nightmare that rewrites the spaces between friends.'),tacticalSkill('Curtain Collapse','Consume friendly rift or cinder marks within 1 of a marked tile. Deal 2 damage to enemies in that area.',3,'detonate',3,'ground','attack',undefined,1)]
  },
  choir:{
    guardian:[troop('Aegis Cantor',12,2,1,1,6,tacticalSkill('Aegis Link','Give an ally within 3 up to 3 shield, then cleanse its roots and curse.',3,'aegislink',3,'ally','support'),'A grounded, many-winged choir vault that shelters a careful procession.'),tacticalSkill('Quiet Sanctuary','Consecrate a sanctuary within 2 for 12 seconds. Allies gain 1 shield and cleanse roots/curses every 3 seconds.',3,'sanctum',2,'ground','support','sanctum',1)],
    ranger:[troop('Sunlance Kite',6,2,3,3,5,tacticalSkill('Sunlance','Strike a foe in your row or column within 4 for 3 ranged damage, ignoring 1 point of shield.',3,'sunlance',4,'enemy','attack'),'A little winged spear that turns a clear firing lane into dawn.'),tacticalSkill('Prism Lattice','Lay a lattice within 3 for 12 seconds. Allies standing there gain +1 ranged basic damage; enemy ground steps cost 2 movement.',3,'prismwall',3,'ground','support','prismwall',1)],
    shaper:[troop('Halo Scribe',8,1,2,2,6,tacticalSkill('Dawn Seal','Erase hostile ground effects within 1 of a tile, then create a 12-second sanctuary there.',4,'dawnseal',3,'ground','support','sanctum',1),'A flying scripture-beast that redraws danger into sanctuary.'),tacticalSkill('Procession','Exchange places with an ally within 3 when both landings are legal. Both gain 2 shield.',3,'choirswap',3,'ally','move')]
  }
};
for(const [faction,roster]of Object.entries(newTroops))for(const [role,[definition,second]]of Object.entries(roster)){
  FACTIONS[faction].units[role]=definition;secondarySkills[faction][role]=second;
}
export const FORMATIONS=Object.freeze({
  bloom:{name:'Living Constellation',description:'Connect 3 or more Brood troops edge-to-edge. Every linked troop gains +1 movement and heals 1 HP every 6 seconds.'},
  chaos:{name:'Impossible Pincer',description:'Stand directly opposite an ally across an adjacent enemy. Your basic attacks against that flanked enemy deal +1 damage.'},
  choir:{name:'Choir Procession',description:'Line up 3 or more Choir troops on consecutive squares in one row or column. They take 1 less ranged damage (minimum 1) and resonance can build up to 5 shield.'}
});
for(const [faction,formation]of Object.entries(FORMATIONS))FACTIONS[faction].formation=formation;
export const GROUND_EFFECTS=Object.freeze({
  mycelium:{name:'Living Mycelium',description:'Friendly: heal 1 HP every 3s. Enemy ground troops: each step here costs 2 movement.',duration:12,polarity:'support',color:'#83e7b0',icon:'mycelium'},
  thornfield:{name:'Briar Field',description:'Enemy ground troops take 1 damage every 3s. Flying troops are safe; leave the marked ground to escape.',duration:12,polarity:'attack',color:'#a2ce68',icon:'thornfield'},
  rift:{name:'Rift Ink',description:'Allies starting here move +1 and phase through enemies for that move. Enemies are cursed on arrival and every 3s.',duration:12,polarity:'support',color:'#c193f1',icon:'rift'},
  cinders:{name:'Cinder Confetti',description:'Enemies take 1 damage on arrival and every 3s. Flying creatures are also affected.',duration:10,polarity:'attack',color:'#ff956f',icon:'cinders'},
  sanctum:{name:'Quiet Sanctuary',description:'Allies gain 1 shield (up to 5) and cleanse roots/curses every 3s.',duration:12,polarity:'support',color:'#ffe4a3',icon:'sanctum'},
  prismwall:{name:'Prism Lattice',description:'Friendly ranged basic attacks gain +1 damage while standing here. Enemy ground steps here cost 2 movement.',duration:12,polarity:'support',color:'#9ee0f1',icon:'prismwall'}
});
for(const [faction,name]of [['bloom','Glowgrub'],['chaos','Pocket Maw'],['choir','Bell Mote']]) {
  FACTIONS[faction].units.minion=troop(name,3,1,2,1,0,null,'A tiny summoned companion. Move, attack, and guard; no special abilities.');
  FACTIONS[faction].units.minion.moveCooldown=2.4;
}
for(const [faction,f]of Object.entries(FACTIONS))for(const [role,d]of Object.entries(f.units)) {
  d.abilities=d.ability?[d.ability,...(secondarySkills[faction]?.[role]?[secondarySkills[faction][role]]:[]),...(summonSkills[faction]?.[role]?[summonSkills[faction][role]]:[])]:[];
  d.draftCost=draftCost(role);
  d.groveMoveBonus=faction==='bloom'?1:0;
  d.movement=faction==='choir'&&!['brute','guardian'].includes(role)?'Flight':faction==='chaos'&&['scout','ranger'].includes(role)?'Phasewalk':faction==='bloom'?(['scout','ranger'].includes(role)?'Skitter':'Rootbound'):'Ground';
  d.movementDescription=d.movement==='Flight'?'Fly over rocks, water, and troops. Land on empty land or water, never rocks.':
    d.movement==='Phasewalk'?'Walk through enemy troops, but land only on an empty ground tile.':
    faction==='bloom'?'Root network: starting in a grove adds 1 tile to your movement range. Ground obstacles still block you.':
    'Ground movement: rocks, water, and enemy troops block your path.';
}

export const MAPS = {
  glade: { key: 'glade', name: 'Prism Glade', subtitle: 'Living groves & open flanks', rocks: [[1,2],[7,4]], water: [[4,1],[4,5]], groves: [[2,2],[6,4],[6,2],[2,4]] },
  fracture: { key: 'fracture', name: 'The Great Fracture', subtitle: 'Broken paths & close encounters', rocks: [[1,1],[7,5],[3,2],[5,4]], water: [[4,2],[4,4]], groves: [[1,4],[7,2]] },
  sanctuary: { key: 'sanctuary', name: 'Bellglass Sanctuary', subtitle: 'Sheltered groves & a contested center', rocks: [[1,3],[7,3]], water: [[3,1],[5,5]], groves: [[2,2],[3,3],[5,3],[6,4],[6,2],[2,4]] }
};

export const MODES = {
  beacons:{key:'beacons',name:'Beacon Rush',description:'Claim flags by entering them. Hold their light to reach 30 points.',goal:30},
  control:{key:'control',name:'Zone Control',description:'Most troops within one orthogonal tile control each flag. Tied zones are contested; reach 20 points.',goal:20},
  elimination:{key:'elimination',name:'Last Army Standing',description:'Defeat every enemy. At the time limit, remaining health wins, then held flags.',goal:0},
  reinforcements:{key:'reinforcements',name:'Reinforcement Clash',description:'Capture persistent flags and reach 30 light. Spend coins and beacon income on fresh reinforcements.',goal:30}
};
export const DIFFICULTIES = {
  story:{key:'story',name:'Story',description:'A patient opponent that gives you time to learn, favors flags, and guards before pressing an attack.',interval:4.8,grace:8},
  easy:{key:'easy',name:'Relaxed',description:'A gentle commander with a generous opening, slower decisions, and a preference for objectives and defense.',interval:3.2,grace:6},
  normal:{key:'normal',name:'Balanced',description:'A measured opponent that combines objectives, attacks, and troop abilities.',interval:1.7,grace:3},
  hard:{key:'hard',name:'Tactician',description:'A decisive commander that exploits vulnerable troops and coordinates its unusual abilities.',interval:1,grace:2},
  expert:{key:'expert',name:'Riftlord',description:'A relentless opponent with fast decisions and a short opening. Troop stats and cooldowns stay the same.',interval:0.6,grace:1.5}
};
export const BOARD_SIZES = [
  {key:'compact',name:'Pocket Rift',width:7,height:5},
  {key:'standard',name:'Standard Rift',width:9,height:7},
  {key:'grand',name:'Grand Rift',width:11,height:9}
];

const SIDES = ['player', 'ai'];
const DIRS = [[1,0],[-1,0],[0,1],[0,-1]];
const distance = (a,b) => Math.abs(a.x-b.x) + Math.abs(a.y-b.y);
const inside = (s,x,y) => Number.isInteger(x) && Number.isInteger(y) && x>=0 && y>=0 && x<s.width && y<s.height;
const key = (x,y) => `${x},${y}`;
const other = side => side === 'player' ? 'ai' : 'player';
const living = (s,side) => s.units.filter(u => u.hp>0 && (!side || u.side===side));
const tileAt = (s,x,y) => s.tiles[y*s.width+x];
const blocked = (s,x,y) => !inside(s,x,y) || ['rock','water'].includes(tileAt(s,x,y).terrain);
export const unitDef = unit => FACTIONS[unit.faction]?.units[unit.role];
export const skillDef = (unit,index=0) => Number.isInteger(index)&&index>=0?unitDef(unit)?.abilities[index]:undefined;
export const skillReadyAt = (unit,index=0) => index===0?unit.abilityReadyAt:(unit.abilityReadyAts?.[index]??0);
export const at = (s,x,y) => s.units.find(u => u.hp>0 && u.x===x && u.y===y) || null;
export const groundEffectsAt = (s,x,y) => (s.groundEffects||[]).filter(effect=>effect.x===x&&effect.y===y&&effect.expiresAt>s.time);
const friendlyField=(s,u,kind,x=u.x,y=u.y)=>groundEffectsAt(s,x,y).some(effect=>effect.side===u.side&&effect.kind===kind);
function flankPartners(s,u,target){
  if(!target||distance(u,target)!==1)return [];
  return living(s,u.side).filter(v=>v.id!==u.id&&v.faction==='chaos'&&v.x===target.x*2-u.x&&v.y===target.y*2-u.y);
}
export function formationStatus(s,u){
  const definition=FORMATIONS[u?.faction];
  if(!definition||!u||u.hp<=0)return {active:false,key:u?.faction||'',name:definition?.name||'',description:definition?.description||'',unitIds:[],targetIds:[],bonus:''};
  const allies=living(s,u.side).filter(v=>v.faction===u.faction),members=new Set([u.id]),targets=[];
  if(u.faction==='bloom'){
    const queue=[u];for(let i=0;i<queue.length;i++)for(const ally of allies)if(!members.has(ally.id)&&distance(queue[i],ally)===1){members.add(ally.id);queue.push(ally);}
  }else if(u.faction==='chaos'){
    for(const target of living(s,other(u.side))){const partners=flankPartners(s,u,target);if(partners.length){targets.push(target.id);partners.forEach(v=>members.add(v.id));}}
  }else{
    const horizontal=new Set([u.id]),vertical=new Set([u.id]);
    for(const [dx,dy,set]of [[1,0,horizontal],[0,1,vertical]])for(const sign of [-1,1])for(let n=1;n<8;n++){
      const ally=allies.find(v=>v.x===u.x+dx*n*sign&&v.y===u.y+dy*n*sign);if(!ally)break;set.add(ally.id);
    }
    for(const id of (horizontal.size>=vertical.size?horizontal:vertical))members.add(id);
  }
  const active=u.faction==='chaos'?targets.length>0:members.size>=3;
  return {...definition,key:u.faction,active,unitIds:[...members],targetIds:targets,bonus:u.faction==='bloom'?'+1 move · +1 HP / 6s':u.faction==='chaos'?'+1 basic damage vs flanked foe':'−1 ranged damage · resonance cap 5'};
}
export const movementRange = (s,u) => unitDef(u).move+(tileAt(s,u.x,u.y).terrain==='grove'?unitDef(u).groveMoveBonus:0)+(u.faction==='bloom'&&formationStatus(s,u).active?1:0)+(friendlyField(s,u,'rift')?1:0);
const phasing=(s,u)=>['Flight','Phasewalk'].includes(unitDef(u).movement)||friendlyField(s,u,'rift');
const movementCost=(s,u,x,y)=>unitDef(u).movement==='Flight'?1:groundEffectsAt(s,x,y).some(effect=>effect.side!==u.side&&['mycelium','prismwall'].includes(effect.kind))?2:1;
function movementBlocked(s,u,x,y,landing=false) {
  if(!inside(s,x,y))return true;
  if(unitDef(u).movement==='Flight')return landing&&tileAt(s,x,y).terrain==='rock';
  return blocked(s,x,y);
}

function nextRandom(s) {
  s.rng = (Math.imul(s.rng, 1664525) + 1013904223) >>> 0;
  return s.rng / 4294967296;
}

function spawn(s,side,role,x,y,ready=true,factionOverride) {
  const faction = factionOverride??s.factions[side], def = FACTIONS[faction].units[role];
  const delay=ready?0:DIFFICULTIES[s.difficulty].grace;
  const u = { id: `u${++s.nextId}`, side, faction, role, x, y, hp:def.hp, maxHp:def.hp, readyAt:s.time+delay, abilityReadyAt:def.abilities.length?s.time+delay:0, abilityReadyAts:def.abilities.map(()=>s.time+delay), shield:0, rootedUntil:0, cursed:0 };
  s.units.push(u);
  return u;
}

export function createGame({ playerFaction='bloom', aiFaction='chaos', difficulty='normal', map='glade', seed=1, mode='beacons', width=9, height=7, terrain='balanced', goal, duration=180, opponentMode='ai', startingMoney=DRAFT_BUDGET, playerArmy, aiArmy, playerDeployment, aiDeployment }={}) {
  if (!FACTIONS[playerFaction] || !FACTIONS[aiFaction]) throw new Error('Choose a valid faction.');
  if (!MAPS[map]) throw new Error('Choose a valid battlefield.');
  if (!MODES[mode]) throw new Error('Choose a valid battle mode.');
  if (!['ai','local'].includes(opponentMode))throw new Error('Choose AI or local two-player.');
  if (![7,9,11].includes(width)||![5,7,9].includes(height))throw new Error('Choose a supported battlefield size.');
  if (!['sparse','balanced','wild'].includes(terrain))throw new Error('Choose a valid terrain density.');
  if(goal!==undefined&&(!Number.isFinite(goal)||goal<1||goal>60))throw new Error('Choose a light goal between 1 and 60.');
  if(!Number.isFinite(duration)||duration<60||duration>300)throw new Error('Choose a duration between 60 and 300 seconds.');
  if(!Number.isInteger(startingMoney)||startingMoney<12||startingMoney>40)throw new Error('Choose starting coins between 12 and 40.');
  const gentle=opponentMode==='local'||['story','easy'].includes(difficulty);
  const armies={player:playerArmy===undefined?defaultArmyForBudget(startingMoney):playerArmy,ai:aiArmy===undefined?(gentle?defaultArmyForBudget(startingMoney):defaultAIArmyForBudget(startingMoney,seed)):aiArmy};
  for(const [side,army]of Object.entries(armies)) {
    if(!Array.isArray(army)||army.length<1||army.length>8||army.filter(role=>role==='leader').length!==1||army.some(role=>!Number.isFinite(draftCost(role))))throw new Error(`${side==='player'?'Player 1':'Player 2'} must field 1–8 troops with exactly one commander.`);
    if(armyCost(army)>startingMoney)throw new Error(`${side==='player'?'Player 1':'Player 2'} army exceeds ${startingMoney} starting coins.`);
    armies[side]=army.slice();
  }
  const centerX=Math.floor(width/2),centerY=Math.floor(height/2),offset=Math.round(width/4);
  const slots=[[centerX,height-1],[centerX-2,height-2],[centerX+2,height-2],[centerX-1,height-1],[centerX+1,height-1],[centerX-2,height-1],[centerX+2,height-1],[centerX,height-2]];
  const deployments={player:playerDeployment,ai:aiDeployment};
  for(const side of SIDES) {
    const positions=deployments[side]===undefined?slots.slice(0,armies[side].length).map(([x,y])=>({x:side==='player'?x:width-1-x,y:side==='player'?y:height-1-y})):deployments[side];
    if(!Array.isArray(positions)||positions.length!==armies[side].length||positions.some(p=>!p||!Number.isInteger(p.x)||!Number.isInteger(p.y)||p.x<0||p.x>=width||p.y<0||p.y>=height||(side==='player'?p.y<height-2:p.y>1))||new Set(positions.map(p=>key(p.x,p.y))).size!==positions.length)throw new Error(`${side==='player'?'Player 1':'Player 2'} deployment must use unique squares in its own two home rows.`);
    deployments[side]=positions.map(({x,y})=>({x,y}));
  }
  const challenge=DIFFICULTIES[difficulty]||DIFFICULTIES.normal;
  const s = {
    width, height, map, mode, terrain, opponentMode, difficulty:challenge.key, startingMoney,armies,deployments,
    factions:{player:playerFaction,ai:aiFaction}, units:[], tiles:[], groundEffects:[],nextGroundId:0,
    flags:[{x:centerX-offset,y:centerY,name:'Dewlight',owner:null,contested:false},{x:centerX,y:centerY,name:'Heartlight',owner:null,contested:false},{x:centerX+offset,y:centerY,name:'Starlight',owner:null,contested:false}],
    activeSide:'player', scores:{player:0,ai:0}, supplies:{player:startingMoney-armyCost(armies.player),ai:startingMoney-armyCost(armies.ai)}, goal:mode==='elimination'?0:Math.round(goal??MODES[mode].goal),
    time:0, nextPulseAt:6, duration, nextAiAt:challenge.grace, aiRecruitAt:challenge.grace,
    winner:null, log:[], nextId:0, rng:(Number(seed)>>>0)||1
  };
  for (const side of SIDES) {
    armies[side].forEach((role,index)=>{const {x,y}=deployments[side][index];spawn(s,side,role,x,y,side==='player'||opponentMode==='local');});
  }
  generateTerrain(s);
  if(mode==='control')updateControl(s,[]);
  logEvent(s, {type:'start',x:centerX,y:height-1,text:`${FACTIONS[playerFaction].name} take the field. ${mode==='elimination'?'Defeat the enemy army.':`Control the flags and reach ${s.goal} light.`}`});
  return s;
}

function generateTerrain(s) {
  for(let y=0;y<s.height;y++)for(let x=0;x<s.width;x++)s.tiles.push({x,y,terrain:'grass'});
  const centerY=Math.floor(s.height/2),flagColumns=s.flags.map(f=>f.x);
  const protectedTile=(x,y)=>y<=1||y>=s.height-2||y===centerY||flagColumns.includes(x);
  const motif=MAPS[s.map];
  for(const [kind,points]of [['rock',motif.rocks],['water',motif.water],['grove',motif.groves]])for(const [baseX,baseY]of points) {
    const x=Math.round(baseX*(s.width-1)/8),y=Math.round(baseY*(s.height-1)/6);
    if(kind==='grove'||!protectedTile(x,y)) {
      tileAt(s,x,y).terrain=kind;
      tileAt(s,s.width-1-x,s.height-1-y).terrain=kind;
    }
  }
  const density={sparse:0.06,balanced:0.14,wild:0.22}[s.terrain],groveDensity={sparse:0.08,balanced:0.14,wild:0.22}[s.terrain];
  const candidates=s.tiles.filter(t=>!protectedTile(t.x,t.y)).map(t=>({...t,order:nextRandom(s)})).sort((a,b)=>a.order-b.order);
  const target=Math.round(s.width*s.height*density);
  const count=kind=>s.tiles.filter(t=>kind?t.terrain===kind:['rock','water'].includes(t.terrain)).length;
  // Add mirrored obstacles so every commander receives the same terrain opportunities.
  for(const t of candidates) {
    if(count()>=target)break;
    const mx=s.width-1-t.x,my=s.height-1-t.y;
    if(protectedTile(mx,my))continue;
    const kind=count('water')===0?'water':count('rock')===0?'rock':nextRandom(s)<0.35?'water':'rock';
    tileAt(s,t.x,t.y).terrain=kind;tileAt(s,mx,my).terrain=kind;
  }
  const groves=Math.round(s.width*s.height*groveDensity);
  for(const t of s.tiles.map(t=>({...t,order:nextRandom(s)})).sort((a,b)=>a.order-b.order)) {
    if(count('grove')>=groves)break;
    if(blocked(s,t.x,t.y))continue;
    tileAt(s,t.x,t.y).terrain='grove';
    const mx=s.width-1-t.x,my=s.height-1-t.y;
    if(!blocked(s,mx,my))tileAt(s,mx,my).terrain='grove';
  }
  // The cross and flag columns connect every deployment and objective. If a side
  // pocket was enclosed by random obstacles, carve its shortest horizontal exit.
  const visited=new Set(),queue=[{x:0,y:0}];
  for(let i=0;i<queue.length;i++) {
    const p=queue[i],k=key(p.x,p.y);if(visited.has(k)||blocked(s,p.x,p.y))continue;visited.add(k);
    for(const[dx,dy]of DIRS)if(inside(s,p.x+dx,p.y+dy))queue.push({x:p.x+dx,y:p.y+dy});
  }
  for(const t of s.tiles)if(!blocked(s,t.x,t.y)&&!visited.has(key(t.x,t.y))) {
    const targetX=flagColumns.slice().sort((a,b)=>Math.abs(a-t.x)-Math.abs(b-t.x))[0];
    for(let x=Math.min(t.x,targetX);x<=Math.max(t.x,targetX);x++) {
      tileAt(s,x,t.y).terrain='grass';
      tileAt(s,s.width-1-x,s.height-1-t.y).terrain='grass';
    }
  }
  // Deployment must not reroll terrain or erase a chosen grove. Both complete
  // home zones exclude obstacles; retain this safeguard for future map motifs.
  for(const u of s.units)if(blocked(s,u.x,u.y))tileAt(s,u.x,u.y).terrain='grass';
}

function logEvent(s,event,events) {
  if (event.text) { s.log.unshift(event.text); s.log.length=Math.min(s.log.length,80); }
  if (events) events.push(event);
}

function lineClear(s,a,b) {
  let x=a.x,y=a.y;
  const dx=Math.abs(b.x-x),dy=Math.abs(b.y-y),sx=x<b.x?1:-1,sy=y<b.y?1:-1;
  let error=dx-dy;
  while(x!==b.x || y!==b.y) {
    const e=error*2;
    if(e>-dy){error-=dy;x+=sx;}
    if(e<dx){error+=dx;y+=sy;}
    // Water blocks a shot travelling through it, not a visible flying target
    // hovering over the endpoint. Otherwise a legal water landing is invulnerable.
    if(blocked(s,x,y)&&!(x===b.x&&y===b.y&&tileAt(s,x,y).terrain==='water'&&at(s,x,y))) return false;
  }
  return true;
}

function reachable(s,u) {
  const max=u.rootedUntil>s.time?0:movementRange(s,u),queue=[{x:u.x,y:u.y,d:0}],best=new Map([[key(u.x,u.y),0]]),result=new Map(),phase=phasing(s,u);
  while(queue.length) {
    queue.sort((a,b)=>a.d-b.d);const p=queue.shift();
    if(p.d>=max||p.d!==best.get(key(p.x,p.y)))continue;
    for(const [dx,dy] of DIRS) {
      const x=p.x+dx,y=p.y+dy,k=key(x,y),d=p.d+movementCost(s,u,x,y);
      if(d>max||d>=(best.get(k)??Infinity)||movementBlocked(s,u,x,y))continue;
      const occupant=at(s,x,y);
      if(occupant&&occupant.side!==u.side&&!phase)continue;
      best.set(k,d);const next={x,y,d};queue.push(next);
      if(!occupant&&!movementBlocked(s,u,x,y,true))result.set(k,next);
    }
  }
  return [...result.values()];
}

function blinkSpot(s,u,v) {
  return DIRS.map(([dx,dy])=>({x:v.x+dx,y:v.y+dy})).filter(p=>!blocked(s,p.x,p.y)&&(!at(s,p.x,p.y)||at(s,p.x,p.y)?.id===u.id))
    .sort((a,b)=>distance(a,u)-distance(b,u)||a.y-b.y||a.x-b.x)[0];
}

function skillTargets(s,u,skillIndex=0) {
  const skill=skillDef(u,skillIndex), allies=living(s,u.side), enemies=living(s,other(u.side));
  if(skill.target==='ground')return s.tiles.filter(p=>distance(u,p)<=skill.range&&!blocked(s,p.x,p.y)&&(skill.kind!=='detonate'||groundEffectsAt(s,p.x,p.y).some(effect=>effect.side===u.side&&['rift','cinders'].includes(effect.kind)))).map(p=>({x:p.x,y:p.y}));
  if(skill.target==='empty')return u.rootedUntil>s.time?[]:s.tiles.filter(p=>distance(u,p)>0&&distance(u,p)<=skill.range&&!at(s,p.x,p.y)&&!movementBlocked(s,u,p.x,p.y,true)).map(p=>({x:p.x,y:p.y}));
  if(skill.target==='ally')return allies.filter(v=>distance(u,v)<=skill.range&&(skill.kind==='germinate'?(v.hp<v.maxHp||v.readyAt>s.time&&friendlyField(s,v,'mycelium')):skill.kind==='aegislink'?(v.shield<5||v.rootedUntil>s.time||v.cursed):v.id!==u.id&&u.rootedUntil<=s.time&&!movementBlocked(s,u,v.x,v.y,true)&&!movementBlocked(s,v,u.x,u.y,true))).map(v=>({x:v.x,y:v.y,targetId:v.id}));
  if(skill.target==='self')return allies.some(v=>distance(u,v)<=skill.range&&v.shield<5)||skill.kind==='grinward'&&enemies.some(v=>distance(u,v)<=1)?[{x:u.x,y:u.y,targetId:u.id}]:[];
  if(['broodlings','pocketmaws','bellmotes'].includes(skill.kind))return allies.length>=8?[]:s.tiles.filter(p=>distance(u,p)<=skill.range&&!blocked(s,p.x,p.y)&&!at(s,p.x,p.y)).map(p=>({x:p.x,y:p.y}));
  if(skill.kind==='seedgrove')return s.tiles.filter(p=>distance(u,p)<=skill.range&&!blocked(s,p.x,p.y)&&!at(s,p.x,p.y)&&p.terrain!=='grove').map(p=>({x:p.x,y:p.y}));
  if(skill.kind==='nectar')return allies.filter(v=>distance(u,v)<=skill.range&&v.hp<v.maxHp).map(v=>({x:v.x,y:v.y,targetId:v.id}));
  if(skill.kind==='wardray')return allies.filter(v=>distance(u,v)<=skill.range&&v.shield<5).map(v=>({x:v.x,y:v.y,targetId:v.id}));
  if(skill.kind==='starstep')return u.rootedUntil>s.time?[]:s.tiles.filter(p=>distance(u,p)>0&&distance(u,p)<=skill.range&&!at(s,p.x,p.y)&&!movementBlocked(s,u,p.x,p.y,true)).map(p=>({x:p.x,y:p.y}));
  if(['bloomcall','wrongnote','concordance','sanctuary'].includes(skill.kind)) {
    const useful=skill.kind==='bloomcall'?allies.some(v=>distance(u,v)<=skill.range&&v.hp<v.maxHp):
      skill.kind==='wrongnote'?enemies.some(v=>distance(u,v)<=skill.range):
      skill.kind==='concordance'?allies.some(v=>v.id!==u.id&&distance(u,v)<=skill.range&&(v.readyAt>s.time||v.shield<3)):
      allies.some(v=>distance(u,v)<=skill.range&&v.shield<5);
    return useful?[{x:u.x,y:u.y,targetId:u.id}]:[];
  }
  if(skill.kind==='sporehop') {
    if(u.rootedUntil>s.time) return [];
    return s.tiles.filter(p=>distance(u,p)>0&&distance(u,p)<=skill.range&&!blocked(s,p.x,p.y)&&!at(s,p.x,p.y)).map(p=>({x:p.x,y:p.y}));
  }
  if(skill.kind==='mercy') return allies.filter(v=>distance(u,v)<=skill.range&&(v.hp<v.maxHp||v.rootedUntil>s.time||v.cursed)).map(v=>({x:v.x,y:v.y,targetId:v.id}));
  return enemies.filter(v=>distance(u,v)<=skill.range&&lineClear(s,u,v)&&
    (!['ray','sunlance'].includes(skill.kind)||u.x===v.x||u.y===v.y)&&
    (skill.kind!=='laughinghex'||!v.cursed)&&
    (skill.kind!=='crookedswap'||u.rootedUntil<=s.time&&!movementBlocked(s,u,v.x,v.y,true)&&!movementBlocked(s,v,u.x,u.y,true))&&
    (skill.kind!=='blinkbite'||u.rootedUntil<=s.time&&blinkSpot(s,u,v))).map(v=>({x:v.x,y:v.y,targetId:v.id}));
}

export function legalActions(s,unitId) {
  const u=s.units.find(v=>v.id===unitId);
  if(s.winner||!u||u.hp<=0) return [];
  const def=unitDef(u), actions=[];
  if(u.readyAt<=s.time) {
    for(const p of reachable(s,u))actions.push({type:'move',unitId,x:p.x,y:p.y,label:'Move'});
    for(const v of living(s,other(u.side))) if(distance(u,v)<=def.range&&lineClear(s,u,v))
      actions.push({type:'attack',unitId,x:v.x,y:v.y,targetId:v.id,label:'Attack'});
    if(u.shield<5)actions.push({type:'guard',unitId,x:u.x,y:u.y,targetId:u.id,label:'Guard'});
  }
  for(let skillIndex=0;skillIndex<def.abilities.length;skillIndex++)if(skillReadyAt(u,skillIndex)<=s.time)for(const target of skillTargets(s,u,skillIndex))actions.push({type:'ability',unitId,skillIndex,...target,label:def.abilities[skillIndex].name});
  return actions;
}

function capture(s,u,events) {
  if(s.mode==='control')return;
  const flag=s.flags.find(f=>f.x===u.x&&f.y===u.y);
  if(!flag||flag.owner===u.side) return;
  flag.owner=u.side;
  logEvent(s,{type:'capture',unitId:u.id,x:u.x,y:u.y,text:`${unitDef(u).name} claimed ${flag.name}.`},events);
  if(u.faction==='bloom') heal(s,u,2,events);
}

function updateControl(s,events) {
  if(s.mode!=='control')return;
  for(const flag of s.flags) {
    const nearby=living(s).filter(u=>distance(u,flag)<=1);
    const player=nearby.filter(u=>u.side==='player').length,ai=nearby.length-player;
    const owner=player>ai?'player':ai>player?'ai':null,contested=player===ai&&player>0;
    if(flag.owner===owner&&flag.contested===contested)continue;
    flag.owner=owner;flag.contested=contested;
    logEvent(s,{type:'capture',side:owner,x:flag.x,y:flag.y,text:contested?`${flag.name} is contested.`:owner?`${FACTIONS[s.factions[owner]].name} control ${flag.name}.`:`${flag.name} is uncontrolled.`},events);
    if(owner) {
      const claimant=nearby.filter(u=>u.side===owner&&u.faction==='bloom').sort((a,b)=>distance(a,flag)-distance(b,flag))[0];
      if(claimant)heal(s,claimant,2,events);
    }
  }
}

function heal(s,u,value,events) {
  const restored=Math.min(value,u.maxHp-u.hp);
  if(restored<=0) return;
  u.hp+=restored;
  logEvent(s,{type:'heal',unitId:u.id,x:u.x,y:u.y,value:restored,text:`${unitDef(u).name} recovered ${restored} HP.`},events);
}

function shield(s,u,value,cap,events) {
  const gained=Math.max(0,Math.min(value,cap-u.shield));
  if(!gained)return;
  u.shield+=gained;
  logEvent(s,{type:'shield',unitId:u.id,x:u.x,y:u.y,value:gained,text:`${unitDef(u).name} gained ${gained} shield.`},events);
}

function damage(s,source,target,value,events,{ranged=false,pierce=0,groundKind}={}) {
  if(target.hp<=0)return;
  if(target.cursed){value+=2;target.cursed=0;}
  if(ranged&&tileAt(s,target.x,target.y).terrain==='grove')value=Math.max(1,value-1);
  if(ranged&&target.faction==='choir'&&formationStatus(s,target).active)value=Math.max(1,value-1);
  const absorbed=Math.min(target.shield,Math.max(0,value-pierce));
  target.shield-=absorbed;
  const dealt=Math.min(target.hp,value-absorbed);
  target.hp-=dealt;
  logEvent(s,{type:'damage',unitId:target.id,sourceId:source.id,x:target.x,y:target.y,value:dealt,absorbed,...(groundKind?{groundKind}:{}),text:`${unitDef(source).name} hit ${unitDef(target).name} for ${dealt}${absorbed?` (${absorbed} shield absorbed)`:''}.`},events);
  if(target.hp<=0) {
    logEvent(s,{type:'death',unitId:target.id,x:target.x,y:target.y,text:`${unitDef(target).name} fell.`},events);
  }
}

function relocate(s,u,x,y,events,kind='move') {
  const fromX=u.x,fromY=u.y;
  u.x=x;u.y=y;
  logEvent(s,{type:kind,unitId:u.id,x,y,fromX,fromY,text:`${unitDef(u).name} moved to ${String.fromCharCode(65+x)}${s.height-y}.`},events);
  capture(s,u,events);
  groundArrival(s,u,events);
}

function groundArrival(s,u,events){
  for(const effect of groundEffectsAt(s,u.x,u.y)){
    if(effect.side===u.side)continue;
    const source=s.units.find(v=>v.id===effect.sourceId);if(!source)continue;
    if(effect.kind==='cinders')damage(s,source,u,1,events,{groundKind:effect.kind});
    if(effect.kind==='rift')u.cursed=1;
  }
}
function paintGround(s,u,kind,center,radius,events,{purge=false}={}){
  if(!Array.isArray(s.groundEffects))s.groundEffects=[];
  if(!Number.isInteger(s.nextGroundId))s.nextGroundId=0;
  for(const tile of s.tiles.filter(tile=>distance(tile,center)<=radius&&!blocked(s,tile.x,tile.y))){
    const previous=groundEffectsAt(s,tile.x,tile.y).find(effect=>effect.side===u.side&&effect.kind===kind);
    s.groundEffects=s.groundEffects.filter(effect=>effect.expiresAt>s.time&&(effect.x!==tile.x||effect.y!==tile.y||effect.side!==u.side&&!purge));
    const effect={id:`g${++s.nextGroundId}`,kind,x:tile.x,y:tile.y,side:u.side,sourceId:u.id,createdAt:s.time,expiresAt:s.time+GROUND_EFFECTS[kind].duration,nextTickAt:previous?.nextTickAt>s.time?previous.nextTickAt:s.time+3};
    s.groundEffects.push(effect);
    logEvent(s,{type:'ground',kind,x:tile.x,y:tile.y,side:u.side,sourceId:u.id,expiresAt:effect.expiresAt,text:`${GROUND_EFFECTS[kind].name} marks ${String.fromCharCode(65+tile.x)}${s.height-tile.y} for ${GROUND_EFFECTS[kind].duration}s.`},events);
    const occupant=at(s,tile.x,tile.y);if(occupant&&!previous)groundArrival(s,occupant,events);
  }
}
function groundPulse(s,events){
  if(!Array.isArray(s.groundEffects))s.groundEffects=[];
  for(const effect of s.groundEffects){
    const source=s.units.find(u=>u.id===effect.sourceId);if(!source)continue;
    while(effect.nextTickAt<=s.time&&effect.nextTickAt<effect.expiresAt){
      const unit=at(s,effect.x,effect.y);
      if(unit){
        const friendly=unit.side===effect.side;
        if(friendly&&effect.kind==='mycelium')heal(s,unit,1,events);
        if(friendly&&effect.kind==='sanctum'){shield(s,unit,1,5,events);unit.rootedUntil=0;unit.cursed=0;}
        if(!friendly&&effect.kind==='rift')unit.cursed=1;
        if(!friendly&&(effect.kind==='cinders'||effect.kind==='thornfield'&&unitDef(unit).movement!=='Flight'))damage(s,source,unit,1,events,{groundKind:effect.kind});
      }
      effect.nextTickAt+=3;
    }
    if(effect.expiresAt<=s.time)logEvent(s,{type:'ground-expire',kind:effect.kind,x:effect.x,y:effect.y,side:effect.side},events);
  }
  s.groundEffects=s.groundEffects.filter(effect=>effect.expiresAt>s.time);
}

function useAbility(s,u,action,events) {
  const skillIndex=action.skillIndex??0,skill=skillDef(u,skillIndex), target=action.targetId?s.units.find(v=>v.id===action.targetId):null;
  logEvent(s,{type:'ability',unitId:u.id,skillIndex,kind:skill.kind,x:action.x,y:action.y,text:`${unitDef(u).name} used ${skill.name}.`},events);
  switch(skill.kind) {
    case 'mycelium':case 'thornfield':case 'rift':case 'cinders':case 'sanctum':case 'prismwall':paintGround(s,u,skill.kind,action,skill.radius,events);break;
    case 'dawnseal':paintGround(s,u,'sanctum',action,skill.radius,events,{purge:true});break;
    case 'rootguard':case 'grinward':{
      for(const ally of living(s,u.side))if(distance(u,ally)<=1)shield(s,ally,2,5,events);
      if(skill.kind==='grinward')for(const foe of living(s,other(u.side)))if(distance(u,foe)<=1)foe.rootedUntil=Math.max(foe.rootedUntil,s.time+1);
      break;
    }
    case 'pinshot':damage(s,u,target,2,events,{ranged:true});if(target.hp>0)target.rootedUntil=Math.max(target.rootedUntil,s.time+2.5);break;
    case 'crosscut':damage(s,u,target,3+(flankPartners(s,u,target).length?1:0),events);break;
    case 'sunlance':damage(s,u,target,3,events,{ranged:true,pierce:1});break;
    case 'pollenstep':case 'riftstep':{
      const origin={x:u.x,y:u.y},kind=skill.kind==='pollenstep'?'mycelium':'rift';relocate(s,u,action.x,action.y,events);
      paintGround(s,u,kind,origin,0,events);paintGround(s,u,kind,action,0,events);break;
    }
    case 'germinate':heal(s,target,3,events);if(friendlyField(s,target,'mycelium'))target.readyAt=s.time;break;
    case 'aegislink':shield(s,target,3,5,events);target.rootedUntil=0;target.cursed=0;break;
    case 'choirswap':{
      const origin={x:u.x,y:u.y},destination={x:target.x,y:target.y};
      u.x=destination.x;u.y=destination.y;target.x=origin.x;target.y=origin.y;
      for(const [troop,from]of [[u,origin],[target,destination]]){logEvent(s,{type:'move',unitId:troop.id,x:troop.x,y:troop.y,fromX:from.x,fromY:from.y,text:`${unitDef(troop).name} joined the procession.`},events);capture(s,troop,events);groundArrival(s,troop,events);if(troop.hp>0)shield(s,troop,2,5,events);}
      break;
    }
    case 'detonate':{
      s.groundEffects=(s.groundEffects||[]).filter(effect=>!(effect.side===u.side&&['rift','cinders'].includes(effect.kind)&&distance(effect,action)<=1));
      for(const enemy of living(s,other(u.side)))if(distance(enemy,action)<=1)damage(s,u,enemy,2,events);break;
    }
    case 'bloomcall': for(const v of living(s,u.side))if(distance(u,v)<=2)heal(s,v,3,events);break;
    case 'sporehop': tileAt(s,u.x,u.y).terrain='grove';relocate(s,u,action.x,action.y,events);break;
    case 'belch': {
      damage(s,u,target,2,events);
      if(target.hp>0) {
        target.rootedUntil=s.time+2.5;
        const candidates=DIRS.map(([dx,dy])=>({x:target.x+dx,y:target.y+dy})).filter(p=>distance(p,u)<distance(target,u)&&!blocked(s,p.x,p.y)&&!at(s,p.x,p.y)).sort((a,b)=>distance(a,u)-distance(b,u));
        if(candidates[0])relocate(s,target,candidates[0].x,candidates[0].y,events);
      } break;
    }
    case 'sporeburst': for(const v of living(s,other(u.side)))if(distance(v,action)<=1)damage(s,u,v,3,events);break;
    case 'wrongnote': for(const v of living(s,other(u.side)))if(distance(u,v)<=2){damage(s,u,v,2,events);if(v.hp>0)v.rootedUntil=s.time+2.5;}break;
    case 'blinkbite': {const p=blinkSpot(s,u,target);relocate(s,u,p.x,p.y,events);damage(s,u,target,3,events);break;}
    case 'foldhorn': {
      const dx=target.x-u.x,dy=target.y-u.y;
      const p={x:target.x+(Math.abs(dx)>=Math.abs(dy)?Math.sign(dx):0),y:target.y+(Math.abs(dx)<Math.abs(dy)?Math.sign(dy):0)};
      const open=!blocked(s,p.x,p.y)&&!at(s,p.x,p.y);
      damage(s,u,target,open?4:5,events);
      if(target.hp>0&&open)relocate(s,target,p.x,p.y,events);
      break;
    }
    case 'omen': damage(s,u,target,2,events);if(target.hp>0)target.cursed=1;break;
    case 'concordance': for(const v of living(s,u.side))if(v.id!==u.id&&distance(u,v)<=2){v.readyAt=s.time;shield(s,v,1,3,events);}break;
    case 'mercy': heal(s,target,4,events);target.rootedUntil=0;target.cursed=0;break;
    case 'sanctuary': for(const v of living(s,u.side))if(distance(u,v)<=1)shield(s,v,4,5,events);break;
    case 'ray': {
      const dx=Math.sign(target.x-u.x),dy=Math.sign(target.y-u.y);
      for(let n=1;n<=5;n++) {
        const x=u.x+dx*n,y=u.y+dy*n;
        if(!inside(s,x,y))break;
        const v=at(s,x,y);
        const barrier=blocked(s,x,y);
        if(barrier&&!(tileAt(s,x,y).terrain==='water'&&v))break;
        if(v&&v.side!==u.side)damage(s,u,v,3,events,{ranged:true});
        if(barrier)break;
      } break;
    }
    case 'seedgrove':tileAt(s,action.x,action.y).terrain='grove';break;
    case 'nectar':heal(s,target,2,events);break;
    case 'laughinghex':target.cursed=1;break;
    case 'crookedswap': {
      const ux=u.x,uy=u.y,vx=target.x,vy=target.y;
      u.x=vx;u.y=vy;target.x=ux;target.y=uy;
      for(const [troop,fromX,fromY]of [[u,ux,uy],[target,vx,vy]]) {
        logEvent(s,{type:'move',unitId:troop.id,x:troop.x,y:troop.y,fromX,fromY,text:`${unitDef(troop).name} swapped through the rift.`},events);
        capture(s,troop,events);
        groundArrival(s,troop,events);
      }
      break;
    }
    case 'starstep':relocate(s,u,action.x,action.y,events);shield(s,u,2,5,events);break;
    case 'wardray':shield(s,target,3,5,events);break;
    case 'broodlings':case 'pocketmaws':case 'bellmotes': {
      const positions=[{x:action.x,y:action.y},...DIRS.map(([dx,dy])=>({x:action.x+dx,y:action.y+dy}))];
      let count=0;
      for(const p of positions) {
        if(count>=2||living(s,u.side).length>=8)break;
        if(blocked(s,p.x,p.y)||at(s,p.x,p.y))continue;
        const tiny=spawn(s,u.side,'minion',p.x,p.y,true,u.faction);tiny.readyAt=s.time+1;
        logEvent(s,{type:'summon',sourceId:u.id,unitId:tiny.id,x:p.x,y:p.y,text:`${unitDef(u).name} summoned a ${unitDef(tiny).name}.`},events);
        capture(s,tiny,events);count++;
        groundArrival(s,tiny,events);
      }
      break;
    }
  }
  if(!Array.isArray(u.abilityReadyAts))u.abilityReadyAts=unitDef(u).abilities.map((_,i)=>i===0?u.abilityReadyAt:0);
  u.abilityReadyAts[skillIndex]=s.time+skill.cooldown;
  if(skillIndex===0)u.abilityReadyAt=u.abilityReadyAts[0];
}

function checkWinner(s,events) {
  if(s.winner)return;
  const player=living(s,'player'),ai=living(s,'ai');
  if(!player.length||!ai.length)s.winner=!player.length&&!ai.length?'draw':player.length?'player':'ai';
  else if(s.time>=s.duration||(s.mode!=='elimination'&&(s.scores.player>=s.goal||s.scores.ai>=s.goal))) {
    const p=s.scores.player-s.scores.ai;
    const hp=player.reduce((n,u)=>n+u.hp,0)-ai.reduce((n,u)=>n+u.hp,0);
    const flags=s.flags.filter(f=>f.owner==='player').length-s.flags.filter(f=>f.owner==='ai').length;
    const primary=s.mode==='elimination'?hp:p,secondary=s.mode==='elimination'?flags:hp;
    s.winner=primary>0?'player':primary<0?'ai':secondary>0?'player':secondary<0?'ai':'draw';
  }
  if(s.winner)logEvent(s,{type:'win',x:Math.floor(s.width/2),y:Math.floor(s.height/2),text:s.winner==='draw'?'The rift settles in a draw.':`${FACTIONS[s.factions[s.winner]].name} won the skirmish!`},events);
}

export function applyAction(s,requested) {
  if(!requested||typeof requested!=='object')return{ok:false,error:'Choose an action.',events:[]};
  if(requested.type==='recruit')return recruit(s,requested.role,requested.side||'player');
  if(requested.type==='ability'&&(!Number.isInteger(requested.skillIndex??0)||(requested.skillIndex??0)<0))return{ok:false,error:'Choose a valid special ability.',events:[]};
  const candidate=legalActions(s,requested.unitId).find(a=>a.type===requested.type&&a.x===requested.x&&a.y===requested.y&&(!requested.targetId||a.targetId===requested.targetId)&&(a.type!=='ability'||a.skillIndex===(requested.skillIndex??0)));
  if(!candidate)return{ok:false,error:'That action is not available.',events:[]};
  const u=s.units.find(v=>v.id===candidate.unitId),events=[];
  const sharedReadyAt=s.time+unitDef(u).moveCooldown;
  if(candidate.type==='move') {u.readyAt=sharedReadyAt;relocate(s,u,candidate.x,candidate.y,events);}
  if(candidate.type==='attack') {
    const target=s.units.find(v=>v.id===candidate.targetId),def=unitDef(u);
    u.readyAt=sharedReadyAt;
    const ranged=distance(u,target)>1;
    damage(s,u,target,def.damage+(u.faction==='chaos'&&u.hp<u.maxHp?1:0)+(u.faction==='chaos'&&flankPartners(s,u,target).length?1:0)+(ranged&&friendlyField(s,u,'prismwall')?1:0),events,{ranged});
  }
  if(candidate.type==='ability'){u.readyAt=Math.max(u.readyAt,sharedReadyAt);useAbility(s,u,candidate,events);}
  if(candidate.type==='guard'){u.readyAt=sharedReadyAt;shield(s,u,2,5,events);}
  updateControl(s,events);
  checkWinner(s,events);
  return{ok:true,events};
}

function passivePulse(s,events) {
  for(const u of living(s)) {
    if(u.faction==='bloom'&&tileAt(s,u.x,u.y).terrain==='grove')heal(s,u,1,events);
    const formation=formationStatus(s,u);
    if(u.faction==='bloom'&&formation.active)heal(s,u,1,events);
    if(u.faction==='choir'&&living(s,u.side).some(v=>v.id!==u.id&&distance(u,v)===1))shield(s,u,1,formation.active?5:3,events);
  }
}

export function recruit(s,role,side='player') {
  const events=[];
  if(!SIDES.includes(side))return{ok:false,error:'Choose a valid army.',events};
  if(s.winner)return{ok:false,error:'The skirmish is over.',events};
  if(s.mode!=='reinforcements')return{ok:false,error:'Buying reinforcements is only available in Reinforcement Clash.',events};
  if(!RECRUIT_ROLES.includes(role))return{ok:false,error:'Choose a non-commander troop to recruit.',events};
  const def=FACTIONS[s.factions[side]].units[role];
  if(living(s,side).length>=8)return{ok:false,error:'Your army is full (8 troops).',events};
  if(s.supplies[side]<def.cost)return{ok:false,error:`You need ${def.cost} supplies.`,events};
  const rows=side==='player'?[s.height-1,s.height-2]:[0,1];
  let spot=null;
  const columns=Array.from({length:s.width},(_,x)=>x).sort((a,b)=>Math.abs(a-(s.width-1)/2)-Math.abs(b-(s.width-1)/2)||a-b);
  for(const y of rows)for(const x of columns)if(!spot&&!blocked(s,x,y)&&!at(s,x,y))spot={x,y};
  if(!spot)return{ok:false,error:'Clear a tile in your deployment zone.',events};
  s.supplies[side]-=def.cost;
  const u=spawn(s,side,role,spot.x,spot.y,false);
  u.readyAt=s.time+def.moveCooldown;
  u.abilityReadyAts=def.abilities.map(skill=>s.time+skill.cooldown);
  u.abilityReadyAt=u.abilityReadyAts[0];
  logEvent(s,{type:'recruit',unitId:u.id,x:u.x,y:u.y,text:`${def.name} arrived. Ready in ${def.moveCooldown} seconds.`},events);
  groundArrival(s,u,events);
  updateControl(s,events);
  return{ok:true,events};
}

function plannedDistance(s,u,start,goal,cache) {
  const movement=unitDef(u).movement,phase=phasing(s,u),id=`${u.side}/${movement}/${phase}/${goal.x},${goal.y}`;
  if(!cache.has(id)) {
    // One reverse search serves every possible move toward this destination.
    // The occupied endpoint is an approach marker, including a flying enemy on
    // water: ground troops can reach the shore without planning a water landing.
    const field=new Map([[key(goal.x,goal.y),0]]),queue=[{x:goal.x,y:goal.y,d:0}];
    while(queue.length){
      queue.sort((a,b)=>a.d-b.d);const p=queue.shift();if(field.get(key(p.x,p.y))!==p.d)continue;
      for(const[dx,dy]of DIRS){
        const x=p.x+dx,y=p.y+dy,k=key(x,y),occupant=at(s,x,y),d=p.d+movementCost(s,u,p.x,p.y);
        if(d>=(field.get(k)??Infinity)||movementBlocked(s,u,x,y)||occupant&&occupant.side!==u.side&&!phase)continue;
        field.set(k,d);queue.push({x,y,d});
      }
    }
    cache.set(id,field);
  }
  return cache.get(id).get(key(start.x,start.y))??30;
}

function controlValue(s,u,position) {
  let value=0;
  for(const flag of s.flags) {
    let allies=0,foes=0;
    for(const troop of living(s))if(distance(troop.id===u.id?position:troop,flag)<=1) {
      if(troop.side===u.side)allies++;else foes++;
    }
    value+=Math.sign(allies-foes);
    // An extra defender matters under pressure, but empty friendly zones should
    // not draw the entire army away from other flags.
    if(allies>foes&&foes>0)value+=Math.min(1,allies-foes-1)*0.15;
  }
  return value;
}

function actionScore(s,u,a,cache) {
  const def=unitDef(u),foes=living(s,other(u.side)),allies=living(s,u.side),target=a.targetId?s.units.find(v=>v.id===a.targetId):null;
  if(a.type==='guard')return 0.3+(u.hp<u.maxHp/2?0.8:0);
  if(a.type==='attack') {
    const dmg=def.damage+(u.faction==='chaos'&&u.hp<u.maxHp?1:0)+(u.faction==='chaos'&&flankPartners(s,u,target).length?1:0)+(distance(u,target)>1&&friendlyField(s,u,'prismwall')?1:0)+(target.cursed?2:0);
    return 9+Math.min(target.hp,Math.max(0,dmg-target.shield))*2+(target.hp+target.shield<=dmg?13:0)+(s.flags.some(f=>distance(f,target)<=(s.mode==='control'?1:0))?s.mode==='control'?6:4:0);
  }
  if(a.type==='ability') {
    const skill=skillDef(u,a.skillIndex??0),kind=skill.kind;
    if(kind==='detonate')return foes.filter(v=>distance(v,a)<=1).reduce((score,v)=>score+10+(v.cursed?3:0)+(v.hp+v.shield<=2?8:0),-2);
    if(skill.target==='ground'){
      const affected=allies.filter(v=>distance(v,a)<=skill.radius),hostile=foes.filter(v=>distance(v,a)<=skill.radius);
      const existing=groundEffectsAt(s,a.x,a.y).find(effect=>effect.side===u.side&&effect.kind===skill.groundKind);
      if(existing&&existing.expiresAt-s.time>3)return -4;
      const objective=s.flags.some(flag=>distance(flag,a)<=1)?3:0;
      if(kind==='mycelium')return affected.reduce((score,v)=>score+Math.min(3,v.maxHp-v.hp)*3+(foes.some(foe=>distance(v,foe)<=3)?2:0.25),-2)+hostile.length*2+objective;
      if(kind==='thornfield'||kind==='cinders')return hostile.filter(v=>kind==='cinders'||unitDef(v).movement!=='Flight').reduce((score,v)=>score+9+(v.hp<=3?4:0),-2)+objective;
      if(kind==='rift')return hostile.reduce((score,v)=>score+(v.cursed?2:9),-2)+affected.length*1.5+objective;
      if(kind==='sanctum'||kind==='dawnseal')return affected.reduce((score,v)=>score+Math.min(3,5-v.shield)*(foes.some(foe=>distance(v,foe)<=3)?2:0.5)+(v.rootedUntil>s.time||v.cursed?4:0),-2)+(kind==='dawnseal'?(s.groundEffects||[]).filter(effect=>effect.expiresAt>s.time&&effect.side!==u.side&&distance(effect,a)<=1).length*4:0)+objective;
      return affected.filter(v=>unitDef(v).range>1).length*4+hostile.length*2+objective-2;
    }
    if(kind==='rootguard'||kind==='grinward')return allies.filter(v=>distance(u,v)<=1).reduce((score,v)=>score+Math.min(2,5-v.shield)*(foes.some(foe=>distance(v,foe)<=3)?3:0.4),-1)+(kind==='grinward'?foes.filter(v=>distance(u,v)<=1).length*5:0);
    if(kind==='pollenstep'||kind==='riftstep')return moveScore(s,u,a,cache)+1+(u.hp<u.maxHp&&kind==='pollenstep'?2:0);
    if(kind==='germinate')return Math.min(3,target.maxHp-target.hp)*4+(friendlyField(s,target,'mycelium')&&target.readyAt>s.time?5:0)-1;
    if(kind==='aegislink')return Math.min(3,5-target.shield)*(foes.some(v=>distance(v,target)<=3)?4:1)+(target.rootedUntil>s.time||target.cursed?4:0)-1;
    if(kind==='choirswap'){
      const swapped={...s,units:s.units.map(v=>v.id===u.id?{...v,x:target.x,y:target.y}:v.id===target.id?{...v,x:u.x,y:u.y}:v)};
      return (formationStatus(swapped,swapped.units.find(v=>v.id===u.id)).active?6:0)+(formationStatus(swapped,swapped.units.find(v=>v.id===target.id)).active?6:0)+Math.min(2,5-u.shield)+Math.min(2,5-target.shield)-2;
    }
    if(['pinshot','crosscut','sunlance'].includes(kind)){
      const value=kind==='pinshot'?2:3+(kind==='crosscut'&&flankPartners(s,u,target).length?1:0);
      return 9+Math.min(target.hp,Math.max(0,value-target.shield+(kind==='sunlance'?1:0)))*2+(target.hp+target.shield<=value?12:0)+(kind==='pinshot'&&target.rootedUntil<=s.time?2:0);
    }
    if(['broodlings','pocketmaws','bellmotes'].includes(kind))return 9+Math.min(2,8-allies.length)*3+(s.flags.some(f=>f.owner!==u.side&&distance(f,a)<=1)?7:0)+(foes.some(v=>distance(v,a)<=3)?3:0);
    if(kind==='seedgrove')return 2+allies.filter(v=>distance(v,a)<=1).reduce((n,v)=>n+(v.hp<v.maxHp?3:1),0)+(s.flags.some(f=>distance(f,a)<=1)?3:0);
    if(kind==='nectar')return Math.min(2,target.maxHp-target.hp)*5-1;
    if(kind==='laughinghex')return 12+(target.hp<=5?3:0);
    if(kind==='crookedswap')return 8+allies.filter(v=>v.id!==u.id&&distance(v,u)<=2).length*3+(s.flags.some(f=>f.x===target.x&&f.y===target.y&&f.owner!==u.side)?8:0);
    if(kind==='starstep')return moveScore(s,u,a,cache)+2;
    if(kind==='wardray')return Math.min(3,5-target.shield)*(foes.some(v=>distance(v,target)<=3)?5:1)-1;
    if(kind==='bloomcall')return allies.filter(v=>distance(u,v)<=2).reduce((n,v)=>n+Math.min(3,v.maxHp-v.hp)*4,0)-1;
    if(kind==='mercy')return Math.min(4,target.maxHp-target.hp)*4+(target.rootedUntil>s.time?3:0)+(target.cursed?2:0)-1;
    if(kind==='concordance')return allies.filter(v=>v.id!==u.id&&distance(u,v)<=2).reduce((n,v)=>n+(v.readyAt>s.time?8:0)+(v.shield<3?1:0),0)-1;
    if(kind==='sanctuary')return allies.filter(v=>distance(u,v)<=1).reduce((n,v)=>n+Math.min(4,5-v.shield)*(foes.some(e=>distance(v,e)<=3)?2:0.35),0)-1;
    if(kind==='sporehop')return moveScore(s,u,a,cache)+1;
    if(kind==='sporeburst')return foes.filter(v=>distance(v,a)<=1).reduce((n,v)=>n+8+Math.min(v.hp,Math.max(0,3-v.shield))*2+(v.hp+v.shield<=3?12:0),0);
    if(kind==='wrongnote')return foes.filter(v=>distance(u,v)<=2).reduce((n,v)=>n+11+(v.hp+v.shield<=2?12:0),0);
    if(kind==='ray'){
      const dx=Math.sign(target.x-u.x),dy=Math.sign(target.y-u.y);
      return foes.filter(v=>(dx?v.y===u.y&&Math.sign(v.x-u.x)===dx:v.x===u.x&&Math.sign(v.y-u.y)===dy)&&distance(u,v)<=5&&lineClear(s,u,v)).reduce((n,v)=>n+13+(v.hp+v.shield<=3?12:0),0);
    }
    const dmg=kind==='foldhorn'?4:kind==='blinkbite'?3:2;
    return 11+Math.min(target.hp,Math.max(0,dmg-target.shield))*2+(target.hp+target.shield<=dmg?13:0)+(kind==='omen'?3:0)+(kind==='belch'?3:0);
  }
  return moveScore(s,u,a,cache);
}

function moveScore(s,u,a,cache) {
  const targets=s.mode==='elimination'?[]:s.flags.filter(f=>f.owner!==u.side),foes=living(s,other(u.side));
  let score=-1;
  if(targets.length) {
    const zone=s.mode==='control'?1:0;
    const before=Math.min(...targets.map(f=>Math.max(0,plannedDistance(s,u,u,f,cache)-zone)));
    const after=Math.min(...targets.map(f=>Math.max(0,plannedDistance(s,u,a,f,cache)-zone)));
    score+=(before-after)*3;
    if(targets.some(f=>distance(f,a)<=zone))score+=zone?12:24;
  } else {
    // Follow traversable routes instead of repeatedly guarding behind a wall.
    const before=Math.min(...foes.map(v=>plannedDistance(s,u,u,v,cache))),after=Math.min(...foes.map(v=>plannedDistance(s,u,a,v,cache)));
    score+=(before-after)*1.4;
  }
  if(s.mode==='control')score+=(controlValue(s,u,a)-controlValue(s,u,u))*24;
  const standing=s.flags.find(f=>f.x===u.x&&f.y===u.y&&f.owner===u.side);
  if(standing&&foes.some(v=>distance(v,standing)<=3))score-=5;
  const threat=foes.filter(v=>distance(v,a)<=unitDef(v).range).reduce((n,v)=>n+unitDef(v).damage,0);
  if(!['story','easy'].includes(s.difficulty))score-=threat*(u.hp<5?1.1:0.25);
  if(foes.some(v=>distance(v,a)<=unitDef(u).range&&lineClear(s,a,v)))score+=4;
  if(u.faction==='bloom'&&tileAt(s,a.x,a.y).terrain==='grove'&&u.hp<u.maxHp)score+=1.5;
  for(const effect of groundEffectsAt(s,a.x,a.y)){
    if(effect.side!==u.side){if(effect.kind==='cinders')score-=u.hp<=3?10:5;if(effect.kind==='thornfield'&&unitDef(u).movement!=='Flight')score-=u.hp<=3?8:4;if(effect.kind==='rift'&&!u.cursed)score-=2;}
    else{if(effect.kind==='mycelium'&&u.hp<u.maxHp)score+=1.5;if(effect.kind==='sanctum'&&u.shield<3)score+=1;if(effect.kind==='prismwall'&&unitDef(u).range>1)score+=1;}
  }
  if(u.faction!=='chaos'){
    const next={...u,x:a.x,y:a.y},positioned={...s,units:s.units.map(v=>v.id===u.id?next:v)};
    score+=(formationStatus(positioned,next).active?1.5:0)-(formationStatus(s,u).active?1:0);
  }else score+=foes.some(foe=>flankPartners(s,{...u,x:a.x,y:a.y},foe).length)?2:0;
  return score;
}

export function chooseAIAction(s) {
  if(s.winner||s.opponentMode==='local')return null;
  const army=living(s,'ai');
  if(s.mode==='reinforcements'&&army.length<7&&s.time>=s.aiRecruitAt&&s.supplies.ai>=4) {
    const roster=army.map(u=>u.role),desired=['shaper','ranger','guardian','mystic','brute','scout'],role=desired.find(role=>!roster.includes(role)&&s.supplies.ai>=FACTIONS[s.factions.ai].units[role].cost)||'scout';
    const rows=[0,1],room=s.tiles.some(t=>rows.includes(t.y)&&!blocked(s,t.x,t.y)&&!at(s,t.x,t.y));
    if(room) {s.aiRecruitAt=s.time+6;return{type:'recruit',role,side:'ai'};}
  }
  const ranked=[],cache=new Map();
  for(const u of army)for(const action of legalActions(s,u.id)) {
    const noise=(nextRandom(s)-0.5)*({story:8,easy:5,normal:2,hard:0.4,expert:0.1}[s.difficulty]??2);
    let score=actionScore(s,u,action,cache);
    if(['story','easy'].includes(s.difficulty)) {
      const foes=living(s,'player'),target=action.targetId?s.units.find(v=>v.id===action.targetId):null;
      const kind=action.type==='ability'?skillDef(u,action.skillIndex??0).kind:null;
      const offensive=action.type==='attack'||action.type==='ability'&&(skillDef(u,action.skillIndex??0).color==='attack'||['belch','sporeburst','wrongnote','blinkbite','foldhorn','omen','ray','laughinghex','crookedswap'].includes(kind));
      const adjacent=target&&target.side!==u.side?distance(u,target)<=1:foes.some(v=>distance(u,v)<=1);
      if(offensive&&!adjacent)score=Math.min(score*(s.difficulty==='story'?0.2:0.4),s.difficulty==='story'?4:5);
      if(action.type==='guard'&&u.shield<5&&foes.some(v=>distance(u,v)<=3))score+=foes.some(v=>distance(u,v)<=1)?2:s.difficulty==='story'?13:11;
      if(action.type==='guard'&&u.shield>=5)score-=5;
      if(action.type==='move'&&s.flags.some(f=>f.owner!==u.side&&f.x===action.x&&f.y===action.y))score+=s.difficulty==='story'?8:5;
    }
    ranked.push({action,score:score+noise});
  }
  ranked.sort((a,b)=>b.score-a.score);
  return ranked[0]?.score>0.15?ranked[0].action:null;
}

/** Advance only while the app is playing. Large/background deltas cannot skip battles. */
export function advance(s,dtSeconds) {
  if(s.winner||!Number.isFinite(dtSeconds)||dtSeconds<=0)return[];
  const events=[];
  s.time=Math.min(s.duration,Math.round((s.time+Math.min(1,dtSeconds))*1e6)/1e6);
  groundPulse(s,events);
  updateControl(s,events);
  while(s.nextPulseAt<=s.time) {
    for(const side of SIDES) {
      const held=s.flags.filter(f=>f.owner===side).length;
      if(s.mode!=='elimination')s.scores[side]+=held;
      if(s.mode==='reinforcements')s.supplies[side]=Math.min(40,s.supplies[side]+2+held);
      if(held&&s.mode!=='elimination')logEvent(s,{type:'score',side,x:Math.floor(s.width/2),y:Math.floor(s.height/2),value:held,text:`${FACTIONS[s.factions[side]].name} gained ${held} light.`},events);
    }
    passivePulse(s,events);
    s.nextPulseAt+=6;
    checkWinner(s,events);
    if(s.winner)return events;
  }
  checkWinner(s,events);
  if(s.winner)return events;
  if(s.opponentMode!=='local'&&s.time>=s.nextAiAt) {
    const action=chooseAIAction(s);
    if(action)events.push(...applyAction(s,action).events);
    s.nextAiAt=s.time+(DIFFICULTIES[s.difficulty]||DIFFICULTIES.normal).interval;
  }
  return events;
}

export const RULES = {
  actions: 'Each troop has its own move/attack cooldown of 2–3.5 seconds. Move, attack, or guard when ready; specials recharge independently.',
  beacons: 'Enter a beacon to claim it. Ownership lasts until an enemy enters. Every 6 seconds each held beacon gives 1 light. First to 30 light wins.',
  terrain: 'Rocks and water block ground movement and sight. Groves reduce ranged basic attacks and Judgment Ray damage by 1, to a minimum of 1.',
  movement: 'Brood starting in groves move 1 extra tile. Chaos scouts and rangers phase through enemies; friendly rifts grant phasing and +1 movement for a move begun there. Choir guardians and brutes are grounded; other Choir troops fly, landing on empty ground or water, never rocks.',
  formations: 'Brood link 3 troops edge-to-edge for +1 movement and 1 healing every 6s. Carnival allies directly opposite each other across a foe deal +1 basic damage to that foe. Choir line up 3 consecutive troops for 1 less ranged damage and resonance shielding up to 5.',
  ground: 'Six painted ground effects last 10–12s and tick every 3s. Mycelium heals, briars sting ground foes, rifts curse, cinders burn, sanctuaries shield and cleanse, and lattices strengthen ranged basics. Enemy mycelium and lattice steps cost 2 movement. New paint replaces your previous effect on that cell; opposing fields may coexist. Leave, wait out, detonate or cleanse fields for counterplay.',
  modes: 'Beacon Rush keeps claimed flags. Zone Control awards majority presence within one orthogonal tile; ties are contested. Last Army Standing awards no light; defeat the enemy or lead in remaining HP at the time limit.',
  opponents: 'Face an AI commander or share the screen in local two-player. Local armies act independently with the same cooldowns; both start ready.',
  reinforcements: 'Only Reinforcement Clash allows purchases and gains 2 coins plus 1 per held beacon every 6 seconds, up to 40. Unspent starting coins carry into battle. Recruit up to 8 living troops.',
  drafting: 'Choose from 7 troops per faction and field 1–8, including exactly one commander. Commanders cost 6; scouts 3; brutes, guardians and shapers 5; mystics and rangers 4. Arrange your home two rows. Start with 24 coins, or choose a custom budget of 12–40.',
  abilities: 'Specials have independent 8–12 second cooldowns and work while movement is recovering. Broodlings, Pocket Maws, and Bell Motes summon up to 2 tiny troops, army cap 8. Roots last 2.5 seconds. Guard gives 2 shield, up to 5.',
  victory: 'Win at 30 light or by defeating the entire enemy army. At 180 seconds, most light wins; remaining HP breaks a tie.'
};
