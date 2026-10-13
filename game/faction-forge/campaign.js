/** Campaign definitions and strategic selection shared by UI and server replay. */
export const CAMPAIGN_CHAPTERS = Object.freeze([
  {id:1,title:'Sparks on the Frontier',theme:'iron-frontier',description:'Draft a balanced army and learn to protect the Vanir gunline.'},
  {id:2,title:'The Aesir Awakening',theme:'crystal-sanctum',description:'Break magical defenses and control the rhythm of battle.'},
  {id:3,title:'The Blight Rising',theme:'blight-wastes',description:'Survive swarms, poison, burrowing ambushes and sustained pressure.'},
  {id:4,title:'The Broken Pact',theme:'contested-forge',description:'Adapt to layered counters and coordinated elite compositions.'},
  {id:5,title:'The Last Crucible',theme:'contested-forge',description:'Defeat each faction’s strongest combined strategies and claim the Forge.'}
]);
const R = (...entries) => entries.map(([type,count=1,lvl=1])=>({type,count,lvl}));
const definitions = [
  ['First Sparks','Vanir','Iron Recruit','balanced',R(['rifleman',1]),'Protect a frontline while your shooters attack.'],
  ['Hold the Line','Vanir','Gatekeeper','spearhead',R(['vguard',1],['rifleman',1]),'Draft a durable frontline before adding more backline.'],
  ['Long Sight','Vanir','SteelHawk','protected_backline',R(['marksman',1],['vguard',1]),'Use mobility to punish a protected sniper.'],
  ['Cover and Fire','Vanir','IronWarden','protected_backline',R(['suppressor',1],['rifleman',2]),'Control tools protect steady damage dealers.'],
  ['The Barrage','Vanir','FrostRanger','wide',R(['vrocket',1],['vguard',1],['medic',1]),'Spread your army and interrupt the artillery.'],
  ['Frontier Command','Vanir','Marshal Cinder','protected_backline',R(['shieldcap',1,2],['rifleman',2],['signalhawk',1]),'Boss: break the command drone’s coordinated gunline.'],
  ['The Crystal Gate','Aesir','Rune Watcher','spearhead',R(['guardian',1],['phaser',1]),'Anti-armor tools help break the crystal guard.'],
  ['Sanctuary','Aesir','Moonwarden','protected_backline',R(['wardmaker',1],['guardian',1],['phaser',1]),'Disrupt wards rather than trading into endless protection.'],
  ['Fractured Sky','Aesir','Stormcaller','wide',R(['storm',2],['blade',1]),'Keep swarms away from chained lightning.'],
  ['The Living Rampart','Aesir','Aegis Regent','spearhead',R(['golem',1],['architect',1],['runebinder',1]),'Find a way through a large tank and layered control.'],
  ['Mirror Crossing','Aesir','Prism Keeper','wide',R(['miragewarden',1],['phaser',2],['guardian',1]),'Area attacks punish fragile mirror duplicates.'],
  ['The Hourglass Throne','Aesir','The Timeless Queen','protected_backline',R(['chronoweaver',1,2],['blade',1,2],['wardmaker',1],['golem',1]),'Boss: collapse the time field before a sustained brawl.'],
  ['A Thousand Footsteps','Helions','AshenWolf','wide',R(['hellspawn',2],['roach',1]),'Area damage is vital against a swarm advance.'],
  ['Poisoned Ground','Helions','Toxin Keeper','spearhead',R(['defiler',1],['spitter',1],['roach',1]),'Avoid an armored brawl in acid and poison.'],
  ['Beneath the Rampart','Helions','Deepclaw','ambush',R(['burrower',2],['viper',1],['defiler',1]),'Control and detection protect a fragile backline.'],
  ['The Brood Chamber','Helions','Broodkeeper','protected_backline',R(['matron',1,2],['hellspawn',1],['defiler',1]),'Focus the summoner before its brood overwhelms you.'],
  ['Blood Tithe','Helions','CrimsonVale','spearhead',R(['siphonleech',2],['carrion',1],['blood',1]),'Wound and ranged burst limit sustained life drain.'],
  ['The Feeding Crown','Helions','The Blood Sovereign','spearhead',R(['siphonleech',1,2],['matron',1,2],['defiler',1,2],['carrion',1]),'Boss: combine anti-heal with support disruption.'],
  ['Broken Signals','Vanir','Silent General','ambush',R(['miner',2,2],['signalhawk',1,2],['marksman',1,2]),'Defend against flanks and coordinated priority fire.'],
  ['Railway to Ruin','Vanir','Rail Marshal','protected_backline',R(['railbreaker',1,2],['vguard',2,2],['suppressor',1]),'Avoid lining up behind armored targets.'],
  ['Reflections at War','Aesir','Mirror Marshal','wide',R(['miragewarden',1,2],['storm',1,2],['blade',1,2],['runebinder',1]),'Protect your supports while clearing decoys.'],
  ['Swarm Accord','Helions','Hive Negotiator','protected_backline',R(['hiveherald',1,2],['hellspawn',2,2],['matron',1,2]),'Break the commander’s pheromone formation.'],
  ['Clockwork Siege','Aesir','Fracture Lord','spearhead',R(['chronoweaver',1,3],['golem',1,2],['architect',1,2],['phaser',1,2]),'Use precise disruption against an entrenched temporal defense.'],
  ['The Shattered Pact','Aesir','Prismatic Regent','wide',R(['miragewarden',1,3],['chronoweaver',1,2],['blade',1,3],['storm',1,2]),'Boss: defeat both misdirection and tempo control.'],
  ['Forge of Iron','Vanir','The Iron Chancellor','protected_backline',R(['railbreaker',1,3],['signalhawk',1,3],['shieldcap',1,2],['rifleman',1,2],['emp',1,2]),'An elite gunline is vulnerable to well-supported dives.'],
  ['Voice of the Hive','Helions','The Crimson Herald','spearhead',R(['hiveherald',1,3],['matron',1,3],['hellspawn',2,2],['siphonleech',1,2]),'Kill the commander and clear newborn reinforcements.'],
  ['The Unending Hour','Aesir','Eternal Architect','protected_backline',R(['chronoweaver',1,3],['golem',1,3],['wardmaker',1,3],['phaser',1,2],['runebinder',1,2]),'Target the backline that sustains a powerful tank.'],
  ['The Last Ambush','Helions','The Underking','ambush',R(['viper',1,3],['burrower',1,3],['blood',1,2],['carrion',1,3],['defiler',1,2]),'Layer control and reveal threats before the ambush connects.'],
  ['Three Crowns','Vanir','Triune War Council','wide',R(['railbreaker',1,3],['signalhawk',1,2],['vguard',1,3],['medic',1,3],['vrocket',1,2]),'Recognize the opposing counters and adapt each draft.'],
  ['The Last Crucible','Helions','The Forge Sovereign','protected_backline',R(['hiveherald',1,3],['siphonleech',1,3],['matron',1,3],['defiler',1,3],['roach',1,3],['burrower',1,2]),'Final boss: defeat an elite army through counters and synergy.']
];
export const CAMPAIGN_UNLOCKS = Object.freeze({
  'forge-06':'signalhawk','forge-12':'chronoweaver','forge-18':'siphonleech',
  'forge-20':'railbreaker','forge-24':'miragewarden','forge-26':'hiveherald'
});

export const CAMPAIGN_MISSIONS = Object.freeze(definitions.map((entry,number)=>{
  const [title,faction,name,formation,roster,objective]=entry;
  const chapter=Math.floor(number/6)+1,index=number%6+1,boss=index===6;
  const skill=Number(Math.min(.96,.32+number*.021+(boss?.035:0)).toFixed(3));
  const id='forge-'+String(number+1).padStart(2,'0');
  const relics=chapter>=4?(faction==='Helions'?['swarm_instinct']:faction==='Aesir'?['arcane_battery']:['anti_armor_rounds']):[];
  const tactics=chapter>=4?(formation==='ambush'?['forced_march']:['bodyguard']):[];
  const unlockedTroops=[...new Set([
    ...Object.entries(CAMPAIGN_UNLOCKS).filter(([key])=>Number(key.slice(6))<=number+1).map(([,key])=>key),
    ...roster.map(r=>r.type).filter(key=>Object.values(CAMPAIGN_UNLOCKS).includes(key))
  ])];
  const opponent={name,faction,skill,difficulty:skill,formation,roster,relics,tactics,unlockedTroops};
  return Object.freeze({id,number:number+1,chapter,index,title,theme:CAMPAIGN_CHAPTERS[chapter-1].theme,
    boss,objective,rewards:{cards:boss?6:3,dust:boss?40:20},replayRewards:{cards:1,dust:5},
    unlockTroop:CAMPAIGN_UNLOCKS[id]||null,prerequisite:number?'forge-'+String(number).padStart(2,'0'):null,
    opponent,aiFaction:faction,difficulty:skill,startingRoster:roster});
}));
export const MISSIONS = CAMPAIGN_MISSIONS;

export function getMission(id) {
  const alias=/^c([1-5])m([1-6])$/i.exec(String(id));
  if (alias) id='forge-'+String((Number(alias[1])-1)*6+Number(alias[2])).padStart(2,'0');
  return CAMPAIGN_MISSIONS.find(m=>m.id===id)||null;
}
const missionProgress = progress => progress?.campaign?.missions || progress?.missions || {};
const completed = p => !!p && (p.completed===true || Number(p.stars)>0);
export function missionAccessible(id,progress) {
  const mission=getMission(id);if (!mission) return false;
  return !mission.prerequisite || completed(missionProgress(progress)[mission.prerequisite]);
}
export function nextMission(progress) {
  const records=missionProgress(progress);
  return CAMPAIGN_MISSIONS.find(m=>!completed(records[m.id])&&missionAccessible(m.id,progress))||null;
}
export function campaignCompletion(progress) {
  const records=missionProgress(progress);
  const count=CAMPAIGN_MISSIONS.filter(m=>completed(records[m.id])).length;
  const stars=CAMPAIGN_MISSIONS.reduce((sum,m)=>sum+Math.max(0,Math.min(3,Number(records[m.id]?.stars)||0)),0);
  return {completed:count,total:30,stars,maxStars:90,percent:Math.round(count/30*100),endlessUnlocked:count===30};
}
export function missionStars({won,lives=0}={}) {
  if (!won) return 0;
  return lives>=3 ? 3 : lives>=2 ? 2 : 1;
}
export function unlocksForMission(id) {const mission=getMission(id);return mission?.unlockTroop?[mission.unlockTroop]:[];}

/** Both sides begin a campaign with an actual deployed army. Without this,
 * a prepared boss plus three draft cards fights a player's three-card army.
 * Starting battle ranks are scenario preparation, independent of Forge tiers. */
export function playerStartingRoster(faction,mission) {
  if(!mission)return [];
  const chapter=Math.max(1,Math.min(6,mission.chapter||1));
  const types={Vanir:['vguard','rifleman','medic','vrocket','signalhawk'],
    Aesir:['guardian','phaser','wardmaker','storm','chronoweaver'],
    Helions:['defiler','roach','carrion','matron','hiveherald']}[faction];
  if(!types)return [];
  const tankRank=chapter>=5?3:chapter>=3?2:1;
  const damageRank=chapter>=6?3:chapter>=4?2:1;
  const roster=[{type:types[0],count:1,lvl:tankRank},{type:types[1],count:1,lvl:damageRank}];
  if(chapter>=2 || mission.number===1 || mission.boss)roster.push({type:types[2],count:1,lvl:chapter>=5?2:1});
  if(chapter>=3)roster.push({type:types[3],count:1,lvl:chapter>=5?2:1});
  if(chapter>=6)roster.push({type:types[4],count:1,lvl:2});
  // Helions' sustain and swarm engines need their later commanders to answer
  // elite frontlines. These are only deployed after the player earned the unlock.
  if(faction==='Helions'&&chapter>=5)roster.push({type:'siphonleech',count:1,lvl:2});
  if(faction==='Helions'&&chapter===5&&mission.number>=27)roster.push({type:'hiveherald',count:1,lvl:2});
  return roster;
}

export function createEndlessMission(stage=1) {
  stage=Math.max(1,Math.min(10000,Math.floor(Number(stage)||1)));
  const templates=[CAMPAIGN_MISSIONS[24],CAMPAIGN_MISSIONS[26],CAMPAIGN_MISSIONS[29],CAMPAIGN_MISSIONS[27],CAMPAIGN_MISSIONS[23]];
  const template=templates[(stage-1)%templates.length];
  // Counter strategy and composition rotate; deployment remains bounded by the grid.
  const roster=template.opponent.roster.map((r,index)=>({...r,lvl:Math.min(3,r.lvl+Math.floor(stage/5)),count:Math.min(3,r.count+((stage+index)%7===0?1:0))}));
  const formation=['protected_backline','wide','spearhead','ambush'][(stage-1)%4];
  const skill=Math.min(.985,.9+stage*.004);
  return {id:'endless-'+stage,stage,number:30+stage,chapter:6,index:stage,title:'Crucible Challenge '+stage,
    theme:template.theme,boss:stage%5===0,objective:'Defeat rotating elite compositions before Crucible pressure begins after '+Math.round(Math.max(8000,60000-stage*450)/1000)+' seconds.',
    prerequisite:'forge-30',rewards:{cards:stage%5===0?2:1,dust:stage%5===0?15:10},replayRewards:{cards:1,dust:5},unlockTroop:null,
    aiFaction:template.aiFaction,difficulty:skill,startingRoster:roster,
    pressure:{startsAfterMs:Math.max(8000,60000-stage*450),intervalMs:Math.max(600,3000-stage*12),damage:1+Math.floor((stage-1)/30)},
    opponent:{...template.opponent,name:template.opponent.name+' '+stage,roster,formation,skill,difficulty:skill,unlockedTroops:Object.values(CAMPAIGN_UNLOCKS)}};
}

/** Endless raises urgency after tactical AI/army strength reaches its bounded
 * ceiling. Pressure is a declared mission hazard, not hidden enemy HP buffs. */
export function prepareBattle(st,mission,now=st.field?.startedAt||0) {
  if(!mission?.stage||!st.field)return;
  const pressure=mission.pressure||createEndlessMission(mission.stage).pressure;
  st.field.endlessPressure={stage:mission.stage,p:0,nextAt:now+pressure.startsAfterMs,
    intervalMs:pressure.intervalMs,damage:pressure.damage,startsAfterMs:pressure.startsAfterMs};
}
export function tickCampaignHazards(st,now) {
  const pressure=st.field?.endlessPressure;
  if(!pressure||now<pressure.nextAt)return;
  for(const u of st.field.units||[])if(u.hp>0&&u.p===pressure.p&&!u.illusion){u.hp-=pressure.damage;u.statusShockUntil=Math.max(u.statusShockUntil||0,now+500);}
  pressure.nextAt=now+pressure.intervalMs;
}

const fronts=new Set(['vguard','miner','shieldcap','blade','guardian','golem','hellspawn','defiler','blood','viper','burrower','siphonleech']);
const support=new Set(['medic','wardmaker','architect','matron','carrion','signalhawk','chronoweaver','miragewarden','hiveherald']);
const splash=new Set(['vrocket','storm','defiler','blood']);
const assassins=new Set(['miner','blade','viper','burrower']);
const tanks=new Set(['vguard','guardian','golem','shieldcap','defiler']);
const swarm=new Set(['hellspawn','matron','hiveherald']);
const countMatching=(roster,keys)=>(roster||[]).reduce((sum,r)=>sum+(keys.has(r.type)?Math.max(1,Number(r.count)||1):0),0);
const skillFor = value => typeof value==='number' ? Math.max(0,Math.min(1,value)) : ({easy:.4,normal:.65,medium:.65,hard:.9,expert:.98}[value]??.65);

/** Selects an index from the actual issued pack. The server and local AI use the
 * existing game's evaluator plus targeted support/formation/upgrade scoring. */
export function scoreAIDraft(card,context={},hooks={}) {
  const mine=context.myRoster||[],enemy=context.enemyRoster||[];
  let score=Number(hooks.evaluateDraftCard?.(card,mine,enemy,context.myRelics||[],context.enemyRelics||[])?.score)||0;
  const mineFront=countMatching(mine,fronts),mineSwarm=countMatching(mine,swarm),mineSupports=countMatching(mine,support);
  const enemyFront=countMatching(enemy,fronts),enemySwarm=countMatching(enemy,swarm),enemyTanks=countMatching(enemy,tanks);
  const enemySplash=countMatching(enemy,splash),enemyAssassins=countMatching(enemy,assassins);
  if (card.type==='troop') {
    if (!hooks.evaluateDraftCard) {score+=fronts.has(card.key)&&mineFront<2?3:0;score+=splash.has(card.key)&&enemySwarm>=2?4:0;score+=assassins.has(card.key)&&countMatching(enemy,support)>=1?3:0;}
    if (card.key==='railbreaker') score+=enemyTanks*1.3-(enemySwarm>=2?2:0);
    if (card.key==='signalhawk') score+=(mine.length>=2?1.5:-2)+(enemy.some(r=>r.type==='burrower')?2:0);
    if (card.key==='chronoweaver') score+=(mine.length>=2?2:-2)+(enemyFront>=3?1:0);
    if (card.key==='miragewarden') score+=enemyAssassins*1.5+mineSupports*.5-enemySplash;
    if (card.key==='siphonleech') score+=(enemyFront>0?1.5:0)-(enemy.some(r=>r.type==='carrion')?2:0);
    if (card.key==='hiveherald') score+=mineSwarm>=2?3.5:-2;
    if (support.has(card.key)&&mineSupports>=3) score-=2;
    score+=.15; // prefer a useful body over unrelated special cards
  } else if (card.type==='upgrade') {
    const own=mine.find(r=>r.type===(card.for||card.key));
    score+=own?2+Math.min(3,Number(own.count)||1)*.75+((own.lvl||1)===2?2:0):-10;
  } else if (card.type==='formation') {
    if (card.key==='wide') score+=enemySplash>=1?3:0;
    if (card.key==='spearhead') score+=countMatching(mine,tanks)>=2?2:0;
    if (card.key==='protected_backline') score+=mineSupports>=1&&enemyAssassins>=1?3:0;
    if (card.key==='ambush') score+=countMatching(mine,assassins)>=2?3:0;
    if (card.key===context.formation) score-=3;
  } else if (card.type==='relic') {
    if (card.key==='swarm_instinct') score+=mineSwarm>=2?3:-2;
    if (card.key==='arcane_battery') score+=mine.some(r=>['wardmaker','architect','storm','chronoweaver'].includes(r.type))?2:-2;
    if (card.key==='anti_armor_rounds') score+=enemyTanks>=2?2.5:0;
    if ((context.myRelics||[]).includes(card.key)) score-=4;
  } else if (card.type==='tactic') {
    if (card.key==='bodyguard') score+=mineSupports&&enemyAssassins?2:0;
    if (card.key==='sabotage') score+=enemyTanks>=2?2:0;
    if ((context.myTactics||[]).includes(card.key)) score-=4;
  }
  return score;
}

export function chooseAIDraft(pack,context={},rng=Math.random,hooks={}) {
  if (!Array.isArray(pack)||!pack.length) return -1;
  const skill=skillFor(context.difficulty??context.skill);
  const ranked=pack.map((card,index)=>({index,score:scoreAIDraft(card,context,hooks)})).sort((a,b)=>b.score-a.score||a.index-b.index);
  // Believable occasional mistakes use a seeded RNG, never invent a draft card.
  if (ranked.length>1 && rng()<(1-skill)*.32) return ranked[Math.min(ranked.length-1,1+Math.floor(rng()*(ranked.length-1)))].index;
  const best=ranked.filter(r=>Math.abs(r.score-ranked[0].score)<.05);
  return best[Math.min(best.length-1,Math.floor(rng()*best.length))].index;
}
export const AI_NAMES=Object.freeze(['IronWarden','AshenWolf','FrostRanger','CrimsonVale','SteelHawk']);
export function practiceOpponent(mmr=1200,rng=Math.random) {
  const rating=Math.max(100,Math.round(Number(mmr)||1200));
  return {name:AI_NAMES[Math.min(AI_NAMES.length-1,Math.floor(rng()*AI_NAMES.length))],isAI:true,
    estimatedMmr:Math.max(100,rating+Math.round((rng()-.5)*120)),difficulty:Math.max(.35,Math.min(.95,.55+(rating-1200)/2000)),
    faction:['Vanir','Aesir','Helions'][Math.min(2,Math.floor(rng()*3))],ranked:false};
}
