/** MOBA Mania: all tunable rules and original, code-drawn hero identities. */
const ability = (id, name, description, icon, color, unlock, mana, cooldown, range, radius, type) =>
  ({ id, name, description, icon, color, unlock, mana, cooldown, range, radius, type });
const choice = (id, name, description, bonuses) => ({ id, name, description, bonuses });
const tier = (level, a, b) => ({ level, choices: [a, b] });
export const HEROES = {
  arc: {
    id:'arc', name:'Arc Rift', title:'The Azure Warden', role:'Mage · control', color:'#64caff',
    description:'Golden armor, a blue-flame staff, living storms and an army of spirits.',
    hp:680, mana:340, speed:178, range:245, damage:39, attackRate:0.82,
    abilities:[
      ability('blue-storm','Blue Storm','Aim a storm. After 0.55s it explodes, then pulses for 3s.','storm','#64caff',1,42,7,400,100,'ground'),
      ability('forcefield','Forcefield','Place a 5s ward: shield allies and slow enemies inside.','field','#81e0ff',2,48,13,340,120,'ground'),
      ability('spirit-warriors','Spirit Warriors','Summon three blue spirits to fight for 14s.','spirit','#a9edff',4,65,19,260,70,'ground'),
      ability('rift-step','Rift Step','Blink toward your aim and unleash an exploding blue nova.','blink','#c5afff',6,60,15,260,110,'ground')
    ],
    talents:[
      tier(3,choice('arc-thunder','Thunderheart','Blue Storm deals 35% more damage.',{stormDamage:.35}),choice('arc-spring','Azure Spring','Forcefields also heal allies for 15 health each second.',{fieldHeal:15})),
      tier(5,choice('arc-reach','Long Conduit','Basic attacks and aimed spells reach 20% farther.',{range:.2}),choice('arc-reserve','Ley Reservoir','Gain 30% maximum mana and 3 extra mana regeneration.',{mana:.3,manaRegen:3})),
      tier(8,choice('arc-legion','Spirit Legion','Summon two additional spirit warriors.',{spiritCount:2}),choice('arc-tempo','Stormcaller','All ability cooldowns are 18% shorter.',{cooldown:.18})),
      tier(10,choice('arc-ascend','Blue Ascension','All damage increases 20%; blue flames burn brighter.',{damage:.2}),choice('arc-eternal','Eternal Aegis','Shields are 45% stronger and maximum health rises 15%.',{shield:.45,hp:.15}))
    ]
  },
  dragon: {
    id:'dragon', name:'Red Dragon Warrior', title:'The Cinder Knight', role:'Frontline · bruiser', color:'#ff7455',
    description:'Lead the charge with a straight-line dragon, close-range fire and draconic armor.',
    hp:950, mana:270, speed:171, range:54, damage:58, attackRate:0.86,
    abilities:[
      ability('dragon-flight','Dragon Flight','Send a flaming dragon in a straight line. It pierces and burns.','dragon','#ff9451',1,38,7.5,470,27,'line'),
      ability('flame-ring','Flame Ring','Ignite the area around you and burn nearby enemies.','flame','#ffbd59',2,42,9,0,125,'self'),
      ability('dragon-shield','Dragon Shield','Reduce incoming damage by 50% for 5s and gain a shield.','shield','#ffd481',4,48,16,0,90,'self'),
      ability('sky-cleave','Sky Cleave','Charge along your aim, then slam and stun enemies for 1s.','slash','#ff674b',6,62,16,255,100,'line')
    ],
    talents:[
      tier(3,choice('dragon-embers','Endless Embers','Burning lasts 2s longer and deals 35% more damage.',{burnDuration:2,burnDamage:.35}),choice('dragon-vigor','Drake Vigor','Gain 20% maximum health.',{hp:.2})),
      tier(5,choice('dragon-hunt','Dragon Hunter','Deal 35% more damage to jungle creatures and beasts.',{jungleDamage:.35}),choice('dragon-haste','Emberstride','Move 12% faster and attack 15% faster.',{speed:.12,attackSpeed:.15})),
      tier(8,choice('dragon-inferno','Wild Inferno','Flame Ring radius increases 30% and damage increases 25%.',{flameRadius:.3,flameDamage:.25}),choice('dragon-shell','Ancient Scales','Dragon Shield lasts 3s longer and its shield is 50% stronger.',{guardDuration:3,shield:.5})),
      tier(10,choice('dragon-doom','Worldfire','All damage increases 20%.',{damage:.2}),choice('dragon-renew','Phoenix Blood','Heal 8% maximum health after casting an ability.',{castHeal:.08}))
    ]
  },
  druid: {
    id:'druid', name:'Emerald Druid', title:'Keeper of the Grove', role:'Summoner · jungle', color:'#73e6a2',
    description:'Grow healing trees and call treants, a mighty bear and a hunting wolf pack.',
    hp:730, mana:360, speed:176, range:215, damage:37, attackRate:0.82,
    abilities:[
      ability('healing-tree','Healing Tree','Grow a tree for 8s. Allies in its grove recover health each second.','tree','#9af0af',1,40,12,340,115,'ground'),
      ability('treants','Treant Guardians','Call two treants at your aim to fight and soak damage for 16s.','treant','#a9c876',2,52,16,250,65,'ground'),
      ability('bear','Ancient Bear','Summon a powerful bear for 18s. Its blows slow enemies.','bear','#d3b57a',4,65,22,200,70,'ground'),
      ability('wolves','Moonlit Pack','Summon three swift wolves for 15s to chase and surround foes.','wolf','#b3ecd2',6,70,22,260,75,'ground')
    ],
    talents:[
      tier(3,choice('druid-roots','Living Roots','Healing trees also slow enemies by 30%.',{treeSlow:.3}),choice('druid-bloom','Deep Bloom','Healing is 40% stronger.',{healing:.4})),
      tier(5,choice('druid-wild','Wild Bond','Summons gain 30% more health and damage.',{summonPower:.3}),choice('druid-cycle','Natural Cycle','Gain 4 extra mana regeneration and 10% movement speed.',{manaRegen:4,speed:.1})),
      tier(8,choice('druid-twins','Twin Ancients','Ancient Bear calls a second bear.',{bearCount:1}),choice('druid-forest','Old Forest','Healing trees last 5s longer; treants gain 5s lifetime.',{treeDuration:5,summonDuration:5})),
      tier(10,choice('druid-alpha','Alpha Pack','Moonlit Pack adds two wolves; summons attack 20% faster.',{wolfCount:2,summonHaste:.2}),choice('druid-heart','Heartwood','Gain 20% maximum health; all cooldowns are 15% shorter.',{hp:.2,cooldown:.15}))
    ]
  },
  angel: {
    id:'angel', name:'Pink Angel', title:'The Rose Seraph', role:'Support · sustain', color:'#ff9ed5',
    description:'Keep your team together with healing light, inspiration and shared barriers.',
    hp:690, mana:380, speed:182, range:225, damage:35, attackRate:0.8,
    abilities:[
      ability('rose-light','Rose Light','Place a 3s healing circle. It also scorches enemies with pink light.','heal','#ffc2e5',1,40,8,365,110,'ground'),
      ability('inspire','Inspire','Allies near your aim move 30% and attack 25% faster for 5s.','haste','#ffe1f3',2,45,13,330,145,'ground'),
      ability('sanctuary','Sanctuary','Shield everyone near your aim for 6s.','shield','#edb4ff',4,60,17,340,145,'ground'),
      ability('seraph-lance','Seraph Lance','Fire a piercing beam of light: hurt enemies and heal allies in its path.','lance','#ff8fcf',6,65,15,540,30,'line')
    ],
    talents:[
      tier(3,choice('angel-grace','Overflowing Grace','Healing is 35% stronger.',{healing:.35}),choice('angel-rebuke','Rose Rebuke','Ability damage increases 30%.',{spellDamage:.3})),
      tier(5,choice('angel-wings','Fleet Wings','Move 15% faster; Inspire lasts 2s longer.',{speed:.15,inspireDuration:2}),choice('angel-aura','Generous Light','Ground spells reach and cover 15% more area.',{range:.15,area:.15})),
      tier(8,choice('angel-glass','Roseglass','Shields are 50% stronger.',{shield:.5}),choice('angel-song','Unending Song','All ability cooldowns are 20% shorter.',{cooldown:.2})),
      tier(10,choice('angel-resolve','Radiant Resolve','Gain 25% health and 20% healing strength.',{hp:.25,healing:.2}),choice('angel-wrath','Seraphic Wrath','All damage rises 25%; attack 20% faster.',{damage:.25,attackSpeed:.2}))
    ]
  },
  ranger: {
    id:'ranger', name:'Dark Ranger', title:'The Thornshade', role:'Marksman · jungle', color:'#b3a4ff',
    description:'Aim venom arrows, spread volleys, stun bombs and piercing long shots.',
    hp:620, mana:285, speed:190, range:285, damage:44, attackRate:0.71,
    abilities:[
      ability('venom-arrow','Venom Arrow','Fire a skill shot that poisons its first target for 5s.','poison','#a5ef7c',1,30,5.5,490,13,'line'),
      ability('fan-arrows','Fan of Arrows','Loose five arrows in a spread. Each arrow finds its own target.','arrows','#cfbdff',2,40,8,390,12,'cone'),
      ability('stun-bomb','Stun Bomb','Lob a bomb to your aim. After 0.55s it stuns for 1.4s.','bomb','#d6a6ff',4,50,13,345,100,'ground'),
      ability('long-shot','Long Shot','A powerful piercing arrow with a long, narrow skill-shot path.','arrow','#f1e4ff',6,58,12,720,16,'line')
    ],
    talents:[
      tier(3,choice('ranger-venom','Virulent Venom','Poison lasts 3s longer and deals 30% more damage.',{poisonDuration:3,poisonDamage:.3}),choice('ranger-hunt','Monster Hunter','Deal 40% more damage to jungle creatures and beasts.',{jungleDamage:.4})),
      tier(5,choice('ranger-fleet','Shadowstep','Move 15% faster and gain 15% attack speed.',{speed:.15,attackSpeed:.15}),choice('ranger-eye','Eagle Eye','Basic attacks and aimed abilities reach 20% farther.',{range:.2})),
      tier(8,choice('ranger-volley','Thorn Barrage','Fan of Arrows fires two additional arrows.',{fanCount:2}),choice('ranger-trap','Concussive Powder','Stun Bomb radius increases 30%; stun lasts 0.6s longer.',{bombRadius:.3,stunDuration:.6})),
      tier(10,choice('ranger-deadeye','Deadeye','Long Shot deals 45% more damage.',{longshotDamage:.45}),choice('ranger-siphon','Night Siphon','Heal for 16% of basic-attack damage dealt.',{lifesteal:.16}))
    ]
  }
};
export const DIFFICULTIES = {
  easy:{id:'easy',name:'Relaxed',description:'Forgiving bots. Slower reactions, simple lane plans and generous retreats.',think:0.9,lead:0,retreat:.27,coordination:.4,aimError:85,dodge:false},
  normal:{id:'normal',name:'Standard',description:'Bots rotate, farm the jungle, defend towers and contest beasts.',think:.48,lead:.45,retreat:.3,coordination:.72,aimError:35,dodge:false},
  hard:{id:'hard',name:'Veteran',description:'Fast rotations, predictive skill shots, focus fire and careful tower dives.',think:.27,lead:.8,retreat:.34,coordination:.93,aimError:13,dodge:true},
  ultimate:{id:'ultimate',name:'Legend',description:'Sharp reactions, objective control and coordinated commitments. No hidden stat bonuses.',think:.15,lead:1,retreat:.38,coordination:1,aimError:3,dodge:true}
};
const point = (x,y) => ({x,y});
const lanes = [
  {id:0,name:'Top',points:[point(260,1100),point(430,735),point(650,420),point(1100,345),point(1600,330),point(2100,345),point(2550,420),point(2770,735),point(2940,1100)]},
  {id:1,name:'Middle',points:[point(260,1100),point(660,1100),point(1100,1100),point(1600,1100),point(2100,1100),point(2540,1100),point(2940,1100)]},
  {id:2,name:'Bottom',points:[point(260,1100),point(430,1465),point(650,1780),point(1100,1855),point(1600,1870),point(2100,1855),point(2550,1780),point(2770,1465),point(2940,1100)]}
];
const camps=[
  {id:'blue-boars',x:830,y:735,type:'boar',team:0}, {id:'blue-wolves',x:1160,y:1470,type:'wolf',team:0}, {id:'blue-golem',x:690,y:1480,type:'golem',team:0},
  {id:'rose-boars',x:2370,y:1465,type:'boar',team:1}, {id:'rose-wolves',x:2040,y:730,type:'wolf',team:1}, {id:'rose-golem',x:2510,y:720,type:'golem',team:1}
];
export function segmentDistance(x,y,a,b) {
  const dx=b.x-a.x,dy=b.y-a.y,t=Math.max(0,Math.min(1,((x-a.x)*dx+(y-a.y)*dy)/(dx*dx+dy*dy||1)));
  return Math.hypot(x-a.x-dx*t,y-a.y-dy*t);
}
export function lanePoint(lane,fraction) {
  const points=lanes[lane].points,lengths=points.slice(1).map((p,i)=>Math.hypot(p.x-points[i].x,p.y-points[i].y));
  let distance=lengths.reduce((a,b)=>a+b,0)*Math.max(0,Math.min(1,fraction));
  for(let i=0;i<lengths.length;i++){if(distance<=lengths[i]||i===lengths.length-1){const t=distance/lengths[i];return point(points[i].x+(points[i+1].x-points[i].x)*t,points[i].y+(points[i+1].y-points[i].y)*t);}distance-=lengths[i];}
}
const obstacles=[];
for(let side=0;side<2;side++) for(let gx=0;gx<10;gx++) for(let gy=0;gy<16;gy++) {
  const x=500+gx*88+(gy%2)*29, y=475+gy*82, px=side?3200-x:x;
  if(Math.abs(px-1600)<235||camps.some(c=>Math.hypot(px-c.x,y-c.y)<145))continue;
  if(lanes.some(l=>l.points.slice(1).some((p,i)=>segmentDistance(px,y,l.points[i],p)<122)))continue;
  // Meandering gaps join the camps to the lanes and river.
  if(Math.abs(y-735)<43||Math.abs(y-1470)<43||Math.abs(px-(side?2180:1020))<48)continue;
  const k=gx*31+gy*17+side*7;
  if(k%7===0)continue;
  obstacles.push({x:px+(k%17-8),y:y+(k%13-6),r:k%5===0?27:24,type:k%9===0?'rock':'tree'});
}
export const WORLD={width:3200,height:2200,lanes,river:[point(1560,110),point(1650,500),point(1570,900),point(1635,1300),point(1560,1700),point(1640,2090)],riverWidth:128,
  obstacles,brush:[{x:1300,y:710,r:72},{x:1900,y:1490,r:72},{x:1420,y:990,r:65},{x:1780,y:1210,r:65},{x:1100,y:570,r:65},{x:2100,y:1630,r:65}],
  bases:[{x:260,y:1100,well:{x:115,y:1100}},{x:2940,y:1100,well:{x:3085,y:1100}}],camps,
  beastPits:[{id:'river-maw',x:1610,y:705,lane:0,type:'maw'},{id:'moss-colossus',x:1590,y:1495,lane:2,type:'colossus'}]};
export const RULES={waveInterval:19,firstWave:4,firstBeast:55,beastInterval:100,campRespawn:44,maxLevel:12,recallTime:4,wellRadius:120};
