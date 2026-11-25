/** MOBA Mania: all tunable rules and original, code-drawn hero identities. */
const ability = (id, name, description, icon, color, unlock, mana, cooldown, range, radius, type) =>
  ({ id, name, description, icon, color, unlock, mana, cooldown, range, radius, type });
const talentIcons = {stormDamage:'storm',fieldHeal:'heal',range:'range',mana:'mana',manaRegen:'mana',spiritCount:'spirit',cooldown:'cooldown',damage:'damage',shield:'shield',hp:'health',burnDuration:'flame',burnDamage:'flame',jungleDamage:'hunt',speed:'speed',attackSpeed:'haste',flameRadius:'flame',guardDuration:'fortress',castHeal:'heal',treeSlow:'tree',healing:'heal',summonPower:'treant',bearCount:'bear',treeDuration:'tree',wolfCount:'wolf',spellDamage:'storm',poisonDuration:'poison',fanCount:'arrows',bombRadius:'bomb',longshotDamage:'arrow',lifesteal:'lifesteal'};
const choice = (id, name, description, bonuses, icon) => ({ id, name, description, bonuses, icon:icon||talentIcons[Object.keys(bonuses)[0]]||'shield', color:'#c4dcff' });
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
  },
  bulwark: {
    id:'bulwark', name:'Purple Knight', title:'The Amethyst Bulwark', role:'Tank · displacement', aiRole:'laner', color:'#b394ff',
    description:'A towering purple knight who protects allies, shoves enemies aside and locks down the frontline.',
    hp:1220, mana:300, speed:159, range:64, damage:50, attackRate:1.02,
    abilities:[
      {...ability('shield-bash','Shield Bash','Sweep your shield forward: damage, push enemies back and stun them for 0.8s.','bash','#c2a3ff',1,32,7,115,115,'cone'),arc:Math.PI*.58},
      ability('amethyst-guard','Amethyst Guard','Gain a strong shield and 40% damage reduction for 5s. Nearby allies receive half the shield.','fortress','#d1bcff',2,48,15,0,150,'self'),
      {...ability('rampart-rush','Rampart Rush','Charge forward, damaging and pushing enemies in your path. Stops at terrain.','charge','#9f82ed',4,45,12,235,38,'line'),mobility:true},
      ability('royal-quake','Royal Quake','Slam the ground: push nearby enemies, stun for 1.25s, then leave them slowed.','quake','#d9b4ff',6,70,22,0,165,'self')
    ],
    talents:[
      tier(3,choice('bulwark-granite','Granite Heart','Gain 18% maximum health.',{hp:.18},'health'),choice('bulwark-impact','Crushing Impact','Shield Bash pushes 55 farther; your stuns last 0.3s longer.',{bashPush:55,knightStun:.3},'bash')),
      tier(5,choice('bulwark-shelter','Royal Shelter','Amethyst Guard shares 80% of its shield with allies.',{allyBarrier:.3},'fortress'),choice('bulwark-march','Relentless March','Move 12% faster; Rampart Rush travels 20% farther.',{speed:.12,rushDistance:.2},'charge')),
      tier(8,choice('bulwark-fault','Fault Line','Royal Quake covers 25% more area and deals 25% more damage.',{quakeRadius:.25,quakeDamage:.25},'quake'),choice('bulwark-bastion','Living Bastion','Shields are 35% stronger; Amethyst Guard lasts 2s longer.',{shield:.35,guardDuration:2},'shield')),
      tier(10,choice('bulwark-crown','Unbroken Crown','Gain 22% maximum health and 12% shorter cooldowns.',{hp:.22,cooldown:.12},'fortress'),choice('bulwark-avalanche','Purple Avalanche','All damage rises 20%; your stuns last 0.35s longer.',{damage:.2,knightStun:.35},'quake'))
    ]
  },
  ember: {
    id:'ember', name:'Orange Assassin', title:'The Cinderblade', role:'Assassin · mobility', aiRole:'jungler', color:'#ffad56',
    description:'An orange-cloaked duelist who darts through openings, lands precise single-target hits and finishes wounded foes.',
    hp:660, mana:300, speed:204, range:53, damage:64, attackRate:.7,
    abilities:[
      ability('heartseeker','Heartseeker','Thrust at the first enemy along your aim for heavy single-target damage.','dagger','#ffc879',1,30,5.5,150,19,'line'),
      {...ability('cinder-dash','Cinder Dash','Dash toward your aim. Cut the first enemy you cross and gain 40% speed for 2s.','dash','#ff9b43',2,35,10,275,25,'line'),mobility:true},
      ability('execution-cut','Execution Cut','A precise single-target cut. Damage increases as the victim loses health.','execute','#ffce8f',4,48,10,165,24,'line'),
      {...ability('flicker-strike','Flicker Strike','Rush to the first enemy along your aim and strike hard. A kill immediately refreshes Cinder Dash.','ambush','#ffe0a4',6,65,18,310,28,'line'),mobility:true}
    ],
    talents:[
      tier(3,choice('ember-edge','Keen Edge','Heartseeker deals 30% more damage.',{heartseekerDamage:.3},'dagger'),choice('ember-siphon','Ember Siphon','Basic attacks heal for 12% of damage dealt.',{lifesteal:.12},'lifesteal')),
      tier(5,choice('ember-dancer','Cinder Dancer','Movement skills travel 15% farther; Cinder Dash speed lasts 1s longer.',{dashDistance:.15,dashDuration:1},'dash'),choice('ember-hunter','Solitary Hunter','Deal 35% more damage to jungle creatures and beasts; gain 10% health.',{jungleDamage:.35,hp:.1},'hunt')),
      tier(8,choice('ember-finish','Merciless Finish','Execution Cut deals 30% more damage.',{executionDamage:.3},'execute'),choice('ember-tempo','Dancing Blades','Attack 25% faster; ability cooldowns are 12% shorter.',{attackSpeed:.25,cooldown:.12},'haste')),
      tier(10,choice('ember-vanish','Flickering Death','Flicker Strike deals 30% more damage and grants a shield on a kill.',{flickerDamage:.3,killBarrier:160},'ambush'),choice('ember-survivor','Last Ember','Gain 25% health and heal 5% maximum health after each cast.',{hp:.25,castHeal:.05},'health'))
    ]
  },
  seraph: {
    id:'seraph', name:'Blue Angel', title:'The Winterwing', role:'Sword · inspire · frost', aiRole:'support', color:'#79caff',
    description:'A blue-winged warrior with a massive sword, sweeping frost attacks, swift movement and inspiring battle cries.',
    hp:840, mana:350, speed:190, range:79, damage:55, attackRate:.78,
    abilities:[
      {...ability('glacial-sweep','Glacial Sweep','Swing your greatsword in a wide forward arc. Enemies take damage and are slowed 35% for 2.6s.','frostblade','#b5e8ff',1,34,7,150,150,'cone'),arc:Math.PI*.85},
      {...ability('wingburst','Wingburst','Surge toward your aim, gain a shield and move 45% faster for 3.5s. Stops at terrain.','wing','#8bd5ff',2,40,12,240,70,'line'),mobility:true},
      ability('northern-inspire','Northern Inspire','Inspire allies around your aim: 25% movement speed, 30% attack speed and a small shield for 5s.','rally','#b9edff',4,55,15,280,175,'ground'),
      ability('winterfall','Winterfall','Freeze a circle at your aim: damage and briefly stun enemies, then slow them by 50% for 4s.','snowflake','#d4f4ff',6,72,20,320,140,'ground')
    ],
    talents:[
      tier(3,choice('seraph-edge','Frozen Edge','Glacial Sweep deals 25% more damage and slows for 1s longer.',{sweepDamage:.25,frostDuration:1},'frostblade'),choice('seraph-plume','Azure Plume','Gain 15% maximum health and 15% stronger shields.',{hp:.15,shield:.15},'wing')),
      tier(5,choice('seraph-tailwind','Tailwind','Move 12% faster; Wingburst haste lasts 1.5s longer.',{speed:.12,wingDuration:1.5},'wing'),choice('seraph-choir','Northern Choir','Northern Inspire covers 25% more area and lasts 2s longer.',{inspireArea:.25,inspireDuration:2},'rally')),
      tier(8,choice('seraph-deepfreeze','Deep Freeze','Winterfall stuns for 0.35s longer and slows 15% more.',{freezeStun:.35,frostSlow:.15},'snowflake'),choice('seraph-broadblade','Greatsword Mastery','Glacial Sweep covers 20% more area; gain 15% attack speed.',{sweepRadius:.2,attackSpeed:.15},'frostblade')),
      tier(10,choice('seraph-blizzard','Blizzard Heart','Winterfall deals 30% more damage; all cooldowns are 12% shorter.',{winterDamage:.3,cooldown:.12},'snowflake'),choice('seraph-vanguard','Heavenly Vanguard','Gain 20% health and 30% stronger shields.',{hp:.2,shield:.3},'shield'))
    ]
  }
};
// Each tier offers three mutually exclusive specializations. Icons are shared by
// menu previews, active abilities and the talent tree, without external assets.
const extraTalents = {
  arc:[
    choice('arc-spark','Focused Spark','Gain 18% ability damage and 1 extra mana regeneration.',{spellDamage:.18,manaRegen:1},'storm'),
    choice('arc-ward','Warden Training','Gain 16% health and 15% stronger shields.',{hp:.16,shield:.15},'shield'),
    choice('arc-leech','Siphoning Current','Basic attacks heal for 12% of damage; attack 12% faster.',{lifesteal:.12,attackSpeed:.12},'lifesteal'),
    choice('arc-riftmaster','Rift Master','Cooldowns are 12% shorter; attacks and spells reach 15% farther.',{cooldown:.12,range:.15},'blink')
  ],
  dragon:[
    choice('dragon-cleave','Battle Claws','Deal 12% more damage and attack 10% faster.',{damage:.12,attackSpeed:.1},'slash'),
    choice('dragon-recovery','Kindled Vitality','Heal 3.5% maximum health after casting an ability.',{castHeal:.035},'heal'),
    choice('dragon-pursuit','Winged Pursuit','Move 10% faster; attacks and aimed abilities reach 15% farther.',{speed:.1,range:.15},'wing'),
    choice('dragon-colossus','Cinder Colossus','Gain 25% health and 20% stronger shields.',{hp:.25,shield:.2},'fortress')
  ],
  druid:[
    choice('druid-bark','Ironbark','Gain 16% maximum health and 1 extra mana regeneration.',{hp:.16,manaRegen:1},'tree'),
    choice('druid-forage','Wild Forager','Deal 35% more jungle damage and 10% more damage overall.',{jungleDamage:.35,damage:.1},'hunt'),
    choice('druid-spores','Restoring Spores','Cooldowns are 13% shorter and healing is 15% stronger.',{cooldown:.13,healing:.15},'heal'),
    choice('druid-guardians','Eternal Guardians','Summons gain 22% health and damage and live 4s longer.',{summonPower:.22,summonDuration:4},'treant')
  ],
  angel:[
    choice('angel-focus','Quiet Devotion','Gain 20% maximum mana and 1.5 extra mana regeneration.',{mana:.2,manaRegen:1.5},'mana'),
    choice('angel-vow','Guardian Vow','Gain 18% health and 15% stronger shields.',{hp:.18,shield:.15},'shield'),
    choice('angel-dawn','Dawn Herald','Ability damage rises 20%; aimed abilities reach 12% farther.',{spellDamage:.2,range:.12},'lance'),
    choice('angel-renew','Renewing Faith','Heal 5% health after each cast; cooldowns are 10% shorter.',{castHeal:.05,cooldown:.1},'heal')
  ],
  ranger:[
    choice('ranger-fangs','Sharpened Fangs','Deal 15% more damage and attack 8% faster.',{damage:.15,attackSpeed:.08},'arrow'),
    choice('ranger-alchemy','Field Alchemy','Gain 25% maximum mana and 2 extra mana regeneration.',{mana:.25,manaRegen:2},'mana'),
    choice('ranger-tracker','Relentless Tracker','Poison deals 25% more damage; cooldowns are 10% shorter.',{poisonDamage:.25,cooldown:.1},'poison'),
    choice('ranger-sentinel','Shadow Sentinel','Gain 20% health; attacks and aimed abilities reach 15% farther.',{hp:.2,range:.15},'range')
  ],
  bulwark:[
    choice('bulwark-discipline','Shield Discipline','All cooldowns are 10% shorter; gain 10% stronger shields.',{cooldown:.1,shield:.1},'shield'),
    choice('bulwark-stamina','Deep Stamina','Gain 25% mana and 2 extra mana regeneration.',{mana:.25,manaRegen:2},'mana'),
    choice('bulwark-recover','Second Wind','Heal 5% maximum health after every cast.',{castHeal:.05},'heal'),
    choice('bulwark-thunder','Thunder March','Rampart Rush deals 45% more damage; move 12% faster.',{rushDamage:.45,speed:.12},'charge')
  ],
  ember:[
    choice('ember-opportunist','Opportunist','Move 10% faster and attack 12% faster.',{speed:.1,attackSpeed:.12},'speed'),
    choice('ember-reserve','Hidden Reserve','Gain 25% mana and 2 extra mana regeneration.',{mana:.25,manaRegen:2},'mana'),
    choice('ember-flow','Searing Flow','Heartseeker and Cinder Dash deal 20% more damage; gain 12% health.',{heartseekerDamage:.2,dashDamage:.2,hp:.12},'dagger'),
    choice('ember-flurry','Endless Flurry','Deal 18% more damage; move 12% faster.',{damage:.18,speed:.12},'dash')
  ],
  seraph:[
    choice('seraph-reserve','Crystal Focus','Gain 20% mana and 2 extra mana regeneration.',{mana:.2,manaRegen:2},'mana'),
    choice('seraph-duelist','Sky Duelist','Deal 12% more damage and attack 12% faster.',{damage:.12,attackSpeed:.12},'frostblade'),
    choice('seraph-cadence','Battle Hymn','Cooldowns are 15% shorter; Inspire shields are 40% stronger.',{cooldown:.15,inspireShield:.4},'rally'),
    choice('seraph-horizon','Endless Horizon','Attacks and spells reach 20% farther; move 10% faster.',{range:.2,speed:.1},'wing')
  ]
};
for (const hero of Object.values(HEROES)) {
  hero.talents.forEach((talent,index)=> {
    talent.choices.push(extraTalents[hero.id][index]);
    for (const option of talent.choices) option.color=hero.color;
  });
}

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
