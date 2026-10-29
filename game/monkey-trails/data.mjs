// Monkey Trails content catalogue. IDs are save-stable; balance lives here.
import {FORGE_GEAR} from './equipment-data.mjs';
import {ELEMENTAL_TECHNIQUES,SPECIES_SIGNATURES,TACTICAL_TECHNIQUES,LIVING_SPECIES_ROWS,ROLE_ASCENSIONS} from './living-combat-data.mjs';
export const ELEMENTS = {
  leaf:{name:'Leaf',icon:'❧',color:'#78d99e',direction:'N',resource:'Canopy seeds',beats:'water',weak:'fire'},
  fire:{name:'Fire',icon:'♨',color:'#ff9564',direction:'S',resource:'Ember stones',beats:'leaf',weak:'water'},
  storm:{name:'Storm',icon:'ϟ',color:'#c6acff',direction:'E',resource:'Storm shards',beats:'water',weak:'earth'},
  water:{name:'Water',icon:'≈',color:'#75c9ff',direction:'W',resource:'River pearls',beats:'fire',weak:'leaf'},
  earth:{name:'Earth',icon:'◆',color:'#d9b482',direction:null,resource:'Quartz pebbles',beats:'storm',weak:'water'},
  ice:{name:'Ice',icon:'❄',color:'#b4eff6',direction:null,resource:'Frost crystals',beats:'leaf',weak:'fire'},
  light:{name:'Light',icon:'☼',color:'#ffe49b',direction:null,resource:'Dawn motes',beats:'shadow',weak:'shadow'},
  shadow:{name:'Shadow',icon:'◐',color:'#b5a3d8',direction:null,resource:'Dusk petals',beats:'light',weak:'light'}
};
export const DIRECTIONS={N:'leaf',S:'fire',E:'storm',W:'water'};
export const STARTERS=['capuchin','macaque','langur','mandrill','spider','howler'];
// Attacking element -> every defending element. Mixed defense multiplies columns;
// a split attack averages its component multipliers, never chooses the best one.
export const TYPE_CHART={
  leaf:  {leaf:.4, fire:.4,water:2.25,storm:2.25,earth:2.25,ice:.4,light:1,shadow:1},
  fire:  {leaf:2.25,fire:.4,water:.4,storm:1,earth:.4,ice:2.25,light:1,shadow:1},
  water: {leaf:.4,fire:2.25,water:.4,storm:1,earth:2.25,ice:.4,light:1,shadow:1},
  storm: {leaf:.4,fire:1,water:2.25,storm:.4,earth:.4,ice:2.25,light:1,shadow:1},
  earth: {leaf:.4,fire:2.25,water:.4,storm:2.25,earth:.4,ice:1,light:1,shadow:1},
  ice:   {leaf:2.25,fire:.4,water:.4,storm:1,earth:2.25,ice:.4,light:1,shadow:1},
  light: {leaf:1,fire:1,water:1,storm:1,earth:.4,ice:1,light:.4,shadow:2.25},
  shadow:{leaf:1,fire:1,water:1,storm:1,earth:1,ice:.4,light:2.25,shadow:.4}
};

const general={
  strike:{name:'Palm Strike',power:18,cost:0,desc:'Reliable physical damage. Always available at zero energy.'},
  brace:{name:'Brace',cost:0,guard:true,priority:3,desc:'Block most incoming damage this round; restore 2 energy.'},
  breathe:{name:'Deep Breath',cost:0,energy:7,desc:'Restore 7 energy. Opponents can still attack.'},
  quickJab:{name:'Quick Jab',power:15,cost:1,priority:1,desc:'A quick physical strike with priority.'},
  heavyPalm:{name:'Heavy Palm',power:35,cost:4,priority:-1,desc:'A powerful, slower physical attack.'},
  tailSweep:{name:'Tail Sweep',power:23,cost:3,status:'slow',duration:2,desc:'Sweep the opponent off balance; slow for 2 rounds.'},
  counterstance:{name:'Counterstance',cost:3,counter:.35,guard:true,cooldown:3,desc:'Brace and reflect 35% of the next hit this round. 3-round cooldown.'},
  focus:{name:'Canopy Focus',cost:2,prepare:.35,duration:2,desc:'Prepare your next damaging ability for 35% more damage within 2 rounds.'},
  ironGuard:{name:'Iron Guard',cost:4,shield:.25,duration:2,cooldown:3,desc:'Gain a shield worth 25% maximum health for 2 rounds. 3-round cooldown.'},
  calmCall:{name:'Calm Call',power:12,cost:2,bondBonus:.12,desc:'A calming call deals light damage and visibly improves recruitment by 12 points.'},
  friendlyFeint:{name:'Friendly Feint',power:15,cost:3,bondBonus:.15,weaken:.15,duration:1,desc:'A playful strike improves recruitment by 15 points and weakens the next attack.'},
  branchRelay:{name:'Branch Relay',power:14,cost:4,switch:true,cooldown:2,desc:'Deal light damage, then switch to a chosen conscious teammate. 2-round cooldown.'},
  guardianHandoff:{name:'Guardian Handoff',cost:4,switch:true,teamShield:.15,duration:2,cooldown:3,desc:'Switch and give the incoming teammate a 15% shield for 2 rounds. 3-round cooldown.'},
  bananaFeint:{name:'Banana Feint',cost:3,switch:true,weaken:.25,duration:1,cooldown:3,desc:'Weaken the opponent’s next hit by 25%, then switch. 3-round cooldown.'},
  rescueSwing:{name:'Rescue Swing',cost:3,switch:true,priority:2,uses:1,desc:'An emergency priority retreat to a conscious teammate. Once per encounter.'},
  canopyRally:{name:'Canopy Rally',cost:3,rally:.18,duration:3,cooldown:4,desc:'Prepare an 18% damage benefit for the next teammate to enter. 4-round cooldown.'},
  tailwindTransfer:{name:'Tailwind Transfer',cost:4,switch:true,transferSpeed:true,duration:2,cooldown:3,desc:'Switch and pass a temporary speed boost for 2 rounds. 3-round cooldown.'},
  rootedChallenge:{name:'Rooted Challenge',power:16,cost:3,root:2,status:'root',duration:2,cooldown:3,desc:'Deal light damage; prevent voluntary switching for 2 rounds. Knockout replacements remain available.'},
  secondWind:{name:'Second Wind',cost:2,switch:true,entryEnergy:5,cooldown:4,desc:'Switch; the incoming teammate restores 5 energy. 4-round cooldown prevents relay loops.'},
  fieldMend:{name:'Field Mend',cost:5,heal:.22,cooldown:3,desc:'Restore 22% maximum health. 3-round cooldown.'},
  clearMind:{name:'Clear Mind',cost:3,cleanse:true,energy:2,cooldown:3,desc:'Remove personal status effects and restore 2 energy. 3-round cooldown.'},
  energySip:{name:'Energy Sip',power:14,cost:2,drainEnergy:3,desc:'Deal light physical damage and drain up to 3 energy.'},
  guardBreak:{name:'Guard Break',power:24,cost:4,breakShield:true,desc:'Break the opponent’s shield before dealing physical damage.'},
  teamMend:{name:'Campfire Chorus',cost:7,teamHeal:.10,uses:2,cooldown:4,desc:'Restore 10% health to conscious teammates. Twice per encounter; 4-round cooldown.'}
};
const elemental={
  vine:{name:'Thorn Lash',type:'leaf',power:22,cost:3,status:'poison',duration:3,desc:'Leaf damage; poison for 3 rounds. Leaf opponents resist poison.'},
  mend:{name:'Canopy Mend',type:'leaf',cost:5,heal:.30,minStage:2,cooldown:3,desc:'Restore 30% maximum health. 3-round cooldown.'},
  seedDrain:{name:'Seed Siphon',type:'leaf',power:24,cost:4,drain:.30,desc:'Leaf damage; restore 30% of damage dealt.'},
  leafCleanse:{name:'Petal Renewal',type:'leaf',cost:4,heal:.12,cleanse:true,cooldown:3,desc:'Restore 12% maximum health and remove personal status effects.'},
  cinder:{name:'Cinder Fist',type:'fire',power:26,cost:3,status:'burn',duration:2,desc:'Fire damage; burn for 2 rounds. Fire opponents resist burn.'},
  flare:{name:'Solar Roar',type:'fire',power:43,cost:6,minStage:2,desc:'Heavy fire damage. A strong pure-element option.'},
  emberGuard:{name:'Ember Guard',type:'fire',cost:4,shield:.20,counter:.20,duration:2,cooldown:3,desc:'A 20% shield and a small retaliatory flare for 2 rounds.'},
  ashRush:{name:'Ash Rush',type:'fire',power:24,cost:4,priority:1,desc:'A fast fire attack that outruns slower techniques.'},
  tide:{name:'Tidal Palm',type:'water',power:27,cost:3,status:'slow',duration:2,desc:'Water damage; slow the opponent for 2 rounds.'},
  spring:{name:'Cleansing Spring',type:'water',cost:5,heal:.22,cleanse:true,minStage:2,cooldown:3,desc:'Restore 22% maximum health and remove status effects.'},
  bubbleGuard:{name:'Bubble Bastion',type:'water',cost:4,shield:.25,duration:2,cooldown:3,desc:'A bubble shield worth 25% maximum health for 2 rounds.'},
  rippleDrain:{name:'Returning Ripple',type:'water',power:25,cost:4,drain:.25,desc:'Water damage; restore 25% of damage dealt.'},
  spark:{name:'Lightning Jab',type:'storm',power:24,cost:3,priority:1,desc:'A fast storm strike with priority.'},
  thunder:{name:'Thunder Howl',type:'storm',power:41,cost:6,minStage:2,desc:'Heavy storm damage.'},
  galeStep:{name:'Gale Step',type:'storm',cost:3,statusSelf:'speed',duration:2,energy:2,cooldown:3,desc:'Gain speed for 2 rounds and restore 2 energy.'},
  staticField:{name:'Static Field',type:'storm',power:20,cost:4,status:'slow',duration:2,desc:'Storm damage; slow for 2 rounds without disabling actions.'},
  stonePalm:{name:'Stone Palm',type:'earth',power:29,cost:3,priority:-1,desc:'A weighty earth attack.'},
  quartzGuard:{name:'Quartz Guard',type:'earth',cost:4,shield:.28,duration:2,cooldown:3,desc:'A crystalline shield worth 28% maximum health for 2 rounds.'},
  faultLine:{name:'Fault Line',type:'earth',power:38,cost:6,breakShield:true,desc:'Shatter shields, then deal earth damage.'},
  rootWall:{name:'Bedrock Challenge',type:'earth',power:18,cost:4,status:'root',root:2,duration:2,cooldown:3,desc:'Earth damage and 2 rounds of voluntary switch restraint.'},
  frostFang:{name:'Frost Fang',type:'ice',power:27,cost:3,status:'slow',duration:2,desc:'Ice damage; slow for 2 rounds.'},
  snowWard:{name:'Snow Ward',type:'ice',cost:4,shield:.25,cleanse:true,duration:2,cooldown:4,desc:'Clear status effects and gain a 25% shield for 2 rounds.'},
  icicleBurst:{name:'Icicle Burst',type:'ice',power:40,cost:6,desc:'A precise burst of heavy ice damage.'},
  thaw:{name:'Gentle Thaw',type:'ice',cost:4,heal:.18,cleanse:true,cooldown:3,desc:'Restore 18% maximum health and clear status effects.'},
  dawnPalm:{name:'Dawn Palm',type:'light',power:27,cost:3,desc:'A reliable burst of light damage.'},
  sunMend:{name:'Sunlit Mend',type:'light',cost:5,heal:.27,cooldown:3,desc:'Restore 27% maximum health. 3-round cooldown.'},
  prismGuard:{name:'Prism Guard',type:'light',cost:4,shield:.22,counter:.18,duration:2,cooldown:3,desc:'A 22% shield with a small reflected flare.'},
  purify:{name:'Purifying Beam',type:'light',power:23,cost:4,cleanse:true,desc:'Clear personal status effects, then deal light damage.'},
  duskClaw:{name:'Dusk Claw',type:'shadow',power:28,cost:3,desc:'A focused shadow strike.'},
  nightDrain:{name:'Night Siphon',type:'shadow',power:25,cost:4,drain:.30,desc:'Shadow damage; restore 30% of damage dealt.'},
  veil:{name:'Velvet Veil',type:'shadow',cost:4,shield:.18,weaken:.20,duration:2,cooldown:3,desc:'Gain an 18% shield and weaken the next opposing attack.'},
  eclipseHex:{name:'Eclipse Hex',type:'shadow',power:22,cost:4,status:'weaken',weaken:.25,duration:2,desc:'Shadow damage; reduce the opponent’s next attack by 25%.'}
};
const hybrids={
  wildfire:{name:'Wildfire Bloom',types:['fire','leaf'],power:38,cost:6,status:'burn',duration:2,desc:'Split fire/leaf damage and burn for 2 rounds.'},
  steam:{name:'Steam Cyclone',types:['fire','water'],power:38,cost:6,status:'slow',duration:2,desc:'Split fire/water damage and slow for 2 rounds.'},
  plasma:{name:'Plasma Pounce',types:['fire','storm'],power:38,cost:6,priority:1,desc:'A fast strike with evenly split fire/storm damage.'},
  monsoon:{name:'Monsoon Canopy',types:['leaf','water'],power:32,cost:6,drain:.35,desc:'Split leaf/water damage; restore 35% of damage dealt.'},
  tempest:{name:'Tempest Vines',types:['leaf','storm'],power:36,cost:6,status:'poison',duration:3,desc:'Split leaf/storm damage and poison for 3 rounds.'},
  typhoon:{name:'Typhoon Crash',types:['water','storm'],power:39,cost:6,status:'slow',duration:2,desc:'Split water/storm damage and slow for 2 rounds.'},
  magma:{name:'Magma Knuckle',types:['fire','earth'],power:41,cost:7,breakShield:true,desc:'Break shields with evenly split fire/earth damage.'},
  frostfire:{name:'Thermal Fracture',types:['fire','ice'],power:44,cost:7,priority:-1,desc:'A slower, costly attack split between fire and ice.'},
  solarFlare:{name:'Solar Purge',types:['fire','light'],power:36,cost:6,cleanse:true,desc:'Cleanse personal status and deal split fire/light damage.'},
  ashcloak:{name:'Smokeshade',types:['fire','shadow'],power:33,cost:6,weaken:.20,duration:1,desc:'Split fire/shadow damage; weaken the next opposing hit.'},
  rootstone:{name:'Rootstone Bulwark',types:['leaf','earth'],cost:6,shield:.32,duration:2,cooldown:4,desc:'Grow a 32% shield for 2 rounds. 4-round cooldown.'},
  frostfern:{name:'Frostfern Siphon',types:['leaf','ice'],power:32,cost:6,drain:.25,status:'slow',duration:2,desc:'Split leaf/ice damage, drain and slow for 2 rounds.'},
  gloamorchid:{name:'Gloam Nectar',types:['leaf','shadow'],power:33,cost:6,drain:.35,desc:'Split leaf/shadow damage; restore 35% of damage dealt.'},
  siltguard:{name:'Riverbed Guard',types:['water','earth'],cost:6,shield:.32,counter:.20,duration:2,cooldown:4,desc:'A 32% shield with a small retaliation for 2 rounds.'},
  glacierflow:{name:'Glacier Current',types:['water','ice'],power:34,cost:6,shield:.15,duration:2,cooldown:3,desc:'Split water/ice damage and gain a modest 15% shield.'},
  moonripple:{name:'Moonripple Counter',types:['water','shadow'],power:33,cost:6,counter:.25,duration:1,cooldown:3,desc:'Split water/shadow damage and prepare a one-round retaliation.'},
  fulgurite:{name:'Glassbolt Hammer',types:['earth','storm'],power:40,cost:7,breakShield:true,desc:'Break a shield with evenly split earth/storm damage.'},
  aurora:{name:'Aurora Handoff',types:['ice','light'],cost:6,switch:true,teamShield:.22,duration:2,cooldown:4,desc:'Switch and grant a 22% shield for 2 rounds. 4-round cooldown.'},
  eclipse:{name:'Twilight Balance',types:['light','shadow'],power:35,cost:6,cleanse:true,desc:'Clear personal status effects; split damage between light and shadow.'},
  obsidian:{name:'Obsidian Lock',types:['earth','shadow'],power:35,cost:6,status:'root',root:2,duration:2,cooldown:3,desc:'Split earth/shadow damage; prevent voluntary switching for 2 rounds.'}
};
const ultra={
  coronationFlare:{name:'Coronation Flare',types:['fire','light'],power:46,cost:8,cleanse:true,cooldown:4,desc:'Solkar crowns the field in sunlight. Split fire/light damage and personal cleanse. 4-round cooldown.'},
  worldrootSanctuary:{name:'Worldroot Sanctuary',types:['leaf','earth'],cost:8,teamHeal:.15,teamShield:.18,duration:2,cooldown:5,desc:'Verdara shelters conscious teammates: heal 15% and shield the next entrant by 18%. 5-round cooldown.'},
  abyssalChorus:{name:'Abyssal Chorus',types:['water','shadow'],power:42,cost:8,drain:.25,weaken:.20,duration:2,cooldown:4,desc:'Tiderex sings split water/shadow damage, drains life, and weakens the next hit. 4-round cooldown.'},
  skybreakRelay:{name:'Skybreak Relay',types:['storm','light'],power:36,cost:8,switch:true,transferSpeed:true,duration:2,cooldown:4,desc:'Voltaro strikes with split storm/light damage, then relays speed to a chosen teammate. 4-round cooldown.'},
  auroraReversal:{name:'Aurora Reversal',types:['ice','light'],power:36,cost:8,cleanse:true,shield:.20,duration:2,cooldown:4,desc:'Cryomane clears personal status, gains a 20% shield, and deals split ice/light damage. 4-round cooldown.'},
  mountainbreaker:{name:'Mountainbreaker',types:['earth','fire'],power:49,cost:9,breakShield:true,priority:-1,cooldown:4,desc:'Obsidrax shatters shields with a heavy, split earth/fire strike. 4-round cooldown.'},
  eclipseFeint:{name:'Eclipse Feint',types:['shadow','storm'],power:34,cost:8,switch:true,weaken:.30,duration:1,cooldown:4,desc:'Noctyra deals split shadow/storm damage, weakens the next hit, then retreats. 4-round cooldown.'},
  renewalChorus:{name:'Renewal Chorus',types:['leaf','water'],cost:8,teamHeal:.18,cleanse:true,cooldown:5,uses:2,desc:'Oroluma heals conscious teammates by 18% and cleanses personal status. Twice per encounter; 5-round cooldown.'}
};
// Expedition II techniques compose the same bounded combat effects as the
// original roster. Common companions keep useful, inexpensive specialties.
const specialties={
  acornAegis:{name:'Acorn Aegis',type:'leaf',cost:4,shield:.22,counter:.30,duration:2,cooldown:3,desc:'Gain a 22% maximum-health shield for 2 rounds; reflect 30% of the next unshielded hit. 3-round cooldown.'},
  pitcherSip:{name:'Pitcher Sip',type:'leaf',power:20,cost:3,drain:.40,status:'poison',duration:2,desc:'Leaf damage; heal 40% of damage dealt and poison for 2 rounds. Leaf opponents resist poison.'},
  petalParcel:{name:'Petal Parcel',type:'leaf',cost:5,teamHeal:.08,switch:true,cooldown:4,desc:'Heal conscious teammates for 8% of their maximum health, then switch to a chosen conscious teammate. 4-round cooldown.'},
  coalCoat:{name:'Coal Coat',type:'fire',cost:4,shield:.20,counter:.25,duration:2,cooldown:3,desc:'Gain a 20% maximum-health shield for 2 rounds; reflect 25% of the next unshielded hit. 3-round cooldown.'},
  wickFlick:{name:'Wick Flick',type:'fire',power:19,cost:3,priority:1,status:'burn',duration:2,desc:'Priority fire damage and burn for 2 rounds. Fire opponents resist burn.'},
  temperedKnuckle:{name:'Tempered Knuckle',type:'fire',power:29,cost:4,breakShield:true,desc:'Break the opponent’s shield and guard before dealing fire damage.'},
  skippingStone:{name:'Skipping Stone',type:'water',power:18,cost:4,switch:true,entryEnergy:3,cooldown:4,desc:'Water damage, then switch. The incoming teammate restores 3 energy, subject to a 4-round recipient cooldown. 4-round ability cooldown.'},
  spiralShell:{name:'Spiral Shell',type:'water',cost:5,shield:.25,cleanse:true,duration:2,cooldown:4,desc:'Clear personal status effects, then gain a 25% maximum-health shield for 2 rounds. 4-round cooldown.'},
  rainChorus:{name:'Rain Chorus',type:'water',cost:6,teamHeal:.09,energy:2,cooldown:4,desc:'Heal conscious teammates for 9% of their maximum health; restore 2 personal energy. 4-round cooldown.'},
  kiteSnare:{name:'Kite Snare',type:'storm',power:20,cost:4,priority:1,status:'slow',duration:2,desc:'Priority storm damage; reduce the opponent’s speed by 35% for 2 rounds.'},
  copperSip:{name:'Copper Sip',type:'storm',power:16,cost:3,drainEnergy:4,desc:'Storm damage; drain up to 4 energy from the opponent and restore that much personal energy.'},
  drumRoll:{name:'Thunder Drumroll',type:'storm',power:31,cost:5,weaken:.15,duration:2,desc:'Storm damage; weaken the opponent’s next damaging attack by 15% within 2 rounds.'},
  pottersRampart:{name:'Potter’s Rampart',type:'earth',cost:5,shield:.30,duration:2,cooldown:3,desc:'Gain a shield worth 30% maximum health for 2 rounds. 3-round cooldown.'},
  flintFeint:{name:'Flint Feint',type:'earth',power:26,cost:4,weaken:.20,duration:2,desc:'Earth damage; weaken the opponent’s next damaging attack by 20% within 2 rounds.'},
  rimeRebuke:{name:'Rime Rebuke',type:'ice',power:20,cost:4,status:'slow',counter:.25,duration:2,cooldown:3,desc:'Ice damage and a 2-round slow. Reflect 25% of the next unshielded hit within 2 rounds. 3-round cooldown.'},
  podShelter:{name:'Snowpod Shelter',type:'ice',cost:4,switch:true,teamShield:.20,duration:2,cooldown:3,desc:'Switch to a conscious teammate and grant a shield worth 20% of their maximum health for 2 rounds. 3-round cooldown.'},
  haloHandoff:{name:'Halo Handoff',type:'light',cost:5,switch:true,teamShield:.12,entryEnergy:3,duration:2,cooldown:4,desc:'Switch; grant a 12% maximum-health shield for 2 rounds and restore 3 energy, subject to a 4-round recipient cooldown. 4-round ability cooldown.'},
  inkBlot:{name:'Ink Blot',type:'shadow',power:18,cost:3,weaken:.25,duration:2,desc:'Shadow damage; weaken the opponent’s next damaging attack by 25% within 2 rounds.'},
  thornUmbra:{name:'Thorn Umbra',types:['leaf','shadow'],power:27,cost:5,drain:.25,status:'poison',duration:3,desc:'Split leaf/shadow damage; heal 25% of damage dealt and poison for 3 rounds. Leaf opponents resist poison.'},
  pressureRelease:{name:'Pressure Release',types:['water','fire'],power:33,cost:6,cleanse:true,desc:'Clear personal status effects, then deal evenly split water/fire damage.'},
  glassGlide:{name:'Glass Glide',types:['earth','ice'],power:28,cost:5,shield:.15,duration:2,cooldown:3,desc:'Split earth/ice damage and gain a 15% maximum-health shield for 2 rounds. 3-round cooldown.'},
  lodestoneGrip:{name:'Lodestone Grip',types:['earth','storm'],power:25,cost:5,drainEnergy:3,root:2,duration:2,cooldown:3,desc:'Split earth/storm damage; drain up to 3 energy and prevent voluntary switching for 2 rounds. 3-round cooldown.'},
  pollenBeacon:{name:'Pollen Beacon',types:['light','leaf'],cost:6,teamHeal:.12,cleanse:true,cooldown:4,minStage:1,desc:'Heal conscious teammates for 12% of their maximum health and clear personal status effects. 4-round cooldown.'},
  brineCurtain:{name:'Brine Curtain',types:['water','shadow'],cost:5,shield:.20,weaken:.25,duration:2,cooldown:3,desc:'Gain a 20% maximum-health shield for 2 rounds; weaken the next opposing damaging attack by 25% within 2 rounds. 3-round cooldown.'},
  hailCadence:{name:'Hail Cadence',types:['ice','storm'],power:30,cost:6,priority:1,status:'slow',duration:2,desc:'Priority split ice/storm damage; reduce the opponent’s speed by 35% for 2 rounds.'},
  funeralLantern:{name:'Lantern Wake',types:['fire','shadow'],power:27,cost:5,status:'burn',drain:.25,duration:2,desc:'Split fire/shadow damage; heal 25% of damage dealt and burn for 2 rounds. Fire opponents resist burn.'}
};
Object.assign(ultra,{
  zeroHourRelay:{name:'Zero-Hour Relay',types:['storm','ice'],power:33,cost:8,priority:1,switch:true,transferSpeed:true,duration:2,cooldown:4,desc:'Chronogibbon deals priority split storm/ice damage, then switches. Pass any remaining speed boost, or grant a 20% boost for 2 rounds. 4-round cooldown.'},
  monolithVerdict:{name:'Monolith Verdict',types:['earth','light'],power:38,cost:8,shield:.22,counter:.25,duration:2,cooldown:4,desc:'Basaltoracle deals split earth/light damage, gains a 22% maximum-health shield for 2 rounds, and reflects 25% of the next unshielded hit. 4-round cooldown.'},
  sovereignSpore:{name:'Sovereign Spore',types:['leaf','shadow'],power:34,cost:8,status:'poison',drain:.30,weaken:.20,duration:3,cooldown:4,desc:'Mirecrown deals split leaf/shadow damage, heals 30% of damage dealt, poisons for 3 rounds, and weakens the next opposing attack by 20% within 3 rounds. Leaf opponents resist poison. 4-round cooldown.'},
  rebirthTide:{name:'Rebirth Tide',types:['fire','water'],power:35,cost:8,teamHeal:.12,cleanse:true,cooldown:5,uses:2,desc:'Phoenixmacaque heals conscious teammates for 12% of their maximum health, clears personal status effects, and deals split fire/water damage. Twice per encounter; 5-round cooldown.'}
});
export const MOVES=Object.fromEntries([
  ...Object.entries(general).map(([id,m])=>[id,{type:'neutral',family:'general',...m}]),
  ...Object.entries(elemental).map(([id,m])=>[id,{family:'elemental',...m}]),
  ...Object.entries(hybrids).map(([id,m])=>[id,{family:'hybrid',type:m.types[0],splitDamage:true,...m}]),
  ...Object.entries(specialties).map(([id,m])=>[id,{family:'specialty',...(m.types?{type:m.types[0],splitDamage:true}:{}),...m}]),
  ...Object.entries(ultra).map(([id,m])=>[id,{family:'ultra',signature:true,type:m.types[0],splitDamage:true,...m}])
]);
for(const[id,m]of Object.entries({...ELEMENTAL_TECHNIQUES,...TACTICAL_TECHNIQUES}))MOVES[id]={family:'progression',expansion:3,...m};
for(const[id,m]of Object.entries(SPECIES_SIGNATURES))MOVES[id]={family:'signature',expansion:3,signature:true,minStage:3,minLevel:10,type:m.type||m.types[0],...(m.types?{splitDamage:true}:{}),...m};
export const HYBRIDS=Object.fromEntries(Object.entries(hybrids).map(([move,m])=>[[...m.types].sort().join('+'),{name:m.name.split(' ')[0],move}]));

// All learnsets are curated. Every species retains strike; traits distinguish
// collection identity from an individual monkey's temporary evolution.
const rows=[
  ['capuchin','Capuchin','The Forager',[], 'utility',78,11,8,13,'common','Resource-efficient supply specialist.','calmCall branchRelay energySip leafCleanse cinder tide spark stonePalm frostFang dawnPalm duskClaw'],
  ['macaque','Macaque','The Striker',[],'attacker',84,15,8,10,'common','Critical hits are twice as likely.','quickJab heavyPalm focus guardBreak cinder vine tide spark stonePalm frostFang dawnPalm duskClaw'],
  ['langur','Langur','The Herbalist',[],'healer',86,10,10,11,'common','Healing restores 25% more health.','fieldMend teamMend clearMind canopyRally mend spring sunMend thaw seedDrain'],
  ['mandrill','Mandrill','The Warden',[],'defender',102,12,13,6,'common','Brace blocks 70% of incoming damage.','ironGuard guardianHandoff counterstance rootedChallenge emberGuard bubbleGuard quartzGuard snowWard prismGuard veil'],
  ['spider','Spider Monkey','The Acrobat',[],'relay',73,12,7,18,'common','A 10% chance to dodge an attack.','quickJab branchRelay rescueSwing bananaFeint tailwindTransfer secondWind ashRush galeStep'],
  ['howler','Howler','The Channeler',[],'caster',88,12,9,9,'common','Elemental attacks deal 15% more damage.','focus canopyRally calmCall flare mend thunder faultLine icicleBurst purify eclipseHex'],
  ['mossmop','Mossmop','The Orchard Mane',['leaf'],'healer',89,10,11,8,'common','A patient recovery specialist.','fieldMend canopyRally seedDrain mend leafCleanse calmCall'],
  ['thornback','Thornback','The Bark Bastion',['leaf'],'defender',99,12,14,5,'uncommon','Contact punishment and rooted defense.','vine counterstance ironGuard rootedChallenge seedDrain guardianHandoff'],
  ['bloomtail','Bloomtail','The Petal Friend',['leaf'],'healer',79,9,9,15,'common','Flowers open to cleanse and support.','mend leafCleanse teamMend friendlyFeint branchRelay seedDrain'],
  ['vineknuckle','Vineknuckle','The Living Grip',['leaf'],'controller',90,14,10,8,'uncommon','Heavy vines control opposing switches.','vine rootedChallenge heavyPalm guardBreak seedDrain canopyRally'],
  ['cinderpaw','Cinderpaw','The Ember Sprinter',['fire'],'attacker',73,14,7,17,'common','Fast pressure through glowing palms.','cinder ashRush quickJab focus bananaFeint flare'],
  ['ashmane','Ashmane','The Charcoal Roar',['fire'],'caster',88,15,9,10,'uncommon','Keeps sustained burn pressure.','cinder flare focus emberGuard guardBreak canopyRally'],
  ['lanternMarmoset','Lantern Marmoset','The Tail Lantern',['fire'],'utility',72,10,8,18,'uncommon','A tiny trickster with a guiding flame.','cinder emberGuard friendlyFeint bananaFeint secondWind rescueSwing'],
  ['kilnfist','Kilnfist','The Furnace Guard',['fire'],'defender',103,16,13,4,'rare','Ceramic gauntlets absorb punishment.','heavyPalm cinder flare emberGuard counterstance guardBreak'],
  ['brooktail','Brooktail','The Ribbon Duelist',['water'],'relay',77,12,8,17,'common','An agile river duelist.','tide rippleDrain quickJab branchRelay tailwindTransfer spring'],
  ['bubblecheek','Bubblecheek','The Bubble Keeper',['water'],'utility',87,9,11,10,'common','Shields companions and manages energy.','bubbleGuard spring secondWind energySip guardianHandoff calmCall'],
  ['reefguard','Reefguard','The Coral Shield',['water'],'defender',101,11,14,5,'uncommon','A protective shell-and-coral defender.','bubbleGuard spring tide guardianHandoff counterstance ironGuard'],
  ['cascadeHowler','Cascade Howler','The Waterfall Voice',['water'],'caster',94,15,9,8,'rare','A resonant wave caster.','tide rippleDrain focus spring canopyRally heavyPalm'],
  ['sparkskip','Sparkskip','The Bright Dash',['storm'],'relay',70,12,7,20,'common','Quick strikes and clever retreats.','spark galeStep branchRelay quickJab rescueSwing tailwindTransfer'],
  ['stormtuft','Stormtuft','The Charged Fist',['storm'],'attacker',82,16,8,14,'uncommon','Charged knuckles deliver decisive bursts.','spark thunder focus guardBreak staticField quickJab'],
  ['cloudcap','Cloudcap','The Cloud Courier',['storm'],'utility',82,10,9,16,'uncommon','A cheerful speed support specialist.','galeStep staticField canopyRally secondWind tailwindTransfer friendlyFeint'],
  ['thunderfang','Thunderfang','The Storm Bruiser',['storm'],'attacker',95,17,11,7,'rare','Large charged fangs break defenses.','spark thunder guardBreak heavyPalm staticField ironGuard'],
  ['pebblepalm','Pebblepalm','The Little Mountain',['earth'],'defender',97,11,14,5,'common','A patient, accessible early defender.','stonePalm quartzGuard ironGuard guardianHandoff rootedChallenge calmCall'],
  ['quartzcrest','Quartzcrest','The Crystal Sage',['earth'],'caster',88,13,13,7,'uncommon','Channels earth through a crystal brow.','stonePalm faultLine quartzGuard focus rootWall clearMind'],
  ['dunetail','Dunetail','The Sand Dancer',['earth'],'controller',79,12,9,16,'uncommon','Sand veils weaken and disrupt.','stonePalm rootWall bananaFeint eclipseHex tailSweep rescueSwing'],
  ['snowmantle','Snowmantle','The Gentle Drift',['ice'],'defender',102,10,14,5,'common','Heavy forearms shield fragile companions.','snowWard thaw frostFang guardianHandoff teamMend ironGuard'],
  ['iciclefang','Iciclefang','The Crystal Hunter',['ice'],'attacker',78,17,8,15,'rare','A sharp-crowned ice attacker.','frostFang icicleBurst focus quickJab guardBreak counterstance'],
  ['frostwhisker','Frostwhisker','The Mist Elder',['ice'],'controller',84,11,11,12,'uncommon','Misty breath slows without stunning.','frostFang snowWard thaw tailSweep calmCall canopyRally'],
  ['sunbloom','Sunbloom','The Golden Gardener',['light'],'healer',85,10,10,12,'uncommon','Purifying warmth supports the team.','sunMend purify prismGuard teamMend leafCleanse calmCall'],
  ['dawncrest','Dawncrest','The First Light',['light'],'defender',86,14,12,11,'rare','A precise, defensive light duelist.','dawnPalm prismGuard purify counterstance focus guardianHandoff'],
  ['duskmask','Duskmask','The Crescent Prowler',['shadow'],'attacker',76,16,8,17,'uncommon','Low stalking attacks and ambush feints.','duskClaw nightDrain eclipseHex bananaFeint quickJab focus'],
  ['nightfrill','Nightfrill','The Velvet Whisper',['shadow'],'controller',82,11,9,16,'uncommon','Luminous ear fans conceal subtle feints.','veil eclipseHex nightDrain friendlyFeint rescueSwing canopyRally'],
  ['wildvine','Wildvine','The Ember Orchid',['fire','leaf'],'drainer',88,14,10,10,'rare','Scorched flowers turn burns into pressure.','wildfire vine cinder seedDrain leafCleanse rootedChallenge'],
  ['steampelt','Steampelt','The Pressure Sage',['fire','water'],'caster',94,14,11,6,'rare','Steam vents build slow, forceful pressure.','steam cinder tide focus bubbleGuard spring'],
  ['plasmafist','Plasmafist','The Violet Bolt',['fire','storm'],'attacker',82,17,8,14,'rare','Explosive gauntlets trade defense for bursts.','plasma spark ashRush flare guardBreak focus'],
  ['magmajaw','Magmajaw','The Obsidian Maw',['fire','earth'],'defender',104,16,14,3,'rare','Armored jaw plates protect a heavy hitter.','magma stonePalm emberGuard heavyPalm faultLine counterstance'],
  ['cinderfrost','Cinderfrost','The Thermal Twin',['fire','ice'],'attacker',86,16,9,11,'rare','An unstable combination of hot and cold.','frostfire cinder frostFang icicleBurst thaw focus'],
  ['sunflare','Sunflare','The Solar Mantle',['fire','light'],'healer',88,13,10,12,'rare','Cleansing light with offensive warmth.','solarFlare purify cinder sunMend canopyRally dawnPalm'],
  ['ashshade','Ashshade','The Smoky Trickster',['fire','shadow'],'relay',79,14,8,17,'rare','A smoky cape conceals retreating feints.','ashcloak ashRush duskClaw veil bananaFeint rescueSwing'],
  ['reedrunner','Reedrunner','The River Reed',['leaf','water'],'healer',80,10,9,17,'uncommon','Mobile healing with a flexible reed tail.','monsoon spring seedDrain branchRelay leafCleanse tailwindTransfer'],
  ['tempestvine','Tempestvine','The Charged Liana',['leaf','storm'],'relay',84,13,9,15,'rare','A specialist in charged relay techniques.','tempest vine spark branchRelay tailwindTransfer rootedChallenge'],
  ['rootstone','Rootstone','The Grove Wall',['leaf','earth'],'defender',108,11,16,3,'rare','Interlocking roots and stones protect allies.','rootstone quartzGuard vine guardianHandoff rootWall counterstance'],
  ['frostfern','Frostfern','The Frozen Garden',['leaf','ice'],'controller',87,11,11,11,'rare','Gentle recovery and patient slowing.','frostfern frostFang seedDrain leafCleanse thaw canopyRally'],
  ['gloamorchid','Gloamorchid','The Night Nectar',['leaf','shadow'],'drainer',83,13,9,14,'rare','Luminous orchids drain and weaken.','gloamorchid seedDrain nightDrain eclipseHex veil friendlyFeint'],
  ['typhoontail','Typhoontail','The Spiral Dash',['water','storm'],'attacker',82,15,8,17,'rare','A disruptive vortex-shaped tail.','typhoon tide spark staticField branchRelay galeStep'],
  ['siltguard','Siltguard','The Riverbank Wall',['water','earth'],'defender',106,12,15,3,'uncommon','Layered river armor protects the team.','siltguard stonePalm tide guardianHandoff bubbleGuard rootWall'],
  ['glacierflow','Glacierflow','The Living Glacier',['water','ice'],'defender',99,12,14,6,'rare','Water moves beneath protective ice.','glacierflow snowWard spring tide frostFang guardianHandoff'],
  ['moonripple','Moonripple','The Lunar Mirror',['water','shadow'],'controller',85,13,10,13,'rare','Shifting reflections reward counterplay.','moonripple rippleDrain duskClaw counterstance veil spring'],
  ['fulgurite','Fulgurite','The Glass Lightning',['earth','storm'],'attacker',92,16,12,7,'rare','Glassy spikes break stubborn shields.','fulgurite faultLine thunder quartzGuard spark guardBreak'],
  ['auroraswing','Auroraswing','The Ribbon Dancer',['ice','light'],'relay',81,11,10,18,'rare','Aurora ribbons shelter passing teammates.','aurora thaw purify tailwindTransfer guardianHandoff icicleBurst'],
  ['eclipsemask','Eclipsemask','The Twilight Balance',['light','shadow'],'controller',87,14,11,12,'rare','An asymmetric mask balances light and dark.','eclipse dawnPalm duskClaw prismGuard veil counterstance'],
  ['obsidianback','Obsidianback','The Blackstone Sentinel',['earth','shadow'],'defender',105,14,15,4,'rare','Low armor and strong disruption.','obsidian rootWall nightDrain quartzGuard heavyPalm rootedChallenge'],
  ['solkar','Solkar, the Crownfire King','Crownfire Sovereign',['fire','light'],'ultra',103,17,12,10,'ultra','A royal mandrill whose crown blooms into solar fire.','coronationFlare flare purify emberGuard guardianHandoff focus'],
  ['verdara','Verdara, the Worldroot Keeper','Worldroot Keeper',['leaf','earth'],'ultra',110,12,16,5,'ultra','A towering langur carries a living forest.','worldrootSanctuary rootstone seedDrain mend rootWall guardianHandoff'],
  ['tiderex','Tiderex, the Abyssal Howler','Abyssal Voice',['water','shadow'],'ultra',102,16,11,9,'ultra','A luminous deep-water mane answers the abyss.','abyssalChorus moonripple tide nightDrain focus veil'],
  ['voltaro','Voltaro, the Sky King','Sky King',['storm','light'],'ultra',90,16,10,18,'ultra','Floating lightning rings guide a streaming cloud cape.','skybreakRelay thunder dawnPalm galeStep tailwindTransfer prismGuard'],
  ['cryomane','Cryomane, the Aurora Emperor','Aurora Emperor',['ice','light'],'ultra',104,15,14,8,'ultra','Layered ice armor gleams under magnificent aurora trails.','auroraReversal icicleBurst snowWard sunMend counterstance aurora'],
  ['obsidrax','Obsidrax, the Mountainbreaker','Mountainbreaker',['earth','fire'],'ultra',111,18,15,3,'ultra','Volcanic fists and glowing lava seams shake the field.','mountainbreaker magma stonePalm emberGuard heavyPalm quartzGuard'],
  ['noctyra','Noctyra, the Eclipse Matriarch','Eclipse Matriarch',['shadow','storm'],'ultra',92,16,10,17,'ultra','An electrical night-sky mantle frames a crescent tail.','eclipseFeint duskClaw spark eclipseHex bananaFeint veil'],
  ['oroluma','Oroluma, the Eternal Canopy','Eternal Canopy',['leaf','water'],'ultra',108,12,13,8,'ultra','An immense vine crown shelters river-like ribbons.','renewalChorus monsoon spring seedDrain canopyRally teamMend'],
  // New identities are appended so the original sixty catalogue numbers remain
  // stable in local checkpoints and encrypted cloud saves.
  ['acornbuckler','Acornbuckler','The Nutshield Squire',['leaf'],'defender',96,10,14,6,'common','An affordable shield-and-counter specialist.','acornAegis vine counterstance guardianHandoff seedDrain rootedChallenge'],
  ['pitcherwink','Pitcherwink','The Nectar Sneak',['leaf'],'drainer',77,11,8,16,'uncommon','A nimble pitcher-flower hunter who sustains through poison and draining.','pitcherSip vine seedDrain energySip friendlyFeint leafCleanse'],
  ['orchidcap','Orchidcap','The Parasol Medic',['leaf'],'healer',81,9,10,13,'common','A small relay healer whose petal parcels recover the whole conscious team.','petalParcel leafCleanse mend branchRelay fieldMend calmCall'],
  ['coalroller','Coalroller','The Hearth Wheel',['fire'],'defender',100,13,12,5,'uncommon','Low speed and sturdy coal armor reward a timed counterattack.','coalCoat cinder counterstance emberGuard heavyPalm guardianHandoff'],
  ['wickwhisk','Wickwhisk','The Candle Dancer',['fire'],'controller',74,12,8,18,'common','Priority burning trades raw damage for reliable pressure.','wickFlick cinder ashRush tailSweep bananaFeint rescueSwing'],
  ['coppersnap','Coppersnap','The Copper Boxer',['fire'],'attacker',80,16,8,13,'uncommon','A furnace-hearted boxer who punches through shields but dislikes drawn-out fights.','temperedKnuckle cinder ashRush focus quickJab emberGuard'],
  ['rillhopper','Rillhopper','The Stone Skipper',['water'],'relay',74,11,8,19,'common','Fast water relays supply energy to a carefully chosen teammate.','skippingStone tide branchRelay secondWind bubbleGuard quickJab'],
  ['nautiloop','Nautiloop','The Spiral Watch',['water'],'defender',97,10,14,6,'uncommon','Spiral shoulder shells cleanse status before rebuilding a shield.','spiralShell bubbleGuard tide counterstance guardianHandoff spring'],
  ['drizzlemane','Drizzlemane','The Rain Poncho',['water'],'healer',86,9,11,10,'common','A modest, energy-efficient team healer with little burst damage.','rainChorus tide rippleDrain fieldMend spring canopyRally'],
  ['kitehowl','Kitehowl','The Wind Tether',['storm'],'controller',78,12,9,17,'uncommon','Priority snares slow quick rivals while relay tricks keep momentum.','kiteSnare spark galeStep staticField tailwindTransfer bananaFeint'],
  ['thimblevolt','Thimblevolt','The Pocket Battery',['storm'],'utility',69,10,7,22,'common','Exceptionally fast, fragile energy theft supports resource-starved teams.','copperSip spark energySip galeStep secondWind rescueSwing'],
  ['drumtuft','Drumtuft','The Thunder Busker',['storm'],'caster',85,15,9,10,'uncommon','A drum-chested caster whose forceful beat softens the next return hit.','drumRoll spark thunder staticField focus canopyRally'],
  ['claypot','Claypot','The Kiln Vessel',['earth'],'defender',105,10,15,3,'common','Slow pottery armor offers a large shield without rare-species offense.','pottersRampart stonePalm quartzGuard guardianHandoff counterstance rootWall'],
  ['flintcap','Flintcap','The Flake Duelist',['earth'],'attacker',85,16,10,10,'uncommon','A flint-helmed duelist turns measured attacks into brief protection.','flintFeint stonePalm guardBreak focus quickJab faultLine'],
  ['rimebeard','Rimebeard','The Frost Reproach',['ice'],'controller',88,11,12,8,'uncommon','Long icy whiskers slow opponents and punish their next hit.','rimeRebuke frostFang thaw snowWard counterstance canopyRally'],
  ['snowpea','Snowpea','The Winter Pod',['ice'],'relay',76,9,10,17,'common','A tiny pod-shaped guardian who shelters the next teammate.','podShelter frostFang thaw guardianHandoff tailwindTransfer friendlyFeint'],
  ['halohand','Halohand','The Ring Bearer',['light'],'relay',79,10,9,17,'common','Ring-shaped palms pass modest shielding and energy to companions.','haloHandoff dawnPalm prismGuard secondWind sunMend calmCall'],
  ['inkcap','Inkcap','The Blot Trickster',['shadow'],'controller',78,12,9,15,'uncommon','A brush-tailed trickster whose ink weakens a threatening attack.','inkBlot duskClaw veil energySip bananaFeint nightDrain'],
  ['bramblebat','Bramblebat','The Upside-Down Thicket',['leaf','shadow'],'drainer',85,13,9,17,'rare','Leaf-winged ambushes combine poison and lifesteal; low armor invites counterplay.','thornUmbra vine duskClaw nightDrain branchRelay veil'],
  ['thermosage','Thermosage','The Pressure Scholar',['water','fire'],'caster',93,15,11,7,'rare','A venting flask collar clears ailments before releasing a balanced steam strike.','pressureRelease steam tide cinder spring focus'],
  ['glassmanta','Glassmanta','The Shard Glider',['earth','ice'],'defender',96,12,14,7,'rare','Crystal arm fins turn attacks into small shields while slow strikes can be outrun.','glassGlide stonePalm frostFang quartzGuard snowWard faultLine'],
  ['magnetuft','Magnetuft','The Lodestone Grip',['earth','storm'],'controller',88,14,12,11,'rare','Magnetic hands restrain switches and drain energy without preventing attacks.','lodestoneGrip fulgurite spark stonePalm galeStep guardBreak'],
  ['lumenmoth','Lumenmoth','The Pollen Beacon',['light','leaf'],'healer',84,10,10,17,'rare','Petal wings become a team-healing beacon after the first awakening.','pollenBeacon sunMend leafCleanse seedDrain dawnPalm canopyRally'],
  ['brineveil','Brineveil','The Veiled Estuary',['water','shadow'],'defender',94,12,13,9,'rare','A jellyfish mantle shields the body and softens the next opposing hit.','brineCurtain moonripple tide duskClaw spring counterstance'],
  ['haildrummer','Haildrummer','The Hail Cadence',['ice','storm'],'attacker',82,16,9,16,'rare','A speedy hailstone percussionist with costly priority control.','hailCadence spark frostFang thunder icicleBurst galeStep'],
  ['pyreclasp','Pyreclasp','The Lantern Mourner',['fire','shadow'],'drainer',87,14,10,13,'rare','Lantern hands restore health through burning split-element attacks.','funeralLantern ashcloak cinder nightDrain veil emberGuard'],
  ['chronogibbon','Chronogibbon, the Zero-Hour Wanderer','Zero-Hour Wanderer',['storm','ice'],'ultra',91,15,10,19,'ultra','An immense gibbon swings through orbiting clock-snow rings; a priority relay bends the team’s tempo.','zeroHourRelay hailCadence thunder snowWard galeStep tailwindTransfer'],
  ['basaltoracle','Basaltoracle, the Dawn Monolith','Dawn Monolith',['earth','light'],'ultra',109,15,16,4,'ultra','A monumental baboon reads radiant stone tablets and answers attacks with a shielded verdict.','monolithVerdict stonePalm dawnPalm quartzGuard prismGuard faultLine'],
  ['mirecrown','Mirecrown, the Spore Sovereign','Spore Sovereign',['leaf','shadow'],'ultra',104,14,13,9,'ultra','A colossal swamp gorilla wears branching mushroom antlers; draining spores steadily weaken challengers.','sovereignSpore thornUmbra seedDrain nightDrain rootWall veil'],
  ['phoenixmacaque','Phoenixmacaque, the Everflow Flame','Everflow Flame',['fire','water'],'ultra',99,15,11,13,'ultra','Fountain-feather wings rise around a regal macaque; cleansing tides restore conscious companions.','rebirthTide pressureRelease cinder tide spring teamMend']
];
const expansionAppearances={
  acornbuckler:'A short pear-shaped body, huge split acorn shield, broad cheek pouches, and a knotted tail.',
  pitcherwink:'A slender hunter with a pitcher-flower head cup, pointed smiling muzzle, and long drinking tail.',
  orchidcap:'A tiny upright marmoset with a broad orchid parasol, petal cape, and button face.',
  coalroller:'A barrel pot belly, wheel-shaped coiled tail, soot eyebrows, and oversized coal mittens.',
  wickwhisk:'Long stilt legs, candle-wick ears, a narrow face, and fine curling whiskers.',
  coppersnap:'An angular boxer with a copper-plated jaw, rectangular furnace chest, and compact fists.',
  rillhopper:'A frog-like crouch, round alert eyes, broad webbed hands, and a skipping-stone tail tip.',
  nautiloop:'A low broad monkey with spiral shell shoulders, a curled eyebrow, and tiny sturdy feet.',
  drizzlemane:'A tall rain-poncho mane, raindrop beard, sleepy face, and skinny dangling arms.',
  kitehowl:'A diamond head ruff, wing-like kite forearms, open howling muzzle, and ribbon bow tail.',
  thimblevolt:'A tiny huge-eared monkey with copper thimble cuffs, zigzag pupils, and an antenna tail.',
  drumtuft:'A stout howler with a circular drum chest, twin mallet hands, and two bobbing head tufts.',
  claypot:'A squat terracotta torso, wide pot-rim collar, narrow head, and flat heavy feet.',
  flintcap:'A triangular flint helmet, sharp brow, broad shoulders, and pointed stone knuckles.',
  rimebeard:'A tall narrow elder with a long icicle beard, drooping brows, and oversized mitten hands.',
  snowpea:'A round baby face and body wrapped in a snow-pea pod cape with a bead-like pea tail.',
  halohand:'Long thin arms, a small head, enormous eyebrows, and glowing ring-shaped palms.',
  inkcap:'A wide mushroom-like ink hood, teardrop face, tapering body, and long brush tail.',
  bramblebat:'Broad leaf-shaped ear wings, a pointed nose, hooked feet, and an inverted hanging stance.',
  thermosage:'A tall flask-shaped body, barrel thermos collar, steaming ears, and a curved flask tail.',
  glassmanta:'Wide gliding crystal arm fins, a diamond face, narrow waist, and a thin shard tail.',
  magnetuft:'A horseshoe crown, blocky magnet hands, square muzzle, and compact grounded body.',
  lumenmoth:'Four broad petal-cape wings, an antenna brow, round luminous cheeks, and tiny feet.',
  brineveil:'A wide translucent jellyfish hat, tiny concealed face, and ribbon-like tentacle tail.',
  haildrummer:'A drum-shaped belly, hailstone knuckles, flat nose, and short sturdy legs.',
  pyreclasp:'A hunched back, very long arms, clasping lantern hands, and tapering smoke hair.',
  chronogibbon:'An immense lanky gibbon with long pendulum arms, an hourglass face, and orbiting clock-snow rings.',
  basaltoracle:'A monumental seated baboon with a squared muzzle, radiant tablet plates, and pillar-like arms.',
  mirecrown:'A towering swamp gorilla with a broad face, mushroom-antler crown, and massive rooted forearms.',
  phoenixmacaque:'A grand upright macaque with a narrow regal face, fountain-feather wings, and a streaming crest.'
};
const livingRegions={jungle:'emerald-jungle',savannah:'sunstone-savannah',caverns:'crystal-caverns',marsh:'whispering-marsh',frostpeak:'frostpeak-mountains',highlands:'thunder-highlands',volcano:'ember-volcano',moonlit:'moonlit-forest',celestial:'celestial-ruins',kingdom:'ancient-monkey-kingdom'};
for(const row of LIVING_SPECIES_ROWS){const[id,name,title,types,role,hp,atk,def,speed,rarity,trait,pool]=row;rows.push([id,name,title,types,role,hp,atk,def,speed,rarity,trait,pool]);}
export const SPECIES=Object.fromEntries(rows.map(([id,name,title,types,role,hp,atk,def,speed,rarity,trait,pool],index)=>[id,{
  id,number:index+1,name,title,types,affinity:types.length?types.map(t=>ELEMENTS[t].name).join(' / '):'Adaptable',role,hp,atk,def,speed,rarity,trait,
  learnset:[...new Set(['strike','brace','breathe',...pool.split(' ')])],signature:rarity==='ultra'?pool.split(' ')[0]:null,
  habitat:types[0]||'canopy',minFeet:index<6?0:index<22?2000:index<32?4000:index<52?6000:4000,
  captureChance:rarity==='common'?.85:rarity==='uncommon'?.65:rarity==='rare'?.45:1,
  encounterWeight:rarity==='common'?1:rarity==='uncommon'?.7:rarity==='rare'?.35:0,
  ...(expansionAppearances[id]?{expansion:2,specialty:pool.split(' ')[0],appearance:expansionAppearances[id],
    minFeet:rarity==='ultra'?5000:rarity==='rare'?10000:index<72?2000:4000,
    evolutionAppearances:['Awakened: elemental shoulder growth and larger silhouette.','Elder: a contrasting elemental crest and layered chest armor.','Ascendant: mastery crown, enlarged gauntlets, and a floating elemental focus.']}:{}),
}]));
for(const row of LIVING_SPECIES_ROWS){const[id,,,,,,,,,rarity,,,signature,region,habitat]=row,p=SPECIES[id];Object.assign(p,{signature,specialty:signature,expansion:3,region:livingRegions[region],habitat,habitats:[habitat],minFeet:rarity==='common'?2000:rarity==='uncommon'?4000:rarity==='rare'?12000:20000,encounterWeight:rarity==='common'?1:rarity==='uncommon'?.62:rarity==='rare'?.24:0,appearance:`${p.title}: bespoke ${habitat} anatomy and a ${p.affinity.toLowerCase()} signature silhouette.`,evolutionAppearances:[`${p.name} grows an elemental mantle and bright signature markings.`,`${p.name} gains a tall crest and layered signature armor.`,`${p.name} reveals a mastery crown and floating signature focus.`]});p.learnset.push(signature);}
// Every species has a role-based Ascendant technique and its own Lv 30 upgrade.
// Older Ultra signature IDs and mechanics stay unchanged for existing PvP replays.
for(const p of Object.values(SPECIES)){
 let signature=p.signature;
 if(!signature){signature=`${p.id}Ascension`;MOVES[signature]={family:'signature',expansion:3,signature:true,owner:p.id,name:`${p.name} Resolve`,type:p.types[0]||'neutral',minStage:3,minLevel:10,...ROLE_ASCENSIONS[p.role||'utility']};}
 else if(MOVES[signature].expansion!==3){const base=MOVES[signature];signature=`${p.id}Ascension`;MOVES[signature]={...base,family:'signature',expansion:3,owner:p.id,name:`${base.name} · Ascendant`,minStage:3,minLevel:10,cost:Math.min(10,base.cost+1),teamEnergy:2,reduceCooldown:1,desc:`${base.desc} Ascendant resonance also restores 2 energy to conscious allies and reduces your other cooldowns by one round.`};}
 p.progressionSignature=signature;
 const original=MOVES[signature],upgrade=`${p.id}MasterSignature`;
 MOVES[upgrade]={...original,owner:p.id,family:'mastery',expansion:3,signature:true,name:`${original.name} · Mastered`,minStage:3,minLevel:30,power:original.power?Math.round(original.power*1.12):undefined,cost:Math.min(10,(original.cost||6)+1),cooldown:Math.max(3,(original.cooldown||4)-1),reduceCooldown:original.reduceCooldown||undefined,desc:`Species mastery: ${original.desc} ${original.power?'12% more base power. ':''}Cooldown is one round shorter (minimum 3).`};
 p.progressionUpgrade=upgrade;
 p.learnset=[...new Set([...p.learnset,signature,upgrade])];
}
// Defensive stances must be established before ordinary fast attacks resolve.
for(const m of Object.values(MOVES))if(m.expansion===3&&m.guard&&m.priority===undefined)m.priority=3;
export const GEAR={
  fang:{name:'Sunfang Wraps',slot:'weapon',stat:'atk',amount:2,icon:'✦',fragmentCost:5,desc:'Woven fist wraps improve attack.'},
  shell:{name:'River-shell Vest',slot:'armor',stat:'def',amount:2,icon:'◈',fragmentCost:5,desc:'Layered river shells improve defense.'},
  heart:{name:'Canopy Heart',slot:'charm',stat:'hp',amount:9,icon:'♥',fragmentCost:5,desc:'A carved seed charm improves maximum health.'},
  wind:{name:'Gale Anklet',slot:'charm',stat:'speed',amount:2,icon:'ϟ',fragmentCost:5,desc:'A feather anklet improves speed.'},
  satchel:{name:'Forager Satchel',slot:'charm',stat:'hp',amount:3,icon:'▰',fragmentCost:6,discoveryReduction:.05,desc:'5% shorter discovery intervals while equipped; total acceleration caps at 20%.'},
  ...FORGE_GEAR
};
export const ITEMS={
  ingot:{name:'Ingot',desc:'A rare mineral crafting material worth 5 forge value. Earth foragers favor these finds.'},
  starOre:{name:'Star Ore',desc:'A very rare walking find used for late-level rare and Ultra training. Also worth 12 forge value.'},
  crownCore:{name:'Crown Core',desc:'Earned only by defeating an Ultra. Required for late-level Ultra training; worth 20 forge value.'},
  berry:{name:'Healing Berry',desc:'Restore 35% health to one conscious monkey.'},
  tonic:{name:'Spark Tonic',desc:'Restore 8 energy.'},
  salve:{name:'Cleanse Balm',desc:'Clear effects and restore 15% health.'},
  bondCharm:{name:'Bond Charm',desc:'One visible post-battle recruitment attempt.'},
  revive:{name:'Revival Seed',desc:'Revive one knocked-out teammate at 30% health.'},
  trailRibbon:{name:'Trail Ribbon',desc:'15% shorter discovery intervals for the next 1,000 accepted feet.'},
  treasureScent:{name:'Treasure Scent',desc:'Improve your next 3 discovery reward rolls.'},
  scoutFeather:{name:'Scout’s Feather',desc:'Reveal details about an eligible Ultra sighting; never creates another Ultra.'},
  campfireSeed:{name:'Campfire Seed',desc:'Improve the next earned camp’s limited recovery.'}
};
export const BALANCE={discoveryFeet:250,encounterFeet:2000,campFeet:6000,minDiscoveryFeet:200,maxDiscoveryReduction:.20,
  discoveryWeights:{essence:.60,fruit:.25,item:.12,fragment:.03},captureAttempts:3,capturePity:4,firstUltraFeet:5000,ultraRadiusFeet:100};

