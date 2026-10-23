// Save-stable equipment IDs. Values describe tier one; combat applies bounded
// upgrade scaling to bonuses/effects rather than multiplying them by the tier.
export const GEAR_RARITIES={
  common:{name:'Common',color:'#b8c7b6'},
  uncommon:{name:'Uncommon',color:'#8cdda8'},
  rare:{name:'Rare',color:'#a5bcff'},
  legendary:{name:'Legendary',color:'#ffd27e'}
};

const rows=[
  ['mossVest','Mosstitch Vest','leaf','common','armor',{hp:5},{},'+5 maximum health.'],
  ['sapwell','Sapwell Pendant','leaf','uncommon','charm',{hp:4},{roundHeal:.015},'+4 maximum health. Restore 1.5% maximum health between rounds while active and conscious.'],
  ['briarKnuckles','Briar Knuckles','leaf','rare','weapon',{atk:2},{physicalDamage:.06,elementDamage:{leaf:.06}},'+2 attack. Deal 6% more physical damage and 6% more Leaf damage.'],
  ['springheart','Springheart Brooch','leaf','rare','charm',{maxEnergy:1},{roundHeal:.025},'+1 maximum energy. Restore 2.5% maximum health between rounds while active and conscious.'],
  ['worldroot','Worldroot Mantle','leaf','legendary','armor',{hp:10,def:1},{roundHeal:.035,elementResist:{leaf:.10}},'+10 maximum health and +1 defense. Restore 3.5% maximum health between rounds while active and conscious; take 10% less Leaf damage.'],

  ['embergrip','Embergrip Wraps','fire','common','weapon',{atk:1},{elementDamage:{fire:.04}},'+1 attack. Deal 4% more Fire damage.'],
  ['kilnVest','Kiln-fired Vest','fire','uncommon','armor',{hp:4,def:1},{elementResist:{fire:.08}},'+4 maximum health and +1 defense. Take 8% less Fire damage.'],
  ['furnaceCore','Furnace Core','fire','rare','charm',{maxEnergy:2},{energyDamage:.10},'+2 maximum energy. Deal 10% more damage with abilities that spend energy.'],
  ['cinderSpurs','Cinder Spurs','fire','rare','charm',{speed:1},{physicalDamage:.08},'+1 speed. Deal 8% more physical damage.'],
  ['phoenixWraps','Phoenix Wraps','fire','legendary','weapon',{atk:2},{energyDamage:.12,roundHeal:.02},'+2 attack. Deal 12% more damage with abilities that spend energy; restore 2% maximum health between rounds while active and conscious.'],

  ['pearlCord','Pearl Life-cord','water','common','charm',{hp:6},{},'+6 maximum health.'],
  ['streamshell','Streamshell Vest','water','uncommon','armor',{def:1},{elementResist:{water:.08}},'+1 defense. Take 8% less Water damage.'],
  ['tidesong','Tidesong Pendant','water','rare','charm',{maxEnergy:2},{roundEnergy:1},'+2 maximum energy. Recover 1 energy between rounds while active and conscious.'],
  ['siphonFin','Siphon-fin Wraps','water','rare','weapon',{atk:1},{lifesteal:.06,elementDamage:{water:.06}},'+1 attack. Restore health equal to 6% of damage dealt; deal 6% more Water damage.'],
  ['oceanheart','Oceanheart Mail','water','legendary','armor',{hp:8,def:1},{roundHeal:.025,roundShield:.04},'+8 maximum health and +1 defense. Between rounds while active and conscious, restore 2.5% maximum health and gain a shield worth 4% maximum health.'],

  ['staticWrist','Static Wrist-wraps','storm','common','weapon',{atk:1},{elementDamage:{storm:.04}},'+1 attack. Deal 4% more Storm damage.'],
  ['skyRunner','Sky-runner Anklet','storm','uncommon','charm',{speed:2},{},'+2 speed.'],
  ['dynamoLoop','Dynamo Loop','storm','rare','charm',{maxEnergy:2},{energyDamage:.10},'+2 maximum energy. Deal 10% more damage with abilities that spend energy.'],
  ['thunderfoil','Thunderfoil Knuckles','storm','rare','weapon',{atk:1},{elementDamage:{storm:.12},physicalDamage:.04},'+1 attack. Deal 12% more Storm damage and 4% more physical damage.'],
  ['tempestEngine','Tempest Engine','storm','legendary','charm',{speed:2,maxEnergy:2},{roundEnergy:1,energyDamage:.12},'+2 speed and +2 maximum energy. Recover 1 energy between rounds while active and conscious; deal 12% more damage with abilities that spend energy.'],

  ['pebbleguard','Pebbleguard Vest','earth','common','armor',{def:1,hp:3},{},'+1 defense and +3 maximum health.'],
  ['basaltClasp','Basalt Clasp','earth','uncommon','charm',{hp:7},{roundShield:.025},'+7 maximum health. Gain a shield worth 2.5% maximum health between rounds while active and conscious.'],
  ['faultHammer','Faultline Knuckles','earth','rare','weapon',{atk:2},{physicalDamage:.10},'+2 attack. Deal 10% more physical damage.'],
  ['geodeWall','Geode Wall-vest','earth','rare','armor',{def:2},{elementResist:{earth:.12}},'+2 defense. Take 12% less Earth damage.'],
  ['atlasShell','Atlas Shell','earth','legendary','armor',{hp:12,def:1},{roundShield:.06},'+12 maximum health and +1 defense. Gain a shield worth 6% maximum health between rounds while active and conscious.'],

  ['rimecoat','Rimecoat','ice','common','armor',{hp:5},{elementResist:{ice:.04}},'+5 maximum health. Take 4% less Ice damage.'],
  ['frostClaws','Frostbite Claws','ice','uncommon','weapon',{atk:1},{elementDamage:{ice:.08}},'+1 attack. Deal 8% more Ice damage.'],
  ['glacierLocket','Glacier Locket','ice','rare','charm',{maxEnergy:2,hp:5},{},'+2 maximum energy and +5 maximum health.'],
  ['snowmend','Snowmend Mantle','ice','rare','armor',{hp:6},{roundHeal:.025},'+6 maximum health. Restore 2.5% maximum health between rounds while active and conscious.'],
  ['everfrost','Everfrost Prism','ice','legendary','charm',{def:1,maxEnergy:2},{elementDamage:{ice:.12},roundShield:.04},'+1 defense and +2 maximum energy. Deal 12% more Ice damage; gain a shield worth 4% maximum health between rounds while active and conscious.'],

  ['dawnBeads','Dawn Beads','light','common','charm',{maxEnergy:1},{},'+1 maximum energy.'],
  ['sunlace','Sunlace Vest','light','uncommon','armor',{hp:6},{roundHeal:.015},'+6 maximum health. Restore 1.5% maximum health between rounds while active and conscious.'],
  ['haloFists','Halo Fist-wraps','light','rare','weapon',{atk:1},{energyDamage:.10,elementDamage:{light:.06}},'+1 attack. Deal 10% more damage with abilities that spend energy and 6% more Light damage.'],
  ['radiantBattery','Radiant Battery','light','rare','charm',{maxEnergy:3},{roundEnergy:1},'+3 maximum energy. Recover 1 energy between rounds while active and conscious.'],
  ['lifegiver','Lifegiver Crown','light','legendary','charm',{hp:8},{roundHeal:.035,roundEnergy:1},'+8 maximum health. Between rounds while active and conscious, restore 3.5% maximum health and recover 1 energy.'],

  ['duskMitts','Dusk Mitts','shadow','common','weapon',{atk:1},{elementDamage:{shadow:.04}},'+1 attack. Deal 4% more Shadow damage.'],
  ['veilweave','Veilweave Vest','shadow','uncommon','armor',{hp:4},{elementResist:{shadow:.08}},'+4 maximum health. Take 8% less Shadow damage.'],
  ['nightdrink','Nightdrink Fang','shadow','rare','weapon',{atk:1},{lifesteal:.075},'+1 attack. Restore health equal to 7.5% of damage dealt.'],
  ['eclipseSeal','Eclipse Seal','shadow','rare','charm',{maxEnergy:2},{energyDamage:.10},'+2 maximum energy. Deal 10% more damage with abilities that spend energy.'],
  ['moonreaper','Moonreaper Wraps','shadow','legendary','weapon',{atk:2},{elementDamage:{shadow:.12},lifesteal:.06},'+2 attack. Deal 12% more Shadow damage; restore health equal to 6% of damage dealt.']
];
const icons={weapon:'✦',armor:'◈',charm:'✧'};
export const FORGE_GEAR=Object.fromEntries(rows.map(([id,name,element,rarity,slot,bonuses,effects,desc])=>[id,{
  name,element,rarity,slot,bonuses,effects,desc,icon:icons[slot],fragmentCost:5
}]));
// Metadata for original gear leaves all original stat/amount values unchanged.
export const LEGACY_GEAR_AFFINITIES={fang:{element:'fire',rarity:'common'},shell:{element:'water',rarity:'common'},heart:{element:'leaf',rarity:'common'},wind:{element:'storm',rarity:'common'},satchel:{element:'leaf',rarity:'uncommon'}};
