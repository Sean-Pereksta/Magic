// Save-stable equipment IDs. Values describe tier one; combat applies bounded
// upgrade scaling to bonuses/effects rather than multiplying them by the tier.
export const GEAR_RARITIES={
  common:{name:'Common',color:'#a7aaa9',symbol:'●',mark:'stone'},
  uncommon:{name:'Uncommon',color:'#79cc91',symbol:'❧',mark:'leaf'},
  rare:{name:'Rare',color:'#83aeff',symbol:'◆',mark:'diamond'},
  epic:{name:'Epic',color:'#c798f4',symbol:'★',mark:'star'},
  legendary:{name:'Legendary',color:'#ffd27e',symbol:'♛',mark:'crown'},
  mythic:{name:'Mythic',color:'#ff897f',symbol:'✺',mark:'crest'}
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
export const ORIGINAL_FORGE_GEAR=Object.fromEntries(rows.map(([id,name,element,rarity,slot,bonuses,effects,desc])=>[id,{
  name,element,rarity,slot,bonuses,effects,desc,icon:icons[slot],fragmentCost:5
}]));
// A separate expansion preserves every released definition and save-stable ID.
// Percent bonuses are additive within a category and scale with upgrade rank.
const expansionRows=[
 ['bambooKnuckles','Bamboo Knuckles','leaf','common','weapon',{atk:.04},{},'Increase Attack by 4%.','Damage'],
 ['pebbleGuard','Pebble Guard','earth','common','armor',{def:.05},{},'Increase Defense by 5%.','Protection'],
 ['emberFang','Ember Fang','fire','common','weapon',{}, {elementDamage:{fire:.05}},'Increase damage from the wearer’s Fire abilities by 5%.','Damage'],
 ['breezeRibbon','Breeze Ribbon','storm','common','charm',{speed:.05},{},'Increase Speed by 5%.','Switching'],
 ['riverglassShell','Riverglass Shell','water','common','armor',{hp:.05},{},'Increase maximum health by 5%.','Protection'],
 ['frostButton','Frost Button','ice','common','charm',{}, {elementResist:{ice:.08}},'Reduce incoming Ice damage by 8%.','Protection'],
 ['duskBand','Dusk Band','shadow','common','charm',{}, {firstHitReduction:.10},'Reduce the first damaging enemy hit received each battle by 10%.','Protection'],
 ['sunseedPendant','Sunseed Pendant','light','common','charm',{}, {consumableHealing:.08},'Healing consumables used on the wearer restore 8% more health.','Healing'],
 ['foragersPouch','Forager’s Pouch','neutral','common','charm',{}, {foragerPouch:12},'Every 12 ordinary resource discoveries, gain one extra common resource from that discovery’s eligible pool. Bonus resources are not new discoveries.','Exploration'],
 ['trackersKnot','Tracker’s Knot','neutral','common','charm',{}, {captureBonus:.05},'While the wearer is active, increase eligible capture chances by 5% of their existing value, respecting the capture cap.','Exploration'],
 ['springbudLocket','Springbud Locket','leaf','uncommon','charm',{hp:.04},{thirdTurnHeal:.04},'Increase maximum health by 4%. At the end of the wearer’s third turn, heal 4% maximum health, once per battle.','Healing'],
 ['rootstoneCuffs','Rootstone Cuffs','earth','uncommon','armor',{}, {healthyDefense:.08},'Increase Defense by 8% while above 50% health.','Protection'],
 ['cinderOpener','Cinder Opener','fire','uncommon','weapon',{}, {firstAttackDamage:.12},'The wearer’s first damaging attack each battle deals 12% more damage.','Damage'],
 ['stormstepAnklet','Stormstep Anklet','storm','uncommon','charm',{}, {stormSpeed:.08},'Using a Storm ability grants 8% Speed through the end of the next wearer turn. Reapplying refreshes duration.','Switching'],
 ['tidecallShell','Tidecall Shell','water','uncommon','armor',{}, {switchInHeal:.05},'The wearer’s first voluntary switch-in restores 5% maximum health, once per battle. Starting deployment and knockout replacements do not count.','Switching,Healing'],
 ['rimeScarf','Rime Scarf','ice','uncommon','armor',{}, {firstIceShield:.05},'The wearer’s first successful Ice attack grants a shield worth 5% maximum health, once per battle.','Protection'],
 ['duskglassLens','Duskglass Lens','shadow','uncommon','charm',{}, {shieldDamage:.12},'Deal 12% more damage to enemy shields. The bonus applies only to shield damage.','Damage'],
 ['dawnthreadBracelet','Dawnthread Bracelet','light','uncommon','charm',{}, {firstSupportHeal:.04},'The first non-damaging support ability used restores 4% maximum health, once per battle.','Healing'],
 ['campwalkerBrooch','Campwalker Brooch','neutral','uncommon','charm',{}, {distanceHealing:.10},'Existing distance-based healing restores 10% more health to the wearer. A base 2% heal becomes 2.2%.','Exploration,Healing'],
 ['fruitkeeperClip','Fruitkeeper Clip','neutral','uncommon','charm',{}, {trainingExperience:.05},'Training fruit used on the wearer grants 5% more training experience.','Exploration'],
 ['briarloopBand','Briarloop Band','leaf','rare','charm',{}, {reflectDamage:.10},'Return 10% of direct health damage received to the attacker, capped at 3% of wearer maximum health per enemy turn. Returned damage cannot trigger equipment effects.','Protection,Damage'],
 ['quakehideBelt','Quakehide Belt','earth','rare','armor',{}, {woundedReduction:.10},'Reduce incoming damage by 10% while below 40% health.','Protection'],
 ['charcoalHeart','Charcoal Heart','fire','rare','charm',{}, {woundedAttack:.10},'Increase Attack by 10% while below 50% health.','Damage'],
 ['staticRelayCharm','Static Relay Charm','storm','rare','charm',{}, {switchOutSpeed:.12},'On the wearer’s first voluntary switch-out, the incoming ally gains 12% Speed for two wearer turns, once per battle.','Switching'],
 ['dewdropChalice','Dewdrop Chalice','water','rare','charm',{}, {firstAbilityHealing:.20},'The wearer’s first healing ability each battle restores 20% more health.','Healing'],
 ['frostguardBrooch','Frostguard Brooch','ice','rare','charm',{}, {effectiveHitShield:.08},'After surviving a super-effective hit, gain a shield worth 8% maximum health, once per battle.','Protection'],
 ['nightfangTooth','Nightfang Tooth','shadow','rare','weapon',{}, {healthyTargetDamage:.10},'Deal 10% more damage to enemies above 75% health.','Damage'],
 ['dawnRelayBell','Dawn Relay Bell','light','rare','charm',{}, {switchOutHeal:.06},'On the wearer’s first voluntary switch-out, restore 6% of the incoming ally’s maximum health, once per battle.','Switching,Healing'],
 ['resourceLens','Resource Lens','neutral','rare','charm',{}, {resourceUpgrade:.08},'Ordinary resource discoveries have an 8% chance to upgrade one common material to its next eligible quality tier.','Exploration'],
 ['trailfinderCompass','Trailfinder Compass','neutral','rare','charm',{}, {encounterWeight:.10},'Choose an element. Increase its eligible wild-encounter selection weight by 10% while in the expedition team. Restricted species remain restricted.','Exploration'],
 ['heartbloomLocket','Heartbloom Locket','leaf','epic','charm',{}, {criticalHeal:.10},'When enemy damage first leaves the surviving wearer below 35% health, heal 10% maximum health, once per battle.','Healing'],
 ['quakeboundBracers','Quakebound Bracers','earth','epic','weapon',{atk:.12,speed:-.04},{},'Increase Attack by 12% and reduce Speed by 4%.','Damage'],
 ['furnaceBeads','Furnace Beads','fire','epic','charm',{}, {consecutiveDamage:.15},'After dealing damage on two consecutive wearer turns, the next damaging attack gains 15% damage. Then restart the sequence. Switching interrupts it.','Damage'],
 ['tempestSpurs','Tempest Spurs','storm','epic','charm',{}, {entrySpeed:.12},'On the wearer’s first entry into battle, gain 12% Speed for three wearer turns.','Switching'],
 ['tidekeeperPearls','Tidekeeper Pearls','water','epic','charm',{}, {periodicHeal:.06},'At the end of every third wearer turn, restore 6% maximum health, up to two activations per battle.','Healing'],
 ['glacierAegis','Glacier Aegis','ice','epic','armor',{}, {entryShield:.12,matchingEntryShield:.16},'On first entry, gain a shield worth 12% maximum health, or 16% for an Ice wearer, once per battle.','Protection'],
 ['moonfangTalon','Moonfang Talon','shadow','epic','weapon',{}, {criticalTargetDamage:.15},'Deal 15% more damage to enemies below 35% health.','Damage'],
 ['sunweaverSeal','Sunweaver Seal','light','epic','charm',{}, {abilityHealShield:.05},'A healing ability also shields its living recipient for 5% of their maximum health, up to two ability activations per battle.','Healing,Protection'],
 ['supplySatchel','Supply Satchel','neutral','epic','charm',{}, {campEssence:1},'Reaching a new trail-camp encounter grants one extra region-appropriate uncommon essence, once for that camp encounter.','Exploration'],
 ['trailblazerMedal','Trailblazer Medal','neutral','epic','charm',{}, {discoveryReduction:.05},'Reduce ordinary discovery distance requirements by 5%. Region unlocks and distance challenges keep their normal requirements.','Exploration'],
 ['canopyHeart','Canopy Relay Heart','leaf','legendary','charm',{hp:.10},{switchOutShield:.12},'Increase maximum health by 10%. First voluntary switch-out shields the incoming ally for 12% of the ally’s maximum health, once per battle.','Switching,Protection'],
 ['mountainheartTotem','Mountainheart Totem','earth','legendary','charm',{def:.08},{effectiveHitNextReduction:.20},'Increase Defense by 8%. After surviving a super-effective hit, reduce the next enemy attack’s damage by 20%, once per battle.','Protection'],
 ['infernoSignet','Inferno Signet','fire','legendary','charm',{atk:.08},{firstHigherFireDamage:.20},'Increase Attack by 8%. The first Fire attack against an enemy with a higher health percentage deals 20% more damage, once per battle.','Damage'],
 ['thunderheadCrest','Thunderhead Crest','storm','legendary','charm',{speed:.08},{switchOutDamage:.18},'Increase Speed by 8%. First voluntary switch-out strengthens the incoming ally’s next damaging attack by 18%, once per battle.','Switching,Damage'],
 ['tideglassPendant','Tideglass Pendant','water','legendary','charm',{}, {surviveFatal:true},'The first otherwise fatal direct enemy hit leaves the wearer at 1 health, once per battle. Damage over time and equipment damage do not qualify.','Protection'],
 ['permafrostMantle','Permafrost Mantle','ice','legendary','armor',{def:.08},{woundedShield:.18},'Increase Defense by 8%. First surviving drop below 40% health from enemy damage grants an 18% maximum-health shield, once per battle.','Protection'],
 ['eclipseTalon','Eclipse Talon','shadow','legendary','weapon',{speed:.06},{switchInAttack:.25,switchInVulnerability:.10},'Increase Speed by 6%. First damaging attack after a voluntary switch-in gains 25% damage, but incoming damage increases 10% until that attack. Once per battle.','Switching,Damage'],
 ['dawnbringerDiadem','Dawnbringer Diadem','light','legendary','charm',{hp:.08},{reserveHeal:.08},'Increase maximum health by 8%. First healing ability also heals the living reserve ally with the lowest health percentage for 8% maximum health, once per battle.','Healing'],
 ['trailwardStandard','Trailward Standard','neutral','legendary','charm',{}, {knockoutShield:.08,knockoutDamage:.10},'When first knocked out, the wearer’s replacement gains an 8% maximum-health shield and 10% damage for two wearer turns, once per battle.','Switching,Protection'],
 ['prismCompass','Prism Compass','neutral','legendary','charm',{}, {prismConvert:.15},'Choose an eligible element. Elemental-resource discoveries have a 15% chance to convert one awarded essence to that element, preserving quantity and quality.','Exploration'],
 ['worldrootHeart','Worldroot Heart','leaf','mythic','charm',{hp:.12},{switchOutTeamHeal:.08},'Increase maximum health by 12%. First voluntary switch-out heals every living expedition monkey for 8% maximum health, once per team per battle.','Switching,Healing'],
 ['heartOfTheFirstMountain','Heart of the First Mountain','earth','mythic','charm',{def:.12},{healthyFirstHitReduction:.25},'Increase Defense by 12%. Reduce the first damaging enemy hit received while above 50% health by 25%, once per battle.','Protection'],
 ['sunforgeCrown','Sunforge Crown','fire','mythic','charm',{atk:.10},{effectiveNextFireDamage:.25},'Increase Attack by 10%. The wearer’s first super-effective hit empowers its next Fire attack with 25% extra damage, once per battle.','Damage'],
 ['eyeOfTheTempest','Eye of the Tempest','storm','mythic','charm',{speed:.10},{replacementSpeed:.15,replacementDamage:.20},'Increase Speed by 10%. On first entry replacing a knocked-out ally, gain another 15% Speed for two wearer turns and 20% damage on the next damaging attack.','Switching,Damage'],
 ['oceanmemoryOrb','Oceanmemory Orb','water','mythic','charm',{hp:.10},{delayedRecovery:.50},'Increase maximum health by 10%. After the first surviving enemy hit, recover half its health damage at the end of the next wearer turn, capped at 15% maximum health.','Healing'],
 ['wintercrownRelic','Wintercrown Relic','ice','mythic','charm',{def:.10},{shieldBreakHeal:.08,shieldBreakAttack:.10},'Increase Defense by 10%. When enemy damage first breaks a shield, heal 8% maximum health and gain 10% Attack for two wearer turns, once per battle.','Protection,Healing'],
 ['nightsovereignFangs','Nightsovereign Fangs','shadow','mythic','weapon',{atk:.08},{limitedLifesteal:.08},'Increase Attack by 8%. Direct attacks heal for 8% of actual health damage dealt, capped at 3% maximum health per wearer turn, for up to five turns per battle.','Damage,Healing'],
 ['beaconOfTheFirstDawn','Beacon of the First Dawn','light','mythic','charm',{}, {teamRevive:.15},'While alive, including in reserve, the first other ally knocked out returns to reserve with 15% health, once per team per battle. Cannot revive the wearer or start a revival chain; no immediate action.','Healing'],
 ['wanderkingCrown','Wanderking Crown','neutral','mythic','charm',{}, {diverseTeamStats:.10},'While all three expedition monkeys have different elements, the wearer gains 10% Attack and 10% Defense.','Damage,Protection'],
 ['trailforgersPrism','Trailforger’s Prism','neutral','mythic','charm',{}, {craftRefund:5},'After crafting, return one common ingredient for every five common ingredients actually spent after camp discounts, rounded down. Apply once per craft across the team; rare materials and essences excluded.','Crafting']
];
const purposeSummary={Damage:'Strengthens attacks for an offensive loadout.',Protection:'Helps the wearer survive dangerous attacks.',Healing:'Restores health during a long expedition.',Switching:'Rewards well-timed team changes.',Exploration:'Improves expedition discoveries and supplies.',Crafting:'Makes resource crafting more efficient.'};
export const EXPANSION_GEAR=Object.fromEntries(expansionRows.map(([id,name,element,rarity,slot,statsPct,effects,desc,purposes])=>{
 const purpose=purposes.split(',').map(value=>value.toLowerCase());
 return[id,{name,element,affinity:element==='neutral'?null:element,rarity,slot,bonuses:{},statsPct,effects,desc,purpose,newItem:true,icon:icons[slot],fragmentCost:5,
  summary:purposeSummary[purposes.split(',')[0]],
  acquisition:`Resource-mixing crafting${rarity==='rare'?'; suitable regional challenges and milestones':''}. ${element==='neutral'?'Mixed resources keep neutral outcomes eligible.':`${element[0].toUpperCase()+element.slice(1)} essence increases this item’s crafting weight.`}`,
  ...(effects.discoveryReduction?{discoveryReduction:effects.discoveryReduction}:{}),...(effects.encounterWeight||effects.prismConvert?{selectableElement:true}:{})}];
}));
export const FORGE_GEAR={...ORIGINAL_FORGE_GEAR,...EXPANSION_GEAR};
// Metadata for original gear leaves all original stat/amount values unchanged.
export const LEGACY_GEAR_AFFINITIES={fang:{element:'fire',rarity:'common'},shell:{element:'water',rarity:'common'},heart:{element:'leaf',rarity:'common'},wind:{element:'storm',rarity:'common'},satchel:{element:'leaf',rarity:'uncommon'}};
