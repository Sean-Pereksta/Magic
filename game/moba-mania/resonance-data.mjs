/** Two original kits with readable, deterministic resource loops. */
const skill=(id,name,description,color,unlock,mana,cooldown,range,radius,type,extra={})=>({id,name,description,icon:id,color,unlock,mana,cooldown,range,radius,type,...extra});
const pick=(id,name,description,bonuses,icon)=>({id,name,description,bonuses,icon,color:'#fff0cf'});
const tier=(level,...choices)=>({level,choices});

export const RESONANCE_HEROES={
  ferrite:{
    id:'ferrite',name:'Ferrite',title:'The Magnetic Colossus',role:'Frontline · polarity',aiRole:'laner',color:'#65d9e6',
    description:'Salvage orbiting scrap, choose a pull or push, then spend your metal on protection or an explosive magnetic storm.',
    hp:1040,mana:310,speed:166,range:66,damage:54,attackRate:.95,
    passive:{name:'Scrap Orbit',description:'Basic hits and Rail Spike salvage one scrap from non-structures, at most once per 0.65s; hold three. Spend it on Scrap Barrier or Lodestone Storm. Polarity starts on PUSH.',icon:'scrap-barrier'},
    abilities:[
      skill('rail-spike','Rail Spike','Launch a metal spike for 90 damage. Its first enemy hit generates scrap; terrain and intervening bodies block it.','#65d9e6',1,30,6,400,15,'line',{theme:'magnet'}),
      skill('scrap-barrier','Scrap Barrier','Consume all scrap. Gain a 100 shield plus 50 per scrap for 4s. Saving three scraps makes a much stronger barrier.','#b8ecf1',2,42,13,0,150,'self',{theme:'magnet'}),
      skill('polarity-pulse','Polarity Pulse','Switch PUSH ↔ PULL. After a 0.35s warning, deal 75 damage and move nearby enemies once: 75 inward or 100 outward.','#e96b76',4,45,10,0,155,'self',{theme:'magnet'}),
      skill('lodestone-storm','Lodestone Storm','Consume scrap and warn for 0.8s: 150 damage +35 per scrap, a 30% slow for 2s, and a pull or push matching your current polarity.','#a5edf0',6,78,25,330,155,'ground',{theme:'magnet'})
    ],
    talents:[
      tier(3,pick('ferrite-steel-point','Steel Point','Rail Spike deals 25% more damage.',{ferriteRailDamage:.25},'rail-spike'),pick('ferrite-heavy-chassis','Heavy Chassis','Gain 18% maximum health.',{hp:.18},'health'),pick('ferrite-tempered-scrap','Tempered Scrap','Scrap Barrier shields 20% more.',{ferriteBarrierPower:.2},'scrap-barrier')),
      tier(5,pick('ferrite-long-rails','Long Rails','Rail Spike reaches 18% farther.',{ferriteRailRange:.18},'rail-spike'),pick('ferrite-hover-drive','Hover Drive','Gain 10% movement speed.',{speed:.1},'speed'),pick('ferrite-grounding','Grounding','Gain 15% tenacity, subject to the shared cap.',{tenacity:.15},'fortress')),
      tier(8,pick('ferrite-wide-field','Wide Field','Polarity Pulse covers 20% more area.',{ferritePulseArea:.2},'polarity-pulse'),pick('ferrite-layered-alloy','Layered Alloy','Each scrap adds 65 instead of 50 base shielding to Scrap Barrier.',{ferriteScrapShield:15},'scrap-barrier'),pick('ferrite-industrious','Industrious','Gain 16% attack speed to collect scrap in combat.',{attackSpeed:.16},'haste')),
      tier(10,pick('ferrite-hard-metal','Hard Metal','Gain 15% general damage.',{damage:.15},'damage'),pick('ferrite-short-circuit','Short Circuit','Reduce ability cooldowns by 12%.',{cooldown:.12},'cooldown'),pick('ferrite-deep-core','Deep Core','Gain 10% health and 20% shield strength.',{hp:.1,shield:.2},'shield')),
      tier(12,pick('ferrite-static-point','Static Point','Rail Spike also slows its target 20% for 1.5s.',{ferriteStaticPoint:1},'rail-spike'),pick('ferrite-shared-alloy','Shared Alloy','Scrap Barrier also grants allied heroes within 150 a shield worth 40% of yours.',{ferriteSharedAlloy:1},'scrap-barrier'),pick('ferrite-induction','Induction','Polarity Pulse salvages one scrap if it damages a non-structure, once per cast.',{ferriteInduction:1},'polarity-pulse')),
      tier(15,pick('ferrite-through-line','Through Line','Rail Spike pierces two additional enemies. A shot still generates at most one scrap.',{ferritePierce:2},'rail-spike'),pick('ferrite-insulated-core','Insulated Core','Scrap Barrier reduces incoming damage by 15% for its first 2s.',{ferriteInsulated:1},'fortress'),pick('ferrite-lighten-load','Lighten the Load','Spending three scraps on Scrap Barrier grants 20% movement speed for 2s.',{ferriteLighten:1},'speed')),
      tier(18,pick('ferrite-aftershock','Aftershock','Lodestone Storm warns of a second blast after 0.6s: 55% damage, smaller radius, no extra movement.',{ferriteAftershock:1},'lodestone-storm'),pick('ferrite-monolith','Monolith','Lodestone Storm also grants you a 90 shield plus 35 per scrap for 4s.',{ferriteMonolith:1},'scrap-barrier'),pick('ferrite-mass-driver','Mass Driver','Lodestone Storm moves enemies 45 farther and its slow lasts 1s longer.',{ferriteMassDriver:1},'polarity-pulse'))
    ]
  },
  cadence:{
    id:'cadence',name:'Cadence',title:'The Wandering Resonator',role:'Support · musical combos',aiRole:'support',color:'#ef9da8',
    description:'Compose Q, E and R notes. Three different notes charge a burst on your next basic hit; the last note chooses your ultimate’s damage, shield or dance-field form.',
    hp:720,mana:370,speed:184,range:235,damage:40,attackRate:.83,
    passive:{name:'Three-Note Score',description:'Q, E and R record notes for 8s. Three different consecutive notes charge your next basic hit for 8s: a 45-damage sound burst. F consumes the score; its last note selects the finale.',icon:'grand-finale'},
    abilities:[
      skill('piercing-note','Piercing Note','Send a ringing note through up to three enemies for 75 damage each. Records Q: a Shatter Finale creates a second warned explosion.','#ef9da8',1,28,5.8,420,15,'line',{theme:'resonance'}),
      skill('sheltering-chime','Sheltering Chime','Grant allied heroes in the circle a 75 shield for 3s. Records E: a Shelter Finale grants a powerful group shield.','#ffedbf',2,40,10,310,115,'ground',{theme:'resonance'}),
      skill('skipbeat','Skipbeat','Dash up to 140, stopping at terrain, and gain 20% movement speed for 2s. Records R: a Dance Finale creates a slowing, hastening stage.','#ebb9f0',4,34,11,140,24,'line',{theme:'resonance',mobility:true}),
      skill('grand-finale','Grand Finale','Consume the score. After 0.65s deal 120 damage. Last Q: second 80-damage blast. Last E: 160 allied shield. Last R: 4s dance field. A full three-note chord widens the finale 20%.','#ffedbf',6,75,25,340,150,'ground',{theme:'resonance'})
    ],
    talents:[
      tier(3,pick('cadence-clear-tone','Clear Tone','Piercing Note deals 25% more damage.',{cadenceNoteDamage:.25},'piercing-note'),pick('cadence-travel-cloak','Travel Cloak','Gain 15% maximum health.',{hp:.15},'health'),pick('cadence-soft-chime','Soft Chime','Sheltering Chime shields 25% more.',{cadenceChimePower:.25},'sheltering-chime')),
      tier(5,pick('cadence-far-song','Far Song','Targeted abilities reach 12% farther.',{abilityRange:.12},'range'),pick('cadence-quickstep','Quickstep','Gain 12% movement speed.',{speed:.12},'speed'),pick('cadence-long-phrase','Long Phrase','Notes stay in the score for 10s instead of 8s.',{cadenceNoteWindow:2},'grand-finale')),
      tier(8,pick('cadence-loud-chord','Loud Chord','The Three-Note Score basic-hit burst deals 30% more damage.',{cadenceChordDamage:.3},'grand-finale'),pick('cadence-broad-stage','Broad Stage','Sheltering Chime covers 20% more area.',{cadenceChimeArea:.2},'sheltering-chime'),pick('cadence-long-step','Long Step','Skipbeat travels 20% farther.',{cadenceStepRange:.2},'skipbeat')),
      tier(10,pick('cadence-fast-measure','Fast Measure','Reduce ability cooldowns by 12%.',{cooldown:.12},'cooldown'),pick('cadence-bright-song','Bright Song','Gain 18% ability damage.',{spellDamage:.18},'damage'),pick('cadence-deep-breath','Deep Breath','Gain 12% maximum mana and 20% shield strength.',{mana:.12,shield:.2},'shield')),
      tier(12,pick('cadence-reverberation','Reverberation','Piercing Note warns of a 25-damage echo at its first hero hit; radius 65, after 0.4s.',{cadenceReverberation:1},'piercing-note'),pick('cadence-shared-rhythm','Shared Rhythm','A charged basic hit also grants you 15% movement and attack speed for 2s.',{cadenceSharedRhythm:1},'haste'),pick('cadence-soft-landing','Soft Landing','Skipbeat grants a 50 shield for 2s.',{cadenceSoftLanding:1},'skipbeat')),
      tier(15,pick('cadence-overtone','Overtone','Sheltering Chime also grants its allied heroes 15% attack speed for 2s.',{cadenceOvertone:1},'sheltering-chime'),pick('cadence-long-refrain','Long Refrain','Skipbeat’s movement boost lasts 1.5s longer.',{cadenceStepDuration:1.5},'skipbeat'),pick('cadence-holding-note','Holding Note','Piercing Note slows enemies 20% for 1.2s.',{cadenceHoldingNote:1},'piercing-note')),
      tier(18,pick('cadence-encore','Encore','Shatter Finale’s second blast deals 120 instead of 80 base damage.',{cadenceEncore:40},'grand-finale'),pick('cadence-grand-shelter','Grand Shelter','Shelter Finale shields 25% more and clears current slows from its recipients.',{cadenceGrandShelter:1},'sheltering-chime'),pick('cadence-last-dance','Last Dance','Dance Finale’s stage lasts 6s instead of 4s.',{cadenceLastDance:2},'skipbeat'))
    ]
  }
};
for(const hero of Object.values(RESONANCE_HEROES))for(const level of hero.talents)for(const option of level.choices)option.color=hero.color;
