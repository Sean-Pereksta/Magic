/* Shared sprite families: beetle, boar, frog, moth, serpent, bird, golem, wisp,
   wolf, bat, crab, shroom, mantis, turtle, scorpion, stag, snail, fox, spider, axolotl, octopus, ray, ram, owl, pangolin, hare, jellyfish, dragonfly, elk, griffin, mole, dragon. */
(() => {
  'use strict';
  const elements = {
    leaf: {name:'Leaf',color:'#87cf71'}, fire: {name:'Fire',color:'#ff8855'},
    frost: {name:'Frost',color:'#8cddea'}, storm: {name:'Storm',color:'#f1cd62'},
    stone: {name:'Stone',color:'#d5ad89'}, spirit: {name:'Spirit',color:'#c6a0ee'}
  };
  const regions = [
    ['Canopy Verge','Sunbeams thread a road of tangled roots.','#78bb83'],
    ['Cinder Orchard','Embers sift through the blackened fruit trees.','#eb895c'],
    ['Glacial Steps','Blue ice hums beneath the mountain trail.','#83cedb'],
    ['Tempest Cliffs','Lightning nests on the windswept ledges.','#b4a6df'],
    ['Amber Badlands','Fossil ridges rise out of the copper dust.','#e4af71'],
    ['Moonlit Fen','Foxfire lights a path across the water.','#97afc9'],
    ['Sunken Archive','Broken shelves emerge from a drowned city.','#71c5b6'],
    ['Astral Crown','Starstones mark the stair down toward a distant sea.','#b296db'],
    ['Coral Causeway','At low tide, coral arches rise above a ribbon of blue pools.','#e5a58e'],
    ['Brasswind Foundry','Furnaces glow behind the turning teeth of brass flywheels.','#da9c61'],
    ['Sporelight Hollow','Blue-green spores drift between towering mushroom caps.','#90d4b8'],
    ['Saltglass Dunes','Sun-struck pyramids watch over a field of singing salt crystals.','#dbca9c'],
    ['Cloudbell Monastery','Pagoda bells ring across the gaps between floating islands.','#c3c3e7'],
    ['Petrified March','Fossil trunks and bleached ribs rise from a red-earth trail.','#c6ac86'],
    ['Lantern Reef','Luminous kelp fronds brush the path beneath a dark sea.','#73d9d0'],
    ['Roseglass Court','Wild roses wind through the panes of a stained-glass pavilion.','#dfa0b2'],
    ['Blackice Fjord','The aurora hangs above black ice and the ribs of longships.','#8cbfda'],
    ['Clockwork Rookery','Pendulums swing beneath the nests of brass-feathered birds.','#d5b577'],
    ['Meteor Quarry','Fallen stars hover above the pits where miners first found them.','#b6a0c9'],
    ['Dawnroot Summit','Gold leaves frame the rising sun at the crest of the world-tree.','#ecd18a'],
    ['Verdigris Aqueduct','Turquoise canals braid beneath the broken arches of a copper-green city.','#72c6b1'],
    ['Emberglass Caldera','Rivers of flame shine through the black glass of a sleeping volcano.','#ed8a69'],
    ['Hushsnow Orchard','Snow-laden branches shelter lanterns that have burned for a thousand winters.','#b7d9d4'],
    ['Thunderstep Savanna','Golden grass bends beneath thunderheads and the silhouettes of storm totems.','#dbbd72'],
    ['Obsidian Ossuary','Pale ribs and polished black pillars guard the resting place of mountain giants.','#b29cbe'],
    ['Dreamtide Marsh','Reflected moons drift upstream through violet reeds and half-remembered doorways.','#b3a0df'],
    ['Honeycomb Cathedral','Amber light falls through waxen towers while enormous hives hum like an organ.','#e5bf79'],
    ['Ashen Railworks','Old rails thread the cinders below skeletal gantries and still-glowing signal lamps.','#c78f7b'],
    ['Winterglass Conservatory','Frozen glasshouses shelter silver vines whose blossoms ring when touched.','#a4dbe2'],
    ['Copperstorm Flats','Lightning walks from spire to copper spire across a sea of blue-green salt.','#7dccc5'],
    ['Titanroot Canyon','Roots thicker than towers bridge the red cliffs above a river of drifting seeds.','#c8aa76'],
    ['Mothmoon Opera','Velvet curtains stir in an abandoned amphitheater beneath a luminous moth-wing moon.','#c6a0d8'],
    ['Bloodbloom Basin','Scarlet flowers drink the red mist as hanging vines weave bridges over still water.','#db929a'],
    ['Shattered Aurora','Ice islands hang in the sky, joined by bright ribbons torn from the northern lights.','#a4c9ed'],
    ['Everdawn Observatory','Ivory orreries turn above the clouds, measuring the first sunrise of every world.','#ead39a']
  ].map(([name,subtitle,color])=>({name,subtitle,color}));
  const rarities = {
    common:{name:'Common',color:'#b8c6a6',rank:0,powerScale:1},
    uncommon:{name:'Uncommon',color:'#8ed18a',rank:1,powerScale:1.12},
    rare:{name:'Rare',color:'#79bfff',rank:2,powerScale:1.28},
    epic:{name:'Epic',color:'#c59aff',rank:3,powerScale:1.5},
    mythic:{name:'Mythic',color:'#ffd17a',rank:4,powerScale:1.8}
  };
  const items = {};
  const milestones = {3:'Echo • An afterimage follows each activation for 40% damage.',5:'Guard pulse • Each activation adds a shield in a visible ring.',10:'Nova • Each activation adds a burst for 65% damage.'};
  const utilityStyles = {
    heal:{name:'Healing',role:'healing',milestones:{3:'Echo • An afterglow adds 40% more healing.',5:'Renewal • Healing grows by another 20% of relic power.',10:'Bloom • A restorative pulse adds 65% more healing.'}},
    shield:{name:'Protection',role:'protection',milestones:{3:'Echo • A second ring adds 40% more shield.',5:'Bastion • Shield grows by another 20% of relic power.',10:'Sanctuary • A ward pulse adds 65% more shield.'}},
    stun:{name:'Stun',role:'control',milestones:{3:'Echo • The interruption lasts 40% longer.',5:'Stillness • Stun duration grows by another 20%.',10:'Thunderlock • Stun duration grows by another 65%.'}},
    freeze:{name:'Freeze',role:'control',milestones:{3:'Echo • The freeze lasts 40% longer.',5:'Deep winter • Freeze duration grows by another 20%.',10:'Permafrost • Freeze duration grows by another 65%.'}}
  };
  const effects = {
    strike:'Strikes once each cooldown.', rapid:'A second blow follows after 0.13 seconds for 23% of the initial hit’s damage.',
    burn:'Sets the target on fire. The burn starts at 60% of the hit’s damage and smolders over time.',
    poison:'Adds poison equal to 23% of the hit’s damage. Poison stacks and ticks once per second.',
    chill:'Chills the target for 2.1 seconds, halving its attack-meter gain.', shock:'Interrupts the attack meter for 0.44 seconds. Overlapping shocks can hold it for up to 0.95 seconds.',
    pierce:'Bypasses a foe’s armor reduction and its protective ward.', guard:'Deals damage and grants shield equal to 60% of relic power. Total shield can reach 40% of maximum HP.',
    drain:'Steals health equal to 23% of the HP removed by the initial hit. Weakness and resistance change the amount stolen.',
    mend:'Hits a foe and restores HP equal to 55% of relic power. Healing is independent of the damage the foe takes.',
    heal:'Restores HP equal to relic power. Deals no damage and applies no offensive elemental effects.',
    shield:'Builds a shield equal to relic power, up to 40% of maximum HP. Deals no damage and applies no offensive elemental effects.',
    stun:'Stuns one foe, stopping its attack meter. Duration grows with relic level and potency. Deals no damage.',
    freeze:'Freezes one foe, stopping its attack meter and movement. Duration grows with relic level and potency. Deals no damage.'
  };
  function item(id,name,element,style,power,cooldown,icon,flavor,recipe=null,rarity='common',unlockRound=1,dropRegion=null) {
    const utility=utilityStyles[style];
    items[id]={id,name,element,style,power,cooldown,icon,description:flavor+' '+effects[style],milestones:utility?.milestones||milestones,
      role:utility?.role||'damage',dealsDamage:!utility,effectDescription:effects[style],fusionDepth:0,
      rarity,unlockRound,unlockRegion:Math.floor((unlockRound-1)/4),dropRegion};
    if(style==='stun'||style==='freeze')items[id].duration=style==='stun'?.75:1;
    if(recipe) items[id].recipe=recipe;
  }
  // Base relics join the regular drop pool. Fusion records are defined below.
  const basics = [
    ['stick','Sproutstaff','leaf','strike',9,1.85,'staff','A green shoot curls around its tip.'],
    ['thorn','Thorn Dart','leaf','poison',6,2.15,'dart','A hollow spine filled with nettle sap.'],
    ['seed','Snapseed','leaf','rapid',5,1.45,'seed','This rattling seedpod scatters hard kernels.'],
    ['gourd','Nectar Gourd','leaf','mend',5,3.3,'gourd','A sip of blossom nectar steadies the traveler.'],
    ['coal','Cinderstone','fire','burn',8,2.65,'rock','A pocket coal still aglow at its heart.'],
    ['torch','Orchard Torch','fire','burn',7,2.05,'torch','A branch dipped in sweet-smelling pitch.'],
    ['pepper','Rocket Pepper','fire','rapid',6,1.55,'pepper','This fruit pops like a string of firecrackers.'],
    ['fang','Kiln Fang','fire','pierce',12,2.6,'fang','A sliver of furnace glass with an ember edge.'],
    ['shard','Rime Shard','frost','chill',8,2.45,'shard','A glacier splinter that never quite melts.'],
    ['bell','Snow Bell','frost','guard',7,3.15,'bell','Its muted chime calls a shell of cold air.'],
    ['flake','Sleet Star','frost','rapid',5,1.4,'star','Six knife-edged points shimmer in flight.'],
    ['needle','Icicle Needle','frost','pierce',11,2.35,'needle','Clear as glass and sharper than steel.'],
    ['coil','Copper Coil','storm','shock',8,2.45,'coil','A crooked wire crackles between its turns.'],
    ['feather','Gale Feather','storm','rapid',5,1.2,'feather','Its hooked vane cuts through the wind.'],
    ['drum','Thunder Drum','storm','strike',15,3.05,'drum','One little tap brings a roll of thunder.'],
    ['kite','Cloud Kite','storm','guard',6,2.75,'kite','A kite woven from the skin of a cloud.'],
    ['sling','Pebble Sling','stone','strike',8,1.65,'sling','A sling sized for a monkey’s paw.'],
    ['shell','Carapace Plate','stone','guard',7,2.95,'shield','A discarded shell, polished smooth on the inside.'],
    ['pick','Flint Pick','stone','pierce',12,2.5,'pick','A chip of flint lashed to a short handle.'],
    ['idol','Basalt Idol','stone','strike',17,3.5,'idol','Its stone feet remember the mountain.'],
    ['wick','Wisp Lantern','spirit','drain',8,2.9,'lantern','A blue flame drinks the dusk.'],
    ['bead','Prayer Beads','spirit','mend',5,3.05,'beads','Warm wooden beads hum under the fingers.'],
    ['mask','Hollow Mask','spirit','drain',10,3.25,'mask','An empty face that breathes in borrowed life.'],
    ['fan','Phantom Fan','spirit','chill',7,2.15,'fan','Each fold hides a breath of the underworld.']
  ];
  basics.push(
    ['bamboo','Split Bamboo Bow','leaf','pierce',10,2.1,'bow','A sapling bow bends without breaking.'],
    ['nettle','Nettle Whip','leaf','poison',7,2.3,'whip','Burred leaves snag on the return stroke.'],
    ['clover','Clover Brooch','leaf','mend',5,3.05,'brooch','A four-leaf charm beaded with dew.'],
    ['acorn','Acorn Cup','leaf','guard',6,2.65,'cup','Its rim bears the rings of the oldest oak.'],
    ['boomerang','Maple Boomerang','leaf','rapid',6,1.6,'boomerang','A forked maple wing sails back to the hand.'],
    ['marble','Sulfur Marble','fire','burn',8,2.15,'orb','A touch wakes a sulfur glow trapped inside.'],
    ['tongs','Kiln Tongs','fire','strike',13,2.55,'tongs','Ember-red jaws snap shut with a satisfying clang.'],
    ['matchbox','Salamander Matches','fire','rapid',5,1.25,'box','Each tiny match spits a jumping spark.'],
    ['oil','Pitch Ampoule','fire','burn',9,2.5,'vial','Black pine resin clings to whatever it strikes.'],
    ['blade','Copper Sickle','fire','pierce',11,2.3,'sickle','The blacksmith left a curl of heat along the blade.'],
    ['mirror','Glacier Mirror','frost','guard',7,2.85,'mirror','Snowflakes glint along its silver rim.'],
    ['skate','Ice Runner','frost','rapid',6,1.5,'skate','A curved steel skate slides silently over stone.'],
    ['flute','Floe Flute','frost','chill',8,2.3,'flute','Each note sounds like river ice cracking.'],
    ['snowball','Packed Snowball','frost','strike',10,1.95,'orb','A dense ball of snow wrapped around a pebble.'],
    ['awl','Walrus Awl','frost','pierce',13,2.6,'awl','This ivory punch can pierce thick frozen hides.'],
    ['fork','Tuning Fork','storm','shock',8,2.35,'fork','Its twin prongs sing just before lightning strikes.'],
    ['sail','Pocket Windsock','storm','rapid',5,1.35,'windsock','A striped streamer twists in invisible gusts.'],
    ['vane','Brass Weathervane','storm','pierce',11,2.15,'arrow','Its arrow always points into danger.'],
    ['crank','Dynamo Crank','storm','shock',9,2.65,'crank','Give it one turn and the teeth spark blue.'],
    ['washer','Copper Washer','storm','guard',6,2.5,'ring','A slim copper band holds a halo of charge.'],
    ['mallet','Quarry Mallet','stone','strike',15,2.95,'hammer','A stubby sledge shaped to split slate.'],
    ['caliper','Flint Caliper','stone','pierce',10,2.1,'caliper','Two sharpened jaws find gaps in armor.'],
    ['quoit','Granite Quoit','stone','rapid',6,1.55,'ring','A smooth throwing ring weighted on one side.'],
    ['mortar','Granite Mortar','stone','guard',8,3.05,'mortar','Its thick bowl fits snugly over a paw.'],
    ['tile','Engraved Keystone','stone','strike',17,3.35,'tablet','A heavy wedge from the oldest arch on the trail.'],
    ['brush','Inkspirit Brush','spirit','drain',8,2.7,'brush','Its thirsty bristles trail a ribbon of purple ink.'],
    ['seal','Dream Seal','spirit','chill',7,2.1,'seal','Press it into the air to hush a restless creature.'],
    ['hourglass','Dusk Hourglass','spirit','chill',9,2.55,'hourglass','Twilight sand drifts upward through the waist.'],
    ['ribbon','Mourning Ribbon','spirit','mend',5,2.9,'ribbon','A soft silk tie that remembers every traveler.'],
    ['crown','Half-Moon Diadem','spirit','guard',6,2.65,'crown','A thread of moonlight stretches between its horns.']
  );
  basics.push(
    ['balm_censer','Balmwood Censer','leaf','mend',7,2.85,'censer','Pip swings a hanging bowl of balmwood resin, breathing in the green smoke.'],
    ['leech_harpoon','Leechvine Harpoon','leaf','drain',11,2.65,'harpoon','A hollow barb carries sap back down its braided tether.'],
    ['ember_glaive','Ember Glaive','fire','burn',12,2.8,'glaive','The broad blade leaves a hot line where its edge passes.'],
    ['cautery_saber','Cautery Saber','fire','mend',9,3.3,'saber','Heat travels from the edge to a healing sigil on the guard.'],
    ['rime_halberd','Rime Halberd','frost','chill',11,2.6,'halberd','Rime grows along the crescent axe and breaks loose when it lands.'],
    ['glacier_sai','Glacier Sai','frost','guard',8,2.55,'sai','Its forked tines catch a blow in a sleeve of ice.'],
    ['arc_chakram','Arc Chakram','storm','shock',9,2.15,'chakram','A split in this throwing ring flashes as it reaches its target.'],
    ['squall_repeater','Squall Repeater','storm','rapid',6,1.3,'repeater','A reed magazine feeds pairs of bolts to a spring-loaded bow.'],
    ['runic_pavise','Runestone Pavise','stone','guard',10,3.15,'pavise','Pip rams with the shield boss, waking the runes along its rim.'],
    ['fault_flail','Faultline Flail','stone','strike',15,2.65,'flail','A star of knapped flint swings from three heavy links.'],
    ['soul_scythe','Soulhook Scythe','spirit','drain',13,3.0,'scythe','A crescent of pale light draws strength from each cut.'],
    ['dawn_harpbow','Dawn Harpbow','spirit','mend',6,2.45,'harpbow','One string looses the arrow while the others ring a healing chord.']
  );
  // Earlier relic IDs keep their stats and recipes. Rarity and home region are
  // independent: a regional find can turn up elsewhere once its drop unlocks.
  const rareOrigins={clover:0,fang:1,bell:2,coil:3,pick:4,wick:5,brush:6,crown:7,
    balm_censer:0,ember_glaive:1,rime_halberd:2,arc_chakram:3,fault_flail:4,soul_scythe:5,dawn_harpbow:6,runic_pavise:7};
  basics.forEach(row=>{
    const region=rareOrigins[row[0]];
    item(...row,null,region===undefined?'common':'rare',1,region??null);
  });
  const regionalFinds = [
    [8,'barnacle_buckler','Barnacle Buckler','stone','guard',12,2.45,'limpet','rare',33,'Seawater pools beneath its scalloped rim.'],
    [8,'kingtide_conch','Kingtide Conch','storm','shock',16,2.4,'conch','mythic',4,'One low note from the shell stills the tide.'],
    [9,'furnace_piston','Furnace Piston','fire','strike',13,2.25,'piston','common',1,'Soot-black bellows drive its brass striking head.'],
    [9,'welders_gauntlet','Welder’s Gauntlet','fire','guard',15,2.65,'gauntlet','rare',37,'The finger plates glow along their riveted seams.'],
    [10,'puffball_pipe','Puffball Pipe','leaf','poison',10,1.95,'puffer','rare',41,'A puff of breath launches a cloud of biting spores.'],
    [10,'mycelium_crook','Mother-Root Crook','leaf','drain',16,2.3,'crook','mythic',9,'Pale feeding threads run the length of the handle.'],
    [11,'saltglass_lens','Saltglass Lens','frost','pierce',15,2.2,'prism','rare',7,'Its salt facets focus a needle of cold light.'],
    [11,'mirage_sash','Mirage Sash','spirit','chill',12,2.15,'scarf','rare',45,'Even in hot air, the silver fringe casts a wintry shadow.'],
    [12,'cloud_gong','Cloudbell Gong','storm','shock',14,2.6,'gong','rare',8,'The hammered bronze face trembles before the storm.'],
    [12,'monsoon_parasol','Monsoon Parasol','storm','guard',17,2.4,'parasol','mythic',49,'Cloud paper unfurls over a frame of lightning rods.'],
    [13,'amber_cleaver','Amber Cleaver','stone','pierce',12,2.1,'cleaver','common',6,'A fossilized scale serves as its serrated edge.'],
    [13,'ribcage_aegis','Ribcage Aegis','stone','guard',16,2.7,'ribcage','rare',53,'Arched ribs lock together around a vertebra boss.'],
    [14,'lurelight_rod','Lurelight Rod','spirit','drain',13,2.3,'lure','rare',10,'The tiny lantern on its tip feeds on wandering light.'],
    [14,'medusa_net','Medusa Net','leaf','poison',15,2.05,'net','mythic',57,'Silvery strands still carry the sting of the deep.'],
    [15,'roseglass_rapier','Roseglass Rapier','leaf','rapid',9,1.35,'rapier','rare',12,'A stained-glass rose opens around the narrow guard.'],
    [15,'briar_veil','Briar Veil','spirit','mend',12,2.4,'veil','rare',61,'Each pale bead holds one drop of healing dew.'],
    [16,'rime_anchor','Rime Anchor','frost','chill',15,2.4,'anchor','rare',16,'Green ice clings to its curved iron flukes.'],
    [16,'boreal_helm','Boreal Helm','frost','guard',18,2.55,'helm','mythic',65,'Northern lights run between its twin ice horns.'],
    [17,'cuckoo_bolt','Cuckoo Bolt','storm','rapid',6,1.25,'birdbolt','common',10,'A weighted bird’s beak clicks into the launcher.'],
    [17,'pendulum_knife','Pendulum Knife','storm','pierce',17,2.2,'pendulum','rare',69,'A single sharpened weight swings from its chain.'],
    [18,'starfire_drill','Starfire Drill','fire','pierce',17,2.55,'drill','rare',17,'Orange sparks jump between the spirals of the bit.'],
    [18,'gravity_maul','Gravity Maul','stone','strike',29,2.65,'gravitymaul','mythic',73,'Three hovering stones form a head heavier than lead.'],
    [19,'sunspun_baton','Sunspun Baton','fire','burn',14,2.25,'scepter','rare',19,'Flame flickers through its open sunburst finial.'],
    [19,'phoenix_diadem','Phoenix Diadem','spirit','mend',16,2.35,'suncrest','mythic',77,'A crown of living dawn warms the brow of its bearer.']
  ];
  for(const [region,id,name,element,style,power,cd,icon,rarity,unlock,flavor] of regionalFinds){
    item(id,name,element,style,power,cd,icon,flavor,null,rarity,unlock,region);
  }
  // The far road adds hand-matched weapons with signature mechanics. A special
  // augments the usual elemental/style effect instead of replacing it.
  const specialText={
    ricochet:'The strike rebounds into another foe.',execute:'Deals extra damage to badly wounded foes.',
    rupture:'Detonates an afflicted target in an elemental burst.',chain:'Lightning jumps between nearby foes.',
    leech:'Returns part of the damage dealt as health.',wardbreak:'Breaks protective wards before the strike lands.',
    bloom:'Spreads venomous growth through the enemy group.',permafrost:'Briefly freezes the target and leaves it deeply chilled.',
    aftershock:'A delayed impact follows the original strike.',soulmark:'Marks the target for stronger follow-up attacks.',
    renewal:'Restores health and raises a protective shield.',overload:'Every third activation releases stored charge in a group-hitting discharge.'
  };
  const counterText={
    cleanse:'Its activations clear venom and all lingering afflictions.',
    antivenom:'Reduces venom damage and helps clear poison.',
    insulated:'Resists enemy shocks and preserves relic tempo.',
    thaw:'Prevents freezing and helps extinguish scorching.',
    unbinding:'Cuts entangling growth and frees slowed relics.',
    purify:'Prevents hostile hexes and corrosive afflictions.',
    blastguard:'Softens scorching attacks and explosive bursts.',
    wardbreaker:'Strikes bypass armor and break protective wards.'
  };
  const farFinds=[
    [20,'canal_trident','Canalkeeper Trident','storm','shock',17,2.45,'trident','rare','chain','Three copper tines draw a current from the aqueduct.'],
    [20,'patina_disc','Patina Discbuckler','stone','guard',14,2.5,'aegis','rare','ricochet','Canal coins are hammered into the rim of this returning shield.'],
    [21,'glassfire_cleaver','Glassfire Cleaver','fire','burn',20,2.6,'cleavers','rare','rupture','A black-glass edge catches embers in its hairline cracks.'],
    [21,'magma_mantle','Magma Mantle','stone','guard',16,2.9,'aegis','rare','aftershock','A moving seam of lava runs through its basalt plates.'],
    [22,'hushwood_bow','Hushwood Longbow','frost','chill',15,2.3,'thornbow','rare','permafrost','Its white branches bend without disturbing the falling snow.'],
    [22,'lantern_apple','Lantern Apple','leaf','mend',10,2.8,'orbiter','rare','renewal','A warm blossom grows inside the clear skin of this winter fruit.'],
    [23,'stormtotem_lance','Stormtotem Lance','storm','shock',19,2.55,'lance','rare','chain','A carved thunderhead crowns the long copper point.'],
    [23,'liongrass_sabre','Liongrass Saber','leaf','rapid',11,1.5,'saber','common','execute','Its serrated green blade whistles above the golden grass.'],
    [24,'bonekey_greatsword','Bonekey Greatsword','stone','pierce',24,2.9,'greatsword','rare','wardbreak','The teeth of an ancient giant form a key along its edge.'],
    [24,'obsidian_missal','Obsidian Missal','spirit','drain',16,2.6,'tome','rare','soulmark','Black pages name every guardian buried in the mountain.'],
    [25,'dreamtide_crook','Dreamtide Crook','spirit','drain',17,2.45,'wand','rare','soulmark','A small floating moon is tethered to its crook by a silver thread.'],
    [25,'marshlight_crossbow','Marshlight Crossbow','leaf','poison',14,2.1,'crossbow','rare','bloom','Its bolts unfold into luminous marsh flowers.'],
    [26,'honeycomb_cannon','Honeycomb Cannon','fire','rapid',12,1.65,'cannon','rare','overload','Six wax-sealed chambers pop open in a fragrant flash.'],
    [26,'amberwax_aegis','Amberwax Aegis','leaf','guard',18,2.85,'aegis','rare','renewal','Honey-colored resin closes over every scratch in its face.'],
    [27,'railspike_lance','Railspike Lance','stone','pierce',22,2.45,'lance','common','ricochet','A polished railspike sits in a socket of spring-loaded iron.'],
    [27,'signal_chainblade','Signal Chainblade','fire','burn',18,2.25,'chainblade','rare','execute','Red signal glass gleams between the teeth of its moving chain.'],
    [28,'silvervine_wand','Silvervine Wand','frost','chill',17,2.2,'wand','rare','permafrost','A frozen vine makes new blossoms whenever its tip strikes.'],
    [28,'glasswing_orbiter','Glasswing Orbiter','spirit','rapid',12,1.5,'orbiter','rare','ricochet','A ring of translucent wings circles a bead of captive moonlight.'],
    [29,'voltaic_crossbow','Voltaic Crossbow','storm','rapid',12,1.45,'crossbow','rare','overload','A pair of tuning forks charge the rails before every shot.'],
    [29,'coppercoil_aegis','Coppercoil Aegis','storm','guard',19,2.65,'aegis','rare','chain','Lightning that strikes the face travels around its spiral rim.'],
    [30,'titanroot_scythe','Titanroot Scythe','leaf','poison',19,2.35,'sicklestaff','rare','bloom','A canyon-root curves into a living blade taller than its bearer.'],
    [30,'canyonfang_greatsword','Canyonfang Greatsword','stone','strike',28,3.05,'greatsword','rare','aftershock','Its sandstone edge carries the weight of a fallen cliff.'],
    [31,'mothmoon_tome','Mothmoon Score','spirit','drain',18,2.45,'tome','rare','soulmark','Silent notes lift from the page like pale moths.'],
    [31,'velvet_crescent','Velvet Crescent','frost','rapid',12,1.4,'chainblade','rare','execute','A crescent of cold stage light swings from a velvet ribbon.'],
    [32,'bloodbloom_thornbow','Bloodbloom Thornbow','leaf','poison',18,2.15,'thornbow','rare','bloom','Scarlet sap feeds every thorn along the bowstring.'],
    [32,'redmist_cleavers','Redmist Cleavers','fire','drain',21,2.55,'cleavers','rare','leech','Twin blades drink the basin mist and glow along their spines.'],
    [33,'aurora_orbiter','Aurora Orbiter','frost','rapid',13,1.45,'orbiter','rare','ricochet','Splinters of the northern lights orbit a core of black ice.'],
    [33,'fracture_lance','Fracture Lance','storm','pierce',24,2.45,'lance','rare','wardbreak','Its forked tip fits exactly into the cracks between worlds.'],
    [34,'dawnstar_greatsword','Dawnstar Greatsword','fire','pierce',29,2.8,'greatsword','mythic','execute','A line of dawn rests in the fuller of this ivory blade.'],
    [34,'orrery_wand','Orrery Wand','spirit','mend',16,2.65,'wand','mythic','renewal','Tiny sun-and-moon rings turn around its warm crystal heart.']
  ];
  const signatures={};
  for(const [region,id,name,el,style,power,cd,icon,rarity,special,flavor] of farFinds){
    item(id,name,el,style,power,cd,icon,flavor,null,rarity,region*4+1,region);
    signatures[id]=special;
  }
  // Counters are available from the first circuit as well as from the new
  // regions, so an imported traveler can answer enemy afflictions immediately.
  const counterFinds=[
    [0,'dewglass_vial','Dewglass Vial','leaf','mend',5,3.15,'vial',1,'cleanse','renewal','Clean dew gathers on the inside of a corked leaf-glass bottle.'],
    [0,'serpentroot_knife','Serpentroot Knife','leaf','poison',7,2.3,'saber',1,'antivenom','bloom','A pale root coils around a blade used by the grove healers.'],
    [1,'ashfelt_mantle','Ashfelt Mantle','stone','guard',7,3.1,'aegis',5,'blastguard','aftershock','Dense felt and overlapping shale turn aside cinders.'],
    [1,'coalheart_warmer','Coalheart Warmer','fire','mend',5,3.05,'censer',1,'thaw','renewal','A coal glows safely inside a little brass cage.'],
    [2,'thawthorn_rapier','Thawthorn Rapier','fire','pierce',10,2.5,'rapier',9,'thaw','execute','Warm sap runs down a copper thorn forged into a narrow sword.'],
    [2,'frostmoss_wrap','Frostmoss Wrap','leaf','guard',6,2.95,'scarf',5,'antivenom','renewal','Cool moss draws the sting from the traveler’s skin.'],
    [3,'grounding_spear','Grounding Spear','stone','pierce',11,2.65,'lance',1,'insulated','wardbreak','A trailing copper braid guides stray lightning into the earth.'],
    [3,'stormcork_charm','Stormcork Charm','storm','guard',6,2.85,'brooch',13,'insulated','chain','A ring of dry cork keeps a captured spark from touching the hand.'],
    [4,'trailcut_sickle','Trailcut Sickle','leaf','rapid',6,1.7,'sickle',1,'unbinding','execute','This hooked edge was made to free a trail from creeping vines.'],
    [4,'bastion_chisel','Bastion Chisel','stone','pierce',12,2.75,'awl',13,'wardbreaker','wardbreak','A mason’s chisel finds the weak seam in any shield.'],
    [5,'pilgrim_saltbell','Pilgrim’s Saltbell','spirit','mend',6,3.1,'bell',1,'purify','soulmark','Every quiet chime scatters a pinch of blessed silver salt.'],
    [5,'rainwash_fan','Rainwash Fan','frost','chill',8,2.55,'fan',9,'cleanse','permafrost','A sweep of the fan calls clean rain from the empty air.']
  ];
  for(const [region,id,name,el,style,power,cd,icon,unlock,counter,special,flavor] of counterFinds){
    item(id,name,el,style,power,cd,icon,flavor,null,'common',unlock,region);
    Object.assign(items[id],{counter,counterDescription:counterText[counter]});signatures[id]=special;
  }
  // Dedicated support relics spend every activation on a single purpose. They
  // carry no damaging signatures; even their milestone echoes remain support.
  const supportFinds=[
    ['dewdrop_chalice','Dewdrop Chalice','leaf','heal',8,2.8,'cup','Morning dew beads along the lip of a living flower.'],
    ['hearth_tea','Hearth Tea','fire','heal',9,3.1,'gourd','A little earthen pot keeps its soothing tea warm.'],
    ['snowmelt_phial','Snowmelt Phial','frost','heal',7,2.5,'vial','The first clear water of spring rests in this frosted bottle.'],
    ['rain_pearl','Rain Pearl','storm','heal',8,2.7,'orb','Gentle rain circles the pearl without falling away.'],
    ['mineral_salve','Mineral Salve','stone','heal',11,3.4,'mortar','A pale clay poultice closes small wounds.'],
    ['kindred_candle','Kindred Candle','spirit','heal',9,2.95,'lantern','Its warm flame remembers every friend along the road.'],
    ['barkward_charm','Barkward Charm','leaf','shield',10,3,'brooch','Layers of soft bark open into a protective shell.'],
    ['ember_screen','Ember Screen','fire','shield',11,3.2,'fan','A curtain of harmless embers turns incoming blows.'],
    ['iceward_mirror','Iceward Mirror','frost','shield',9,2.7,'mirror','The face reflects a quiet wall of blue ice.'],
    ['static_canopy','Static Canopy','storm','shield',8,2.5,'kite','A small cloud spreads a charged shelter overhead.'],
    ['granite_ward','Granite Ward','stone','shield',13,3.45,'shield','Six smooth stones lock together into a floating wall.'],
    ['moonveil_spool','Moonveil Spool','spirit','shield',10,2.9,'ribbon','Silver thread weaves a soft mantle around its bearer.'],
    ['snaproot_totem','Snaproot Totem','leaf','stun',8,3.2,'idol','A tap of the root stills a foe without drawing blood.'],
    ['flashpowder_pouch','Flashpowder Pouch','fire','stun',9,3.4,'box','A bright, soundless flash steals a moment of attention.'],
    ['hushbell','Hushbell','frost','stun',7,3,'bell','Its note leaves the air too still for a hostile strike.'],
    ['thunderclap_cymbal','Thunderclap Cymbal','storm','stun',10,3.3,'gong','One clean clap arrests an enemy in mid-motion.'],
    ['stillstone_seal','Stillstone Seal','stone','stun',11,3.6,'seal','The old mountain sigil commands a moment of stillness.'],
    ['dreamcatcher','Dreamcatcher','spirit','stun',8,3.1,'net','Gentle dream threads hold a restless mind for a heartbeat.'],
    ['winterseed','Winterseed','leaf','freeze',8,3.7,'seed','A sleeping winter waits inside the silver seed coat.'],
    ['coldflame_lantern','Coldflame Lantern','fire','freeze',9,3.9,'lantern','The pale flame draws warmth away without scorching.'],
    ['glacier_hourglass','Glacier Hourglass','frost','freeze',10,3.8,'hourglass','Every falling crystal holds a second of frozen time.'],
    ['sleetwind_chime','Sleetwind Chime','storm','freeze',8,3.5,'bell','Its clear note wraps a foe in motionless sleet.'],
    ['frostsalt_geode','Frostsalt Geode','stone','freeze',12,4.1,'rock','A hollow stone grows a new lining of bitter frost.'],
    ['pale_moon_orb','Pale Moon Orb','spirit','freeze',9,3.6,'orb','A winter moon leaves its reflection frozen in the air.']
  ];
  for(const [id,name,element,style,power,cd,icon,flavor] of supportFinds){
    item(id,name,element,style,power,cd,icon,flavor,null,'common',1,Object.keys(elements).indexOf(element));
  }
  // Recipes are hand matched; order of ingredients is irrelevant.
  const recipes = [
    ['briar','Briar Scepter','stick','thorn','leaf','poison',14,2.1,'staff'],
    ['sprinkler','Bloom Sprinkler','seed','gourd','leaf','mend',11,2.8,'gourd'],
    ['firebrand','Firebrand','stick','torch','fire','burn',16,2.1,'staff'],
    ['wasp','Cinderwasp','thorn','pepper','fire','poison',15,1.95,'dart'],
    ['popper','Popcorn Mortar','seed','coal','fire','rapid',12,1.55,'seed'],
    ['tea','Kettle Gourd','gourd','torch','fire','mend',11,2.65,'gourd'],
    ['icebloom','Icebloom Wand','stick','shard','frost','chill',15,2.15,'staff'],
    ['frostbite','Frostbite Quill','thorn','needle','frost','poison',16,2.05,'needle'],
    ['blizzard','Blizzard Pod','seed','flake','frost','rapid',11,1.35,'seed'],
    ['chime','Winter Chimes','bell','bead','frost','mend',11,2.7,'bell'],
    ['rod','Stormcatcher','stick','coil','storm','shock',16,2.15,'staff'],
    ['quill','Thunder Quill','thorn','feather','storm','pierce',14,1.7,'feather'],
    ['rain','Rainmaker','gourd','drum','storm','mend',12,2.9,'drum'],
    ['pinwheel','Seed Pinwheel','seed','kite','leaf','rapid',12,1.45,'star'],
    ['catapult','Root Catapult','stick','sling','leaf','strike',18,1.95,'sling'],
    ['buckler','Bramble Buckler','thorn','shell','leaf','guard',14,2.55,'shield'],
    ['fossil','Fossil Seed','seed','idol','stone','strike',24,2.85,'idol'],
    ['well','Stone Wellflask','gourd','pick','stone','mend',11,2.8,'gourd'],
    ['pilgrim','Pilgrim’s Crook','stick','bead','spirit','mend',12,2.85,'staff'],
    ['ghostthorn','Ghost Thorn','thorn','wick','spirit','drain',14,2.4,'dart'],
    ['lullaby','Lullaby Rattle','seed','mask','spirit','chill',15,2.15,'beads'],
    ['mooncup','Mooncup','gourd','fan','spirit','mend',12,2.7,'gourd'],
    ['inferno','Kiln Brazier','coal','torch','fire','burn',19,2.4,'lantern'],
    ['salamander','Salamander Tooth','pepper','fang','fire','pierce',18,1.95,'fang'],
    ['obsidian','Obsidian Splinter','coal','shard','stone','pierce',21,2.15,'shard'],
    ['steam','Steam Whistle','torch','bell','frost','shock',16,2.35,'bell'],
    ['aurora','Aurora Star','pepper','flake','frost','rapid',12,1.25,'star'],
    ['brand','Comet Needle','fang','needle','fire','pierce',23,2.25,'needle'],
    ['plasma','Plasma Coil','coal','coil','storm','burn',18,2.25,'coil'],
    ['phoenix','Phoenix Plume','torch','feather','fire','rapid',13,1.45,'feather'],
    ['cracker','Cracker Drum','pepper','drum','fire','strike',24,2.65,'drum'],
    ['sunray','Sunray Kite','fang','kite','storm','guard',15,2.4,'kite'],
    ['magma','Magma Sling','coal','sling','fire','burn',17,1.95,'sling'],
    ['forge','Forge Shield','torch','shell','fire','guard',15,2.6,'shield'],
    ['emberpick','Ember Pick','fang','pick','fire','pierce',22,2.2,'pick'],
    ['wildfire','Wildfire Idol','pepper','idol','fire','strike',26,2.85,'idol'],
    ['candle','Wake Candle','coal','wick','spirit','drain',18,2.55,'lantern'],
    ['ashes','Ash Prayer','torch','bead','spirit','mend',13,2.75,'beads'],
    ['oni','Oni Visor','fang','mask','fire','drain',19,2.7,'mask'],
    ['firefan','Firefly Fan','pepper','fan','fire','rapid',12,1.3,'fan'],
    ['glacier','Glacier Bell','shard','bell','frost','guard',15,2.65,'bell'],
    ['hail','Hail Spindle','flake','needle','frost','pierce',18,1.95,'needle'],
    ['thundersnow','Thundersnow Rod','shard','coil','storm','chill',18,2.3,'shard'],
    ['snowbird','Snowbird Plume','flake','feather','frost','rapid',12,1.2,'feather'],
    ['tremor','Tremor Drum','drum','idol','stone','strike',27,2.95,'drum'],
    ['lodestone','Lodestone Plate','coil','shell','storm','guard',16,2.55,'shield'],
    ['gargoyle','Gargoyle Mask','shell','mask','stone','drain',18,2.7,'mask'],
    ['witchlight','Witchlight Kite','kite','wick','spirit','drain',16,2.25,'kite'],
    ['monsoon','Monsoon Fan','fan','feather','storm','rapid',13,1.35,'fan'],
    ['echo','Echo Sling','sling','bell','frost','chill',16,1.95,'sling'],
    ['reaper','Grave Pick','pick','wick','spirit','drain',18,2.55,'pick'],
    ['runes','Rune Beads','bead','idol','stone','guard',15,2.7,'beads'],
    ['worldtree','Worldtree Staff','briar','icebloom','leaf','poison',28,1.95,'staff'],
    ['volcano','Volcano Heart','inferno','tremor','fire','burn',36,2.5,'rock'],
    ['daybreak','Daybreak Mantle','phoenix','sunray','storm','guard',26,2.2,'kite'],
    ['cometfall','Cometfall','brand','aurora','frost','rapid',23,1.3,'star'],
    ['nightjar','Lantern of Lost Stars','candle','witchlight','spirit','drain',31,2.4,'lantern'],
    ['titanshell','Titan Carapace','buckler','forge','stone','guard',27,2.25,'shield'],
    ['tempest','Tempest Crown','thundersnow','plasma','storm','shock',32,2.05,'coil'],
    ['ambrosia','Ambrosia Flask','mooncup','tea','leaf','mend',23,2.55,'gourd'],
    ['suntalon','Eclipse Talon','oni','salamander','fire','pierce',36,1.95,'fang'],
    ['oracle','Oracle’s Weatherglass','rain','glacier','frost','chill',31,2.05,'bell']
  ];
  recipes.push(
    ['hedgebow','Hedgehunter Bow','bamboo','thorn','leaf','poison',16,1.95,'bow'],
    ['lash','Smoldervine Lash','nettle','coal','fire','burn',17,2.15,'whip'],
    ['fourwinds','Four Winds Brooch','clover','feather','storm','mend',11,2.6,'brooch'],
    ['oakheart','Oakheart Grail','acorn','shell','leaf','guard',14,2.3,'cup'],
    ['snowmaple','Snowmaple Returner','boomerang','flake','frost','rapid',12,1.3,'boomerang'],
    ['helios','Captured Sun','marble','wick','fire','drain',16,2.2,'orb'],
    ['crucible','Crucible Grips','tongs','idol','stone','strike',24,2.6,'tongs'],
    ['swarmbox','Spark Beetle Box','matchbox','seed','fire','rapid',11,1.25,'box'],
    ['flamewash','Witchfire Elixir','oil','mask','spirit','burn',17,2.25,'vial'],
    ['harvest','Harvest Crescent','blade','gourd','leaf','drain',17,2.35,'sickle'],
    ['lens','Prismatic Mirror','mirror','coil','storm','guard',14,2.35,'mirror'],
    ['cometskates','Comet Skates','skate','pepper','fire','rapid',12,1.3,'skate'],
    ['lullabyflute','Lullwater Flute','flute','fan','spirit','chill',16,2.0,'flute'],
    ['hailshot','Hailshot','snowball','sling','frost','strike',21,2.05,'orb'],
    ['suture','Polar Suture','awl','bead','frost','mend',13,2.6,'awl'],
    ['resonator','Seismic Tuning Fork','fork','pick','stone','shock',17,2.2,'fork'],
    ['rainsleeve','Cloudburst Sleeve','sail','shard','frost','chill',15,1.9,'windsock'],
    ['northstar','Northstar Arrow','vane','needle','storm','pierce',19,1.95,'arrow'],
    ['stormengine','Tempest Dynamo','crank','drum','storm','shock',18,2.3,'crank'],
    ['haloring','Aegis Halo','washer','bell','storm','guard',14,2.3,'ring'],
    ['meteormaul','Meteor Maul','mallet','torch','fire','strike',24,2.6,'hammer'],
    ['gearshear','Gear Shears','caliper','fang','stone','pierce',19,1.95,'caliper'],
    ['vaultwheel','Vaultwheel','quoit','kite','storm','rapid',12,1.35,'ring'],
    ['hearthbowl','Hearth Bowl','mortar','coal','stone','guard',17,2.65,'mortar'],
    ['sunstone','Sun-Gate Keystone','tile','torch','fire','burn',21,2.65,'tablet'],
    ['nightstroke','Midnight Inkstone','brush','pick','spirit','drain',17,2.25,'brush'],
    ['frostseal','Polar Signet','seal','bell','frost','chill',16,2.0,'seal'],
    ['deadreckon','Reckoning Glass','hourglass','mask','spirit','drain',18,2.45,'hourglass'],
    ['windknot','Windknot Sash','ribbon','feather','storm','mend',11,2.55,'ribbon'],
    ['moonrise','Moonrise Coronet','crown','shard','frost','guard',15,2.35,'crown']
  );
  recipes.push(
    ['seraph_harp','Seraph’s Harp','balm_censer','dawn_harpbow','spirit','mend',14,2.6,'harpbow'],
    ['bloodfire_trident','Bloodfire Trident','leech_harpoon','ember_glaive','fire','drain',21,2.4,'trident'],
    ['tempered_twins','Tempered Twinblades','cautery_saber','glacier_sai','frost','guard',18,2.5,'twinblades'],
    ['aurora_crescent','Aurora Crescent','rime_halberd','arc_chakram','storm','shock',19,2.15,'chakram'],
    ['bastion_ballista','Bastion Ballista','squall_repeater','runic_pavise','stone','pierce',24,2.35,'ballista'],
    ['gravemoon_reaper','Gravemoon Reaper','fault_flail','soul_scythe','spirit','drain',23,2.75,'chainscythe']
  );
  recipes.push(
    ['undertow_bastion','Undertow Bastion','barnacle_buckler','kingtide_conch','storm','guard',25,2.25,'limpet'],
    ['boiler_knuckle','Boiler Knuckle','furnace_piston','welders_gauntlet','fire','burn',21,2.2,'gauntlet'],
    ['pilgrim_of_spores','Pilgrim’s Sporestaff','puffball_pipe','mycelium_crook','leaf','poison',22,1.85,'crook'],
    ['mirrordune','Mirrordune Saber','saltglass_lens','mirage_sash','frost','pierce',25,2.05,'prism'],
    ['thunder_pagoda','Thunder Pagoda','cloud_gong','monsoon_parasol','storm','shock',26,2.3,'parasol'],
    ['saurian_jaw','Saurian Jaw','amber_cleaver','ribcage_aegis','stone','pierce',25,2.2,'cleaver'],
    ['abyssal_lantern','Abyssal Lantern','lurelight_rod','medusa_net','spirit','drain',24,2.15,'lure'],
    ['damask_foil','Damask Foil','roseglass_rapier','briar_veil','spirit','mend',19,2.1,'rapier'],
    ['winter_figurehead','Winter Figurehead','rime_anchor','boreal_helm','frost','guard',26,2.25,'helm'],
    ['seconds_hand','Second’s Hand','cuckoo_bolt','pendulum_knife','storm','rapid',14,1.25,'pendulum'],
    ['impact_core','Impact Core','starfire_drill','gravity_maul','fire','strike',37,2.55,'drill'],
    ['sunroot_crown','Sunroot Crown','sunspun_baton','phoenix_diadem','spirit','mend',23,2.1,'suncrest'],
    ['coral_recurve','Coral Recurve','barnacle_buckler','bamboo','leaf','pierce',19,2.1,'bow'],
    ['furnace_horn','Furnace Horn','furnace_piston','kingtide_conch','fire','burn',23,2.1,'conch'],
    ['silkspore_fan','Silkspore Fan','puffball_pipe','fan','leaf','poison',17,1.8,'fan'],
    ['sunblind_lens','Sunblind Lens','saltglass_lens','sunspun_baton','fire','shock',19,2.15,'prism'],
    ['vesper_bell','Vesper Bell','cloud_gong','bead','spirit','mend',15,2.45,'gong'],
    ['marrow_lance','Marrow Lance','amber_cleaver','leech_harpoon','stone','drain',20,2.1,'harpoon'],
    ['winter_lure','Winter Lure','lurelight_rod','rime_anchor','frost','chill',19,1.9,'anchor'],
    ['thornblade_waltz','Thornblade Waltz','damask_foil','briar','leaf','rapid',18,1.25,'rapier'],
    ['aurora_keel','Aurora Keel','winter_figurehead','echo','frost','chill',32,1.95,'anchor'],
    ['hour_of_owls','Hour of Owls','seconds_hand','monsoon','storm','rapid',18,1.2,'birdbolt'],
    ['crater_song','Crater Song','impact_core','tremor','stone','strike',44,2.55,'gravitymaul'],
    ['dawn_of_the_grove','Dawn of the Grove','sunroot_crown','ambrosia','leaf','mend',29,2.1,'scepter']
  );
  const farFusions=[
    ['verdigris_torrent','Verdigris Torrent','canal_trident','patina_disc','storm','shock',29,2.25,'trident','chain'],
    ['tidebreaker_lance','Tidebreaker Lance','canal_trident','needle','frost','pierce',28,2.3,'lance','wardbreak'],
    ['caldera_heart','Caldera Heart','glassfire_cleaver','magma_mantle','fire','burn',32,2.55,'cannon','overload'],
    ['ashglass_executioner','Ashglass Executioner','glassfire_cleaver','soul_scythe','spirit','drain',32,2.65,'greatsword','execute'],
    ['silent_solstice','Silent Solstice','hushwood_bow','lantern_apple','frost','chill',25,2.1,'thornbow','permafrost'],
    ['winter_bloom','Winter Bloom','lantern_apple','thorn','leaf','poison',23,2.15,'wand','bloom'],
    ['savanna_king','The Savanna King','stormtotem_lance','liongrass_sabre','storm','pierce',31,2.3,'lance','overload'],
    ['monsoon_repeater','Monsoon Repeater','stormtotem_lance','squall_repeater','storm','rapid',18,1.4,'crossbow','ricochet'],
    ['ossuary_sentence','Ossuary Sentence','bonekey_greatsword','obsidian_missal','spirit','drain',35,2.75,'greatsword','execute'],
    ['giantbreaker','Giantbreaker','bonekey_greatsword','gravity_maul','stone','strike',43,3.0,'gravitymaul','aftershock'],
    ['reverie_harvester','Reverie Harvester','dreamtide_crook','marshlight_crossbow','spirit','drain',29,2.35,'sicklestaff','leech'],
    ['drowned_moon','Drowned Moon','dreamtide_crook','mirror','frost','chill',27,2.2,'orbiter','permafrost'],
    ['cathedral_sun','Cathedral Sun','honeycomb_cannon','amberwax_aegis','fire','burn',31,2.35,'cannon','rupture'],
    ['queens_mercy','The Queen’s Mercy','amberwax_aegis','dawn_harpbow','spirit','mend',22,2.6,'harpbow','renewal'],
    ['cinder_express','Cinder Express','railspike_lance','signal_chainblade','fire','rapid',21,1.65,'crossbow','overload'],
    ['switchyard_hammer','Switchyard Hammer','railspike_lance','mallet','stone','strike',36,2.7,'hammer','aftershock'],
    ['winterglass_prism','Winterglass Prism','silvervine_wand','glasswing_orbiter','frost','pierce',30,2.15,'orbiter','wardbreak'],
    ['solstice_vine','Solstice Vine','silvervine_wand','gourd','leaf','mend',20,2.6,'wand','renewal'],
    ['stormglass_apparatus','Stormglass Apparatus','voltaic_crossbow','coppercoil_aegis','storm','shock',31,2.2,'cannon','chain'],
    ['faraday_rosary','Faraday Rosary','coppercoil_aegis','bead','spirit','guard',25,2.55,'beads','wardbreak'],
    ['worldroot_reaper','Worldroot Reaper','titanroot_scythe','canyonfang_greatsword','leaf','drain',36,2.65,'sicklestaff','leech'],
    ['faultline_colossus','Faultline Colossus','canyonfang_greatsword','fault_flail','stone','strike',44,2.9,'greatsword','rupture'],
    ['encore_eclipse','Encore Eclipse','mothmoon_tome','velvet_crescent','spirit','rapid',20,1.5,'chainblade','soulmark'],
    ['aria_of_thaw','Aria of Thaw','mothmoon_tome','flute','frost','chill',28,2.1,'tome','permafrost'],
    ['crimson_harvest','Crimson Harvest','bloodbloom_thornbow','redmist_cleavers','leaf','drain',35,2.4,'thornbow','renewal'],
    ['sanguine_comet','Sanguine Comet','redmist_cleavers','brand','fire','pierce',37,2.4,'cleavers','execute'],
    ['heavens_splitter','Heavensplitter','aurora_orbiter','fracture_lance','frost','pierce',36,2.2,'lance','permafrost'],
    ['prismstorm_cannon','Prismstorm Cannon','aurora_orbiter','coil','storm','shock',29,2.2,'cannon','overload'],
    ['firstlight_judgment','Firstlight Judgment','dawnstar_greatsword','orrery_wand','fire','burn',46,2.9,'greatsword','rupture'],
    ['atlas_of_returns','Atlas of Returns','orrery_wand','hourglass','spirit','drain',34,2.45,'tome','soulmark']
  ];
  for(const row of farFusions){recipes.push(row.slice(0,9));signatures[row[0]]=row[9];}
  recipes.push(
    ['blossom_grail','Blossom Grail','dewdrop_chalice','gourd','leaf','heal',16,2.65,'cup'],
    ['clover_fountain','Clover Fountain','dewdrop_chalice','clover','leaf','heal',17,2.6,'gourd'],
    ['sunbrew_kettle','Sunbrew Kettle','hearth_tea','torch','fire','heal',18,2.8,'gourd'],
    ['emberbalm_censer','Emberbalm Censer','hearth_tea','coalheart_warmer','fire','heal',18,2.75,'censer'],
    ['springwater_ampoule','Springwater Ampoule','snowmelt_phial','shard','frost','heal',16,2.3,'vial'],
    ['snowpetal_brooch','Snowpetal Brooch','snowmelt_phial','ribbon','frost','heal',17,2.4,'brooch'],
    ['cloudburst_chalice','Cloudburst Chalice','rain_pearl','gourd','storm','heal',17,2.5,'cup'],
    ['rainbow_rosary','Rainbow Rosary','rain_pearl','bead','storm','heal',18,2.55,'beads'],
    ['earthspring_bowl','Earthspring Bowl','mineral_salve','mortar','stone','heal',22,2.95,'mortar'],
    ['amberhoney_jar','Amberhoney Jar','mineral_salve','acorn','leaf','heal',20,2.8,'cup'],
    ['soulsalve_lamp','Soulsalve Lamp','kindred_candle','wick','spirit','heal',19,2.7,'lantern'],
    ['pilgrim_hymn','Pilgrim’s Hymn','kindred_candle','bead','spirit','heal',18,2.65,'harpbow'],
    ['oakbark_bastion','Oakbark Bastion','barkward_charm','shell','leaf','shield',21,2.8,'shield'],
    ['thornweave_mantle','Thornweave Mantle','barkward_charm','nettle','leaf','shield',20,2.7,'scarf'],
    ['cinder_sanctuary','Cinder Sanctuary','ember_screen','coal','fire','shield',23,2.95,'fan'],
    ['hearthguard_lantern','Hearthguard Lantern','ember_screen','torch','fire','shield',22,2.8,'lantern'],
    ['snowglass_barrier','Snowglass Barrier','iceward_mirror','bell','frost','shield',20,2.55,'mirror'],
    ['rimeplate_aegis','Rimeplate Aegis','iceward_mirror','glacier_sai','frost','shield',21,2.5,'aegis'],
    ['stormsilk_parasol','Stormsilk Parasol','static_canopy','sail','storm','shield',18,2.3,'parasol'],
    ['faraday_kite','Faraday Kite','static_canopy','washer','storm','shield',19,2.25,'kite'],
    ['mountainheart_wall','Mountainheart Wall','granite_ward','idol','stone','shield',27,3.1,'pavise'],
    ['runic_refuge','Runic Refuge','granite_ward','runic_pavise','stone','shield',26,2.95,'shield'],
    ['silverthread_veil','Silverthread Veil','moonveil_spool','crown','spirit','shield',21,2.65,'veil'],
    ['dreamwoven_canopy','Dreamwoven Canopy','moonveil_spool','mask','spirit','shield',22,2.7,'scarf'],
    ['rootlock_totem','Rootlock Totem','snaproot_totem','stick','leaf','stun',17,2.95,'idol'],
    ['pollen_hush','Pollen Hush','snaproot_totem','seed','leaf','stun',16,2.8,'puffer'],
    ['dayflash_prism','Dayflash Prism','flashpowder_pouch','marble','fire','stun',19,3.05,'prism'],
    ['sunblink_box','Sunblink Box','flashpowder_pouch','matchbox','fire','stun',18,2.9,'box'],
    ['silence_chimes','Silence Chimes','hushbell','flute','frost','stun',16,2.8,'bell'],
    ['stillwinter_gong','Stillwinter Gong','hushbell','snowball','frost','stun',18,2.9,'gong'],
    ['skylock_cymbal','Skylock Cymbal','thunderclap_cymbal','coil','storm','stun',21,3,'gong'],
    ['stormpause_fork','Stormpause Fork','thunderclap_cymbal','fork','storm','stun',20,2.95,'fork'],
    ['mountains_command','Mountain’s Command','stillstone_seal','tile','stone','stun',23,3.25,'tablet'],
    ['gravity_signet','Gravity Signet','stillstone_seal','mallet','stone','stun',22,3.1,'seal'],
    ['reverie_net','Reverie Net','dreamcatcher','fan','spirit','stun',17,2.85,'net'],
    ['slumber_score','Slumber Score','dreamcatcher','seal','spirit','stun',18,2.8,'tome'],
    ['frostbloom_seed','Frostbloom Seed','winterseed','thorn','leaf','freeze',17,3.4,'seed'],
    ['winterorchid_cup','Winterorchid Cup','winterseed','acorn','leaf','freeze',18,3.35,'cup'],
    ['blueflame_beacon','Blueflame Beacon','coldflame_lantern','torch','fire','freeze',19,3.55,'lantern'],
    ['coldstar_censer','Coldstar Censer','coldflame_lantern','coal','fire','freeze',20,3.6,'censer'],
    ['deepwinter_glass','Deepwinter Glass','glacier_hourglass','shard','frost','freeze',21,3.45,'hourglass'],
    ['stillfrost_mirror','Stillfrost Mirror','glacier_hourglass','mirror','frost','freeze',22,3.4,'mirror'],
    ['northwind_bells','Northwind Bells','sleetwind_chime','feather','storm','freeze',18,3.2,'bell'],
    ['hailcloud_gong','Hailcloud Gong','sleetwind_chime','drum','storm','freeze',21,3.25,'gong'],
    ['wintervault_geode','Wintervault Geode','frostsalt_geode','pick','stone','freeze',25,3.7,'rock'],
    ['glacierheart_idol','Glacierheart Idol','frostsalt_geode','idol','stone','freeze',26,3.65,'idol'],
    ['moonfrost_orbiter','Moonfrost Orbiter','pale_moon_orb','crown','spirit','freeze',19,3.25,'orbiter'],
    ['dreamice_lantern','Dreamice Lantern','pale_moon_orb','wick','spirit','freeze',20,3.3,'lantern'],
    // Second-stage fusions consume two crafted relics and keep their pure role.
    ['everbloom_fountain','Everbloom Fountain','blossom_grail','clover_fountain','leaf','heal',32,2.35,'cup'],
    ['sunheart_sanctum','Sunheart Sanctum','sunbrew_kettle','emberbalm_censer','fire','heal',35,2.5,'censer'],
    ['firstspring_chalice','Firstspring Chalice','springwater_ampoule','snowpetal_brooch','frost','heal',31,2.15,'cup'],
    ['monsoon_benediction','Monsoon Benediction','cloudburst_chalice','rainbow_rosary','storm','heal',34,2.25,'beads'],
    ['worldspring_cauldron','Worldspring Cauldron','earthspring_bowl','amberhoney_jar','stone','heal',40,2.6,'mortar'],
    ['kindred_constellation','Kindred Constellation','soulsalve_lamp','pilgrim_hymn','spirit','heal',36,2.4,'harpbow'],
    ['worldbark_citadel','Worldbark Citadel','oakbark_bastion','thornweave_mantle','leaf','shield',40,2.45,'pavise'],
    ['sunfire_shelter','Sunfire Shelter','cinder_sanctuary','hearthguard_lantern','fire','shield',44,2.5,'aegis'],
    ['glacial_sanctuary','Glacial Sanctuary','snowglass_barrier','rimeplate_aegis','frost','shield',40,2.3,'shield'],
    ['tempest_pavilion','Tempest Pavilion','stormsilk_parasol','faraday_kite','storm','shield',37,2.05,'parasol'],
    ['titanheart_bastion','Titanheart Bastion','mountainheart_wall','runic_refuge','stone','shield',50,2.7,'pavise'],
    ['moonlit_haven','Moonlit Haven','silverthread_veil','dreamwoven_canopy','spirit','shield',43,2.4,'veil'],
    ['heartwood_stillness','Heartwood Stillness','rootlock_totem','pollen_hush','leaf','stun',32,2.5,'idol'],
    ['noonday_hush','Noonday Hush','dayflash_prism','sunblink_box','fire','stun',35,2.6,'prism'],
    ['silent_solstice_bells','Silent Solstice Bells','silence_chimes','stillwinter_gong','frost','stun',34,2.5,'bell'],
    ['thunderous_silence','Thunderous Silence','skylock_cymbal','stormpause_fork','storm','stun',39,2.6,'gong'],
    ['mountains_verdict','Mountain’s Verdict','mountains_command','gravity_signet','stone','stun',43,2.8,'tablet'],
    ['dreamless_lullaby','Dreamless Lullaby','reverie_net','slumber_score','spirit','stun',34,2.5,'tome'],
    ['everwinter_garden','Everwinter Garden','frostbloom_seed','winterorchid_cup','leaf','freeze',34,3,'seed'],
    ['frozen_sun_beacon','Frozen Sun Beacon','blueflame_beacon','coldstar_censer','fire','freeze',38,3.15,'lantern'],
    ['eternal_winterglass','Eternal Winterglass','deepwinter_glass','stillfrost_mirror','frost','freeze',42,3,'hourglass'],
    ['eye_of_the_blizzard','Eye of the Blizzard','northwind_bells','hailcloud_gong','storm','freeze',39,2.9,'gong'],
    ['winterworld_heart','Winterworld Heart','wintervault_geode','glacierheart_idol','stone','freeze',49,3.25,'idol'],
    ['moon_of_still_tides','Moon of Still Tides','moonfrost_orbiter','dreamice_lantern','spirit','freeze',38,2.95,'orbiter']
  );
  // Cross-road recipes open new uses for old finds and lead into a third craft.
  // Every ingredient pair is unique; each generation follows its parents.
  const crossroadFusions=[
    // Twenty-four first-stage recipes use only uncrafted base relics.
    ['vinetongue_spear','Vinetongue Spear','bamboo','leech_harpoon','leaf','drain',22,2.25,'lance','leech'],
    ['cindersnap_repeater','Cindersnap Repeater','matchbox','squall_repeater','fire','rapid',14,1.3,'repeater','overload'],
    ['sootbloom_scythe','Sootbloom Scythe','nettle','ember_glaive','fire','poison',24,2.4,'sicklestaff','bloom'],
    ['rimewire_chakram','Rimewire Chakram','needle','arc_chakram','frost','shock',23,2.2,'chakram','permafrost'],
    ['galequarry_ballista','Galequarry Ballista','vane','caliper','storm','pierce',25,2.15,'ballista','ricochet'],
    ['moonflint_scythe','Moonflint Scythe','soul_scythe','pick','spirit','drain',27,2.55,'scythe','soulmark'],
    ['scarlet_comet_blade','Scarlet Comet Blade','blade','flake','fire','rapid',17,1.5,'saber','execute'],
    ['thunderbark_maul','Thunderbark Maul','mallet','crank','storm','strike',31,2.7,'hammer','aftershock'],
    ['glacial_soulbow','Glacial Soulbow','hushwood_bow','dawn_harpbow','frost','mend',27,2.35,'harpbow','renewal'],
    ['auric_flare_cannon','Auric Flare Cannon','honeycomb_cannon','marble','fire','burn',29,2.2,'cannon','rupture'],
    ['inkthorn_fan','Inkthorn Fan','brush','nettle','spirit','poison',23,2.25,'fan','bloom'],
    ['emberreef_lance','Emberreef Lance','barnacle_buckler','fang','fire','pierce',27,2.35,'lance','wardbreak'],
    ['rosewater_benediction','Rosewater Benediction','dewdrop_chalice','snowmelt_phial','leaf','heal',18,2.45,'cup'],
    ['warmrain_kettle','Warmrain Kettle','hearth_tea','rain_pearl','storm','heal',20,2.55,'gourd'],
    ['kinship_salve','Kinship Salve','mineral_salve','kindred_candle','spirit','heal',23,2.8,'mortar'],
    ['willowice_shelter','Willowice Shelter','barkward_charm','iceward_mirror','frost','shield',23,2.6,'aegis'],
    ['moonstorm_mantle','Moonstorm Mantle','static_canopy','moonveil_spool','spirit','shield',22,2.4,'scarf'],
    ['cinderstone_refuge','Cinderstone Refuge','ember_screen','granite_ward','stone','shield',28,3,'pavise'],
    ['hushroot_carillon','Hushroot Carillon','snaproot_totem','hushbell','leaf','stun',18,2.85,'bell'],
    ['dreamflash_prism','Dreamflash Prism','flashpowder_pouch','dreamcatcher','spirit','stun',21,2.95,'prism'],
    ['stormseal_gong','Stormseal Gong','thunderclap_cymbal','stillstone_seal','stone','stun',24,3.1,'gong'],
    ['wintermoon_seed','Wintermoon Seed','winterseed','pale_moon_orb','spirit','freeze',20,3.3,'seed'],
    ['coldtempest_beacon','Coldtempest Beacon','coldflame_lantern','sleetwind_chime','storm','freeze',21,3.25,'lantern'],
    ['saltglass_stillness','Saltglass Stillness','glacier_hourglass','frostsalt_geode','stone','freeze',26,3.55,'hourglass'],
    // Eighteen second-stage recipes pair first-stage crafts from different roads.
    ['verdant_eclipse_spear','Verdant Eclipse Spear','vinetongue_spear','ghostthorn','spirit','drain',40,2.1,'lance','leech'],
    ['sunburst_hive','Sunburst Hive','cindersnap_repeater','swarmbox','fire','rapid',25,1.2,'cannon','overload'],
    ['plaguefire_harvester','Plaguefire Harvester','sootbloom_scythe','lash','leaf','poison',43,2.15,'sicklestaff','bloom'],
    ['polar_thunderwheel','Polar Thunderwheel','rimewire_chakram','aurora_crescent','frost','shock',42,2,'chakram','permafrost'],
    ['horizon_ballista','Horizon Ballista','galequarry_ballista','northstar','storm','pierce',45,1.95,'ballista','chain'],
    ['midnight_faultreaper','Midnight Faultreaper','moonflint_scythe','gravemoon_reaper','stone','drain',49,2.35,'chainscythe','rupture'],
    ['crimson_meteor_dance','Crimson Meteor Dance','scarlet_comet_blade','cometskates','fire','rapid',31,1.3,'twinblades','execute'],
    ['stormroot_colossus','Stormroot Colossus','thunderbark_maul','stormengine','storm','strike',55,2.4,'gravitymaul','aftershock'],
    ['winterdawn_harp','Winterdawn Harp','glacial_soulbow','seraph_harp','spirit','mend',42,2.15,'harpbow','renewal'],
    ['caldera_tidebreaker','Caldera Tidebreaker','auric_flare_cannon','emberreef_lance','fire','pierce',49,2.15,'trident','wardbreak'],
    ['rosepetal_spring','Rosepetal Spring','rosewater_benediction','springwater_ampoule','leaf','heal',34,2.2,'cup'],
    ['hearthlight_rain','Hearthlight Rain','warmrain_kettle','kinship_salve','spirit','heal',43,2.35,'censer'],
    ['willowmoon_citadel','Willowmoon Citadel','willowice_shelter','silverthread_veil','frost','shield',44,2.35,'pavise'],
    ['stormstone_pavilion','Stormstone Pavilion','moonstorm_mantle','cinderstone_refuge','storm','shield',50,2.25,'parasol'],
    ['rootbound_silence','Rootbound Silence','hushroot_carillon','silence_chimes','leaf','stun',34,2.55,'bell'],
    ['reverie_thunderbell','Reverie Thunderbell','dreamflash_prism','stormseal_gong','spirit','stun',44,2.65,'gong'],
    ['moonfrost_garden','Moonfrost Garden','wintermoon_seed','frostbloom_seed','spirit','freeze',38,3,'seed'],
    ['tempest_glacier_prism','Tempest Glacier Prism','coldtempest_beacon','saltglass_stillness','frost','freeze',48,2.95,'prism'],
    // Eight third-stage destinations reward an extended fusion plan.
    ['eclipse_worldreaper','Eclipse Worldreaper','verdant_eclipse_spear','worldtree','spirit','drain',68,1.95,'chainscythe','leech'],
    ['stellar_hive_engine','Stellar Hive Engine','sunburst_hive','cometfall','fire','rapid',44,1.1,'cannon','overload'],
    ['stormwinter_singularity','Stormwinter Singularity','polar_thunderwheel','tempest','storm','shock',71,1.85,'orbiter','chain'],
    ['caldera_skybreaker','Caldera Skybreaker','caldera_tidebreaker','horizon_ballista','fire','pierce',83,1.95,'greatsword','wardbreak'],
    ['garden_of_first_rain','Garden of First Rain','rosepetal_spring','hearthlight_rain','leaf','heal',74,2.05,'cup'],
    ['citadel_of_four_skies','Citadel of Four Skies','willowmoon_citadel','stormstone_pavilion','storm','shield',86,2.05,'pavise'],
    ['last_bell_of_dreams','Last Bell of Dreams','rootbound_silence','reverie_thunderbell','spirit','stun',77,2.35,'bell'],
    ['winter_beyond_the_moon','Winter Beyond the Moon','moonfrost_garden','tempest_glacier_prism','frost','freeze',82,2.7,'orbiter']
  ];
  for(const row of crossroadFusions){
    recipes.push(row.slice(0,9));if(row[9])signatures[row[0]]=row[9];
  }
  for(const [id,name,a,b,element,style,power,cooldown,icon] of recipes){
    const pair=[items[a],items[b]];
    const mythic=pair.some(d=>d.rarity==='mythic')||pair.every(d=>d.recipe);
    const unlock=Math.max(mythic?4:1,...pair.map(d=>d.unlockRound));
    item(id,name,element,style,power,cooldown,icon,`${pair[0].name} fused with ${pair[1].name}.`,
      [a,b],mythic?'mythic':'rare',unlock,pair[0].dropRegion??pair[1].dropRegion);
  }
  for(const [id,special] of Object.entries(signatures))Object.assign(items[id],{special,specialDescription:specialText[special]});
  const farCounters={
    patina_disc:'insulated',glassfire_cleaver:'thaw',magma_mantle:'blastguard',lantern_apple:'thaw',
    stormtotem_lance:'insulated',liongrass_sabre:'unbinding',bonekey_greatsword:'wardbreaker',obsidian_missal:'purify',
    dreamtide_crook:'purify',marshlight_crossbow:'antivenom',honeycomb_cannon:'unbinding',amberwax_aegis:'cleanse',
    railspike_lance:'wardbreaker',signal_chainblade:'thaw',silvervine_wand:'cleanse',glasswing_orbiter:'blastguard',
    voltaic_crossbow:'insulated',coppercoil_aegis:'insulated',titanroot_scythe:'unbinding',canyonfang_greatsword:'blastguard',
    mothmoon_tome:'purify',velvet_crescent:'thaw',bloodbloom_thornbow:'antivenom',redmist_cleavers:'antivenom',
    aurora_orbiter:'cleanse',fracture_lance:'wardbreaker',dawnstar_greatsword:'wardbreaker',orrery_wand:'purify',
    verdigris_torrent:'insulated',caldera_heart:'blastguard',silent_solstice:'thaw',winter_bloom:'antivenom',
    savanna_king:'unbinding',ossuary_sentence:'wardbreaker',reverie_harvester:'purify',cathedral_sun:'cleanse',
    cinder_express:'blastguard',winterglass_prism:'thaw',stormglass_apparatus:'insulated',worldroot_reaper:'unbinding',
    encore_eclipse:'purify',crimson_harvest:'antivenom',heavens_splitter:'cleanse',firstlight_judgment:'wardbreaker',
    glacial_soulbow:'thaw',galequarry_ballista:'insulated',emberreef_lance:'wardbreaker',
    rosewater_benediction:'cleanse',warmrain_kettle:'thaw',kinship_salve:'purify',
    moonstorm_mantle:'insulated',cinderstone_refuge:'blastguard',
    horizon_ballista:'insulated',midnight_faultreaper:'purify',caldera_tidebreaker:'wardbreaker',
    rosepetal_spring:'cleanse',hearthlight_rain:'thaw',willowmoon_citadel:'thaw',stormstone_pavilion:'insulated',
    eclipse_worldreaper:'antivenom',caldera_skybreaker:'wardbreaker',garden_of_first_rain:'cleanse',
    citadel_of_four_skies:'insulated',last_bell_of_dreams:'purify',winter_beyond_the_moon:'thaw'
  };
  for(const [id,counter] of Object.entries(farCounters))Object.assign(items[id],{counter,counterDescription:counterText[counter]});
  const roster = [
    [ ['Mossback Beetle','beetle','armored','fire','leaf','#88bc62'], ['Pollen Moth','moth','swarm','frost','leaf','#ecd28a'], ['Bramble Boar','boar','brute','fire','stone','#bf815c'], ['Sapsip Toad','frog','leech','storm','leaf','#73b675'], ['Lanterncap','shroom','regenerating','fire','spirit','#dd926a'], ['Vine Adder','serpent','venom','frost','leaf','#91b64d'], ['Crownroot Sentinel','golem','veteran','fire','leaf','#7f9c65'] ],
    [ ['Coalback Boar','boar','brute','frost','fire','#b55e44'], ['Ashwing Bat','bat','swift','storm','fire','#a28195'], ['Cinder Moth','moth','volatile','frost','fire','#e68e61'], ['Furnace Crab','crab','armored','frost','stone','#cb7857'], ['Soot Hound','wolf','swift','frost','fire','#8a7876'], ['Spark Viper','serpent','venom','stone','fire','#df934a'], ['Orchard Pyre','golem','warded','frost','fire','#ff9865'] ],
    [ ['Snowcap Harewolf','wolf','swift','fire','frost','#b2d3df'], ['Rime Carapace','beetle','armored','fire','frost','#8ebecf'], ['Glasswing Owl','bird','warded','stone','frost','#b5a9da'], ['Chillblood Toad','frog','leech','storm','frost','#7bb6cd'], ['Icicle Bat','bat','swarm','fire','frost','#a9cada'], ['Crevasse Wisp','wisp','regenerating','spirit','frost','#82dce3'], ['Glacial Colossus','golem','brute','fire','frost','#9ac9e0'] ],
    [ ['Gale Kestrel','bird','swift','stone','storm','#baa3d5'], ['Static Moth','moth','volatile','stone','storm','#d9ba71'], ['Copperclaw','crab','armored','leaf','storm','#bd8c64'], ['Wire Serpent','serpent','swift','stone','storm','#cca768'], ['Drumcloud Boar','boar','brute','stone','storm','#a8a6c1'], ['Arc Wisp','wisp','warded','spirit','storm','#fae18b'], ['Thunder Roc','bird','veteran','stone','storm','#dfbc72'] ],
    [ ['Sandglass Beetle','beetle','armored','leaf','stone','#cea567'], ['Dust Runner','wolf','swift','frost','stone','#d39964'], ['Mesa Rockling','golem','brute','leaf','stone','#bb835e'], ['Copper Rattler','serpent','venom','frost','stone','#bd8050'], ['Dune Locust Moth','moth','swarm','storm','stone','#ceb785'], ['Mirage Spirit','wisp','warded','spirit','fire','#e9bf70'], ['Canyon King','boar','veteran','leaf','stone','#cc8964'] ],
    [ ['Mooncap Widow','shroom','venom','fire','spirit','#a391c9'], ['Mirelight Toad','frog','regenerating','storm','leaf','#77ab9d'], ['Reedstalk Adder','serpent','leech','frost','spirit','#9992b1'], ['Duskwing Bat','bat','swarm','storm','spirit','#a799bb'], ['Bog Wolf','wolf','swift','fire','leaf','#81978e'], ['Drowned Lantern','wisp','volatile','spirit','frost','#83d2ca'], ['Fenmother','shroom','regenerating','fire','spirit','#b2a4d9'] ],
    [ ['Page-Eater Moth','moth','swarm','fire','leaf','#dbd2aa'], ['Inkpool Crab','crab','armored','storm','stone','#90b3b1'], ['Script Serpent','serpent','venom','frost','spirit','#85aa9b'], ['Bookmark Bat','bat','leech','storm','spirit','#ae8992'], ['Brass Librarian','golem','warded','leaf','stone','#bba772'], ['Inkcap','shroom','volatile','fire','frost','#8dabaf'], ['The Archivist','bird','veteran','storm','spirit','#83bdc1'] ],
    [ ['Stardust Beetle','beetle','armored','stone','spirit','#ba9bcb'], ['Nebula Hound','wolf','swift','frost','spirit','#a596c8'], ['Meteor Crab','crab','brute','frost','fire','#d79291'], ['Voidglass Moth','moth','warded','storm','spirit','#bca8e2'], ['Orbit Toad','frog','regenerating','leaf','frost','#92b3cd'], ['Eclipse Bat','bat','leech','fire','spirit','#b298bb'], ['The Crownless Star','wisp','veteran','spirit','storm','#e7c6e8'] ]
  ];
  const roadCreatures = [
    [['Budblade Mantis','mantis','swift','frost','leaf','#b0cd72'],['Hedgeback Turtle','turtle','armored','fire','stone','#759c64'],['Cloverhorn Stag','stag','regenerating','fire','leaf','#c0ac74'],['Orchid Webspinner','spider','venom','storm','leaf','#cf938a']],
    [['Blisterclaw Scorpion','scorpion','venom','frost','fire','#d79453'],['Furnace Fox','fox','swift','frost','fire','#e0a272'],['Charcoal Snail','snail','armored','leaf','stone','#92807b'],['Magma Frill','axolotl','volatile','frost','fire','#ed8469']],
    [['Icetooth Fox','fox','swift','fire','frost','#bbc9e3'],['Crystal Antler','stag','warded','stone','frost','#b2cbd6'],['Coldwater Frill','axolotl','regenerating','fire','frost','#94c3cf'],['Icefall Shellback','turtle','armored','storm','frost','#9daabc']],
    [['Needlestorm Mantis','mantis','swarm','stone','storm','#d2bb79'],['Lightning Stinger','scorpion','volatile','stone','storm','#ecc974'],['Wireweaver','spider','venom','fire','storm','#bb9cc9'],['Kite-Eared Fox','fox','warded','frost','storm','#b4b0d3']],
    [['Ocher Scorpion','scorpion','brute','frost','stone','#b78c52'],['Painted Shellback','turtle','veteran','leaf','stone','#c19b71'],['Juniper Stag','stag','swift','leaf','stone','#caa26e'],['Cactus Glider','snail','warded','frost','leaf','#a6ac6c']],
    [['Bog Lantern Snail','snail','leech','fire','spirit','#a8a686'],['Ghost Frill','axolotl','warded','spirit','frost','#b5a5cc'],['Willow Mantis','mantis','venom','frost','leaf','#8fba91'],['Marsh Threader','spider','swarm','storm','spirit','#b59bad']],
    [['Inkblot Axolotl','axolotl','leech','storm','spirit','#8d9fba'],['Folio Turtle','turtle','armored','fire','leaf','#aba580'],['Marginalia Fox','fox','swift','stone','spirit','#b89d90'],['Clockwork Mantis','mantis','veteran','leaf','stone','#c0ad7d']],
    [['Comet Stag','stag','brute','frost','fire','#d7a4a6'],['Dark-Matter Spider','spider','warded','spirit','storm','#9d8cc1'],['Starglass Scorpion','scorpion','venom','stone','spirit','#c0a3ce'],['Spiral-Galaxy Snail','snail','regenerating','storm','spirit','#c5abd1']]
  ];
  // The guardian is the final entry in each primary roster. Original regions
  // keep their four additional residents after the guardian, preserving IDs.
  const outerRoad = [
    [
      ['Tideglass Hermit','crab','armored','storm','stone','#deb69c'],
      ['Foam Dart','ray','swift','storm','frost','#91cdd4'],
      ['Coral Tunneler','mole','brute','leaf','stone','#bb8a82'],
      ['Blue-Ring Scribe','octopus','venom','frost','spirit','#91a4e2'],
      ['Shellbell Snail','snail','warded','fire','stone','#d6ba83'],
      ['Salt-Skipper','frog','swarm','storm','frost','#84b1a6'],
      ['Anemone Frill','axolotl','regenerating','fire','leaf','#e8a5b7'],
      ['The Eightfold Admiral','octopus','veteran','storm','frost','#e79183']
    ],[
      ['Cinder Bellows','ray','volatile','frost','fire','#de8f64'],
      ['Rivet-Eater','pangolin','armored','leaf','stone','#a38d7b'],
      ['Copper Stoker','golem','brute','frost','fire','#bf845f'],
      ['Ladlewing','bat','swarm','storm','fire','#cda69b'],
      ['Sootspool Widow','spider','venom','frost','fire','#ae8086'],
      ['Brass-Tusk Runner','boar','swift','stone','storm','#c1a36b'],
      ['Crucible Wisp','wisp','leech','frost','spirit','#ffd185'],
      ['The Bellows Manta','ray','brute','frost','fire','#f0b572']
    ],[
      ['Glowgill Forager','shroom','regenerating','fire','leaf','#8bdbb0'],
      ['Moldcap Mantis','mantis','venom','frost','leaf','#abd494'],
      ['Blister Puff','frog','volatile','fire','spirit','#bed59e'],
      ['Undercap Hare','hare','swift','frost','leaf','#c3b099'],
      ['Bluegill Lantern','jellyfish','leech','storm','frost','#a6dbdd'],
      ['Threadroot Stag','stag','warded','fire','leaf','#9da77d'],
      ['Mycelial Mite','beetle','swarm','fire','stone','#beac82'],
      ['Shepherd of Spores','ram','regenerating','fire','leaf','#c2d68a']
    ],[
      ['Brinewing Skimmer','dragonfly','swift','stone','storm','#c8d8ce'],
      ['Fulgurite Coil','serpent','venom','frost','stone','#d4b87a'],
      ['Prism Carver','mole','armored','leaf','stone','#c4a282'],
      ['Halite Antler','elk','brute','leaf','frost','#d4c6a8'],
      ['Silver Mirage','fox','warded','spirit','frost','#bec9c0'],
      ['Sunblind Moth','moth','swarm','storm','fire','#e4d696'],
      ['Bitterglass Orb','wisp','volatile','stone','storm','#f3ce97'],
      ['Keeper of Seven Mirrors','owl','warded','stone','spirit','#ecd8a3']
    ],[
      ['Eave Hopper','hare','swift','stone','storm','#c7c3da'],
      ['Bellclapper Bat','bat','swarm','storm','spirit','#c6acce'],
      ['Prayer-Slate Tortoise','turtle','warded','leaf','stone','#acc6bf'],
      ['Gale Pilgrim','griffin','veteran','stone','storm','#c2b8cb'],
      ['Incense Feeder','moth','leech','frost','spirit','#d1aaba'],
      ['Skyrope Adder','serpent','venom','frost','storm','#b4cbd3'],
      ['Hailstep Ram','ram','brute','fire','frost','#c8d5e4'],
      ['Abbot of the Ninth Bell','pangolin','armored','leaf','storm','#d8c3a4']
    ],[
      ['Copalback Grazer','boar','armored','leaf','stone','#be9661'],
      ['Marrow Mantis','mantis','venom','frost','stone','#ceba87'],
      ['Petrified Warden','golem','veteran','leaf','stone','#c3ac91'],
      ['Rattlehorn Ram','ram','brute','frost','stone','#c29984'],
      ['Ember Resin','wisp','volatile','frost','fire','#d0a365'],
      ['Frond-Eater','pangolin','regenerating','fire','leaf','#aa9f77'],
      ['Knothole Owl','owl','swift','storm','leaf','#b2a18c'],
      ['Marrowjack the Long-Eared','hare','swift','frost','stone','#deba90']
    ],[
      ['Kelp-Ribbon Ray','ray','swift','storm','frost','#78c0bb'],
      ['Pearlkeeper','octopus','warded','storm','spirit','#baa5cd'],
      ['Glassbell Jelly','jellyfish','leech','storm','frost','#8ed2dc'],
      ['Eelgrass Serpent','serpent','venom','frost','leaf','#7ead97'],
      ['Lampfin Larva','axolotl','regenerating','fire','leaf','#8cc6ca'],
      ['Urchin-Claw Hermit','crab','armored','storm','stone','#a6a4a6'],
      ['Bubble Lantern','wisp','volatile','stone','storm','#a2d9ce'],
      ['Chandelier of the Deep','jellyfish','leech','storm','spirit','#9ce7e3']
    ],[
      ['Velvet Thorn','spider','venom','fire','leaf','#c596ab'],
      ['Damask Herald','griffin','veteran','stone','spirit','#dba9a3'],
      ['Petal Mask','moth','swarm','storm','leaf','#e4bcc8'],
      ['Gilt Hedgeknight','pangolin','armored','leaf','stone','#c3ac86'],
      ['Rosehip Familiar','fox','swift','frost','leaf','#cf998f'],
      ['Mosaic Stag','stag','warded','stone','spirit','#cdafc8'],
      ['Latticewing','dragonfly','leech','storm','spirit','#d3a4bd'],
      ['The Glass Marchioness','dragonfly','swift','stone','leaf','#f2bccd']
    ],[
      ['Sleet Prowler','wolf','swift','fire','frost','#a3b7ca'],
      ['Rime Gull','bird','swarm','storm','frost','#b3d0da'],
      ['Keelback Seal-Ram','ram','brute','fire','stone','#a6afc0'],
      ['Longship Lichen','shroom','regenerating','fire','leaf','#95b6ac'],
      ['Floehorn Elk','elk','warded','stone','frost','#acc5d3'],
      ['Blackice Skater','beetle','armored','fire','frost','#94aebe'],
      ['Wreck Lantern','jellyfish','leech','spirit','frost','#90ccd4'],
      ['The Northern Figurehead','elk','brute','fire','frost','#a6d6e2']
    ],[
      ['Winding Swift','bird','swarm','stone','storm','#c9b98d'],
      ['Brass Barn Owl','owl','warded','stone','storm','#cdad7e'],
      ['Mainspring Hare','hare','swift','frost','storm','#bdaf93'],
      ['Tick-Tock Weaver','spider','venom','fire','stone','#b9967c'],
      ['Gearshaft Mole','mole','armored','leaf','stone','#b9a285'],
      ['Oilbath Manta','ray','regenerating','frost','fire','#b0a592'],
      ['Loose Escapement','golem','volatile','stone','storm','#cea977'],
      ['Meridian Talon','griffin','veteran','stone','storm','#e0bd7c']
    ],[
      ['Comet Burrower','mole','swift','frost','fire','#b194a6'],
      ['Basalt Armadillo','pangolin','armored','leaf','stone','#a9a0aa'],
      ['Quarry Foreman','golem','brute','leaf','stone','#b39f99'],
      ['Gravity Jelly','jellyfish','warded','spirit','stone','#a8a4ce'],
      ['Crater Gnawer','beetle','swarm','fire','stone','#bc9e99'],
      ['Voidscale Drake','dragon','leech','frost','spirit','#b49bc2'],
      ['Falling Cinder','wisp','volatile','frost','fire','#d2999d'],
      ['The Worldborer','mole','armored','leaf','stone','#d1b0b2']
    ],[
      ['Suncrest Elk','elk','brute','frost','fire','#d8c091'],
      ['Halo Skimmer','dragonfly','swift','stone','storm','#d4cb93'],
      ['Dawnpetal Owl','owl','warded','stone','spirit','#dfc59b'],
      ['Seedlight Swarm','moth','swarm','fire','leaf','#d1cb82'],
      ['Golden Rootkeeper','golem','regenerating','fire','leaf','#b8bd7e'],
      ['Crownfire Hatchling','dragon','volatile','frost','fire','#dda477'],
      ['Brightbark Viper','serpent','venom','frost','leaf','#b8c783'],
      ['Solivar, the Dawnroot Dragon','dragon','veteran','frost','fire','#f0c082']
    ]
  ];
  // Each far-road resident carries an additional, counterable affliction.
  // Familiar sprite families and traits keep tells readable as the road grows.
  const farRoad=[
    [
      ['Canal-Coin Hermit','crab','armored','storm','stone','#a8bb92','bulwark'],
      ['Patina Spitter','snail','venom','fire','leaf','#79bfa3','corrosive'],
      ['Copperfin Skimmer','ray','swift','stone','storm','#8ac9bf','shocking'],
      ['Sluicegate Weaver','spider','swarm','frost','leaf','#9fbf9b','entangling'],
      ['Drowned Censer','wisp','leech','spirit','frost','#92d5cb','hexed'],
      ['Verdigris Frill','axolotl','regenerating','fire','leaf','#7cb7a2','corrosive'],
      ['The Brinewrought Regent','golem','warded','leaf','stone','#9cc8ad','corrosive']
    ],[
      ['Glassback Salamander','axolotl','volatile','frost','fire','#e79d7a','scorching'],
      ['Lava-Thread Mantis','mantis','swift','frost','fire','#d27a68','scorching'],
      ['Obsidian Stag','stag','armored','leaf','stone','#ad8b96','bulwark'],
      ['Sulfur Widow','spider','venom','frost','fire','#d2a46c','corrosive'],
      ['Cindervein Moth','moth','swarm','storm','fire','#f0b68b','shocking'],
      ['Magma-Scale Pangolin','pangolin','brute','frost','stone','#b88675','bulwark'],
      ['Vespara, the Glassfire Wyrm','dragon','veteran','frost','fire','#f1a476','scorching']
    ],[
      ['Lanternsnow Hare','hare','swift','fire','frost','#d4dfcd','freezing'],
      ['Hushwood Antler','elk','warded','stone','frost','#b6d4d3','bulwark'],
      ['Frostapple Beetle','beetle','armored','fire','leaf','#a5c6b8','freezing'],
      ['Snowveil Fox','fox','leech','spirit','frost','#d2d6e1','hexed'],
      ['Iceblossom Snail','snail','regenerating','fire','leaf','#bed8cc','entangling'],
      ['Whitebell Bat','bat','swarm','storm','frost','#ccdbea','freezing'],
      ['The Orchard’s Last Lantern','owl','warded','fire','frost','#e3e3c8','freezing']
    ],[
      ['Thundergrass Stalker','wolf','swift','stone','storm','#c7ad78','shocking'],
      ['Coppermane Boar','boar','brute','frost','stone','#c29b63','bulwark'],
      ['Static-Feather Kite','bird','swarm','stone','storm','#d7c58a','shocking'],
      ['Stormtotem Scarab','beetle','warded','leaf','stone','#b9a371','bulwark'],
      ['Liongrass Viper','serpent','venom','frost','leaf','#a9ae6d','entangling'],
      ['Heat-Haze Manta','ray','volatile','frost','fire','#d8b28d','scorching'],
      ['Kharu, the Walking Thunder','griffin','veteran','stone','storm','#e3c580','shocking']
    ],[
      ['Bonekey Scuttler','crab','armored','leaf','stone','#b9b4b7','bulwark'],
      ['Funeral Moth','moth','leech','fire','spirit','#b9a2ce','hexed'],
      ['Obsidian Fang','wolf','swift','frost','stone','#8c829e','corrosive'],
      ['Marrow-Mist Jelly','jellyfish','warded','storm','spirit','#c7b4d8','hexed'],
      ['Graveglass Warden','golem','brute','leaf','stone','#aa9bad','bulwark'],
      ['Ashbone Scorpion','scorpion','venom','frost','fire','#b49a99','scorching'],
      ['The Giant Beneath the Bells','golem','armored','leaf','stone','#cbbcd0','bulwark']
    ],[
      ['Dreamreed Lurker','frog','regenerating','storm','leaf','#aba1c5','entangling'],
      ['Moonwell Axolotl','axolotl','leech','fire','spirit','#bca8dd','hexed'],
      ['Reverie Threader','spider','venom','frost','leaf','#b69bd3','corrosive'],
      ['Drowsy Lantern','wisp','warded','spirit','frost','#b6c8e6','freezing'],
      ['Mirrorsleep Eel','serpent','swift','stone','storm','#9d9ec9','shocking'],
      ['Doorway Moth','moth','swarm','fire','spirit','#d4bbdf','hexed'],
      ['The Dreamtide Ferryman','jellyfish','leech','storm','spirit','#cab3ec','hexed']
    ],[
      ['Waxen Mason','beetle','armored','fire','stone','#ccac66','bulwark'],
      ['Honeydrop Slug','snail','regenerating','frost','leaf','#e2bd76','entangling'],
      ['Pollen-Script Moth','moth','swarm','fire','leaf','#e6ce91','corrosive'],
      ['Candlewick Bat','bat','volatile','frost','fire','#c39875','scorching'],
      ['Amber Organist','golem','warded','leaf','stone','#c5ac6c','bulwark'],
      ['Royal Wax Mantis','mantis','swift','frost','leaf','#d6b379','entangling'],
      ['The Queen of Seven Hives','moth','veteran','fire','leaf','#f0cf8e','entangling']
    ],[
      ['Signal-Lamp Hound','wolf','swift','frost','fire','#c98977','scorching'],
      ['Railspike Mole','mole','armored','leaf','stone','#a28b7e','bulwark'],
      ['Cinder-Ticket Wisp','wisp','volatile','frost','fire','#e6ab7e','scorching'],
      ['Switchyard Weaver','spider','venom','storm','stone','#aa827c','corrosive'],
      ['Copper Boilerback','turtle','brute','frost','stone','#c19a82','bulwark'],
      ['Sootsignal Crow','bird','swarm','stone','storm','#b4a0a1','shocking'],
      ['The Last Cinder Express','boar','brute','frost','fire','#d7a184','scorching']
    ],[
      ['Silvervine Mantis','mantis','swift','fire','leaf','#acd0cb','entangling'],
      ['Greenhouse Frostling','golem','warded','fire','frost','#b9d7db','freezing'],
      ['Glass-Petal Dragonfly','dragonfly','swarm','stone','frost','#c4dfe1','freezing'],
      ['Mirror Fern Spider','spider','venom','storm','leaf','#a7bfc7','corrosive'],
      ['Snowglass Tortoise','turtle','armored','fire','frost','#9bbdcf','bulwark'],
      ['Orchid-Wake Owl','owl','leech','spirit','frost','#c4c9de','hexed'],
      ['The Gardener of Still Winters','elk','regenerating','fire','frost','#cbe3e0','freezing']
    ],[
      ['Voltaic Skimmer','ray','swift','stone','storm','#87c9c0','shocking'],
      ['Saltcoil Viper','serpent','venom','frost','storm','#97bfae','corrosive'],
      ['Copperstorm Ram','ram','brute','frost','stone','#b69f83','bulwark'],
      ['Arcglass Widow','spider','swarm','fire','storm','#91b4c6','shocking'],
      ['Grounding Tortoise','turtle','warded','leaf','stone','#9cab94','bulwark'],
      ['Blue-Salt Lantern','wisp','volatile','stone','storm','#a6ddd7','shocking'],
      ['The Faraday Leviathan','dragon','veteran','stone','storm','#9ed7cc','shocking']
    ],[
      ['Canyon-Root Pangolin','pangolin','armored','fire','leaf','#b4ac7e','bulwark'],
      ['Seedriver Elk','elk','regenerating','fire','leaf','#cab68b','entangling'],
      ['Redcliff Mole','mole','brute','leaf','stone','#bc916e','corrosive'],
      ['Titanroot Weaver','spider','venom','frost','leaf','#a9af77','entangling'],
      ['Falling-Slate Griffin','griffin','swift','frost','stone','#c8ac91','bulwark'],
      ['Seed-Tide Moth','moth','swarm','fire','leaf','#d1c28b','entangling'],
      ['The Root That Holds the Mountain','golem','regenerating','fire','leaf','#c2ba88','entangling']
    ],[
      ['Velvet-Wing Usher','bat','swift','storm','spirit','#b199bd','hexed'],
      ['Moon-Score Moth','moth','leech','fire','spirit','#d4bce6','hexed'],
      ['Curtain-Spool Spider','spider','venom','frost','leaf','#c199ba','entangling'],
      ['Hushlight Owl','owl','warded','stone','frost','#c2b3dc','freezing'],
      ['Gilded Stagehand','golem','armored','leaf','stone','#bfa487','bulwark'],
      ['Crescent Chorus','jellyfish','swarm','storm','spirit','#d4b5df','hexed'],
      ['The Mothmoon Prima','wisp','veteran','spirit','frost','#e4c5ef','hexed']
    ],[
      ['Bloodbloom Stalker','mantis','swift','frost','leaf','#b4a37c','entangling'],
      ['Scarlet-Sap Adder','serpent','venom','frost','leaf','#c49292','corrosive'],
      ['Redmist Familiar','fox','leech','storm','spirit','#d2a19d','hexed'],
      ['Basin Lanterncap','shroom','regenerating','fire','leaf','#d2a49f','entangling'],
      ['Thorn-Crown Beetle','beetle','armored','fire','stone','#b79783','bulwark'],
      ['Cinder-Petal Moth','moth','volatile','frost','fire','#e2aaa0','scorching'],
      ['Vesper, the Crimson Antler','stag','leech','fire','leaf','#e3a8af','corrosive']
    ],[
      ['Aurora-Scale Drake','dragon','swift','fire','frost','#b4c4e5','freezing'],
      ['Fracturewing','dragonfly','swarm','stone','storm','#b8d4e6','shocking'],
      ['Polar Mirrorback','turtle','armored','fire','frost','#b3c7db','bulwark'],
      ['Skyshard Jelly','jellyfish','warded','storm','spirit','#c2c9eb','hexed'],
      ['Northern-Ribbon Ray','ray','leech','stone','storm','#b5dbdd','shocking'],
      ['Blackice Voidling','wisp','volatile','fire','frost','#a8b7df','freezing'],
      ['The Warden of Broken Skies','griffin','veteran','fire','frost','#d0dff2','freezing']
    ],[
      ['Orrery-Keeper Owl','owl','warded','stone','spirit','#d7c7a9','hexed'],
      ['Firstlight Griffin','griffin','swift','frost','fire','#e5c898','scorching'],
      ['Ivory Sunscarab','beetle','armored','leaf','stone','#d5c795','bulwark'],
      ['Golden Hour Hare','hare','swarm','frost','storm','#d5bf8e','shocking'],
      ['Daybreak Rootkeeper','golem','regenerating','fire','leaf','#c4c789','entangling'],
      ['Moonring Serpent','serpent','venom','spirit','frost','#ccbdd1','corrosive'],
      ['Aurelion, the First Sunrise','dragon','veteran','frost','fire','#f1d196','bulwark']
    ]
  ];
  const enemies=[...roster,...outerRoad,...farRoad].flatMap((list,region)=> {
    const guardianIndex=list.length-1;
    return [...list,...(roadCreatures[region]||[])].map(([name,species,trait,weak,resist,color,affliction],index)=>({id:`r${region}e${index}`,name,species,color,weak,resist,trait,region,boss:index===guardianIndex,...(affliction?{affliction}:{})}));
  });
  // Tier scores estimate sustained damage plus the value of control, sustain,
  // and signature mechanics.
  // Existing mythics keep their identity; the two starter weapons and the
  // first cleansing vial remain accessible common finds.
  const styleValue={strike:1,rapid:1.23,burn:1.4,poison:1.32,pierce:1.12,guard:1.2,mend:1.3,drain:1.22,chill:1.12,shock:1.12,heal:1.15,shield:1.1,stun:1.2,freeze:1.25};
  const tierNames=Object.keys(rarities),tierThresholds=[5.8,9.5,12.5,22];
  for(const def of Object.values(items)){
    const score=def.power/def.cooldown*(styleValue[def.style]||1)+(def.special?2.5:0)+(def.counter?.8:0);
    let rank=tierThresholds.filter(threshold=>score>=threshold).length;
    if(def.rarity==='mythic')rank=4;
    if(['stick','sling','dewglass_vial'].includes(def.id))rank=0;
    def.rarity=tierNames[rank];def.rarityScore=Math.round(score*100)/100;
  }
  // Recipes are topologically ordered, so each crafted parent is finalized
  // before it becomes an ingredient. Keep cooldowns stable for saved runs and
  // raise potency enough to improve both each activation and sustained output.
  for(const [id,,a,b] of recipes){
    const def=items[id],parents=[items[a],items[b]];
    const rank=Math.max(rarities[def.rarity].rank,...parents.map(d=>rarities[d.rarity].rank));
    def.rarity=tierNames[rank];
    const scale=rarities[def.rarity].powerScale;
    const parentPotency=parents.map(d=>d.power*rarities[d.rarity].powerScale);
    const potencyFloor=Math.max(...parentPotency)*1.05;
    const rateFloor=Math.max(...parents.map((d,i)=>parentPotency[i]/d.cooldown))*def.cooldown*1.05;
    def.power=Math.ceil(Math.max(def.power,potencyFloor/scale,rateFloor/scale)*100)/100;
    def.fusionDepth=1+Math.max(...parents.map(d=>d.fusionDepth));
    if(def.style==='stun'||def.style==='freeze'){
      def.duration=Math.round(Math.max(def.duration,...parents.filter(d=>d.style===def.style).map(d=>d.duration*1.05))*1000)/1000;
    }
    def.rarityScore=Math.round((def.power/def.cooldown*(styleValue[def.style]||1)+(def.special?2.5:0)+(def.counter?.8:0))*100)/100;
  }
  const pairKey=(a,b)=>a<b?a+'|'+b:b+'|'+a;
  const recipeMap=new Map(recipes.map(([id,,a,b])=>[pairKey(a,b),id]));
  window.Catalog={items,enemies,regions,elements,rarities,effects,utilityStyles,starter:['stick','sling'],combo:(a,b)=>recipeMap.get(pairKey(a,b))??null};
})();
