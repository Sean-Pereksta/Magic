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
    ['Dawnroot Summit','Gold leaves frame the rising sun at the crest of the world-tree.','#ecd18a']
  ].map(([name,subtitle,color])=>({name,subtitle,color}));
  const rarities = {
    common:{name:'Common',color:'#b8c6a6'}, rare:{name:'Rare',color:'#79d5f6'}, mythic:{name:'Mythic',color:'#ffc97e'}
  };
  const items = {};
  const milestones = {3:'Echo • An afterimage follows each activation for 40% damage.',5:'Guard pulse • Each activation adds a shield in a visible ring.',10:'Nova • Each activation adds a burst for 65% damage.'};
  const effects = {
    strike:'Strikes once each cooldown.', rapid:'A second blow follows after 0.13 seconds for 23% of the initial hit’s damage.',
    burn:'Sets the target on fire. The burn starts at 60% of the hit’s damage and smolders over time.',
    poison:'Adds poison equal to 23% of the hit’s damage. Poison stacks and ticks once per second.',
    chill:'Chills the target for 2.1 seconds, halving its attack-meter gain.', shock:'Interrupts the attack meter for 0.44 seconds. Overlapping shocks can hold it for up to 0.95 seconds.',
    pierce:'Bypasses a foe’s armor reduction and its protective ward.', guard:'Deals damage and grants shield equal to 60% of relic power. Total shield can reach 40% of maximum HP.',
    drain:'Steals health equal to 23% of the HP removed by the initial hit. Weakness and resistance change the amount stolen.',
    mend:'Hits a foe and restores HP equal to 55% of relic power. Healing is independent of the damage the foe takes.'
  };
  function item(id,name,element,style,power,cooldown,icon,flavor,recipe=null,rarity='common',unlockRound=1,dropRegion=null) {
    items[id]={id,name,element,style,power,cooldown,icon,description:flavor+' '+effects[style],milestones,
      rarity,unlockRound,unlockRegion:Math.floor((unlockRound-1)/4),dropRegion};
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
  for(const [id,name,a,b,element,style,power,cooldown,icon] of recipes){
    const pair=[items[a],items[b]];
    const mythic=pair.some(d=>d.rarity==='mythic')||pair.every(d=>d.recipe);
    const unlock=Math.max(mythic?4:1,...pair.map(d=>d.unlockRound));
    item(id,name,element,style,power,cooldown,icon,`${pair[0].name} fused with ${pair[1].name}.`,
      [a,b],mythic?'mythic':'rare',unlock,pair[0].dropRegion??pair[1].dropRegion);
  }
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
  const enemies=[...roster,...outerRoad].flatMap((list,region)=> {
    const guardianIndex=list.length-1;
    return [...list,...(roadCreatures[region]||[])].map(([name,species,trait,weak,resist,color],index)=>({id:`r${region}e${index}`,name,species,color,weak,resist,trait,region,boss:index===guardianIndex}));
  });
  const pairKey=(a,b)=>a<b?a+'|'+b:b+'|'+a;
  const recipeMap=new Map(recipes.map(([id,,a,b])=>[pairKey(a,b),id]));
  window.Catalog={items,enemies,regions,elements,rarities,starter:['stick','sling'],combo:(a,b)=>recipeMap.get(pairKey(a,b))??null};
})();
