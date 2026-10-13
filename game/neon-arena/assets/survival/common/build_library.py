"""Author and validate Endless Breach's reusable map/material definitions.

Run from any directory. x/z use the existing game's logical units (1.45 world
units); heights are already world units. Atlas UVs address padded inner cells.
The arrays below deliberately author different routes, landmarks and elevations.
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IDS = ['breach_ring','derelict_station','xenofall_wastes','black_ice_cryolab','living_hive','drowned_powerworks','orbital_graveyard','swarm_heart']
NAMES = ['Breach Ring','Derelict Station','Xenofall Wastes','Black Ice Cryolab','The Living Hive','Drowned Powerworks','Orbital Graveyard','The Swarm Heart']
WAVES = [0,5,10,15,20,25,30,40]
DESC = ['Fortify a military perimeter with watchtowers and cargo lanes.','Two abandoned platforms, curved tunnels and a crossing concourse.','Open alien canyons, bone arches, corrosive pools and crystal ridges.','Research rooms, a frozen courtyard and elevated coolant passages.','Winding ribbed chambers and membrane bridges above nesting grounds.','Flooded turbine halls, wet channels and powered maintenance bridges.','Wrecked docking bays, shattered hulls and low gravity observation decks.','An immense beating heart surrounded by branching living galleries.']
SKIES = [[.065,.105,.16],[.025,.035,.085],[.32,.13,.23],[.14,.24,.34],[.065,.016,.09],[.025,.10,.14],[.006,.008,.03],[.07,.012,.025]]
PALETTES = [['#22384d','#324b60','#54b6cf','#365e75'],['#152d4a','#20334f','#cf78d6','#4b658a'],['#754438','#88533e','#69cebd','#995d52'],['#54798b','#728fa1','#a6f1ff','#5cb4da'],['#31203b','#472642','#73cdbe','#96507f'],['#244147','#365159','#e4c851','#5e9c9e'],['#344151','#506176','#73b5ff','#8696a6'],['#381826','#542338','#f67392','#a64676']]
MATERIALS = [
    ['armored plate','grid plate','utility grate','damaged steel','cyan conduit','landing pad','access hatch','oil stained panel'],
    ['platform tile','yellow edge warning','broken concrete','track bed','maintenance grate','concourse tile','dusty panel','direction markings'],
    ['cracked soil','red sand','rock rubble','impact crater','mineral seam','acid stained soil','bone earth','sandstone transition'],
    ['frosted lab tile','white ceramic','cracked ice','frozen grate','coolant hatch','broken flooring','snow drift','ice transition'],
    ['organic membrane','veined tissue','hardened chitin','slippery growth','egg bed','pulsing tissue','membrane transition','dark carapace'],
    ['wet grating','industrial concrete','flooded metal','rusted panel','electrical grid','reactor tile','water ripple','drain channel'],
    ['ship floor plate','dock bay','magnetic walkway','damaged floor','maintenance grid','gravity markings','cargo deck','fractured plate'],
    ['dark carapace','glowing veins','living tissue','hardened armor','fractured tissue','pulsing membrane','bridge muscle','energy seam']
]
MESH_SETS = [
 [('watchtower','generator'),('antenna','antenna'),('generator','generator'),('cargo_container','cargo_crate'),('barricade','cargo_crate'),('floodlight','antenna'),('transformer','turbine'),('mounted_machine','generator'),('walkway_support','pipe'),('beacon','cryopod')],
 [('train_carriage','train_carriage'),('subway_tunnel','tunnel_arch'),('escalator','generator'),('ticket_gate','cargo_crate'),('maintenance_stair','cargo_crate'),('station_sign','cargo_crate'),('vending_machine','cryopod'),('bridge_support','pipe'),('platform_canopy','tunnel_arch'),('blast_door','cargo_crate')],
 [('alien_skeleton','ribcage'),('ribcage_tunnel','ribcage'),('crystal_cluster','crystal_cluster'),('rock_arch','rock_arch'),('cliff_outcrop','crystal_cluster'),('acid_formation','egg_cluster'),('alien_cactus','antenna'),('meteor_formation','crystal_cluster'),('monolith','crystal_cluster'),('burrow_mound','heart')],
 [('cryopod','cryopod'),('research_console','generator'),('refrigeration_tank','pipe'),('lab_machine','turbine'),('frozen_specimen','egg_cluster'),('ice_column','crystal_cluster'),('research_support','pipe'),('containment_door','cargo_crate'),('coolant_pipe','pipe'),('experiment_chamber','tunnel_arch')],
 [('egg_cluster','egg_cluster'),('organic_column','pipe'),('ribbed_tunnel','rib_arch'),('nesting_pod','heart'),('bridge_rib','rib_arch'),('hanging_tendril','pipe'),('biological_arch','rib_arch'),('wall_growth','heart'),('cocoon_cluster','egg_cluster'),('brood_structure','ribcage')],
 [('transformer','generator'),('turbine','turbine'),('pump','generator'),('bridge_support','pipe'),('floodgate','cargo_crate'),('conduit_cluster','pipe'),('reactor_core','cryopod'),('collapsed_catwalk','spaceship_hull'),('industrial_pipe','pipe'),('control_room','train_carriage')],
 [('broken_shuttle','spaceship_hull'),('airlock','tunnel_arch'),('docking_arm','generator'),('engine_housing','turbine'),('floating_wreckage','spaceship_hull'),('observation_chamber','tunnel_arch'),('hull_breach','tunnel_arch'),('ladder_support','antenna'),('station_frame','rib_arch'),('cargo_transport','train_carriage'),('distant_planet','distant_planet'),('starfield','starfield')],
 [('alien_heart','heart'),('biological_pillar','pipe'),('suspended_bridge','rib_arch'),('giant_egg','egg_cluster'),('energy_conduit','pipe'),('tendril_cluster','antenna'),('curved_rib','rib_arch'),('living_gateway','rib_arch'),('containment_chamber','tunnel_arch'),('brood_structure','ribcage')]
]

def save(path, data):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(data,indent=2)+'\n')

def wall(x,z,w,d,h=5,tile='wall_00',hp=850,y=0,tag=None):
    return dict(x=x,z=z,w=w,d=d,h=h,y=y,tile=tile,topTile='upper_00',hp=hp,tags=[tag] if tag else [])
def platform(x,z,w,d,top=4,tile='floor_04'):
    return dict(x=x,z=z,w=w,d=d,y=top-.55,h=.55,tile=tile,sideTile='trim_00',hp=1600)
def ramp(x,z,w,d,axis='z',a=0,b=4,tile='floor_03'):
    return dict(x=x,z=z,w=w,d=d,axis=axis,fromHeight=a,toHeight=b,tile=tile)
def prop(name,x,z,w=3,d=3,h=5,y=0,yaw=0,tile='wall_04',decorative=False,pulse=False):
    return dict(prefab=name,x=x,z=z,w=w,d=d,h=h,y=y,yaw=yaw,tile=tile,decorative=decorative,pulse=pulse)
def region(x,z,w,d,tile,y=0,kind='floor',**extra):
    return dict(x=x,z=z,w=w,d=d,tile=tile,y=y,kind=kind,**extra)

def base(i):
    return dict(version=2,id=IDS[i],name=NAMES[i],description=DESC[i],unlockWave=WAVES[i],coordinateSystem=dict(horizontalScale=1.45,heightScale=1),size=[88,88],floorHeight=0,theme=dict(sky=SKIES[i],floorA=PALETTES[i][0],floorB=PALETTES[i][1],grid=PALETTES[i][2],gridSoft=PALETTES[i][3]),defaultFloor='floor_00',floorRegions=[],walls=[],platforms=[],ramps=[],props=[],spawnZones=[],playerSpawns=[dict(x=-4,z=-3,r=1.7),dict(x=4,z=-3,r=1.7)],pickupRegions=[dict(x=0,z=0,w=20,d=20)],buildRegions=[dict(x=0,z=0,w=72,d=72,maxHeight=24)],hazardZones=[],burrowZones=[dict(x=-32,z=26,r=5),dict(x=30,z=-28,r=5)],climbSurfaces=[],lighting=[],events=[],navigation=dict(gridStep=4,links=[]),boundaries=dict(height=9,tile='wall_02'))

def spawn(x,z,types=None): return dict(x=x,z=z,r=2.2,types=types or ['ground','fly','burrow'])
maps=[base(i) for i in range(8)]

# The familiar weak red ring is reconstructed with textured destructible plating.
m=maps[0]
for v in range(-36,37,4):
    for x,z,w,d in [(v,-39,3.8,.7),(v,39,3.8,.7),(-39,v,.7,3.8),(39,v,.7,3.8)]:m['walls'].append(wall(x,z,w,d,4,'wall_02',50,tag='breach_perimeter'))
m['floorRegions']=[region(0,0,24,24,'floor_05'),region(-25,0,14,64,'floor_01'),region(25,0,14,64,'floor_02'),region(0,-25,52,8,'floor_04')]
m['platforms']=[platform(-22,-19,12,10,4),platform(22,19,12,10,4),platform(0,25,20,6,4)]
m['ramps']=[ramp(-22,-8,6,12,'z',4,0),ramp(22,8,6,12,'z',0,4),ramp(-16,25,12,6,'x',0,4)]
m['props']=[prop('generator',-12,15,6,5,5),prop('transformer',15,-18,6,5,6),prop('antenna',31,-30,3,3,15),prop('antenna',-31,30,3,3,13)]
for x,z in [(-30,-13),(-30,13),(30,-4),(30,7),(-10,-29),(10,29)]:m['props'].append(prop('cargo_container',x,z,7,4,3))
m['spawnZones']=[spawn(42,0),spawn(-42,0),spawn(0,42),spawn(0,-42),spawn(42,30),spawn(-42,-30)]
m['lighting']=[dict(x=x,z=z,y=9,color='cyan',range=12) for x,z in [(-22,-19),(22,19),(-12,15),(15,-18)]]

# Side platforms and rounded rolling stock are connected by an upper crosswalk.
m=maps[1]
m['floorRegions']=[region(-15,0,16,82,'floor_01'),region(15,0,16,82,'floor_01'),region(0,0,10,84,'floor_03'),region(0,0,80,14,'floor_05'),region(-35,0,12,74,'floor_04'),region(35,0,12,74,'floor_04')]
for x in [-27,27]:
    for z in [-29,29]:m['walls'].append(wall(x,z,1.4,24,7,'wall_00',1100))
for x in [-38,38]:
    for z in [-25,25]:m['walls'].append(wall(x,z,1,21,6,'wall_06',700))
m['walls'] += [wall(-15,-40,12,1,8),wall(15,40,12,1,8),wall(-5,-19,1,10,2.5,'wall_05'),wall(5,21,1,10,2.5,'wall_05')]
m['platforms']=[platform(0,0,56,8,5,'floor_06'),platform(-30,0,8,20,5,'floor_06'),platform(30,0,8,20,5,'floor_06')]
m['ramps']=[ramp(-30,-18,8,16,'z',0,5),ramp(30,18,8,16,'z',5,0)]
m['props']=[prop('train_carriage',-6,-25,7,22,5),prop('train_carriage',6,25,7,22,5),prop('subway_tunnel',0,-39,18,8,11,decorative=False),prop('subway_tunnel',0,39,18,8,11,decorative=False),prop('platform_canopy',-15,-20,13,24,10,decorative=True),prop('platform_canopy',15,20,13,24,10,decorative=True),prop('vending_machine',-21,12,2,2,3),prop('ticket_gate',23,-12,4,2,1.3),prop('station_sign',-15,4,8,.4,2,y=8,decorative=True)]
m['spawnZones']=[spawn(0,-39),spawn(0,39),spawn(-34,-38),spawn(34,38),spawn(-34,32),spawn(34,-32)]
m['events']=[dict(id='station_blackout',type='visibility',period=70,warning=4,duration=10,description='Emergency lighting dims. Use the illuminated concourse.')]

# A loose zigzag canyon and natural arches leave several viable defense sites.
m=maps[2]
m['floorRegions']=[region(0,0,74,38,'floor_01'),region(-25,19,20,34,'floor_02'),region(25,-18,24,36,'floor_02'),region(0,30,48,18,'floor_06'),region(0,-31,56,17,'floor_03')]
m['platforms']=[platform(-29,-17,18,17,5,'floor_02'),platform(29,17,18,17,5,'floor_02'),platform(-4,30,20,10,3,'floor_06')]
m['ramps']=[ramp(-29,-2,12,14,'z',5,0,'floor_07'),ramp(29,2,12,14,'z',0,5,'floor_07'),ramp(-20,30,12,10,'x',0,3,'floor_07')]
m['props']=[prop('rock_arch',0,-18,18,8,12),prop('ribcage_tunnel',4,15,15,19,10),prop('alien_skeleton',-20,19,12,18,5,decorative=True),prop('monolith',32,-28,5,5,17),prop('crystal_cluster',-34,8,10,8,12),prop('crystal_cluster',17,-28,8,8,14),prop('meteor_formation',-12,-31,11,9,8),prop('alien_cactus',34,34,4,4,10),prop('burrow_mound',-31,31,7,7,1.3,decorative=True)]
m['hazardZones']=[region(-8,6,10,8,'hazard_00',kind='acid',warning=3,period=54,duration=8,damage=7),region(23,-9,8,9,'hazard_01',kind='acid',warning=3,period=72,duration=7,damage=7)]
m['spawnZones']=[spawn(-38,-32),spawn(38,32),spawn(-38,24),spawn(38,-28),spawn(0,39),spawn(0,-39)]
m['boundaries']=dict(height=6,tile='wall_01',irregular=True)

# Four room wings use split walls with generous doorways and side routes.
m=maps[3]
m['floorRegions']=[region(0,0,28,28,'floor_01'),region(-26,-24,25,25,'floor_04'),region(26,-24,25,25,'floor_00'),region(-26,24,25,25,'floor_03'),region(26,24,25,25,'floor_02'),region(0,32,24,16,'floor_06')]
for x in [-15,15]:
    for z in [-28,28]:m['walls'].append(wall(x,z,1.2,21,6,'wall_01',1050))
for z in [-15,15]:
    for x in [-28,28]:m['walls'].append(wall(x,z,21,1.2,6,'wall_04',1050))
m['walls'] += [wall(-8,-15,5,1,4,'wall_05'),wall(8,15,5,1,4,'wall_05')]
m['platforms']=[platform(0,-28,40,7,4.5,'floor_03'),platform(-19,-24,8,7,4.5,'floor_03'),platform(19,-24,8,7,4.5,'floor_03')]
m['ramps']=[ramp(-29,-24,12,7,'x',0,4.5),ramp(29,-24,12,7,'x',4.5,0)]
m['props']=[prop('experiment_chamber',0,3,12,10,9),prop('research_console',-25,25,5,4,2),prop('refrigeration_tank',31,25,5,5,8),prop('lab_machine',-29,-27,5,5,6,y=4.5),prop('coolant_pipe',35,-4,3,3,13),prop('ice_column',-35,5,5,5,10)]
for x,z in [(-29,28),(-23,31),(27,-27),(32,-25),(-29,-30)]:m['props'].append(prop('cryopod',x,z,2.6,2.6,5))
m['hazardZones']=[region(0,26,18,12,'hazard_00',kind='freeze',warning=5,period=65,duration=9,damage=0,slow=.6),region(26,-8,16,8,'hazard_01',kind='freeze',warning=5,period=90,duration=8,slow=.65)]
m['events']=[dict(id='cooling_failure',type='fog',period=75,warning=5,duration=9,visibility=.45,description='Cooling failure: freezing vapor enters the side laboratories.')]
m['spawnZones']=[spawn(-37,-37),spawn(37,-37),spawn(-37,37),spawn(37,37),spawn(0,-40),spawn(0,39)]

# Ribbed winding lanes encircle a raised membrane crossover, leaving ground loops.
m=maps[4]
m['floorRegions']=[region(0,0,32,28,'floor_01'),region(-24,-19,24,28,'floor_02'),region(25,21,26,30,'floor_04'),region(0,-32,36,15,'floor_06'),region(0,33,30,16,'floor_05')]
for x,z,w,d in [(-15,-29,1.5,18),(15,29,1.5,18),(-28,3,19,1.5),(28,-3,19,1.5),(-7,14,1.5,14),(7,-14,1.5,14)]:m['walls'].append(wall(x,z,w,d,6,'wall_00',650))
m['platforms']=[platform(-18,0,10,22,5,'floor_06'),platform(0,8,28,7,5,'floor_06'),platform(18,16,10,22,5,'floor_06')]
m['ramps']=[ramp(-18,-20,10,18,'z',0,5,'floor_07'),ramp(18,35,10,16,'z',5,0,'floor_07')]
m['props']=[prop('ribbed_tunnel',0,-29,16,16,12),prop('biological_arch',0,28,19,9,13),prop('brood_structure',29,-25,11,14,9),prop('organic_column',-29,-26,5,5,16),prop('organic_column',29,30,5,5,18),prop('egg_cluster',-26,23,11,8,4),prop('cocoon_cluster',25,1,5,5,6),prop('nesting_pod',-3,-9,7,6,4,decorative=False,pulse=True),prop('hanging_tendril',-15,0,2,2,5,y=14,decorative=True)]
m['hazardZones']=[region(-5,25,12,8,'hazard_02',kind='growth',warning=5,period=80,duration=9,damage=0,structure=dict(x=-5,z=25,w=9,d=1,h=2.5,tile='wall_05'))]
m['events']=[dict(id='hive_growth',type='geometry',period=80,warning=5,duration=9,description='Membrane growth closes the nesting shortcut; both outer loops stay open.')]
m['spawnZones']=[spawn(0,-39),spawn(0,39),spawn(-38,-12),spawn(38,12),spawn(-34,32),spawn(34,-32)]

# Deep-looking water remains shallow and traversable; bridges add tactical height.
m=maps[5]
m['floorRegions']=[region(0,0,15,84,'floor_06',kind='water',slow=.78),region(0,10,82,10,'floor_06',kind='water',slow=.78),region(-26,-20,26,32,'floor_01'),region(26,-20,26,32,'floor_00'),region(-26,29,26,20,'floor_03'),region(26,29,26,20,'floor_05')]
m['platforms']=[platform(0,-18,52,8,4,'floor_00'),platform(0,29,52,8,4,'floor_00'),platform(-22,7,8,43,4,'floor_00'),platform(22,7,8,43,4,'floor_00')]
m['ramps']=[ramp(-33,-18,14,8,'x',0,4),ramp(33,-18,14,8,'x',4,0),ramp(-22,38,8,10,'z',4,0),ramp(22,38,8,10,'z',4,0)]
m['walls']=[wall(-12,-29,1,13,6,'wall_01'),wall(12,-29,1,13,6,'wall_01'),wall(-31,14,14,1,4,'wall_03'),wall(31,14,14,1,4,'wall_03')]
m['props']=[prop('turbine',-29,-25,9,9,9),prop('turbine',29,-25,9,9,9),prop('reactor_core',0,0,6,6,10,pulse=True),prop('pump',-30,29,7,6,6),prop('transformer',30,29,8,7,7),prop('industrial_pipe',-39,0,4,4,15),prop('industrial_pipe',39,0,4,4,15),prop('control_room',-28,-3,12,8,5),prop('conduit_cluster',10,31,3,3,7)]
m['hazardZones']=[region(0,12,13,9,'hazard_00',kind='electric',warning=5,period=70,duration=7,damage=9),region(26,27,10,12,'hazard_01',kind='electric',warning=5,period=95,duration=6,damage=9)]
m['spawnZones']=[spawn(-38,-38),spawn(38,-38),spawn(-38,38),spawn(38,38),spawn(0,-39),spawn(-39,7)]

# Spacecraft silhouettes divide the dock; upper arms connect both observation decks.
m=maps[6]
m['defaultFloor']='floor_04'
m['floorRegions']=[region(0,0,42,38,'floor_01'),region(-29,-23,23,28,'floor_00'),region(29,23,23,28,'floor_02'),region(-29,25,22,25,'floor_06'),region(29,-25,22,25,'floor_03')]
m['platforms']=[platform(-27,0,9,47,5,'floor_02'),platform(27,0,9,47,5,'floor_02'),platform(0,-27,55,8,5,'floor_02'),platform(0,27,55,8,5,'floor_02')]
m['ramps']=[ramp(-27,-36,9,10,'z',0,5),ramp(27,36,9,10,'z',5,0),ramp(-27,36,9,10,'z',5,0),ramp(27,-36,9,10,'z',0,5)]
m['walls']=[wall(-8,-6,1,13,4,'wall_02'),wall(8,6,1,13,4,'wall_02'),wall(-35,12,10,1,5,'wall_06'),wall(35,-12,10,1,5,'wall_06')]
m['props']=[prop('broken_shuttle',-6,-18,15,22,7,yaw=.15),prop('cargo_transport',10,20,13,18,6,yaw=-.1),prop('engine_housing',-15,12,7,7,5),prop('airlock',-27,0,13,9,9,y=5),prop('hull_breach',27,0,13,9,9,y=5),prop('docking_arm',-38,-26,3,3,19),prop('station_frame',37,29,10,8,15,decorative=True),prop('floating_wreckage',39,39,8,12,5,y=19,decorative=True),prop('floating_wreckage',-42,39,9,13,5,y=21,decorative=True)]
m['hazardZones']=[region(-26,3,9,14,'hazard_03',y=5,kind='low_gravity',gravity=.48),region(26,-3,9,14,'hazard_03',y=5,kind='low_gravity',gravity=.48)]
m['props'] += [dict(**prop('distant_planet',0,-76,42,42,60,y=20,tile='upper_07',decorative=True),background=True,surfaceType='planet'),dict(**prop('starfield',0,0,170,170,95,y=12,tile='detail_07',decorative=True),background=True,surfaceType='stars')]
m['lighting']=[dict(x=0,z=-58,y=20,color='planet',range=22),dict(x=-27,z=0,y=13,color='blue',range=18)]
m['spawnZones']=[spawn(-38,-37),spawn(38,37),spawn(-38,37),spawn(38,-37),spawn(0,-39),spawn(0,39)]

# The giant central heart rises above the lower floor; four branching galleries
# provide clockwise/counterclockwise escape routes and two connected high decks.
m=maps[7]
m['floorRegions']=[region(0,0,40,34,'floor_01'),region(-28,0,19,65,'floor_02'),region(28,0,19,65,'floor_02'),region(0,-29,60,18,'floor_06'),region(0,29,60,18,'floor_06')]
m['platforms']=[platform(-25,0,12,30,6,'floor_06'),platform(25,0,12,30,6,'floor_06'),platform(0,-18,40,8,6,'floor_06'),platform(0,18,40,8,6,'floor_06')]
m['ramps']=[ramp(-25,-28,10,26,'z',0,6,'floor_07'),ramp(25,28,10,26,'z',6,0,'floor_07'),ramp(-25,28,10,26,'z',6,0,'floor_07'),ramp(25,-28,10,26,'z',0,6,'floor_07')]
m['walls']=[wall(-13,-31,1.5,13,8,'wall_01',900),wall(13,31,1.5,13,8,'wall_01',900),wall(-32,20,10,1.4,7,'wall_04',850),wall(32,-20,10,1.4,7,'wall_04',850)]
m['props']=[prop('alien_heart',0,0,17,14,18,pulse=True),prop('biological_pillar',-35,-28,6,6,24),prop('biological_pillar',35,28,6,6,24),prop('living_gateway',0,-35,22,9,18),prop('living_gateway',0,35,22,9,18),prop('curved_rib',-25,0,16,13,13,y=6),prop('curved_rib',25,0,16,13,13,y=6),prop('giant_egg',-30,28,12,10,9),prop('brood_structure',31,-31,13,12,12),prop('energy_conduit',-9,15,3,3,14,pulse=True),prop('energy_conduit',9,-15,3,3,14,pulse=True)]
m['playerSpawns']=[dict(x=-4,z=-12,r=2),dict(x=4,z=12,r=2)]
m['hazardZones']=[region(-13,5,8,12,'hazard_00',kind='pulse',warning=5,period=58,duration=6,damage=10),region(13,-5,8,12,'hazard_00',kind='pulse',warning=5,period=76,duration=6,damage=10),region(0,30,14,8,'hazard_02',kind='growth',warning=6,period=100,duration=10,structure=dict(x=0,z=30,w=10,d=1.2,h=3,tile='wall_05'))]
m['events']=[dict(id='heart_pulse',type='pulse',period=58,warning=5,duration=6,description='The heart is charging. Leave the glowing vein channels.'),dict(id='heart_growth',type='geometry',period=100,warning=6,duration=10,description='Living tissue blocks one shortcut; the side galleries remain open.')]
m['spawnZones']=[spawn(0,-40),spawn(0,40),spawn(-39,0),spawn(39,0),spawn(-36,34),spawn(36,-34)]

for i,m in enumerate(maps):
    # Cover pockets, pickup regions and climb metadata are tied to each layout.
    m['pickupRegions'] += [dict(x=s['x']*.55,z=s['z']*.55,w=6,d=6) for s in m['spawnZones'][:4]]
    m['climbSurfaces']=[dict(x=p['x'],z=p['z'],w=p['w'],d=p['d'],top=p['y']+p['h']) for p in m['platforms']]
    save(ROOT/'maps'/f'{m["id"]}.json',m)
    tiles=[]
    for category,count,start in [('floor',16,0),('wall',16,16),('upper',8,32),('trim',8,40),('hazard',8,48),('detail',8,56)]:
        for n in range(count):
            slot=start+n;col=slot%8;row=slot//8
            label=MATERIALS[i][n%8] if category=='floor' else f'{NAMES[i]} {category} {n+1}'
            tiles.append(dict(id=f'{category}_{n:02}',name=label,slot=slot,atlas='color',pixelRect=[col*256+8,row*256+8,240,240],uv=[(col*256+8.5)/2048,(row*256+8.5)/2048,(col*256+247.5)/2048,(row*256+247.5)/2048],material='organic' if i in [4,7] else 'stone' if i==2 else 'metal',orientation='horizontal' if category=='floor' else 'any',compatibleNeighbors=[f'{category}_*','trim_*'],repeat=True,emissive=.35 if category=='hazard' else .10 if category=='detail' else 0,gameplay=dict(walkable=category=='floor',surface=label,seamless=category in ['floor','wall','upper'])) )
    save(ROOT/'tilesets'/IDS[i]/'tileset.json',dict(version=2,id=IDS[i],atlasSize=[2048,2048],allocation=256,padding=8,atlasColor='atlas_color.webp',atlasMobile='atlas_color_mobile.webp',filter='linear',mipmaps=False,tiles=tiles))
    entries=[]
    for name,mesh in MESH_SETS[i]:
        filename=f'prefabs/{name}.json';entries.append(dict(id=name,file=filename,tags=['environment','curved' if mesh not in ['cargo_crate','generator'] else 'industrial']))
        save(ROOT/'tilesets'/IDS[i]/filename,dict(version=1,id=name,geometry=f'../../../common/meshes/{dict(pipe="curved-pipe",heart="organic-heart").get(mesh,mesh.replace("_","-"))}.json',defaultTile='wall_04',collisionSource='geometry',tags=['environment',mesh]))
    save(ROOT/'tilesets'/IDS[i]/'prefabs.json',dict(version=1,items=entries))

save(ROOT/'manifest.json',dict(version=2,worldScale=1.45,atlasContract=dict(size=2048,columns=8,rows=8,cell=256,padding=8,inner=240,mipmaps=False),common=dict(meshDirectory='common/meshes/',materialContract='common/material_contract.json'),maps=[dict(id=IDS[i],name=NAMES[i],unlockWave=WAVES[i],description=DESC[i],blueprint=f'maps/{IDS[i]}.json',tileset=f'tilesets/{IDS[i]}/tileset.json',prefabs=f'tilesets/{IDS[i]}/prefabs.json',card=f'tilesets/{IDS[i]}/map_card.webp') for i in range(8)]))
save(ROOT/'common'/'material_contract.json',dict(version=1,coordinates='x/z logical coordinates × 1.45; y already world units',texture='One selected 2048 atlas; clamp to edge, linear sampling, no mipmaps to prevent neighboring cell bleed',uv='Half texel inset inside 8px extruded padding. Per-face UVs are remapped to the selected tile inner rectangle.',collision='Simplified separate boxes. Hollow prefabs use individual support boxes; decoration has no gameplay collision.',ramps='Smooth rendered slope; collision step strips ≤0.15 high plus followRamp actor support hook.',navigation='Cached layered graph, ground heights in node.y/node.height. Rebuild links after destruction or dynamic geometry.'))
print(f'Authored {len(maps)} maps, 512 padded materials, {sum(map(len,MESH_SETS))} prefab definitions.')
