import { VIEW } from "../config.js";
import { normalizeBiome } from "./regions.js";
export const CHUNK_WIDTH = 3200;
const floor = VIEW.floorY;
const pad = (x, y, w, extra = {}) => ({ x, y, w, h: 28, kind: "platform", oneWay: true, ...extra });
const ground = (x, w, y = floor) => ({ x, y, w, h: 500, kind: "ground", oneWay: false });

// Each score is a deliberately authored rhythm of heights, not mirrored filler.
// Entrances/exits share a safe seam; all critical jumps work without upgrades.
const SCORES = [
  ["shore-rhythm", "Shore Rhythm", 1, [0,-40,0,-65,-25,0,-50,0,-35,0], ["river","branches"]],
  ["olive-terraces", "Olive Terraces", 1, [-45,-90,-45,0,-45,-90,-45,0,-45,0], ["terraces","secret"]],
  ["meadow-stones", "Meadow Stones", 1, [0,-35,-70,-35,0,35,0,-35,0,0], ["stepping","movers"]],
  ["river-islands", "River Islands", 2, [-30,-85,-30,20,-40,-95,-40,15,-30,0], ["river","movers"]],
  ["farm-roofline", "Farm Roofline", 2, [-65,-130,-65,0,-75,-145,-75,0,-65,0], ["roofs","branches"]],
  ["reed-grotto", "Reed Grotto", 2, [55,110,150,85,20,-45,20,85,45,0], ["cave","secret"]],
  ["sandstone-waves", "Sandstone Waves", 1, [-40,-80,-40,0,40,80,40,0,-40,0], ["slope","dunes"]],
  ["canyon-lips", "Canyon Lips", 2, [-60,-120,-60,0,-60,-120,-60,0,-60,0], ["canyon","branches"]],
  ["oasis-crossing", "Oasis Crossing", 2, [30,65,20,-30,-80,-30,20,65,20,0], ["river","movers"]],
  ["wind-cut-stairs", "Wind Cut Stairs", 3, [-70,-140,-210,-140,-70,0,-70,-140,-70,0], ["wind","vertical"]],
  ["sand-chase", "Sand Chase", 3, [0,-40,0,-40,0,-40,0,-40,0,0], ["chase","collapse"]],
  ["caravan-vaults", "Caravan Vaults", 2, [0,-70,-140,-70,0,45,90,45,0,0], ["puzzle","secret"]],
  ["aqueduct-arches", "Aqueduct Arches", 2, [-55,-110,-55,0,-55,-110,-55,0,-55,0], ["arches","movers"]],
  ["market-rooftops", "Market Rooftops", 2, [-75,-150,-75,0,-75,-150,-75,0,-75,0], ["roofs","branches"]],
  ["watchtower-rise", "Watchtower Rise", 3, [-65,-130,-195,-260,-195,-130,-65,0,-65,0], ["vertical","walls"]],
  ["siege-scaffold", "Siege Scaffold", 3, [-45,-105,-165,-105,-45,15,-45,-105,-45,0], ["collapse","movers"]],
  ["city-cistern", "City Cistern", 3, [65,130,195,130,65,0,-65,0,65,0], ["cave","puzzle"]],
  ["colonnade-duel", "Colonnade Duel", 3, [0,-35,0,-35,0,-35,0,-35,0,0], ["arena","arches"]],
  ["cedar-switchback", "Cedar Switchback", 3, [-70,-140,-210,-280,-210,-140,-70,0,-70,0], ["vertical","branches"]],
  ["snow-spires", "Snow Spires", 4, [-80,-160,-240,-320,-240,-160,-80,0,-80,0], ["vertical","wind"]],
  ["ropeway", "Ropeway", 3, [-45,-110,-45,20,-45,-110,-45,20,-45,0], ["movers","branches"]],
  ["mountain-scree", "Mountain Scree", 4, [-55,-110,-165,-110,-55,0,55,0,-55,0], ["collapse","rockfall"]],
  ["ravine-gallery", "Ravine Gallery", 3, [50,100,150,100,50,0,-50,-100,-50,0], ["cave","secret"]],
  ["ridge-flight", "Ridge Flight", 4, [-70,-140,-210,-140,-70,0,-70,-140,-70,0], ["wind","optional-wings"]],
  ["temple-stair", "Temple Stair", 3, [-70,-140,-210,-140,-70,0,-70,-140,-70,0], ["stairs","puzzle"]],
  ["sunken-sanctum", "Sunken Sanctum", 4, [70,140,210,140,70,0,-70,0,70,0], ["cave","secret"]],
  ["broken-portico", "Broken Portico", 4, [-70,-140,-70,0,-70,-140,-70,0,-70,0], ["collapse","arches"]],
  ["guardian-court", "Guardian Court", 4, [0,-45,-90,-45,0,0,-45,-90,-45,0], ["arena","branches"]],
  ["buried-waterway", "Buried Waterway", 4, [60,120,60,0,-60,-120,-60,0,60,0], ["river","movers"]],
  ["echo-vault", "Echo Vault", 4, [-65,-130,-195,-130,-65,0,65,130,65,0], ["puzzle","walls"]],
  ["storm-ladders", "Storm Ladders", 5, [-85,-170,-255,-340,-255,-170,-85,0,-85,0], ["vertical","wind"]],
  ["lightning-causeway", "Lightning Causeway", 5, [-45,-90,-45,0,-45,-90,-45,0,-45,0], ["lightning","arena"]],
  ["shattered-summit", "Shattered Summit", 5, [-75,-150,-225,-150,-75,0,-75,-150,-75,0], ["collapse","wind"]],
  ["tempest-race", "Tempest Race", 5, [0,-60,0,-60,0,-60,0,-60,0,0], ["chase","lightning"]],
  ["cloud-split", "Cloud Split", 5, [-70,-140,-210,-140,-70,0,-70,-140,-70,0], ["branches","optional-wings"]],
  ["final-ascent", "Final Ascent", 6, [-85,-170,-255,-340,-255,-170,-85,0,-85,0], ["vertical","movers","collapse"]],
];

const BIOME_TAGS = {
  countryside: ["river","terraces","roofs","secret","stepping"],
  desert: ["dunes","canyon","chase","puzzle","wind"],
  city: ["roofs","arches","vertical","arena","puzzle"],
  mountains: ["vertical","wind","rockfall","movers","cave"],
  ruins: ["secret","puzzle","arches","collapse","stairs"],
  storm: ["wind","lightning","chase","vertical","collapse"],
};

function buildModule(spec, index) {
  const [id,name,difficulty,heights,tags] = spec;
  const platforms = [ground(0, 300)]; platforms[0].entry = true;
  heights.forEach((height, i) => {
    const x = 365 + i * 250;
    const width = [190,175,205,185][(index+i)%4];
    const collapse = tags.includes("collapse") && i > 1 && i < 8 && i % 2 === 0;
    platforms.push(pad(x, floor + height, width, { kind: collapse ? "collapsing" : "platform", temporary: collapse, route: true }));
  });
  platforms.push(ground(2860, 340)); platforms[platforms.length - 1].exit = true;
  const movers = [], hazards = [], pickupSpawns = [], enemySpawns = [], props = [], switches = [];
  if (tags.includes("arena")) {
    platforms.splice(4,4,pad(1115,floor-35,950,{route:true,arena:true}));
    props.push({x:1145,y:floor-35,w:220,h:200,artFrame:"arch",structure:true});
    props.push({x:1835,y:floor-35,w:220,h:200,artFrame:"arch",structure:true});
  }
  if (tags.includes("branches") || tags.includes("secret") || tags.includes("optional-wings")) {
    // The upper loop reconnects before the seam; a lower chamber has its own stair out.
    [2,3,4,5,6].forEach((i,j) => platforms.push(pad(365+i*250, floor+heights[i]-90-j%2*25, 150, { branch: "upper", secret: tags.includes("secret") })));
    const p = platforms[3]; pickupSpawns.push({x:p.x+55,y:p.y-155,type:tags.includes("optional-wings") ? "wings" : "holyLight",secret:true});
    if (tags.includes("secret")) {
      platforms.push(pad(1170, floor+260, 330, {branch:"secret",secret:true}));
      platforms.push(pad(1535, floor+185, 160, {branch:"secret"}));
      platforms.push(pad(1750, floor+110, 155, {branch:"secret"}));
      platforms.push(pad(1955, floor+35, 160, {branch:"secret"}));
      pickupSpawns.push({x:1310,y:floor+210,type:"shield",secret:true});
    }
  }
  if (tags.includes("movers")) {
    for (const i of [2,6]) movers.push(pad(365+i*250+55, floor+heights[i]-105, 125,
      {kind:"moving",axis:i===2?"y":"x",range:i===2?55:65,speed:1.1+(index%3)*0.15,baseX:365+i*250+55,baseY:floor+heights[i]-105,phase:index*0.37,branch:"upper"}));
  }
  if (tags.includes("optional-wings")) {
    // These high shelves sit above the ordinary upper loop: a second jump opens them.
    const highY=floor+Math.min(heights[3],heights[4],heights[5])-280;
    platforms.push(pad(1370,highY,165,{branch:"wings",requires:"wings",secret:true}));
    platforms.push(pad(1595,highY-35,165,{branch:"wings",requires:"wings",secret:true}));
    pickupSpawns.push({x:1660,y:highY-82,type:"fireRain",secret:true,requires:"wings"});
  }
  if (tags.includes("slope")) {
    // A pair of actual slopes forms a walkable optional crest on the entry island.
    platforms.push({x:145,y:floor-48,w:70,h:76,rise:48,kind:"slope",slopeDirection:"up",oneWay:false,branch:"slope"});
    platforms.push({x:215,y:floor-48,w:70,h:76,rise:48,kind:"slope",slopeDirection:"down",oneWay:false,branch:"slope"});
  }
  if (tags.includes("walls")) {
    const wallBase = platforms[5].y;
    platforms.push({x:1280,y:wallBase-280,w:44,h:190,kind:"wall",oneWay:false,branch:"upper"});
    platforms.push(pad(1370,wallBase-295,165,{branch:"wall-jump"}));
    pickupSpawns.push({x:1440,y:wallBase-340,type:"bread",secret:true});
  }
  if (tags.includes("puzzle")) {
    const approachSurface=platforms[3];
    const sw = { x:approachSurface.x+approachSurface.w-60, y:approachSurface.y-20, w:44,h:22,activated:false };
    switches.push(sw);
    platforms.push(pad(1140,floor+heights[3]-85,330,{branch:"puzzle"}));
    platforms.push({x:1340,y:floor+heights[3]-150,w:38,h:65,kind:"gate",oneWay:false,switchLocal:0,branch:"puzzle"});
    props.push({x:1340,y:floor+heights[3]-85,w:64,h:100,artFrame:"gate-closed",structure:true,switchLocal:0});
    pickupSpawns.push({x:1410,y:floor+heights[3]-130,type:"fireRain",secret:true});
  }
  const mainSurfaces=platforms.filter(p=>p.route);
  for (let i=0;i<mainSurfaces.length;i+=2) {
    const p=mainSurfaces[i]; enemySpawns.push({x:p.x+60,y:p.y-52,elite:difficulty>=5&&i===6});
  }
  if(tags.includes("arena"))for(const x of [1510,1840])enemySpawns.push({x,y:floor-35-52,elite:difficulty>=4,arena:true});
  pickupSpawns.push({x:platforms[6].x+60,y:platforms[6].y-52,type:index%3===0?"bread":"holyLight"});
  return {id,name,region:"countryside",difficulty,tags,width:CHUNK_WIDTH,platforms,movers,hazards,enemySpawns,pickupSpawns,props,switches,checkpointX:100};
}

export const ROOM_LIBRARY = SCORES.map(buildModule);
export const SAFE_FALLBACK_ROOM = ROOM_LIBRARY[0];
export function roomsFor(region, maxDifficulty = 6) {
  const biome=normalizeBiome(region), preferred=BIOME_TAGS[biome];
  const all=ROOM_LIBRARY.filter(room=>room.difficulty<=Math.max(2,maxDifficulty));
  const matching=all.filter(room=>room.tags.some(tag=>preferred.includes(tag)));
  return matching.length?matching:all;
}
export function roomForId(id) { return ROOM_LIBRARY.find(room=>room.id===id); }
