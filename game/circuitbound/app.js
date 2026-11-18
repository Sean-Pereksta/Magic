(() => {
  'use strict';
  const canvas=document.getElementById('game'),ctx=canvas.getContext('2d',{alpha:false});
  canvas.tabIndex=-1;
  const DEEP=CircuitDeepWorld;
  const TILE=24,WORLD_W=DEEP.WIDTH,WORLD_H=DEEP.HEIGHT,SAVE_KEY='circuitbound-save-v1';
  let vw=innerWidth,vh=innerHeight,dpr=Math.min(devicePixelRatio||1,2),world,meta=new Map(),seed=0,gameTime=0,paused=false,craftOpen=false,manualOpen=false,lens=false,last=0,accum=0,toastTimer=0,selected=0,rotation=0,pointer={x:0,y:0,wx:0,wy:0,tx:0,ty:0,down:false,button:0},mining={key:'',progress:0},linkDrag=null;
  const E=CircuitEngineering;
  const V=typeof CircuitVertical==='undefined'?null:CircuitVertical;
  let industry=null;
  let offsiteWork=new Map();
  let engine=null, savedEngineering=null, survival=null, savedSurvival=null, ui=null, lensMode='ports', linkMode='new', selectedAssembly=null;
  const STARTER_SLOTS=['pickaxe','tool','dirt','platform','ladder','wire','generator','workbench','stone','link'];
  let hotbar=STARTER_SLOTS.slice(),activeSlot=0,buildDrag=null,touchAim=null,touchLine=null,touchStart=null;
  let blueprint=null,smartPlacement=true,manualRotation=false,modalOpen=false,ropeDismount=0;
  let workshop=null,guides=null,robotics=null,starship=null,loadedGuide=null,loadedRobotics=null,loadedStarship=null,inspected=null,activePointer=null,pendingClick=null,touchConfirm=null;
  let fleet=null,loadedFleet=null;
  const settings={difficulty:'standard',soundVolume:.35,musicVolume:.15,smartPlacement:true,smartCursor:false},particles=[];
  let audioContext=null,musicNode=null,musicGain=null,lastAmbient=0,gridFaults=0,activeSounds=0;
  const camera={x:0,y:0,shake:0},keys={};
  const tileMotions=new Map();
  const MOVE_DURATION=.16;
  let riderMotion=null;
  let ledgeDrop=0;
  const player={x:9*TILE,y:20*TILE,w:15,h:34,vx:0,vy:0,onGround:false,facing:1,health:100,walk:0};
  const stats={copperMined:0,crafted:0,motionEvents:0,coreFound:false,coreReached:false,started:Date.now()};
  const STARTING={dirt:28,stone:0,wood:0,copper:12,iron:10,crystal:5,copperBlock:6,ironBlock:4,wire:16,signal:12,generator:1,switch:1,sensor:1,motor:1,shaft:8,gear:2,piston:1,lamp:2,conveyor:6,winch:1,platform:12,door:2,ladder:8,ledge:8};
  const inventory={...STARTING};
  const T={AIR:0,GRASS:1,DIRT:2,STONE:3,WOOD:4,COPPER:5,IRON:6,CRYSTAL:7,OBSIDIAN:8,PLANK:9,COPPER_BLOCK:10,IRON_BLOCK:11,WIRE:12,SIGNAL:13,GENERATOR:14,SWITCH:15,SENSOR:16,MOTOR:17,SHAFT:18,GEAR:19,PISTON:20,LAMP:21,CONVEYOR:22,WINCH:23,PLATFORM:24,CORE:25};
  const defs={
    0:{name:'Air',solid:false},1:{name:'Grass',solid:true,hard:.35,drop:'dirt',weight:2,strength:2,conduct:0},2:{name:'Dirt',solid:true,hard:.35,drop:'dirt',weight:2,strength:2,conduct:0},3:{name:'Stone',solid:true,hard:1.15,drop:'stone',weight:5,strength:8,conduct:0},4:{name:'Wood',solid:true,hard:.6,drop:'wood',weight:1,strength:3,conduct:0},5:{name:'Copper Ore',solid:true,hard:1.3,drop:'copper',weight:5,strength:6,conduct:7},6:{name:'Iron Ore',solid:true,hard:1.55,drop:'iron',weight:6,strength:9,conduct:4,magnetic:true},7:{name:'Resonant Crystal',solid:true,hard:1.15,drop:'crystal',weight:3,strength:4,conduct:8},8:{name:'Obsidian',solid:true,hard:2.2,drop:'obsidian',weight:8,strength:12,conduct:0},9:{name:'Wood Platform',solid:true,platform:true,hard:.4,drop:'platform',weight:1,strength:3,conduct:0},10:{name:'Copper Block',solid:true,hard:1,drop:'copper',weight:5,strength:5,conduct:9,electric:true,capacity:20},11:{name:'Iron Block',solid:true,hard:1.4,drop:'iron',weight:7,strength:10,conduct:5,electric:true,capacity:30,magnetic:true},12:{name:'Copper Wire',solid:false,hard:.15,drop:'wire',electric:true,capacity:10,weight:0},13:{name:'Signal Wire',solid:false,hard:.15,drop:'signal',signal:true,weight:0},14:{name:'Hand Generator',solid:true,hard:.8,drop:'generator',electric:true,source:12,capacity:50,weight:5},15:{name:'Lever Switch',solid:false,hard:.25,drop:'switch',signal:true,emitter:true,weight:1},16:{name:'Player Sensor',solid:false,hard:.3,drop:'sensor',signal:true,emitter:true,weight:1},17:{name:'Motor',solid:true,hard:.8,drop:'motor',electric:true,mechanical:true,load:5,capacity:30,weight:5},18:{name:'Iron Shaft',solid:false,hard:.45,drop:'shaft',mechanical:true,weight:2},19:{name:'Gearbox',solid:true,hard:.7,drop:'gear',mechanical:true,weight:4},20:{name:'Piston',solid:true,hard:.9,drop:'piston',mechanical:true,weight:6},21:{name:'Crystal Lamp',solid:false,hard:.3,drop:'lamp',electric:true,signal:true,load:1,capacity:20,weight:1},22:{name:'Conveyor',solid:true,platform:true,hard:.55,drop:'conveyor',mechanical:true,weight:3},23:{name:'Winch',solid:true,hard:.8,drop:'winch',mechanical:true,weight:5},24:{name:'Wood Platform',solid:true,platform:true,hard:.4,drop:'platform',weight:1,strength:3},25:{name:'Foundry Core',solid:true,hard:99,electric:true,signal:true,load:6,capacity:80,weight:99}
  };
  const items=[
    {key:'dirt',tile:T.DIRT,name:'Dirt',icon:'▦',color:'#79553b'},{key:'stone',tile:T.STONE,name:'Stone',icon:'◆',color:'#68767a'},{key:'platform',tile:T.PLATFORM,name:'Platform',icon:'═',color:'#ba8752'},{key:'wire',tile:T.WIRE,name:'Copper Wire',icon:'⌁',color:'#e69b55'},{key:'signal',tile:T.SIGNAL,name:'Signal Wire',icon:'⌁',color:'#ee5a5a'},{key:'generator',tile:T.GENERATOR,name:'Generator',icon:'⚡',color:'#d9a347'},{key:'motor',tile:T.MOTOR,name:'Motor',icon:'⚙',color:'#5fcfd0'},{key:'shaft',tile:T.SHAFT,name:'Shaft',icon:'━',color:'#aebcba'},{key:'piston',tile:T.PISTON,name:'Piston',icon:'▣',color:'#85a1a5'},{key:'link',tile:-1,name:'Link Tool',icon:'⌗',color:'#72f2dc'},
    {key:'switch',tile:T.SWITCH,name:'Switch',icon:'⇄',color:'#9aef77'},{key:'sensor',tile:T.SENSOR,name:'Player Sensor',icon:'◉',color:'#61eee5'},{key:'gear',tile:T.GEAR,name:'Gearbox',icon:'✣',color:'#e2ca85'},{key:'lamp',tile:T.LAMP,name:'Crystal Lamp',icon:'✦',color:'#fff07b'},{key:'conveyor',tile:T.CONVEYOR,name:'Conveyor',icon:'»',color:'#79cfcc'},{key:'winch',tile:T.WINCH,name:'Winch',icon:'◎',color:'#d2b869'},{key:'copperBlock',tile:T.COPPER_BLOCK,name:'Copper Block',icon:'▥',color:'#d68752'},{key:'ironBlock',tile:T.IRON_BLOCK,name:'Iron Block',icon:'▥',color:'#a8b6b6'}
  ];
  const recipes=[
    {key:'platform',name:'Wood Platform',icon:'═',out:4,cost:{wood:1},desc:'Light movable structure'},
    {key:'copperBlock',name:'Copper Block',icon:'▥',out:2,cost:{copper:3},desc:'Conductive structural block'},
    {key:'ironBlock',name:'Iron Block',icon:'▥',out:2,cost:{iron:3},desc:'Strong magnetic conductor'},
    {key:'wire',name:'Copper Wire',icon:'⌁',out:4,cost:{copper:1},desc:'Carries up to 10 power'},
    {key:'signal',name:'Signal Wire',icon:'⌁',out:4,cost:{copper:1,crystal:1},desc:'Carries instructions'},
    {key:'generator',name:'Hand Generator',icon:'⚡',out:1,cost:{copper:4,iron:3,stone:3},desc:'Produces 12 power'},
    {key:'switch',name:'Lever Switch',icon:'⇄',out:1,cost:{wood:2,copper:1},desc:'Manual signal source'},
    {key:'sensor',name:'Player Sensor',icon:'◉',out:1,cost:{crystal:2,copper:2},desc:'Signals within 6 blocks'},
    {key:'motor',name:'Motor',icon:'⚙',out:1,cost:{iron:4,copper:3},desc:'Power becomes rotation'},
    {key:'shaft',name:'Iron Shaft',icon:'━',out:3,cost:{iron:2},desc:'Carries rotation'},
    {key:'gear',name:'Gearbox',icon:'✣',out:1,cost:{iron:3},desc:'Right-click: torque / speed'},
    {key:'piston',name:'Piston',icon:'▣',out:1,cost:{iron:4,stone:2},desc:'Pushes linked blocks'},
    {key:'lamp',name:'Crystal Lamp',icon:'✦',out:2,cost:{crystal:1,copper:1},desc:'Consumes 1 power'},
    {key:'conveyor',name:'Conveyor',icon:'»',out:3,cost:{iron:2,wood:2},desc:'Moves players and items'},
    {key:'winch',name:'Winch',icon:'◎',out:1,cost:{iron:4,copper:2},desc:'Raises linked structures'}
  ];
  E.install(T,defs,items,recipes);
  // Player-scale construction components live after the engineering catalog IDs.
  // IDs 52–54 are reserved by existing worlds. Never shift saved construction tiles.
  T.DOOR=52;T.LADDER=53;T.LEDGE=54;
  defs[T.DOOR]={name:'Wood Door',solid:true,door:true,hard:.55,drop:'door',weight:2,strength:4,conduct:0};
  defs[T.LADDER]={name:'Ladder',solid:false,ladder:true,hard:.35,drop:'ladder',weight:1,strength:2,conduct:0};
  defs[T.LEDGE]={name:'Wood Ledge',solid:true,platform:true,ledge:true,hard:.35,drop:'ledge',weight:1,strength:3,conduct:0};
  items.push(
    {key:'door',tile:T.DOOR,name:'Door',icon:'▯',color:'#c88a50'},
    {key:'ladder',tile:T.LADDER,name:'Ladder',icon:'H',color:'#d5a665'},
    {key:'ledge',tile:T.LEDGE,name:'Ledge',icon:'▔',color:'#c89557'}
  );
  recipes.push(
    {key:'door',name:'Wood Door',icon:'▯',out:1,cost:{wood:4,iron:1},desc:'Right-click to open or close',category:'structure'},
    {key:'ladder',name:'Ladder',icon:'H',out:4,cost:{wood:2},desc:'Climb up or down with W/S',category:'structure'},
    {key:'ledge',name:'Wood Ledge',icon:'▔',out:4,cost:{wood:1},desc:'Jump through; press down to drop through',category:'structure'}
  );
  const verticalParts=[
    ['ROPE',90,'rope','Climbing Rope','│',{wood:1},4,{ladder:true,rope:true}],
    ['ROPE_LADDER',91,'ropeLadder','Rope Ladder','H',{wood:2},6,{ladder:true,rope:true}],
    ['REINFORCED_LADDER',92,'reinforcedLadder','Reinforced Ladder','╫',{iron:1,wood:1},4,{ladder:true}],
    ['SCAFFOLD',93,'scaffold','Wood Scaffold','╳',{wood:2},4,{platform:true}],
    ['METAL_SCAFFOLD',94,'metalScaffold','Metal Scaffold','╬',{iron:2},4,{platform:true}],
    ['BRIDGE',95,'bridge','Rope Bridge','═',{wood:2},4,{platform:true}],
    ['ANCHOR',96,'anchor','Wall Anchor','⚓',{iron:1},2,{anchor:true}],
    ['RAIL',97,'rail','Lift Guide Rail','║',{iron:1},3,{rail:true}],
    ['MECHANICAL_ROPE',98,'mechanicalRope','Mechanical Rope','┆',{iron:1,wood:1},4,{ladder:true,rope:true}],
    ['COUNTERWEIGHT',99,'counterweight','Lift Counterweight','▣',{iron:3,stone:3},1,{counterweight:true,solid:true,weight:8,strength:20}]
  ];
  for(const [symbol,id,key,name,icon,cost,out,flags] of verticalParts){T[symbol]=id;defs[id]={name,drop:key,hard:.3,weight:1,strength:flags.rope?3:10,solid:!!flags.platform,...flags};items.push({key,tile:id,name,icon,color:'#d2b37c'});recipes.push({key,name,icon,cost,out,category:'vertical',desc:flags.rope?'Attach to a ceiling, anchor or existing rope; drag downward. W/S climbs.':flags.ladder?'Drag vertically. Hold W/S to climb; Space dismounts.':flags.platform?'Drag horizontally. Jump through from below; Down drops through.':'Supports ropes and guided lifts.'});}
  if(typeof CircuitSurvival!=='undefined')CircuitSurvival.install(T,defs,items,recipes);
  const technology={refiner:[2,'refining'],assembler:[2,'refining'],battery:[2,'electrical'],recoveryStation:[2,'electrical'],arcTower:[3,'electrical'],advancedFabricator:[4,'fabrication'],excavator:[4,'fabrication'],teslaCoil:[4,'reactor'],energyRifle:[4,'reactor'],poweredMiningTool:[4,'reactor']};
  for(const recipe of recipes)if(technology[recipe.key]){[recipe.chapter,recipe.schematic]=technology[recipe.key];}
  for(const key of ['wood','copper','iron','crystal','obsidian'])if(!items.some(i=>i.key===key))items.push({key,tile:-2,name:key[0].toUpperCase()+key.slice(1),icon:'◆',color:'#c3b087',resource:true});
  recipes.push({key:'ironIngot',name:'Hand-forged Iron',icon:'▰',out:1,cost:{iron:3,stone:1},desc:'Manual forging. A powered crusher and smelter use one ore per ingot.',category:'resources'},
    {key:'copperIngot',name:'Hand-forged Copper',icon:'▰',out:1,cost:{copper:3,stone:1},desc:'Manual forging. Automate for better material efficiency.',category:'resources'});
  CircuitRoboticsCatalog.install(T,defs,items,recipes);
  CircuitDeepIndustry.install(T,defs,items,recipes);
  CircuitFoundryAutomation.install(T,defs,items,recipes);
  CircuitWorkshop.install(T,defs,items,recipes);
  const scannerRecipe={key:'handheldScanner',name:'Handheld Scanner',icon:'⌖',out:1,cost:{cobaltIngot:2,circuit:2,crystal:4},desc:'Pin nearby deep deposits from the Expedition map.',category:'exploration',station:'workbench'};
  recipes.push(scannerRecipe);
  items.push({key:scannerRecipe.key,tile:-2,name:scannerRecipe.name,icon:scannerRecipe.icon,color:'#9ce5df',equipment:true});
  const itemByKey=new Map(items.map((item,index)=>[item.key,{item,index}]));
  const initialPickaxe=itemByKey.get('pickaxe').index;
  selected=initialPickaxe;
  const art=globalThis.CircuitArt?.create(T,defs,items);
  const deepArt=globalThis.CircuitboundFoundryArt;
  const depthRock={faultGalleries:'faultRock',osmiumBasin:'basinRock',gravityHollows:'gravityRock',nullCrystalVaults:'vaultRock',starcoreMantle:'mantleRock'};
  const palette={dirt:'#78543a',stone:'#647176',wood:'#a67240',copper:'#c77743',iron:'#8fa0a1',crystal:'#62e2e0',obsidian:'#352d49'};
  const missionDefs=[['copperMined','Mine 8 copper ore'],['crafted','Fabricate any machine part'],['grid','Power a crystal lamp'],['motionEvents','Move a linked structure'],['coreReached','Power the eastern Foundry Core']];

  function resize(){vw=innerWidth;vh=innerHeight;dpr=Math.min(devicePixelRatio||1,2);canvas.width=Math.floor(vw*dpr);canvas.height=Math.floor(vh*dpr);canvas.style.width=vw+'px';canvas.style.height=vh+'px';ctx.setTransform(dpr,0,0,dpr,0,0);ctx.imageSmoothingEnabled=false}
  addEventListener('resize',resize);resize();
  const idx=(x,y)=>x+y*WORLD_W,inside=(x,y)=>x>=0&&y>=0&&x<WORLD_W&&y<WORLD_H,get=(x,y)=>world.get(x,y);
  const tileByIndex=k=>world.getIndex(k);
  function set(x,y,v,m){if(!inside(x,y))return;world.set(x,y,v);const k=idx(x,y);tileMotions.delete(k);if(v===0)meta.delete(k);else if(m)meta.set(k,{...m});engine?.changed(x,y)}
  function hash(x,y,s=seed){return DEEP.hash(x,y,s)}
  function groundAt(x){return DEEP.groundAt(x,seed)}
  function generate(){savedEngineering=null;seed=Math.floor(Math.random()*999999);world=new DEEP.World(T,defs,seed);meta=new Map();tileMotions.clear();offsiteWork.clear();riderMotion=null;
    for(let c=0;c<34;c++){const cx=12+Math.floor(hash(c,60)*198),cy=37+Math.floor(hash(c,61)*36),rx=3+Math.floor(hash(c,62)*7),ry=2+Math.floor(hash(c,63)*4);for(let x=cx-rx;x<=cx+rx;x++)for(let y=cy-ry;y<=cy+ry;y++)if(((x-cx)/rx)**2+((y-cy)/ry)**2<1&&inside(x,y))set(x,y,T.AIR)}
    for(let x=4;x<14;x++)set(x,29,T.PLATFORM);set(5,28,T.GENERATOR,{rot:0});set(6,28,T.WIRE,{});set(7,28,T.WIRE,{});set(8,28,T.LAMP,{});
    for(const x of [18,43,78,119,151,183]){const g=groundAt(x);for(let y=g-1;y>g-5;y--)set(x,y,T.WOOD);for(let tx=x-2;tx<=x+2;tx++)for(let ty=g-8;ty<=g-4;ty++)if(Math.abs(tx-x)+Math.abs(ty-(g-6))<4&&get(tx,ty)===T.AIR)set(tx,ty,T.WOOD)}
    const coreX=205,base=groundAt(coreX);for(let x=201;x<=209;x++)for(let y=base-7;y<base;y++)if(x===201||x===209||y===base-7)set(x,y,T.IRON_BLOCK);set(coreX,base-3,T.CORE,{});for(let x=203;x<=207;x++)set(x,base-2,T.WIRE,{});
    prepareExpedition();
    player.x=9*TILE;player.y=29*TILE-player.h;player.vx=player.vy=0;player.health=100;gameTime=0;accum=0;selectedAssembly=null;selected=initialPickaxe;activeSlot=0;hotbar=STARTER_SLOTS.slice();rotation=0;blueprint=null;touchAim=null;clearActions();closeInspector();guides?.reset();robotics?.reset();starship?.reset();
    fleet?.reset();
    for(const key of Object.keys(inventory))delete inventory[key];Object.assign(inventory,STARTING,{medkit:2});
    for(const key of Object.keys(stats))delete stats[key];Object.assign(stats,{copperMined:0,crafted:0,motionEvents:0,coreFound:false,coreReached:false,started:Date.now()});
    installRareGeology();
    world.fixturesInto(set);
    survival?.reset();survival?.setDifficulty(settings.difficulty);if(engine){engine.reset();industry?.reset();simulateNetworks();renderHotbar();renderMissions()}
    camera.x=0;camera.y=Math.max(0,player.y-vh*.55);showToast('Mine wood and stone near the outpost to craft a Field Workbench.');if(engine)saveGame()}
  function prepareExpedition(){
    // A walkable entrance joins the starter deck to real ore and the natural cave network.
    for(let x=14;x<=27;x++)for(let y=27;y<=42;y++)if(x<20||y>33)set(x,y,T.AIR);
    for(let y=29;y<=42;y++)set(17,y,T.LADDER);
    for(let x=14;x<=27;x++)set(x,43,T.STONE);
    for(let x=20;x<=27;x++)for(let y=34;y<=35;y++)set(x,y,T.COPPER);
    for(let x=20;x<=30;x++)for(let y=42;y<=44;y++)set(x,y,T.IRON);
    for(let x=9;x<=17;x++)set(x,29,x===17?T.LADDER:T.PLATFORM);
    // Two isolated branches keep the tutorial within the 10-power wire capacity.
    // Bridge the gap at (9,27) to restore the core; place the field-kit generator at (11,28) to run the motor.
    for(let x=6;x<=8;x++)set(x,27,T.WIRE,{});set(9,27,T.AIR);
    for(let y=24;y<=27;y++)set(10,y,T.WIRE,{});set(10,23,T.CORE,{});
    set(10,28,T.AIR);set(11,28,T.AIR);set(12,28,T.MOTOR,{rot:0});
    set(14,28,T.STORAGE,{rot:0,contents:{wood:8}});
    // Keep starter resources to the left so there is jump clearance toward the motor and shaft.
    for(const [x,y] of [[7,24],[8,24],[7,25],[8,25],[7,26],[8,26]])set(x,y,T.WOOD);
    for(const [x,y] of CircuitGuides.starterStone)set(x,y,T.STONE);
    for(let y=44;y<79;y++)for(let x=32;x<36;x++)set(x,y,T.AIR);
    for(const [cx,cy,kind,schematic] of [[35,49,T.IRON,'refining'],[76,58,T.CRYSTAL,'electrical'],[128,68,T.IRON,'fabrication'],[180,77,T.OBSIDIAN,'reactor']]){
      for(let x=cx-7;x<=cx+7;x++)for(let y=cy-3;y<=cy+3;y++)if(inside(x,y)&&((x-cx)/7)**2+((y-cy)/3)**2<1)set(x,y,T.AIR);
      for(let x=cx-5;x<=cx+5;x++)for(let y=cy+3;y<=cy+4;y++)if(inside(x,y))set(x,y,kind);
      set(cx-4,cy+2,T.STORAGE,{rot:0,contents:{iron:3,crystal:2,...(schematic==='fabrication'?{steel:4,advancedComponent:1}:{})},discovery:true,schematic});
      set(cx,cy+1,T.WINCH,{rot:0,ruin:true,disabled:true});set(cx,cy+2,T.FRAME,{rot:0});
    }
    for(let x=171;x<=194;x++)for(let y=74;y<=81;y++)set(x,y,T.AIR);
    for(let x=171;x<=194;x++)set(x,82,T.OBSIDIAN);
    for(let y=81;y<DEEP.LEGACY_HEIGHT;y++){
      for(let x=179;x<=183;x++)set(x,y,T.AIR);
      set(181,y,T.LADDER);
    }
    set(172,81,T.STORAGE,{rot:0,contents:{crystal:6,advancedComponent:2},discovery:true,schematic:'reactor'});
    if(hash(92,91)>.82){const cx=92+Math.floor(hash(91,92)*26),cy=61;for(let x=cx-4;x<=cx+4;x++)for(let y=cy-3;y<=cy+1;y++)set(x,y,T.AIR);for(let x=cx-4;x<=cx+4;x++)set(x,cy+2,T.IRON_BLOCK);set(cx,cy+1,T.STORAGE,{rot:0,contents:{steel:6,circuit:3,repairKit:2},discovery:true,schematic:'fabrication'});}
  }
  function installRareGeology(){
    const legacyBounds=(x,y)=>x>=0&&y>=0&&x<DEEP.LEGACY_WIDTH&&y<DEEP.LEGACY_HEIGHT;
    CircuitRoboticsWorld.install({T,get,set,inside:legacyBounds,getMeta:(x,y)=>legacyBounds(x,y)?meta.get(idx(x,y)):true,hash,stats});
  }
  function surveyFromStation(data){
    const station=world.sitesById.get(data.surveySite);
    if(!station)return;
    const found=world.survey(station.x,station.y,data.surveyRadius,false);
    if(found.length)showToast(`${found.length} deposit ${found.length===1?'area':'areas'} marked on your map. Scan nearby to pinpoint the seam.`);
    else showToast('Nearby deposit areas are already on your map.');
  }
  function validSave(raw){
    const d=JSON.parse(raw);if(!d)return null;
    const legacy=Array.isArray(d.world),oldSize=DEEP.LEGACY_WIDTH*DEEP.LEGACY_HEIGHT;
    const worldSize=legacy?oldSize:WORLD_W*WORLD_H;
    if(legacy?(d.world.length!==oldSize||d.world.some(id=>!Number.isInteger(id)||!defs[id])):!DEEP.validSave(d.world,defs))return null;
    if(d.meta===undefined)d.meta=[];
    if(!Array.isArray(d.meta)||d.meta.some(e=>!Array.isArray(e)||!Number.isInteger(e[0])||e[0]<0||e[0]>=worldSize||!e[1]||typeof e[1]!=='object'))return null;
    for(const [,m] of d.meta){if(m.rot!==undefined&&(!Number.isInteger(m.rot)||m.rot<0||m.rot>3))return null;for(const field of ['contents','outputs'])if(m[field]!==undefined&&(!m[field]||typeof m[field]!=='object'||Array.isArray(m[field])||Object.entries(m[field]).some(([key,n])=>!/^[a-zA-Z][a-zA-Z0-9]*$/.test(key)||!Number.isFinite(n)||n<0)))return null;}
    if(!d.player||!Number.isFinite(d.player.x)||!Number.isFinite(d.player.y)||d.player.x<0||d.player.x>WORLD_W*TILE||d.player.y<0||d.player.y>(WORLD_H+2)*TILE)return null;return d;
  }
  function saveGame(){try{
    const data={seed,world:world.export(),meta:Array.from(meta.entries()),player:{x:player.x,y:player.y,health:player.health},inventory,stats,gameTime,version:6,hotbar,activeSlot,guide:guides?.export(),robotics:robotics?.export(),starship:starship?.export(),engineering:engine?.export(),survival:survival?.export(),settings};
    if(fleet)data.fleet=fleet.export();
    const previous=localStorage.getItem(SAVE_KEY);if(previous){try{if(validSave(previous))localStorage.setItem(SAVE_KEY+'-backup',previous)}catch{}}
    localStorage.setItem(SAVE_KEY,JSON.stringify(data));return true;
  }catch{showToast('World could not be saved: device storage is full or unavailable.');return false}}
  function loadGame(){for(const key of [SAVE_KEY,SAVE_KEY+'-backup']){try{
    const raw=localStorage.getItem(key);if(!raw)continue;const d=validSave(raw);if(!d)continue;
    const legacy=Array.isArray(d.world);
    seed=Number.isInteger(d.seed)?d.seed:0;
    world=new DEEP.World(T,defs,seed,legacy?null:d.world);
    meta=new Map(legacy?d.meta.map(([key,data])=>[DEEP.remapLegacyKey(key),data]):d.meta);
    if(legacy){
      world.importLegacy(d.world);
      world.fixturesInto(set);
      for(const enemy of Array.isArray(d.survival?.enemies)?d.survival.enemies:[]){
        if(Number.isInteger(enemy?.attackMachine))enemy.attackMachine=DEEP.remapLegacyKey(enemy.attackMachine);
      }
    }
    Object.assign(player,d.player);player.vx=player.vy=0;player.health=Math.max(0,Math.min(100,Number(player.health)||100));
    for(const k of Object.keys(inventory))delete inventory[k];for(const [k,n] of Object.entries(d.inventory||{}))if(Number.isFinite(n)&&n>=0)inventory[k]=Math.floor(n);
    Object.assign(stats,d.stats||{});
    CircuitWorkshop.migrate(inventory,stats,d.version);
    loadedGuide=d.guide;loadedRobotics=d.robotics;loadedStarship=d.starship;loadedFleet=d.fleet;
    if(Array.isArray(d.hotbar)&&d.hotbar.length===STARTER_SLOTS.length){
      hotbar=d.hotbar.map(key=>itemByKey.has(key)?key:null);
      const slot=Number.isInteger(d.activeSlot)?Math.max(0,Math.min(hotbar.length-1,d.activeSlot)):0;
      activeSlot=slot;selected=itemByKey.get(hotbar[slot])?.index??-1;
    }
    gameTime=Math.max(0,Number(d.gameTime)||0);savedEngineering=d.engineering;savedSurvival=d.survival;
    if(d.settings){settings.difficulty=['casual','standard','hardcore'].includes(d.settings.difficulty)?d.settings.difficulty:'standard';for(const k of ['soundVolume','musicVolume'])if(Number.isFinite(d.settings[k]))settings[k]=Math.max(0,Math.min(1,d.settings[k]));if(typeof d.settings.smartPlacement==='boolean')settings.smartPlacement=smartPlacement=d.settings.smartPlacement;settings.smartCursor=d.settings.smartCursor===true;}
    if(key!==SAVE_KEY)showToast('Recovered the last valid backup of your world.');return true;
  }catch{}}return false}
  const loaded=loadGame();if(!loaded)generate();
  engine=new E.Simulation({width:WORLD_W,height:WORLD_H,get,set,meta:()=>meta,defs,T,placedKeys:()=>world.placedKeys()});
  engine.reset();engine.restore(savedEngineering);
  industry=CircuitDeepIndustry.create({T,defs,get,set,engine,inventory,stats,player,tile:TILE,inside,placementBlock,recoverItems,notify:showToast});
  if(typeof CircuitSurvival!=='undefined'){survival=new CircuitSurvival.Survival({T,defs,get,set,meta:()=>meta,engine,player,inventory,stats,width:WORLD_W,height:WORLD_H,tile:TILE,notify:showToast,placedKeys:()=>world.placedKeys(),campaignHeight:DEEP.LEGACY_HEIGHT,regionForDepth:DEEP.regionAt});survival.restore(savedSurvival);survival.setDifficulty(settings.difficulty);}
  setupEngineeringUI();

  function tileRect(x,y){return{x:x*TILE-camera.x,y:y*TILE-camera.y}}
  function motionRemaining(start){return Math.max(0,1-(gameTime-start)/MOVE_DURATION);}
  function tileMotion(x,y){
    const motion=tileMotions.get(idx(x,y));
    if(!motion)return {x,y,angle:0};
    const left=motionRemaining(motion.start);
    if(motion.pivot){
      const angle=-motion.turn*Math.PI/2*left,[cx,cy]=motion.pivot,dx=x-cx,dy=y-cy;
      return {x:cx+dx*Math.cos(angle)-dy*Math.sin(angle),y:cy+dx*Math.sin(angle)+dy*Math.cos(angle),angle};
    }
    return {x:x+motion.dx*left,y:y+motion.dy*left,angle:0};
  }
  function drawSky(){const day=(Math.sin(gameTime*.012)+1)/2,top=mixColor('#071221','#24536a',day),bot=mixColor('#17303d','#79a8a0',day),g=ctx.createLinearGradient(0,0,0,vh);g.addColorStop(0,top);g.addColorStop(1,bot);ctx.fillStyle=g;ctx.fillRect(0,0,vw,vh);ctx.fillStyle=day>.3?'#ffe6a1':'#d9efff';const sunX=((gameTime*.9)%(vw+180))-90;ctx.fillRect(Math.floor(sunX),55,24,24);for(let i=0;i<45;i++){const x=(hash(i,8)*vw*1.4-camera.x*.03)%(vw+20),y=20+hash(i,9)*vh*.45;if(day<.35){ctx.fillStyle=`rgba(220,245,255,${.2+(1-day)*.6})`;ctx.fillRect(x,y,hash(i,10)>.7?2:1,hash(i,10)>.7?2:1)}}ctx.fillStyle='rgba(18,45,57,.58)';ctx.beginPath();ctx.moveTo(0,vh*.72);for(let x=0;x<=vw;x+=70){const wx=x+camera.x*.16;ctx.lineTo(x,vh*.57+Math.sin(wx*.006)*42+hash(Math.floor(wx/70),23)*50)}ctx.lineTo(vw,vh);ctx.lineTo(0,vh);ctx.fill();ctx.fillStyle='rgba(9,29,39,.72)';ctx.beginPath();ctx.moveTo(0,vh*.8);for(let x=0;x<=vw;x+=55){const wx=x+camera.x*.28;ctx.lineTo(x,vh*.68+Math.sin(wx*.009)*30+hash(Math.floor(wx/55),25)*40)}ctx.lineTo(vw,vh);ctx.lineTo(0,vh);ctx.fill()}
  function mixColor(a,b,t){const pa=parseInt(a.slice(1),16),pb=parseInt(b.slice(1),16),ar=pa>>16,ag=pa>>8&255,ab=pa&255,br=pb>>16,bg=pb>>8&255,bb=pb&255;return`rgb(${ar+(br-ar)*t|0},${ag+(bg-ag)*t|0},${ab+(bb-ab)*t|0})`}
  function drawTile(id,x,y,m){
    const animated=tileMotions.has(idx(x,y))?tileMotion(x,y):null;
    const p=animated?tileRect(animated.x,animated.y):tileRect(x,y),sx=Math.floor(p.x),sy=Math.floor(p.y);
    if(sx<-TILE||sy<-TILE||sx>vw||sy>vh)return;
    const n=hash(x,y);ctx.save();ctx.translate(sx,sy);
    if(animated?.angle){ctx.translate(TILE/2,TILE/2);ctx.rotate(animated.angle);ctx.translate(-TILE/2,-TILE/2);}
    const rockArt=y>=84?depthRock[DEEP.regionAt(y).id]:null;
    if(defs[id]?.deepOre&&rockArt)deepArt?.draw(ctx,rockArt,0,0,TILE,TILE);
    const artKey=defs[id]?.deepOre?defs[id].drop:id===T.STONE?rockArt:null;
    const painted=drawFoundryTile(id,m,x,y)||(artKey&&deepArt?.draw(ctx,artKey,0,0,TILE,TILE,{time:gameTime,working:!!defs[id]?.deepOre}))||art?.tile(ctx,id,m,gameTime);
    if(painted){} else if(id===T.WORKBENCH){machineBox('#9e7046');ctx.fillStyle='#e2b57b';ctx.fillRect(2,7,20,4);ctx.fillRect(4,12,3,12);ctx.fillRect(17,12,3,12);}
    else if(id===T.GRASS){ctx.fillStyle='#725038';ctx.fillRect(0,0,TILE,TILE);ctx.fillStyle='#5b9a55';ctx.fillRect(0,0,TILE,6);ctx.fillStyle='#86be62';for(let i=0;i<4;i++)ctx.fillRect((n*97+i*7)%22,1,2,4)}
    else if(id===T.DIRT){ctx.fillStyle='#755139';ctx.fillRect(0,0,TILE,TILE);ctx.fillStyle='#916645';ctx.fillRect(3+(n*11)%10,5,3,3);ctx.fillRect(15,16,2,2)}
    else if(id===T.STONE){ctx.fillStyle='#5d6b70';ctx.fillRect(0,0,TILE,TILE);ctx.fillStyle='#76858a';ctx.fillRect(3,4,7,3);ctx.fillRect(14,14,6,4);ctx.fillStyle='#46545a';ctx.fillRect(1,18,8,2);ctx.fillRect(17,5,4,3)}
    else if(id===T.WOOD){ctx.fillStyle='#926136';ctx.fillRect(0,0,TILE,TILE);ctx.fillStyle='#ba7c43';ctx.fillRect(3,0,3,TILE);ctx.fillRect(15,0,2,TILE);ctx.fillStyle='#6d462b';ctx.fillRect(7,8,5,2);ctx.fillRect(17,17,4,2)}
    else if(id>=T.COPPER&&id<=T.OBSIDIAN){drawOre(id,n)}
    else if(id===T.PLANK||id===T.PLATFORM){ctx.fillStyle='#9b6a3e';ctx.fillRect(0,0,TILE,12);ctx.fillStyle='#d19a58';ctx.fillRect(0,0,TILE,3);ctx.fillStyle='#65452d';ctx.fillRect(5,3,2,9);ctx.fillRect(18,3,2,9)}
    else if(id===T.COPPER_BLOCK||id===T.IRON_BLOCK){ctx.fillStyle=id===T.COPPER_BLOCK?'#b76f42':'#778b8f';ctx.fillRect(0,0,TILE,TILE);ctx.fillStyle=id===T.COPPER_BLOCK?'#e1a06b':'#a8b7b7';ctx.fillRect(2,2,20,3);ctx.fillRect(2,2,3,20);ctx.fillStyle='#27373b';ctx.fillRect(3,3,3,3);ctx.fillRect(18,3,3,3);ctx.fillRect(3,18,3,3);ctx.fillRect(18,18,3,3)}
    else if(id===T.WIRE||id===T.SIGNAL){const powered=m&&m.powered;ctx.strokeStyle=id===T.WIRE?(powered?'#fff06a':'#c87842'):(m&&m.active?'#ff8376':'#b63e45');ctx.lineWidth=powered?4:3;ctx.beginPath();ctx.moveTo(0,12);ctx.lineTo(24,12);ctx.moveTo(12,0);ctx.lineTo(12,24);ctx.stroke();ctx.fillStyle=ctx.strokeStyle;ctx.fillRect(9,9,6,6)}
    else if(id===T.GENERATOR){ctx.fillStyle='#2c454a';ctx.fillRect(1,3,22,21);ctx.fillStyle='#d29b3f';ctx.fillRect(4,6,16,12);ctx.fillStyle='#15272d';ctx.fillRect(8,8,8,8);ctx.fillStyle='#fff09c';ctx.fillRect(11,6,3,5);ctx.fillRect(9,10,3,5)}
    else if(id===T.SWITCH){ctx.fillStyle='#273b3e';ctx.fillRect(4,7,16,13);ctx.strokeStyle=m&&m.on?'#95ef75':'#d2cec1';ctx.lineWidth=4;ctx.beginPath();ctx.moveTo(12,13);ctx.lineTo(m&&m.on?18:7,5);ctx.stroke()}
    else if(id===T.SENSOR){ctx.fillStyle='#28444a';ctx.fillRect(3,8,18,12);ctx.strokeStyle=m&&m.active?'#74fff1':'#64918f';ctx.lineWidth=2;ctx.beginPath();ctx.arc(12,12,6,Math.PI,0);ctx.stroke();ctx.fillStyle=m&&m.active?'#d9fff8':'#496b6d';ctx.fillRect(10,10,4,4)}
    else if(id===T.MOTOR){machineBox('#2a6063');ctx.strokeStyle=m&&m.running?'#82fff2':'#8cadac';ctx.lineWidth=3;ctx.beginPath();ctx.arc(12,12,6,0,Math.PI*2);ctx.stroke();ctx.save();ctx.translate(12,12);ctx.rotate((m&&m.running?gameTime*.15:0));ctx.fillStyle=ctx.strokeStyle;ctx.fillRect(-1,-8,2,16);ctx.fillRect(-8,-1,16,2);ctx.restore()}
    else if(id===T.SHAFT){ctx.save();ctx.translate(12,12);ctx.rotate(((m&&m.spin)||0)+((m&&m.rot)||0)*Math.PI/2);ctx.fillStyle=m&&m.running?'#d6f5e9':'#8b9b9c';ctx.fillRect(-12,-3,24,6);ctx.fillStyle='#34474b';for(let q=-8;q<=8;q+=8)ctx.fillRect(q,-5,3,10);ctx.restore()}
    else if(id===T.GEAR){machineBox(m&&m.mode===0?'#6d5940':m&&m.mode===2?'#5d446d':'#455e61');ctx.save();ctx.translate(12,12);ctx.rotate((m&&m.running?gameTime*.12:0));ctx.strokeStyle=m&&m.running?'#ffe68a':'#a9b5ae';ctx.lineWidth=4;ctx.beginPath();ctx.arc(0,0,m&&m.mode===0?7:m&&m.mode===2?4:6,0,Math.PI*2);ctx.stroke();for(let a=0;a<8;a++){ctx.rotate(Math.PI/4);ctx.fillStyle=ctx.strokeStyle;ctx.fillRect(5,-2,6,4)}ctx.restore()}
    else if(id===T.PISTON){ctx.save();ctx.translate(12,12);ctx.rotate(((m&&m.rot)||0)*Math.PI/2);ctx.translate(-12,-12);ctx.fillStyle='#3b5054';ctx.fillRect(2,3,14,18);ctx.fillStyle=m&&m.running?'#82dcd2':'#87989a';ctx.fillRect(15,9,m&&m.extended?9:6,6);ctx.fillStyle='#d4b665';ctx.fillRect(m&&m.extended?20:17,6,4,12);ctx.restore()}
    else if(id===T.LAMP){ctx.fillStyle='#263c41';ctx.fillRect(7,16,10,6);const on=m&&m.powered&&m.signalOK;if(on){const g=ctx.createRadialGradient(12,10,1,12,10,18);g.addColorStop(0,'#fff9b5');g.addColorStop(1,'rgba(255,214,70,0)');ctx.fillStyle=g;ctx.fillRect(-8,-10,40,40)}ctx.fillStyle=on?'#fff172':'#71888a';ctx.fillRect(6,4,12,13);ctx.fillStyle=on?'#fffbd2':'#9bb0ad';ctx.fillRect(9,6,6,8)}
    else if(id===T.CONVEYOR){ctx.fillStyle='#354b50';ctx.fillRect(0,2,24,13);ctx.fillStyle=m&&m.running?'#63d6c7':'#789092';ctx.fillRect(0,0,24,5);ctx.fillStyle='#18292e';for(let q=3;q<24;q+=7)ctx.fillRect(q,7,3,5)}
    else if(id===T.FREIGHT_RAIL){const railTop=CircuitFoundryAutomation.RAIL_TOP*TILE;ctx.fillStyle='#453e39';ctx.fillRect(0,railTop,TILE,TILE-railTop);ctx.fillStyle='#b8b0a2';ctx.fillRect(0,railTop,TILE,2);ctx.fillStyle='#70645a';for(let x=2;x<TILE;x+=8)ctx.fillRect(x,railTop+2,3,3);}
    else if(id===T.WINCH){machineBox('#4b5552');ctx.strokeStyle=m&&m.running?'#f0cf70':'#a1aeaa';ctx.lineWidth=3;ctx.beginPath();ctx.arc(12,12,7,0,Math.PI*2);ctx.stroke();ctx.fillStyle='#33291f';ctx.fillRect(10,10,4,14)}
    else if(id===T.DOOR){ctx.fillStyle='#503724';ctx.fillRect(1,1,22,22);if(m&&m.open){ctx.fillStyle='#b97b43';ctx.fillRect(18,2,4,20);ctx.fillStyle='#f0c36c';ctx.fillRect(18,11,2,2)}else{ctx.fillStyle='#a96f3e';ctx.fillRect(3,2,18,20);ctx.fillStyle='#754925';ctx.fillRect(6,5,11,5);ctx.fillRect(6,13,11,6);ctx.fillStyle='#f0c36c';ctx.fillRect(17,11,2,2)}}
    else if(defs[id]?.ladder){ctx.fillStyle=defs[id].rope?'#d6bd83':'#b57b42';if(id===T.ROPE||id===T.MECHANICAL_ROPE){ctx.fillRect(10,0,3,24);ctx.fillStyle='#8e7854';for(let q=1;q<24;q+=6)ctx.fillRect(9,q,5,2)}else{ctx.fillRect(4,0,3,24);ctx.fillRect(17,0,3,24);ctx.fillStyle=id===T.REINFORCED_LADDER?'#bdcbd1':'#d8a65d';for(let q=3;q<24;q+=6)ctx.fillRect(5,q,14,2)}}
    else if([T.SCAFFOLD,T.METAL_SCAFFOLD,T.BRIDGE].includes(id)){ctx.strokeStyle=id===T.METAL_SCAFFOLD?'#9fbecb':'#b78c5e';ctx.lineWidth=3;ctx.strokeRect(2,3,20,19);ctx.beginPath();ctx.moveTo(2,22);ctx.lineTo(22,4);ctx.stroke();ctx.fillStyle='#d6bb84';ctx.fillRect(0,0,24,4)}
    else if(id===T.COUNTERWEIGHT){ctx.strokeStyle='#c2cfd0';ctx.lineWidth=2;ctx.beginPath();ctx.arc(12,4,3,0,Math.PI*2);ctx.stroke();ctx.fillStyle='#5f727b';ctx.fillRect(2,7,20,16);ctx.fillStyle='#a7b8bc';ctx.fillRect(3,8,18,3);ctx.fillStyle='#293e46';for(let q=5;q<20;q+=5)ctx.fillRect(q,13,3,7);}
    else if(id===T.ANCHOR||id===T.RAIL){ctx.strokeStyle='#a8bac0';ctx.lineWidth=3;ctx.strokeRect(id===T.RAIL?7:4,1,id===T.RAIL?10:16,22)}
    else if(id===T.LEDGE){ctx.fillStyle='#644327';ctx.fillRect(0,2,24,5);ctx.fillStyle='#d5a260';ctx.fillRect(0,0,24,3);ctx.fillStyle='#9b6838';ctx.fillRect(3,7,3,4);ctx.fillRect(18,7,3,4)}
    else if(id===T.CORE){ctx.fillStyle='#172f35';ctx.fillRect(0,0,24,24);ctx.strokeStyle=m&&m.powered?'#fff174':'#5de4df';ctx.lineWidth=2;ctx.strokeRect(3,3,18,18);ctx.save();ctx.translate(12,12);ctx.rotate(gameTime*.04);ctx.fillStyle=m&&m.powered?'#fff5a2':'#5de4df';ctx.fillRect(-3,-9,6,18);ctx.fillRect(-9,-3,18,6);ctx.restore()}
    if(!painted&&id>=26&&defs[id].kind&&id!==T.WORKBENCH){machineBox(E.COLORS[defs[id].kind]||'#8fcab8');ctx.fillStyle='#10212b';ctx.font='bold 12px monospace';ctx.textAlign='center';ctx.fillText(defs[id].icon,12,16);ctx.textAlign='left';if(m?.working||m?.firing){ctx.strokeStyle='#fff3a4';ctx.lineWidth=2;ctx.strokeRect(2,2,20,20)}if(m?.progress){ctx.fillStyle='#8cf0bd';ctx.fillRect(2,20,20*Math.min(1,m.progress),2)}}
    if(m&&m.linked&&lens){ctx.strokeStyle=assemblyColor(m.assembly);ctx.lineWidth=2;ctx.setLineDash([4,3]);ctx.strokeRect(1,1,22,22);ctx.setLineDash([])}if(lens&&m){if(m.powered){ctx.strokeStyle='#fff26a';ctx.lineWidth=2;ctx.strokeRect(2,2,20,20)}if(m.active){ctx.fillStyle='#ff5454';ctx.fillRect(9,1,6,3)}if(m.running){ctx.fillStyle='#5ff1dc';ctx.fillRect(1,19,22,3)}if(m.overload){ctx.fillStyle=gameTime%4<2?'#ff4d35':'#fff';ctx.fillRect(2,2,4,4);ctx.fillRect(18,17,3,3)}}drawEngineeringOverlay(id,m||{},x,y,painted);ctx.restore();
    function machineBox(c){ctx.fillStyle=c;ctx.fillRect(1,2,22,defs[id]?.solid?22:20);ctx.fillStyle='#12272d';ctx.fillRect(3,4,3,3);ctx.fillRect(18,4,3,3);ctx.fillRect(3,17,3,3);ctx.fillRect(18,17,3,3)}
  }
  function drawFoundryTile(id,m,x,y){
    const d=defs[id],rig=d?.foundryRig,atlas=globalThis.CircuitboundFoundryArt;
    if(d?.foundryPart){
      const c=industry?.controller(x,y);
      if(!c||!atlas?.ready?.(c.rig.key))return false;
      if(id===T.DEEP_RIG_COOLANT){
        ctx.fillStyle=c.m.working?'#85e7fc':'#427492';ctx.fillRect(18,8,5,7);
      }
      return true;
    }
    if(!rig)return false;
    return atlas?.draw(ctx,rig.key,-TILE,-TILE,TILE*3,TILE*3,{time:gameTime,working:!!m?.working,grounded:true});
  }
  function drawFoundryOverlays(){
    const focused=inspected&&industry?.describe(inspected.x,inspected.y)?.focus;
    industry?.visible(camera.x/TILE,camera.y/TILE,(camera.x+vw)/TILE,(camera.y+vh)/TILE,(x,y,rig,m)=>{
      const origin=tileRect(x,y),middle=origin.x+TILE/2,top=origin.y-4;
      const color=m.working?'#81eeb9':m.foundryStage>2?'#ffb563':'#e2a48b';
      ctx.save();
      if(m.working&&m.foundryCut){
        const target=tileRect(m.foundryCut.x,m.foundryCut.y);
        const bx=target.x+TILE/2,start=origin.y+TILE,by=start+Math.max(TILE/3,(target.y+7-start)*(m.progress||0));
        ctx.strokeStyle=rig.color;ctx.lineWidth=4;ctx.beginPath();ctx.moveTo(bx,start);ctx.lineTo(bx,by);ctx.stroke();
        ctx.fillStyle='#d8e6d8';ctx.beginPath();ctx.moveTo(bx-5,by-4);ctx.lineTo(bx+5,by-4);ctx.lineTo(bx+Math.cos(gameTime*22)*3,by+5);ctx.fill();
      }
      if(rig.id===T.SEISMIC_STRIP_MINER){
        const hx=middle+(m.foundryHead||0)*TILE;
        ctx.fillStyle=rig.color;ctx.fillRect(hx-7,origin.y-TILE*1.5-2,14,4);
      }
      if(rig.id===T.MANTLE_EXTRACTOR&&m.working&&m.foundryCoolant>0){
        ctx.strokeStyle='#7ed9fa';ctx.lineWidth=2;ctx.setLineDash([3,3]);ctx.lineDashOffset=-gameTime*14;
        ctx.beginPath();ctx.moveTo(middle+TILE*1.5,origin.y-TILE/2);ctx.lineTo(middle,origin.y-TILE/2);ctx.lineTo(middle,origin.y+TILE/2);ctx.stroke();ctx.setLineDash([]);
      }
      ctx.fillStyle='#182328';ctx.fillRect(middle-7,top-5,14,8);ctx.fillStyle=color;ctx.fillRect(middle-2,top-3,4,4);
      if(!m.working&&(lens||distanceToTile(x,y)<4)&&m.foundryFault){
        ctx.font='600 11px system-ui';ctx.textAlign='center';
        const w=ctx.measureText(m.foundryFault).width;
        ctx.fillStyle='#17252bea';ctx.fillRect(middle-w/2-5,top-22,w+10,16);
        ctx.fillStyle=color;ctx.fillText(m.foundryFault,middle,top-10);
      }
      ctx.restore();
    });
    if(focused){
      ctx.save();ctx.strokeStyle=focused.color;ctx.lineWidth=2;
      ctx.setLineDash([4,3]);ctx.lineDashOffset=-gameTime*13;
      for(const [x,y] of focused.cells){const p=tileRect(x,y);ctx.strokeRect(p.x+2,p.y+2,TILE-4,TILE-4);}
      ctx.restore();
    }
  }
  function drawOre(id,n){ctx.fillStyle=id===T.OBSIDIAN?'#29243a':'#515f64';ctx.fillRect(0,0,24,24);const c=id===T.COPPER?'#d27d45':id===T.IRON?'#aab8b7':id===T.CRYSTAL?'#58dfdc':'#5a4b76';ctx.fillStyle=c;for(let i=0;i<4;i++){const x=3+((n*131+i*7)%16),y=3+((n*79+i*11)%16);ctx.fillRect(x,y,id===T.CRYSTAL?3:5,id===T.CRYSTAL?8:4)}}
  function drawWorld(){
    const x0=Math.max(0,Math.floor(camera.x/TILE)-1),x1=Math.min(WORLD_W-1,Math.ceil((camera.x+vw)/TILE)+1);
    const y0=Math.max(0,Math.floor(camera.y/TILE)-1),y1=Math.min(WORLD_H-1,Math.ceil((camera.y+vh)/TILE)+1);
    for(let y=y0;y<=y1;y++)for(let x=x0;x<=x1;x++){
      const id=get(x,y);if(id)drawTile(id,x,y,meta.get(idx(x,y)));
    }
    for(const key of tileMotions.keys()){
      const x=key%WORLD_W,y=Math.floor(key/WORLD_W);
      if(x<x0||x>x1||y<y0||y>y1)drawTile(tileByIndex(key),x,y,meta.get(key));
    }
    if(linkDrag){const a=tileRect(Math.min(linkDrag.x0,linkDrag.x1),Math.min(linkDrag.y0,linkDrag.y1)),w=(Math.abs(linkDrag.x1-linkDrag.x0)+1)*TILE,h=(Math.abs(linkDrag.y1-linkDrag.y0)+1)*TILE;ctx.fillStyle='rgba(72,255,223,.13)';ctx.fillRect(a.x,a.y,w,h);ctx.strokeStyle='#6affdf';ctx.lineWidth=2;ctx.strokeRect(a.x,a.y,w,h)}if(mining.key){const [x,y]=mining.key.split(',').map(Number),p=tileRect(x,y),id=get(x,y),hard=defs[id]?.hard||1,progress=Math.min(1,mining.progress/hard);ctx.strokeStyle='#fff1a4';ctx.lineWidth=3;ctx.strokeRect(p.x+2,p.y+2,20,20);ctx.fillStyle='#ffcf52';ctx.fillRect(p.x+2,p.y+18,20*progress,3)}}
  function drawPlayer(){
    const left=riderMotion?motionRemaining(riderMotion.start):0;
    const x=Math.round(player.x-camera.x+(riderMotion?.dx||0)*left),y=Math.round(player.y-camera.y+(riderMotion?.dy||0)*left),step=Math.sin(player.walk)*3;
    const target=mining.key?.split(',').map(Number);
    const miningProgress=target?.length===2?Math.min(1,mining.progress/(defs[get(...target)]?.hard||1)):0;
    if(art?.player(ctx,player,x,y,{time:gameTime,mining:!!mining.key,miningProgress,climbing:!!player.climbing,attack:survival?.attackAnimation||0,weapon:selectedItem()?.weapon||survival?.attackAnimation?survival?.weapon:null,attackAngle:survival?.beam?.angle??survival?.attackAngle??0,charging:!!(survival?.beam?.charge||survival?.foundryCharge)}))return;
    ctx.save();ctx.translate(x+player.w/2,y);ctx.scale(player.facing,1);ctx.fillStyle='#1b2d32';ctx.fillRect(-8,2,16,11);ctx.fillStyle='#e3bd83';ctx.fillRect(-6,7,12,11);ctx.fillStyle='#e5ad4f';ctx.fillRect(-8,0,16,7);ctx.fillStyle='#f9d46a';ctx.fillRect(-6,-3,12,4);ctx.fillStyle='#112126';ctx.fillRect(2,9,2,3);ctx.fillStyle='#327a79';ctx.fillRect(-7,18,14,10);ctx.fillStyle='#c9873d';ctx.fillRect(-9,19,3,12);ctx.fillStyle='#243d47';ctx.fillRect(-6,28,5,7+step);ctx.fillRect(1,28,5,7-step);ctx.restore()}
  function drawTarget(){
    if(modalOpen||inspected||!pointer.aimed||blueprint||selectedAction()==='build'||guides?.isPlacing())return;
    const [x,y]=selectedAction()==='mine'?miningTarget():[pointer.tx,pointer.ty];
    const p=tileRect(x,y),reachable=inside(x,y)&&distanceToTile(x,y)<=6;
    ctx.strokeStyle=reachable?'#fff5b4de':'#ff5a46b3';ctx.lineWidth=1;
    ctx.strokeRect(Math.floor(p.x)+.5,Math.floor(p.y)+.5,TILE-1,TILE-1);
    if(selectedAction()==='mine'&&reachable&&get(x,y)){
      const m=meta.get(idx(x,y));
      if(m?.assembly||Object.values(m?.contents||{}).some(Boolean)||Object.values(m?.outputs||{}).some(Boolean)){
        ctx.fillStyle='#ffd68b';ctx.font='bold 11px system-ui';
        ctx.fillText(m.assembly?'Linked assembly':'Contains cargo',p.x+TILE+7,p.y+TILE/2);
      }
    }
  }
  function render(){
    for(const [key,motion] of tileMotions)if(!motionRemaining(motion.start))tileMotions.delete(key);
    if(riderMotion&&!motionRemaining(riderMotion.start))riderMotion=null;
    const depth=(camera.y+vh*.55)/TILE;
    const region=globalThis.CircuitDeepWorld?.regionAt(depth);
    const freightYard=depth<46&&player.x/TILE>=220&&world?.sites?.some(site=>site.kind==='surface'&&Math.abs(site.x-player.x/TILE)<27);
    if(!deepArt?.drawRegion(ctx,region?.start>=84?region.id:freightYard?'freightYard':'',0,0,vw,vh)&&!art?.scenery(ctx,camera,vw,vh,TILE,gameTime)){drawSky();drawUnderground();}
    drawWorld();drawFoundryOverlays();drawMiningCracks();drawDrops();robotics?.draw(ctx,camera,vw,vh,TILE,gameTime,art);
    fleet?.draw(ctx,camera,vw,vh,TILE,gameTime,(context,key,x,y,size,facing,phase,grounded)=>
      globalThis.CircuitboundFoundryArt?.draw(context,key,x-size/2,y-size/2,size,size,
        {time:gameTime,facing,phase,grounded,working:phase!=='Ready'&&phase!=='Waiting'&&!phase.startsWith('Docked')}));
    survival?.draw(ctx,camera,vw,vh);starship?.draw(ctx,camera,vw,vh,art,gameTime);
    if(!starship?.active)drawPlayer();
    if(!modalOpen){
      guides?.draw(ctx,tileRect,pointer);
      const info=drawBuildPreview();drawTarget();updateAimLabel(info);
    }
    drawParticles();if(camera.shake>0)camera.shake*=.88;
  }
  function drawMiningCracks(){if(!mining.key)return;const [x,y]=mining.key.split(',').map(Number),p=tileRect(x,y),fraction=Math.min(1,mining.progress/(defs[get(x,y)]?.hard||1));ctx.strokeStyle='#10252ce0';ctx.lineWidth=1.5;ctx.beginPath();ctx.moveTo(p.x+4,p.y+3);ctx.lineTo(p.x+12,p.y+10);ctx.lineTo(p.x+9,p.y+17);if(fraction>.25){ctx.moveTo(p.x+12,p.y+10);ctx.lineTo(p.x+19,p.y+6);}if(fraction>.5){ctx.moveTo(p.x+9,p.y+17);ctx.lineTo(p.x+17,p.y+22);ctx.moveTo(p.x+10,p.y+12);ctx.lineTo(p.x+2,p.y+18);}if(fraction>.75){ctx.moveTo(p.x+12,p.y+10);ctx.lineTo(p.x+22,p.y+16);}ctx.stroke();}
  function drawUnderground(){const y0=Math.max(0,Math.floor(camera.y/TILE)),y1=Math.min(WORLD_H,Math.ceil((camera.y+vh)/TILE));for(let y=y0;y<y1;y++){if(y<33)continue;ctx.fillStyle=y<44?'#172b31':y<55?'#1d2934':y<66?'#1f233a':y<75?'#24232c':'#2b1825';ctx.fillRect(0,y*TILE-camera.y,vw,TILE);ctx.fillStyle='#ffffff08';for(let x=Math.max(0,Math.floor(camera.x/TILE));x<Math.min(WORLD_W,(camera.x+vw)/TILE);x++)if(hash(x,y)>.85)ctx.fillRect(x*TILE-camera.x+4,y*TILE-camera.y+5,9,2);}}
  function burst(x,y,color){for(let i=0;i<8&&particles.length<96;i++)particles.push({x,y,vx:(Math.random()-.5)*100,vy:-30-Math.random()*80,life:.6,color});}
  function drawParticles(){for(const p of particles){ctx.fillStyle=p.color;ctx.globalAlpha=Math.max(0,p.life/.6);ctx.fillRect(p.x-camera.x,p.y-camera.y,3,3)}ctx.globalAlpha=1;}
  function updateParticles(dt){for(const p of particles){p.life-=dt;p.x+=p.vx*dt;p.y+=p.vy*dt;p.vy+=180*dt;}for(let i=particles.length-1;i>=0;i--)if(particles[i].life<=0)particles.splice(i,1);}
  function drawBuildPreview(){
    const item=selectedItem();
    if(!pointer.aimed||inspected||guides?.isPlacing()&&!blueprint)return null;
    const drag=buildDrag||touchLine;
    const rigPlan=!blueprint&&defs[item?.tile]?.foundryRig?industry.placement(pointer.tx,pointer.ty,item):null;
    const plan=blueprint?blueprintCells(blueprint,pointer.tx,pointer.ty).map(p=>({...p,reason:!placementValid(p.x,p.y,p.id)?'Blocked or out of reach.':!blueprintStatus(blueprint).canBuild?'Missing components.':null})):
      rigPlan?.cells||(item?.tile>=0?placementPlan(drag?lineCells(drag,item):[[pointer.tx,pointer.ty]],item).cells:[]);
    let paintedRig=false;
    if(rigPlan){
      const origin=tileRect(pointer.tx-1,pointer.ty-1);
      ctx.save();ctx.globalAlpha=.62;
      paintedRig=globalThis.CircuitboundFoundryArt?.draw(ctx,item.key,origin.x,origin.y,TILE*3,TILE*3,{time:gameTime,grounded:true});
      ctx.restore();
    }
    for(const cell of plan){
      if(!defs[cell.id])continue;
      const r=tileRect(cell.x,cell.y),valid=!cell.reason&&!(rigPlan&&!(inventory[item.key]>0));
      ctx.save();ctx.translate(r.x,r.y);ctx.globalAlpha=valid?.67:.35;
      if(!paintedRig&&!art?.tile(ctx,cell.id,{rot:cell.rot},gameTime)){
        ctx.fillStyle=item?.color||'#8bd7bb';ctx.fillRect(3,3,18,18);
      }
      ctx.restore();
      ctx.fillStyle=valid?'#74edc027':'#ff676747';ctx.fillRect(r.x,r.y,TILE,TILE);
      ctx.strokeStyle=valid?'#7ff5cd':'#ff7979';ctx.lineWidth=1;ctx.strokeRect(r.x+1,r.y+1,22,22);
      if(valid)drawProposedPorts(cell,r);
    }
    if(blueprint||!plan.length)return null;
    if(rigPlan)return {label:rigPlan.reason||'1 '+item.name+' · 3×3 footprint',x:pointer.tx,y:pointer.ty};
    const count=plan.filter(p=>!p.reason).length,reason=plan.find(p=>p.reason)?.reason,first=plan[0],last=plan[Math.max(0,count-1)];
    const direction=E.ports(defs[item.tile],{rot:first.rot}).length?' · '+directions[first.rot]:'';
    const cost=count?count+' '+item.name+direction:'';
    return {label:[cost,reason].filter(Boolean).join(' · '),x:last.x,y:last.y};
  }
  function drawProposedPorts(cell,r){
    const ports=E.ports(defs[cell.id],{rot:cell.rot||0});
    for(const p of ports){
      const [dx,dy]=E.DIRS[p.side],x=cell.x+dx,y=cell.y+dy;
      if(!inside(x,y))continue;
      const matching=E.ports(defs[get(x,y)],meta.get(idx(x,y))||{}).find(other=>other.side===(p.side+2)%4&&(E.compatible(p,other)||E.compatible(other,p)));
      if(!matching)continue;
      const startX=r.x+12,startY=r.y+12,endX=startX+dx*20,endY=startY+dy*20;
      const sign=E.compatible(p,matching)?1:-1,ax=startX+dx*15,ay=startY+dy*15;
      ctx.strokeStyle=ctx.fillStyle=E.COLORS[p.type];ctx.lineWidth=2;ctx.beginPath();ctx.moveTo(startX,startY);ctx.lineTo(endX,endY);ctx.stroke();
      if(p.mode==='both'&&matching.mode==='both')ctx.fillRect(ax-3,ay-3,6,6);
      else{ctx.beginPath();ctx.moveTo(ax+dx*sign*4,ay+dy*sign*4);ctx.lineTo(ax-dx*sign*3+dy*4,ay-dy*sign*3-dx*4);ctx.lineTo(ax-dx*sign*3-dy*4,ay-dy*sign*3+dx*4);ctx.fill();}
    }
  }
  function updateAimLabel(info){
    const el=document.getElementById('cursorLabel');
    if(inspected||!pointer.aimed){el.style.display='none';return;}
    const ground=aimedGroundDrop();
    if(ground)info={label:'Pick up '+ground.count+' '+itemLabel(ground.key)+' · '+(touchAim?'Use':'F'),x:pointer.tx,y:pointer.ty};
    if(!info){
      const {tx:x,ty:y}=pointer,id=get(x,y),d=defs[id];
      const deepRig=(d?.foundryRig||d?.foundryPart)&&distanceToTile(x,y)<=6?industry.controller(x,y):null;
      if(deepRig)info={label:deepRig.m.foundryFault||deepRig.rig.name,x,y};
      else if(inside(x,y)&&id&&d&&(lens||distanceToTile(x,y)<=6&&(d.door||id===T.WORKBENCH||d.transport)))
        info={label:d.name+(lens?'':' · '+(touchAim?'Use':'Right-click')),x,y};
    }
    if(!info?.label){el.style.display='none';return;}
    const target=tileRect(info.x,info.y);
    el.style.display='block';
    if(el.textContent!==info.label)el.textContent=info.label;
    el.style.left=Math.max(7,Math.min(vw-190,target.x+25))+'px';el.style.top=Math.max(9,target.y-18)+'px';
  }
  function blueprintCells(plan,x,y){return (plan.placements||[]).map(p=>({...p,x:x+p.x,y:y+p.y,id:items.find(i=>i.key===p.key)?.tile}));}
  function blueprintStatus(plan){const missing=Object.entries(plan.cost||{}).filter(([key,n])=>(inventory[key]||0)<n);return {canBuild:missing.length===0,reason:missing.length?'Missing '+missing.map(([k,n])=>`${n-(inventory[k]||0)} ${k}`).join(', '):'Aim at clear space within reach. Every component uses your inventory.'};}
  function placeBlueprint(){if(!blueprint)return false;const status=blueprintStatus(blueprint);if(!status.canBuild){showToast(status.reason);return false}const cells=blueprintCells(blueprint,pointer.tx,pointer.ty);if(cells.some(p=>p.id<0||!placementValid(p.x,p.y,p.id))){showToast('Blueprint blocked or out of reach. Clear terrain and stand near its center.');return false}
    for(const [key,n] of Object.entries(blueprint.cost))inventory[key]-=n;
    for(const p of cells)set(p.x,p.y,p.id,{...defaultMeta(p.id),rot:p.rot||0,...p.meta,placedItem:p.key});
    for(const group of blueprint.links||[])engine.link(group.map(([x,y])=>idx(pointer.tx+x,pointer.ty+y)));
    stats.built=(stats.built||0)+cells.length;for(const p of cells)guides?.placed(p.key,p.x,p.y);showToast(`${blueprint.name} built. Inspect its ports to finish connections and supply materials.`);blueprint=null;renderHotbar();simulateNetworks();saveGame();return true;
  }
  function lineOfSight(x1,y1,x2,y2){const n=Math.ceil(Math.hypot(x2-x1,y2-y1)*4),sx=Math.floor(x1),sy=Math.floor(y1);for(let i=1;i<n;i++){const x=Math.floor(x1+(x2-x1)*i/n),y=Math.floor(y1+(y2-y1)*i/n);if(x===sx&&y===sy)continue;const id=get(x,y);if(defs[id]?.solid&&!defs[id]?.platform&&!meta.get(idx(x,y))?.open)return false;}return true;}
  function unlockAudio(){if(audioContext)return;const AC=globalThis.AudioContext||globalThis.webkitAudioContext;if(!AC)return;try{audioContext=new AC();musicGain=audioContext.createGain();musicGain.gain.value=settings.musicVolume*.025;musicGain.connect(audioContext.destination);musicNode=audioContext.createOscillator();musicNode.type='sine';musicNode.frequency.value=82.4;musicNode.connect(musicGain);musicNode.start();}catch{audioContext=null;}}
  function sound(kind){if(!audioContext||settings.soundVolume<=0||activeSounds>=8)return;try{const o=audioContext.createOscillator(),g=audioContext.createGain(),now=audioContext.currentTime;o.type=['copper','discovery'].includes(kind)?'triangle':'sine';o.frequency.setValueAtTime(({copper:520,iron:270,crystal:880,stone:140,wood:190,dirt:110,obsidian:95,craft:660,build:220,warning:420,failure:90,discovery:1040})[kind]||170,now);o.frequency.exponentialRampToValueAtTime(70,now+.12);g.gain.setValueAtTime(settings.soundVolume*.07,now);g.gain.exponentialRampToValueAtTime(.0001,now+.15);o.connect(g);g.connect(audioContext.destination);activeSounds++;o.onended=()=>{o.disconnect();g.disconnect();activeSounds=Math.max(0,activeSounds-1);};o.start(now);o.stop(now+.16);}catch{}}

  function rectCollides(x,y,w,h,dy=0){
    const left=Math.floor(x/TILE),right=Math.floor((x+w-.01)/TILE),top=Math.floor(y/TILE),bottom=Math.floor((y+h-.01)/TILE);
    for(let ty=top;ty<=bottom;ty++)for(let tx=left;tx<=right;tx++){
      const id=get(tx,ty),d=defs[id],m=meta.get(idx(tx,ty));
      if(d&&d.solid){
        if(d.door&&m?.open)continue;
        if(d.platform&&dy<0)continue;
        if(d.platform&&ledgeDrop>0)continue;
        if(d.platform&&y+h-dy>ty*TILE+5)continue;
        return true;
      }
    }
    return false;
  }
  function playerTouchesLadder(){
    const tx=Math.floor((player.x+player.w/2)/TILE),top=Math.floor((player.y+3)/TILE),bottom=Math.floor((player.y+player.h-3)/TILE);
    for(let ty=top;ty<=bottom;ty++){const id=get(tx,ty);if(defs[id]?.ladder&&supportedRope(tx,ty,id))return true;}
    return false;
  }
  function standingOnPlatform(){
    const ty=Math.floor((player.y+player.h+2)/TILE),left=Math.floor((player.x+2)/TILE),right=Math.floor((player.x+player.w-3)/TILE);
    for(let tx=left;tx<=right;tx++)if(defs[get(tx,ty)]?.platform)return true;
    return false;
  }
  function movePlayer(dt){
    const left=keys.KeyA||keys.ArrowLeft||keys.touchleft,right=keys.KeyD||keys.ArrowRight||keys.touchright;
    const up=keys.KeyW||keys.ArrowUp||keys.touchjump,down=keys.KeyS||keys.ArrowDown||keys.touchdown;
    const dir=(left?-1:0)+(right?1:0),onLadder=ropeDismount<=0&&playerTouchesLadder(),accel=player.onGround?1150:650;
    player.climbing=onLadder&&!player.onGround;
    ropeDismount=Math.max(0,ropeDismount-dt);
    ledgeDrop=Math.max(0,ledgeDrop-dt);
    if(down&&player.onGround&&!onLadder&&standingOnPlatform()){ledgeDrop=.22;player.onGround=false;player.y+=3;player.vy=Math.max(player.vy,70)}
    if(onLadder&&down)ledgeDrop=Math.max(ledgeDrop,.12);
    player.vx+=dir*accel*dt;player.vx*=Math.pow(player.onGround?0.0008:0.04,dt);player.vx=Math.max(-180,Math.min(180,player.vx));
    if(dir){player.facing=dir;player.walk+=dt*10}
    if(onLadder){
      if(keys.Space){player.vy=-270;player.onGround=false;keys.Space=false;ropeDismount=.3}
      else if(up||down){player.vy=(down?1:-1)*125;player.onGround=false}
      else{player.vy*=Math.pow(.002,dt);if(Math.abs(player.vy)<3)player.vy=0}
    }else{
      if((keys.Space||keys.KeyW||keys.ArrowUp||keys.touchjump)&&player.onGround){player.vy=-335;player.onGround=false;keys.Space=keys.KeyW=keys.ArrowUp=keys.touchjump=false}
      const gravity=regionExposure()?.region==='gravityHollows'?680:920;
      player.vy=Math.min(570,player.vy+gravity*dt);
    }
    let nx=player.x+player.vx*dt;
    if(!rectCollides(nx,player.y,player.w,player.h,0))player.x=nx;else{const step=Math.sign(player.vx);for(let i=0;i<Math.abs(player.vx*dt);i++)if(!rectCollides(player.x+step,player.y,player.w,player.h,0))player.x+=step;else break;player.vx=0}
    player.onGround=false;let ny=player.y+player.vy*dt;
    if(!rectCollides(player.x,ny,player.w,player.h,player.vy*dt))player.y=ny;else{const step=Math.sign(player.vy);for(let i=0;i<Math.ceil(Math.abs(player.vy*dt));i++)if(!rectCollides(player.x,player.y+step,player.w,player.h,step))player.y+=step;else break;if(player.vy>0){player.onGround=true;const overlap=(player.y+player.h)%TILE;if(overlap>0&&overlap<1.1)player.y-=overlap;}player.vy=0}
    if(player.y>WORLD_H*TILE){player.health-=10;if(survival){if(player.health<=0)survival.respawn();else Object.assign(player,survival.safeCheckpoint(),{vx:0,vy:0});}else{player.x=9*TILE;player.y=(groundAt(9)-4)*TILE;player.vy=0;}showToast('Recovered at your base checkpoint')}
    player.x=Math.max(TILE,Math.min(WORLD_W*TILE-player.w-TILE,player.x));
    if(Math.abs(player.x/TILE-205)<4&&!stats.coreFound){stats.coreFound=true;showToast('Foundry Core found — connect a 6-power grid')}
    applyConveyors(dt);followPlayer(dt);
  }
  function followPlayer(dt){camera.x+=(player.x+player.w/2-vw*.5-camera.x)*Math.min(1,dt*6);camera.y+=(player.y+player.h/2-vh*.55-camera.y)*Math.min(1,dt*5);camera.x=Math.max(0,Math.min(WORLD_W*TILE-vw,camera.x));camera.y=Math.max(0,Math.min(WORLD_H*TILE-vh,camera.y))
  }
  function applyConveyors(dt){const footX=Math.floor((player.x+player.w/2)/TILE),footY=Math.floor((player.y+player.h+2)/TILE),m=meta.get(idx(footX,footY));if(get(footX,footY)===T.CONVEYOR&&m&&m.running){const nx=player.x+((m.rot||0)===2?-1:1)*Math.abs(m.speed||1)*75*dt;if(!rectCollides(nx,player.y,player.w,player.h))player.x=nx}}
  function distanceToTile(x,y){return Math.hypot(x+.5-(player.x+player.w/2)/TILE,y+.5-(player.y+player.h/2)/TILE)}
  const directions=['right','down','left','up'];
  function selectedItem(){return items[selected];}
  function selectedAction(){
    const item=selectedItem();
    if(item?.tool==='mining')return 'mine';
    if(item?.weapon)return 'combat';
    if(item?.key==='link'||item?.tile>=0)return 'build';
    return 'interact';
  }
  function targetAt(x,y){
    pointer.tx=x;pointer.ty=y;
    pointer.wx=(x+.5)*TILE;pointer.wy=(y+.5)*TILE;
    pointer.aimed=true;
  }
  function updatePointer(e){
    const r=canvas.getBoundingClientRect();
    pointer.x=(e.clientX-r.left)*vw/r.width;pointer.y=(e.clientY-r.top)*vh/r.height;
    const wx=pointer.x+camera.x,wy=pointer.y+camera.y;
    targetAt(Math.floor(wx/TILE),Math.floor(wy/TILE));
    if(e.pointerType!=='touch'){pointer.wx=wx;pointer.wy=wy;touchAim=null;}
  }
  function miningTarget(){
    const {tx:x,ty:y}=pointer;
    if(!settings.smartCursor||get(x,y)!==T.AIR)return [x,y];
    const candidates=[];
    for(let dy=-2;dy<=2;dy++)for(let dx=-2;dx<=2;dx++){
      const tx=x+dx,ty=y+dy,id=get(tx,ty);
      if(inside(tx,ty)&&id&&id!==T.CORE&&distanceToTile(tx,ty)<=6)
        candidates.push({x:tx,y:ty,score:Math.hypot(dx,dy)+distanceToTile(tx,ty)*.08});
    }
    candidates.sort((a,b)=>a.score-b.score);
    return candidates.length?[candidates[0].x,candidates[0].y]:[x,y];
  }
  function makeDrag(){return {x0:pointer.tx,y0:pointer.ty,x1:pointer.tx,y1:pointer.ty};}
  function doPrimary(touch=false){
    if(!pointer.aimed||inspected||starship?.active)return;
    if(fleet?.selectTarget(pointer.tx,pointer.ty))return;
    if(blueprint){placeBlueprint();return;}
    if(guides?.act(pointer.tx,pointer.ty))return;
    const item=selectedItem(),action=selectedAction();
    if(action==='mine'){pointer.down=true;pointer.button=0;return;}
    if(action==='combat'){
      survival?.selectWeapon(item.key);
      if(survival?.attack(pointer.wx,pointer.wy))sound('iron');
      return;
    }
    if(item?.key==='link'){
      if(distanceToTile(pointer.tx,pointer.ty)>=9)return showToast('Aim at blocks within reach.');
      if(!touch||!linkDrag){linkDrag=makeDrag();if(touch)showToast('Aim at the opposite corner, then tap Link.');}
      else{Object.assign(linkDrag,{x1:pointer.tx,y1:pointer.ty});finishLink();linkDrag=null;}
      return;
    }
    if(action==='build'){
      if(touch&&touchLine){placeLine(touchLine);touchLine=null;return;}
      if(!touch&&isLinePart(item.tile)){buildDrag=makeDrag();return;}
      placeAtAim();
    }
  }
  canvas.addEventListener('pointermove',e=>{
    if(modalOpen||(activePointer!==null&&activePointer!==e.pointerId))return;
    if(e.pointerType==='touch'&&!touchStart)return;
    updatePointer(e);
    if(e.pointerType==='touch')touchAim=[pointer.tx,pointer.ty];
    if(linkDrag&&e.pointerType!=='touch'){linkDrag.x1=pointer.tx;linkDrag.y1=pointer.ty;}
    if(buildDrag){buildDrag.x1=pointer.tx;buildDrag.y1=pointer.ty;}
    if(touchStart&&isLinePart(selectedItem()?.tile)&&!blueprint&&!guides?.isPlacing())
      touchLine={...touchStart,x1:pointer.tx,y1:pointer.ty};
  });
  canvas.addEventListener('pointerdown',e=>{
    if(modalOpen||activePointer!==null||![0,2].includes(e.button))return;
    updatePointer(e);unlockAudio();
    if(e.pointerType==='touch'){
      activePointer=e.pointerId;canvas.setPointerCapture?.(e.pointerId);
      touchAim=[pointer.tx,pointer.ty];touchStart=makeDrag();touchLine=null;return;
    }
    if(e.button===2){e.preventDefault();interactAtPointer();return;}
    activePointer=e.pointerId;canvas.setPointerCapture?.(e.pointerId);
    const item=selectedItem();
    if(blueprint||guides?.isPlacing()||item?.tile>=0&&!isLinePart(item.tile))
      pendingClick=[pointer.tx,pointer.ty];
    else doPrimary();
  });
  function releasePointer(commit=true){
    pointer.down=false;
    const captured=activePointer;
    activePointer=null;
    if(captured!==null&&canvas.hasPointerCapture?.(captured))canvas.releasePointerCapture(captured);
    if(touchStart){
      if(!commit)touchLine=null;
      if(touchLine&&touchLine.x0===touchLine.x1&&touchLine.y0===touchLine.y1)touchLine=null;
      touchStart=null;
    }else{
      if(commit&&pendingClick?.[0]===pointer.tx&&pendingClick?.[1]===pointer.ty)doPrimary();
      if(linkDrag&&commit)finishLink();
      linkDrag=null;
      if(buildDrag&&commit)placeLine(buildDrag);
    }
    pendingClick=null;buildDrag=null;mining.key='';mining.progress=0;
  }
  function clearActions(){
    releasePointer(false);touchLine=null;linkDrag=null;touchConfirm=null;
    survival?.cancelCharge?.();
    for(const k of Object.keys(keys))keys[k]=false;
  }
  canvas.addEventListener('pointerup',e=>{
    if(activePointer!==e.pointerId)return;
    updatePointer(e);
    if(buildDrag){buildDrag.x1=pointer.tx;buildDrag.y1=pointer.ty;}
    if(linkDrag&&!touchStart){linkDrag.x1=pointer.tx;linkDrag.y1=pointer.ty;}
    if(touchStart)touchAim=[pointer.tx,pointer.ty];
    releasePointer();
  });
  for(const name of ['pointercancel','lostpointercapture'])canvas.addEventListener(name,e=>{
    if(activePointer!==e.pointerId)return;
    releasePointer(false);
    if(name==='pointercancel')survival?.cancelCharge?.();
  });
  canvas.addEventListener('contextmenu',e=>e.preventDefault());
  function isLinePart(id){
    return defs[id]?.ladder||defs[id]?.platform||[T.DIRT,T.STONE,T.COPPER_BLOCK,T.IRON_BLOCK,T.FRAME,T.BEAM,T.WIRE,T.SIGNAL,T.RAIL,T.SUPER_CABLE,T.FIBER_WIRE].includes(id)||defs[id]?.lineBuild;
  }
  function lineCells(drag,item){
    const id=item.tile,dx=drag.x1-drag.x0,dy=drag.y1-drag.y0;
    const vertical=defs[id]?.ladder||defs[id]?.rail||Math.abs(dy)>Math.abs(dx);
    const cells=[],amount=Math.min(50,Math.abs(vertical?dy:dx));
    for(let i=0;i<=amount;i++)cells.push([drag.x0+(vertical?0:Math.sign(dx)*i),drag.y0+(vertical?Math.sign(dy)*i:0)]);
    return cells;
  }
  function supportedRope(x,y,id,planned){
    if(!defs[id]?.rope)return true;
    for(let yy=y-1;yy>=Math.max(0,y-60);yy--){
      const above=planned?.get(idx(x,yy))?.id??get(x,yy);
      if(defs[above]?.rope)continue;
      return !!(defs[above]?.solid||defs[above]?.anchor||defs[above]?.ladder||[T.WINCH,T.BEAM].includes(above));
    }
    return false;
  }
  function placementBlock(x,y,item,planned){
    const id=item?.tile;
    if(id==null||id<0)return 'Choose a building part.';
    if(!inside(x,y)||distanceToTile(x,y)>6)return 'Out of reach.';
    if(starship?.hitTest(x,y))return 'Tile occupied by the starship.';
    if(get(x,y)!==T.AIR||planned?.has(idx(x,y)))return 'Tile occupied.';
    if(defs[id]?.solid&&playerOverlapsTile(x,y))return 'Move clear of the tile.';
    if(!supportedRope(x,y,id,planned))return 'Rope needs support above.';
    return null;
  }
  function placementValid(x,y,id){return !placementBlock(x,y,{tile:id});}
  function placementRotation(x,y,item){
    return smartPlacement&&!manualRotation?engine.smartRotation(x,y,item.tile,rotation):rotation;
  }
  function placementPlan(cells,item){
    const foundry=industry?.placement(cells[0]?.[0],cells[0]?.[1],item);
    if(foundry)return foundry;
    const plan=[],placed=new Map();
    let count=0,reason=null;
    for(const [x,y] of cells){
      const rot=placementRotation(x,y,item);
      reason=reason||placementBlock(x,y,item,placed)||((inventory[item.key]||0)<=count?'Out of '+item.name+'.':null);
      const cell={x,y,id:item.tile,rot,reason};
      plan.push(cell);
      if(!reason){placed.set(idx(x,y),cell);count++;}
    }
    return {item,cells:plan,count,reason};
  }
  function commitPlacement(x,y,item,rot,extra={}){
    if(placementBlock(x,y,item)||(inventory[item.key]||0)<=0)return false;
    set(x,y,item.tile,{...defaultMeta(item.tile),rot,...extra,placedItem:item.key});
    inventory[item.key]--;stats.built=(stats.built||0)+1;
    if(defs[item.tile]?.ladder)stats.laddersBuilt=(stats.laddersBuilt||0)+1;
    guides?.placed(item.key,x,y);sound('build');return true;
  }
  function placeLine(drag){
    const item=selectedItem();if(!item||item.tile<0)return;
    const plan=placementPlan(lineCells(drag,item),item);
    let count=0;
    for(const cell of plan.cells){if(cell.reason||!commitPlacement(cell.x,cell.y,item,cell.rot))break;count++;}
    if(count){renderHotbar();simulateNetworks();}
    showToast(count?[count+' '+(count===1?'tile':'tiles')+' placed',plan.reason].filter(Boolean).join(' · '):plan.reason||'Line blocked.');
  }
  function placeAtAim(){
    const item=selectedItem();if(!item||item.tile<0)return;
    if(defs[item.tile].foundryRig){
      if(industry.place(pointer.tx,pointer.ty,item)){sound('build');renderHotbar();simulateNetworks();industry.tick();saveGame();}
      return;
    }
    const {tx:x,ty:y}=pointer,plan=placementPlan([[x,y]],item),cell=plan.cells[0];
    if(plan.reason)return showToast(plan.reason);
    if(commitPlacement(x,y,item,cell.rot)){renderHotbar();simulateNetworks();}
  }
  function interactAtPointer(){
    const {tx:x,ty:y}=pointer,id=get(x,y),k=idx(x,y),d=defs[id];
    if(starship&&(starship.active||starship.hitTest(x,y))){starship.interact(x,y);clearActions();return;}
    if(fleet?.selectTarget(x,y)||pickUpAimedDrop()||fleet?.recoverNearby(x,y))return;
    if(!inside(x,y)||distanceToTile(x,y)>6)return showToast('Move within reach.');
    if(id===T.WORKBENCH){
      guides?.benchOpened();
      workshop?.open('craft','workbench');
      return;
    }
    if(id===T.DOOR||id===T.SWITCH){configureComponent(id,k,meta.get(k)||{});return;}
    if(id&&d&&(d.foundryPart||d.ports?.length||d.transport||d.producer||d.mechanical||d.electric||d.signal)){
      inspected={x,y,id,k,node:meta.get(k)?.nodeId};
      document.getElementById('lensPanel').classList.add('show');
      clearActions();updateInspector();document.getElementById('inspectorClose')?.focus({preventScroll:true});return;
    }
    showToast('Aim at a nearby machine, door, or switch.');
  }
  function defaultMeta(id){const m={rot:rotation,on:false,powered:false,active:false,running:false,linked:false};if(id===T.GEAR)m.mode=1;if(id===T.DOOR)m.open=false;return m}
  function playerOverlapsTile(x,y){return player.x<x*TILE+TILE&&player.x+player.w>x*TILE&&player.y<y*TILE+TILE&&player.y+player.h>y*TILE}
  function mineStep(dt){
    if(!pointer.down||pointer.button!==0||modalOpen||inspected||starship?.active||selectedAction()!=='mine')return;
    const [x,y]=miningTarget(),id=get(x,y);
    if(!inside(x,y)||distanceToTile(x,y)>6||!id||id===T.CORE){mining.key='';mining.progress=0;return;}
    const requirement=defs[id].miningTier||0;
    const carriedTool=inventory.poweredMiningTool&&survival?.energy>1?3:inventory.steelDrillTool?2:inventory.reinforcedPickaxe?1:0;
    const deepOre=defs[id].deepOre?CircuitDeepIndustry.rigs.find(rig=>rig.ore.id===id):null;
    const issue=fleet?.canDismantle(x,y)||robotics?.canDismantle(x,y)||starship?.canDismantle(x,y)||
      (deepOre?'Extract this seam with a '+deepOre.name+'.':null)||
      (requirement>carriedTool?'Needs '+(['','a reinforced pickaxe','a steel drill tool','a charged powered mining tool','a Plasma Excavator'][requirement]||'stronger equipment')+'.':null);
    if(issue){showToast(issue);pointer.down=false;mining.key='';mining.progress=0;return;}
    const k=x+','+y;if(mining.key!==k){mining.key=k;mining.progress=0}
    const multiplier=survival?.miningMultiplier(id)||1;mining.progress+=dt*multiplier;survival?.spendToolEnergy(dt);
    if(mining.progress<(defs[id]?.hard||1))return;
    const rig=industry?.dismantle(x,y);
    if(rig){
      if(rig.ok){burst(x*TILE+12,y*TILE+12,'#afcbc4');renderHotbar();simulateNetworks();saveGame();}
      mining.key='';mining.progress=0;showToast(rig.message);return;
    }
    const m=meta.get(idx(x,y))||{};
    const placedItem=itemByKey.get(m.placedItem)?.item.tile===id?m.placedItem:
      Object.hasOwn(m,'linked')&&Object.hasOwn(m,'on')?items.find(item=>item.tile===id)?.key:null;
    const drop=placedItem||defs[id].drop;
    const cargo=[...(drop?[[drop,1]]:[]),...Object.entries(m.contents||{}),...Object.entries(m.outputs||{})].filter(([,n])=>n>0);
    const recovered=placedItem?recoverItems(cargo,x,y):engine.spawnBatch(cargo,x+.5,y+.5);
    if(!recovered){showToast('Make room in your backpack or collect a nearby pile first.');return;}
    if(id===T.COPPER)stats.copperMined++;if(id===T.IRON)stats.ironMined=(stats.ironMined||0)+1;stats.mined=(stats.mined||0)+1;
    discoverSchematic(m);const old=m.assembly;set(x,y,T.AIR);if(old)engine.splitAssembly(old);
    burst(x*TILE+12,y*TILE+12,palette[drop]||'#b6d1c8');sound(drop||'stone');mining.key='';mining.progress=0;camera.shake=4;
    if(!placedItem)collectDrops(true);
    renderHotbar();simulateNetworks();
    showToast(defs[id].name+(recovered.loose?' recovered · '+recovered.loose+' left on the ground':' recovered'));
  }
  function finishLink(){
    const minX=Math.max(0,Math.min(linkDrag.x0,linkDrag.x1)),maxX=Math.min(WORLD_W-1,Math.max(linkDrag.x0,linkDrag.x1));
    const minY=Math.max(0,Math.min(linkDrag.y0,linkDrag.y1)),maxY=Math.min(WORLD_H-1,Math.max(linkDrag.y0,linkDrag.y1));
    const keys=[];for(let y=minY;y<=maxY;y++)for(let x=minX;x<=maxX;x++){const id=get(x,y),d=defs[id];if(id&&id!==T.CORE&&!d.foundryRig&&!d.foundryPart&&distanceToTile(x,y)<10)keys.push(idx(x,y));}
    if(!keys.length){showToast('Select blocks within reach');return}
    if(linkMode==='inspect'){selectedAssembly=meta.get(keys[0])?.assembly||null;showToast(selectedAssembly?`Inspecting structure ${selectedAssembly}`:'That block is not linked');updateInspector();return}
    if(linkMode==='add'&&!selectedAssembly){showToast('Use Inspect first to select the structure to extend');return}
    const id=engine.link(keys,linkMode,selectedAssembly);if(id)selectedAssembly=id;
    showToast(`${keys.length} blocks: ${linkMode==='remove'?'detached':`structure ${id} (disconnected pieces split)`}`);simulateNetworks();
  }
  function neighbors(x,y){return [[x+1,y],[x-1,y],[x,y+1],[x,y-1]].filter(p=>inside(p[0],p[1]))}
  function simulateNetworks(dt=0){
    if(!engine)return;engine.step(dt);
    const liveCore=engine.active.some(k=>tileByIndex(k)===T.CORE&&meta.get(k)?.powered&&meta.get(k)?.signalOK);
    if(liveCore&&!stats.coreReached){stats.coreReached=true;showToast('Foundry Core online — commission complete!')}
    updatePowerHud(engine.supply,engine.load);
  }
  function updateSensors(){for(const k of engine.nodes)if(tileByIndex(k)===T.SENSOR){const m=meta.get(k),x=k%WORLD_W,y=Math.floor(k/WORLD_W);m.sensorActive=Math.hypot(player.x/TILE-x,player.y/TILE-y)<6}}
  function updateMachines(dt=.05){
    for(const [k,m] of Array.from(meta.entries())){
      if(meta.get(k)!==m)continue; // An earlier actuator may already have moved this component.
      const id=tileByIndex(k),x=k%WORLD_W,y=Math.floor(k/WORLD_W);m.machineClock=(m.machineClock||0)+dt;
      if(id===T.PISTON){
        const enabled=m.running&&m.signalOK!==false;
        if(enabled&&!m.extended){if(operatePiston(x,y,m,1)){m.extended=true;m.animationTime=gameTime;}}
        else if(!enabled&&m.extended&&m.running){if(operatePiston(x,y,m,-1)){m.extended=false;m.animationTime=gameTime;}}
      }else if(id===T.WINCH&&m.running&&!m.manualPaused&&m.machineClock>.5/Math.max(.1,Math.abs(m.speed))){
        const direction=Object.prototype.hasOwnProperty.call(m.signalInputs||{},'EN')?(m.signalInputs.EN?-1:1):(m.liftDirection||-1);
        m.machineClock=0;if(operateWinch(x,y,m,direction))stats.motionEvents++;
      }else if((id===T.BEARING||id===T.HINGE)&&m.running){
        if(!m.jointAssembly){const [dx,dy]=dirs[m.rot||0];m.jointAssembly=meta.get(idx(x+dx,y+dy))?.assembly||null;}
        if(!m.jointAssembly){m.fault='NO LINKED ASSEMBLY AT FRONT';continue}
        const target=id===T.HINGE?(m.signalInputs?.EN?1:0):null;
        if(m.machineClock>=1/Math.max(.1,Math.abs(m.speed))&&(id===T.BEARING||target!==(m.jointStep||0))){
          m.machineClock=0;const turn=id===T.HINGE?(target?1:-1):Math.sign(m.speed);
          if(rotateAssembly(x,y,m,turn)){m.jointStep=id===T.HINGE?target:((m.jointStep||0)+turn+4)%4;stats.motionEvents++;}
        }
      }
    }
  }
  const dirs=[[1,0],[0,1],[-1,0],[0,-1]];
  function collectLinked(x,y){return engine.group(x,y)}
  function markTranslation(nx,ny,previous){
    tileMotions.set(idx(nx,ny),{dx:previous.x-nx,dy:previous.y-ny,start:gameTime});
  }
  function markRiderMotion(oldX,oldY){
    const left=riderMotion?motionRemaining(riderMotion.start):0;
    riderMotion={dx:oldX-player.x+(riderMotion?.dx||0)*left,dy:oldY-player.y+(riderMotion?.dy||0)*left,start:gameTime};
  }
  function moveGroup(group,dx,dy,torque,machine={}){
    const own=new Set(group.map(p=>idx(...p))),weight=group.reduce((n,[x,y])=>n+(defs[get(x,y)]?.weight??1),0);
    const required=weight*(dy<0?1.25:1);machine.requiredTorque=required;machine.availableTorque=torque||0;
    if(engine.applyStress(group,required)!==null){machine.fault='STRUCTURAL FAILURE — weak connection detached';return false}
    if(required>(torque||0)){machine.fault=`STALLED: need ${required.toFixed(1)} / have ${(torque||0).toFixed(1)} torque`;return false}
    engine.rebuild();const bodies=new Set(group.map(([x,y])=>meta.get(idx(x,y))?.assembly).filter(Boolean));
    if(engine.active.some(k=>bodies.has(meta.get(k)?.jointAssembly))){machine.fault='LOAD IS ATTACHED TO AN ANCHORED JOINT';return false}
    if(group.some(([x,y])=>[T.BEARING,T.HINGE].includes(get(x,y)))){machine.fault='JOINT MOUNT IS ANCHORED';return false}
    const riders=group.some(([x,y])=>Math.abs(player.y+player.h-y*TILE)<5&&player.x+player.w>x*TILE&&player.x<(x+1)*TILE);
    for(const [x,y] of group){const nx=x+dx,ny=y+dy;if(!inside(nx,ny)||(!own.has(idx(nx,ny))&&get(nx,ny)!==T.AIR)){
      machine.fault='MOVEMENT BLOCKED';return false}
      if(!riders&&defs[get(x,y)]?.solid&&playerOverlapsTile(nx,ny)){machine.fault='PLAYER IN PATH';return false}
    }
    if(riders&&!canCarryPlayer(dx*TILE,dy*TILE,own)){machine.fault='RIDER PATH BLOCKED';return false}
    const parts=group.map(([x,y])=>({x,y,id:get(x,y),m:meta.get(idx(x,y)),visual:tileMotion(x,y)}));
    for(const p of parts)set(p.x,p.y,T.AIR);
    for(const p of parts){set(p.x+dx,p.y+dy,p.id,p.m);markTranslation(p.x+dx,p.y+dy,p.visual);}
    if(riders){const {x,y}=player;player.x+=dx*TILE;player.y+=dy*TILE;markRiderMotion(x,y);}
    machine.fault='';return true;
  }
  function operatePiston(x,y,m,way){
    const [dx,dy]=dirs[m.rot||0],fx=x+dx*(way>0?1:2),fy=y+dy*(way>0?1:2),id=get(fx,fy);
    if(!id)return way<0;const group=collectLinked(fx,fy);
    if(group.some(([gx,gy])=>gx===x&&gy===y)){m.fault='ACTUATOR LINKED TO ITS OWN LOAD';return false}
    const ok=moveGroup(group,dx*way,dy*way,m.torque,m);if(ok){stats.motionEvents++;camera.shake=3}return ok;
  }
  function operateWinch(x,y,m,dy){
    const reach=V?.cableReach(x,y,get,T)||50;
    if(m.liftAssembly){const info=engine.structureInfo(m.liftAssembly),group=info.keys.map(k=>engine.xy(k));if(group.length&&group.every(([gx,gy])=>Math.abs(gx-x)<12&&gy>y&&gy<=y+reach))return moveLiftLoad(group,x,y,m,dy);m.liftAssembly=null;}
    for(let yy=y+1;yy<Math.min(WORLD_H,y+reach+1);yy++)if(get(x,yy)!==T.AIR&&!defs[get(x,yy)]?.ladder&&!defs[get(x,yy)]?.rail){
      const group=collectLinked(x,yy);if(group.some(([gx,gy])=>gx===x&&gy===y)){m.fault='WINCH LINKED TO LOAD';return false}
      const assembly=meta.get(idx(x,yy))?.assembly||null;
      if(yy>y+50&&(!assembly||!V?.hasGuide(group,get,T)))break;
      m.liftAssembly=assembly;
      return moveLiftLoad(group,x,y,m,dy);
    }m.fault='NO LOAD BELOW';return false;
  }
  function moveLiftLoad(group,x,y,m,dy){
    if(V){const counterGroup=V.findCounterweight(engine,group,x,y);m.counterweightAssembly=counterGroup.length?meta.get(idx(...counterGroup[0]))?.assembly:null;
      if(counterGroup.length||m.guided||V.hasGuide(group,get,T)){
        const oldX=player.x,oldY=player.y;
        const active=[...group.map(([x,y])=>[x,y+dy,tileMotion(x,y)]),...counterGroup.map(([x,y])=>[x,y-dy,tileMotion(x,y)])];
        const moved=V.moveLift({group,counterGroup,x,y,m,dy,torque:m.torque,engine,get,set,meta:()=>meta,defs,T,player,tile:TILE,width:WORLD_W,height:WORLD_H});
        if(moved){
          for(const [x,ny,previous] of active)markTranslation(x,ny,previous);
          if(player.y!==oldY)markRiderMotion(oldX,oldY);
        }
        return moved;
      }}
    m.counterbalanced=false;return moveGroup(group,0,dy,m.torque,m);
  }
  function assemblyColor(id){return `hsl(${((id||1)*137.5)%360} 80% 70%)`}
  function configureComponent(id,k,m){
    const mode=()=>{engine.revision++;simulateNetworks();updateInspector();return true};
    if(m.ruin&&m.disabled){if((inventory.repairKit||0)>0)inventory.repairKit--;else if((inventory.iron||0)>=3&&(inventory.copper||0)>=2){inventory.iron-=3;inventory.copper-=2;}else{showToast('Repair requires a repair kit, or 3 iron and 2 copper.');return true;}m.disabled=false;m.ruin=false;meta.set(k,m);renderHotbar();showToast(`${defs[id].name} repaired. Connect its power or drive ports to reuse it.`);return mode();}
    if(defs[id]?.machine?.recipes){const choices=defs[id].machine.recipes,current=engine.recipe(k),next=choices[(choices.indexOf(current)+1)%choices.length];m.recipe=next.key;m.progress=0;meta.set(k,m);if(!lens)toggleLens();showToast(`${defs[id].name}: ${next.key}. Shift+F inserts held material; F collects output.`);return mode()}
    if(defs[id]?.producer||defs[id]?.defense){if(!lens)toggleLens();updateInspector();showToast(engine.diagnose(k).fault||'Shift+F: insert held materials · F: collect output');return true}
    if(id===T.RECOVERY_STATION){survival?.setCheckpoint();return true}
    if(id===T.DOOR){if(m.open&&playerOverlapsTile(k%WORLD_W,Math.floor(k/WORLD_W))){showToast('Step clear of the doorway before closing it.');return true}m.open=!m.open;meta.set(k,m);showToast(m.open?'Door opened':'Door closed');return mode()}
    if(id===T.SWITCH){m.on=!m.on;showToast(m.on?'Switch ON':'Switch OFF');return mode()}
    if(id===T.WINCH){if(m.manualPaused){m.manualPaused=false;m.liftDirection=-1;}else if(m.liftDirection===1)m.manualPaused=true;else m.liftDirection=1;showToast(m.manualPaused?'Lift paused. Interact to resume.':Object.prototype.hasOwnProperty.call(m.signalInputs||{},'EN')?'Lift controlled by its connected switch.':m.liftDirection<0?'Lift: raise.':'Lift: lower.');return mode()}
    if(id===T.GEAR){m.mode=((m.mode??1)+1)%3;showToast(['Torque: ½ speed, 2× torque','Balanced: 1:1','Speed: 2× speed, ½ torque'][m.mode]);return mode()}
    if(id===T.TIMER||id===T.PULSE){m.delay=E.DELAYS[(E.DELAYS.indexOf(m.delay||1)+1)%E.DELAYS.length];m.timer=m.phase=0;showToast(`${defs[id].name}: ${m.delay}s`);return mode()}
    if(id===T.COUNTER){m.target=((m.target||3)%15)+1;showToast(`Counter target: ${m.target}; top port resets count`);return mode()}
    if(id===T.FILTER){const choices=['iron','copper','stone','wood','crystal','dirt','obsidian',...items.filter(i=>i.tile>0).map(i=>i.key)];
      m.filter=keys.ShiftLeft?(selectedItem()?.key||'iron'):choices[(choices.indexOf(m.filter||'iron')+1)%choices.length];showToast(`Filter: ${m.filter} (Shift-right-click: held part)`);return mode()}
    if(id===T.STORAGE){showToast('F: take contents · Shift+F: store selected part · hopper at front extracts');return true}
    if(id===T.BEARING||id===T.HINGE){showToast('Link the load separately. Rear: shaft drive. Front: attach load. Top: hinge control.');return true}
    return false;
  }
  function canCarryPlayer(dx,dy,ignore){
    const steps=Math.max(1,Math.ceil(Math.hypot(dx,dy)/6));
    for(let i=1;i<=steps;i++){
      const x=player.x+dx*i/steps,y=player.y+dy*i/steps;
      for(let ty=Math.floor(y/TILE);ty<=Math.floor((y+player.h-.01)/TILE);ty++)for(let tx=Math.floor(x/TILE);tx<=Math.floor((x+player.w-.01)/TILE);tx++)
        if(!ignore.has(idx(tx,ty))&&defs[get(tx,ty)]?.solid)return false;
    }return true;
  }
  function rotateAssembly(x,y,m,turn){
    const info=engine.structureInfo(m.jointAssembly),own=new Set(info.keys);
    if(!info.keys.length){m.jointAssembly=null;m.fault='ASSEMBLY DETACHED';return false}
    if(own.has(idx(x,y))){m.fault='LINK LOAD SEPARATELY FROM JOINT MOUNT';return false}
    if(info.keys.length>128){m.fault='JOINT LIMIT: 128 BLOCKS';return false}
    const group=info.keys.map(k=>engine.xy(k));
    const inertia=group.reduce((n,[gx,gy])=>n+(defs[get(gx,gy)].weight||1)*Math.max(1,Math.hypot(gx-x,gy-y)),0);
    m.requiredTorque=inertia;m.availableTorque=m.torque;
    if(engine.applyStress(group,inertia)!==null){m.fault='STRUCTURAL FAILURE — connection detached';return false}
    if(inertia>m.torque){m.fault=`STALLED: need ${inertia.toFixed(1)} / have ${(m.torque||0).toFixed(1)} torque`;return false}
    if(group.some(([gx,gy])=>[T.BEARING,T.HINGE].includes(get(gx,gy)))){m.fault='ANOTHER JOINT ANCHORS THIS ASSEMBLY';return false}
    // Grid-quantized sweep: sample the entire arc, not just the final destination.
    for(let step=1;step<=18;step++){
      const angle=turn*Math.PI/2*step/18,c=Math.cos(angle),sn=Math.sin(angle);
      for(const [gx,gy] of group){const nx=Math.round(x+(gx-x)*c-(gy-y)*sn),ny=Math.round(y+(gx-x)*sn+(gy-y)*c);
        if(!inside(nx,ny)||(!own.has(idx(nx,ny))&&get(nx,ny)!==T.AIR)){m.fault='JOINT SWEEP BLOCKED';return false}
        if(playerOverlapsTile(nx,ny)){m.fault='PLAYER IN JOINT SWEEP';return false}
      }
    }
    const parts=group.map(([gx,gy])=>({x:gx,y:gy,id:get(gx,gy),m:meta.get(idx(gx,gy))||{}}));
    for(const p of parts)set(p.x,p.y,T.AIR);
    for(const p of parts){
      const nx=x-turn*(p.y-y),ny=y+turn*(p.x-x);
      set(nx,ny,p.id,{...p.m,rot:((p.m.rot||0)+turn+4)%4});
      tileMotions.set(idx(nx,ny),{pivot:[x,y],turn,start:gameTime});
    }
    m.fault='';return true;
  }
  function inventoryRoom(){return Math.max(0,2048-Object.values(inventory).reduce((n,v)=>n+(Number.isFinite(v)?v:0),0));}
  function recoverItems(entries,x,y){
    const carried=[],overflow=[];
    let room=inventoryRoom(),loose=0;
    for(const [key,count] of entries){
      const take=Math.min(count,room);
      if(take>0){carried.push([key,take]);room-=take;}
      if(count>take){overflow.push([key,count-take]);loose+=count-take;}
    }
    if(!engine.spawnBatch(overflow,x+.5,y+.5))return null;
    for(const [key,count] of carried)inventory[key]=(inventory[key]||0)+count;
    return {loose};
  }
  function itemLabel(key){return itemByKey.get(key)?.item.name||key;}
  function canReachDrop(drop,px,py){
    return !engine.dropSolid(Math.floor(drop.x),Math.floor(drop.y))&&lineOfSight(px,py,drop.x,drop.y);
  }
  function putDropInPack(drop,room){
    const count=Math.min(drop.count,room);
    if(count<=0)return 0;
    inventory[drop.key]=(inventory[drop.key]||0)+count;drop.count-=count;
    return count;
  }
  function collectDrops(immediate=false){
    const px=(player.x+player.w/2)/TILE,py=(player.y+player.h/2)/TILE;
    let room=inventoryRoom(),collected=0;
    for(const drop of engine.drops){
      const distance=Math.hypot(drop.x-px,drop.y-py);
      if(drop.manual){
        if(distance>3)delete drop.manual;
        continue;
      }
      if(!room||distance>=2.6||!immediate&&drop.age<=.15||!canReachDrop(drop,px,py))continue;
      const count=putDropInPack(drop,room);
      collected+=count;room-=count;
    }
    if(collected){engine.drops=engine.drops.filter(d=>d.count>0);renderHotbar();}
  }
  function aimedGroundDrop(){
    if(!pointer.aimed||!engine?.drops.length)return null;
    const px=(player.x+player.w/2)/TILE,py=(player.y+player.h/2)/TILE;
    return engine.drops.find(d=>Math.floor(d.x)===pointer.tx&&Math.floor(d.y)===pointer.ty
      &&Math.hypot(d.x-px,d.y-py)<=6&&canReachDrop(d,px,py));
  }
  function pickUpAimedDrop(){
    const drop=aimedGroundDrop();
    if(!drop)return false;
    const count=putDropInPack(drop,inventoryRoom());
    if(!count){showToast('Backpack full.');return true;}
    if(!drop.count)engine.drops=engine.drops.filter(item=>item!==drop);
    renderHotbar();saveGame();showToast('Picked up '+count+' '+itemLabel(drop.key)+'.');
    return true;
  }
  function interactItems(){
    if(paused||craftOpen||manualOpen)return;
    const k=idx(pointer.tx,pointer.ty),id=get(pointer.tx,pointer.ty),m=meta.get(k);
    if(distanceToTile(pointer.tx,pointer.ty)>6||!m||(!defs[id].transport&&id!==T.CONVEYOR)){showToast('Aim at nearby item machinery');return}
    discoverSchematic(m);
    if(keys.ShiftLeft||keys.ShiftRight){const key=selectedItem()?.key;if(key&&(inventory[key]||0)>0){const n=engine.insert(k,key,1);inventory[key]-=n;showToast(n?`${key} inserted`:'Full or filtered')}}
    else {let total=0;const buffers=defs[id].producer?[m.outputs||{},...(keys.ShiftLeft?[]:[m.contents||{}])]:[m.contents||{}];for(const buffer of buffers)for(const [key,n] of Object.entries(buffer)){const take=Math.min(n,inventoryRoom());inventory[key]=(inventory[key]||0)+take;buffer[key]-=take;total+=take;if(!buffer[key])delete buffer[key];}showToast(`Collected ${total} items`)}
    renderHotbar();updateInspector();
  }
  function discoverSchematic(m){if(!m?.schematic||m.schematicClaimed)return;const known=Array.isArray(stats.schematics)?stats.schematics:[];if(!known.includes(m.schematic)){known.push(m.schematic);stats.schematics=known;showToast(`Schematic discovered: ${m.schematic}. Related fabrication is now unlocked.`);sound('crystal');}m.schematicClaimed=true;}
  function dropHeldItem(){
    if(paused||craftOpen||manualOpen)return;
    const item=selectedItem();if(!item||item.permanent||(inventory[item.key]||0)<1)return;
    const x=pointer.tx,y=pointer.ty;if(!pointer.aimed||distanceToTile(x,y)>6||!inside(x,y)){showToast('Aim at a spot within reach.');return}
    const id=get(x,y),def=defs[id],transport=def?.transport||id===T.CONVEYOR;
    if(transport){
      if(!engine.insert(idx(x,y),item.key,1)){showToast('That machine is full or cannot take this item.');return;}
    }else{
      const onSurface=engine.dropSolid(x,y),dropY=onSurface?y-E.DROP_HALF:y+.5;
      const px=(player.x+player.w/2)/TILE,py=(player.y+player.h/2)/TILE;
      if(engine.dropSolid(x,Math.floor(dropY))||!lineOfSight(px,py,x+.5,dropY)){
        showToast('Aim at an open spot or the top of a surface.');return;
      }
      if(!engine.spawn(item.key,1,x+.5,dropY,{manual:true})){showToast('Collect a loose pile before dropping more items.');return;}
    }
    inventory[item.key]--;renderHotbar();saveGame();
    showToast(transport?`${item.name} loaded into ${def.name}.`:`${item.name} dropped. Aim at it and press F to pick it up.`);
  }
  function drawDrops(){
    const half=TILE*E.DROP_HALF;
    for(const d of engine.drops){const x=d.x*TILE-camera.x,y=d.y*TILE-camera.y;if(x<-10||y<-10||x>vw+10||y>vh+10)continue;
      const base=y+half;
      if(!art?.item(ctx,d.key,x-half,y-half,half*2,true)){
        ctx.fillStyle=palette[d.key]||itemByKey.get(d.key)?.item.color||'#dfdca9';ctx.fillRect(x-3,base-7,7,7);
        ctx.strokeStyle='#17262b';ctx.strokeRect(x-2.5,base-6.5,6,6);
      }
      if(lens&&d.count>1){ctx.fillStyle='#fff';ctx.font='9px monospace';ctx.fillText(d.count,x+4,y)}
    }
  }
  function drawEngineeringOverlay(id,m,x,y,painted){
    const ports=E.ports(defs[id],m);
    const cargo=Object.entries(m.contents||{}).find(([,n])=>n>0);
    if(cargo){
      const [key,count]=cargo,[dx,dy]=E.DIRS[m.rot||0];
      const offset=id===T.CONVEYOR&&m.running?((m.itemClock||0)/.25-.5)*14:0;
      const cargoX=8+dx*offset,cargoY=(id===T.CONVEYOR?-8:4)+dy*offset;
      ctx.fillStyle=palette[key]||items.find(i=>i.key===key)?.color||'#e4dfa3';
      if(!art?.item(ctx,key,cargoX,cargoY,8,id===T.CONVEYOR))ctx.fillRect(cargoX+1,cargoY+2,6,6);
      if(count>1)ctx.fillRect(cargoX+7,cargoY+3,3,3);
    }
    if(defs[id].kind==='signal'){ctx.fillStyle=m.active?'#fff79c':'#173139';ctx.fillRect(3,3,4,3)}
    if(!painted&&id>=26&&defs[id].mechanical&&m.running){
      const angle=gameTime*m.speed*Math.PI/2;ctx.strokeStyle='#fff9d1';ctx.lineWidth=2;
      ctx.beginPath();ctx.moveTo(12,12);ctx.lineTo(12+Math.cos(angle)*7,12+Math.sin(angle)*7);ctx.stroke();
    }
    if(ports.length&&!lens&&inspected?.x===x&&inspected?.y===y){ctx.fillStyle='#ffffffbb';const [dx,dy]=E.DIRS[m.rot||0];ctx.beginPath();ctx.moveTo(12+dx*10,12+dy*10);ctx.lineTo(12+dx*5-dy*3,12+dy*5+dx*3);ctx.lineTo(12+dx*5+dy*3,12+dy*5-dx*3);ctx.fill()}
    if(m.lastShot&&engine.elapsed-m.lastShot.time<.13){ctx.strokeStyle=id===T.TURRET?'#ffe48f':'#89eeff';ctx.lineWidth=2;ctx.beginPath();ctx.moveTo(12,12);ctx.lineTo((m.lastShot.x-x)*TILE,(m.lastShot.y-y)*TILE);ctx.stroke();}
    if(id===T.WINCH){for(const assembly of [m.liftAssembly,m.counterweightAssembly])if(assembly){const info=engine.structureInfo(assembly);if(!info.keys.length)continue;const cells=info.keys.map(k=>tileMotion(...engine.xy(k))),cx=cells.reduce((n,p)=>n+p.x,0)/cells.length+.5,cy=Math.min(...cells.map(p=>p.y));ctx.strokeStyle=m.running?'#b5ded0':'#687e7b';ctx.lineWidth=1.5;ctx.beginPath();ctx.moveTo(12,18);ctx.lineTo((cx-x)*TILE,18);ctx.lineTo((cx-x)*TILE,(cy-y)*TILE);ctx.stroke();}}
    if(!lens&&(inspected?.x!==x||inspected?.y!==y))return;
    if(lensMode==='structure'&&m.assembly){const ratio=(m.stress||0)/((defs[id].strength||8)*4);ctx.fillStyle=E.strengthColor(ratio)+'66';ctx.fillRect(2,2,20,20)}
    if(!lens||lensMode==='ports'||['power','signal','mechanical','item'].includes(lensMode)){
      const sideCount={};for(const p of ports){if(lens&&lensMode!=='ports'&&p.type!==lensMode)continue;
        const [dx,dy]=E.DIRS[p.side],offset=(sideCount[p.side]||0)*5;sideCount[p.side]=(sideCount[p.side]||0)+1;
        const px=12+dx*10-dy*offset,py=12+dy*10+dx*offset;ctx.fillStyle=E.COLORS[p.type];
        if(p.mode==='both')ctx.fillRect(px-2,py-2,4,4);else{const sign=p.mode==='out'?1:-1;ctx.beginPath();ctx.moveTo(px+dx*3*sign,py+dy*3*sign);ctx.lineTo(px-dx*2*sign-dy*3,py-dy*2*sign+dx*3);ctx.lineTo(px-dx*2*sign+dy*3,py-dy*2*sign-dx*3);ctx.fill()}
      }
    }
    if(lensMode==='item'&&(defs[id].transport||id===T.CONVEYOR)){ctx.fillStyle='#fff';ctx.font='9px monospace';ctx.fillText(engine.stored(idx(x,y)),2,10)}
    if(lensMode==='mechanical'&&id===T.BELT){for(const e of engine.mechanical.get(idx(x,y))||[])if(e.remote){const [nx,ny]=engine.xy(e.to);ctx.strokeStyle=m.running?'#8df4e4':'#829c98';ctx.setLineDash([3,3]);ctx.beginPath();ctx.moveTo(12,12);ctx.lineTo((nx-x)*TILE+12,(ny-y)*TILE+12);ctx.stroke();ctx.setLineDash([])}}
    if(m.fault||m.machineFault||m.defenseFault||m.mechanicalFault||m.logisticsFault||m.logicUnstable){ctx.fillStyle='#ff6b5f';ctx.font='bold 11px monospace';ctx.fillText('!',17,10)}
  }
  function cycleLens(){const modes=['ports','power','signal','mechanical','structure','item'];lensMode=modes[(modes.indexOf(lensMode)+1)%modes.length];if(!lens)toggleLens();document.getElementById('viewBtn').textContent=`View: ${lensMode} [V]`;updateInspector()}
  function cycleLink(){const modes=['new','add','remove','inspect'];linkMode=modes[(modes.indexOf(linkMode)+1)%modes.length];document.getElementById('linkModeBtn').textContent=`Link: ${linkMode} [L]`;selectItem('link',hotbar.indexOf('link'));showToast(`Link mode: ${linkMode}`)}
  function setupEngineeringUI(){
    const actions=document.querySelector('.top-actions');
    const view=document.createElement('button');view.id='viewBtn';view.className='icon-btn';view.textContent='View: ports [V]';view.onclick=cycleLens;actions.prepend(view);
    const mode=document.createElement('button');mode.id='linkModeBtn';mode.className='icon-btn';mode.textContent='Link: new [L]';mode.onclick=cycleLink;actions.prepend(mode);
    const card=(title,text)=>{const el=document.createElement('div');el.className='manual-card';const h=document.createElement('h3'),p=document.createElement('p');h.textContent=title;p.textContent=text;el.append(h,p);return el};
    const manual=document.querySelector('.manual-grid');manual.replaceChildren(
      card('Explore and build','A/D moves, Space jumps, and W/S climbs. Select a pickaxe to mine, a part to place, or a weapon to attack. Right-click a machine or door to use it. Drag line-buildable parts and release to place the preview. E opens crafting, I opens the backpack. R rotates; G toggles the smart cursor.'),
      card('Read the ports','C equips the Lens. V or the View button changes modes. Arrows show input/output; squares are two-way. Yellow: electricity. Red: signals. Cyan: rotation. Green: items. Purple: joint. Matching ports must face each other. Motor rear = power, top = enable, front = shaft.'),
      card('Logic and memory','Default gate orientation: rear A, top B, front output. NOT inverts A. Latch: rear SET, top RESET (reset wins). Toggle flips on each rising edge. Counter counts rising edges, top resets. Open the counter and choose Change settings. Unconnected gate inputs are OFF; a NOT with no input outputs ON.'),
      card('Timing','Timer requires a continuously high input for its selected delay. Pulse Generator repeats once per delay; rear input optionally enables it. Change settings cycles 0.25 / 0.5 / 1 / 2 / 5 / 10 seconds. Simulation uses 50ms steps. Insert a latch/toggle for feedback; unstable combinational loops are flagged.'),
      card('Drive paths','Shafts run rear to front. Bevel gears turn 90° toward the local bottom. Gearbox settings cycle torque / balanced / speed. Small gear doubles speed, halves torque and reverses; large gear does the opposite. Clutch needs a top signal. A Belt Drive reaches a facing pulley or joint across up to six cells. Rotate belt and pulley to send drive vertically. For a full-turn bearing, leave space around the load and belt-drive the mount from a distance; this belt runs behind its own rotating load.'),
      card('Link distinct structures','Select Link Tool (0). L cycles New / Add / Remove / Inspect. Drag a rectangle. Each connected selection gets its own colored structure ID. Inspect a block first to choose an existing structure for Add. Removing blocks splits disconnected sections. Keep actuators and mounts separate from their loads.'),
        card('Joints and load','A piston pushes one block-length; a powered piston with a low enable signal retracts. Winches lift loads below repeatedly. Line a deep shaft with Magnetic Guide Rail starting beside the winch to extend its reach from 50 to 480 tiles. Bearings rotate separately linked loads at their front; hinges swing 0-90° using a top signal. Sweeps stop at obstacles or the player. Joint mounts stay anchored. Heavy or long arms need torque gearing.'),
      card('Strength and failures','Structural Lens: blue low stress, green normal, yellow high, orange danger, red failure risk. Heavy lifting and long rotating arms raise stress. Three extreme overload attempts can detach a weak connection, without deleting the block. Inspector shows required and available torque, blocked paths and power shortages.'),
      card('Physical logistics','Conveyors need rear or bottom shaft drive. Items enter at the rear or fall from above; front is output. Hoppers collect above/rear and output front. Storage holds 64 items; attach a hopper to its front to extract. Splitters alternate front/bottom output and wait if the selected route blocks. Filters pass only the selected material. Filter settings cycle raw materials. Hold Shift while changing settings to select the held component.'),
      card('Load and recover','X drops one held item. Aim at a loose pile and press F, right-click, or tap Use to pick it up. You can also collect it by walking away and coming back. Aim at a machine and press F to collect its contents or Shift+F to load one held item. Dismantling a placed part returns it and its cargo to your backpack, with any overflow left on the ground.'),
      card('Try a sensor door','Lay a generator behind a motor, then a shaft into a piston. Place a sensor above the motor, rotated downward to face its enable port. Link a lightweight door in front of the piston, separately from the drive. For automatic return, power the motor continuously and connect the sensor to the piston enable instead.'),
      card('Automate and defend','B opens real component blueprints. A drill needs side power, rear drive and an output hopper; it removes terrain and stops when output fills. Feed ore through a driven crusher, powered smelter and assembler. F recovers goods; Shift+F inserts the selected material. Ammunition fabricators feed real rounds to powered turrets. Enemy sensors connect through signal wires; loss of power or ammunition stops the defense.'),
      card('Survive the depths','Q attacks, Tab cycles owned weapons, Z consumes a healing item. Selecting a weapon makes the primary action attack. Enemies show a warning before striking. Defend walls and machinery during warned raids. J tracks five chapters and unlocks. Equipment and progression survive death; marked caches retain dropped resources. Recovery stations establish checkpoints near power.'),
      card('Climb and lift','Ladders drag vertically; hold W/S or touch arrows to climb. Jump dismounts. Ropes need a ceiling or anchor above them. Scaffolds and bridges support jump-through and Down to drop. A powered winch lifts a separately linked deck; its connected switch selects raise/lower. Clear the shaft and protect the drive. R keeps manual rotation; Smart placement in Pause is optional.')
    );
  }

  function renderResearchManual(){
    const manual=document.querySelector('.manual-grid');if(!manual)return;
    let card=manual.querySelector('.robotics-research');
    if(!card){card=document.createElement('article');card.className='manual-card robotics-research';manual.append(card);}
    const heading=document.createElement('h3');heading.textContent='Deep engineering';
    const minerals=document.createElement('p');minerals.textContent='Find blue cobalt below the outpost with a steel drill. Powered tools or an Ion Bore reach iridium deeper down. Mine gold-veined aetherium with a Plasma Excavator. Start with an Induction Smelter, then feed rare ingots into a Robotics Foundry.';
    const machines=document.createElement('p');machines.textContent='Drone docks need power, clear flight corridors, and available bins. Defensive drones take turret rounds. Place a Starship Pad under open air, use Launch with a Starship and Flight Cell, then land before pressing Use or F to exit.';
    const list=document.createElement('ul');
    for(const [key,site] of Object.entries(stats.roboticsSites||{})){
      const li=document.createElement('li');const known=stats.schematics?.includes(key);
      li.textContent=site.name+' vault · sector '+site.x+', depth '+site.y+(known?' · recovered':'');list.append(li);
    }
    const foundry=document.createElement('h3');foundry.textContent='Beneath the Foundry';
    const descent=document.createElement('p');descent.textContent='Find the shaft below the Reactor Abyss or fly to a distant surface entrance. Press M to open the expedition map. Read survey stations for approximate seam locations, then scan from nearby to mark the deposit.';
    const extraction=document.createElement('p');extraction.textContent='Bring a Deepcore Borer to the Osmium Basin. Place it beside an unworked seam, then open its controls for the anchor, power, drive and output checks. Refine the ore in an Induction Smelter. The new weapons require schematics recovered from underground sites.';
    const freight=document.createElement('p');freight.textContent='Name an outpost on the map to pin a route home. Link two powered freight stops with freight rail for travel. A Fleet Dock lets you deploy drones and assign ground robots to real bins, tracks and machines.';
    card.replaceChildren(heading,minerals,machines,list,foundry,descent,extraction,freight);
  }
  function selectItem(key,slot=-1){
    releasePointer(false);touchLine=null;touchConfirm=null;
    selected=itemByKey.get(key)?.index??-1;activeSlot=slot;manualRotation=false;
    if(selectedItem()?.weapon)survival?.selectWeapon(key);
    else survival?.cancelCharge?.();
    renderHotbar();ui?.refresh();
  }
  function assignToHotbar(key,slot){
    if(!itemByKey.has(key)||!Number.isInteger(slot)||slot<0||slot>=hotbar.length)return;
    const old=hotbar.indexOf(key);
    if(old>=0&&old!==slot)hotbar[old]=hotbar[slot];
    hotbar[slot]=key;selectItem(key,slot);saveGame();
  }
  let barSignature='',barNodes=[];
  function renderHotbar(){
    const el=document.getElementById('hotbar');
    const signature=hotbar.join('|');
    if(!barNodes.length||signature!==barSignature){
      barSignature=signature;
      barNodes=hotbar.map((key,i)=>{
        const it=itemByKey.get(key)?.item,b=document.createElement('button');
        const digit=document.createElement('span');digit.className='key';digit.textContent=i===9?'0':i+1;
        const icon=it&&(art?.icon(it.key,it.icon));
        const image=icon||document.createElement('span');
        if(!icon){image.className='mini';image.textContent=it?.icon||'·';if(it?.color)image.style.color=it.color;}
        const count=document.createElement('span');count.className='count';
        b.type='button';b.className='slot';b.title=it?.name||'Empty slot';b.dataset.item=key||'';
        b.setAttribute('aria-label',it?.name||'Empty slot '+(i+1));b.append(digit,image,count);
        b.onclick=event=>{selectItem(hotbar[i],i);if(event.detail)canvas.focus({preventScroll:true});};
        return {b,count,it};
      });
      el.replaceChildren(...barNodes.map(n=>n.b));
    }
    for(const [i,{b,count,it}] of barNodes.entries()){
      b.classList.toggle('selected',i===activeSlot);b.setAttribute('aria-pressed',String(i===activeSlot));
      const label=it&&!it.permanent&&it.key!=='link'?String(inventory[it.key]||0):'';
      if(count.textContent!==label)count.textContent=label;
    }
    const item=selectedItem();
    const action=selectedAction();
    const hint=blueprint?'Place full blueprint':item?.key==='link'?'Drag to link blocks':
      action==='mine'?'Hold to mine':action==='combat'?'Attack':action==='build'?'Place · R to rotate':'Use right-click or F';
    const label=document.getElementById('modeHint');
    if(label)label.textContent=item?item.name+' · '+hint:'Choose a hotbar item or open your backpack.';
  }
  function renderCraft(){workshop?.render();}
  function renderMissions(){
    const journal=survival?.getJournal(),guide=guides?.objective();
    const objective=guide||deepObjective()||journal?.current;
    const el=document.getElementById('missionList');
    const title=objective?.title||objective?.label||'Keep exploring';
    const detail=objective?.message||objective?.detail||(objective?.target>1?Math.min(objective.target,objective.current)+' / '+objective.target:'');
    const current=title+(detail?' · '+detail:'');
    const recipe=guide?.action==='craft-workbench'?'workbench':guide?.action==='craft-ladder'?'ladder':null;
    if(el.dataset.objective!==current){
      el.dataset.objective=current;
      if(recipe){
        const button=document.createElement('button');
        button.type='button';button.className='subtle-btn objective-action';button.textContent=current;
        button.onclick=()=>workshop.open('craft','hand',recipe);
        el.replaceChildren(button);
      }else el.textContent=current;
    }
    ui?.refresh();
  }
  function updatePowerHud(supply,load){const faults=engine.active.filter(k=>{const d=defs[tileByIndex(k)],m=meta.get(k)||{};return d.load&&!m.disabled&&m.signalOK!==false&&(!m.powered||m.overload);});if(faults.length>gridFaults)sound('failure');gridFaults=faults.length;const label=document.getElementById('powerText');label.textContent=`${load} / ${supply}${faults.length?' ⚠ '+faults.length:''}`;label.title=faults.length?faults.map(k=>defs[tileByIndex(k)].name+': check input power').join(' · '):'All connected consumers have power';document.getElementById('powerBar').style.width=(supply?Math.min(100,load/supply*100):0)+'%'}
  const portNames={power:'Power',mechanical:'Rotation',signal:'Signal',item:'Items',structure:'Structure',fluid:'Fluid'};
  const portSymbols={power:'⚡',mechanical:'↻',signal:'◆',item:'■',structure:'◇',fluid:'●'};
  function closeInspector(){
    inspected=null;
    document.getElementById('lensPanel')?.classList.remove('show');
  }
  function machineStatus(d,m,diag){
    if(m.overload)return ['Wire overloaded · '+diag.load+' load / '+diag.capacity+' capacity','Replace the weakest wire with a higher-capacity conductor or move a consumer to another grid.'];
    if(m.ruin&&m.disabled)return ['Damaged machine','Repair before connecting power or drive.'];
    if(m.signalOK===false||m.disabled)return ['Stopped by control','Turn on the connected control signal.'];
    const fault=m.fault||diag.fault||'';
    if(/output.*full|receiver.*full|storage.*full|buffer.*full/i.test(fault))return ['Output storage full','Collect output or give the next receiver more room.'];
    if(/blocked|clearance|obstruct/i.test(fault))return ['Path blocked','Clear the highlighted output or travel path.'];
    if(d.load&&!diag.powered)return [diag.connections.some(c=>c.type==='power'&&c.count)?'Insufficient power':'Power disconnected','Connect a generator or a live circuit to the power input.'];
    if(/torque/i.test(fault))return ['Insufficient torque','Use torque gearing or reduce the load.'];
    if(/drive|rotation/i.test(fault))return ['Drive disconnected or stopped','Connect a running motor or shaft to the rotation input.'];
    if(/need|missing|required|input|supply/i.test(fault)&&diag.recipe){
      const material=Object.keys(diag.recipe.cost).find(key=>(m.contents?.[key]||0)<diag.recipe.cost[key]);
      if(material)return ['Waiting for '+(itemByKey.get(material)?.item.name||material),'Load the ingredient or feed the item input.'];
    }
    if(fault)return [fault.charAt(0)+fault.slice(1).toLowerCase(),'Check the matching port or connected component.'];
    return [m.running||m.working||m.firing||m.active?'Running':diag.powered?'Powered · ready':'Ready',''];
  }
  function updateInspector(){
    if(!inspected)return;
    const {x,y,id,k,node}=inspected,d=defs[id],m=meta.get(k)||{};
    if(get(x,y)!==id||(node!=null&&m.nodeId!==node)||distanceToTile(x,y)>6){closeInspector();return;}
    const deepRig=industry?.describe(x,y);
    if(deepRig){renderFoundryInspector(deepRig);return;}
    const diag=engine.diagnose(k),machine=machineStatus(d,m,diag);
    const drones=fleet?.describe(x,y)||robotics?.describe(x,y),pad=starship?.describe(x,y),special=drones||pad;
    const status=special?.status||machine[0],advice=special?.detail||machine[1];
    const name=document.getElementById('inspectName');
    const panelName=special?.name||m.siteName||d.name;
    if(name.textContent!==panelName)name.textContent=panelName;
    const el=document.getElementById('inspectProps'),actions=document.getElementById('machineActions');
    const ports=E.ports(d,m),recipe=diag.recipe,detailsOpen=el.querySelector('details')?.open;
    const inputs=recipe?Object.keys(recipe.cost):d.ammo?[d.ammo]:[selectedItem()?.key].filter(Boolean);
    const stash=Object.entries(m.contents||{}).filter(([,n])=>n);
    const output=Object.entries(m.outputs||{}).filter(([,n])=>n);
    const summary=[status,advice,m.rot,ports,diag.connections,diag.torque,diag.supply,diag.load,diag.capacity,
      m.requiredTorque,m.energy,m.assembly,m.filter,m.mode,m.delay,m.target,m.manualPaused,recipe?.key,stash,output,
      drones?.deployedRows,m.counterweightMass,m.liftDirection,m.progress?Math.floor(m.progress*20):0];
    const signature=JSON.stringify([k,id,node,summary]);
    const element=(tag,label,className)=>{
      const e=document.createElement(tag);
      if(label)e.textContent=label;
      if(className)e.className=className;
      return e;
    };
    const countLabel=([key,n])=>n+' '+(itemByKey.get(key)?.item.name||key);
    if(el.dataset.signature!==signature){
      el.dataset.signature=signature;
      const fragment=document.createDocumentFragment();
      fragment.append(element('strong',status,'machine-status'));
      if(advice)fragment.append(element('p',advice,'machine-next'));
      for(const row of drones?.deployedRows||[]){
        const cargo=Object.entries(row.counts||row.cargo||{}).map(([key,n])=>n+' '+(itemByKey.get(key)?.item.name||key)).join(', ');
        const label=typeof row==='string'?row:[row.name,row.phase,row.charge!=null?Math.round(row.charge)+'% charge':'',cargo,row.reason].filter(Boolean).join(' · ');
        if(label)fragment.append(element('p',label,'machine-next'));
      }
      if(stash.length)fragment.append(element('p','Stored · '+stash.map(countLabel).join(', '),'machine-next'));
      if(output.length)fragment.append(element('p','Output · '+output.map(countLabel).join(', '),'machine-next'));
      if(recipe)fragment.append(element('p',Object.entries(recipe.cost).map(countLabel).join(' + ')+' → '+Object.entries(recipe.out||{}).map(countLabel).join(' + '),'machine-next'));
      if(ports.length){
        const list=element('ul',null,'machine-ports');
        for(const p of ports){
          const [dx,dy]=E.DIRS[p.side],nx=x+dx,ny=y+dy;
          const connected=inside(nx,ny)&&E.ports(defs[get(nx,ny)],meta.get(idx(nx,ny))||{}).some(b=>b.side===(p.side+2)%4&&(E.compatible(p,b)||E.compatible(b,p)));
          const li=element('li',portSymbols[p.type]+' '+(portNames[p.type]||p.type)+' '+p.mode+' · '+directions[p.side]+(connected?' · linked':''));
          li.style.setProperty('--port-color',E.COLORS[p.type]);list.append(li);
        }
        fragment.append(list);
      }
      const detail=element('details',null,'machine-details');detail.open=!!detailsOpen;
      detail.append(element('summary','Machine details'));
      const rows=[['Orientation',directions[m.rot||0]],['Grid',String(diag.load)+' load / '+diag.supply+' supply']];
      if(d.mechanical)rows.push(['Torque',diag.torque.toFixed(1)+' / '+(m.requiredTorque||0).toFixed(1)],['Speed',(m.speed||0).toFixed(2)]);
      if(id===T.WINCH)rows.push(['Lift',m.guided?'Rail-guided':'Free travel'],['Load / counterweight',(m.loadMass||0)+' / '+(m.counterweightMass||0)]);
      if(d.transport)rows.push(['Storage',engine.stored(k)+' / '+engine.capacity(k)]);
      if(d.producer&&recipe)rows.push(['Progress',Math.round(diag.progress*100)+'%']);
      if(m.assembly)rows.push(['Assembly',m.assembly]);
      for(const [label,value] of rows){
        const row=element('div',null,'property');row.append(element('span',label),element('span',value));detail.append(row);
      }
      fragment.append(detail);el.replaceChildren(fragment);
    }
    const actionSignature=JSON.stringify([k,id,node,m.schematicClaimed,m.ruin,m.disabled,recipe?.key,drones?.role,drones?.deployed,drones?.ready,drones?.recalled,pad?.canLaunch,pad?.canRecall,pad?.canPack,inputs.map(key=>[key,inventory[key]||0])]);
    const controlsSignature=actionSignature+'|'+(fleet?.controlsKey(x,y)||'');
    if(actions.dataset.signature===controlsSignature)return;
    actions.dataset.signature=controlsSignature;actions.replaceChildren();
    const apply=fn=>()=>{
      if(inspected?.k!==k||get(x,y)!==id||distanceToTile(x,y)>6)return showToast('Move within reach of the machine.');
      fn();renderHotbar();simulateNetworks();updateInspector();
    };
    const add=(label,fn)=>{const button=element('button',label,'icon-btn');button.type='button';button.onclick=apply(fn);actions.append(button);};
    const collect=buffer=>{
      let total=0;
      for(const [key,n] of Object.entries(buffer)){
        const count=Math.min(n,inventoryRoom());
        if(count){inventory[key]=(inventory[key]||0)+count;buffer[key]-=count;total+=count;}
        if(!buffer[key])delete buffer[key];
      }
      showToast(total?'Collected '+total+' items.':inventoryRoom()?'No items to collect.':'Backpack full.');
    };
    if(m.schematic&&!m.schematicClaimed)add('Recover schematic',()=>discoverSchematic(m));
    if(m.surveyStation)add('Read survey bearings',()=>{
      surveyFromStation(m);
      saveGame();
      toggleModal('foundryMapPanel',true);
    });
    if(pad){
      add('Launch starship',()=>{if(starship.launch(x,y)){clearActions();closeInspector();}});
      if(pad.canRecall)add('Recall starship · 1 Flight Cell',()=>starship.recall(x,y));
      if(pad.canPack)add('Pack starship',()=>starship.recover());
    }
    if(d.foundryFleet)fleet?.appendControls(actions,x,y,{apply,closeInspector,
      refresh:()=>{delete actions.dataset.signature;}});
    if(drones?.role){
      const roleName={miner:'mining',collector:'collection',defense:'defensive'}[drones.role]||drones.role;
      add('Deploy '+roleName+' drone',()=>robotics.deploy(x,y,drones.role));
      if(drones.deployed){
        if(drones.recalled)add('Resume drone operations',()=>robotics.resume(x,y));
        else add('Recall drones',()=>robotics.recall(x,y));
        add('Pack docked drone',()=>robotics.retrieve(x,y,drones.role));
      }
    }
    if(m.ruin&&m.disabled)add('Repair machine',()=>configureComponent(id,k,m));
    if([T.FILTER,T.GEAR,T.WINCH,T.TIMER,T.PULSE,T.COUNTER,T.RECOVERY_STATION].includes(id))
      add(id===T.RECOVERY_STATION?'Set checkpoint':'Change settings',()=>configureComponent(id,k,m));
    if(d.machine?.recipes?.length){
      const choice=CircuitWorkshop.machineRecipes(d.machine.recipes,recipe,items,key=>apply(()=>{
        if(key!==engine.recipe(k)?.key){m.recipe=key;m.progress=0;meta.set(k,m);}
      })());
      if(choice)actions.append(choice);
    }
    if(d.transport){
      for(const key of inputs)add('Load '+(itemByKey.get(key)?.item.name||key)+' ('+(inventory[key]||0)+')',()=>{
        const limit=recipe?CircuitWorkshop.loadableInput(engine,k,recipe.cost,key):Infinity;
        const amount=engine.insert(k,key,Math.min(limit,inventory[key]||0));
        if(amount)inventory[key]-=amount;
        showToast(amount?'Loaded '+amount+' '+key+'.':'Input full, incompatible, or missing materials.');
      });
      add(d.producer?'Collect output':'Take contents',()=>collect(d.producer?m.outputs||{}:m.contents||{}));
      if(d.producer)add('Recover inputs',()=>collect(m.contents||{}));
    }
  }
  function renderFoundryInspector(rig){
    const {x,y,m,options,coolant}=rig;
    const props=document.getElementById('inspectProps'),actions=document.getElementById('machineActions');
    document.getElementById('inspectName').textContent=rig.rig.name;
    const outputs=Object.entries(m.outputs||{}).filter(([,count])=>count>0);
    const signature=JSON.stringify([x,y,rig.status,rig.detail,rig.steps,options,coolant,rig.integrity,outputs,inventory.coolantCanister||0,inventory.repairKit||0,inventoryRoom()]);
    if(props.dataset.signature===signature)return;
    props.dataset.signature=signature;
    actions.dataset.signature='';
    props.replaceChildren();
    actions.replaceChildren();
    const addText=(tag,text,className='machine-next',parent=props)=>{
      const el=document.createElement(tag);
      el.className=className;el.textContent=text;parent.append(el);return el;
    };
    addText('strong',rig.status,'machine-status');
    addText('p',rig.detail);
    addText('p',rig.steps.map(([name,done])=>(done?'✓ ':'')+name).join(' → '));
    if(coolant)addText('p',coolant.loaded+' coolant loaded · '+coolant.cuts+' cuts in current cycle');
    if(rig.integrity<100)addText('p',rig.integrity+'% rig integrity');
    if(outputs.length)addText('p','Buffered · '+outputs.map(([key,n])=>n+' '+(itemByKey.get(key)?.item.name||key)).join(', '));
    const add=(label,action)=>{
      const button=addText('button',label,'icon-btn',actions);
      button.type='button';
      button.onclick=()=>{
        if(!inspected||!industry.controller(x,y)||distanceToTile(inspected.x,inspected.y)>6)return;
        action();renderHotbar();simulateNetworks();industry.tick();updateInspector();saveGame();
      };
      return button;
    };
    if(options?.mode)add('Cutting · '+(options.mode==='fast'?'Fast':'Precision'),()=>industry.configure(x,y,'mode'));
    if(options?.left){
      add('Left limit · '+options.left+' tiles',()=>industry.configure(x,y,'left'));
      add('Right limit · '+options.right+' tiles',()=>industry.configure(x,y,'right'));
      add('Unload · '+options.unload,()=>industry.configure(x,y,'unload'));
    }
    if(coolant){
      const button=add('Load coolant ('+(inventory.coolantCanister||0)+')',()=>industry.configure(x,y,'coolant'));
      button.disabled=!(inventory.coolantCanister>0);
    }
    if(rig.integrity<100||m.ruin){
      const button=add('Install repair kit ('+(inventory.repairKit||0)+')',()=>industry.configure(x,y,'repair'));
      button.disabled=!(inventory.repairKit>0);
    }
    if(outputs.length)add('Collect buffered output',()=>{
      let moved=0;
      for(const [key,count] of Object.entries(m.outputs||{})){
        const n=Math.min(count,inventoryRoom());
        if(n<=0)break;
        inventory[key]=(inventory[key]||0)+n;
        m.outputs[key]-=n;
        if(!m.outputs[key])delete m.outputs[key];
        moved+=n;
      }
      showToast(moved?'Collected '+moved+' items.':'Backpack full.');
    });
  }
  function showToast(msg){if(/Warning|base under attack/i.test(msg))sound('warning');else if(/Chapter complete|discovered|Core online|Guardian defeated/i.test(msg))sound('discovery');const e=document.getElementById('toast');e.textContent=msg;e.classList.add('show');clearTimeout(toastTimer);toastTimer=setTimeout(()=>e.classList.remove('show'),1800)}
  function toggleLens(){
    lens=!lens;
    document.getElementById('lensBtn').classList.toggle('active',lens);
    document.getElementById('lensBtn').setAttribute('aria-pressed',String(lens));
    document.body.classList.toggle('lens-active',lens);
    if(!lens)document.getElementById('cursorLabel').style.display='none';
  }
  function toggleModal(id,force){
    const panel=document.getElementById(id);if(!panel)return;
    const open=force??!panel.classList.contains('show');
    if(!open&&!panel.classList.contains('show'))return;
    for(const other of document.querySelectorAll('.modal.show'))other.classList.remove('show');
    if(open){closeInspector();panel.classList.add('show');if(id==='manual')renderResearchManual();if(id==='foundryMapPanel')expeditionMap?.open();}
    craftOpen=id==='craftPanel'&&open;manualOpen=id==='manual'&&open;modalOpen=paused=open;
    clearActions();document.getElementById('cursorLabel').style.display='none';
    if(musicGain)musicGain.gain.value=open?0:settings.musicVolume*.025;
    if(craftOpen)renderCraft();
  }
  function openCrafting(){
    if(craftOpen){toggleModal('craftPanel',false);return;}
    workshop.open('craft','hand',guides?.objective()?.action==='craft-workbench'?'workbench':undefined);
  }
  function rotateHeld(){
    rotation=(rotation+1)%4;manualRotation=true;
    showToast('Placement · '+directions[rotation]);
  }
  function setSmartCursor(value){
    settings.smartCursor=!!value;saveGame();ui?.refresh();
  }
  function toggleSmartCursor(){
    setSmartCursor(!settings.smartCursor);showToast(settings.smartCursor?'Smart cursor':'Exact targeting');
  }
  function quickAttack(){
    if(survival?.attack(pointer.aimed?pointer.wx:player.x+player.facing*60,pointer.aimed?pointer.wy:player.y+15))sound('iron');
  }
  function combatDodge(){
    if(starship?.active)return;
    const left=keys.KeyA||keys.ArrowLeft||keys.touchleft,right=keys.KeyD||keys.ArrowRight||keys.touchright;
    survival?.dodge?.(Number(!!right)-Number(!!left));
  }
  function cycleWeapon(){
    survival?.cycleWeapon();
    if(selectedItem()?.weapon&&survival?.weapon)selectItem(survival.weapon,hotbar.indexOf(survival.weapon));
    ui?.refresh();
  }
  addEventListener('keydown',e=>{
    if(e.target?.matches('input,select,textarea,[contenteditable="true"]')||e.metaKey||e.ctrlKey||e.altKey)return;
    if(e.repeat&&!['KeyA','KeyD','KeyW','KeyS','ArrowLeft','ArrowRight','ArrowUp','ArrowDown','Space'].includes(e.code))return;
    const movement=['Space','ArrowUp','ArrowDown','ArrowLeft','ArrowRight'];
    if(!modalOpen&&movement.includes(e.code)&&!e.target?.matches('button'))e.preventDefault();
    if(modalOpen){
      if(e.code==='KeyE'&&craftOpen||e.code==='KeyH'&&manualOpen)
        toggleModal(craftOpen?'craftPanel':'manual',false);
      return;
    }
    if(e.target?.matches('button')&&e.code==='Space')return;
    keys[e.code]=true;
    if(/^Digit[0-9]$/.test(e.code)){
      const n=Number(e.code.slice(5)),slot=n===0?9:n-1;
      selectItem(hotbar[slot],slot);return;
    }
    switch(e.code){
      case 'KeyE':openCrafting();break;
      case 'KeyH':toggleModal('manual');break;
      case 'KeyC':toggleLens();break;
      case 'KeyG':toggleSmartCursor();break;
      case 'KeyR':
        if(e.shiftKey){
          const {tx:x,ty:y}=pointer,k=idx(x,y),id=get(x,y);
          if(inside(x,y)&&id&&!defs[id].foundryRig&&!defs[id].foundryPart&&E.ports(defs[id],{}).length&&distanceToTile(x,y)<=6){
            const m=meta.get(k)||{};m.rot=((m.rot||0)+1)%4;meta.set(k,m);engine.changed(x,y);simulateNetworks();showToast('Block rotated');
          }
        }else rotateHeld();
        break;
      case 'KeyV':cycleLens();break;
      case 'KeyL':cycleLink();break;
      case 'KeyF':
        if(fleet?.selectTarget(pointer.tx,pointer.ty))break;
        if(!starship?.active&&!e.shiftKey&&pickUpAimedDrop())break;
        if(starship?.active||industry?.controller(pointer.tx,pointer.ty))interactAtPointer();
        else if(e.shiftKey||defs[get(pointer.tx,pointer.ty)]?.transport)interactItems();
        else interactAtPointer();
        break;
      case 'KeyU':starship?.refuel();break;
      case 'KeyX':dropHeldItem();break;
      case 'KeyQ':if(!starship?.active)quickAttack();break;
      case 'KeyK':combatDodge();break;
      case 'KeyZ':survival?.heal();renderHotbar();ui?.refresh();break;
      case 'Tab':if(!inspected&&e.target===document.body){e.preventDefault();cycleWeapon();}break;
      case 'Escape':if(!fleet?.cancelTarget())toggleModal('pausePanel',true);break;
    }
  });
  addEventListener('keyup',e=>keys[e.code]=false);
  addEventListener('pagehide',saveGame);
  addEventListener('blur',()=>{clearActions();if(!modalOpen)toggleModal('pausePanel',true);});
  document.getElementById('lensBtn').onclick=toggleLens;
  document.getElementById('manualBtn').onclick=()=>toggleModal('manual');
  document.getElementById('pauseBtn').onclick=()=>toggleModal('pausePanel',true);
  document.getElementById('resumeBtn').onclick=()=>toggleModal('pausePanel',false);
  document.getElementById('saveBtn').onclick=()=>{saveGame();showToast('World saved');};
  document.getElementById('newBtn').onclick=()=>{if(confirm('Generate a new world? The current save will be replaced.')){generate();toggleModal('pausePanel',false);}};
  document.querySelectorAll('[data-close]').forEach(b=>b.addEventListener('click',()=>toggleModal(b.dataset.close,false)));
  document.querySelectorAll('[data-touch]').forEach(b=>{
    const action=b.dataset.touch,k='touch'+action;
    b.addEventListener('pointerdown',e=>{
      e.preventDefault();if(modalOpen)return;unlockAudio();
      b.setPointerCapture?.(e.pointerId);
      if(action==='rotate'){rotateHeld();return;}
      if(action==='aim'){toggleSmartCursor();return;}
      if(action==='interact'){interactAtPointer();return;}
      if(action==='dodge'){combatDodge();return;}
      if(action==='place'){touchConfirm={id:e.pointerId,x:pointer.tx,y:pointer.ty};return;}
      if(action==='attack'||action==='mine'){
        if(action==='attack'&&!pointer.aimed){quickAttack();return;}
        doPrimary(true);return;
      }
      keys[k]=true;
    });
    const release=e=>{
      e.preventDefault();keys[k]=false;
      if(action==='attack'&&e.type==='pointercancel')survival?.cancelCharge?.();
      if(action==='mine'){pointer.down=false;mining.key='';mining.progress=0;}
      if(action==='place'&&touchConfirm?.id===e.pointerId){
        const aimed=touchConfirm.x===pointer.tx&&touchConfirm.y===pointer.ty;
        touchConfirm=null;
        const rect=b.getBoundingClientRect();
        if(e.type==='pointerup'&&aimed&&!modalOpen&&e.clientX>=rect.left&&e.clientX<=rect.right&&e.clientY>=rect.top&&e.clientY<=rect.bottom)doPrimary(true);
      }
    };
    for(const name of ['pointerup','pointercancel','lostpointercapture'])b.addEventListener(name,release);
  });
  document.getElementById('weaponBtn').onclick=()=>{if(!modalOpen)cycleWeapon();};
  document.getElementById('healBtn').onclick=()=>{if(!modalOpen){survival?.heal();renderHotbar();ui?.refresh();}};
  setInterval(()=>{if(!paused)saveGame();},10000);
  function productionBudgets(dt){
    const previous=offsiteWork, budgets=new Map();
    const playerX=(player.x+player.w/2)/TILE,playerY=(player.y+player.h/2)/TILE;
    offsiteWork=new Map();
    return key=>{
      if(budgets.has(key))return budgets.get(key);
      const node=meta.get(key)?.nodeId;
      const nearby=Math.abs(key%WORLD_W-playerX)<=64&&Math.abs(Math.floor(key/WORLD_W)-playerY)<=64;
      const elapsed=(previous.get(node)||0)+dt;
      if(node&&!nearby&&elapsed<.25){offsiteWork.set(node,elapsed);budgets.set(key,0);return 0;}
      budgets.set(key,elapsed);return elapsed;
    };
  }
  function simulationStep(dt=.05){
    updateSensors();simulateNetworks(dt);updateMachines(dt);
    const budgetFor=productionBudgets(dt);
    engine.production?.(dt,{set,budgetFor,onMined:(key,n,x,y)=>{stats.automined=(stats.automined||0)+n;if(Math.abs(player.x/TILE-x)<64&&Math.abs(player.y/TILE-y)<64)burst(x*TILE+12,y*TILE+12,palette[key]||'#9cbcb9');},onProduced:(key,n)=>{stats.produced=(stats.produced||0)+n;}});
    industry?.tick(dt);
    engine.logistics(dt,{budgetFor});robotics?.tick(dt);
    fleet?.tick(dt);
    if(survival){survival.tick(dt,gameTime);const targets=survival.enemies.map(e=>({id:e.id,x:e.x/TILE,y:e.y/TILE,health:e.hp,enemy:e}));engine.defenses?.(dt,{targets,damage:(t,n,kind)=>{survival.hit(t.enemy,n,kind==='electric'?'defense':kind);t.health=t.enemy.hp;},heal:(x,y,n)=>{if(Math.hypot((player.x+player.w/2)/TILE-x,(player.y+player.h/2)/TILE-y)>3)return 0;const amount=Math.max(0,Math.min(n,100-player.health));player.health+=amount;return amount;},lineOfSight,canClose:(x,y)=>!playerOverlapsTile(x,y)});}
    updateRegionExposure(dt);
    collectDrops();updateParticles(dt);if(gameTime-lastAmbient>4){lastAmbient=gameTime;if(engine.active.some(k=>meta.get(k)?.working))sound('build');}
  }
  function tick(t){
    const dt=Math.min(.1,(t-last)/1000||0);last=t;
    if(!paused&&!craftOpen&&!manualOpen){
      gameTime+=dt;
      if(starship?.active){starship.tick(dt,keys);followPlayer(dt);}
      else{starship?.tick(dt,{});for(let remaining=dt;remaining>0;remaining-=.016)movePlayer(Math.min(.016,remaining));}
      if(pointer.aimed&&!touchAim&&!buildDrag&&!linkDrag){
        pointer.wx=pointer.x+camera.x;pointer.wy=pointer.y+camera.y;
        pointer.tx=Math.floor(pointer.wx/TILE);pointer.ty=Math.floor(pointer.wy/TILE);
      }
      mineStep(dt);accum+=dt;
      while(accum>=.05){accum-=.05;simulationStep(.05);}
      if(Math.floor(gameTime*4)!==lastHud){lastHud=Math.floor(gameTime*4);guides?.tick();renderMissions();renderFlightStatus();updateInspector();updateExpedition();document.getElementById('healthText').textContent=Math.max(0,player.health|0);document.getElementById('healthBar').style.width=player.health+'%'}
    }render();requestAnimationFrame(tick);
  }
  let lastHud=-1;
  const expeditionPosition=()=>({x:Math.floor((player.x+player.w/2)/TILE),y:Math.floor((player.y+player.h/2)/TILE)});
  let regionWarning='';
  function regionExposure(){
    const {x,y}=expeditionPosition();
    if(y<196||starship?.active||x%26<8||hash(Math.floor(x/26),Math.floor(y/12))<.48)return null;
    const region=globalThis.CircuitDeepWorld?.regionAt(y).id;
    if(region==='gravityHollows')return {region,message:'Low-gravity pocket'};
    if(region==='nullCrystalVaults')return {region,message:'Null interference · tool charge draining'};
    const phase=(gameTime+x*.075)%19;
    return phase>=11?{region,message:phase<14?'Heat surge building · leave the pocket':'Heat surge · retreat to the shaft',hot:phase>=14}:null;
  }
  function updateRegionExposure(dt){
    const exposure=regionExposure();
    const warning=exposure?.message||'';
    if(warning&&regionWarning!==warning)showToast(warning);
    regionWarning=warning;
    if(exposure?.region==='nullCrystalVaults'&&survival)survival.energy=Math.max(0,survival.energy-dt*3);
    if(exposure?.hot)survival?.damagePlayer(5,'mantle heat');
  }
  function deepObjective(){
    if(player.y/TILE<84||!globalThis.CircuitDeepIndustry)return null;
    const carried=key=>(inventory[key]||0)>0;
    for(const {key,name} of globalThis.CircuitDeepIndustry.rigs){
      if(carried(key)||engine.active.some(k=>defs[get(...engine.xy(k))]?.drop===key))return {
        title:'Develop a deep outpost',detail:'Inspect your rig for the next connection. Use the map to log a freight route.'
      };
    }
    return {title:'Locate an Osmium seam',detail:'Read a survey station, then build a Deepcore Borer at a Field Workbench.'};
  }
  function arrivalAt(stop){
    for(const [dx,dy] of [[0,0],[-1,0],[1,0],[-2,0],[2,0],[0,-1],[-1,-1],[1,-1]]){
      const x=(stop.x+dx+.5)*TILE-player.w/2,y=(stop.y+dy+1)*TILE-player.h;
      if(!rectCollides(x,y,player.w,player.h)&&rectCollides(x,y+3,player.w,player.h,3))return {x,y};
    }
    return null;
  }
  function freightTrip(destination){
    if(starship?.active)return {reason:'Land the Starship before using rail travel.'};
    const origin=fleet?.stations().filter(stop=>stop.powered&&distanceToTile(stop.x,stop.y)<=5)
      .sort((a,b)=>distanceToTile(a.x,a.y)-distanceToTile(b.x,b.y))[0];
    if(!origin)return {reason:'Move next to a powered freight stop.'};
    if(origin.x===destination.x&&origin.y===destination.y)return {reason:'You are at this stop.'};
    if(!destination.powered)return {reason:'Power the destination freight stop.'};
    if(!fleet.routeBetween(origin,destination)?.length)return {reason:'Build a continuous freight rail to this stop.'};
    const arrival=arrivalAt(destination);
    return arrival?{ok:true,arrival}:{reason:'Clear two tiles of standing room beside the stop.'};
  }
  function updateExpedition(){
    if(!world?.reveal)return;
    const {x,y}=expeditionPosition();
    world.reveal(x,y);
    if(y>=120)activateFoundryEncounters(x,y);
    const marker=expeditionMap?.pinned();
    const waypoint=document.getElementById('foundryWaypoint');
    if(!waypoint)return;
    waypoint.hidden=!marker;
    if(marker){
      const dx=marker.x-x,dy=marker.y-y;
      const h=Math.abs(dx)>2?`${Math.abs(dx)} ${dx>0?'east':'west'}`:'';
      const v=Math.abs(dy)>2?`${Math.abs(dy)} ${dy>0?'down':'up'}`:'';
      waypoint.textContent=[marker.name,h,v].filter(Boolean).join(' · ');
    }
  }
  function activateFoundryEncounters(x,y){
    if(!survival)return;
    const encounters=[['plan-anchorHarpoon','crawler',10,10],['plan-prismRifle','drone',6,58]];
    const visited=stats.foundryEncounters||[];
    for(const [id,type,offset,rise] of encounters){
      if(visited.includes(id))continue;
      const site=world.sitesById.get(id);
      if(!site||Math.hypot(site.x-x,site.y-y)>15)continue;
      for(const direction of [1,-1]){
        const ex=(site.x+direction*offset+.5)*TILE,ey=(site.y+1)*TILE-rise;
        if(Math.hypot(ex/TILE-x,ey/TILE-y)<5||!survival.spawn(type,ex,ey,{ignoreDistance:true}))continue;
        visited.push(id);stats.foundryEncounters=visited;break;
      }
    }
  }
  const expeditionMap=globalThis.CircuitFoundryMap?.create({
    panel:document.getElementById('foundryMapPanel'),getWorld:()=>world,locate:expeditionPosition,
    terrain:(x,y)=>!!defs[get(x,y)]?.solid,
    stations:()=>fleet?.stations()||[],
    lifts:()=>engine.active.flatMap(k=>{
      const [x,y]=engine.xy(k),m=meta.get(k);
      return get(x,y)===T.WINCH&&m?.guided?[{x,y,name:'Guided lift',powered:!!(m.powered||m.running)}]:[];
    }),
    canTravel:freightTrip,
    travel:destination=>{
      const {ok,reason,arrival}=freightTrip(destination);
      if(!ok)return reason;
      Object.assign(player,arrival,{vx:0,vy:0,onGround:false});
      riderMotion=null;
      followPlayer(1);
      updateExpedition();
      toggleModal('foundryMapPanel',false);
      saveGame();
      showToast(`Arrived at ${destination.name}.`);
      return '';
    },
    canScan:()=>!!inventory.handheldScanner,
    scan:()=>{
      if(!inventory.handheldScanner)return 'Craft a Handheld Scanner at a Field Workbench.';
      const {x,y}=expeditionPosition();
      const results=world.survey(x,y,38,true);
      if(results.length)saveGame();
      return results.length?`${results.length} deposit ${results.length===1?'marked':'markers added'}.`:'No unmarked deposit within scanner range.';
    },
    save:saveGame
  });
  function renderFlightStatus(){
    const box=document.getElementById('flightStatus');if(!box)return;
    const flying=starship?.status();box.hidden=!flying?.nearby&&!flying?.piloted;
    if(box.hidden)return;
    document.getElementById('flightReadout').textContent=flying.status+' · Fuel '+flying.fuelPercent+'%';
    document.getElementById('flightFuelBtn').hidden=!flying.canRefuel;
    document.getElementById('flightPackBtn').hidden=flying.piloted||!flying.landed;
  }
  const vehicleOpts={T,defs,inventory,stats,player,engine,get,set,getMeta:(x,y)=>meta.get(idx(x,y)),inside,
    notify:showToast,save:saveGame,inventoryRoom,lineOfSight,tile:TILE,width:WORLD_W,height:WORLD_H,cols:WORLD_W,rows:WORLD_H,
    enemies:()=>survival?.enemies||[],hit:(enemy,n)=>survival?.hit(enemy,n,'defense')};
  robotics=CircuitRobotics.create(vehicleOpts);
  robotics.restore(loadedRobotics);
  fleet=CircuitFoundryAutomation.create({...vehicleOpts,items,getSurvival:()=>survival,
    survey:(x,y,radius)=>world.survey(x,y,radius,true),
    getBlueprint:()=>blueprint,
    rigAt:(x,y)=>industry?.controller(x,y),
    canPlace:(x,y,id)=>!(defs[id]?.solid&&playerOverlapsTile(x,y))&&!starship?.hitTest(x,y)&&supportedRope(x,y,id)});
  fleet.restore(loadedFleet);
  starship=CircuitStarship.create(vehicleOpts);
  starship.restore(loadedStarship);
  document.getElementById('flightFuelBtn')?.addEventListener('click',()=>{starship.refuel();renderHotbar();renderFlightStatus();});
  document.getElementById('flightPackBtn')?.addEventListener('click',()=>{starship.recover();renderHotbar();renderFlightStatus();});
  workshop=CircuitWorkshop.create({inventory,items,recipes,defs,T,player,tile:TILE,get,distanceToTile,
    getSurvival:()=>survival,getEngine:()=>engine,getHotbar:()=>hotbar,inventoryRoom,stats,assignToHotbar,
    readyToPlace:key=>{
      selectItem(key,hotbar.indexOf(key));toggleModal('craftPanel',false);
      queueMicrotask(()=>canvas.focus({preventScroll:true}));
    },
    sound,notify:showToast,refreshHotbar:renderHotbar,openCraft:()=>toggleModal('craftPanel',true),save:saveGame});
  guides=CircuitGuides.create({T,items,inventory,stats,get,inside,
    getMeta:(x,y)=>meta.get(idx(x,y)),
    canPlace:placementBlock,
    place:(x,y,item,rot,extra)=>{
      if(!commitPlacement(x,y,item,rot,extra))return false;
      renderHotbar();simulateNetworks();return true;
    },
    link:cells=>engine.link(cells.map(([x,y])=>idx(x,y))),
    notify:showToast,save:saveGame});
  guides.restore(loadedGuide,{legacy:loaded&&!loadedGuide});
  ui=CircuitInterface.create({
    getState:()=>({
      mode:starship.active?'interact':blueprint||guides.isPlacing()?'build':selectedAction(),flying:starship.active,
      ...settings,smartPlacement,inventory,items,selected:selectedItem(),
      progression:survival?.getJournal(),hud:survival?.getHud(),
      blueprint,guide:guides.current(),firstRun:!loaded,inspectorOpen:!!inspected
    }),
    setSmartCursor,
    setSmartPlacement:value=>{settings.smartPlacement=smartPlacement=!!value;manualRotation=false;saveGame();},
    setDifficulty:value=>{if(survival?.setDifficulty(value)){settings.difficulty=value;saveGame();}},
    setAudio:values=>{Object.assign(settings,values);if(musicGain)musicGain.gain.value=paused?0:settings.musicVolume*.025;saveGame();},
    openModal:toggleModal,openWorkshop:view=>view==='craft'?openCrafting():workshop.open(view),
    selectBlueprint:plan=>{blueprint=plan;guides.pause();renderHotbar();showToast('Aim near clear ground to place the assembly.');},
    cancelBlueprint:()=>{blueprint=null;renderHotbar();},
    startGuide:plan=>{blueprint=null;guides.start(plan);renderHotbar();},
    pauseGuide:()=>guides.pause(),resumeGuide:()=>{blueprint=null;guides.resume();},
    getBlueprintStatus:blueprintStatus,closeInspector,start:unlockAudio
  });
  renderHotbar();simulateNetworks();renderMissions();requestAnimationFrame(tick);
})();
