(() => {
  'use strict';
  const canvas=document.getElementById('game'),ctx=canvas.getContext('2d',{alpha:false});
  const TILE=24,WORLD_W=220,WORLD_H=84,SAVE_KEY='circuitbound-save-v1';
  let vw=innerWidth,vh=innerHeight,dpr=Math.min(devicePixelRatio||1,2),world,meta=new Map(),seed=0,gameTime=0,paused=false,craftOpen=false,manualOpen=false,lens=false,last=0,accum=0,toastTimer=0,selected=0,rotation=0,pointer={x:0,y:0,wx:0,wy:0,tx:0,ty:0,down:false,button:0},mining={key:'',progress:0},linkDrag=null;
  const E=CircuitEngineering;
  let engine=null, savedEngineering=null, lensMode='ports', linkMode='new', selectedAssembly=null;
  const camera={x:0,y:0,shake:0},keys={};
  let ledgeDrop=0;
  const player={x:9*TILE,y:20*TILE,w:15,h:34,vx:0,vy:0,onGround:false,facing:1,health:100,walk:0};
  const stats={copperMined:0,crafted:0,motionEvents:0,coreFound:false,coreReached:false,started:Date.now()};
  const STARTING={dirt:28,stone:24,wood:18,copper:12,iron:10,crystal:5,copperBlock:6,ironBlock:4,wire:16,signal:12,generator:1,switch:1,sensor:1,motor:1,shaft:8,gear:2,piston:1,lamp:2,conveyor:6,winch:1,platform:12,door:2,ladder:8,ledge:8};
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
  const nextCustomTile=Math.max(...Object.values(T).filter(Number.isInteger))+1;
  T.DOOR=nextCustomTile;T.LADDER=nextCustomTile+1;T.LEDGE=nextCustomTile+2;
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
  const palette={dirt:'#78543a',stone:'#647176',wood:'#a67240',copper:'#c77743',iron:'#8fa0a1',crystal:'#62e2e0',obsidian:'#352d49'};
  const missionDefs=[['copperMined','Mine 8 copper ore'],['crafted','Fabricate any machine part'],['grid','Power a crystal lamp'],['motionEvents','Move a linked structure'],['coreReached','Power the eastern Foundry Core']];

  function resize(){vw=innerWidth;vh=innerHeight;dpr=Math.min(devicePixelRatio||1,2);canvas.width=Math.floor(vw*dpr);canvas.height=Math.floor(vh*dpr);canvas.style.width=vw+'px';canvas.style.height=vh+'px';ctx.setTransform(dpr,0,0,dpr,0,0);ctx.imageSmoothingEnabled=false}
  addEventListener('resize',resize);resize();
  const idx=(x,y)=>x+y*WORLD_W,inside=(x,y)=>x>=0&&y>=0&&x<WORLD_W&&y<WORLD_H,get=(x,y)=>inside(x,y)?world[idx(x,y)]:T.STONE;
  function set(x,y,v,m){if(!inside(x,y))return;world[idx(x,y)]=v;const k=idx(x,y);if(v===0)meta.delete(k);else if(m)meta.set(k,{...m});engine?.changed(x,y)}
  function hash(x,y,s=seed){let n=x*374761393+y*668265263+s*1442695041;n=(n^(n>>13))*1274126177;return((n^(n>>16))>>>0)/4294967295}
  function groundAt(x){return 31+Math.floor(Math.sin(x*.12)*2+Math.sin(x*.035)*5+hash(x,1)*2)}
  function generate(){savedEngineering=null;seed=Math.floor(Math.random()*999999);world=new Uint8Array(WORLD_W*WORLD_H);meta=new Map();
    for(let x=0;x<WORLD_W;x++){const g=groundAt(x);for(let y=g;y<WORLD_H;y++){let id=y===g?T.GRASS:y<g+5?T.DIRT:T.STONE;const r=hash(x,y);if(y>g+5&&r>.93)id=T.COPPER;if(y>g+9&&r<.045)id=T.IRON;if(y>g+14&&r>.973)id=T.CRYSTAL;if(y>WORLD_H-7&&r>.45)id=T.OBSIDIAN;world[idx(x,y)]=id}}
    for(let c=0;c<34;c++){const cx=12+Math.floor(hash(c,60)*198),cy=37+Math.floor(hash(c,61)*36),rx=3+Math.floor(hash(c,62)*7),ry=2+Math.floor(hash(c,63)*4);for(let x=cx-rx;x<=cx+rx;x++)for(let y=cy-ry;y<=cy+ry;y++)if(((x-cx)/rx)**2+((y-cy)/ry)**2<1&&inside(x,y))set(x,y,T.AIR)}
    for(let x=4;x<14;x++)set(x,29,T.PLATFORM);set(5,28,T.GENERATOR,{rot:0});set(6,28,T.WIRE,{});set(7,28,T.WIRE,{});set(8,28,T.LAMP,{});
    for(const x of [18,43,78,119,151,183]){const g=groundAt(x);for(let y=g-1;y>g-5;y--)set(x,y,T.WOOD);for(let tx=x-2;tx<=x+2;tx++)for(let ty=g-8;ty<=g-4;ty++)if(Math.abs(tx-x)+Math.abs(ty-(g-6))<4&&get(tx,ty)===T.AIR)set(tx,ty,T.WOOD)}
    const coreX=205,base=groundAt(coreX);for(let x=201;x<=209;x++)for(let y=base-7;y<base;y++)if(x===201||x===209||y===base-7)set(x,y,T.IRON_BLOCK);set(coreX,base-3,T.CORE,{});for(let x=203;x<=207;x++)set(x,base-2,T.WIRE,{});
    player.x=9*TILE;player.y=(groundAt(9)-3)*TILE;player.vx=player.vy=0;player.health=100;gameTime=0;accum=0;selectedAssembly=null;selected=0;rotation=0;for(const key of Object.keys(inventory))delete inventory[key];Object.assign(inventory,STARTING);Object.assign(stats,{copperMined:0,crafted:0,motionEvents:0,coreFound:false,coreReached:false,started:Date.now()});if(engine){engine.reset();simulateNetworks();renderHotbar();renderMissions()}showToast('Field kit issued — wake the eastern Foundry Core');saveGame()}
  function saveGame(){try{const data={seed,world:Array.from(world),meta:Array.from(meta.entries()),player:{x:player.x,y:player.y,health:player.health},inventory,stats,gameTime,version:2,engineering:engine?.export()};localStorage.setItem(SAVE_KEY,JSON.stringify(data))}catch(e){}}
  function loadGame(){try{const d=JSON.parse(localStorage.getItem(SAVE_KEY));if(!d||!d.world||d.world.length!==WORLD_W*WORLD_H)return false;seed=d.seed;world=Uint8Array.from(d.world);meta=new Map(d.meta||[]);Object.assign(player,d.player||{});Object.assign(inventory,d.inventory||{});Object.assign(stats,d.stats||{});gameTime=d.gameTime||0;savedEngineering=d.engineering;return true}catch(e){return false}}
  if(!loadGame())generate();
  engine=new E.Simulation({width:WORLD_W,height:WORLD_H,get,meta:()=>meta,defs,T});
  engine.reset();engine.restore(savedEngineering);
  setupEngineeringUI();

  function tileRect(x,y){return{x:x*TILE-camera.x,y:y*TILE-camera.y}}
  function drawSky(){const day=(Math.sin(gameTime*.012)+1)/2,top=mixColor('#071221','#24536a',day),bot=mixColor('#17303d','#79a8a0',day),g=ctx.createLinearGradient(0,0,0,vh);g.addColorStop(0,top);g.addColorStop(1,bot);ctx.fillStyle=g;ctx.fillRect(0,0,vw,vh);ctx.fillStyle=day>.3?'#ffe6a1':'#d9efff';const sunX=((gameTime*.9)%(vw+180))-90;ctx.fillRect(Math.floor(sunX),55,24,24);for(let i=0;i<45;i++){const x=(hash(i,8)*vw*1.4-camera.x*.03)%(vw+20),y=20+hash(i,9)*vh*.45;if(day<.35){ctx.fillStyle=`rgba(220,245,255,${.2+(1-day)*.6})`;ctx.fillRect(x,y,hash(i,10)>.7?2:1,hash(i,10)>.7?2:1)}}ctx.fillStyle='rgba(18,45,57,.58)';ctx.beginPath();ctx.moveTo(0,vh*.72);for(let x=0;x<=vw;x+=70){const wx=x+camera.x*.16;ctx.lineTo(x,vh*.57+Math.sin(wx*.006)*42+hash(Math.floor(wx/70),23)*50)}ctx.lineTo(vw,vh);ctx.lineTo(0,vh);ctx.fill();ctx.fillStyle='rgba(9,29,39,.72)';ctx.beginPath();ctx.moveTo(0,vh*.8);for(let x=0;x<=vw;x+=55){const wx=x+camera.x*.28;ctx.lineTo(x,vh*.68+Math.sin(wx*.009)*30+hash(Math.floor(wx/55),25)*40)}ctx.lineTo(vw,vh);ctx.lineTo(0,vh);ctx.fill()}
  function mixColor(a,b,t){const pa=parseInt(a.slice(1),16),pb=parseInt(b.slice(1),16),ar=pa>>16,ag=pa>>8&255,ab=pa&255,br=pb>>16,bg=pb>>8&255,bb=pb&255;return`rgb(${ar+(br-ar)*t|0},${ag+(bg-ag)*t|0},${ab+(bb-ab)*t|0})`}
  function drawTile(id,x,y,m){const p=tileRect(x,y),sx=Math.floor(p.x),sy=Math.floor(p.y);if(sx<-TILE||sy<-TILE||sx>vw||sy>vh)return;const n=hash(x,y);ctx.save();ctx.translate(sx,sy);
    if(id===T.GRASS){ctx.fillStyle='#725038';ctx.fillRect(0,0,TILE,TILE);ctx.fillStyle='#5b9a55';ctx.fillRect(0,0,TILE,6);ctx.fillStyle='#86be62';for(let i=0;i<4;i++)ctx.fillRect((n*97+i*7)%22,1,2,4)}
    else if(id===T.DIRT){ctx.fillStyle='#755139';ctx.fillRect(0,0,TILE,TILE);ctx.fillStyle='#916645';ctx.fillRect(3+(n*11)%10,5,3,3);ctx.fillRect(15,16,2,2)}
    else if(id===T.STONE){ctx.fillStyle='#5d6b70';ctx.fillRect(0,0,TILE,TILE);ctx.fillStyle='#76858a';ctx.fillRect(3,4,7,3);ctx.fillRect(14,14,6,4);ctx.fillStyle='#46545a';ctx.fillRect(1,18,8,2);ctx.fillRect(17,5,4,3)}
    else if(id===T.WOOD){ctx.fillStyle='#926136';ctx.fillRect(0,0,TILE,TILE);ctx.fillStyle='#ba7c43';ctx.fillRect(3,0,3,TILE);ctx.fillRect(15,0,2,TILE);ctx.fillStyle='#6d462b';ctx.fillRect(7,8,5,2);ctx.fillRect(17,17,4,2)}
    else if(id>=T.COPPER&&id<=T.OBSIDIAN){drawOre(id,n)}
    else if(id===T.PLANK||id===T.PLATFORM){ctx.fillStyle='#9b6a3e';ctx.fillRect(0,6,TILE,12);ctx.fillStyle='#d19a58';ctx.fillRect(0,6,TILE,3);ctx.fillStyle='#65452d';ctx.fillRect(5,9,2,9);ctx.fillRect(18,9,2,9)}
    else if(id===T.COPPER_BLOCK||id===T.IRON_BLOCK){ctx.fillStyle=id===T.COPPER_BLOCK?'#b76f42':'#778b8f';ctx.fillRect(0,0,TILE,TILE);ctx.fillStyle=id===T.COPPER_BLOCK?'#e1a06b':'#a8b7b7';ctx.fillRect(2,2,20,3);ctx.fillRect(2,2,3,20);ctx.fillStyle='#27373b';ctx.fillRect(3,3,3,3);ctx.fillRect(18,3,3,3);ctx.fillRect(3,18,3,3);ctx.fillRect(18,18,3,3)}
    else if(id===T.WIRE||id===T.SIGNAL){const powered=m&&m.powered;ctx.strokeStyle=id===T.WIRE?(powered?'#fff06a':'#c87842'):(m&&m.active?'#ff8376':'#b63e45');ctx.lineWidth=powered?4:3;ctx.beginPath();ctx.moveTo(0,12);ctx.lineTo(24,12);ctx.moveTo(12,0);ctx.lineTo(12,24);ctx.stroke();ctx.fillStyle=ctx.strokeStyle;ctx.fillRect(9,9,6,6)}
    else if(id===T.GENERATOR){ctx.fillStyle='#2c454a';ctx.fillRect(1,3,22,19);ctx.fillStyle='#d29b3f';ctx.fillRect(4,6,16,12);ctx.fillStyle='#15272d';ctx.fillRect(8,8,8,8);ctx.fillStyle='#fff09c';ctx.fillRect(11,6,3,5);ctx.fillRect(9,10,3,5)}
    else if(id===T.SWITCH){ctx.fillStyle='#273b3e';ctx.fillRect(4,7,16,13);ctx.strokeStyle=m&&m.on?'#95ef75':'#d2cec1';ctx.lineWidth=4;ctx.beginPath();ctx.moveTo(12,13);ctx.lineTo(m&&m.on?18:7,5);ctx.stroke()}
    else if(id===T.SENSOR){ctx.fillStyle='#28444a';ctx.fillRect(3,8,18,12);ctx.strokeStyle=m&&m.active?'#74fff1':'#64918f';ctx.lineWidth=2;ctx.beginPath();ctx.arc(12,12,6,Math.PI,0);ctx.stroke();ctx.fillStyle=m&&m.active?'#d9fff8':'#496b6d';ctx.fillRect(10,10,4,4)}
    else if(id===T.MOTOR){machineBox('#2a6063');ctx.strokeStyle=m&&m.running?'#82fff2':'#8cadac';ctx.lineWidth=3;ctx.beginPath();ctx.arc(12,12,6,0,Math.PI*2);ctx.stroke();ctx.save();ctx.translate(12,12);ctx.rotate((m&&m.running?gameTime*.15:0));ctx.fillStyle=ctx.strokeStyle;ctx.fillRect(-1,-8,2,16);ctx.fillRect(-8,-1,16,2);ctx.restore()}
    else if(id===T.SHAFT){ctx.save();ctx.translate(12,12);ctx.rotate(((m&&m.spin)||0)+((m&&m.rot)||0)*Math.PI/2);ctx.fillStyle=m&&m.running?'#d6f5e9':'#8b9b9c';ctx.fillRect(-12,-3,24,6);ctx.fillStyle='#34474b';for(let q=-8;q<=8;q+=8)ctx.fillRect(q,-5,3,10);ctx.restore()}
    else if(id===T.GEAR){machineBox(m&&m.mode===0?'#6d5940':m&&m.mode===2?'#5d446d':'#455e61');ctx.save();ctx.translate(12,12);ctx.rotate((m&&m.running?gameTime*.12:0));ctx.strokeStyle=m&&m.running?'#ffe68a':'#a9b5ae';ctx.lineWidth=4;ctx.beginPath();ctx.arc(0,0,m&&m.mode===0?7:m&&m.mode===2?4:6,0,Math.PI*2);ctx.stroke();for(let a=0;a<8;a++){ctx.rotate(Math.PI/4);ctx.fillStyle=ctx.strokeStyle;ctx.fillRect(5,-2,6,4)}ctx.restore()}
    else if(id===T.PISTON){ctx.save();ctx.translate(12,12);ctx.rotate(((m&&m.rot)||0)*Math.PI/2);ctx.translate(-12,-12);ctx.fillStyle='#3b5054';ctx.fillRect(2,3,14,18);ctx.fillStyle=m&&m.running?'#82dcd2':'#87989a';ctx.fillRect(15,9,m&&m.extended?9:6,6);ctx.fillStyle='#d4b665';ctx.fillRect(m&&m.extended?20:17,6,4,12);ctx.restore()}
    else if(id===T.LAMP){ctx.fillStyle='#263c41';ctx.fillRect(7,16,10,6);const on=m&&m.powered&&m.signalOK;if(on){const g=ctx.createRadialGradient(12,10,1,12,10,18);g.addColorStop(0,'#fff9b5');g.addColorStop(1,'rgba(255,214,70,0)');ctx.fillStyle=g;ctx.fillRect(-8,-10,40,40)}ctx.fillStyle=on?'#fff172':'#71888a';ctx.fillRect(6,4,12,13);ctx.fillStyle=on?'#fffbd2':'#9bb0ad';ctx.fillRect(9,6,6,8)}
    else if(id===T.CONVEYOR){ctx.fillStyle='#354b50';ctx.fillRect(0,7,24,13);ctx.fillStyle=m&&m.running?'#63d6c7':'#789092';ctx.fillRect(0,5,24,5);ctx.fillStyle='#18292e';for(let q=3;q<24;q+=7)ctx.fillRect(q,12,3,5)}
    else if(id===T.WINCH){machineBox('#4b5552');ctx.strokeStyle=m&&m.running?'#f0cf70':'#a1aeaa';ctx.lineWidth=3;ctx.beginPath();ctx.arc(12,12,7,0,Math.PI*2);ctx.stroke();ctx.fillStyle='#33291f';ctx.fillRect(10,10,4,14)}
    else if(id===T.DOOR){ctx.fillStyle='#503724';ctx.fillRect(1,1,22,22);if(m&&m.open){ctx.fillStyle='#b97b43';ctx.fillRect(18,2,4,20);ctx.fillStyle='#f0c36c';ctx.fillRect(18,11,2,2)}else{ctx.fillStyle='#a96f3e';ctx.fillRect(3,2,18,20);ctx.fillStyle='#754925';ctx.fillRect(6,5,11,5);ctx.fillRect(6,13,11,6);ctx.fillStyle='#f0c36c';ctx.fillRect(17,11,2,2)}}
    else if(id===T.LADDER){ctx.fillStyle='#b57b42';ctx.fillRect(4,0,3,24);ctx.fillRect(17,0,3,24);ctx.fillStyle='#d8a65d';for(let q=3;q<24;q+=6)ctx.fillRect(5,q,14,2)}
    else if(id===T.LEDGE){ctx.fillStyle='#644327';ctx.fillRect(0,7,24,5);ctx.fillStyle='#d5a260';ctx.fillRect(0,5,24,3);ctx.fillStyle='#9b6838';ctx.fillRect(3,12,3,4);ctx.fillRect(18,12,3,4)}
    else if(id===T.CORE){ctx.fillStyle='#172f35';ctx.fillRect(0,0,24,24);ctx.strokeStyle=m&&m.powered?'#fff174':'#5de4df';ctx.lineWidth=2;ctx.strokeRect(3,3,18,18);ctx.save();ctx.translate(12,12);ctx.rotate(gameTime*.04);ctx.fillStyle=m&&m.powered?'#fff5a2':'#5de4df';ctx.fillRect(-3,-9,6,18);ctx.fillRect(-9,-3,18,6);ctx.restore()}
    if(id>=26&&defs[id].kind){machineBox(E.COLORS[defs[id].kind]);ctx.fillStyle='#10212b';ctx.font='bold 12px monospace';ctx.textAlign='center';ctx.fillText(defs[id].icon,12,16);ctx.textAlign='left'}
    if(m&&m.linked&&lens){ctx.strokeStyle=assemblyColor(m.assembly);ctx.lineWidth=2;ctx.setLineDash([4,3]);ctx.strokeRect(1,1,22,22);ctx.setLineDash([])}if(lens&&m){if(m.powered){ctx.strokeStyle='#fff26a';ctx.lineWidth=2;ctx.strokeRect(2,2,20,20)}if(m.active){ctx.fillStyle='#ff5454';ctx.fillRect(9,1,6,3)}if(m.running){ctx.fillStyle='#5ff1dc';ctx.fillRect(1,19,22,3)}if(m.overload){ctx.fillStyle=gameTime%4<2?'#ff4d35':'#fff';ctx.fillRect(2,2,4,4);ctx.fillRect(18,17,3,3)}}drawEngineeringOverlay(id,m||{},x,y);ctx.restore();
    function machineBox(c){ctx.fillStyle=c;ctx.fillRect(1,2,22,20);ctx.fillStyle='#12272d';ctx.fillRect(3,4,3,3);ctx.fillRect(18,4,3,3);ctx.fillRect(3,17,3,3);ctx.fillRect(18,17,3,3)}
  }
  function drawOre(id,n){ctx.fillStyle=id===T.OBSIDIAN?'#29243a':'#515f64';ctx.fillRect(0,0,24,24);const c=id===T.COPPER?'#d27d45':id===T.IRON?'#aab8b7':id===T.CRYSTAL?'#58dfdc':'#5a4b76';ctx.fillStyle=c;for(let i=0;i<4;i++){const x=3+((n*131+i*7)%16),y=3+((n*79+i*11)%16);ctx.fillRect(x,y,id===T.CRYSTAL?3:5,id===T.CRYSTAL?8:4)}}
  function drawWorld(){const x0=Math.max(0,Math.floor(camera.x/TILE)-1),x1=Math.min(WORLD_W-1,Math.ceil((camera.x+vw)/TILE)+1),y0=Math.max(0,Math.floor(camera.y/TILE)-1),y1=Math.min(WORLD_H-1,Math.ceil((camera.y+vh)/TILE)+1);for(let y=y0;y<=y1;y++)for(let x=x0;x<=x1;x++){const id=get(x,y);if(id)drawTile(id,x,y,meta.get(idx(x,y)))}if(linkDrag){const a=tileRect(Math.min(linkDrag.x0,linkDrag.x1),Math.min(linkDrag.y0,linkDrag.y1)),w=(Math.abs(linkDrag.x1-linkDrag.x0)+1)*TILE,h=(Math.abs(linkDrag.y1-linkDrag.y0)+1)*TILE;ctx.fillStyle='rgba(72,255,223,.13)';ctx.fillRect(a.x,a.y,w,h);ctx.strokeStyle='#6affdf';ctx.lineWidth=2;ctx.strokeRect(a.x,a.y,w,h)}if(mining.key){const [x,y]=mining.key.split(',').map(Number),p=tileRect(x,y),id=get(x,y),hard=defs[id]?.hard||1,progress=Math.min(1,mining.progress/hard);ctx.strokeStyle='#fff1a4';ctx.lineWidth=3;ctx.strokeRect(p.x+2,p.y+2,20,20);ctx.fillStyle='#ffcf52';ctx.fillRect(p.x+2,p.y+18,20*progress,3)}}
  function drawPlayer(){const x=Math.round(player.x-camera.x),y=Math.round(player.y-camera.y),step=Math.sin(player.walk)*3;ctx.save();ctx.translate(x+player.w/2,y);ctx.scale(player.facing,1);ctx.fillStyle='#1b2d32';ctx.fillRect(-8,2,16,11);ctx.fillStyle='#e3bd83';ctx.fillRect(-6,7,12,11);ctx.fillStyle='#e5ad4f';ctx.fillRect(-8,0,16,7);ctx.fillStyle='#f9d46a';ctx.fillRect(-6,-3,12,4);ctx.fillStyle='#112126';ctx.fillRect(2,9,2,3);ctx.fillStyle='#327a79';ctx.fillRect(-7,18,14,10);ctx.fillStyle='#c9873d';ctx.fillRect(-9,19,3,12);ctx.fillStyle='#243d47';ctx.fillRect(-6,28,5,7+step);ctx.fillRect(1,28,5,7-step);ctx.restore()}
  function drawTarget(){if(craftOpen||manualOpen||paused)return;const p=tileRect(pointer.tx,pointer.ty),reachable=distanceToTile(pointer.tx,pointer.ty)<6;ctx.strokeStyle=reachable?'rgba(255,245,180,.9)':'rgba(255,90,70,.7)';ctx.lineWidth=1;ctx.strokeRect(Math.floor(p.x)+.5,Math.floor(p.y)+.5,TILE-1,TILE-1)}
  function render(){drawSky();drawWorld();drawDrops();drawPlayer();drawTarget();if(camera.shake>0)camera.shake*=.88}

  function rectCollides(x,y,w,h,dy=0){
    const left=Math.floor(x/TILE),right=Math.floor((x+w-1)/TILE),top=Math.floor(y/TILE),bottom=Math.floor((y+h-1)/TILE);
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
    for(let ty=top;ty<=bottom;ty++)if(defs[get(tx,ty)]?.ladder)return true;
    return false;
  }
  function standingOnPlatform(){
    const ty=Math.floor((player.y+player.h+2)/TILE),left=Math.floor((player.x+2)/TILE),right=Math.floor((player.x+player.w-3)/TILE);
    for(let tx=left;tx<=right;tx++)if(defs[get(tx,ty)]?.platform)return true;
    return false;
  }
  function movePlayer(dt){
    const left=keys.KeyA||keys.ArrowLeft||keys.touchleft,right=keys.KeyD||keys.ArrowRight||keys.touchright;
    const up=keys.KeyW||keys.ArrowUp||keys.touchjump,down=keys.KeyS||keys.ArrowDown;
    const dir=(left?-1:0)+(right?1:0),onLadder=playerTouchesLadder(),accel=player.onGround?1150:650;
    ledgeDrop=Math.max(0,ledgeDrop-dt);
    if(down&&player.onGround&&!onLadder&&standingOnPlatform()){ledgeDrop=.22;player.onGround=false;player.y+=3;player.vy=Math.max(player.vy,70)}
    if(onLadder&&down)ledgeDrop=Math.max(ledgeDrop,.12);
    player.vx+=dir*accel*dt;player.vx*=Math.pow(player.onGround?0.0008:0.04,dt);player.vx=Math.max(-180,Math.min(180,player.vx));
    if(dir){player.facing=dir;player.walk+=dt*10}
    if(onLadder){
      if(keys.Space){player.vy=-270;player.onGround=false;keys.Space=false}
      else if(up||down){player.vy=(down?1:-1)*125;player.onGround=false}
      else{player.vy*=Math.pow(.002,dt);if(Math.abs(player.vy)<3)player.vy=0}
    }else{
      if((keys.Space||keys.KeyW||keys.ArrowUp||keys.touchjump)&&player.onGround){player.vy=-335;player.onGround=false;keys.Space=keys.KeyW=keys.ArrowUp=keys.touchjump=false}
      player.vy=Math.min(570,player.vy+920*dt);
    }
    let nx=player.x+player.vx*dt;
    if(!rectCollides(nx,player.y,player.w,player.h,0))player.x=nx;else{const step=Math.sign(player.vx);for(let i=0;i<Math.abs(player.vx*dt);i++)if(!rectCollides(player.x+step,player.y,player.w,player.h,0))player.x+=step;else break;player.vx=0}
    player.onGround=false;let ny=player.y+player.vy*dt;
    if(!rectCollides(player.x,ny,player.w,player.h,player.vy))player.y=ny;else{const step=Math.sign(player.vy);for(let i=0;i<Math.abs(player.vy*dt);i++)if(!rectCollides(player.x,player.y+step,player.w,player.h,player.vy))player.y+=step;else break;if(player.vy>0)player.onGround=true;player.vy=0}
    if(player.y>WORLD_H*TILE){player.health-=10;player.x=9*TILE;player.y=(groundAt(9)-4)*TILE;player.vy=0;showToast('Recovered at the field camp')}
    player.x=Math.max(TILE,Math.min(WORLD_W*TILE-player.w-TILE,player.x));
    if(Math.abs(player.x/TILE-205)<4&&!stats.coreFound){stats.coreFound=true;showToast('Foundry Core found — connect a 6-power grid')}
    applyConveyors(dt);camera.x+=(player.x+player.w/2-vw*.5-camera.x)*Math.min(1,dt*6);camera.y+=(player.y+player.h/2-vh*.55-camera.y)*Math.min(1,dt*5);camera.x=Math.max(0,Math.min(WORLD_W*TILE-vw,camera.x));camera.y=Math.max(0,Math.min(WORLD_H*TILE-vh,camera.y))
  }
  function applyConveyors(dt){const footX=Math.floor((player.x+player.w/2)/TILE),footY=Math.floor((player.y+player.h+2)/TILE),m=meta.get(idx(footX,footY));if(get(footX,footY)===T.CONVEYOR&&m&&m.running){const nx=player.x+((m.rot||0)===2?-1:1)*Math.abs(m.speed||1)*75*dt;if(!rectCollides(nx,player.y,player.w,player.h))player.x=nx}}
  function distanceToTile(x,y){return Math.hypot(x+.5-(player.x+player.w/2)/TILE,y+.5-(player.y+player.h/2)/TILE)}
  function updatePointer(e){const r=canvas.getBoundingClientRect();pointer.x=(e.clientX-r.left)*vw/r.width;pointer.y=(e.clientY-r.top)*vh/r.height;pointer.wx=pointer.x+camera.x;pointer.wy=pointer.y+camera.y;pointer.tx=Math.floor(pointer.wx/TILE);pointer.ty=Math.floor(pointer.wy/TILE);updateInspector()}
  canvas.addEventListener('pointermove',e=>{updatePointer(e);if(linkDrag){linkDrag.x1=pointer.tx;linkDrag.y1=pointer.ty}});canvas.addEventListener('pointerdown',e=>{if(craftOpen||manualOpen||paused)return;updatePointer(e);pointer.down=true;pointer.button=e.button;canvas.setPointerCapture?.(e.pointerId);if(items[selected].key==='link'&&e.button===0&&distanceToTile(pointer.tx,pointer.ty)<9){linkDrag={x0:pointer.tx,y0:pointer.ty,x1:pointer.tx,y1:pointer.ty};return}if(e.button===2){e.preventDefault();useOrPlace()}});canvas.addEventListener('pointerup',()=>{pointer.down=false;if(linkDrag){finishLink();linkDrag=null}mining.key='';mining.progress=0});canvas.addEventListener('contextmenu',e=>e.preventDefault());
  function useOrPlace(){
    const x=pointer.tx,y=pointer.ty,id=get(x,y),k=idx(x,y),m=meta.get(k)||{};
    if(distanceToTile(x,y)>6){showToast('Too far away');return}
    if(id && configureComponent(id,k,m))return;
    const item=items[selected];if(item.tile<0)return;
    if(id!==T.AIR){showToast('That space is occupied');return}
    if((inventory[item.key]||0)<=0){showToast(`No ${item.name} left — press E to fabricate`);return}
    if(defs[item.tile].solid&&playerOverlapsTile(x,y)){showToast('Cannot build inside yourself');return}
    set(x,y,item.tile,defaultMeta(item.tile));inventory[item.key]--;renderHotbar();
    showToast(`${item.name} placed — ${['right','down','left','up'][rotation]}`);simulateNetworks();
  }
  function defaultMeta(id){const m={rot:rotation,on:false,powered:false,active:false,running:false,linked:false};if(id===T.GEAR)m.mode=1;if(id===T.DOOR)m.open=false;return m}
  function playerOverlapsTile(x,y){return player.x<x*TILE+TILE&&player.x+player.w>x*TILE&&player.y<y*TILE+TILE&&player.y+player.h>y*TILE}
  function mineStep(dt){
    if(!pointer.down||pointer.button!==0||items[selected].key==='link'||craftOpen||paused)return;
    const x=pointer.tx,y=pointer.ty;if(distanceToTile(x,y)>6)return;const id=get(x,y);if(!id||id===T.CORE)return;
    const k=x+','+y;if(mining.key!==k){mining.key=k;mining.progress=0}mining.progress+=dt*(keys.ShiftLeft?2:1);
    if(mining.progress<(defs[id]?.hard||1))return;
    const drop=defs[id].drop,m=meta.get(idx(x,y))||{},cargo=Object.entries(m.contents||{}).filter(([,n])=>n>0);
    // Reserve capacity before removing anything, so a full item budget never loses a mined block or its cargo.
    if(engine.drops.length+cargo.length+1>512){showToast('Collect loose items before mining more');return}
    if(drop)engine.spawn(drop,1,x+.5,y+.5);
    for(const [key,n] of cargo)engine.spawn(key,n,x+.5,y+.5);
    if(id===T.COPPER)stats.copperMined++;
    const old=m.assembly;set(x,y,T.AIR);if(old)engine.splitAssembly(old);
    mining.key='';mining.progress=0;camera.shake=4;renderHotbar();simulateNetworks();showToast(`${defs[id].name} dropped — walk close to collect`);
  }
  function finishLink(){
    const minX=Math.max(0,Math.min(linkDrag.x0,linkDrag.x1)),maxX=Math.min(WORLD_W-1,Math.max(linkDrag.x0,linkDrag.x1));
    const minY=Math.max(0,Math.min(linkDrag.y0,linkDrag.y1)),maxY=Math.min(WORLD_H-1,Math.max(linkDrag.y0,linkDrag.y1));
    const keys=[];for(let y=minY;y<=maxY;y++)for(let x=minX;x<=maxX;x++)if(get(x,y)&&get(x,y)!==T.CORE&&distanceToTile(x,y)<10)keys.push(idx(x,y));
    if(!keys.length){showToast('Select blocks within reach');return}
    if(linkMode==='inspect'){selectedAssembly=meta.get(keys[0])?.assembly||null;showToast(selectedAssembly?`Inspecting structure ${selectedAssembly}`:'That block is not linked');updateInspector();return}
    if(linkMode==='add'&&!selectedAssembly){showToast('Use Inspect first to select the structure to extend');return}
    const id=engine.link(keys,linkMode,selectedAssembly);if(id)selectedAssembly=id;
    showToast(`${keys.length} blocks: ${linkMode==='remove'?'detached':`structure ${id} (disconnected pieces split)`}`);simulateNetworks();
  }
  function neighbors(x,y){return [[x+1,y],[x-1,y],[x,y+1],[x,y-1]].filter(p=>inside(p[0],p[1]))}
  function simulateNetworks(dt=0){
    if(!engine)return;engine.step(dt);
    const liveCore=engine.active.some(k=>world[k]===T.CORE&&meta.get(k)?.powered&&meta.get(k)?.signalOK);
    if(liveCore&&!stats.coreReached){stats.coreReached=true;showToast('Foundry Core online — commission complete!')}
    updatePowerHud(engine.supply,engine.load);
  }
  function updateSensors(){for(const k of engine.nodes)if(world[k]===T.SENSOR){const m=meta.get(k),x=k%WORLD_W,y=Math.floor(k/WORLD_W);m.sensorActive=Math.hypot(player.x/TILE-x,player.y/TILE-y)<6}}
  function updateMachines(dt=.05){
    for(const [k,m] of Array.from(meta.entries())){
      if(meta.get(k)!==m)continue; // An earlier actuator may already have moved this component.
      const id=world[k],x=k%WORLD_W,y=Math.floor(k/WORLD_W);m.machineClock=(m.machineClock||0)+dt;
      if(id===T.PISTON){
        const enabled=m.running&&m.signalOK!==false;
        if(enabled&&!m.extended){if(operatePiston(x,y,m,1))m.extended=true}
        else if(!enabled&&m.extended&&m.running){if(operatePiston(x,y,m,-1))m.extended=false}
      }else if(id===T.WINCH&&m.running&&m.signalOK!==false&&m.machineClock>.5/Math.max(.1,Math.abs(m.speed))){
        m.machineClock=0;if(operateWinch(x,y,m,-1))stats.motionEvents++;
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
    const parts=group.map(([x,y])=>({x,y,id:get(x,y),m:meta.get(idx(x,y))}));
    for(const p of parts)set(p.x,p.y,T.AIR);for(const p of parts)set(p.x+dx,p.y+dy,p.id,p.m);
    if(riders){player.x+=dx*TILE;player.y+=dy*TILE}machine.fault='';return true;
  }
  function operatePiston(x,y,m,way){
    const [dx,dy]=dirs[m.rot||0],fx=x+dx*(way>0?1:2),fy=y+dy*(way>0?1:2),id=get(fx,fy);
    if(!id)return way<0;const group=collectLinked(fx,fy);
    if(group.some(([gx,gy])=>gx===x&&gy===y)){m.fault='ACTUATOR LINKED TO ITS OWN LOAD';return false}
    const ok=moveGroup(group,dx*way,dy*way,m.torque,m);if(ok){stats.motionEvents++;camera.shake=3}return ok;
  }
  function operateWinch(x,y,m,dy){
    for(let yy=y+1;yy<Math.min(WORLD_H,y+12);yy++)if(get(x,yy)!==T.AIR){
      const group=collectLinked(x,yy);if(group.some(([gx,gy])=>gx===x&&gy===y)){m.fault='WINCH LINKED TO LOAD';return false}
      return moveGroup(group,0,dy,m.torque,m);
    }m.fault='NO LOAD BELOW';return false;
  }
  function assemblyColor(id){return `hsl(${((id||1)*137.5)%360} 80% 70%)`}
  function configureComponent(id,k,m){
    const mode=()=>{engine.revision++;simulateNetworks();updateInspector();return true};
    if(id===T.DOOR){m.open=!m.open;meta.set(k,m);showToast(m.open?'Door opened':'Door closed');return mode()}
    if(id===T.SWITCH){m.on=!m.on;showToast(m.on?'Switch ON':'Switch OFF');return mode()}
    if(id===T.GEAR){m.mode=((m.mode??1)+1)%3;showToast(['Torque: ½ speed, 2× torque','Balanced: 1:1','Speed: 2× speed, ½ torque'][m.mode]);return mode()}
    if(id===T.TIMER||id===T.PULSE){m.delay=E.DELAYS[(E.DELAYS.indexOf(m.delay||1)+1)%E.DELAYS.length];m.timer=m.phase=0;showToast(`${defs[id].name}: ${m.delay}s`);return mode()}
    if(id===T.COUNTER){m.target=((m.target||3)%15)+1;showToast(`Counter target: ${m.target}; top port resets count`);return mode()}
    if(id===T.FILTER){const choices=['iron','copper','stone','wood','crystal','dirt','obsidian',...items.filter(i=>i.tile>0).map(i=>i.key)];
      m.filter=keys.ShiftLeft?items[selected].key:choices[(choices.indexOf(m.filter||'iron')+1)%choices.length];showToast(`Filter: ${m.filter} (Shift-right-click: held part)`);return mode()}
    if(id===T.STORAGE){showToast('F: take contents · Shift+F: store selected part · hopper at front extracts');return true}
    if(id===T.BEARING||id===T.HINGE){showToast('Link the load separately. Rear: shaft drive. Front: attach load. Top: hinge control.');return true}
    return false;
  }
  function canCarryPlayer(dx,dy,ignore){
    const steps=Math.max(1,Math.ceil(Math.hypot(dx,dy)/6));
    for(let i=1;i<=steps;i++){
      const x=player.x+dx*i/steps,y=player.y+dy*i/steps;
      for(let ty=Math.floor(y/TILE);ty<=Math.floor((y+player.h-1)/TILE);ty++)for(let tx=Math.floor(x/TILE);tx<=Math.floor((x+player.w-1)/TILE);tx++)
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
    for(const p of parts){const nx=x-turn*(p.y-y),ny=y+turn*(p.x-x);set(nx,ny,p.id,{...p.m,rot:((p.m.rot||0)+turn+4)%4});}
    m.fault='';return true;
  }
  function collectDrops(){
    let collected=false;
    for(const d of engine.drops)if(d.age>.5&&Math.hypot(d.x-(player.x+player.w/2)/TILE,d.y-(player.y+player.h/2)/TILE)<1.35){
      // Never pick through a solid wall.
      const tx=Math.floor(d.x),ty=Math.floor(d.y);if(defs[get(tx,ty)]?.solid)continue;
      inventory[d.key]=(inventory[d.key]||0)+d.count;d.count=0;collected=true;
    }
    if(collected){engine.drops=engine.drops.filter(d=>d.count>0);renderHotbar()}
  }
  function interactItems(){
    if(paused||craftOpen||manualOpen)return;
    const k=idx(pointer.tx,pointer.ty),id=get(pointer.tx,pointer.ty),m=meta.get(k);
    if(distanceToTile(pointer.tx,pointer.ty)>6||!m||(!defs[id].transport&&id!==T.CONVEYOR)){showToast('Aim at nearby item machinery');return}
    if(keys.ShiftLeft||keys.ShiftRight){const key=items[selected].key;if((inventory[key]||0)>0){const n=engine.insert(k,key,1);inventory[key]-=n;showToast(n?`${key} inserted`:'Full or filtered')}}
    else {let total=0;for(const [key,n] of Object.entries(m.contents||{})){inventory[key]=(inventory[key]||0)+n;total+=n}m.contents={};showToast(`Collected ${total} items`)}
    renderHotbar();updateInspector();
  }
  function dropHeldItem(){
    if(paused||craftOpen||manualOpen)return;
    const item=items[selected];if(!item||item.tile<0||(inventory[item.key]||0)<1)return;
    const x=pointer.tx,y=pointer.ty;if(distanceToTile(x,y)>6||!inside(x,y)){showToast('Drop within reach');return}
    if(defs[get(x,y)]?.solid&&!defs[get(x,y)]?.transport&&get(x,y)!==T.CONVEYOR){showToast('Aim at empty space or item machinery');return}
    if(engine.spawn(item.key,1,x+.5,y+.5)){inventory[item.key]--;renderHotbar();showToast(`${item.name} dropped`)}
  }
  function drawDrops(){
    for(const d of engine.drops){const x=d.x*TILE-camera.x,y=d.y*TILE-camera.y;if(x<-10||y<-10||x>vw+10||y>vh+10)continue;
      ctx.fillStyle=palette[d.key]||items.find(i=>i.key===d.key)?.color||'#dfdca9';ctx.fillRect(x-3,y-3,7,7);ctx.strokeStyle='#17262b';ctx.strokeRect(x-3,y-3,7,7);
      if(lens&&d.count>1){ctx.fillStyle='#fff';ctx.font='9px monospace';ctx.fillText(d.count,x+4,y)}
    }
  }
  function drawEngineeringOverlay(id,m,x,y){
    const ports=E.ports(defs[id],m);
    const cargo=Object.entries(m.contents||{}).find(([,n])=>n>0);
    if(cargo){
      const [key,count]=cargo,[dx,dy]=E.DIRS[m.rot||0];
      const offset=id===T.CONVEYOR&&m.running?((m.itemClock||0)/.25-.5)*14:0;
      ctx.fillStyle=palette[key]||items.find(i=>i.key===key)?.color||'#e4dfa3';
      ctx.fillRect(9+dx*offset,5+dy*offset,6,6);
      if(count>1)ctx.fillRect(15+dx*offset,7+dy*offset,3,3);
    }
    if(defs[id].kind==='signal'){ctx.fillStyle=m.active?'#fff79c':'#173139';ctx.fillRect(3,3,4,3)}
    if(id>=26&&defs[id].mechanical&&m.running){
      const angle=gameTime*m.speed*Math.PI/2;ctx.strokeStyle='#fff9d1';ctx.lineWidth=2;
      ctx.beginPath();ctx.moveTo(12,12);ctx.lineTo(12+Math.cos(angle)*7,12+Math.sin(angle)*7);ctx.stroke();
    }
    if(ports.length&&!lens){ctx.fillStyle='#ffffffbb';const [dx,dy]=E.DIRS[m.rot||0];ctx.beginPath();ctx.moveTo(12+dx*10,12+dy*10);ctx.lineTo(12+dx*5-dy*3,12+dy*5+dx*3);ctx.lineTo(12+dx*5+dy*3,12+dy*5-dx*3);ctx.fill()}
    if(!lens)return;
    if(lensMode==='structure'&&m.assembly){const ratio=(m.stress||0)/((defs[id].strength||8)*4);ctx.fillStyle=E.strengthColor(ratio)+'66';ctx.fillRect(2,2,20,20)}
    if(lensMode==='ports'||['power','signal','mechanical','item'].includes(lensMode)){
      const sideCount={};for(const p of ports){if(lensMode!=='ports'&&p.type!==lensMode)continue;
        const [dx,dy]=E.DIRS[p.side],offset=(sideCount[p.side]||0)*5;sideCount[p.side]=(sideCount[p.side]||0)+1;
        const px=12+dx*10-dy*offset,py=12+dy*10+dx*offset;ctx.fillStyle=E.COLORS[p.type];
        if(p.mode==='both')ctx.fillRect(px-2,py-2,4,4);else{const sign=p.mode==='out'?1:-1;ctx.beginPath();ctx.moveTo(px+dx*3*sign,py+dy*3*sign);ctx.lineTo(px-dx*2*sign-dy*3,py-dy*2*sign+dx*3);ctx.lineTo(px-dx*2*sign+dy*3,py-dy*2*sign-dx*3);ctx.fill()}
      }
    }
    if(lensMode==='item'&&(defs[id].transport||id===T.CONVEYOR)){ctx.fillStyle='#fff';ctx.font='9px monospace';ctx.fillText(engine.stored(idx(x,y)),2,10)}
    if(lensMode==='mechanical'&&id===T.BELT){for(const e of engine.mechanical.get(idx(x,y))||[])if(e.remote){const [nx,ny]=engine.xy(e.to);ctx.strokeStyle=m.running?'#8df4e4':'#829c98';ctx.setLineDash([3,3]);ctx.beginPath();ctx.moveTo(12,12);ctx.lineTo((nx-x)*TILE+12,(ny-y)*TILE+12);ctx.stroke();ctx.setLineDash([])}}
    if(m.fault||m.mechanicalFault||m.logisticsFault||m.logicUnstable){ctx.fillStyle='#ff6b5f';ctx.font='bold 11px monospace';ctx.fillText('!',17,10)}
  }
  function cycleLens(){const modes=['ports','power','signal','mechanical','structure','item'];lensMode=modes[(modes.indexOf(lensMode)+1)%modes.length];if(!lens)toggleLens();document.getElementById('viewBtn').textContent=`View: ${lensMode} [V]`;updateInspector()}
  function cycleLink(){const modes=['new','add','remove','inspect'];linkMode=modes[(modes.indexOf(linkMode)+1)%modes.length];document.getElementById('linkModeBtn').textContent=`Link: ${linkMode} [L]`;selected=items.findIndex(i=>i.key==='link');renderHotbar();showToast(`Link mode: ${linkMode}`)}
  function setupEngineeringUI(){
    document.querySelector('.tag').textContent='ENGINEERING 02';
    const actions=document.querySelector('.top-actions');
    const view=document.createElement('button');view.id='viewBtn';view.className='icon-btn';view.textContent='View: ports [V]';view.onclick=cycleLens;actions.prepend(view);
    const mode=document.createElement('button');mode.id='linkModeBtn';mode.className='icon-btn';mode.textContent='Link: new [L]';mode.onclick=cycleLink;actions.prepend(mode);
    const tools=document.createElement('div');tools.className='engineering-tools';
    for(const [label,action] of [['Rotate held [R]',()=>{rotation=(rotation+1)%4;showToast('Placement: '+['right','down','left','up'][rotation])}],['Take items [F]',interactItems],['Drop item [X]',dropHeldItem]]){const b=document.createElement('button');b.className='icon-btn';b.textContent=label;b.onclick=action;tools.append(b)}
    document.getElementById('hud').append(tools);
    const card=(title,text)=>{const el=document.createElement('div');el.className='manual-card';const h=document.createElement('h3'),p=document.createElement('p');h.textContent=title;p.textContent=text;el.append(h,p);return el};
    const manual=document.querySelector('.manual-grid');manual.replaceChildren(
      card('Explore and fabricate','A/D moves, Space jumps. W/S climbs ladders. Jump upward through ledges and press S/Down while standing on one to drop through. Right-click a door to open or close it. Left click mines; E opens the categorized, searchable Fabricator. Crafting selects the new part. Right click places; R rotates before placing. Shift+R rotates the block under the cursor.'),
      card('Read the ports','C equips the Lens. V or the View button changes modes. Arrows show input/output; squares are two-way. Yellow: electricity. Red: signals. Cyan: rotation. Green: items. Purple: joint. Matching ports must face each other. Motor rear = power, top = enable, front = shaft.'),
      card('Logic and memory','Default gate orientation: rear A, top B, front output. NOT inverts A. Latch: rear SET, top RESET (reset wins). Toggle flips on each rising edge. Counter counts rising edges, top resets. Right-click target 1–15. Unconnected gate inputs are OFF; a NOT with no input outputs ON.'),
      card('Timing','Timer requires a continuously high input for its selected delay. Pulse Generator repeats once per delay; rear input optionally enables it. Right-click cycles 0.25 / 0.5 / 1 / 2 / 5 / 10 seconds. Simulation uses 50ms steps. Insert a latch/toggle for feedback; unstable combinational loops are flagged.'),
      card('Drive paths','Shafts run rear to front. Bevel gears turn 90° toward the local bottom. Gearbox right-click: torque / balanced / speed. Small gear doubles speed, halves torque and reverses; large gear does the opposite. Clutch needs a top signal. A Belt Drive reaches a facing pulley or joint across up to six cells. Rotate belt and pulley to send drive vertically. For a full-turn bearing, leave space around the load and belt-drive the mount from a distance; this belt runs behind its own rotating load.'),
      card('Link distinct structures','Select Link Tool (0). L cycles New / Add / Remove / Inspect. Drag a rectangle. Each connected selection gets its own colored structure ID. Inspect a block first to choose an existing structure for Add. Removing blocks splits disconnected sections. Keep actuators and mounts separate from their loads.'),
      card('Joints and load','A piston pushes one block-length; a powered piston with a low enable signal retracts. Winches lift loads below repeatedly. Bearings rotate separately linked loads at their front; hinges swing 0–90° using a top signal. This first version uses grid quarter-turns, not free rigid-body rotation. Sweeps stop at obstacles or the player. Joint mounts stay anchored. Heavy or long arms need torque gearing.'),
      card('Strength and failures','Structural Lens: blue low stress, green normal, yellow high, orange danger, red failure risk. Heavy lifting and long rotating arms raise stress. Three extreme overload attempts can detach a weak connection, without deleting the block. Inspector shows required and available torque, blocked paths and power shortages.'),
      card('Physical logistics','Conveyors need rear or bottom shaft drive. Items enter at the rear or fall from above; front is output. Hoppers collect above/rear and output front. Storage holds 64 items; attach a hopper to its front to extract. Splitters alternate front/bottom output and wait if the selected route blocks. Filters pass only the selected material. Right-click cycles raw materials; Shift-right-click selects the held component.'),
      card('Load and recover','Aim at item machinery: F takes its contents; Shift+F inserts one held part. X drops a held part into the world. Mining a container drops all cargo. Full outputs block instead of destroying items. Machines keep their contents, counters, configuration and links when moved and saved.'),
      card('Try a sensor door','Lay a generator behind a motor, then a shaft into a piston. Place a sensor above the motor, rotated downward to face its enable port. Link a lightweight door in front of the piston, separately from the drive. For automatic return, power the motor continuously and connect the sensor to the piston enable instead.'),
      card('Try physical logistics','Drive a conveyor from below with an upward-facing motor. Face its output into a splitter and two storage bins with matching ports. Drop items to see alternating routing. Insert a filter to allow one material; rejected items wait upstream. The Item Lens explains blockage. Later expansions add sorting routers, drills, vehicles, factories, blueprints and fluids.')
    );
    document.querySelector('.hint').textContent='A/D move · SPACE jump · W/S ladders · DOWN drop ledge · RIGHT door/place · E fabricate · R rotate · C lens';
  }

  function renderHotbar(){const el=document.getElementById('hotbar');el.innerHTML='';items.forEach((it,i)=>{const b=document.createElement('button');b.className='slot'+(i===selected?' selected':'');const key=i<9?i+1:i===9?'0':'';b.innerHTML=`<span class="key">${key}</span><span class="mini" style="color:${it.color}">${it.icon}</span><span class="count">${it.key==='link'?'∞':inventory[it.key]||0}</span><span class="name">${it.name}</span>`;b.title=it.name;b.setAttribute('aria-label',it.name);b.onclick=()=>{selected=i;renderHotbar();showToast(it.name)};el.appendChild(b)})}
  const craftState={category:'all',query:'',affordable:false};
  const craftCategories=['all','structure','power','logic','motion','logistics'];
  function recipeCategory(r){
    if(r.category)return r.category;
    const structure=new Set(['platform','copperBlock','ironBlock','frame','beam','door','ladder','ledge']);
    const power=new Set(['wire','generator','lamp']);
    const logic=new Set(['signal','switch','sensor','and','or','not','xor','nand','nor','timer','pulse','toggle','latch','counter','signalSplit']);
    const motion=new Set(['motor','shaft','gear','piston','winch','clutch','belt','pulley','bevel','smallGear','largeGear','bearing','hinge']);
    const logistics=new Set(['conveyor','hopper','storage','splitter','filter']);
    if(structure.has(r.key))return 'structure';if(power.has(r.key))return 'power';if(logic.has(r.key))return 'logic';if(motion.has(r.key))return 'motion';if(logistics.has(r.key))return 'logistics';return 'structure';
  }
  function ensureCraftControls(){
    if(document.getElementById('craftSearch'))return;
    const controls=document.createElement('div');controls.className='craft-controls';
    const search=document.createElement('input');search.id='craftSearch';search.type='search';search.placeholder='Search parts, uses, or materials…';search.autocomplete='off';search.addEventListener('input',()=>{craftState.query=search.value.trim().toLowerCase();renderCraft()});
    const tabs=document.createElement('div');tabs.className='craft-tabs';
    for(const category of craftCategories){const b=document.createElement('button');b.type='button';b.dataset.craftCategory=category;b.textContent=category==='all'?'All':category[0].toUpperCase()+category.slice(1);b.onclick=()=>{craftState.category=category;renderCraft()};tabs.appendChild(b)}
    const filter=document.createElement('label');filter.className='craft-affordable';const cb=document.createElement('input');cb.type='checkbox';cb.onchange=()=>{craftState.affordable=cb.checked;renderCraft()};filter.append(cb,document.createTextNode(' Can craft now'));
    const count=document.createElement('span');count.id='craftResultCount';count.className='craft-result-count';
    controls.append(search,tabs,filter,count);document.getElementById('recipes').before(controls);
  }
  function renderCraft(){
    ensureCraftControls();
    const resources=['dirt','stone','wood','copper','iron','crystal'];document.getElementById('resourceChips').innerHTML=resources.map(k=>`<span class="chip" style="color:${palette[k]||'#fff'}">${k.toUpperCase()} ${inventory[k]||0}</span>`).join('');
    document.querySelectorAll('[data-craft-category]').forEach(b=>b.classList.toggle('active',b.dataset.craftCategory===craftState.category));
    const query=craftState.query,visible=recipes.filter(r=>{const can=Object.entries(r.cost).every(([k,v])=>(inventory[k]||0)>=v),category=recipeCategory(r),haystack=`${r.name} ${r.desc} ${Object.keys(r.cost).join(' ')} ${category}`.toLowerCase();return(craftState.category==='all'||category===craftState.category)&&(!query||haystack.includes(query))&&(!craftState.affordable||can)});
    document.getElementById('craftResultCount').textContent=`${visible.length} of ${recipes.length} parts`;
    const el=document.getElementById('recipes');el.innerHTML='';
    if(!visible.length){const empty=document.createElement('div');empty.className='craft-empty';empty.textContent='No parts match those filters.';el.appendChild(empty);return}
    visible.forEach(r=>{const can=Object.entries(r.cost).every(([k,v])=>(inventory[k]||0)>=v),b=document.createElement('button'),category=recipeCategory(r);b.className='recipe';b.disabled=!can;b.innerHTML=`<span class="recipe-icon">${r.icon}</span><span><span class="recipe-category">${category}</span><b>${r.name}</b><small>${Object.entries(r.cost).map(([k,v])=>`${v} ${k}`).join(' · ')}<br>${r.desc}</small></span><span class="make">+${r.out}</span>`;b.onclick=()=>{craft(r);selected=items.findIndex(i=>i.key===r.key);renderHotbar()};el.appendChild(b)});
  }
  function craft(r){if(!Object.entries(r.cost).every(([k,v])=>(inventory[k]||0)>=v))return;for(const [k,v] of Object.entries(r.cost))inventory[k]-=v;inventory[r.key]=(inventory[r.key]||0)+r.out;stats.crafted++;renderCraft();renderHotbar();showToast(`${r.name} fabricated ×${r.out}`)}
  function renderMissions(){const grid=Array.from(meta.entries()).some(([k,m])=>world[k]===T.LAMP&&m.powered&&m.signalOK);let currentFound=false;document.getElementById('missionList').innerHTML=missionDefs.map(([key,label],i)=>{const done=key==='grid'?grid:key==='copperMined'?stats.copperMined>=8:key==='crafted'?stats.crafted>=1:key==='motionEvents'?stats.motionEvents>=1:!!stats[key],current=!done&&!currentFound;if(current)currentFound=true;return`<div class="mission ${done?'done':current?'current':''}"><span class="check">${done?'✓':i+1}</span><span>${label}${key==='copperMined'&&!done?` (${Math.min(8,stats.copperMined)}/8)`:''}</span></div>`}).join('')}
  function updatePowerHud(supply,load){document.getElementById('powerText').textContent=`${load} / ${supply}`;document.getElementById('powerBar').style.width=(supply?Math.min(100,load/supply*100):0)+'%'}
  function updateInspector(){
    const box=document.getElementById('cursorLabel');if(!lens){box.style.display='none';return}
    const id=get(pointer.tx,pointer.ty),d=defs[id],m=meta.get(idx(pointer.tx,pointer.ty))||{};
    if(!id||!d){box.style.display='none';document.getElementById('inspectName').textContent='Aim at a block';document.getElementById('inspectProps').textContent='Ports: yellow power · red signals · cyan drive · green items · purple joints. R rotates the held part.';return}
    box.style.display='block';box.style.left=pointer.x+'px';box.style.top=pointer.y+'px';box.textContent=d.name;
    document.getElementById('inspectName').textContent=d.name;
    const props=[['View',lensMode],['Direction',['right','down','left','up'][m.rot||0]],['Weight / strength',`${d.weight||0} / ${d.strength||8}`]];
    if(d.ports&&lensMode==='ports')for(const p of E.ports(d,m))props.push([`${['→','↓','←','↑'][p.side]} ${p.name}`,`${p.type} ${p.mode}`]);
    if(d.electric){props.push(['Grid #',m.powerNet||'—'],['Generation / draw',`${m.supply||0} / ${m.load||0}`],['Capacity',m.capacity||'—'],['Power',m.overload?'GRID OVERLOAD':m.powered?'LIVE':'INSUFFICIENT POWER']);}
    if(E.ports(d,m).some(p=>p.type==='signal')){props.push(['Output',m.active?'ON':'OFF']);for(const [key,value] of Object.entries(m.signalInputs||{}))props.push([key,value?'ON':'OFF']);}
    if(id===T.COUNTER)props.push(['Count / target',`${m.count||0} / ${m.target||3}`]);
    if(id===T.TIMER||id===T.PULSE)props.push(['Delay',`${m.delay||1}s`]);
    if(d.mechanical)props.push(['Signed speed',(m.speed||0).toFixed(2)],['Available torque',(m.torque||0).toFixed(1)],['Required torque',(m.requiredTorque||0).toFixed(1)]);
    if(m.assembly){const info=engine.structureInfo(m.assembly);props.push(['Structure #',m.assembly],['Blocks / mass',`${info.keys.length} / ${info.mass}`],['Peak stress',info.stress.toFixed(1)]);
      const joints=engine.active.filter(k=>meta.get(k)?.jointAssembly===m.assembly);props.push(['Joints',joints.length],['Constraint',joints.length?'Pivot anchored':'Free translation']);}
    if(d.transport||id===T.CONVEYOR)props.push(['Stored / capacity',`${engine.stored(idx(pointer.tx,pointer.ty))} / ${engine.capacity(idx(pointer.tx,pointer.ty))}`],['Contents',Object.entries(m.contents||{}).filter(([,n])=>n).map(([k,n])=>`${k}: ${n}`).join(', ')||'Empty']);
    if(id===T.FILTER)props.push(['Filter',m.filter||'iron']);
    for(const fault of [m.fault,m.mechanicalFault,m.logisticsFault,m.logicUnstable?'SIGNAL LOOP / >64 GATES — insert memory':''])if(fault)props.push(['Status',fault]);
    const el=document.getElementById('inspectProps');el.replaceChildren();for(const [label,value] of props){const row=document.createElement('div');row.className='property';const a=document.createElement('span'),b=document.createElement('span');a.textContent=label;b.textContent=value;row.append(a,b);el.append(row)}
  }
  function showToast(msg){const e=document.getElementById('toast');e.textContent=msg;e.classList.add('show');clearTimeout(toastTimer);toastTimer=setTimeout(()=>e.classList.remove('show'),1800)}
  function toggleLens(){lens=!lens;document.getElementById('lensBtn').classList.toggle('active',lens);document.getElementById('lensPanel').classList.toggle('show',lens);if(!lens)document.getElementById('cursorLabel').style.display='none';showToast(lens?"Engineer's Lens equipped":"Engineer's Lens stowed")}
  function toggleModal(id,force){const e=document.getElementById(id),open=force??!e.classList.contains('show');for(const q of document.querySelectorAll('.modal'))q.classList.remove('show');if(open)e.classList.add('show');craftOpen=id==='craftPanel'&&open;manualOpen=id==='manual'&&open;paused=id==='pausePanel'&&open;if(id==='craftPanel'&&open)renderCraft()}
  addEventListener('keydown',e=>{if(['Space','ArrowUp','ArrowDown','ArrowLeft','ArrowRight'].includes(e.code))e.preventDefault();if(e.repeat&&['KeyE','KeyC','KeyH','KeyR','Escape'].includes(e.code))return;if(e.target?.matches('input,select,textarea'))return;keys[e.code]=true;if(e.code.startsWith('Digit')){const n=Number(e.code.slice(5));selected=n===0?9:n-1;renderHotbar()}if(e.code==='KeyE')toggleModal('craftPanel');if(e.code==='KeyH')toggleModal('manual');if(e.code==='KeyC')toggleLens();if(e.code==='KeyR'&&e.shiftKey){const k=idx(pointer.tx,pointer.ty),id=get(pointer.tx,pointer.ty);if(id&&distanceToTile(pointer.tx,pointer.ty)<=6){const m=meta.get(k)||{};m.rot=((m.rot||0)+1)%4;meta.set(k,m);engine.changed(pointer.tx,pointer.ty);simulateNetworks();showToast('Block rotated')}return}if(e.code==='KeyR'){rotation=(rotation+1)%4;showToast('Placement direction: '+['right','down','left','up'][rotation])}if(e.code==='KeyV'){cycleLens()}if(e.code==='KeyL'){cycleLink()}if(e.code==='KeyF'){interactItems()}if(e.code==='KeyX'){dropHeldItem()}if(e.code==='Escape')toggleModal('pausePanel',!paused)});addEventListener('keyup',e=>keys[e.code]=false);addEventListener('blur',()=>{for(const k of Object.keys(keys))keys[k]=false;pointer.down=false;linkDrag=null;if(!craftOpen&&!manualOpen)toggleModal('pausePanel',true)});
  document.getElementById('lensBtn').onclick=toggleLens;document.getElementById('manualBtn').onclick=()=>toggleModal('manual');document.getElementById('pauseBtn').onclick=()=>toggleModal('pausePanel',true);document.getElementById('resumeBtn').onclick=()=>toggleModal('pausePanel',false);document.getElementById('saveBtn').onclick=()=>{saveGame();showToast('World saved')};document.getElementById('newBtn').onclick=()=>{if(confirm('Generate a new world? The current save will be replaced.')){generate();toggleModal('pausePanel',false)}};document.querySelectorAll('[data-close]').forEach(b=>b.addEventListener('click',()=>toggleModal(b.dataset.close,false)));
  document.querySelectorAll('[data-touch]').forEach(b=>{const k='touch'+b.dataset.touch;b.addEventListener('pointerdown',e=>{e.preventDefault();if(b.dataset.touch==='place'){useOrPlace();return}keys[k]=true;if(b.dataset.touch==='mine'){pointer.down=true;pointer.button=0}});const up=e=>{e.preventDefault();keys[k]=false;if(b.dataset.touch==='mine')pointer.down=false};b.addEventListener('pointerup',up);b.addEventListener('pointercancel',up)});
  setInterval(()=>{if(!paused)saveGame()},10000);
  function tick(t){
    const dt=Math.min(.1,(t-last)/1000||0);last=t;
    if(!paused&&!craftOpen&&!manualOpen){
      gameTime+=dt;movePlayer(Math.min(dt,.033));mineStep(dt);accum+=dt;
      while(accum>=.05){accum-=.05;updateSensors();simulateNetworks(.05);updateMachines(.05);engine.logistics(.05);collectDrops();}
      if(Math.floor(gameTime*4)!==lastHud){lastHud=Math.floor(gameTime*4);renderMissions();updateInspector();document.getElementById('healthText').textContent=Math.max(0,player.health|0);document.getElementById('healthBar').style.width=player.health+'%'}
    }render();requestAnimationFrame(tick);
  }
  let lastHud=-1;
  renderHotbar();simulateNetworks();renderMissions();requestAnimationFrame(tick);
})();
