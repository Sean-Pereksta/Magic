const fs = require('node:fs');

const appPath = 'game/circuitbound/app.js';
const cssPath = 'game/circuitbound/styles.css';
let app = fs.readFileSync(appPath, 'utf8');
let css = fs.readFileSync(cssPath, 'utf8');

function replaceOnce(search, replacement, label) {
  if (!app.includes(search)) throw new Error(`Patch anchor not found: ${label}`);
  app = app.replace(search, replacement);
}
function replaceRegexOnce(regex, replacement, label) {
  if (!regex.test(app)) throw new Error(`Patch pattern not found: ${label}`);
  app = app.replace(regex, replacement);
}

if (app.includes("T.DOOR=nextCustomTile")) {
  console.log('Circuitbound interaction patch is already applied.');
  process.exit(0);
}

replaceOnce(
  ',winch:1,platform:12};',
  ',winch:1,platform:12,door:2,ladder:8,ledge:8};',
  'starter inventory'
);

replaceOnce(
  '  E.install(T,defs,items,recipes);',
  `  E.install(T,defs,items,recipes);\n  // Player-scale construction components live after the engineering catalog IDs.\n  const nextCustomTile=Math.max(...Object.values(T).filter(Number.isInteger))+1;\n  T.DOOR=nextCustomTile;T.LADDER=nextCustomTile+1;T.LEDGE=nextCustomTile+2;\n  defs[T.DOOR]={name:'Wood Door',solid:true,door:true,hard:.55,drop:'door',weight:2,strength:4,conduct:0};\n  defs[T.LADDER]={name:'Ladder',solid:false,ladder:true,hard:.35,drop:'ladder',weight:1,strength:2,conduct:0};\n  defs[T.LEDGE]={name:'Wood Ledge',solid:true,platform:true,ledge:true,hard:.35,drop:'ledge',weight:1,strength:3,conduct:0};\n  items.push(\n    {key:'door',tile:T.DOOR,name:'Door',icon:'▯',color:'#c88a50'},\n    {key:'ladder',tile:T.LADDER,name:'Ladder',icon:'H',color:'#d5a665'},\n    {key:'ledge',tile:T.LEDGE,name:'Ledge',icon:'▔',color:'#c89557'}\n  );\n  recipes.push(\n    {key:'door',name:'Wood Door',icon:'▯',out:1,cost:{wood:4,iron:1},desc:'Right-click to open or close',category:'structure'},\n    {key:'ladder',name:'Ladder',icon:'H',out:4,cost:{wood:2},desc:'Climb up or down with W/S',category:'structure'},\n    {key:'ledge',name:'Wood Ledge',icon:'▔',out:4,cost:{wood:1},desc:'Jump through; press down to drop through',category:'structure'}\n  );`,
  'custom construction catalog'
);

replaceOnce(
  "    else if(id===T.CORE){",
  `    else if(id===T.DOOR){ctx.fillStyle='#503724';ctx.fillRect(1,1,22,22);if(m&&m.open){ctx.fillStyle='#b97b43';ctx.fillRect(18,2,4,20);ctx.fillStyle='#f0c36c';ctx.fillRect(18,11,2,2)}else{ctx.fillStyle='#a96f3e';ctx.fillRect(3,2,18,20);ctx.fillStyle='#754925';ctx.fillRect(6,5,11,5);ctx.fillRect(6,13,11,6);ctx.fillStyle='#f0c36c';ctx.fillRect(17,11,2,2)}}\n    else if(id===T.LADDER){ctx.fillStyle='#b57b42';ctx.fillRect(4,0,3,24);ctx.fillRect(17,0,3,24);ctx.fillStyle='#d8a65d';for(let q=3;q<24;q+=6)ctx.fillRect(5,q,14,2)}\n    else if(id===T.LEDGE){ctx.fillStyle='#644327';ctx.fillRect(0,7,24,5);ctx.fillStyle='#d5a260';ctx.fillRect(0,5,24,3);ctx.fillStyle='#9b6838';ctx.fillRect(3,12,3,4);ctx.fillRect(18,12,3,4)}\n    else if(id===T.CORE){`,
  'custom tile drawing'
);

replaceOnce(
  '    if(id>=26){machineBox(E.COLORS[defs[id].kind]);',
  '    if(id>=26&&defs[id].kind){machineBox(E.COLORS[defs[id].kind]);',
  'engineering fallback drawing'
);

replaceRegexOnce(
  /  function rectCollides\(x,y,w,h,dy=0\)\{[^\n]*\}\n/,
  `  function rectCollides(x,y,w,h,dy=0){\n    const left=Math.floor(x/TILE),right=Math.floor((x+w-1)/TILE),top=Math.floor(y/TILE),bottom=Math.floor((y+h-1)/TILE);\n    for(let ty=top;ty<=bottom;ty++)for(let tx=left;tx<=right;tx++){\n      const id=get(tx,ty),d=defs[id],m=meta.get(idx(tx,ty));\n      if(d&&d.solid){\n        if(d.door&&m?.open)continue;\n        if(d.platform&&dy<0)continue;\n        if(d.platform&&ledgeDrop>0)continue;\n        if(d.platform&&y+h-dy>ty*TILE+5)continue;\n        return true;\n      }\n    }\n    return false;\n  }\n`,
  'collision rules'
);

replaceRegexOnce(
  /  function movePlayer\(dt\)\{[^\n]*\}\n  function applyConveyors/,
  `  function playerTouchesLadder(){\n    const tx=Math.floor((player.x+player.w/2)/TILE),top=Math.floor((player.y+3)/TILE),bottom=Math.floor((player.y+player.h-3)/TILE);\n    for(let ty=top;ty<=bottom;ty++)if(defs[get(tx,ty)]?.ladder)return true;\n    return false;\n  }\n  function standingOnPlatform(){\n    const ty=Math.floor((player.y+player.h+2)/TILE),left=Math.floor((player.x+2)/TILE),right=Math.floor((player.x+player.w-3)/TILE);\n    for(let tx=left;tx<=right;tx++)if(defs[get(tx,ty)]?.platform)return true;\n    return false;\n  }\n  function movePlayer(dt){\n    const left=keys.KeyA||keys.ArrowLeft||keys.touchleft,right=keys.KeyD||keys.ArrowRight||keys.touchright;\n    const up=keys.KeyW||keys.ArrowUp||keys.touchjump,down=keys.KeyS||keys.ArrowDown;\n    const dir=(left?-1:0)+(right?1:0),onLadder=playerTouchesLadder(),accel=player.onGround?1150:650;\n    ledgeDrop=Math.max(0,ledgeDrop-dt);\n    if(down&&player.onGround&&!onLadder&&standingOnPlatform()){ledgeDrop=.22;player.onGround=false;player.y+=3;player.vy=Math.max(player.vy,70)}\n    if(onLadder&&down)ledgeDrop=Math.max(ledgeDrop,.12);\n    player.vx+=dir*accel*dt;player.vx*=Math.pow(player.onGround?0.0008:0.04,dt);player.vx=Math.max(-180,Math.min(180,player.vx));\n    if(dir){player.facing=dir;player.walk+=dt*10}\n    if(onLadder){\n      if(keys.Space){player.vy=-270;player.onGround=false;keys.Space=false}\n      else if(up||down){player.vy=(down?1:-1)*125;player.onGround=false}\n      else{player.vy*=Math.pow(.002,dt);if(Math.abs(player.vy)<3)player.vy=0}\n    }else{\n      if((keys.Space||keys.KeyW||keys.ArrowUp||keys.touchjump)&&player.onGround){player.vy=-335;player.onGround=false;keys.Space=keys.KeyW=keys.ArrowUp=keys.touchjump=false}\n      player.vy=Math.min(570,player.vy+920*dt);\n    }\n    let nx=player.x+player.vx*dt;\n    if(!rectCollides(nx,player.y,player.w,player.h,0))player.x=nx;else{const step=Math.sign(player.vx);for(let i=0;i<Math.abs(player.vx*dt);i++)if(!rectCollides(player.x+step,player.y,player.w,player.h,0))player.x+=step;else break;player.vx=0}\n    player.onGround=false;let ny=player.y+player.vy*dt;\n    if(!rectCollides(player.x,ny,player.w,player.h,player.vy))player.y=ny;else{const step=Math.sign(player.vy);for(let i=0;i<Math.abs(player.vy*dt);i++)if(!rectCollides(player.x,player.y+step,player.w,player.h,player.vy))player.y+=step;else break;if(player.vy>0)player.onGround=true;player.vy=0}\n    if(player.y>WORLD_H*TILE){player.health-=10;player.x=9*TILE;player.y=(groundAt(9)-4)*TILE;player.vy=0;showToast('Recovered at the field camp')}\n    player.x=Math.max(TILE,Math.min(WORLD_W*TILE-player.w-TILE,player.x));\n    if(Math.abs(player.x/TILE-205)<4&&!stats.coreFound){stats.coreFound=true;showToast('Foundry Core found — connect a 6-power grid')}\n    applyConveyors(dt);camera.x+=(player.x+player.w/2-vw*.5-camera.x)*Math.min(1,dt*6);camera.y+=(player.y+player.h/2-vh*.55-camera.y)*Math.min(1,dt*5);camera.x=Math.max(0,Math.min(WORLD_W*TILE-vw,camera.x));camera.y=Math.max(0,Math.min(WORLD_H*TILE-vh,camera.y))\n  }\n  function applyConveyors`,
  'player movement'
);

replaceOnce(
  "    const mode=()=>{engine.revision++;simulateNetworks();updateInspector();return true};",
  "    const mode=()=>{engine.revision++;simulateNetworks();updateInspector();return true};\n    if(id===T.DOOR){m.open=!m.open;meta.set(k,m);showToast(m.open?'Door opened':'Door closed');return mode()}",
  'door interaction'
);

replaceOnce(
  "  function defaultMeta(id){const m={rot:rotation,on:false,powered:false,active:false,running:false,linked:false};if(id===T.GEAR)m.mode=1;return m}",
  "  function defaultMeta(id){const m={rot:rotation,on:false,powered:false,active:false,running:false,linked:false};if(id===T.GEAR)m.mode=1;if(id===T.DOOR)m.open=false;return m}",
  'door placement metadata'
);

replaceRegexOnce(
  /  function renderCraft\(\)\{[^\n]*\}\n  function craft/,
  `  const craftState={category:'all',query:'',affordable:false};\n  const craftCategories=['all','structure','power','logic','motion','logistics'];\n  function recipeCategory(r){\n    if(r.category)return r.category;\n    const structure=new Set(['platform','copperBlock','ironBlock','frame','beam','door','ladder','ledge']);\n    const power=new Set(['wire','generator','lamp']);\n    const logic=new Set(['signal','switch','sensor','and','or','not','xor','nand','nor','timer','pulse','toggle','latch','counter','signalSplit']);\n    const motion=new Set(['motor','shaft','gear','piston','winch','clutch','belt','pulley','bevel','smallGear','largeGear','bearing','hinge']);\n    const logistics=new Set(['conveyor','hopper','storage','splitter','filter']);\n    if(structure.has(r.key))return 'structure';if(power.has(r.key))return 'power';if(logic.has(r.key))return 'logic';if(motion.has(r.key))return 'motion';if(logistics.has(r.key))return 'logistics';return 'structure';\n  }\n  function ensureCraftControls(){\n    if(document.getElementById('craftSearch'))return;\n    const controls=document.createElement('div');controls.className='craft-controls';\n    const search=document.createElement('input');search.id='craftSearch';search.type='search';search.placeholder='Search parts, uses, or materials…';search.autocomplete='off';search.addEventListener('input',()=>{craftState.query=search.value.trim().toLowerCase();renderCraft()});\n    const tabs=document.createElement('div');tabs.className='craft-tabs';\n    for(const category of craftCategories){const b=document.createElement('button');b.type='button';b.dataset.craftCategory=category;b.textContent=category==='all'?'All':category[0].toUpperCase()+category.slice(1);b.onclick=()=>{craftState.category=category;renderCraft()};tabs.appendChild(b)}\n    const filter=document.createElement('label');filter.className='craft-affordable';const cb=document.createElement('input');cb.type='checkbox';cb.onchange=()=>{craftState.affordable=cb.checked;renderCraft()};filter.append(cb,document.createTextNode(' Can craft now'));\n    const count=document.createElement('span');count.id='craftResultCount';count.className='craft-result-count';\n    controls.append(search,tabs,filter,count);document.getElementById('recipes').before(controls);\n  }\n  function renderCraft(){\n    ensureCraftControls();\n    const resources=['dirt','stone','wood','copper','iron','crystal'];document.getElementById('resourceChips').innerHTML=resources.map(k=>\`<span class="chip" style="color:\${palette[k]||'#fff'}">\${k.toUpperCase()} \${inventory[k]||0}</span>\`).join('');\n    document.querySelectorAll('[data-craft-category]').forEach(b=>b.classList.toggle('active',b.dataset.craftCategory===craftState.category));\n    const query=craftState.query,visible=recipes.filter(r=>{const can=Object.entries(r.cost).every(([k,v])=>(inventory[k]||0)>=v),category=recipeCategory(r),haystack=\`\${r.name} \${r.desc} \${Object.keys(r.cost).join(' ')} \${category}\`.toLowerCase();return(craftState.category==='all'||category===craftState.category)&&(!query||haystack.includes(query))&&(!craftState.affordable||can)});\n    document.getElementById('craftResultCount').textContent=\`\${visible.length} of \${recipes.length} parts\`;\n    const el=document.getElementById('recipes');el.innerHTML='';\n    if(!visible.length){const empty=document.createElement('div');empty.className='craft-empty';empty.textContent='No parts match those filters.';el.appendChild(empty);return}\n    visible.forEach(r=>{const can=Object.entries(r.cost).every(([k,v])=>(inventory[k]||0)>=v),b=document.createElement('button'),category=recipeCategory(r);b.className='recipe';b.disabled=!can;b.innerHTML=\`<span class="recipe-icon">\${r.icon}</span><span><span class="recipe-category">\${category}</span><b>\${r.name}</b><small>\${Object.entries(r.cost).map(([k,v])=>\`\${v} \${k}\`).join(' · ')}<br>\${r.desc}</small></span><span class="make">+\${r.out}</span>\`;b.onclick=()=>{craft(r);selected=items.findIndex(i=>i.key===r.key);renderHotbar()};el.appendChild(b)});\n  }\n  function craft`,
  'fabricator filtering'
);

replaceOnce(
  "    document.querySelector('.hint').textContent='A/D move · SPACE jump · E fabricate · R rotate · C lens · V view · L link mode · F take · X drop';",
  "    document.querySelector('.hint').textContent='A/D move · SPACE jump · W/S ladders · DOWN drop ledge · RIGHT door/place · E fabricate · R rotate · C lens';",
  'HUD hint'
);

replaceOnce(
  "card('Explore and fabricate','A/D moves, Space jumps. Left click mines; walk near dropped resources to collect. E opens the Fabricator. Crafting selects the new part. Right click places; R rotates before placing. Shift+R rotates the block under the cursor. The hotbar scrolls horizontally.'),",
  "card('Explore and fabricate','A/D moves, Space jumps. W/S climbs ladders. Jump upward through ledges and press S/Down while standing on one to drop through. Right-click a door to open or close it. Left click mines; E opens the categorized, searchable Fabricator. Crafting selects the new part. Right click places; R rotates before placing. Shift+R rotates the block under the cursor.'),",
  'manual movement help'
);

const craftCss = `\n/* Circuitbound: navigable Fabricator controls */\n.craft-controls{display:grid;grid-template-columns:minmax(190px,1fr) auto;gap:8px 12px;align-items:center;margin:4px 0 12px;padding:10px;background:#0c2028;border:1px solid #314c55;position:sticky;top:-17px;z-index:3}\n#craftSearch{grid-column:1/-1;width:100%;background:#08171d;border:1px solid #46636b;color:var(--ink);padding:9px 10px;font:700 11px ui-monospace,monospace;outline:0}#craftSearch:focus{border-color:var(--amber);box-shadow:0 0 0 2px #ffc24a22}#craftSearch::placeholder{color:#78908b}\n.craft-tabs{display:flex;gap:5px;flex-wrap:wrap}.craft-tabs button{background:#142932;border:1px solid #3c5962;color:#c9d8d3;padding:6px 8px;cursor:pointer;font:800 10px ui-monospace,monospace;text-transform:uppercase}.craft-tabs button:hover,.craft-tabs button.active{border-color:var(--amber);color:#ffe2a0;background:#362f20}\n.craft-affordable{justify-self:end;font:700 10px ui-monospace,monospace;color:#b9cbc5;white-space:nowrap;cursor:pointer}.craft-affordable input{accent-color:#ffc24a}.craft-result-count{grid-column:1/-1;font:700 9px ui-monospace,monospace;color:#7f9b95}.recipe-category{display:block;color:var(--cyan);font:800 8px ui-monospace,monospace;text-transform:uppercase;letter-spacing:.1em;margin-bottom:2px}.craft-empty{grid-column:1/-1;padding:24px;text-align:center;border:1px dashed #3b555c;color:#8ea6a0;font:700 11px ui-monospace,monospace}\n@media(max-width:650px){.craft-controls{grid-template-columns:1fr;position:static}.craft-tabs{grid-column:1}.craft-affordable{justify-self:start}.craft-result-count{grid-column:1}}\n`;
if (!css.includes('Circuitbound: navigable Fabricator controls')) css += craftCss;

fs.writeFileSync(appPath, app);
fs.writeFileSync(cssPath, css);
console.log('Applied Circuitbound doors, ladders, ledges, and Fabricator navigation patch.');
