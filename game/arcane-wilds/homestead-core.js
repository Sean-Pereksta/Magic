/* Hearthglade's deterministic economy and clocks. No DOM, networking or timers. */
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.AWHomeCore=api;})(globalThis,()=>{
  'use strict';
  const HOUR=3600000, MINUTE=60000, DAY=24*HOUR;
  const tiers=[{name:'Homestead deed',size:0,beds:0,trees:0,cost:{}},
    {name:"Wanderer's Cottage",size:6,beds:4,trees:0,cost:{gold:60,timber:20,stone:10}},
    {name:'Established Homestead',size:8,beds:8,trees:2,cost:{gold:180,timber:40,stone:30,iron:8}},
    {name:'Arcane Manor',size:10,beds:16,trees:4,continent:'meridian',cost:{gold:480,timber:75,stone:50,iron:12,frost:8}},
    {name:"Wayfarer's Sanctum",size:12,beds:24,trees:8,continent:'gloam',cost:{gold:1200,timber:100,stone:75,iron:20,dust:16,bone:10}}];
  const crops={
    lanternberry:{name:'Lanternberries',hours:.5,seed:3,yield:3,value:2,continent:'verdant',color:'#eeaf64'},
    strawberry:{name:'Sunmere Strawberries',hours:2,seed:5,yield:4,value:3,continent:'verdant',color:'#e96577'},
    goldenroot:{name:'Goldenroot',hours:4,seed:8,yield:4,value:4,continent:'verdant',color:'#ffc965'},
    melon:{name:'Honey Melon',hours:8,seed:12,yield:3,value:8,continent:'verdant',color:'#bdde85'},
    moonmint:{name:'Moonmint',hours:6,seed:10,yield:4,value:5,continent:'verdant',color:'#94d8c9'},
    sunapple:{name:'Sunapple Tree',hours:48,repeat:16,seed:25,yield:5,value:5,tree:true,continent:'verdant',color:'#ffc978'},
    stormgrape:{name:'Stormgrapes',hours:12,seed:18,yield:5,value:7,continent:'meridian',color:'#bca7ed'},
    emberpepper:{name:'Emberpeppers',hours:16,seed:20,yield:4,value:10,continent:'meridian',color:'#ff8563'},
    frostpear:{name:'Frostpear Tree',hours:60,repeat:20,seed:50,yield:5,value:9,tree:true,continent:'meridian',color:'#a4d6f1'},
    starplum:{name:'Starplum Tree',hours:72,repeat:24,seed:80,yield:5,value:14,tree:true,continent:'gloam',color:'#caa7ff'}
  };
  // footprint is measured in walkable world tiles; rugs and floor pieces do not block.
  const items={
    hearth:{name:'Hearth',icon:'🔥',size:[1,1],cost:{gold:12,stone:6},use:'rest'},
    bed:{name:'Comfortable Bed',icon:'🛏',size:[1,2],cost:{gold:15,timber:8,hide:2},use:'sleep'},
    chest:{name:'Storage Chest',icon:'📦',size:[1,1],cost:{gold:8,timber:6},use:'inventory'},
    workbench:{name:"Builder's Workbench",icon:'🔨',size:[2,1],cost:{gold:12,timber:8,iron:2},use:'build'},
    stove:{name:'Cooking Hearth',icon:'🍲',size:[1,1],cost:{gold:30,stone:8,iron:3},use:'cook'},
    alchemy:{name:'Alchemy Bench',icon:'⚗',size:[2,1],cost:{gold:65,timber:10,dust:6},tier:2,use:'alchemy'},
    coinbloom:{name:'Coinbloom Urn',icon:'🌼',size:[1,1],cost:{gold:45,stone:5,dust:3},producer:{resource:'gold',every:HOUR/4,cap:96}},
    condenser:{name:'Arcane Condenser',icon:'💎',size:[1,1],cost:{gold:110,iron:8,frost:4},tier:2,producer:{resource:'dust',every:6*HOUR,cap:4}},
    timberRack:{name:'Enchanted Timber Rack',icon:'🪵',size:[2,1],cost:{gold:35,timber:8,dust:2},outdoor:true,producer:{resource:'timber',every:45*MINUTE,cap:32}},
    stoneCache:{name:'Stone Cache',icon:'🪨',size:[1,1],cost:{gold:35,stone:8,dust:2},outdoor:true,producer:{resource:'stone',every:HOUR,cap:24}},
    composter:{name:'Composter',icon:'♻',size:[1,1],cost:{gold:20,timber:8,fiber:4},outdoor:true,use:'compost'},
    seedCabinet:{name:'Seed Cabinet',icon:'🌱',size:[1,1],cost:{gold:20,timber:8},use:'seeds'},
    cistern:{name:'Water Cistern',icon:'💧',size:[1,1],cost:{gold:75,stone:12,iron:4},outdoor:true,tier:2,use:'refill'},
    sprinkler:{name:'Garden Sprinkler',icon:'⛲',size:[1,1],cost:{gold:50,iron:5,stone:4},outdoor:true,tier:2,use:'irrigation'},
    pantry:{name:'Pantry',icon:'🥕',size:[1,1],cost:{gold:20,timber:10},use:'pantry'},
    lectern:{name:'Spell Lectern',icon:'📖',size:[1,1],cost:{gold:25,timber:8,dust:2},use:'spellbook'},
    trophy:{name:'Trophy Display',icon:'🏆',size:[1,1],cost:{gold:35,timber:8,iron:2},use:'trophies'},
    stable:{name:'Stable Shelter',icon:'🐎',size:[2,2],cost:{gold:70,timber:20,hide:4},outdoor:true,tier:2,use:'mounts'},
    villagePortal:{name:'Village Portal',icon:'◉',size:[1,1],cost:{gold:120,stone:10,iron:4,dust:3},portal:'village',tier:2},
    continentalPortal:{name:'Continental Portal',icon:'◎',size:[1,1],cost:{gold:400,stone:25,frost:8,ember:6,dust:12},portal:'continental',tier:3},
    shadowPortal:{name:'Shadow Gate',icon:'✦',size:[1,1],cost:{gold:800,stone:35,bone:12,voidshard:8,dust:20},portal:'shadow',tier:4},
    chair:{name:'Wooden Chair',icon:'🪑',size:[1,1],cost:{gold:4,timber:3}},
    table:{name:'Dining Table',icon:'▤',size:[2,1],cost:{gold:10,timber:6}},
    bookcase:{name:'Bookcase',icon:'📚',size:[1,1],cost:{gold:15,timber:6}},
    lantern:{name:'Lantern',icon:'🏮',size:[1,1],cost:{gold:8,iron:1},solid:false},
    rug:{name:'Woven Rug',icon:'▧',size:[2,2],cost:{gold:12,fiber:6},solid:false},
    flowers:{name:'Flower Pot',icon:'🌷',size:[1,1],cost:{gold:5,stone:2},solid:false},
    partition:{name:'Room Partition',icon:'▥',size:[1,2],cost:{gold:4,timber:5}},
    doorway:{name:'Interior Doorway',icon:'🚪',size:[1,1],cost:{gold:5,timber:4},solid:false},
    floor:{name:'Stone Floor Inlay',icon:'◇',size:[1,1],cost:{stone:2},solid:false}
  };
  const recipes={
    berryMeal:{name:'Berry Preserve',needs:{lanternberry:3},heal:.35,station:'stove'},
    gardenStew:{name:'Garden Stew',needs:{goldenroot:2,strawberry:2},heal:.65,station:'stove'},
    orchardFeast:{name:'Orchard Feast',needs:{sunapple:3,melon:1},heal:1,station:'stove'},
    moonTonic:{name:'Moonmint Tonic',needs:{moonmint:3,lanternberry:2},heal:.75,station:'alchemy'},
    astralFeast:{name:'Astral Feast',needs:{starplum:2,frostpear:2,emberpepper:1},heal:1,station:'stove'}
  };
  const clone=x=>JSON.parse(JSON.stringify(x)), num=x=>Number.isFinite(Number(x))?Math.max(0,Number(x)):0;
  const require=(ok,message)=>{if(!ok)throw new Error(message);};
  const rect=(x,y,w,h)=>({x,y,w,h});
  function fresh(id,now){return {version:1,journeyId:id,revision:0,serial:0,mode:'local',deed:false,tier:0,house:{x:4,y:6,rotation:0},items:[],plots:[],seeds:{},produce:{},meals:{},fertilizer:0,canLevel:1,water:6,rested:0,visited:false,claims:[],requests:[],returnPoint:null,lastEvaluatedAt:now};}
  function normalize(raw,id,now){
    const h={...fresh(id,now),...clone(raw||{})};h.version=1;h.tier=Math.min(4,Math.floor(num(h.tier)));h.revision=Math.floor(num(h.revision));h.serial=Math.floor(num(h.serial));
    for(const k of ['items','plots','claims','requests'])if(!Array.isArray(h[k]))h[k]=[];
    h.items=h.items.filter(i=>items[i.kind]).slice(0,150);h.plots=h.plots.filter(p=>Number.isFinite(p.x)&&Number.isFinite(p.y)).slice(0,32);
    for(const k of ['seeds','produce','meals'])if(!h[k]||typeof h[k]!=='object'||Array.isArray(h[k]))h[k]={};
    h.requests=h.requests.slice(-128);h.water=Math.min(h.canLevel>1?24:6,num(h.water));h.lastEvaluatedAt=num(h.lastEvaluatedAt)||now;
    for(const p of h.plots){if(p.plant&&!crops[p.plant.crop])p.plant=null;}
    return h;
  }
  function interior(h){const size=tiers[h.tier].size;return rect((18-size)/2,(14-size)/2,size,size);}
  function footprint(item){const d=items[item.kind],r=(item.rotation||0)%2;return rect(item.x,item.y,d.size[r?1:0],d.size[r?0:1]);}
  const overlaps=(a,b)=>a.x<b.x+b.w&&a.x+a.w>b.x&&a.y<b.y+b.h&&a.y+a.h>b.y;
  const contains=(a,b)=>b.x>=a.x&&b.y>=a.y&&b.x+b.w<=a.x+a.w&&b.y+b.h<=a.y+a.h;
  const tileKey=(x,y)=>x+','+y;
  const garden=rect(11,2,6,4), workshop=rect(11,9,6,3), orchard=rect(2,1,8,4), clearing=rect(2,5,8,6);
  function houseRect(h){return rect(h.house.x,h.house.y,4,3);}
  function blocked(h,inside=true){return h.items.filter(i=>!i.packed&&!!items[i.kind].outdoor!==inside&&items[i.kind].solid!==false).map(footprint);}
  function placement(h,item){
    const d=items[item.kind];if(!d)return 'Unknown furnishing.';
    if(!Number.isInteger(item.x)||!Number.isInteger(item.y))return 'Snap to a whole floor tile.';
    if(h.tier<(d.tier||1))return `Requires house tier ${d.tier||1}.`;
    const r=footprint(item),others=h.items.filter(i=>i.id!==item.id&&!i.packed&&!!items[i.kind].outdoor===!!d.outdoor);
    if(d.outdoor){
      const zones=[workshop];
      if(!zones.some(z=>contains(z,r)))return 'Place outdoor structures in the workshop yard (right of the path).';
    }else if(!contains(interior(h),r))return 'Place furnishings within the house floor.';
    if(d.solid!==false&&others.some(i=>items[i.kind].solid!==false&&overlaps(r,footprint(i))))return 'Another furnishing occupies these tiles.';
    if(d.producer&&others.some(i=>items[i.kind].producer?.resource===d.producer.resource))return 'Only one active producer of each resource. Upgrade the existing one.';
    if(d.solid===false)return '';
    const floor=d.outdoor?rect(0,0,18,14):interior(h),door=d.outdoor?rect(17,7,1,1):{x:8,y:floor.y+floor.h-1,w:2,h:1};
    if(overlaps(r,door))return 'Keep the entrance clear.';
    const solids=[...others.filter(i=>items[i.kind].solid!==false).map(footprint),r];
    const free=(x,y)=>contains(floor,rect(x,y,1,1))&&!solids.some(b=>overlaps(b,rect(x,y,1,1)));
    const seen=new Set(),queue=[[door.x,door.y]];
    for(let q=0;q<queue.length;q++){const [x,y]=queue[q],key=tileKey(x,y);if(seen.has(key)||!free(x,y))continue;seen.add(key);for(const [dx,dy] of [[1,0],[-1,0],[0,1],[0,-1]])queue.push([x+dx,y+dy]);}
    for(let x=floor.x;x<floor.x+floor.w;x++)for(let y=floor.y;y<floor.y+floor.h;y++)if(free(x,y)&&!seen.has(tileKey(x,y)))return 'This placement cuts off part of the house.';
    for(const b of solids){let reachable=false;for(let x=b.x-1;x<=b.x+b.w;x++)for(let y=b.y-1;y<=b.y+b.h;y++)if(((x===b.x-1||x===b.x+b.w)&&y>=b.y&&y<b.y+b.h||(y===b.y-1||y===b.y+b.h)&&x>=b.x&&x<b.x+b.w)&&seen.has(tileKey(x,y)))reachable=true;if(!reachable)return 'Leave a reachable edge beside every furnishing.';}
    return '';
  }
  function waterHours(plot){return plot.tree?24:plot.soil>=2?16:plot.soil===1?12:8;}
  function grow(plot,now){
    const p=plot.plant;if(!p)return;now=Math.max(num(p.evaluatedAt),now);
    const delta=Math.max(0,Math.min(now,num(p.wateredUntil))-num(p.evaluatedAt));
    p.growthMs=Math.min(p.requiredGrowthMs,num(p.growthMs)+delta);p.evaluatedAt=now;
  }
  function ready(plot){return !!plot.plant&&plot.plant.growthMs>=plot.plant.requiredGrowthMs;}
  function sprinklerPlots(h){
    // A sprinkler irrigates the four closest annual beds; the yard has fixed buried channels.
    const result=new Set();for(const s of h.items.filter(i=>i.kind==='sprinkler'&&!i.packed)){
      for(const p of h.plots.filter(p=>!p.tree).sort((a,b)=>Math.hypot(a.x-s.x,a.y-s.y)-Math.hypot(b.x-s.x,b.y-s.y)||a.id.localeCompare(b.id)).slice(0,4))result.add(p.id);
    }return result;
  }
  function settle(h,time){
    const now=Math.max(num(h.lastEvaluatedAt),num(time)),from=num(h.lastEvaluatedAt),tanks=h.items.filter(i=>i.kind==='cistern'&&!i.packed),covered=sprinklerPlots(h);
    // Event stepping is bounded by stored water, not by hours/days offline.
    let supply=tanks.reduce((sum,t)=>sum+num(t.water),0),steps=0;
    while(supply>0&&steps++<4800){
      const due=h.plots.filter(p=>covered.has(p.id)&&p.plant&&!ready(p)).map(p=>({p,at:Math.max(from,num(p.plant.wateredUntil),num(p.plant.evaluatedAt))})).filter(e=>e.at<=now).sort((a,b)=>a.at-b.at||a.p.id.localeCompare(b.p.id))[0];
      if(!due)break;grow(due.p,due.at);if(ready(due.p))continue;
      const tank=tanks.find(t=>t.water>0);tank.water--;supply--;due.p.plant.lastWateredAt=due.at;due.p.plant.wateredUntil=due.at+waterHours(due.p)*HOUR;
    }
    for(const p of h.plots)grow(p,now);
    for(const i of h.items){
      const d=items[i.kind];if(!d?.producer)continue;
      const last=Number.isFinite(i.evaluatedAt)?i.evaluatedAt:now;i.evaluatedAt=now;
      if(i.packed)continue;
      const level=Math.min(3,i.level||1),cap=d.producer.cap*level,every=d.producer.every/(1+(level-1)*.5);
      if(num(i.stored)>=cap){i.stored=cap;i.remainder=0;continue;}
      const elapsed=Math.max(0,now-last)+num(i.remainder),made=Math.floor(elapsed/every);
      i.stored=Math.min(cap,num(i.stored)+made);i.remainder=i.stored>=cap?0:elapsed%every;
    }
    h.lastEvaluatedAt=now;return h;
  }
  function charge(wallet,cost){for(const [k,v] of Object.entries(cost)){const n=k==='gold'?wallet.gold:wallet.materials[k];require(num(n)>=v,`Need ${v} ${k}; you have ${num(n)}.`);}for(const[k,v]of Object.entries(cost))if(k==='gold')wallet.gold-=v;else wallet.materials[k]-=v;}
  function add(wallet,k,n){if(k==='gold')wallet.gold=num(wallet.gold)+n;else wallet.materials[k]=num(wallet.materials[k])+n;}
  function unlocked(ctx,c){return !c||(ctx.unlocked||['verdant']).includes(c);}
  function portalReason(item,destination,ctx){
    const d=items[item.kind];if(!d?.portal)return 'This is not a portal.';
    if(destination?.shadow)return d.portal==='shadow'&&ctx.shadowUnlocked&&(ctx.shadowWaystones||[]).includes(destination.depth)?'':'Earn this Shadow Sanctuary checkpoint first.';
    if(d.portal==='shadow')return 'The Shadow Gate accepts earned Shadow Sanctuary checkpoints.';
    if(!destination?.town||!(ctx.visited||[]).includes(destination.id)||!unlocked(ctx,destination.continent))return 'Visit and unlock this settlement first.';
    if(d.portal==='village'&&destination.continent!=='verdant')return 'Build a Continental Portal for other continents.';
    return '';
  }
  function apply(input,walletInput,action,time,ctx={}){
    const h=clone(input),wallet=clone(walletInput),now=Math.max(num(time),num(h.lastEvaluatedAt));wallet.materials||={};
    require(action&&typeof action.type==='string','Invalid home action.');
    require(typeof action.requestId==='string'&&action.requestId.length>0,'A unique action identifier is required.');
    if(h.requests.includes(action.requestId))return {home:h,wallet,effects:[],duplicate:true};
    const effects=[];settle(h,now);
    const atHome=ctx.atHome===true,atTown=ctx.atTown===true;
    require(atHome||atTown||action.type==='claimExpedition','Return to Hearthglade or a home supplier first.');
    const nextId=prefix=>`${prefix}-${++h.serial}`;
    const findItem=()=>{const i=h.items.find(i=>i.id===action.id);require(i,'Furnishing not found.');return i;};
    const findPlot=()=>{const p=h.plots.find(p=>p.id===action.id);require(p,'Garden bed not found.');return p;};
    const station=kind=>h.items.some(i=>i.kind===kind&&!i.packed);
    const allHome=()=>require(atHome,'This action is available only at home.');
    switch(action.type){
      case 'claimExpedition': {
        require(ctx.earned===true,'Complete the encounter first.');require(typeof action.node==='string','Encounter missing.');
        if(!h.claims.includes(action.node)){h.claims.push(action.node);add(wallet,'timber',6);add(wallet,'stone',4);add(wallet,'fiber',3);
          if(!h.deed&&ctx.multiWave){h.deed=true;add(wallet,'gold',60);add(wallet,'timber',20);add(wallet,'stone',10);h.seeds.lanternberry=num(h.seeds.lanternberry)+4;h.seeds.strawberry=num(h.seeds.strawberry)+2;
            for(const kind of ['hearth','bed','chest','workbench'])h.items.push({id:nextId('item'),kind,packed:true,gift:true,level:1,evaluatedAt:now,stored:0,remainder:0});effects.push({type:'message',text:'A Place to Return: Hearthglade deed earned! Starter building materials and seeds are ready.'});}}
        break;
      }
      case 'buildHouse': {allHome();require(h.deed,'Clear a multi-wave encounter to earn your homestead deed.');require(h.tier<4,'Your house is fully expanded.');const t=tiers[h.tier+1];require(unlocked(ctx,t.continent),'Explore the next continent before this expansion.');charge(wallet,t.cost);h.tier++;effects.push({type:'message',text:t.name+' built.'});break;}
      case 'moveHouse': {allHome();require(h.tier>0,'Build the cottage first.');const r=rect(action.x,action.y,4,3);require(Number.isInteger(r.x)&&Number.isInteger(r.y)&&contains(clearing,r),'Keep the house within its clearing.');h.house={x:r.x,y:r.y,rotation:0};break;}
      case 'craft': {allHome();require(h.tier>0,'Build your cottage first.');const d=items[action.kind];require(d,'Unknown blueprint.');require(h.tier>=(d.tier||1),'Upgrade the house to unlock this blueprint.');require(h.items.length<150,'Pack or dismantle unused furnishings before crafting more.');charge(wallet,d.cost);h.items.push({id:nextId('item'),kind:action.kind,packed:true,level:1,evaluatedAt:now,stored:0,remainder:0,water:0});break;}
      case 'place': {allHome();const i=findItem(),candidate={...i,x:action.x,y:action.y,rotation:action.rotation||0,packed:false};const reason=placement(h,candidate);require(!reason,reason);Object.assign(i,candidate);i.evaluatedAt=now;break;}
      case 'pack': {allHome();const i=findItem();i.packed=true;i.evaluatedAt=now;break;}
      case 'dismantle': {allHome();const i=findItem();require(i.packed,'Pack this furnishing before dismantling.');require(!num(i.stored),'Collect stored output before dismantling.');if(!i.gift)for(const[k,v]of Object.entries(items[i.kind].cost))add(wallet,k,Math.floor(v*.5));h.items=h.items.filter(x=>x.id!==i.id);break;}
      case 'upgradeProducer': {allHome();const i=findItem(),d=items[i.kind];require(d.producer,'Not a producer.');require(i.level<3,'Maximum producer level.');charge(wallet,{gold:60*i.level,iron:4*i.level,dust:2*i.level});i.level++;break;}
      case 'collect': {allHome();let collected=0;for(const i of h.items){const p=items[i.kind].producer;if(p&&(!action.id||action.id===i.id)){add(wallet,p.resource,num(i.stored));collected+=num(i.stored);i.stored=0;i.evaluatedAt=now;}}effects.push({type:'message',text:collected?'Collected '+collected+' supplies.':'No supplies ready yet.'});break;}
      case 'bed': {allHome();require(h.tier>0,'Build a cottage first.');const tree=!!action.tree,limit=tree?tiers[h.tier].trees:tiers[h.tier].beds;require(h.plots.filter(p=>p.tree===tree).length<limit,'Expand the house to unlock more growing spaces.');const r=rect(action.x,action.y,tree?2:1,tree?2:1);require(Number.isInteger(r.x)&&Number.isInteger(r.y)&&contains(tree?orchard:garden,r),'Place beds in the garden or trees in the orchard.');require(!h.plots.some(p=>overlaps(r,rect(p.x,p.y,p.tree?2:1,p.tree?2:1))),'Growing spaces overlap.');charge(wallet,tree?{gold:10,timber:4}:{timber:3,fiber:1});h.plots.push({id:nextId('plot'),x:r.x,y:r.y,tree,soil:0,plant:null});break;}
      case 'buySeeds': {require(atTown,'Buy seeds from a settlement supplier.');const c=crops[action.crop],count=Math.floor(num(action.count)||1);require(c&&count<=99,'Choose a valid seed quantity.');require(ctx.continent===c.continent,'This seed is sold on its home continent.');require(unlocked(ctx,c.continent),'That continent is locked.');charge(wallet,{gold:c.seed*count});h.seeds[action.crop]=num(h.seeds[action.crop])+count;break;}
      case 'buyMaterials': {require(atTown,'Visit a settlement supplier.');const price={timber:2,stone:2,fiber:1}[action.resource],count=Math.floor(num(action.count)||10);require(price&&count<=100,'Invalid supply order.');charge(wallet,{gold:price*count});add(wallet,action.resource,count);break;}
      case 'plant': {allHome();const p=findPlot(),c=crops[action.crop];require(c,'Unknown crop.');require(!p.plant,'Harvest or remove the existing plant first.');require(!!c.tree===p.tree,'Use orchard plots for fruit trees and garden beds for annual crops.');require(num(h.seeds[action.crop])>0,'Purchase seeds at a settlement.');require(unlocked(ctx,c.continent),'Unlock this crop’s continent first.');h.seeds[action.crop]--;p.plant={id:nextId('plant'),crop:action.crop,plantedAt:now,cycleStartedAt:now,lastWateredAt:null,wateredUntil:now,evaluatedAt:now,growthMs:0,requiredGrowthMs:c.hours*HOUR,harvestNumber:0,fertilized:false};break;}
      case 'water': {allHome();const targets=action.id?[findPlot()]:h.plots.filter(p=>p.plant&&!ready(p)&&p.plant.wateredUntil<=now+HOUR);require(targets.some(p=>p.plant&&!ready(p)),'No growing plants need water.');for(const p of targets){if(!p.plant||ready(p))continue;if(!h.water)break;h.water--;p.plant.lastWateredAt=now;p.plant.wateredUntil=now+waterHours(p)*HOUR;p.plant.evaluatedAt=now;}require(h.water!==input.water,'Refill your watering can at the well.');h.firstWatered=true;break;}
      case 'refillCan': allHome();h.water=h.canLevel>1?24:6;break;
      case 'upgradeCan': allHome();require(h.canLevel===1,'Your watering can is already upgraded.');charge(wallet,{gold:50,iron:4});h.canLevel=2;h.water=24;break;
      case 'refillCistern': {allHome();const tank=findItem();require(tank.kind==='cistern'&&!tank.packed,'Place a cistern first.');tank.water=32;break;}
      case 'upgradeSoil': {allHome();const p=findPlot();require(!p.tree,'Orchard soil already retains a full day of water.');require(p.soil<2,'This bed is already a greenhouse bed.');require(p.soil===0||h.tier>=3,'Greenhouses require the Arcane Manor.');charge(wallet,p.soil===0?{gold:15,fiber:4}:{gold:40,frost:2,ember:2});p.soil++;break;}
      case 'fertilize': {allHome();const p=findPlot();require(p.plant&&!ready(p)&&!p.plant.fertilized,'Fertilize a growing crop once per harvest cycle.');require(h.fertilizer>0,'Make fertilizer at a composter.');h.fertilizer--;p.plant.fertilized=true;p.plant.requiredGrowthMs=Math.max(p.plant.growthMs,p.plant.requiredGrowthMs*.9);break;}
      case 'harvest': {allHome();const p=findPlot();require(ready(p),'This crop is not mature yet.');const plant=p.plant,c=crops[plant.crop];h.produce[plant.crop]=num(h.produce[plant.crop])+c.yield+(plant.fertilized?1:0);if(c.tree){plant.harvestNumber++;plant.cycleStartedAt=now;plant.growthMs=0;plant.requiredGrowthMs=c.repeat*HOUR;plant.evaluatedAt=now;plant.wateredUntil=now;plant.fertilized=false;}else p.plant=null;h.firstHarvest=true;break;}
      case 'removePlant': allHome();findPlot().plant=null;break;
      case 'sell': {require(atTown,'Sell produce to a settlement supplier.');const c=crops[action.crop],count=Math.floor(num(action.count)||1);require(c&&num(h.produce[action.crop])>=count,'Not enough produce.');h.produce[action.crop]-=count;add(wallet,'gold',c.value*count);break;}
      case 'compost': {allHome();require(station('composter'),'Place a composter in your workshop yard.');require(crops[action.crop]&&num(h.produce[action.crop])>=2,'Composting uses two produce and one Plant Fiber.');charge(wallet,{fiber:1});h.produce[action.crop]-=2;h.fertilizer++;break;}
      case 'cook': {allHome();const r=recipes[action.recipe];require(r&&station(r.station),'Place the required cooking or alchemy station.');for(const[k,v]of Object.entries(r.needs))require(num(h.produce[k])>=v,'Missing '+v+' '+crops[k].name+'.');for(const[k,v]of Object.entries(r.needs))h.produce[k]-=v;h.meals[action.recipe]=num(h.meals[action.recipe])+1;break;}
      case 'eat': {const r=recipes[action.recipe];require(r&&num(h.meals[action.recipe])>0,'Prepare this meal first.');h.meals[action.recipe]--;effects.push({type:'heal',fraction:r.heal});break;}
      case 'rest': {allHome();const i=findItem();require(!i.packed&&['bed','hearth'].includes(i.kind),'Place a bed or hearth before resting.');effects.push({type:'heal',fraction:1});if(i.kind==='bed')h.rested=3;break;}
      case 'attune': {allHome();const i=findItem(),reason=portalReason(i,action.destination,ctx);require(!reason,reason);i.destination=clone(action.destination);break;}
      default: throw new Error('Unknown home action: '+action.type);
    }
    h.revision++;h.requests.push(action.requestId);h.requests=h.requests.slice(-128);return {home:h,wallet,effects};
  }
  return {HOUR,MINUTE,DAY,tiers,crops,items,recipes,fresh,normalize,clone,settle,grow,ready,waterHours,placement,footprint,interior,houseRect,blocked,garden,orchard,workshop,clearing,overlaps,contains,portalReason,apply};
});
