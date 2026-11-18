/* Expedition fleet, freight stations and route planning. Simulation is DOM-free. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.CircuitFoundryAutomation = factory();
})(globalThis, function () {
  'use strict';

  const roles = {
    surveyDrone: { name:'Survey Drone', job:'Scans a marked area for rare seams and pins the results.', cost:{surveyLens:2,cobaltIngot:3,fluxCell:1}, color:'#6fe1df', speed:8, range:42, carry:0 },
    rigTender: { name:'Rig Tender', job:'Loads cooled canisters and repair supplies, then returns empties to its pickup.', cost:{osmiumIngot:3,servoCore:2,precisionBearing:1}, color:'#80c5ff', speed:7, range:42, carry:10 },
    freightDrone: { name:'Freight Drone', job:'Moves stocked cargo between two marked depots.', cost:{graviteIngot:3,roboticFrame:2,servoCore:2}, color:'#ffa75d', speed:7, range:52, carry:24 },
    repairDrone: { name:'Repair Drone', job:'Repairs machines near the target using repair kits from its pickup.', cost:{osmiumIngot:3,precisionBearing:2,surveyLens:1}, color:'#9beaaf', speed:7, range:38, carry:5 },
    relayDrone: { name:'Relay Drone', job:'Holds position to extend fleet command range by 24 tiles until it needs to recharge.', cost:{nullQuartz:3,phaseLens:1,fluxCell:3}, color:'#db9dff', speed:7, range:44, carry:0 },
    shieldDrone: { name:'Shield Drone', job:'Blocks shots coming from its shield side while guarding you or a marked machine.', cost:{nullQuartz:2,graviteIngot:2,fluxCell:3}, color:'#9bbcff', speed:8, range:38, carry:0 },
    interceptorDrone: { name:'Interceptor Drone', job:'Fires turret rounds at incoming shots and airborne enemies in its marked zone.', cost:{starcoreIngot:2,servoCore:2,phaseLens:1}, color:'#f59d8f', speed:10, range:46, carry:12 },
    constructionDrone: { name:'Construction Drone', job:'Builds an approved plan using the exact parts stocked at the dock.', cost:{nullQuartz:2,servoCore:2,precisionBearing:3}, color:'#f3ce74', speed:6, range:32, carry:0 },
    railHauler: { name:'Rail Hauler', job:'Moves bulk cargo between named, powered stops joined by Freight Rail.', cost:{osmiumIngot:6,graviteIngot:3,roboticFrame:2,servoCore:2}, ground:true, rail:true, color:'#eda878', speed:12, range:720, carry:64 },
    tunnelCrawler: { name:'Tunnel Crawler', job:'Clears a path through ordinary soil and stone toward the marked tile.', cost:{osmiumIngot:6,diamondHead:2,servoCore:2}, ground:true, color:'#cba66a', speed:4, range:34, carry:0 },
    stabilizerWalker: { name:'Stabilizer Walker', job:'Anchors beside a marked rig to keep it stable during hazard surges.', cost:{graviteIngot:5,roboticFrame:2,servoCore:2}, ground:true, color:'#82ddbc', speed:4.5, range:34, carry:0 },
    bastionWalker: { name:'Bastion Walker', job:'Patrols between dock and defense point or holds position. Reloads from a stocked ammo bin.', cost:{starcoreIngot:3,osmiumIngot:4,roboticFrame:2,servoCore:3}, ground:true, color:'#ed9d87', speed:5, range:38, carry:16 }
  };
  const stationParts = [
    ['FOUNDRY_FLEET_DOCK',231,'foundryFleetDock','Expedition Fleet Dock','⊔','#8dcbd4',{steel:8,cobaltIngot:4,roboticFrame:2,fluxCell:3},'Powers, supplies, and assigns up to four expedition drones or robots.'],
    ['FREIGHT_STOP',232,'foundryRailStation','Freight Stop','▤','#dcad7f',{steel:8,osmiumIngot:3,roboticFrame:1,circuit:2},'Name this stop and join it to a second powered stop with Freight Rail. Buffers up to 160 cargo.'],
    ['FREIGHT_RAIL',233,'foundryRail','Freight Rail','═','#caa67e',{steel:2,ironIngot:2},'Drag in lines and join segments to connect freight stops. Ground rails are separate from vertical lift guides.']
  ];
  const port = (type, side, mode, label) => ({type,side,mode,label});
  const stockPorts = [port('item',0,'out','OUT'),port('item',2,'in','IN'),port('power',1,'in','PWR'),port('power',3,'in','PWR')];
  const cardinals = [[1,0],[-1,0],[0,1],[0,-1]];
  const slopes = [[1,1],[1,-1],[-1,1],[-1,-1]];
  const sum = stock => Object.values(stock||{}).reduce((total,n)=>total+n,0);
  const dist = (a,b) => Math.hypot(a.x-b.x,a.y-b.y);
  const same = (a,b) => !!a&&!!b&&a.x===b.x&&a.y===b.y;
  const dockCapacity = 4;
  const RAIL_TOP = 19 / 24;
  const flyingTypes = new Set(['wisp','leech','drone','stinger','watcher','scout']);
  const railIntervals = [5,15,30,60];
  const DEFAULT_BLUEPRINTS = [
    {name:'Four-tile walkway',placements:Array.from({length:4},(_,x)=>({x,y:0,key:'platform'}))},
    {name:'Four-tile ladder',placements:Array.from({length:4},(_,y)=>({x:0,y,key:'ladder'}))},
    {name:'Four-tile power spur',placements:Array.from({length:4},(_,x)=>({x,y:0,key:'wire'}))}
  ];

  function install(T,defs,items,recipes) {
    for (const [symbol,id,key,name,icon,color,cost,desc] of stationParts) {
      T[symbol]=id;
      const rail=symbol==='FREIGHT_RAIL';
      defs[id]={name,drop:key,hard:rail?.45:1.4,weight:rail?2:8,strength:rail?10:28,solid:!rail,foundryFleet:true};
      if (rail) Object.assign(defs[id],{lineBuild:true,freightRail:true});
      else Object.assign(defs[id],{kind:'power',electric:true,load:symbol==='FREIGHT_STOP'?4:8,ports:stockPorts,
        transport:true,inventoryCapacity:symbol==='FREIGHT_STOP'?160:96,retainItems:true});
      items.push({key,tile:id,name,icon,color});
      recipes.push({key,name,icon,cost,out:rail?8:1,desc,category:rail?'vertical':'fabrication',station:'workbench'});
    }
    for (const [key,role] of Object.entries(roles)) {
      const icon=role.ground?'▣':'◈';
      items.push({key,tile:-2,name:role.name,icon,color:role.color});
      recipes.push({key,name:role.name,icon,cost:{[role.ground?'roboticFrame':'droneChassis']:1,...role.cost},
        out:1,category:'fabrication',station:'workbench',desc:role.job});
    }
  }

  function create({T,defs,inventory,engine,get,set,getMeta,inside,player,items=[],stats={},tile=24,
    notify=()=>{},save=()=>{},inventoryRoom=()=>2048,lineOfSight=()=>true,getSurvival=()=>null,survey=()=>[],
    getBlueprint=()=>null,canPlace=()=>true,rigAt=()=>null}) {
    const units=[];
    const placeable=new Map(items.filter(item=>item.tile>0).map(item=>[item.key,item.tile]));
    const itemNames=new Map(items.map(item=>[item.key,item.name]));
    const stabilized=new Map(), freightPaths=new Map();
    let nextUnit=1,assigning=null,elapsed=0,stationsRevision=-1,railRevision=-1,stationPositions=[];
    const location=unit=>({x:Math.round(unit.x),y:Math.round(unit.y)});
    const stock=(point,field='contents')=>engine.data(engine.key(point.x,point.y))[field]||{};
    const point=(x,y)=>({x,y});
    const worldPlayer=()=>point((player.x+player.w/2)/tile,(player.y+player.h/2)/tile);
    const validPoint=value=>Number.isInteger(value?.x)&&Number.isInteger(value?.y)&&inside(value.x,value.y);
    const modeOf=unit=>roles[unit.key].rail?'rail':unit.key==='tunnelCrawler'?
      unit.action==='tunnel'?'crawl':'crawler':roles[unit.key].ground?'ground':'flight';
    const textName=key=>itemNames.get(key)||key;
    const movementCost=unit=>roles[unit.key].rail?.035:roles[unit.key].ground?.3:.52;
    const reserveFor=(unit,home)=>Math.max(18,dist(unit,home)*movementCost(unit)*1.35+6);

    function take(buffer,key,n) {
      const count=Math.max(0,Math.min(n,buffer[key]||0));
      if (count) buffer[key]-=count;
      if (!buffer[key]) delete buffer[key];
      return count;
    }
    function add(buffer,key,n) {
      if (n>0) buffer[key]=(buffer[key]||0)+n;
    }
    function powered(p) {
      const m=getMeta(p.x,p.y);
      return !!m?.powered&&!m.disabled&&m.signalOK!==false;
    }
    function solid(x,y) {
      if (!inside(x,y)) return true;
      const d=defs[get(x,y)];
      return !!d?.solid&&!d.platform&&!getMeta(x,y)?.open;
    }
    function diggable(x,y) {
      return [T.DIRT,T.STONE,T.GRASS,T.OBSIDIAN].includes(get(x,y))&&!getMeta(x,y);
    }
    function enterable(x,y,mode) {
      if (!inside(x,y)) return false;
      if (mode==='rail') return get(x,y)===T.FREIGHT_RAIL;
      if (mode==='crawl'&&diggable(x,y)) return true;
      if (solid(x,y)) return false;
      if ((mode==='crawler'||mode==='crawl')&&cardinals.some(([dx,dy])=>solid(x+dx,y+dy))) return true;
      return mode==='flight'||solid(x,y+1)||!!defs[get(x,y+1)]?.platform||!!defs[get(x,y)]?.ladder;
    }

    function localPoints(p,mode,center=false) {
      const positions=cardinals.map(([dx,dy])=>point(p.x+dx,p.y+dy));
      if (center) positions.unshift(p);
      return positions.filter(n=>enterable(n.x,n.y,mode));
    }
    function route(start,targets,mode,limit=10000,bound=80) {
      if (!targets.length) return null;
      const keys=new Set(targets.map(p=>engine.key(p.x,p.y)));
      const startKey=engine.key(start.x,start.y);
      if (keys.has(startKey)) return [];
      const parents=new Map([[startKey,null]]);
      const queue=[startKey], dirs=(mode==='ground'||mode==='crawl'||mode==='crawler')?[...cardinals,...slopes]:cardinals;
      for (let i=0;i<queue.length&&i<limit;i++) {
        const [x,y]=engine.xy(queue[i]);
        for (const [dx,dy] of dirs) {
          const nx=x+dx,ny=y+dy;
          if (Math.abs(nx-start.x)+Math.abs(ny-start.y)>bound||!enterable(nx,ny,mode)) continue;
          if (dx&&dy&&solid(x,ny)&&solid(nx,y)&&mode!=='crawl') continue;
          const key=engine.key(nx,ny);
          if (parents.has(key)) continue;
          parents.set(key,queue[i]);
          if (keys.has(key)) {
            const path=[];
            for (let last=key;last!==startKey;last=parents.get(last)) path.push(point(...engine.xy(last)));
            return path.reverse();
          }
          queue.push(key);
        }
      }
      return null;
    }
    function getHome(unit) {
      const p=unit.home;
      if (get(p.x,p.y)===T.FOUNDRY_FLEET_DOCK&&(!p.node||getMeta(p.x,p.y)?.nodeId===p.node)) return p;
      if (!p.node) return null;
      for (const k of engine.nodes) {
        const [x,y]=engine.xy(k);
        if (get(x,y)===T.FOUNDRY_FLEET_DOCK&&getMeta(x,y)?.nodeId===p.node) {
          unit.home={x,y,node:p.node};
          return unit.home;
        }
      }
      return null;
    }
    function stations() {
      if (stationsRevision!==engine.revision) {
        stationPositions=Array.from(engine.nodes,k=>point(...engine.xy(k))).filter(p=>get(p.x,p.y)===T.FREIGHT_STOP);
        stationsRevision=engine.revision;
      }
      return stationPositions.map(p=>({...p,name:getMeta(p.x,p.y)?.freightName||`Freight Stop ${p.x}, ${p.y}`,powered:powered(p)}));
    }
    function routeBetween(from,to) {
      if (!validPoint(from)||!validPoint(to)||same(from,to)||get(from.x,from.y)!==T.FREIGHT_STOP||
          get(to.x,to.y)!==T.FREIGHT_STOP||!powered(from)||!powered(to)) return null;
      if (railRevision!==engine.revision||freightPaths.size>=64) {
        freightPaths.clear();
        railRevision=engine.revision;
      }
      const cacheKey=engine.key(from.x,from.y)+'>'+engine.key(to.x,to.y);
      if (freightPaths.has(cacheKey)) return freightPaths.get(cacheKey);
      const ends=localPoints(to,'rail');
      for (const start of localPoints(from,'rail')) {
        const path=route(start,ends,'rail',40000,2800);
        if (path) {
          const connected=[start,...path];
          freightPaths.set(cacheKey,connected);
          return connected;
        }
      }
      freightPaths.set(cacheKey,null);
      return null;
    }

    function wait(unit,reason) {
      unit.phase='Waiting';
      unit.reason=reason;
      unit.path=[];
      unit.action=null;
      unit.destination=null;
      unit.next=elapsed+(unit.priority==='urgent'?.4:1.25);
    }
    function finish(unit,reason='') {
      unit.path=[];
      unit.action=null;
      unit.destination=null;
      unit.phase='Ready';
      unit.reason=reason;
      unit.next=elapsed+.12;
    }
    function travel(unit,home,where,action,phase,{center=false}={}) {
      const d=roles[unit.key];
      const relay=units.some(other=>other.key==='relayDrone'&&other.action==='guard'&&other.phase==='Relaying'&&same(home,other.home)&&dist(other,where)<36);
      if (action!=='dock'&&dist(home,where)>d.range+(relay?24:0)) {
        wait(unit,'Target outside command range');
        return false;
      }
      const from=location(unit);
      unit.action=action;
      const mode=modeOf(unit);
      const targets=localPoints(where,mode,center);
      if (center&&enterable(where.x,where.y,mode)) targets.splice(1);
      const path=route(from,targets,mode,mode==='rail'?40000:9000,mode==='rail'?1500:100);
      if (!path) {
        wait(unit,action==='dock'?'No path to dock':mode==='rail'?'No connected freight rail':'Path blocked');
        return false;
      }
      unit.phase=phase;
      unit.reason='';
      unit.destination={...where};
      unit.path=path;
      return true;
    }

    function load(unit,source,keys=Object.keys(stock(source))) {
      const contents=stock(source), space=Math.max(0,roles[unit.key].carry-sum(unit.cargo));
      let n=0;
      for (const key of keys) {
        if (!itemNames.has(key)) continue;
        const used=take(contents,key,space-n);
        add(unit.cargo,key,used);
        n+=used;
        if (n>=space) break;
      }
      return n;
    }
    function unload(unit,destination,keys=Object.keys(unit.cargo)) {
      const k=engine.key(destination.x,destination.y);
      let n=0;
      for (const key of keys) {
        const inserted=engine.insert(k,key,unit.cargo[key]);
        take(unit.cargo,key,inserted);
        n+=inserted;
      }
      return n;
    }
    function needsRepair(p) {
      const m=getMeta(p.x,p.y), d=defs[get(p.x,p.y)];
      return !!m&&!!d&&!d.rareOre&&(m.disabled&&m.ruin||Number.isFinite(m.hp)&&m.hp<(m.maxHp||m.hp));
    }
    function repair(unit,p) {
      if (!needsRepair(p)||!take(unit.cargo,'repairKit',1)) return false;
      const m=getMeta(p.x,p.y);
      if (m.ruin&&m.disabled) { m.ruin=false;m.disabled=false; }
      if (Number.isFinite(m.hp)) m.hp=Math.min(m.maxHp||m.hp,m.hp+60);
      engine.revision++;
      return true;
    }
    function repairTarget(p) {
      let target=null, distance=8.01;
      for (const k of engine.nodes) {
        const candidate=point(...engine.xy(k)), d=dist(candidate,p);
        if (d<distance&&needsRepair(candidate)) { target=candidate;distance=d; }
      }
      return target;
    }
    function canUseBin(p) {
      return p&&defs[get(p.x,p.y)]?.transport&&!defs[get(p.x,p.y)]?.producer;
    }
    function ship(unit,home) {
      const source=unit.pickup, target=unit.target;
      if (!canUseBin(source)||!canUseBin(target)||same(source,target)) return wait(unit,'Set two different cargo depots');
      if (unit.key==='railHauler'&&!routeBetween(source,target)) return wait(unit,'Connect both powered stops with Freight Rail');
      if (sum(unit.cargo)) return travel(unit,home,target,'unload','Delivering cargo');
      if (unit.key==='railHauler'&&unit.dispatchAt>elapsed)
        return wait(unit,'Next departure in '+Math.ceil(unit.dispatchAt-elapsed)+'s');
      if (!sum(stock(source))) return wait(unit,'Pickup depot is empty');
      return travel(unit,home,source,'load','Heading to pickup');
    }
    function resupply(unit,home,key) {
      const source=unit.pickup||home;
      if (!canUseBin(source)) return wait(unit,'Reassign missing supply bin');
      if (!stock(source)[key]) return wait(unit,'Out of '+textName(key).toLowerCase());
      return travel(unit,home,source,'supply','Heading to resupply');
    }
    function plan(unit,home,dt) {
      if (elapsed<unit.next) return;
      const p=location(unit), mode=modeOf(unit);
      const docked=localPoints(home,mode).some(n=>same(n,p));
      if (unit.recalled||!powered(home)||unit.charge<reserveFor(unit,home)||docked&&unit.charge<80) {
        if (!docked) return travel(unit,home,home,'dock','Returning to dock');
        unit.phase=powered(home)?'Docked · charging':'Docked · needs power';
        unit.reason=unit.recalled?'Recalled':'';
        if (powered(home)) unit.charge=Math.min(100,unit.charge+dt*42);
        return;
      }
      if (unit.done) {
        if (!docked) return travel(unit,home,home,'dock','Returning to dock');
        return wait(unit,unit.result||'Assignment complete');
      }
      if (!unit.target&&!(unit.key==='shieldDrone'&&unit.follow)) return wait(unit,'Mark a work target');
      const target=unit.target;
      const supply=unit.pickup||home;
      switch (unit.key) {
        case 'freightDrone':case 'railHauler': return ship(unit,home);
        case 'rigTender': {
          const def=defs[get(target.x,target.y)];
          if (!def?.foundryRig) return wait(unit,'Mark an extraction rig');
          if (unit.cargo.emptyCoolantCanister) return travel(unit,home,supply,'unload','Returning empty canisters');
          const rigStock=stock(target);
          const needsCoolant=def.accepts?.includes('coolantCanister')||def.foundryRig.coolant;
          const needsKit=needsRepair(target);
          if (stock(target,'outputs').emptyCoolantCanister||needsKit&&unit.cargo.repairKit||
              needsCoolant&&unit.cargo.coolantCanister&&(rigStock.coolantCanister||0)<2)
            return travel(unit,home,target,'tend','Servicing extraction rig');
          if (needsKit) return resupply(unit,home,'repairKit');
          if (needsCoolant&&(rigStock.coolantCanister||0)<2) return resupply(unit,home,'coolantCanister');
          return wait(unit,'Rig supplied');
        }
        case 'repairDrone': {
          const candidate=repairTarget(target);
          if (!candidate) return wait(unit,'No damaged machine in zone');
          if (!unit.cargo.repairKit) return resupply(unit,home,'repairKit');
          return travel(unit,home,candidate,'repair','Heading to damaged machine');
        }
        case 'surveyDrone': return travel(unit,home,target,'survey','Surveying mapped area',{center:true});
        case 'constructionDrone':
          if (!unit.blueprint) return wait(unit,'Approve a blueprint');
          return travel(unit,home,target,'build','Heading to build site',{center:true});
        case 'shieldDrone': {
          const at=worldPlayer(), guard=unit.follow?point(Math.floor(at.x),Math.floor(at.y)):target;
          return travel(unit,home,guard,'guard','Heading to shielding position',{center:true});
        }
        case 'interceptorDrone':case 'bastionWalker':
          if (!unit.cargo.ammo) return resupply(unit,home,'ammo');
          return travel(unit,home,target,'guard','Heading to defense point',{center:true});
        case 'stabilizerWalker':
          if (!defs[get(target.x,target.y)]?.foundryRig) return wait(unit,'Mark an extraction rig');
          return travel(unit,home,target,'guard','Heading to stabilize rig');
        case 'relayDrone': return travel(unit,home,target,'guard','Heading to relay point',{center:true});
        case 'tunnelCrawler': return travel(unit,home,target,'tunnel','Clearing tunnel corridor',{center:true});
      }
    }

    function blueprintCopy(plan) {
      const cells=plan?.placements;
      if (!Array.isArray(cells)||!cells.length||cells.length>40) return null;
      const placements=[];
      for (const p of cells) {
        if (!Number.isInteger(p.x)||!Number.isInteger(p.y)||Math.abs(p.x)>12||Math.abs(p.y)>12||!placeable.has(p.key)) return null;
        placements.push({x:p.x,y:p.y,key:p.key,rot:Number.isInteger(p.rot)?((p.rot%4+4)%4):0});
      }
      const cost={};
      for(const p of placements) add(cost,p.key,1);
      for(const [key,n] of Object.entries(plan.cost||{})) if(itemNames.has(key)&&Number.isFinite(n)&&n>0) cost[key]=Math.max(cost[key]||0,Math.ceil(n));
      const links=(Array.isArray(plan.links)?plan.links:[]).filter(group=>Array.isArray(group)&&group.length>1&&
        group.every(pos=>Array.isArray(pos)&&placements.some(p=>p.x===pos[0]&&p.y===pos[1])));
      return {name:String(plan.name||'Approved blueprint').slice(0,60),placements,cost,links};
    }
    function build(unit,home,target) {
      const {placements,cost,links}=unit.blueprint;
      const supplied=stock(home), cells=placements.map(p=>({...p,x:p.x+target.x,y:p.y+target.y,id:placeable.get(p.key)}));
      if (cells.some(p=>!inside(p.x,p.y)||get(p.x,p.y)!==T.AIR||!canPlace(p.x,p.y,p.id)))
        return wait(unit,'Blueprint obstructed');
      const missing=Object.keys(cost).find(key=>(supplied[key]||0)<cost[key]);
      if (missing) return wait(unit,'Out of '+textName(missing).toLowerCase());
      for (const [key,n] of Object.entries(cost)) take(supplied,key,n);
      for (const p of cells) set(p.x,p.y,p.id,{rot:p.rot,placedItem:p.key});
      for (const group of links) engine.link(group.map(([x,y])=>engine.key(target.x+x,target.y+y)));
      stats.built=(stats.built||0)+cells.length;
      unit.done=true;unit.result=unit.blueprint.name+' built';
      notify(unit.result);
      finish(unit);
    }
    function finishJob(unit,home) {
      const p=unit.destination;
      unit.cooldown=elapsed+.37;
      switch (unit.action) {
        case 'dock': return finish(unit);
        case 'load':
          if (!canUseBin(p)) return wait(unit,'Pickup depot missing');
          if (!load(unit,p)) return wait(unit,'Pickup depot is empty');
          break;
        case 'unload':
          if (!canUseBin(p)) return wait(unit,'Delivery depot missing');
          unload(unit,p,unit.key==='rigTender'?['emptyCoolantCanister']:undefined);
          if (unit.key==='rigTender'?unit.cargo.emptyCoolantCanister:sum(unit.cargo)) return wait(unit,'Depot full or filtered');
          if (unit.key==='railHauler') unit.dispatchAt=elapsed+(unit.dispatchInterval||railIntervals[0]);
          break;
        case 'supply': {
          if (!canUseBin(p)) return wait(unit,'Supply bin missing');
          if (unit.key==='rigTender') {
            const before=sum(unit.cargo);
            const contents=stock(p);
            for (const key of ['coolantCanister','repairKit']) add(unit.cargo,key,take(contents,key,Math.max(0,3-(unit.cargo[key]||0))));
            if (sum(unit.cargo)===before) return wait(unit,'Supply bin is empty');
          } else if (!load(unit,p,[unit.key==='repairDrone'?'repairKit':'ammo'])) return wait(unit,'Supply bin is empty');
          break;
        }
        case 'tend': {
          if (!defs[get(p.x,p.y)]?.foundryRig) return wait(unit,'Assigned rig was removed');
          const amount=Math.max(0,2-(stock(p).coolantCanister||0));
          const sent=engine.insert(engine.key(p.x,p.y),'coolantCanister',Math.min(amount,unit.cargo.coolantCanister||0));
          take(unit.cargo,'coolantCanister',sent);
          repair(unit,p);
          const space=roles[unit.key].carry-sum(unit.cargo);
          add(unit.cargo,'emptyCoolantCanister',take(stock(p,'outputs'),'emptyCoolantCanister',space));
          if (stock(p,'outputs').emptyCoolantCanister&&space===0) return wait(unit,'Cargo full · unload supplies');
          break;
        }
        case 'repair': repair(unit,p);break;
        case 'survey': {
          const found=survey(p.x,p.y,14)||[];
          unit.done=true;unit.result=found.length?found.length+' deposit areas pinned':'No new deposits found in range';
          notify(unit.result);
          break;
        }
        case 'build': return build(unit,home,p);
        case 'tunnel': unit.done=true;unit.result='Corridor cleared';break;
      }
      finish(unit);
    }

    function screenEnemies(p,radius) {
      const s=getSurvival();
      return (s?.enemies||[]).filter(e=>e.hp>0&&Math.hypot(e.x/tile-p.x,e.y/tile-p.y)<=radius);
    }
    function serviceGuard(unit,home,dt) {
      const p=unit.destination;
      const s=getSurvival(), origin=point(unit.x+.5,unit.y+.5);
      const playerPoint=worldPlayer();
      switch (unit.key) {
        case 'relayDrone': unit.phase='Relaying';unit.charge-=dt*.5;return;
        case 'stabilizerWalker': {
          const m=getMeta(p.x,p.y);
          if (!m||!defs[get(p.x,p.y)]?.foundryRig) return wait(unit,'Assigned rig was removed');
          m.foundryStabilized=true;stabilized.set(unit.id,m);
          unit.phase='Anchored · stabilizing';unit.charge-=dt*.38;return;
        }
        case 'shieldDrone': {
          const target=unit.follow?playerPoint:point(p.x+.5,p.y+.5);
          if (dist(origin,target)>2.4) return finish(unit);
          const settings=unit.shieldDir||1;
          unit.phase='Shield projected';unit.charge-=dt*.55;
          if (elapsed<unit.cooldown) return;
          const shot=s?.projectiles?.find(shot=>{
            if (shot.owner!=='enemy'||shot.life<=0) return false;
            const relative=point(shot.x/tile-target.x,shot.y/tile-target.y);
            const direction=settings===1?1:settings===-1?-1:player.facing||1;
            return Math.abs(relative.y)<2.1&&relative.x*direction>0&&relative.x*direction<2.6&&shot.vx*direction<0;
          });
          if (shot) {
            shot.life=0;unit.charge-=3;unit.cooldown=elapsed+.12;unit.flash=elapsed+.2;
          }
          return;
        }
        default: {
          const interceptor=unit.key==='interceptorDrone';
          if (!unit.cargo.ammo) return finish(unit);
          unit.phase=interceptor?'Intercepting':'Holding defense point';unit.charge-=dt*(interceptor?.3:.12);
          if (elapsed<unit.cooldown) return;
          const radius=interceptor?7:8;
          const shot=interceptor&&s?.projectiles?.find(shot=>shot.owner==='enemy'&&shot.life>0&&
            Math.hypot(shot.x/tile-origin.x,shot.y/tile-origin.y)<radius&&
            lineOfSight(origin.x,origin.y,shot.x/tile,shot.y/tile));
          const enemy=!shot&&screenEnemies(origin,radius).filter(e=>(!interceptor||flyingTypes.has(e.type))&&
            lineOfSight(origin.x,origin.y,e.x/tile,e.y/tile)).sort((a,b)=>
              Math.hypot(a.x/tile-origin.x,a.y/tile-origin.y)-Math.hypot(b.x/tile-origin.x,b.y/tile-origin.y))[0];
          if (shot||enemy) {
            take(unit.cargo,'ammo',1);
            if (shot) { unit.beam=point(shot.x/tile,shot.y/tile);shot.life=0; }
            else { unit.beam=point(enemy.x/tile,enemy.y/tile);s.hit(enemy,interceptor?18:23,'defense'); }
            unit.cooldown=elapsed+(interceptor?.38:.7);unit.flash=elapsed+.17;
          } else if (!interceptor&&unit.patrol&&elapsed>(unit.patrolNext||0)) {
            unit.patrolNext=elapsed+7;
            const atDock=same(p,home);
            travel(unit,home,atDock?unit.target:home,'guard','Patrolling defense route',{center:atDock});
          }
        }
      }
    }
    function move(unit,dt) {
      const role=roles[unit.key], mode=modeOf(unit);
      let budget=role.speed*dt;
      while (budget>0&&unit.path.length) {
        const p=unit.path[0];
        if (!enterable(p.x,p.y,mode)) { wait(unit,'Path blocked');return; }
        if (mode==='crawl'&&diggable(p.x,p.y)) {
          unit.phase='Cutting tunnel';
          unit.cut=(unit.cut||0)+dt;
          if (unit.cut<.34) return;
          const id=get(p.x,p.y), key=defs[id]?.drop;
          if (key&&!engine.spawn(key,1,p.x+.5,p.y+.5)) { wait(unit,'Clear loose materials to resume');return; }
          set(p.x,p.y,T.AIR);
          unit.cut=0;
        }
        const d=dist(unit,p);
        const take=Math.min(d,budget);
        if (d>0) {
          if (Math.abs(p.x-unit.x)>.02) unit.facing=Math.sign(p.x-unit.x);
          unit.x+=(p.x-unit.x)*take/d;unit.y+=(p.y-unit.y)*take/d;
        }
        if (d<=budget) { unit.x=p.x;unit.y=p.y;unit.path.shift(); }
        budget-=take;
        unit.charge=Math.max(0,unit.charge-take*movementCost(unit));
      }
    }
    function releaseAnchor(unit) {
      const machine=stabilized.get(unit.id);
      if (!machine) return;
      stabilized.delete(unit.id);
      if (![...stabilized.values()].includes(machine)) machine.foundryStabilized=false;
    }
    function tick(dt) {
      elapsed+=dt;
      const nearby=worldPlayer();
      for (const unit of units) {
        const step=Math.min(.3,(unit.step||0)+dt);
        if (dist(unit,nearby)>72&&step<.25) { unit.step=step;continue; }
        unit.step=0;
        releaseAnchor(unit);
        const home=getHome(unit);
        if (!home) { wait(unit,'Dock lost · claim at a Fleet Dock');continue; }
        if (unit.charge<=0&&!localPoints(home,modeOf(unit)).some(p=>same(p,location(unit)))) {
          wait(unit,'Battery empty · recover at unit');continue;
        }
        // Recall and power loss take precedence over work but keep cargo aboard.
        if (unit.action&&unit.action!=='dock'&&(unit.recalled||unit.charge<reserveFor(unit,home)||!powered(home))) finish(unit);
        if (!unit.action) plan(unit,home,step);
        if (!unit.action) continue;
        if (unit.path.length) {
          move(unit,step);
          continue;
        }
        if (unit.action==='guard') serviceGuard(unit,home,step);
        else if (elapsed>=unit.cooldown) finishJob(unit,home);
      }
    }

    function roster(x,y) {
      return units.filter(unit=>same(getHome(unit),point(x,y)));
    }
    function deployAt(x,y,key) {
      const home=point(x,y),role=roles[key];
      if (!role||get(x,y)!==T.FOUNDRY_FLEET_DOCK) return;
      if (!powered(home)) return notify('Power the Expedition Fleet Dock');
      if (roster(x,y).length>=dockCapacity) return notify('Fleet dock is full');
      if (!(inventory[key]>0)) return notify('Craft a '+role.name+' at a Workbench');
      const mode=modeOf({key});
      const spawn=localPoints(home,mode)[0];
      if (!spawn) return notify(role.rail?'Attach freight rail beside the dock':'Clear a route beside the dock');
      take(inventory,key,1);
      units.push({id:nextUnit++,key,home:{x,y,node:getMeta(x,y)?.nodeId},...spawn,charge:100,
        cargo:{},path:[],phase:'Ready',next:0,cooldown:0,facing:1});
      save();
    }
    function recover(unit,home) {
      const distance=dist(location(unit),home), playerDistance=dist(unit,worldPlayer());
      if (distance>2.1&&playerDistance>3) {
        notify('Recall the unit or move within 3 tiles of it');
        return;
      }
      if (inventoryRoom()<sum(unit.cargo)+1) return notify('Make backpack space for the unit and its cargo');
      add(inventory,unit.key,1);
      for (const [key,n] of Object.entries(unit.cargo)) add(inventory,key,n);
      releaseAnchor(unit);
      units.splice(units.indexOf(unit),1);
      save();
    }
    function mark(unit,kind,closeInspector) {
      assigning={id:unit.id,kind};
      closeInspector();
      notify(kind==='pickup'?'Aim at a supply or pickup depot and use Interact':'Aim at the work target and use Interact');
    }
    function selectTarget(x,y) {
      if (!assigning) return false;
      const {id,kind}=assigning;
      const unit=units.find(other=>other.id===id);
      let p=point(x,y);
      if (!unit) { assigning=null;return true; }
      if (!validPoint(p)) { notify('Aim inside the world');return true; }
      const home=getHome(unit);
      if (!home) { assigning=null;notify('Dock lost · claim at a Fleet Dock');return true; }
      const d=roles[unit.key], def=defs[get(x,y)];
      if (dist(home,p)>d.range+24) { notify('Target outside command range');return true; }
      if (d.rail&&get(x,y)!==T.FREIGHT_STOP) { notify('Mark a Freight Stop');return true; }
      if (kind==='pickup'&&!canUseBin(p)) { notify('Mark a cargo or supply bin');return true; }
      if (kind==='target') {
        if (unit.key==='freightDrone'&&!canUseBin(p)) { notify('Mark a cargo bin');return true; }
        if (['rigTender','stabilizerWalker'].includes(unit.key)&&!def?.foundryRig) {
          const rig=rigAt(x,y);
          if (!rig) { notify('Mark an extraction rig');return true; }
          p=point(rig.x,rig.y);
        }
        unit.follow=false;
      }
      assigning=null;
      unit[kind]=p;
      unit.done=false;unit.recalled=false;
      finish(unit);
      save();
      notify(kind==='pickup'?'Pickup assigned':'Work target assigned');
      return true;
    }
    function cancelTarget() {
      if (!assigning) return false;
      assigning=null;notify('Target assignment canceled');
      return true;
    }

    function describe(x,y) {
      const p=point(x,y), id=get(x,y);
      if (id===T.FREIGHT_STOP) {
        const name=getMeta(x,y)?.freightName||`Freight Stop ${x}, ${y}`;
        return {status:powered(p)?'Freight stop online':'Needs 4 power',detail:'Connect this stop to another powered stop with Freight Rail.',name};
      }
      if (id!==T.FOUNDRY_FLEET_DOCK) return null;
      const list=roster(x,y);
      return {status:powered(p)?'Dock online':'Needs 8 power',
        detail:`${list.length} / ${dockCapacity} units · Assign a target and stock supplies below.`,
        deployedRows:list.map(unit=>({name:roles[unit.key].name,phase:unit.phase,charge:unit.charge,cargo:unit.cargo,reason:unit.reason}))};
    }

    function appendControls(container,x,y,{apply,closeInspector,refresh}) {
      const p=point(x,y), id=get(x,y), doc=container.ownerDocument;
      if (id!==T.FREIGHT_STOP&&id!==T.FOUNDRY_FLEET_DOCK) return;
      const make=(tag,text)=>{const node=doc.createElement(tag);if(text)node.textContent=text;return node;};
      const button=(parent,label,run)=>{
        const b=make('button',label);
        b.type='button';b.className='icon-btn';b.onclick=run;parent.append(b);
        return b;
      };
      const update=fn=>apply(()=>{fn();refresh();});
      const select=(label,options)=>{
        const el=make('select');
        el.setAttribute('aria-label',label);
        for (const [key,name] of options) { const option=make('option',name);option.value=key;el.append(option); }
        return el;
      };
      const panel=make('section');panel.className='foundry-fleet-controls';
      if (id===T.FREIGHT_STOP) {
        const name=make('input');name.type='text';name.maxLength=32;name.placeholder='Freight stop name';
        name.value=getMeta(x,y)?.freightName||'';
        name.setAttribute('aria-label','Freight stop name');
        panel.append(name);
        const rename=update(()=>{engine.data(engine.key(x,y)).freightName=name.value.trim().slice(0,32);save();});
        button(panel,'Name stop',rename);
        name.onkeydown=event=>{if(event.key==='Enter'){event.preventDefault();rename();}};
        container.append(panel);
        return;
      }
      const deploy=select('Fleet unit to deploy',Object.entries(roles).filter(([key])=>inventory[key]>0).map(([key,r])=>[key,r.name]));
      if (deploy.options.length) { panel.append(deploy);button(panel,'Deploy selected unit',update(()=>deployAt(x,y,deploy.value))); }
      else panel.append(make('p','Craft a fleet unit at a Workbench to deploy it here.'));
      const assigned=roster(x,y);
      const supplies=new Set(['ammo','coolantCanister','repairKit']);
      for (const unit of assigned)
        for (const key of Object.keys(unit.blueprint?.cost||{})) supplies.add(key);
      const suppliesMenu=select('Supplies to load',Array.from(supplies).filter(key=>inventory[key]>0)
        .map(key=>[key,textName(key)]));
      if (suppliesMenu.options.length) {
        const supplyControls=make('div');supplyControls.append(suppliesMenu);
        button(supplyControls,'Stock dock',update(()=>{
          const key=suppliesMenu.value,available=inventory[key]||0;
          const delivered=engine.insert(engine.key(x,y),key,available);
          take(inventory,key,delivered);
          notify(delivered?delivered+' '+textName(key)+' delivered':'Dock inventory full');
          if (delivered) save();
        }));
        panel.append(supplyControls);
      }
      for (const unit of assigned) {
        const entry=make('details');entry.open=!!unit.controlsOpen;
        entry.ontoggle=()=>{unit.controlsOpen=entry.open;};
        const role=roles[unit.key];
        entry.append(make('summary',role.name),make('p',role.job),
          make('p',role.range+'-tile work range'+(role.carry?' · '+role.carry+' cargo capacity':'')));
        const coords=where=>where?(getMeta(where.x,where.y)?.freightName||`${where.x}, ${where.y}`):'Unset';
        const supply=unit.pickup?coords(unit.pickup):'This dock';
        entry.append(make('p','Target · '+(unit.follow?'Follow player':coords(unit.target))+
          (roles[unit.key].carry?' · Pickup · '+supply:'')));
        button(entry,'Mark target',()=>mark(unit,'target',closeInspector));
        if (roles[unit.key].carry) button(entry,'Mark pickup',()=>mark(unit,'pickup',closeInspector));
        if (unit.key==='shieldDrone') {
          button(entry,'Follow player',update(()=>{unit.follow=true;unit.recalled=false;finish(unit);save();}));
          button(entry,unit.shieldDir===-1?'Shield left':'Shield right',update(()=>{unit.shieldDir=unit.shieldDir===-1?1:-1;save();}));
        }
        if (unit.key==='bastionWalker') button(entry,unit.patrol?'Switch to hold':'Switch to patrol',update(()=>{unit.patrol=!unit.patrol;finish(unit);save();}));
        if (unit.key==='railHauler') {
          const cadence=select('Rail dispatch interval',railIntervals.map(n=>[n,'Dispatch every '+n+'s']));
          cadence.value=String(unit.dispatchInterval||railIntervals[0]);
          cadence.onchange=update(()=>{
            unit.dispatchInterval=Number(cadence.value);
            unit.dispatchAt=0;
            save();
          });
          entry.append(cadence);
        }
        if (unit.key==='constructionDrone') {
          const selected=blueprintCopy(getBlueprint());
          const plans=[...DEFAULT_BLUEPRINTS,...(selected?[selected]:[])];
          const planSelect=select('Construction blueprint',plans.map((value,index)=>[index,value.name]));
          entry.append(planSelect);
          button(entry,unit.blueprint?'Approved · '+unit.blueprint.name:'Approve blueprint',update(()=>{
            unit.blueprint=blueprintCopy(plans[Number(planSelect.value)]);unit.done=false;finish(unit);save();
          }));
        }
        if (unit.key==='tunnelCrawler'&&unit.target) {
          button(entry,'Clear route again',update(()=>{unit.done=false;finish(unit);save();}));
        }
        button(entry,unit.priority==='urgent'?'Priority · high':'Priority · standard',update(()=>{unit.priority=unit.priority==='urgent'?'normal':'urgent';save();}));
        button(entry,unit.recalled?'Resume unit':'Recall unit',update(()=>{unit.recalled=!unit.recalled;finish(unit);save();}));
        button(entry,'Pack unit and cargo',update(()=>recover(unit,p)));
        panel.append(entry);
      }
      for (const unit of units) {
        if (getHome(unit)||dist(unit,p)>12) continue;
        button(panel,'Claim '+roles[unit.key].name,update(()=>{
          if (roster(x,y).length>=dockCapacity) return notify('Fleet dock is full');
          unit.home={x,y,node:getMeta(x,y)?.nodeId};unit.recalled=true;finish(unit);save();
        }));
      }
      container.append(panel);
    }
    function controlsKey(x,y) {
      if (get(x,y)===T.FREIGHT_STOP) return getMeta(x,y)?.freightName||'';
      if (get(x,y)!==T.FOUNDRY_FLEET_DOCK) return '';
      const available=key=>inventory[key]>0;
      return JSON.stringify([Object.keys(roles).filter(available),['ammo','coolantCanister','repairKit'].filter(available),roster(x,y).map(unit=>
        [unit.id,unit.target,unit.pickup,unit.priority,unit.recalled,unit.blueprint?.name,unit.follow,unit.patrol,unit.shieldDir,unit.dispatchInterval]),
        units.filter(unit=>!getHome(unit)&&dist(unit,point(x,y))<=12).map(unit=>unit.id)]);
    }
    function canDismantle(x,y) {
      if (get(x,y)===T.FOUNDRY_FLEET_DOCK&&roster(x,y).length) return 'Pack assigned fleet units before dismantling this dock.';
      return null;
    }
    function recoverNearby(x,y) {
      if (dist(point(x,y),worldPlayer())>3) return false;
      const unit=units.find(u=>dist(u,point(x,y))<.9&&(u.charge<=0||!getHome(u)));
      if (!unit) return false;
      recover(unit,getHome(unit)||point(x,y));return true;
    }

    function draw(ctx,camera,w,h,size,time,painted) {
      for (const unit of units) {
        const role=roles[unit.key], x=(unit.x+.5)*size-camera.x;
        const surface=role.rail?RAIL_TOP:role.ground?1:.5;
        const contactY=(unit.y+surface)*size-camera.y, spriteSize=size*(role.ground?1.65:1.45);
        if (x<-40||x>w+40||contactY<-40||contactY>h+40) continue;
        const moving=unit.path.length>0;
        const hover=role.ground?0:Math.sin(time*(moving?12:6)+unit.id)*(moving?1.7:2.6);
        const y=role.ground?contactY-spriteSize/2:contactY+hover;
        ctx.save();
        if (!painted?.(ctx,unit.key,x,y,spriteSize,unit.facing,unit.phase,!!role.ground)) {
          const baseY=role.ground?contactY-8:y-4;
          ctx.fillStyle=role.color;ctx.globalAlpha=.9;
          ctx.fillRect(x-6,baseY,12,8);
          ctx.fillStyle='#20343e';ctx.fillRect(x-4,baseY+2,8,4);
          ctx.globalAlpha=1;
        }
        if (unit.phase==='Shield projected') {
          const direction=unit.shieldDir||1;
          ctx.strokeStyle=role.color;ctx.globalAlpha=.63+.28*Math.sin(time*8);
          ctx.lineWidth=1.5;ctx.beginPath();ctx.ellipse(x+direction*14,y,4+(unit.flash>elapsed?2:0),13,0,0,Math.PI*2);ctx.stroke();
        }
        if (unit.phase==='Relaying') {
          ctx.strokeStyle=role.color;ctx.globalAlpha=.55;
          const radius=9+((time*15+unit.id)%15);
          ctx.beginPath();ctx.arc(x,y,radius,-Math.PI*.83,-Math.PI*.17);ctx.stroke();
        }
        if (unit.beam&&unit.flash>elapsed) {
          ctx.strokeStyle=role.color;ctx.globalAlpha=.85;ctx.lineWidth=2;
          ctx.beginPath();ctx.moveTo(x,y);ctx.lineTo(unit.beam.x*size-camera.x,unit.beam.y*size-camera.y);ctx.stroke();
        }
        if (unit.phase==='Waiting'&&unit.reason&&!unit.done) {
          ctx.globalAlpha=.9;ctx.fillStyle='#f4bd78';ctx.font='bold 11px system-ui';ctx.textAlign='center';ctx.fillText('!',x,y-13);
        }
        ctx.restore();
      }
    }
    function reset() {
      units.length=0;nextUnit=1;assigning=null;elapsed=0;stationsRevision=-1;railRevision=-1;
      freightPaths.clear();
      for (const m of stabilized.values()) m.foundryStabilized=false;
      for (const k of engine.nodes) {
        const m=getMeta(...engine.xy(k));
        if (m?.foundryStabilized) m.foundryStabilized=false;
      }
      stabilized.clear();
    }
    function restore(data) {
      reset();
      const ids=new Set();
      for (const row of Array.isArray(data?.units)?data.units:[]) {
        if (!Object.hasOwn(roles,row?.key)||!validPoint(row.home)||!Number.isFinite(row.x)||!Number.isFinite(row.y)||
            !validPoint(point(Math.round(row.x),Math.round(row.y)))||!Number.isInteger(row.id)||row.id<=0||ids.has(row.id)) continue;
        ids.add(row.id);nextUnit=Math.max(nextUnit,row.id+1);
        const unit={id:row.id,key:row.key,home:{x:row.home.x,y:row.home.y,node:row.home.node},
          x:row.x,y:row.y,charge:Math.max(0,Math.min(100,Number.isFinite(row.charge)?row.charge:100)),cargo:{},
          priority:row.priority==='urgent'?'urgent':'normal',done:!!row.done,result:String(row.result||'').slice(0,110),
          recalled:!!row.recalled,target:validPoint(row.target)?row.target:null,pickup:validPoint(row.pickup)?row.pickup:null,
          patrol:!!row.patrol,follow:!!row.follow,shieldDir:row.shieldDir===-1?-1:1,blueprint:blueprintCopy(row.blueprint),
          dispatchInterval:railIntervals.includes(row.dispatchInterval)?row.dispatchInterval:railIntervals[0],
          dispatchAt:Math.max(0,Math.min(60,Number(row.dispatchIn)||0)),
          path:[],phase:'Ready',next:0,cooldown:0,facing:row.facing===-1?-1:1};
        for (const [key,n] of Object.entries(row.cargo||{})) if (itemNames.has(key)&&Number.isFinite(n)&&n>0)
          add(unit.cargo,key,Math.min(Math.floor(n),roles[row.key].carry-sum(unit.cargo)));
        units.push(unit);
      }
    }
    function exported() {
      return {units:units.map(unit=>{
        const {path,phase,next,cooldown,step,beam,flash,action,destination,controlsOpen,dispatchAt,...state}=unit;
        return {...state,dispatchIn:Math.max(0,(dispatchAt||0)-elapsed),cargo:{...state.cargo}};
      })};
    }
    return {tick,draw,describe,appendControls,controlsKey,canDismantle,recoverNearby,selectTarget,cancelTarget,
      reset,restore,export:exported,stations,routeBetween};
  }
  return {install,create,roles,RAIL_TOP};
});
