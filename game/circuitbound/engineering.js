/* Circuitbound engineering primitives. No DOM; shared by the game and Node tests. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.CircuitEngineering = factory();
})(typeof globalThis === 'object' ? globalThis : this, function () {
  'use strict';
  const DIRS = [[1, 0], [0, 1], [-1, 0], [0, -1]];
  const COLORS = {power:'#ffe365', signal:'#ff7789', mechanical:'#63e9de', item:'#a9e878', fluid:'#69adff', structure:'#d3a4ff'};
  const DELAYS = [.25, .5, 1, 2, 5, 10];
  const CATALOG = [
    ['AND','and','AND Gate','&','signal','A AND B', {copper:1,crystal:1}],
    ['OR','or','OR Gate','≥1','signal','A OR B', {copper:1,crystal:1}],
    ['NOT','not','NOT Gate','!','signal','Invert rear input', {copper:1,crystal:1}],
    ['XOR','xor','XOR Gate','⊕','signal','Exactly one input', {copper:1,crystal:1}],
    ['NAND','nand','NAND Gate','!&','signal','Invert AND', {copper:1,crystal:1}],
    ['NOR','nor','NOR Gate','!≥','signal','Invert OR', {copper:1,crystal:1}],
    ['TIMER','timer','Timer','⌛','signal','Hold input for selected delay', {copper:1,crystal:2}],
    ['PULSE','pulse','Pulse Generator','∿','signal','Repeating pulse; optional rear enable', {copper:1,crystal:2}],
    ['TOGGLE','toggle','Toggle','T','signal','Rising edge flips memory', {copper:1,crystal:2}],
    ['LATCH','latch','Latch','SR','signal','Rear SET; top RESET wins', {copper:1,crystal:2}],
    ['COUNTER','counter','Counter','#','signal','Rear counts edges; top resets; target 1–15', {copper:2,crystal:2}],
    ['CLUTCH','clutch','Clutch','C','mechanical','Top signal engages rear → front drive', {iron:3,copper:1}],
    ['BELT','belt','Belt Drive','==','mechanical','Front links across ≤6 cells to pulley or joint', {wood:2,iron:1}],
    ['PULLEY','pulley','Pulley','◎','mechanical','Rear → front; can receive a remote belt', {iron:2}],
    ['BEVEL','bevel','Bevel Gear','┓','mechanical','Rear input → bottom output (90°)', {iron:3}],
    ['SMALL_GEAR','smallGear','Small Gear','g','mechanical','2× speed / ½ torque; reverses rotation', {iron:2}],
    ['LARGE_GEAR','largeGear','Large Gear','G','mechanical','½ speed / 2× torque; reverses rotation', {iron:4}],
    ['BEARING','bearing','Bearing','◉','mechanical','Drive rotates attached assembly in grid quarter-turns', {iron:4}],
    ['HINGE','hinge','Hinge','∟','mechanical','Drive + top signal swings attached assembly 90°', {iron:3,copper:1}],
    ['HOPPER','hopper','Hopper','▽','item','Collects above; passes out front', {iron:2,wood:1}],
    ['STORAGE','storage','Storage Bin','▤','item','64 items; rear input; front output to hopper', {wood:4,iron:1}],
    ['SPLITTER','splitter','Item Splitter','Y','item','Rear input → front / bottom alternately', {iron:2,wood:1}],
    ['FILTER','filter','Item Filter','≡','item','Rear input → front; configure allowed material', {iron:2,crystal:1}],
    ['SIGNAL_SPLIT','signalSplit','Signal Splitter','↗','signal','Rear input → front / bottom', {copper:1}],
    ['FRAME','frame','Light Frame','□','structure','Light structural member', {wood:1}],
    ['BEAM','beam','Reinforced Beam','I','structure','Strong structural member', {iron:2}],
    ['DRILL','drill','Industrial Drill','▼','item','Rear drive + side power; mines in front; output below', {iron:8,copper:5,stone:4}],
    ['EXCAVATOR','excavator','Advanced Excavator','▾','item','High-torque drill mines a three-wide corridor', {steel:8,advancedComponent:2,gear:2}],
    ['CRUSHER','crusher','Ore Crusher','⋈','item','Top drive + bottom power; rear ore → front crushed ore', {iron:6,stone:5,gear:1}],
    ['SMELTER','smelter','Electric Smelter','♨','item','Bottom power; rear crushed ore → front ingots', {iron:6,copper:4,stone:8}],
    ['REFINER','refiner','Metal Refiner','◇','item','Refines iron ingots and stone into steel', {ironIngot:6,copperIngot:3,crystal:2}],
    ['ASSEMBLER','assembler','Component Assembler','⚒','item','Makes circuits from copper ingots and crystal', {ironIngot:5,copperIngot:4,gear:1}],
    ['AMMO_FABRICATOR','ammoFabricator','Ammunition Fabricator','⋮','item','One iron ingot becomes four turret rounds', {ironIngot:4,copper:3}],
    ['ADVANCED_FABRICATOR','advancedFabricator','Advanced Fabricator','✧','item','Steel + circuit → advanced components', {steel:6,circuit:4,crystal:4}],
    ['REPAIR_STATION','repairStation','Repair Station','+','item','Iron and crystal become repair kits', {iron:5,copper:3,crystal:2}],
    ['BATTERY','battery','Grid Battery','▥','power','Stores surplus power; supplies 8 power during shortages', {iron:4,copper:6,crystal:4}],
    ['TURRET','turret','Basic Turret','↗','power','Rear ammunition; bottom power; optional top enable', {iron:8,copper:4,gear:1}],
    ['ARC_TOWER','arcTower','Arc Tower','ϟ','power','Powered short-range defense; optional top enable', {ironIngot:6,copperIngot:6,crystal:5}],
    ['TESLA_COIL','teslaCoil','Tesla Coil','↯','power','High-power area burst defense', {steel:6,advancedComponent:2,crystal:8}],
    ['ENEMY_SENSOR','enemySensor','Enemy Detector','◉','signal','Signals when enemies are within eight tiles', {copper:3,crystal:3}],
    ['AUTO_DOOR','autoDoor','Automated Door','▯','power','Powered door opens when a top signal is high', {iron:5,copper:2}],
    ['BARRIER','barrier','Defensive Barrier','▦','structure','Reinforced wall with 240 durability', {iron:5,stone:5}],
    ['TRAP','trap','Mechanical Trap','⋀','mechanical','Rear rotation drives a short-range trap', {iron:5,gear:1,wood:2}],
    ['RECOVERY_STATION','recoveryStation','Recovery Station','✚','power','Powered station heals nearby engineers', {ironIngot:5,circuit:2,crystal:4}]
  ];
  // IDs 52–54 belong to the original door, ladder and ledge. Keep every saved tile stable.
  const MACHINES={
    DRILL:{load:2,torque:4,time:1.5},EXCAVATOR:{load:6,torque:10,time:.8},
    CRUSHER:{load:2,torque:3,time:1.5,recipes:[
      {key:'crushedIron',cost:{iron:1},out:{crushedIron:1}},
      {key:'crushedCopper',cost:{copper:1},out:{crushedCopper:1}}]},
    SMELTER:{load:4,time:2,recipes:[
      {key:'ironIngot',cost:{crushedIron:1},out:{ironIngot:1}},
      {key:'copperIngot',cost:{crushedCopper:1},out:{copperIngot:1}},
      {key:'rawIron',cost:{iron:2},out:{ironIngot:1}},
      {key:'rawCopper',cost:{copper:2},out:{copperIngot:1}}]},
    REFINER:{load:5,time:3,recipes:[{key:'steel',cost:{ironIngot:2,stone:1},out:{steel:1}}]},
    ASSEMBLER:{load:3,time:2.5,recipes:[
      {key:'circuit',cost:{copperIngot:2,crystal:1},out:{circuit:1}},
      {key:'gear',cost:{ironIngot:2},out:{gear:1}},
      {key:'wire',cost:{copperIngot:1},out:{wire:6}}]},
    AMMO_FABRICATOR:{load:3,time:1.5,recipes:[{key:'ammo',cost:{ironIngot:1},out:{ammo:4}}]},
    ADVANCED_FABRICATOR:{load:6,time:4,recipes:[{key:'advancedComponent',cost:{steel:2,circuit:1},out:{advancedComponent:1}}]},
    REPAIR_STATION:{load:2,time:3,recipes:[{key:'repairKit',cost:{iron:2,crystal:1},out:{repairKit:1}}]}
  };
  const GOODS=[['crushedIron','Crushed Iron','◆'],['crushedCopper','Crushed Copper','◆'],
    ['ironIngot','Iron Ingot','▰'],['copperIngot','Copper Ingot','▰'],['steel','Steel','▰'],
    ['circuit','Circuit','▣'],['ammo','Turret Rounds','⋮'],['advancedComponent','Advanced Component','✧'],['repairKit','Repair Kit','+']];
  const port = (side, type, mode, name = type) => ({side, type, mode, name});
  const all = (type, mode) => DIRS.map((_,side)=>port(side,type,mode));
  function install(T, defs, items, recipes) {
    CATALOG.forEach(([symbol,key,name,icon,kind,desc,cost],i)=>{
      const id=26+i+(i>=26?3:0); T[symbol]=id;
      defs[id]={name,drop:key,solid:kind!=='signal' && !['BELT','PULLEY','SMALL_GEAR'].includes(symbol),
        hard:.6,weight:kind==='signal'?1:kind==='structure'?(symbol==='FRAME'?1:3):3,
        strength:symbol==='BEAM'?16:symbol==='FRAME'?4:8,
        signal:kind==='signal',mechanical:kind==='mechanical',transport:kind==='item',symbol,icon,kind};
      items.push({key,tile:id,name,icon,color:COLORS[kind]});
      recipes.push({key,name,icon,out:1,cost,desc});
    });
    GOODS.forEach(([key,name,icon])=>{if(!items.some(item=>item.key===key))items.push({key,tile:-2,name,icon,color:'#b9d7e4',resource:true});});
    for(const [symbol,machine] of Object.entries(MACHINES))Object.assign(defs[T[symbol]],{
      machine,producer:true,transport:true,electric:true,load:machine.load,capacity:40,inventoryCapacity:24,outputCapacity:16,mechanical:!!machine.torque});
    Object.assign(defs[T.BATTERY],{electric:true,capacity:50,battery:true,storage:120,discharge:8,charge:4});
    [T.TURRET,T.ARC_TOWER,T.TESLA_COIL,T.AUTO_DOOR,T.RECOVERY_STATION].forEach((id,i)=>Object.assign(defs[id],{
      electric:true,load:[3,6,10,1,4][i],capacity:40,defense:i<3,door:id===T.AUTO_DOOR}));
    Object.assign(defs[T.TURRET],{transport:true,inventoryCapacity:48,ammo:'ammo',range:9,damage:14,cooldown:.65});
    Object.assign(defs[T.ARC_TOWER],{range:5,damage:11,cooldown:.8});
    Object.assign(defs[T.TESLA_COIL],{range:4,damage:18,cooldown:1.5,area:true});
    Object.assign(defs[T.TRAP],{defense:true,range:1.7,damage:20,cooldown:1.2,torque:3});
    Object.assign(defs[T.BARRIER],{hp:240,strength:30});
    Object.assign(defs[T.ENEMY_SENSOR],{sensor:true,range:8});
    // Recovered manufactured blocks remain components, not a lossy raw-material conversion.
    defs[T.COPPER_BLOCK].drop='copperBlock'; defs[T.IRON_BLOCK].drop='ironBlock';
    const map = new Map();
    [T.WIRE,T.COPPER_BLOCK,T.IRON_BLOCK].forEach(id=>map.set(id,all('power','both')));
    map.set(T.GENERATOR,all('power','out'));
    map.set(T.BATTERY,all('power','both'));
    map.set(T.SIGNAL,all('signal','both'));
    [T.SWITCH,T.SENSOR].forEach(id=>map.set(id,[port(0,'signal','out','OUT')]));
    map.set(T.MOTOR,[port(2,'power','in','PWR'),port(3,'signal','in','EN'),port(0,'mechanical','out','DRIVE')]);
    [T.LAMP,T.CORE].forEach(id=>map.set(id,[port(2,'power','in','PWR'),port(1,'power','in','PWR'),port(3,'signal','in','EN')]));
    [T.AND,T.OR,T.XOR,T.NAND,T.NOR,T.LATCH,T.COUNTER].forEach(id=>map.set(id,
      [port(2,'signal','in',id===T.COUNTER?'COUNT':id===T.LATCH?'SET':'A'),
        port(3,'signal','in',id===T.COUNTER||id===T.LATCH?'RESET':'B'),port(0,'signal','out','OUT')]));
    [T.NOT,T.TIMER,T.PULSE,T.TOGGLE].forEach(id=>map.set(id,[port(2,'signal','in','A'),port(0,'signal','out','OUT')]));
    map.set(T.SIGNAL_SPLIT,[port(2,'signal','in','A'),port(0,'signal','out','OUT'),port(1,'signal','out','OUT')]);
    [T.SHAFT,T.GEAR,T.BELT,T.PULLEY,T.SMALL_GEAR,T.LARGE_GEAR,T.CLUTCH].forEach(id=>map.set(id,
      [port(2,'mechanical','in','DRIVE'),port(0,'mechanical','out','DRIVE'),...(id===T.CLUTCH?[port(3,'signal','in','EN')]:[])]));
    map.set(T.BEVEL,[port(2,'mechanical','in','DRIVE'),port(1,'mechanical','out','DRIVE')]);
    [T.PISTON,T.WINCH,T.BEARING,T.HINGE].forEach(id=>map.set(id,
      [port(2,'mechanical','in','DRIVE'),port(3,'signal','in','EN'),port(0,'structure','both','JOINT')]));
    map.set(T.CONVEYOR,[port(1,'mechanical','in','DRIVE'),port(2,'mechanical','in','DRIVE'),
      port(0,'mechanical','out','DRIVE'),port(2,'item','in','IN'),port(0,'item','out','OUT'),port(3,'item','in','DROP')]);
    map.set(T.HOPPER,[port(3,'item','in','IN'),port(2,'item','in','IN'),port(0,'item','out','OUT')]);
    [T.STORAGE,T.FILTER].forEach(id=>map.set(id,[port(2,'item','in','IN'),port(0,'item','out','OUT')]));
    map.set(T.SPLITTER,[port(2,'item','in','IN'),port(0,'item','out','OUT'),port(1,'item','out','OUT2')]);
    for(const [symbol,machine] of Object.entries(MACHINES)){
      const mining=symbol==='DRILL'||symbol==='EXCAVATOR';
      map.set(T[symbol],[port(1,'power','in','PWR'),...(mining?[port(3,'power','in','PWR'),port(2,'mechanical','in','DRIVE'),port(1,'item','out','OUT')]:
        [port(2,'item','in','IN'),port(0,'item','out','OUT'),...(machine.torque?[port(3,'mechanical','in','DRIVE')]:[])]),port(3,'signal','in','EN')]);
    }
    [T.TURRET,T.ARC_TOWER,T.TESLA_COIL,T.AUTO_DOOR,T.RECOVERY_STATION].forEach(id=>map.set(id,
      [port(1,'power','in','PWR'),port(2,'power','in','PWR'),port(3,'signal','in','EN'),...(id===T.TURRET?[port(2,'item','in','AMMO')]:[])]));
    map.set(T.ENEMY_SENSOR,[port(0,'signal','out','OUT')]);
    map.set(T.TRAP,[port(2,'mechanical','in','DRIVE'),port(3,'signal','in','EN')]);
    for (const [id,ports] of map) defs[id].ports=ports;
  }
  function ports(d,m={}) {return (d?.ports||[]).map(p=>({...p,side:(p.side+(m.rot||0))%4}));}
  function compatible(a,b) {return a.type===b.type && a.mode!=='in' && b.mode!=='out';}
  function strengthColor(ratio) {return ratio<.25?'#64a9ff':ratio<.6?'#8ee36c':ratio<.85?'#ffe365':ratio<1?'#ff9d46':'#ff5b5b';}
  class Simulation {
    constructor({width,height,get,set,meta,defs,T}) {
      Object.assign(this,{width,height,get,set,getMeta:meta,defs,T});
      this.nodes=new Set();this.revision=0;this.built=-1;this.rebuilds=0;
      this.drops=[];this.nextDrop=1;this.nextAssembly=1;this.nextNode=1;this.elapsed=0;
      this.metrics={mined:0,produced:0,shots:0,healed:0};
    }
    key(x,y){return x+y*this.width;}
    xy(k){return [k%this.width,Math.floor(k/this.width)];}
    inside(x,y){return x>=0&&y>=0&&x<this.width&&y<this.height;}
    id(k){const [x,y]=this.xy(k);return this.get(x,y);}
    data(k){let m=this.getMeta().get(k);if(!m){m={};this.getMeta().set(k,m);}return m;}
    pp(k,type){return ports(this.defs[this.id(k)],this.getMeta().get(k)||{}).filter(p=>!type||p.type===type);}
    changed(x,y){const k=this.key(x,y),id=this.get(x,y),m=this.getMeta().get(k);
      if(id&&(this.defs[id]?.ports||m?.assembly))this.nodes.add(k);else this.nodes.delete(k);
      this.revision++;
    }
    reset(){
      this.nodes.clear();this.drops=[];this.nextDrop=1;this.nextAssembly=1;this.nextNode=1;this.elapsed=0;
      this.metrics={mined:0,produced:0,shots:0,healed:0};
      for(let y=0;y<this.height;y++)for(let x=0;x<this.width;x++){
        const id=this.get(x,y),k=this.key(x,y),m=this.getMeta().get(k);
        if(id&&(this.defs[id]?.ports||m?.linked||m?.assembly))this.nodes.add(k);
        if(m?.assembly)this.nextAssembly=Math.max(this.nextAssembly,m.assembly+1);
        if(m?.nodeId)this.nextNode=Math.max(this.nextNode,m.nodeId+1);
      }
      // One-time migration: old touching "linked" flags become distinct connected assemblies.
      const old=new Set([...this.nodes].filter(k=>this.data(k).linked&&!this.data(k).assembly));
      while(old.size){const start=old.values().next().value,id=this.nextAssembly++,q=[start];old.delete(start);
        for(let i=0;i<q.length;i++){const k=q[i];this.data(k).assembly=id;const [x,y]=this.xy(k);
          for(const [dx,dy] of DIRS){const n=this.key(x+dx,y+dy);if(this.inside(x+dx,y+dy)&&old.delete(n))q.push(n);}}
      }
      this.revision++;this.built=-1;
    }
    neighbor(k,side){const [x,y]=this.xy(k),[dx,dy]=DIRS[side];return this.inside(x+dx,y+dy)?this.key(x+dx,y+dy):null;}
    connections(k,type){const result=[];
      for(const p of this.pp(k,type)){
        const n=this.neighbor(k,p.side);if(n===null||!this.nodes.has(n))continue;
        for(const other of this.pp(n,type))if(other.side===(p.side+2)%4&&compatible(p,other))result.push({to:n,fromPort:p,toPort:other});
      }return result;
    }
    rebuild(){
      if(this.built===this.revision)return;
      this.built=this.revision;this.rebuilds++;this.active=[...this.nodes].sort((a,b)=>a-b);
      this.powerNets=[];this.wireNet=new Map();this.wires=[];this.drivers=new Map();this.inputs=new Map();
      this.mechanical=new Map();this.assemblies=new Map();this.nodeKeys=new Map();this.transport=[];this.signalNodes=[];
      for(const k of this.active){const m=this.data(k);if(!m.nodeId)m.nodeId=this.nextNode++;this.nodeKeys.set(m.nodeId,k);
        if(m.assembly){if(!this.assemblies.has(m.assembly))this.assemblies.set(m.assembly,[]);this.assemblies.get(m.assembly).push(k);}
        if(this.defs[this.id(k)].transport||this.id(k)===this.T.CONVEYOR)this.transport.push(k);
        if(this.pp(k,'signal').length)this.signalNodes.push(k);
      }
      const seen=new Set();
      for(const k of this.active){if(seen.has(k)||!this.pp(k,'power').length)continue;
        const q=[k];seen.add(k);
        for(let i=0;i<q.length;i++){
          const a=q[i];for(const p of this.pp(a,'power')){const n=this.neighbor(a,p.side);
            if(n===null||seen.has(n)||!this.nodes.has(n))continue;
            if(this.pp(n,'power').some(b=>b.side===(p.side+2)%4&&(compatible(p,b)||compatible(b,p)))){seen.add(n);q.push(n);}}
        }this.powerNets.push(q);
      }
      // Only passive wire tiles share a bus. Gate input/output ports never short through the gate.
      for(const k of this.signalNodes){if(this.id(k)!==this.T.SIGNAL||this.wireNet.has(k))continue;
        const net=this.wires.length,q=[k];this.wireNet.set(k,net);
        for(let i=0;i<q.length;i++)for(const e of this.connections(q[i],'signal')){
          if(this.id(e.to)===this.T.SIGNAL&&!this.wireNet.has(e.to)){this.wireNet.set(e.to,net);q.push(e.to);}}
        this.wires.push(q);this.drivers.set(net,new Set());
      }
      for(const k of this.signalNodes){
        for(const e of this.connections(k,'signal')){
          if(this.id(k)!==this.T.SIGNAL && e.fromPort.mode==='out' && this.wireNet.has(e.to))this.drivers.get(this.wireNet.get(e.to)).add(k);
        }
        const ins={};
        for(const p of this.pp(k,'signal').filter(p=>p.mode==='in')){const n=this.neighbor(k,p.side);
          if(n!==null&&this.nodes.has(n)&&this.pp(n,'signal').some(b=>b.side===(p.side+2)%4&&compatible(b,p)))
            ins[p.name]=this.wireNet.has(n)?{net:this.wireNet.get(n)}:{node:n};
        }this.inputs.set(k,ins);
      }
      this.transportSet=new Set(this.transport);
      for(const k of this.active){if(!this.pp(k,'mechanical').length)continue;
        const edges=this.connections(k,'mechanical');
        if(this.id(k)===this.T.BELT){const p=this.pp(k,'mechanical').find(p=>p.mode==='out'),[x,y]=this.xy(k),[dx,dy]=DIRS[p.side],obstacles=[];
          const near=this.neighbor(k,p.side);if(near!==null&&this.id(near))obstacles.push(near);
          for(let dist=2;dist<=7;dist++){const nx=x+dx*dist,ny=y+dy*dist;
            if(!this.inside(nx,ny))break;
            const id=this.get(nx,ny);if(!id)continue;
            const to=this.key(nx,ny),other=this.pp(to,'mechanical').find(b=>b.side===(p.side+2)%4&&compatible(p,b));
            if([this.T.BELT,this.T.PULLEY,this.T.BEARING,this.T.HINGE].includes(id)&&other){
              // A joint's belt is routed behind its own rotating load. Unrelated obstacles still block it.
              const joint=[this.T.BEARING,this.T.HINGE].includes(id)?this.data(to).jointAssembly:null;
              if(obstacles.every(n=>joint&&this.data(n).assembly===joint))edges.push({to,fromPort:p,toPort:other,remote:true});break;
            }
            obstacles.push(to);
          }
        }this.mechanical.set(k,edges);
      }
    }
    input(k,name,outputs,buses){const p=this.inputs.get(k)?.[name];return !p?false:p.net!==undefined?!!buses[p.net]:!!outputs.get(p.node);}
    buses(outputs){return this.wires.map((_,i)=>[...this.drivers.get(i)].some(k=>outputs.get(k)));}
    logic(dt){
      const T=this.T,comb=new Set([T.AND,T.OR,T.NOT,T.XOR,T.NAND,T.NOR,T.SIGNAL_SPLIT]);
      let values=new Map(this.signalNodes.map(k=>[k,!!this.data(k).output]));
      for(const k of this.signalNodes){const id=this.id(k),m=this.data(k);m.logicUnstable=false;
        if(id===T.SWITCH)values.set(k,!!m.on);if(id===T.SENSOR||id===T.ENEMY_SENSOR)values.set(k,!!m.sensorActive);}
      const settle=()=>{
        let stable=false;
        // Bounded fixed-point pass: propagation is independent of placement / tile traversal order.
        for(let pass=0;pass<64;pass++){const b=this.buses(values),next=new Map(values);let changed=false;
          for(const k of this.signalNodes){const id=this.id(k);if(!comb.has(id))continue;
            const a=this.input(k,'A',values,b),bb=this.input(k,'B',values,b);
            const v=id===T.AND?a&&bb:id===T.OR?a||bb:id===T.NOT?!a:id===T.XOR?a!==bb:id===T.NAND?!(a&&bb):id===T.NOR?!(a||bb):a;
            if(v!==values.get(k))changed=true;next.set(k,v);
          }values=next;if(!changed){stable=true;break;}
        }
        if(!stable)for(const k of this.signalNodes)if(comb.has(this.id(k)))this.data(k).logicUnstable=true;
      };
      settle();
      if(dt>0){const b=this.buses(values),next=new Map(values);
        for(const k of this.signalNodes){const m=this.data(k),id=this.id(k),ins=this.inputs.get(k)||{};
          const a=this.input(k,id===T.LATCH?'SET':id===T.COUNTER?'COUNT':'A',values,b),reset=this.input(k,'RESET',values,b),edge=a&&!m.prevInput;
          const delay=DELAYS.includes(m.delay)?m.delay:1;let out=values.get(k);
          if(id===T.TIMER){m.timer=a?(m.timer||0)+dt:0;out=a&&m.timer+1e-8>=delay;}
          if(id===T.PULSE){const enabled=!ins.A||a;if(enabled){m.phase=(m.phase||0)+dt;out=m.phase+1e-8>=delay;if(out)m.phase%=delay;}else{m.phase=0;out=false;}}
          if(id===T.TOGGLE){if(edge)m.memory=!m.memory;out=!!m.memory;}
          if(id===T.LATCH){if(reset)m.memory=false;else if(a)m.memory=true;out=!!m.memory;}
          if(id===T.COUNTER){m.count=reset?0:Math.min(15,(m.count||0)+(edge?1:0));out=m.count>=(m.target||3);}
          m.prevInput=a;next.set(k,!!out);
        }values=next;settle();
      }
      const b=this.buses(values);
      for(const k of this.signalNodes){const m=this.data(k);m.output=!!values.get(k);m.active=this.id(k)===T.SIGNAL?b[this.wireNet.get(k)]:m.output;
        m.signalInputs={};for(const name of Object.keys(this.inputs.get(k)||{}))m.signalInputs[name]=this.input(k,name,values,b);
        m.signalOK=!this.inputs.get(k)?.EN||this.input(k,'EN',values,b);
      }
    }
    power(dt=.05){let supplyTotal=0,loadTotal=0;
      this.powerNets.forEach((net,i)=>{let supply=0,load=0,capacity=Infinity;
        const batteries=[];
        for(const k of net){const d=this.defs[this.id(k)],m=this.data(k);supply+=m.disabled?0:d.source||0;
          load+=m.disabled||m.signalOK===false?0:d.load||0;
          if(m.leechUntil>this.elapsed)load+=m.leechDrain||2;
          capacity=Math.min(capacity,d.capacity||Infinity);if(d.battery)batteries.push(k);}
        // Zero-time inspector refreshes observe the same grid without spending or adding energy.
        const horizon=dt>0?dt:.05,generated=supply;
        for(const k of batteries){const d=this.defs[this.id(k)],m=this.data(k);m.energy=Math.max(0,Math.min(d.storage,Number(m.energy)||0));m.batteryFlow=0;}
        const available=batteries.reduce((n,k)=>n+(this.data(k).disabled?0:Math.min(this.defs[this.id(k)].discharge,this.data(k).energy/horizon)),0);
        let remaining=Math.max(0,load-supply),discharged=0;
        // Brownouts and overloaded cables stop consumers; they cannot drain a battery without doing work.
        if(load<=capacity&&generated+available+1e-8>=load)for(const k of batteries){
          const d=this.defs[this.id(k)],m=this.data(k);if(m.disabled)continue;
          const amount=Math.min(remaining,d.discharge,m.energy/horizon);
          if(amount>0){m.energy=Math.max(0,m.energy-amount*Math.max(0,dt));m.batteryFlow=-amount;remaining-=amount;discharged+=amount;}}
        supply+=discharged;
        let surplus=Math.max(0,Math.min(generated-load,capacity-load)),charging=0;
        for(const k of batteries){const d=this.defs[this.id(k)],m=this.data(k);if(m.disabled||m.batteryFlow<0)continue;
          const amount=Math.min(surplus,d.charge,(d.storage-m.energy)/horizon);
          if(amount>0){m.energy+=amount*Math.max(0,dt);m.batteryFlow=amount;surplus-=amount;charging+=amount;}}
        // Charging is an actual load and shares the cable's remaining capacity with machinery.
        load+=charging;
        for(const k of net){const m=this.data(k);Object.assign(m,{powerNet:i+1,supply,load,capacity,overload:load>capacity,powered:supply>0&&supply+1e-8>=load&&load<=capacity});}
        supplyTotal+=supply;loadTotal+=load;
      });this.supply=supplyTotal;this.load=loadTotal;
    }
    ratio(k){const T=this.T,id=this.id(k),m=this.data(k);
      return id===T.SMALL_GEAR?-2:id===T.LARGE_GEAR?-.5:id===T.GEAR?(m.mode===0?.5:m.mode===2?2:1):1;
    }
    mechanics(){const T=this.T;
      for(const k of this.mechanical.keys()){const m=this.data(k);m.running=false;m.torque=0;m.speed=0;m.driveSource=null;m.drivePath=[];m.mechanicalFault='';}
      for(const source of this.mechanical.keys()){
        if(this.id(source)!==T.MOTOR)continue;const src=this.data(source);
        if(src.disabled||!src.powered||src.signalOK===false||src.overload)continue;
        const q=[{k:source,torque:12,speed:1,path:[]}],seen=new Set();
        for(let i=0;i<q.length;i++){
          const {k,torque,speed,path}=q[i];if(seen.has(k))continue;seen.add(k);const m=this.data(k),id=this.id(k);
          if(m.disabled){m.mechanicalFault='COMPONENT DISABLED';continue;}
          if(id===T.CLUTCH&&!m.signalInputs?.EN){m.mechanicalFault='CLUTCH OPEN';continue;}
          const ratio=this.ratio(k),outTorque=torque/Math.abs(ratio),outSpeed=speed*ratio;
          if(outTorque>4096||Math.abs(outSpeed)>64){m.mechanicalFault='TRANSMISSION LIMIT';continue;}
          if(m.driveSource!==null&&m.driveSource!==source){m.mechanicalFault='MULTIPLE DRIVES — isolate with a clutch';continue;}
          Object.assign(m,{running:true,torque:outTorque,speed:outSpeed,driveSource:source,drivePath:[...path,k]});
          const edges=(this.mechanical.get(k)||[]).filter(e=>!seen.has(e.to));
          // Branches divide available torque; gearing never multiplies total power.
          for(const e of edges)q.push({k:e.to,torque:outTorque/edges.length,speed:outSpeed,path:[...path,k]});
        }
      }
    }
    step(dt=.05){this.rebuild();this.elapsed+=dt;this.logic(dt);this.power(dt);this.mechanics();this.updateStress(dt);}
    group(x,y){this.rebuild();const k=this.key(x,y),m=this.data(k);return (m.assembly?this.assemblies.get(m.assembly):[k])?.map(k=>this.xy(k))||[];}
    structureInfo(id){this.rebuild();const keys=this.assemblies.get(id)||[];
      return {keys,mass:keys.reduce((n,k)=>n+(this.defs[this.id(k)].weight??1),0),
        strength:keys.length?Math.min(...keys.map(k=>this.defs[this.id(k)].strength||8)):0,
        stress:keys.length?Math.max(...keys.map(k=>this.data(k).stress||0)):0};
    }
    splitAssembly(id){
      const remaining=new Set([...this.nodes].filter(k=>this.data(k).assembly===id));let first=true;
      while(remaining.size){const start=remaining.values().next().value,q=[start],next=first?id:this.nextAssembly++;first=false;remaining.delete(start);
        for(let i=0;i<q.length;i++){this.data(q[i]).assembly=next;for(let side=0;side<4;side++){const n=this.neighbor(q[i],side);if(remaining.delete(n))q.push(n);}}
      }this.revision++;
    }
    link(keys,mode='new',target=null){
      const valid=keys.filter(k=>this.id(k)&&this.id(k)!==this.T.CORE),old=new Set(valid.map(k=>this.data(k).assembly).filter(Boolean));
      if(mode==='add'&&!target)return null;
      const id=mode==='remove'?null:mode==='add'?target:this.nextAssembly++;
      for(const k of valid){const m=this.data(k);m.assembly=id;m.linked=!!id;this.nodes.add(k);}
      for(const previous of old)if(previous!==id)this.splitAssembly(previous);
      if(id)this.splitAssembly(id);this.revision++;return id;
    }
    updateStress(dt){
      for(const [id,keys] of this.assemblies){const info=this.structureInfo(id);
        for(const k of keys){const m=this.data(k);m.stress=Math.max(0,(m.stress||0)-dt*2);m.mass=info.mass;}
      }
    }
    applyStress(group,force){const keys=group.map(p=>this.key(...p));let failure=null;
      for(const k of keys){const m=this.data(k),strength=this.defs[this.id(k)].strength||8;
        m.stress=force/Math.max(1,Math.sqrt(keys.length));m.stressRatio=m.stress/(strength*4);
        // Generous: only repeated extreme loads detach a weak connection; material is never deleted.
        m.strikes=m.stressRatio>1.5?(m.strikes||0)+1:0;
        if(m.strikes>=3&&m.assembly)failure=k;
      }
      if(failure!==null){const m=this.data(failure),id=m.assembly;m.assembly=null;m.linked=false;m.strikes=0;this.splitAssembly(id);}
      return failure;
    }
    spawn(key,count,x,y){
      if(typeof key!=='string'||!key||!Number.isFinite(count)||count<=0||!Number.isFinite(x)||!Number.isFinite(y))return false;
      const stack=this.drops.find(d=>d.key===key&&Math.abs(d.x-x)<.4&&Math.abs(d.y-y)<.4);
      if(stack){stack.count+=count;return true;}
      // Refuse mining rather than destroying resources when the entity budget is exhausted.
      if(this.drops.length>=512)return false;
      this.drops.push({id:this.nextDrop++,key,count,x,y,age:0});return true;
    }
    capacity(k){return this.defs[this.id(k)]?.inventoryCapacity||(this.id(k)===this.T.STORAGE?64:8);}
    stored(k){return Object.values(this.data(k).contents||{}).reduce((a,b)=>a+b,0);}
    insert(k,key,count=1){const m=this.data(k),d=this.defs[this.id(k)];
      if(!d?.transport&&this.id(k)!==this.T.CONVEYOR)return 0;
      if(d.ammo&&key!==d.ammo||d.producer&&!d.machine.recipes)return 0;
      if(this.id(k)===this.T.FILTER&&(m.filter||'iron')!==key)return 0;
      if(!key||!Number.isFinite(count)||count<=0)return 0;
      const n=Math.max(0,Math.min(count,this.capacity(k)-this.stored(k)));if(!n)return 0;
      if(!m.contents)m.contents={};m.contents[key]=(m.contents[key]||0)+n;return n;
    }
    recipe(k){const d=this.defs[this.id(k)],m=this.data(k),recipes=d?.machine?.recipes||[];
      return recipes.find(r=>r.key===m.recipe)||recipes[0]||null;
    }
    outputRoom(k){const d=this.defs[this.id(k)];return Math.max(0,(d.outputCapacity||16)-Object.values(this.data(k).outputs||{}).reduce((n,v)=>n+v,0));}
    outputConnection(k,key){
      for(const p of this.pp(k,'item').filter(p=>p.mode==='out')){const n=this.neighbor(k,p.side);if(n===null)continue;
        const d=this.defs[this.id(n)],m=this.data(n);
        if(!d?.transport||!this.pp(n,'item').some(b=>b.side===(p.side+2)%4&&compatible(p,b)))continue;
        if(this.stored(n)>=this.capacity(n)||this.id(n)===this.T.FILTER&&(m.filter||'iron')!==key||d.ammo&&d.ammo!==key)continue;
        if(d.producer&&!d.machine?.recipes)continue;return n;
      }return null;
    }
    miningTarget(k){const m=this.data(k),[x,y]=this.xy(k),[dx,dy]=DIRS[m.rot||0],wide=this.id(k)===this.T.EXCAVATOR;
      const allowed=new Set([this.T.GRASS,this.T.DIRT,this.T.STONE,this.T.WOOD,this.T.COPPER,this.T.IRON,this.T.CRYSTAL,this.T.OBSIDIAN]);
      const blocked=new Set();
      for(let depth=1;depth<=(wide?10:6);depth++)for(const offset of wide?[0,-1,1]:[0]){
        if(blocked.has(offset))continue;
        const tx=x+dx*depth-dy*offset,ty=y+dy*depth+dx*offset;
        if(!this.inside(tx,ty)){blocked.add(offset);continue;}const id=this.get(tx,ty);
        if(!id)continue;
        // The first obstruction ends this ray; machines never tunnel through protected construction.
        if(!allowed.has(id)||this.getMeta().get(this.key(tx,ty))?.assembly){blocked.add(offset);continue;}
        return {x:tx,y:ty,id,key:this.defs[id]?.drop};
      }return null;
    }
    production(dt,{set=this.set,onMined,onProduced}={}){
      this.rebuild();dt=Math.max(0,Math.min(Number(dt)||0,2));
      for(const k of this.active){const d=this.defs[this.id(k)];if(!d?.producer)continue;
        const m=this.data(k),machine=d.machine,recipe=this.recipe(k);m.machineFault='';m.working=false;m.efficiency=0;
        if(m.disabled||m.signalOK===false){m.machineFault='DISABLED BY CONTROL SIGNAL';continue;}
        if(!m.powered){m.machineFault=m.overload?'POWER CABLE OVERLOADED':'CONNECT POWER TO PWR INPUT';continue;}
        if(machine.torque&&(!m.running||m.torque<machine.torque)){m.machineFault=!m.running?'CONNECT MOTOR TO DRIVE INPUT':`NEEDS ${machine.torque} TORQUE`;continue;}
        const speed=machine.torque?Math.min(2,Math.abs(m.speed||0)):1;
        if(!recipe){
          const target=this.miningTarget(k);
          if(!target?.key){m.machineFault='NO MINEABLE BLOCKS IN FRONT';m.progress=0;continue;}
          const targetKey=`${target.x},${target.y},${target.id}`;
          if(m.miningTarget!==targetKey){m.miningTarget=targetKey;m.progress=0;}
          if(typeof set!=='function'){m.machineFault='WORLD MINING CONNECTION UNAVAILABLE';continue;}
          if(this.outputRoom(k)<1||this.outputConnection(k,target.key)===null){m.machineFault='OUTPUT BLOCKED: CONNECT A HOPPER WITH SPACE';continue;}
          m.working=true;m.efficiency=speed;m.progress=(m.progress||0)+dt*speed/(machine.time*Math.max(.3,this.defs[target.id].hard||1));
          if(m.progress+1e-8<1)continue;
          // Commit only after a real tile is removed. A refused edit produces no resources.
          set(target.x,target.y,this.T.AIR);
          if(this.get(target.x,target.y)!==this.T.AIR){m.machineFault='MINING TARGET PROTECTED';m.working=false;m.progress=0;continue;}
          this.changed(target.x,target.y);m.outputs=m.outputs||{};m.outputs[target.key]=(m.outputs[target.key]||0)+1;
          m.progress=Math.max(0,m.progress-1);m.totalMined=(m.totalMined||0)+1;this.metrics.mined++;
          if(onMined)onMined(target.key,1,target.x,target.y);continue;
        }
        if(m.activeRecipe!==recipe.key){m.activeRecipe=recipe.key;m.progress=0;}
        const missing=()=>Object.entries(recipe.cost).find(([key,n])=>(m.contents?.[key]||0)<n);
        if(missing()){const [key,n]=missing();m.machineFault=`NEEDS ${n} ${key}`;continue;}
        const amount=Object.values(recipe.out).reduce((n,v)=>n+v,0);
        if(this.outputRoom(k)<amount){m.machineFault='OUTPUT BUFFER FULL — EMPTY OR CONNECT LOGISTICS';continue;}
        m.working=true;m.efficiency=speed;m.progress=(m.progress||0)+dt*speed/machine.time;
        for(let cycle=0;cycle<16&&m.progress+1e-8>=1&&!missing()&&this.outputRoom(k)>=amount;cycle++){
          // Ingredients and output capacity are checked together, then committed as one inventory operation.
          for(const [key,n] of Object.entries(recipe.cost)){m.contents[key]-=n;if(!m.contents[key])delete m.contents[key];}
          m.outputs=m.outputs||{};for(const [key,n] of Object.entries(recipe.out))m.outputs[key]=(m.outputs[key]||0)+n;
          m.progress=Math.max(0,m.progress-1);m.totalProduced=(m.totalProduced||0)+amount;this.metrics.produced+=amount;
          if(onProduced)onProduced(recipe.key,amount,k);
        }
        // Completed work cannot accumulate while blocked and burst into free future production.
        m.progress=Math.min(1,m.progress);
      }
    }
    defenses(dt,{targets=[],damage,heal,lineOfSight,canClose}={}){
      this.rebuild();dt=Math.max(0,Math.min(Number(dt)||0,2));
      for(const k of this.active){const id=this.id(k),d=this.defs[id],m=this.data(k),[x,y]=this.xy(k),cx=x+.5,cy=y+.5;
        if(id===this.T.ENEMY_SENSOR){m.sensorActive=targets.some(t=>t.health>0&&Math.hypot(t.x-cx,t.y-cy)<=d.range);continue;}
        if(id===this.T.AUTO_DOOR){m.defenseFault='';const commandedOpen=!!(!m.disabled&&m.powered&&m.signalInputs?.EN);
          if(!commandedOpen&&canClose&&!canClose(x,y)){m.open=true;m.defenseFault='WAITING FOR DOORWAY TO CLEAR';}
          else m.open=commandedOpen;continue;}
        if(id===this.T.RECOVERY_STATION){m.healClock=Math.max(0,(m.healClock||0)-dt);
          if(!m.disabled&&m.powered&&m.signalOK!==false&&m.healClock<=0&&heal){const amount=heal(cx,cy,4);if(amount>0){m.healClock=1;this.metrics.healed+=amount;}}continue;}
        if(!d.defense)continue;m.attackClock=Math.max(0,(m.attackClock||0)-dt);m.defenseFault='';m.firing=false;
        if(m.disabled||m.signalOK===false){m.defenseFault='DISABLED BY CONTROL SIGNAL';continue;}
        if(id===this.T.TRAP?(!m.running||m.torque<d.torque):!m.powered){m.defenseFault=id===this.T.TRAP?'NO MECHANICAL DRIVE':'NO POWER';continue;}
        if(d.ammo&&!(m.contents?.[d.ammo]>0)){m.defenseFault='SUPPLY TURRET ROUNDS TO REAR ITEM INPUT';continue;}
        const candidates=targets.filter(t=>t.health>0&&Math.hypot(t.x-cx,t.y-cy)<=d.range&&(!lineOfSight||lineOfSight(cx,cy,t.x,t.y)))
          .sort((a,b)=>Math.hypot(a.x-cx,a.y-cy)-Math.hypot(b.x-cx,b.y-cy));
        const target=candidates[0];if(!target||m.attackClock>0||!damage)continue;
        if(d.ammo){m.contents[d.ammo]--;if(!m.contents[d.ammo])delete m.contents[d.ammo];}
        for(const t of d.area?candidates:[target])damage(t,d.damage,id===this.T.TURRET?'turret':id===this.T.TRAP?'trap':id===this.T.TESLA_COIL?'tesla':'arc');
        m.attackClock=d.cooldown;m.lastShot={x:target.x,y:target.y,time:this.elapsed};m.firing=true;this.metrics.shots++;
      }
    }
    diagnose(k){this.rebuild();const d=this.defs[this.id(k)],m=this.data(k);return {
      name:d?.name,powered:!!m.powered,supply:m.supply||0,load:m.load||0,capacity:m.capacity||0,
      running:!!m.running,torque:m.torque||0,signal:m.signalOK!==false,working:!!m.working,
      recipe:this.recipe(k),progress:m.progress||0,efficiency:m.efficiency||0,energy:m.energy||0,
      contents:{...(m.contents||{})},outputs:{...(m.outputs||{})},
      fault:m.machineFault||m.defenseFault||m.mechanicalFault||m.logisticsFault||
        (d?.load&&!m.powered?'CONNECT POWER TO PWR INPUT':d?.mechanical&&!m.running?'CONNECT MOTOR TO DRIVE INPUT':''),
      connections:['power','mechanical','signal','item'].map(type=>({type,count:this.pp(k,type).filter(p=>{
        const n=this.neighbor(k,p.side);return n!==null&&this.pp(n,type).some(b=>b.side===(p.side+2)%4&&(compatible(p,b)||compatible(b,p)));
      }).length}))};}
    smartRotation(x,y,id,fallback=0){
      const d=this.defs[id];let best=-1,candidates=[];
      for(let rot=0;rot<4;rot++){let score=0;
        for(const p of ports(d,{rot})){const [dx,dy]=DIRS[p.side],nx=x+dx,ny=y+dy;if(!this.inside(nx,ny))continue;
          const other=this.defs[this.get(nx,ny)],m=this.getMeta().get(this.key(nx,ny))||{};
          for(const b of ports(other,m))if(b.side===(p.side+2)%4&&(compatible(p,b)||compatible(b,p)))score+=p.type==='power'?1:2;
        }
        if(score>best){best=score;candidates=[rot];}else if(score===best)candidates.push(rot);
      }return candidates.length===1?candidates[0]:fallback;
    }
    logistics(dt){
      this.rebuild();const T=this.T;
      // Each item crosses at most one component per tick, independent of tile iteration order.
      const ready=new Map(this.transport.map(k=>[k,{...((this.defs[this.id(k)].producer?this.data(k).outputs:this.data(k).contents)||{})}]));
      for(const k of this.transport){const m=this.data(k);m.itemClock=(m.itemClock||0)+dt;
        if(m.itemClock<.25)continue;m.itemClock%=.25;m.logisticsFault='';
        const id=this.id(k);if(id===T.CONVEYOR&&!m.running){m.logisticsFault='NO ROTATION';continue;}
        const key=Object.keys(ready.get(k)).find(key=>ready.get(k)[key]>0);if(!key)continue;
        if(id===T.FILTER&&key!==(m.filter||'iron')){m.logisticsFault='FILTER CHANGED — withdraw incompatible contents';continue;}
        const outs=this.pp(k,'item').filter(p=>p.mode==='out');
        const p=outs[id===T.SPLITTER?(m.route||0)%outs.length:0];if(!p)continue;
        const n=this.neighbor(k,p.side);let moved=0;
        if(n!==null){const nid=this.id(n),[nx,ny]=this.xy(n);
          if((id!==T.STORAGE||nid===T.HOPPER)&&this.pp(n,'item').some(b=>b.side===(p.side+2)%4&&compatible(p,b)))moved=this.insert(n,key,1);
          // Manufacturing retains finished goods until a receiver is attached. Transport belts and
          // hoppers may intentionally unload physical items onto the ground.
          if(!nid&&id!==T.STORAGE&&!this.defs[id].producer)moved=this.spawn(key,1,nx+.5,ny+.5)?1:0;
        }
        if(moved){const inventory=this.defs[id].producer?m.outputs:m.contents;inventory[key]--;if(!inventory[key])delete inventory[key];if(id===T.SPLITTER)m.route=((m.route||0)+1)%2;}
        else m.logisticsFault='OUTPUT BLOCKED / FULL / FILTERED';
      }
      for(const drop of this.drops){drop.age+=dt;const x=Math.floor(drop.x),y=Math.floor(drop.y),k=this.key(x,y);
        // Capture from the component's top input; direction is checked before accepting a falling item.
        const below=this.key(x,y+1),same=this.inside(x,y)&&this.transportSet.has(k);
        const target=same?k:this.inside(x,y+1)&&this.pp(below,'item').some(p=>p.side===3&&p.mode!=='out')?below:null;
        if(target!==null){const amount=this.insert(target,drop.key,drop.count);drop.count-=amount;if(!drop.count)continue;}
        const support=this.get(x,y+1),sm=this.getMeta().get(below);
        if(support===T.CONVEYOR&&sm?.running){const [dx,dy]=DIRS[sm.rot||0];const next=drop.x+dx*Math.abs(sm.speed||1)*dt*2;
          if(!this.defs[this.get(Math.floor(next),y)]?.solid)drop.x=next;if(dy)drop.y+=dy*dt;}
        if(!this.defs[support]?.solid&&target===null)drop.y=Math.min(this.height-1.2,drop.y+dt*4);
      }
      this.drops=this.drops.filter(d=>d.count>0);
    }
    export(){return {version:3,drops:this.drops,nextDrop:this.nextDrop,nextAssembly:this.nextAssembly,metrics:this.metrics,elapsed:this.elapsed};}
    restore(d){if(!d)return;this.drops=(d.drops||[]).filter(v=>typeof v.key==='string'&&Number.isFinite(v.count)&&v.count>0&&Number.isFinite(v.x)&&Number.isFinite(v.y)).slice(0,512);
      this.nextDrop=Math.max(d.nextDrop||1,...this.drops.map(v=>v.id+1));this.nextAssembly=Math.max(this.nextAssembly,d.nextAssembly||1);
      if(Number.isFinite(d.elapsed))this.elapsed=Math.max(0,d.elapsed);
      for(const key of Object.keys(this.metrics))if(Number.isFinite(d.metrics?.[key]))this.metrics[key]=Math.max(0,d.metrics[key]);}
  }
  return {install,Simulation,ports,compatible,DIRS,COLORS,DELAYS,CATALOG,MACHINES,GOODS,strengthColor};
});
