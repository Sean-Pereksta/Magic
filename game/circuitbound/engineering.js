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
    ['BEAM','beam','Reinforced Beam','I','structure','Strong structural member', {iron:2}]
  ];
  const port = (side, type, mode, name = type) => ({side, type, mode, name});
  const all = (type, mode) => DIRS.map((_,side)=>port(side,type,mode));
  function install(T, defs, items, recipes) {
    CATALOG.forEach(([symbol,key,name,icon,kind,desc,cost],i)=>{
      const id=26+i; T[symbol]=id;
      defs[id]={name,drop:key,solid:kind!=='signal' && !['BELT','PULLEY','SMALL_GEAR'].includes(symbol),
        hard:.6,weight:kind==='signal'?1:kind==='structure'?(symbol==='FRAME'?1:3):3,
        strength:symbol==='BEAM'?16:symbol==='FRAME'?4:8,
        signal:kind==='signal',mechanical:kind==='mechanical',transport:kind==='item',symbol,icon,kind};
      items.push({key,tile:id,name,icon,color:COLORS[kind]});
      recipes.push({key,name,icon,out:1,cost,desc});
    });
    // Recovered manufactured blocks remain components, not a lossy raw-material conversion.
    defs[T.COPPER_BLOCK].drop='copperBlock'; defs[T.IRON_BLOCK].drop='ironBlock';
    const map = new Map();
    [T.WIRE,T.COPPER_BLOCK,T.IRON_BLOCK].forEach(id=>map.set(id,all('power','both')));
    map.set(T.GENERATOR,all('power','out'));
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
    for (const [id,ports] of map) defs[id].ports=ports;
  }
  function ports(d,m={}) {return (d?.ports||[]).map(p=>({...p,side:(p.side+(m.rot||0))%4}));}
  function compatible(a,b) {return a.type===b.type && a.mode!=='in' && b.mode!=='out';}
  function strengthColor(ratio) {return ratio<.25?'#64a9ff':ratio<.6?'#8ee36c':ratio<.85?'#ffe365':ratio<1?'#ff9d46':'#ff5b5b';}
  class Simulation {
    constructor({width,height,get,meta,defs,T}) {
      Object.assign(this,{width,height,get,getMeta:meta,defs,T});
      this.nodes=new Set();this.revision=0;this.built=-1;this.rebuilds=0;
      this.drops=[];this.nextDrop=1;this.nextAssembly=1;this.nextNode=1;this.elapsed=0;
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
        if(id===T.SWITCH)values.set(k,!!m.on);if(id===T.SENSOR)values.set(k,!!m.sensorActive);}
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
    power(){let supplyTotal=0,loadTotal=0;
      this.powerNets.forEach((net,i)=>{let supply=0,load=0,capacity=Infinity;
        for(const k of net){const d=this.defs[this.id(k)];supply+=d.source||0;load+=d.load||0;capacity=Math.min(capacity,d.capacity||Infinity);}
        for(const k of net){const m=this.data(k);Object.assign(m,{powerNet:i+1,supply,load,capacity,overload:load>capacity,powered:supply>0&&supply>=load&&load<=capacity});}
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
        if(!src.powered||src.signalOK===false||src.overload)continue;
        const q=[{k:source,torque:12,speed:1,path:[]}],seen=new Set();
        for(let i=0;i<q.length;i++){
          const {k,torque,speed,path}=q[i];if(seen.has(k))continue;seen.add(k);const m=this.data(k),id=this.id(k);
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
    step(dt=.05){this.rebuild();this.elapsed+=dt;this.logic(dt);this.power();this.mechanics();this.updateStress(dt);}
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
      if(!key||count<=0)return false;
      const stack=this.drops.find(d=>d.key===key&&Math.abs(d.x-x)<.4&&Math.abs(d.y-y)<.4);
      if(stack){stack.count+=count;return true;}
      // Refuse mining rather than destroying resources when the entity budget is exhausted.
      if(this.drops.length>=512)return false;
      this.drops.push({id:this.nextDrop++,key,count,x,y,age:0});return true;
    }
    capacity(k){return this.id(k)===this.T.STORAGE?64:8;}
    stored(k){return Object.values(this.data(k).contents||{}).reduce((a,b)=>a+b,0);}
    insert(k,key,count=1){const m=this.data(k),d=this.defs[this.id(k)];
      if(!d?.transport&&this.id(k)!==this.T.CONVEYOR)return 0;
      if(this.id(k)===this.T.FILTER&&(m.filter||'iron')!==key)return 0;
      const n=Math.max(0,Math.min(count,this.capacity(k)-this.stored(k)));if(!n)return 0;
      if(!m.contents)m.contents={};m.contents[key]=(m.contents[key]||0)+n;return n;
    }
    logistics(dt){
      this.rebuild();const T=this.T;
      // Each item crosses at most one component per tick, independent of tile iteration order.
      const ready=new Map(this.transport.map(k=>[k,{...(this.data(k).contents||{})}]));
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
          // Storage only feeds an attached hopper; other devices may release real loose items.
          if(!nid&&id!==T.STORAGE)moved=this.spawn(key,1,nx+.5,ny+.5)?1:0;
        }
        if(moved){m.contents[key]--;if(!m.contents[key])delete m.contents[key];if(id===T.SPLITTER)m.route=((m.route||0)+1)%2;}
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
    export(){return {version:2,drops:this.drops,nextDrop:this.nextDrop,nextAssembly:this.nextAssembly};}
    restore(d){if(!d)return;this.drops=(d.drops||[]).filter(v=>typeof v.key==='string'&&Number.isFinite(v.count)&&v.count>0&&Number.isFinite(v.x)&&Number.isFinite(v.y)).slice(0,512);
      this.nextDrop=Math.max(d.nextDrop||1,...this.drops.map(v=>v.id+1));this.nextAssembly=Math.max(this.nextAssembly,d.nextAssembly||1);}
  }
  return {install,Simulation,ports,compatible,DIRS,COLORS,DELAYS,CATALOG,strengthColor};
});
