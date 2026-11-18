(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.CircuitRobotics = factory();
})(typeof globalThis === 'object' ? globalThis : this, function () {
  'use strict';

  const directions = [[0,-1],[1,0],[-1,0],[0,1]];
  const roles = {
    miner: { label:'Mining', item:'miningDrone', cargo:'ore', color:'#f7b57f', speed:5.2 },
    collector: { label:'Collection', item:'collectionDrone', cargo:'salvage', color:'#99f5cc', speed:6.5 },
    defense: { label:'Defense', item:'defenseDrone', color:'#ff918e', speed:7.8 }
  };
  const capacity=8, ammunition=6, battery=100, reserve=24, dispatchCharge=65;
  const cargoSize=drone=>Object.values(drone.cargo).reduce((sum,n)=>sum+n,0);
  const deviceOnline=device=>!!device?.data.powered&&device.data.signalOK!==false&&!device.data.disabled;
  const distance=(x,y,tx,ty)=>Math.hypot(tx-x,ty-y);

  function create({T,defs,inventory,stats,engine,get,set,getMeta,inside,notify=()=>{},save=()=>{},
    cols,rows,width=cols,height=rows,tile=24,enemies=()=>[],hit=()=>{},lineOfSight=()=>true,inventoryRoom}) {
    const key=(x,y)=>x+y*width;
    const coords=k=>[k%width,Math.floor(k/width)];
    const traversable=(x,y)=>inside(x,y)&&(!defs[get(x,y)]?.solid||!!getMeta(x,y)?.open);
    const inventorySpace=inventoryRoom||(()=>Math.max(0,2048-Object.values(inventory).reduce((sum,n)=>sum+n,0)));
    let drones=[],nextDrone=1,devices=[],deviceClock=0,deviceRevision=-1;
    const shots=[];

    function deviceAt(x,y) {
      const id=get(x,y),def=defs[id];
      if (!def?.droneRole) return null;
      const data=getMeta(x,y)||engine.data(key(x,y));
      return {x,y,id,def,data,node:data.nodeId||null};
    }

    function syncDevices(force=false) {
      if (!force&&deviceClock>0&&engine.revision===deviceRevision) return;
      deviceClock=.6;deviceRevision=engine.revision;
      devices=[...(engine.nodes||[])].map(k=>deviceAt(...coords(k))).filter(Boolean);
    }

    function belongs(drone,dock) {
      return drone.home.node&&dock.node?drone.home.node===dock.node:drone.home.x===dock.x&&drone.home.y===dock.y;
    }

    function homeOf(drone) {
      const home=devices.find(device=>device.def.droneRole==='dock'&&belongs(drone,device));
      if (home) {
        drone.home.x=home.x;drone.home.y=home.y;drone.home.node=home.node;
        return home;
      }
      const at=deviceAt(drone.home.x,drone.home.y);
      if (at?.def.droneRole!=='dock'||at.def.droneSpecialty&&at.def.droneSpecialty!==drone.role) return null;
      if (!belongs(drone,at)&&drones.filter(other=>belongs(other,at)).length>=(at.def.droneSlots||3)) return null;
      drone.home.node=at.node;
      return at;
    }

    function pairedBins(home,role) {
      return devices.filter(device=>device.def.droneRole==='depot'&&
        (!device.def.droneCargo||device.def.droneCargo===roles[role].cargo)&&distance(home.x,home.y,device.x,device.y)<=24);
    }

    function room(device) {
      return Math.max(0,(device.def.inventoryCapacity||128)-Object.values(device.data.contents||{}).reduce((sum,n)=>sum+n,0));
    }

    function airPort(device) {
      return directions.map(([dx,dy])=>[device.x+dx,device.y+dy]).find(([x,y])=>traversable(x,y));
    }

    function field(home,drone) {
      const beacons=devices.filter(device=>device.def.droneRole==='beacon'&&deviceOnline(device)&&distance(home.x,home.y,device.x,device.y)<=17);
      const radius=Math.min(25,Math.max(12,...beacons.map(beacon=>Math.ceil(distance(home.x,home.y,beacon.x,beacon.y))+8),
        Math.ceil(distance(home.x,home.y,drone.x,drone.y))+2));
      return {beacons,radius};
    }

    function search(drone,home,radius) {
      let start=[Math.floor(drone.x),Math.floor(drone.y)];
      if (!traversable(...start)) {
        const adjacent=directions.map(([dx,dy])=>[start[0]+dx,start[1]+dy]).find(([x,y])=>traversable(x,y));
        if (!adjacent) return null;
        start=adjacent;
      }
      const first=key(...start),parents=new Map([[first,-1]]),nodes=[first];
      for (let index=0;index<nodes.length;index++) {
        const current=nodes[index],[x,y]=coords(current);
        for (const [dx,dy] of directions) {
          const nx=x+dx,ny=y+dy,next=key(nx,ny);
          if (parents.has(next)||!traversable(nx,ny)||distance(home.x,home.y,nx,ny)>radius) continue;
          parents.set(next,current);nodes.push(next);
        }
      }
      return {nodes,parents};
    }

    function planPath(drone,navigation,destination,job,phase) {
      if (destination==null||!navigation?.parents.has(destination)) return false;
      const backwards=[];
      for (let current=destination;current!==-1;current=navigation.parents.get(current)) backwards.push(current);
      drone.path=backwards.reverse().map(k=>coords(k).map(n=>n+.5));
      drone.job=job;drone.phase=phase;drone.wait=0;
      return true;
    }

    function nearDevice(navigation,device) {
      const gates=new Set(directions.filter(([dx,dy])=>traversable(device.x+dx,device.y+dy)).map(([dx,dy])=>key(device.x+dx,device.y+dy)));
      const index=navigation.nodes.findIndex(node=>gates.has(node));
      return index<0?null:{ node:navigation.nodes[index], index };
    }

    function retirePlan(drone,phase='waiting',reason='') {
      drone.phase=phase;drone.job=null;drone.path=[];drone.work=0;drone.wait=.45;drone.reason=reason;
    }

    function returnHome(drone,home,navigation) {
      const dock=nearDevice(navigation,home);
      if (dock) return planPath(drone,navigation,dock.node,{kind:'dock',x:home.x,y:home.y},'returning');
      retirePlan(drone,'waiting','Clear a flight path to the dock.');
      return false;
    }

    function visitCharger(drone,home,navigation) {
      const dockPort=nearDevice(navigation,home);
      const stops=devices.filter(device=>device.def.droneRole==='charger'&&deviceOnline(device))
        .map(device=>({device,port:nearDevice(navigation,device)})).filter(stop=>stop.port);
      stops.sort((a,b)=>a.port.index-b.port.index);
      const closest=stops[0];
      if (!closest||dockPort&&dockPort.index<=closest.port.index) return false;
      const {device,port}=closest;
      return planPath(drone,navigation,port.node,{kind:'charger',x:device.x,y:device.y},'returning');
    }

    function deliver(drone,home,navigation) {
      const bins=pairedBins(home,drone.role);
      const candidates=bins.filter(bin=>room(bin)>0).map(bin=>({bin,port:nearDevice(navigation,bin)})).filter(entry=>entry.port);
      candidates.sort((a,b)=>Number(!!b.bin.def.droneCargo)-Number(!!a.bin.def.droneCargo)||a.port.index-b.port.index);
      if (candidates.length) {
        const {bin,port}=candidates[0];
        return planPath(drone,navigation,port.node,{kind:'depot',x:bin.x,y:bin.y},'delivering');
      }
      retirePlan(drone,'waiting',bins.length?bins.some(bin=>room(bin)>0)?'Clear a flight path to a collection bin.':'Collection bin full. Take its contents.':'Place a compatible collection bin within range.');
      return false;
    }

    function eligibleTerrain(x,y) {
      const id=get(x,y),def=defs[id];
      if (!inside(x,y)||!def?.drop||def.rareOre||getMeta(x,y)) return false;
      return [T.STONE,T.WOOD,T.COPPER,T.IRON,T.CRYSTAL,T.OBSIDIAN].includes(id);
    }

    function occupiedByJob(drone,kind,x,y,id) {
      return drones.some(other=>other!==drone&&other.job?.kind===kind&&
        (id!==undefined?other.job.id===id:other.job.x===x&&other.job.y===y));
    }

    function withinSurvey(field,x,y) {
      return !field.beacons.length||field.beacons.some(beacon=>distance(beacon.x,beacon.y,x,y)<=8);
    }

    function findMine(drone,navigation,field) {
      const air=key(Math.floor(drone.x),Math.floor(drone.y));
      const checked=new Set();let best=null;
      for (const node of navigation.nodes) {
        const [x,y]=coords(node);
        for (const [dx,dy] of directions) {
          const tx=x+dx,ty=y+dy,k=key(tx,ty);
          if (!inside(tx,ty)||checked.has(k)) continue;
          checked.add(k);
          if (!eligibleTerrain(tx,ty)||!withinSurvey(field,tx,ty)||occupiedByJob(drone,'mine',tx,ty)) continue;
          const id=get(tx,ty),ore=[T.COPPER,T.IRON,T.CRYSTAL].includes(id);
          const score=distance(...coords(air),tx,ty)+(ore?0:4)+Math.max(0,(defs[id].hard||1)-1);
          if (!best||score<best.score) best={node,score,job:{kind:'mine',x:tx,y:ty,tile:id}};
        }
      }
      return best;
    }

    function findCargo(drone,navigation,field) {
      let best=null;
      for (const drop of engine.drops) {
        if (drop.count<=0||drop.age<.2||occupiedByJob(drone,'drop',0,0,drop.id)) continue;
        const x=Math.floor(drop.x),y=Math.floor(drop.y),node=key(x,y),score=distance(drone.x,drone.y,drop.x,drop.y);
        if (!traversable(x,y)||!navigation.parents.has(node)||!withinSurvey(field,x,y)) continue;
        if (!best||score<best.score) best={node,score,job:{kind:'drop',id:drop.id,x,y}};
      }
      for (const k of engine.active||[]) {
        const [x,y]=coords(k),id=get(x,y),def=defs[id],output=getMeta(x,y)?.outputs;
        if (!def?.producer||!output||!withinSurvey(field,x,y)||occupiedByJob(drone,'output',x,y)) continue;
        const item=Object.keys(output).find(key=>output[key]>0);
        if (!item) continue;
        const port=nearDevice(navigation,{x,y}),score=distance(drone.x,drone.y,x+.5,y+.5)+1;
        if (port&&(!best||score<best.score)) best={node:port.node,score,job:{kind:'output',x,y,item}};
      }
      return best;
    }

    function findDefense(navigation,home,field) {
      const targets=enemies().filter(enemy=>enemy.hp>0&&distance(home.x,home.y,enemy.x/tile,enemy.y/tile)<=field.radius+5);
      for (const node of navigation.nodes) {
        const [x,y]=coords(node),at=targets.find(enemy=>distance(x+.5,y+.5,enemy.x/tile,enemy.y/tile)<=5&&
          lineOfSight(x+.5,y+.5,enemy.x/tile,enemy.y/tile));
        if (at) return {node,job:{kind:'defense',id:at.id}};
      }
      return null;
    }

    function recharge(drone,home,dt) {
      if (!deviceOnline(home)) { drone.reason='Connect dock power and enable its control signal.';return; }
      const extra=devices.some(device=>device.def.droneRole==='charger'&&deviceOnline(device)&&distance(home.x,home.y,device.x,device.y)<=5);
      drone.charge=Math.min(battery,drone.charge+dt*(extra?39:19));
      if (drone.role==='defense'&&drone.rounds<ammunition) {
        const stock=home.data.contents||={},loaded=Math.min(ammunition-drone.rounds,stock.ammo||0);
        if (loaded>0) { drone.rounds+=loaded;stock.ammo-=loaded;if (!stock.ammo) delete stock.ammo; }
      }
    }

    function selectJob(drone,home) {
      const zone=field(home,drone),nav=search(drone,home,zone.radius);
      if (!nav) return retirePlan(drone,'waiting','Drone is enclosed. Clear one connecting tile.');
      drone.reason='';
      if (cargoSize(drone)&&(drone.returnRequested||drone.charge<=reserve||cargoSize(drone)>=capacity||!deviceOnline(home))) return deliver(drone,home,nav);
      if (drone.recalled) {
        if (cargoSize(drone)) return deliver(drone,home,nav);
        if (drone.phase!=='docked') return returnHome(drone,home,nav);
        drone.reason='Recalled. Resume at the dock.';drone.wait=.5;return;
      }
      if (!deviceOnline(home)||drone.charge<=(drone.phase==='docked'?dispatchCharge:reserve)) {
        if (drone.phase!=='docked') {
          if (deviceOnline(home)&&(drone.role!=='defense'||drone.rounds)&&visitCharger(drone,home,nav)) return true;
          return returnHome(drone,home,nav);
        }
        drone.reason=!deviceOnline(home)?'Connect dock power and enable its control signal.':'Charging';drone.wait=.4;return;
      }
      if (drone.role!=='defense'&&!pairedBins(home,drone.role).some(bin=>room(bin)>0&&nearDevice(nav,bin))) {
        if (drone.phase!=='docked'&&cargoSize(drone)) return deliver(drone,home,nav);
        const bins=pairedBins(home,drone.role);
        drone.reason=bins.length?(bins.some(bin=>room(bin)>0)?'Clear a flight path to the collection bin.':'Take contents from the full collection bin.'):
          'Place '+(drone.role==='miner'?'an Ore Collection Bin':'a Salvage Collection Bin')+' nearby.';
        if (drone.phase!=='docked') return returnHome(drone,home,nav);
        drone.wait=.6;return;
      }
      if (drone.role==='defense'&&!drone.rounds) {
        if (drone.phase!=='docked') return returnHome(drone,home,nav);
        drone.reason='Load turret rounds into the dock.';drone.wait=.5;return;
      }
      const next=drone.role==='miner'?findMine(drone,nav,zone):drone.role==='collector'?findCargo(drone,nav,zone):findDefense(nav,home,zone);
      if (next) return planPath(drone,nav,next.node,next.job,'outbound');
      if (cargoSize(drone)) return deliver(drone,home,nav);
      if (drone.phase!=='docked') return returnHome(drone,home,nav);
      drone.reason=drone.role==='miner'?'No exposed mineable tiles in range. Supply a powered survey beacon near a vein.':drone.role==='collector'?'Waiting for loose items or machine output.':'Patrolling. Waiting for nearby hostiles.';
      drone.wait=.65;
    }

    function move(drone,dt) {
      let step=roles[drone.role].speed*(drone.charge>0?1:.52)*dt;
      while (drone.path.length&&step>0) {
        const [x,y]=drone.path[0];
        if (!traversable(Math.floor(x),Math.floor(y))) { retirePlan(drone,'waiting','Flight path blocked.');return; }
        const dx=x-drone.x,dy=y-drone.y,length=Math.hypot(dx,dy);
        if (Math.abs(dx)>.01) drone.facing=Math.sign(dx);
        if (length<=step) { drone.x=x;drone.y=y;step-=length;drone.path.shift(); }
        else { drone.x+=dx/length*step;drone.y+=dy/length*step;step=0; }
      }
      drone.charge=Math.max(0,drone.charge-dt*3.1);
    }

    function unload(drone,depot) {
      if (!depot||depot.def.droneRole!=='depot'||depot.def.droneCargo&&depot.def.droneCargo!==roles[drone.role].cargo) return;
      const stock=depot.data.contents||(depot.data.contents={});let space=room(depot);
      for (const [key,n] of Object.entries(drone.cargo)) {
        const amount=Math.min(n,space);if (!amount) break;
        stock[key]=(stock[key]||0)+amount;drone.cargo[key]-=amount;space-=amount;
        if (!drone.cargo[key]) delete drone.cargo[key];
        stats.droneDelivered=(stats.droneDelivered||0)+amount;
      }
    }

    function carry(drone,key,n) {
      const count=Math.min(n,capacity-cargoSize(drone));
      if (count>0) drone.cargo[key]=(drone.cargo[key]||0)+count;
      return count;
    }

    function completeJob(drone,home,dt) {
      const job=drone.job;
      if (!job) return retirePlan(drone);
      switch (job.kind) {
        case 'dock':
          if (distance(drone.x,drone.y,home.x+.5,home.y+.5)<=1.2) {
            drone.returnRequested=false;return retirePlan(drone,'docked');
          }
          return retirePlan(drone);
        case 'depot':
          unload(drone,deviceAt(job.x,job.y));
          drone.returnRequested=true;return retirePlan(drone);
        case 'charger': {
          const charger=deviceAt(job.x,job.y);
          if (charger?.def.droneRole!=='charger'||!deviceOnline(charger)||drone.recalled) return retirePlan(drone);
          drone.phase='charging';drone.reason='Charging at a field pad.';
          drone.charge=Math.min(battery,drone.charge+dt*38);
          if (drone.charge>=dispatchCharge+10) return retirePlan(drone);
          return;
        }
        case 'mine': {
          if (!eligibleTerrain(job.x,job.y)||get(job.x,job.y)!==job.tile||cargoSize(drone)>=capacity) return retirePlan(drone);
          drone.phase='working';drone.work+=dt;drone.charge=Math.max(0,drone.charge-dt*4);
          if (drone.work<(defs[job.tile].hard||1)*.58) return;
          set(job.x,job.y,T.AIR);
          if (get(job.x,job.y)===T.AIR) { carry(drone,defs[job.tile].drop,1);stats.automined=(stats.automined||0)+1;stats.droneMined=(stats.droneMined||0)+1; }
          return retirePlan(drone);
        }
        case 'drop': {
          const drop=engine.drops.find(entry=>entry.id===job.id);
          if (drop&&distance(drone.x,drone.y,drop.x,drop.y)<=1.1&&lineOfSight(drone.x,drone.y,drop.x,drop.y)) {
            drop.count-=carry(drone,drop.key,drop.count);
            if (drop.count<=0) engine.drops=engine.drops.filter(entry=>entry!==drop);
          }
          return retirePlan(drone);
        }
        case 'output': {
          const output=getMeta(job.x,job.y)?.outputs;
          if (defs[get(job.x,job.y)]?.producer&&(output?.[job.item]||0)>0) {
            output[job.item]-=carry(drone,job.item,output[job.item]);
            if (!output[job.item]) delete output[job.item];
          }
          return retirePlan(drone);
        }
        case 'defense': {
          const enemy=enemies().find(entry=>entry.id===job.id&&entry.hp>0);
          if (!enemy||!drone.rounds||distance(drone.x,drone.y,enemy.x/tile,enemy.y/tile)>5.5||!lineOfSight(drone.x,drone.y,enemy.x/tile,enemy.y/tile)) return retirePlan(drone);
          drone.phase='working';drone.work=Math.max(0,drone.work-dt);
          if (drone.work>0) return;
          drone.work=.42;drone.rounds--;drone.charge=Math.max(0,drone.charge-3);
          drone.facing=Math.sign(enemy.x/tile-drone.x)||drone.facing;
          shots.push({x:drone.x,y:drone.y,tx:enemy.x/tile,ty:enemy.y/tile,left:.12});
          hit(enemy,12);stats.droneShots=(stats.droneShots||0)+1;
          if (!drone.rounds) { drone.returnRequested=true;return retirePlan(drone); }
          return;
        }
        default:return retirePlan(drone);
      }
    }

    function tick(dt) {
      if (!drones.length) return;
      deviceClock-=dt;syncDevices();
      for (let i=shots.length-1;i>=0;i--) if ((shots[i].left-=dt)<=0) shots.splice(i,1);
      for (const drone of drones) {
        const home=homeOf(drone);
        if (!home) { retirePlan(drone,'waiting','Restore the missing dock to recover this drone.');continue; }
        if (drone.phase==='docked') recharge(drone,home,dt);
        if ((drone.phase==='outbound'||drone.phase==='working')&&(!deviceOnline(home)||drone.recalled||drone.charge<=reserve)) {
          drone.returnRequested=true;retirePlan(drone);
        }
        if (drone.path.length) move(drone,dt);
        if (!drone.path.length&&drone.job) completeJob(drone,home,dt);
        if (drone.job) continue;
        drone.wait-=dt;
        if (drone.wait<=0) selectJob(drone,home);
      }
    }

    function deploy(x,y,role) {
      const dock=deviceAt(x,y);role=role||dock?.def.droneSpecialty;
      if (!dock||dock.def.droneRole!=='dock'||!roles[role]||dock.def.droneSpecialty&&dock.def.droneSpecialty!==role) return false;
      if (!deviceOnline(dock)) {notify('Power and enable the drone dock first.');return false;}
      if (drones.filter(drone=>belongs(drone,dock)).length>=(dock.def.droneSlots||3)) {notify('Drone dock slots are full.');return false;}
      const part=roles[role].item;
      if ((inventory[part]||0)<1) {notify('Craft a '+roles[role].label+' Drone first.');return false;}
      const port=airPort(dock);
      if (!port) {notify('Clear an air tile beside the drone dock.');return false;}
      inventory[part]--;
      drones.push({id:nextDrone++,role,home:{x,y,node:dock.node},x:port[0]+.5,y:port[1]+.5,facing:1,
        phase:'docked',charge:battery,rounds:0,cargo:{},path:[],job:null,wait:0,work:0,reason:'',recalled:false,returnRequested:false});
      syncDevices(true);notify(roles[role].label+' Drone commissioned.');save();return true;
    }

    function command(x,y,recalled) {
      const dock=deviceAt(x,y);
      if (!dock) return false;
      const owned=drones.filter(drone=>belongs(drone,dock));
      if (!owned.length) return false;
      for (const drone of owned) { drone.recalled=recalled;drone.returnRequested=recalled;drone.wait=0; }
      notify(recalled?'Recall sent. Drones deposit cargo before returning.':'Drone dispatch resumed.');save();return true;
    }

    function retrieve(x,y,role) {
      const dock=deviceAt(x,y);
      if (!dock) return false;
      const drone=drones.find(entry=>belongs(entry,dock)&&(!role||entry.role===role)&&entry.phase==='docked'&&!cargoSize(entry));
      if (!drone) {notify('Recall a drone and wait for it to finish docking.');return false;}
      if (inventorySpace()<1+drone.rounds) {notify('Make backpack room for the drone and its unused rounds.');return false;}
      const item=roles[drone.role].item;
      inventory[item]=(inventory[item]||0)+1;
      if (drone.rounds) inventory.ammo=(inventory.ammo||0)+drone.rounds;
      drones=drones.filter(entry=>entry!==drone);notify(roles[drone.role].label+' Drone recovered.');save();return true;
    }

    function describe(x,y) {
      const device=deviceAt(x,y);
      if (!device) return null;
      const {def,data}=device;
      if (def.droneRole==='depot') {
        const free=room(device),limit=def.inventoryCapacity||128;
        const supply=def.droneCargo==='ammo',used=(limit-free)+' / '+limit+' items.';
        return {status:supply?'Defense supply':free?'Collection bin ready':'Collection bin full',
          detail:used+(supply?' Route turret rounds into the Defense Drone Dock.':' Take contents to move goods into your backpack.'),counts:{...data.contents}};
      }
      if (def.droneRole!=='dock') return {status:deviceOnline(device)?'Online':'Needs power',detail:def.droneRole==='beacon'?'Marks an area for nearby drones to search.':'Drones can land beside this charging pad to refill their batteries.'};
      const roster=drones.filter(drone=>belongs(drone,device));
      const deployedRows=roster.map(drone=>({id:drone.id,role:drone.role,name:roles[drone.role].label,phase:drone.phase,
        charge:Math.round(drone.charge),cargo:cargoSize(drone),counts:{...drone.cargo},rounds:drone.rounds,reason:drone.reason}));
      const role=def.droneSpecialty,available=role?(inventory[roles[role].item]||0):0;
      const status=deviceOnline(device)?(roster.length?'Dock online':'Ready to deploy'):'Needs power';
      return {status,detail:roster.find(drone=>drone.reason)?.reason||roster.length+' / '+(def.droneSlots||3)+' drones commissioned.',
        role,deployed:roster.length,capacity:def.droneSlots||3,ready:roster.filter(drone=>drone.phase==='docked'&&!cargoSize(drone)).length,
        recalled:roster.length>0&&roster.every(drone=>drone.recalled),available,rounds:data.contents?.ammo||0,deployedRows};
    }

    function canDismantle(x,y) {
      const dock=deviceAt(x,y);
      return dock?.def.droneRole==='dock'&&drones.some(drone=>belongs(drone,dock))?'Recall and recover docked drones before dismantling this dock.':null;
    }

    function draw(ctx,camera,viewWidth,viewHeight,tileSize,time,art=globalThis.CircuitArt) {
      for (const shot of shots) {
        ctx.save();ctx.strokeStyle='#ffbba0';ctx.globalAlpha=shot.left/.12;ctx.lineWidth=1.5;
        ctx.beginPath();ctx.moveTo(shot.x*tileSize-camera.x,shot.y*tileSize-camera.y);
        ctx.lineTo(shot.tx*tileSize-camera.x,shot.ty*tileSize-camera.y);ctx.stroke();ctx.restore();
      }
      for (const drone of drones) {
        const x=drone.x*tileSize-camera.x,y=drone.y*tileSize-camera.y;
        if (x<-28||y<-28||x>viewWidth+28||y>viewHeight+28) continue;
        const style=roles[drone.role],bob=Math.sin(time*7+drone.id)*1.1,speed=drone.phase==='docked'?2:9;
        ctx.save();ctx.translate(x,y+bob);ctx.scale(drone.facing,1);
        if (drone.path.length) {
          ctx.fillStyle=style.color;ctx.globalAlpha=.3;ctx.fillRect(-14,-1,7,2);ctx.globalAlpha=1;
        }
        const rendered=art?.drone?.(ctx,style.item,-13,-10,26,20,time*speed+drone.id)||art?.item?.(ctx,style.item,-12,-12,24);
        if (!rendered) {ctx.fillStyle='#283844';ctx.fillRect(-8,-4,16,8);ctx.fillStyle=style.color;ctx.fillRect(2,-2,5,3);}
        if (cargoSize(drone)) {
          const cargo=Object.keys(drone.cargo)[0];
          if (!art?.item?.(ctx,cargo,-4,5,8)) {ctx.fillStyle=style.color;ctx.fillRect(-4,7,8,3);}
        }
        if (drone.phase==='charging'||drone.job?.kind==='mine'&&drone.phase==='working') {
          ctx.fillStyle=drone.phase==='charging'?'#9afde4':style.color;
          for (let i=0;i<3;i++) {
            const phase=(time*8+i/3)%1;
            ctx.globalAlpha=(1-phase)*.85;
            ctx.fillRect((i-1)*(3+phase*7),5+phase*8,2,2);
          }
        }
        ctx.restore();
      }
    }

    function exported() {
      return {nextDrone,drones:drones.map(drone=>({...drone,home:{...drone.home},cargo:{...drone.cargo},path:drone.path.map(point=>[...point]),job:drone.job?{...drone.job}:null}))};
    }

    function restore(data) {
      drones=[];shots.length=0;nextDrone=1;
      if (!Array.isArray(data?.drones)) return;
      const seen=new Set();
      for (const drone of data.drones) {
        if (!roles[drone?.role]||!Number.isSafeInteger(drone.id)||drone.id<=0||seen.has(drone.id)||
          !Number.isFinite(drone.x)||!Number.isFinite(drone.y)||!inside(Math.floor(drone.x),Math.floor(drone.y))||
          !Number.isInteger(drone.home?.x)||!Number.isInteger(drone.home?.y)||!inside(drone.home.x,drone.home.y)) continue;
        const cargo={};let remaining=capacity;
        for (const [key,n] of Object.entries(drone.cargo||{})) {
          if (!/^[a-z][a-z0-9]*$/i.test(key)||!Number.isSafeInteger(n)||n<=0||remaining<=0) continue;
          cargo[key]=Math.min(n,remaining);remaining-=cargo[key];
        }
        const home={...drone.home},dock=deviceAt(home.x,home.y);
        const docked=drone.phase==='docked'&&dock&&distance(drone.x,drone.y,dock.x+.5,dock.y+.5)<=1.2;
        drones.push({id:drone.id,role:drone.role,home,x:drone.x,y:drone.y,facing:drone.facing===-1?-1:1,
          phase:docked?'docked':'waiting',charge:Math.max(0,Math.min(battery,Number(drone.charge)||0)),rounds:Math.max(0,Math.min(ammunition,Math.floor(drone.rounds)||0)),
          cargo,path:[],job:null,wait:0,work:0,reason:'',recalled:!!drone.recalled,returnRequested:!!drone.returnRequested});
        seen.add(drone.id);nextDrone=Math.max(nextDrone,drone.id+1);
      }
      if (Number.isSafeInteger(data.nextDrone)) nextDrone=Math.max(nextDrone,data.nextDrone);
      syncDevices(true);
    }

    return {tick,draw,describe,deploy,retrieve,canDismantle,recall:(x,y)=>command(x,y,true),resume:(x,y)=>command(x,y,false),
      export:exported,restore,reset:()=>{drones=[];shots.length=0;devices=[];nextDrone=1;deviceClock=0;deviceRevision=-1;}};
  }

  return {create};
});
