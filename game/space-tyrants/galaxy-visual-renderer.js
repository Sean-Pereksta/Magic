/* Final canvas compositor. Replaces the accumulated legacy draw wrappers so a
   fleet, station or battle is drawn once. Simulation/update/save functions are
   deliberately untouched. All coordinates below are CSS screen pixels. */
function stxGVLine(a,b,color,alpha=1,width=1,dash=[]){
  ctx.save();ctx.strokeStyle=color;ctx.globalAlpha=alpha;ctx.lineWidth=width;
  ctx.setLineDash(dash);ctx.beginPath();ctx.moveTo(a.x,a.y);ctx.lineTo(b.x,b.y);ctx.stroke();ctx.restore();
}
function stxGVRing(s,r,color,alpha=1,width=1,start=0,end=6.283){
  ctx.save();ctx.strokeStyle=color;ctx.globalAlpha=alpha;ctx.lineWidth=width;
  ctx.beginPath();ctx.arc(s.x,s.y,Math.max(1,r),start,end);ctx.stroke();ctx.restore();
}
function stxGVLabel(text,x,y,color='#b8c9df',priority=0){
  stxGV.labels.push({text:String(text),x,y,color,priority});
}
function stxGVHit(kind,id,s,r,data){stxGV.hits.push({kind,id,x:s.x,y:s.y,r,data})}
function stxGVDrawLabels(){
  const occupied=[];
  for(const l of stxGV.labels.sort((a,b)=>b.priority-a.priority)){
    const width=Math.min(270,l.text.length*5.8+12),height=17;
    const box={x:l.x-width/2,y:l.y-11,w:width,h:height};
    if(box.x<2||box.y<2||box.x+width>innerWidth-2||box.y+height>innerHeight-2)continue;
    if(occupied.some(b=>box.x<b.x+b.w&&box.x+box.w>b.x&&box.y<b.y+b.h&&box.y+box.h>b.y))continue;
    if(!stxGVSpend('labels'))break;
    ctx.save();ctx.fillStyle='rgba(3,9,21,.85)';ctx.fillRect(box.x,box.y,width,height);
    ctx.fillStyle=l.color;ctx.font='600 10px system-ui';ctx.textAlign='center';
    ctx.fillText(l.text,l.x,l.y,width-10);ctx.restore();occupied.push(box);
  }
}
function stxGVDrawPlanet(p){
  const z=state.camera.zoom,s=worldToScreen(p.x,p.y),r=clamp(p.r*z,5,42);
  const selected=stxGVFocused('planet',p.id),development=stxGVDevelopment(p),family=stxGVFamily(p);
  const color=stxGVColor(p.owner),detail=(selected||z>.82)&&(stxGVSpend('planetDetail')||selected);
  const sprite=stxGVPlanetSprite(p),palette=STX_GV_PALETTES[family];
  ctx.save();
  // Atmosphere and ringed families retain their geological color.
  if(r>9&&(detail||stxGV.quality>.7)){
    const g=ctx.createRadialGradient(s.x,s.y,r*.8,s.x,s.y,r*1.3);
    g.addColorStop(0,palette[1]+'00');g.addColorStop(.55,palette[1]+'38');g.addColorStop(1,palette[1]+'00');
    ctx.fillStyle=g;ctx.fillRect(s.x-r*1.3,s.y-r*1.3,r*2.6,r*2.6);
  }
  if(family==='Gas giant'||family==='Exotic'){
    ctx.strokeStyle=palette[2]+'77';ctx.lineWidth=Math.max(1,r*.15);
    ctx.beginPath();ctx.ellipse(s.x,s.y,r*1.7,r*.43,-.4,0,Math.PI);ctx.stroke();
  }
  if(sprite)ctx.drawImage(sprite,s.x-r,s.y-r,r*2,r*2);
  else{const g=ctx.createRadialGradient(s.x-r*.3,s.y-r*.3,0,s.x,s.y,r);g.addColorStop(0,palette[1]);g.addColorStop(1,palette[0]);ctx.fillStyle=g;ctx.beginPath();ctx.arc(s.x,s.y,r,0,6.283);ctx.fill()}
  if(family==='Gas giant'||family==='Exotic'){
    ctx.strokeStyle=palette[2]+'99';ctx.lineWidth=Math.max(1,r*.13);
    ctx.beginPath();ctx.ellipse(s.x,s.y,r*1.7,r*.43,-.4,Math.PI,6.283);ctx.stroke();
  }
  if(p.owner!=null){
    const capture=stxGV.pulses.find(f=>f.kind==='capture'&&f.id===p.id&&f.until>stxGV.time);
    if(capture){
      const progress=clamp((stxGV.time-capture.start)/1.8,0,1);
      stxGVRing(s,r+4,capture.oldColor,1-progress,2.5);
      stxGVRing(s,r+4,color,1,2.5,-Math.PI/2,-Math.PI/2+progress*6.283);
    }else stxGVRing(s,r+3,color,.75,development>=3?2:1.2);
    if(development>=3||p.home){stxGVRing(s,r+8,color,.2,4);stxGVRing(s,r+12,color,.09,5)}
    if(z>.45||selected){
      ctx.strokeStyle=color+'55';ctx.lineWidth=development>=3?1.6:1;
      ctx.beginPath();ctx.ellipse(s.x,s.y,r*(1.65+development*.12),r*.64,-.24,0,6.283);ctx.stroke();
    }
    if(detail||z>.45){
      const lights=Math.min(detail?72:12,Math.floor(3+Math.sqrt(Math.max(0,p.pop||0))*24+Math.min(30,(p.infra?.city||0)*2)));
      const phase=stxGVHash(p.id)%628/100;
      ctx.save();ctx.beginPath();ctx.arc(s.x,s.y,r*.92,0,6.283);ctx.clip();
      for(let i=0;i<lights;i++){
        // Stable world-specific settlement clusters on the night-facing hemisphere.
        const seed=stxGVHash(`${p.id}:city:${Math.floor(i/5)}`),a=(seed%628)/100;
        const cx=.18+((seed>>>8)%55)/100,cy=Math.sin(a)*.6;
        const x=s.x+r*(cx+Math.cos(i*2.399+phase)*.095),y=s.y+r*(cy+Math.sin(i*2.399+phase)*.08);
        ctx.globalAlpha=clamp((p.cityLights??1)*(1-(p.warDamage||0))*(.35+(i%3)*.2),.05,1);
        ctx.fillStyle=i%4===0?color:'#ffd5a1';ctx.fillRect(x,y,i%5===0?2:1.1,1.1);
        if(development>=3&&i%5===0){ctx.globalAlpha=.16;ctx.fillRect(x-2,y,7,.7)}
      }
      ctx.restore();
      if(detail&&['Terran','Ocean','Jungle','Toxic','Ice'].includes(family)){
        ctx.save();ctx.translate(s.x,s.y);ctx.rotate((stxGV.reduced?0:stxGV.time*.018)+phase);
        ctx.strokeStyle=palette[2]+'50';ctx.lineWidth=Math.max(1,r*.05);
        ctx.beginPath();ctx.ellipse(0,-r*.2,r*.84,r*.23,.3,.3,2.8);ctx.stroke();ctx.restore();
      }
      // Activity density reads actual recent trade volume and development.
      const traffic=Math.min(9,Math.floor((p.tradeVolume||0)/4)+Math.floor(Math.sqrt(Math.max(0,p.pop||0))*4)+(p.infra?.shipyard||0)+(p.tradeStation?1:0));
      for(let i=0;i<traffic;i++){
        if(stxGV.budget.civilians<1||!stxGVSpend('orbitTraffic')||!stxGVSpend('civilians'))break;
        const a=(stxGV.reduced?0:stxGV.time*.18)+i*2.1+phase,rr=r*(1.95+(i%2)*.25);
        stxGVShip(s.x+Math.cos(a)*rr,s.y+Math.sin(a)*rr*.47,a+Math.PI/2,1.3,'#efc572','freighter',.6);
      }
    }
    const defense=Math.min(6,(p.infra?.defense||0)+(p.orbitals?.base||0)*2);
    if(defense>0&&(z>.5||selected)){
      for(let i=0;i<Math.min(detail?4:2,Math.ceil(defense/2));i++){
        const a=.5+i*1.8,rr=r+9;
        stxGVRing(s,rr,color,.25+defense*.055,1.5,a,a+.65);
        if(detail){ctx.fillStyle='#18314d';ctx.strokeStyle=color;ctx.fillRect(s.x+Math.cos(a)*rr-2,s.y+Math.sin(a)*rr-2,4,4);ctx.strokeRect(s.x+Math.cos(a)*rr-2,s.y+Math.sin(a)*rr-2,4,4)}
      }
    }
  }
  if(p.home&&p.owner!=null)stxGVIcon('capital',s.x,s.y-r-15,7,color);
  const roles=stxGVRoles(p);
  if(z>.55||selected){
    const role=roles.find(x=>x!=='capital');if(role)stxGVIcon(role,s.x+r+13,s.y,6,color);
    if(p.shortages?.size&&p.owner===0)stxGVIcon('warning',s.x-r-13,s.y,6,'#f0b95f');
  }
  stxGVDrawProjects(p,s,r,selected);
  if(selected){stxGVRing(s,r+16,'#e6f6ff',.85,1,0,.9);stxGVRing(s,r+16,'#e6f6ff',.85,1,Math.PI,Math.PI+.9)}
  if(z>.65||selected||p.home)stxGVLabel(p.name,s.x,s.y+r+26,selected?'#ffffff':color,selected?100:development*5);
  stxGVHit('planet',p.id,s,Math.max(12,r+4),p);ctx.restore();state.stats.visible++;
}
function stxGVDrawProjects(p,s,r,selected){
  if(state.camera.zoom<.7&&!selected)return;
  const q=p.localProject||p.reconstruction||p.buildQueue?.[0]||p.orbitalProject||p.scanProject||p.tradeStationProject;
  if(q)stxGVRing(s,r+7,'#78efff',.85,2,-Math.PI/2,-Math.PI/2+6.283*clamp(q.progress||0,0,1));
  let i=0;
  for(const project of (p.physicalProjects||[])){
    if(project.phase==='operations'||i>=3)continue;
    const a=1.2+i*.85,x=s.x+Math.cos(a)*r*2.4,y=s.y+Math.sin(a)*r*1.1;i++;
    ctx.save();ctx.strokeStyle='#78efff';ctx.setLineDash([2,3]);ctx.strokeRect(x-7,y-5,14,10);ctx.restore();
    stxGVRing({x,y},10,'#78efff',.8,1,-1.57,-1.57+6.283*clamp(project.progress||0,0,1));
  }
}
function stxGVDrawStations(){
  const z=state.camera.zoom,items=[];
  for(const p of state.planets){
    if(!stxGVPointVisible(p,200))continue;
    for(const [i,f] of (p.orbitalFacilities||[]).entries()){
      const pos=stxOLFacilityPosition(p,f,i,stxGV.facilityTime||0);
      items.push({p,f,pos,id:f.id,kind:'facility',type:f.kind,tier:f.tier||1,status:f.hp<=0?'wreck':'operational'});
    }
    // Legacy saves may only have numeric orbitals. Render those too, without
    // drawing a second copy when a physical facility already represents them.
    for(const [kind,value] of [['station',p.orbitals?.station],['military',p.orbitals?.base],['trade',p.tradeStation?.level]]){
      if(!value||(p.orbitalFacilities||[]).some(f=>f.kind===kind))continue;
      const f={id:`legacy:${p.id}:${kind}`,kind,tier:value,name:kind==='trade'?'Trade station':'Orbital '+kind};
      items.push({p,f,pos:stxOLFacilityPosition(p,f,0,stxGV.facilityTime||0),id:f.id,kind:'facility',type:kind,tier:value,status:'operational'});
    }
  }
  for(const b of stxGV.index.bases.values())if(b.status!=='cancelled'&&stxGVPointVisible(b,150))
    items.push({p:b,f:b,pos:b,id:b.id,kind:'base',type:b.type,tier:b.tier||1,status:b.status});
  items.sort((a,b)=>stxGVPriority(b.pos,{selected:stxGVFocused(b.kind,b.id),owner:b.p.owner})-stxGVPriority(a.pos,{selected:stxGVFocused(a.kind,a.id),owner:a.p.owner}));
  for(const item of items){
    const {p,f,pos,id,kind,type,tier,status}=item,selected=stxGVFocused(kind,id);
    if(z<.45&&!selected&&kind!=='base')continue;
    if(!stxGVSpend('stations'))break;
    const s=worldToScreen(pos.x,pos.y),color=stxGVColor(p.owner);
    const size=stxGVStation(s.x,s.y,type,tier,color,status,selected);
    if(type==='sensor'){stxGVRing({x:s.x,y:s.y-size},size*1.3,'#b5a3ef',.8,1,-2.7,-.4)}
    const q=f.upgradeProject||f.project;
    if(q)stxGVRing(s,size*2,'#8cefff',.9,2,-1.57,-1.57+6.283*clamp(q.progress||0,0,1));
    if(f.supplyReadiness!=null&&f.supplyReadiness<.4&&status==='operational')stxGVIcon('warning',s.x,s.y-size-10,6,'#f0b95f');
    if(selected)stxGVRing(s,size*2.2,'#ffffff',.6,1);
    if(selected||z>1.1)stxGVLabel(f.name||type,s.x,s.y+size*2+12,color,selected?95:5);
    stxGVHit(kind,id,s,Math.max(11,size*1.5),{...item});state.stats.visible++;
  }
}
function stxGVFormation(center,power,angle,color,{selected=false,seed=0,travel=false,veterans=0}={}){
  const tier=stxGVTier(power),z=state.camera.zoom,scale=clamp(z,.42,1.5);
  const desired=z<.45?Math.min(tier.count,1+STX_GV_TIERS.indexOf(tier)):
    Math.max(1,Math.round(tier.count*(z<.9?.58:1)*(selected?1:stxGV.quality)));
  const n=Math.min(desired,stxGV.budget.craft),span=tier.span*scale;
  const hullScale=clamp((1.5+STX_GV_TIERS.indexOf(tier)*.48)*scale,1.35,6);
  const groups=Math.ceil(n/8);let extent=span;
  for(let i=0;i<n;i++){
    if(!stxGVSpend('craft'))break;
    const group=Math.floor(i/8),slot=i%8,row=Math.ceil(slot/2),column=[0,-1,1][group%3];
    // Squadron centers have enough separation for their complete escort wings;
    // adjacent groups must not collapse into overlapping capital silhouettes.
    const spacing=Math.max(hullScale*3.6,span*.135),offset=groups>3?span*.63:groups>1?span*.2:0;
    const forward=offset+span*.14-row*spacing-Math.floor(group/3)*span*.84-Math.abs(column)*span*.22;
    const lateral=column*span*.7+(slot%2?1:-1)*row*spacing*.57;
    extent=Math.max(extent,Math.hypot(forward,lateral)+hullScale*3);
    const wobble=stxGV.reduced?0:Math.sin(stxGV.time*1.3+i+seed)*.55;
    const x=center.x+Math.cos(angle)*forward-Math.sin(angle)*(lateral+wobble);
    const y=center.y+Math.sin(angle)*forward+Math.cos(angle)*(lateral+wobble);
    const capital=i===0||i%8===0&&power>=250;
    const hull=capital?tier.hull:['fighter','corvette','frigate','destroyer'][Math.min(3,Math.floor(power/90)+(i%2))];
    stxGVShip(x,y,angle,hullScale*(capital?1:.57),color,hull,travel?2+(selected?1:0):.4);
  }
  if(selected||veterans>.25){
    const count=Math.min(3,Math.floor(veterans*4));
    for(let i=0;i<count;i++)stxGVIcon('reinforcement',center.x+span*.4+i*6,center.y-12,3,'#f6d991');
  }
  if(selected)stxGVRing(center,Math.max(17,extent),'#d8f5ff',.55,1);
  return {span:extent,count:n};
}
function stxGVDrawFleets(){
  const items=[];
  for(const f of state.fleets){
    if(f.destroyed||stxGV.index.busy.has(f.id))continue;
    const pos=stxGVFleetPosition(f);
    if(!stxGVPointVisible(pos,150)||f.owner!==0&&!stxGVDetected(pos))continue;
    const selected=stxGVFocused('fleet',f.id);
    items.push({f,pos,selected,priority:stxGVPriority(pos,{owner:f.owner,power:f.strength,selected})});
  }
  // Unregistered military ships still get a formation; registered transits are
  // represented by the fleet entry and never drawn a second time.
  for(const s of state.ships)if(STX_MILITARY_TYPES.has(s.type)&&!stxGV.index.fleets.has(s.fleetId)&&stxGVShipVisible(s)&&stxGVPointVisible(s,150))
    items.push({f:{id:s.id,name:s.vesselName||'Task force',strength:s.strength,owner:s.owner},pos:{x:s.x,y:s.y,ship:s},selected:false,priority:stxGVPriority(s,{owner:s.owner,power:s.strength})});
  for(const {f,pos,selected} of items.sort((a,b)=>b.priority-a.priority)){
    const s=worldToScreen(pos.x,pos.y),ship=pos.ship;
    const target=ship&&(Number.isFinite(ship.targetX)?{x:ship.targetX,y:ship.targetY}:stxGV.index.planets.get(ship.to));
    const angle=target?Math.atan2(target.y-pos.y,target.x-pos.x):(pos.orbitAngle||stxFleetPhase(f.id))+Math.PI/2;
    // Moving force strength, when present, is the actual deployed strength.
    const power=ship?.strength??f.strength,color=stxGVColor(f.owner);
    const {span,count}=stxGVFormation(s,power,angle,color,{selected,seed:stxGVHash(f.id),travel:!!ship,veterans:f.veterans||0});
    if(count===0){
      // Exhaustion reduces detail, never removes strategic force scale.
      stxGVRing(s,3+STX_GV_TIERS.indexOf(stxGVTier(power))*1.7,color,.8,2);
    }
    if(f.owner===0&&(selected||state.camera.zoom>.6)){
      ctx.save();ctx.setLineDash([2,5]);stxGVRing(s,Math.max(13,span*.35),'#5cf2a2',selected?.9:.3,1);ctx.restore();
    }
    if(selected||power>=250)stxGVLabel(`${Math.round(power)} · ${f.name}`,s.x,s.y-Math.max(19,span*.55),color,selected?98:35);
    stxGVHit('fleet',f.id,s,Math.max(13,span*.95),{f,pos,power});state.stats.visible++;
  }
}
function stxGVDrawCivilianShips(){
  const z=state.camera.zoom;
  const ships=state.ships.filter(s=>!s.stxCancelled&&!s.stxIntercepted&&!STX_MILITARY_TYPES.has(s.type)&&stxGVPointVisible(s,30));
  ships.sort((a,b)=>Number(b.from===state.selected?.id||b.to===state.selected?.id)-Number(a.from===state.selected?.id||a.to===state.selected?.id));
  for(const s of ships){
    const focused=s.from===state.selected?.id||s.to===state.selected?.id;
    if(z<.55&&!focused)continue;
    if(!stxGVSpend('civilians'))break;
    const target=Number.isFinite(s.targetX)?{x:s.targetX,y:s.targetY}:stxGV.index.planets.get(s.to);
    const point=worldToScreen(s.x,s.y),angle=target?Math.atan2(target.y-s.y,target.x-s.x):0,kind=stxGVRouteKind(s);
    const color=kind==='trade'?'#efc572':kind==='logistics'?'#78dae9':'#b6cbe5';
    stxGVShip(point.x,point.y,angle,clamp(1.5*z,1.1,2.6),color,s.type==='tanker'?'tanker':kind==='trade'?'freighter':'transport',1.3);
    state.stats.visible++;
  }
}
function stxGVRouteOnscreen(a,b){
  // Segment bounding box retains routes crossing the viewport even when both
  // endpoints are offscreen (the legacy endpoint-only cull lost these).
  return !(Math.max(a.x,b.x)<0||Math.min(a.x,b.x)>innerWidth||Math.max(a.y,b.y)<0||Math.min(a.y,b.y)>innerHeight);
}
function stxGVDrawRoutes(){
  const mode=state.stxNetworkOverlay,z=state.camera.zoom;
  const routes=stxGVRoutes().map(r=>{
    const focused=stxGVFocused('route',r.id)||r.a.id===state.selected?.id||r.b.id===state.selected?.id||r.fleetId&&stxGVFocused('fleet',r.fleetId);
    return {...r,focused,blocked:stxGVRouteBlocked(r)};
  }).sort((a,b)=>Number(b.focused)-Number(a.focused)||b.ships.length-a.ships.length);
  for(const r of routes){
    const {focused,kind}=r,a=worldToScreen(r.a.x,r.a.y),b=worldToScreen(r.b.x,r.b.y);
    const recent=r.fleetId&&stxGV.pulses.some(f=>f.kind==='departure'&&f.fleetId===r.fleetId&&f.until>stxGV.time);
    if(!stxGVRouteOnscreen(a,b))continue;
    if(kind==='military'&&!focused&&!recent&&mode!=='military')continue;
    if(kind==='logistics'&&!focused&&mode!=='logistics')continue;
    if(kind==='civilian'&&!focused)continue;
    if(kind==='trade'&&!focused&&mode!=='trade'&&(z<.65||r.ships.length<2))continue;
    if(!stxGVSpend('routes'))break;
    const color=kind==='military'?stxGVColor(r.owner):kind==='trade'?'#efc572':kind==='logistics'?'#78dae9':'#9cabce';
    const alpha=focused?.8:mode===kind?.38:.16,dash=kind==='trade'?[5,9]:kind==='civilian'?[1,9]:[];
    if(r.blocked){
      const left={x:a.x+(b.x-a.x)*.46,y:a.y+(b.y-a.y)*.46},right={x:a.x+(b.x-a.x)*.54,y:a.y+(b.y-a.y)*.54};
      stxGVLine(a,left,color,.22,1,[4,10]);stxGVLine(right,b,color,.22,1,[4,10]);
      stxGVIcon(r.blocked==='Embargo'?'embargo':'interruption',(a.x+b.x)/2,(a.y+b.y)/2,7,'#ff7189');
    }else{
      if(kind==='trade'&&focused)stxGVLine(a,b,color,.06,5,dash);
      stxGVLine(a,b,color,alpha,kind==='military'?1.4:kind==='logistics'?.65:1,dash);
      if(kind==='military'){
        const angle=Math.atan2(b.y-a.y,b.x-a.x),p={x:a.x+(b.x-a.x)*.8,y:a.y+(b.y-a.y)*.8};
        stxGVShip(p.x,p.y,angle,2,color,'fighter',0);
        if(r.ships.some(s=>s.reinforcement||s.battleId))stxGVIcon('reinforcement',p.x,p.y-12,6,color);
      }
    }
    const mid={x:(a.x+b.x)/2,y:(a.y+b.y)/2};
    stxGV.hits.push({kind:'route',id:r.id,a,b,x:mid.x,y:mid.y,r:6,data:r});
  }
  // Diplomatic lines are actual active agreements, displayed contextually.
  if(mode==='trade'&&state.selected){
    let count=0;
    for(const agreement of state.rivalDiplomacy?.agreements||[]){
      if(count>=8||agreement.active===false||agreement.expiresAt<=state.simTime||
        ![agreement.by,agreement.with].includes(state.selected.owner))continue;
      const other=agreement.by===state.selected.owner?agreement.with:agreement.by;
      const target=state.planets.find(p=>p.owner===other&&p.home)||state.planets.find(p=>p.owner===other);
      if(!target)continue;
      const a=worldToScreen(state.selected.x,state.selected.y),b=worldToScreen(target.x,target.y);
      if(!stxGVRouteOnscreen(a,b)||!stxGVSpend('routes'))continue;
      count++;const embargo=agreement.kind==='embargo';
      stxGVLine(a,b,embargo?'#ff7189':'#aa9cdf',.3,.8,[1,8]);
      stxGVIcon(embargo?'embargo':'diplomacy',(a.x+b.x)/2,(a.y+b.y)/2,6,embargo?'#ff7189':'#aa9cdf');
    }
  }
}

function stxGVBattlePoint(center,i,count,side,radius,time,remaining){
  const a=(i/Math.max(1,count)-.5)*2.15+(side>0?Math.PI:0);
  const maneuver=stxGV.reduced?0:Math.sin(time*.9+i*2.4)*3;
  // A depleted line falls back. This is an exposed strength ratio, not a
  // prediction of combat outcome or an additional morale simulation.
  const retreat=(1-remaining)*radius*.3;
  return {x:center.x+Math.cos(a)*(radius+retreat)+maneuver,
    y:center.y+Math.sin(a)*radius*.7+(stxGV.reduced?0:Math.cos(time+i)*2)};
}
function stxGVDrawBattle(item,allowance){
  const {b,location,invasion,selected}=item,s=worldToScreen(location.x,location.y),z=state.camera.zoom;
  const power=Math.max(0,b.attackerStrength)+Math.max(0,b.defenderStrength);
  const radius=clamp((45+Math.sqrt(power)*1.9)*z,27,140),time=stxGV.reduced?0:stxGV.time;
  const ac=stxGVColor(b.attacker),dc=stxGVColor(b.defender);
  const ar=clamp(b.attackerStrength/Math.max(1,b.attackerInitial),0,1),dr=clamp(b.defenderStrength/Math.max(1,b.defenderInitial),0,1);
  stxGVIcon(invasion?'invasion':'battle',s.x,s.y-radius-15,selected?10:8,'#ff7890');
  if(invasion){
    stxGVRing(s,clamp(location.r*z,5,42)+19,'#ff627f',.5,2,-.4,2.2);
    stxGVRing(s,clamp(location.r*z,5,42)+19,'#ff627f',.5,2,2.7,5.5);
  }
  if(selected||z>.55)stxGVLabel(`${invasion?'INVASION':'FLEET BATTLE'} · ${location.name}`,s.x,s.y-radius-30,'#ffb4bd',selected?120:50);
  if(allowance<8){
    stxGVRing(s,radius,ac,.6,2,Math.PI/2,Math.PI*1.5);
    stxGVRing(s,radius,dc,.6,2,-Math.PI/2,Math.PI/2);return;
  }
  const usedBefore=stxGV.stats.combat||0;
  let remaining=allowance;
  const spend=()=>{if(remaining<=0||!stxGVSpend('combat'))return false;remaining--;return true};
  const craftSlots=Math.floor(allowance*.48),share=b.attackerStrength/Math.max(1,power);
  const an=b.attackerStrength>0?Math.max(1,Math.round(craftSlots*share)):0;
  const dn=b.defenderStrength>0?Math.max(1,craftSlots-an):0;
  const ap=[],dp=[];
  for(const [side,count,points,ratio,color,strength] of [[1,an,ap,ar,ac,b.attackerStrength],[-1,dn,dp,dr,dc,b.defenderStrength]]){
    for(let i=0;i<count;i++){
      if(!spend())break;
      const point=stxGVBattlePoint(s,i,count,side,radius,time,ratio);points.push(point);
      const tier=stxGVTier(strength),size=clamp((i===0?2.4+STX_GV_TIERS.indexOf(tier)*.4:1.7)*z,1.4,5.8);
      stxGVShip(point.x,point.y,Math.atan2(s.y-point.y,s.x-point.x),size,color,i===0?tier.hull:i%4===0?'destroyer':'fighter',.6);
    }
  }
  const shotSlots=Math.floor(allowance*.25);
  for(let i=0;i<shotSlots&&ap.length&&dp.length;i++){
    if(!spend())break;
    const from=(i%2?ap:dp)[i%(i%2?ap.length:dp.length)],to=(i%2?dp:ap)[(i*3)%(i%2?dp.length:ap.length)];
    const phase=stxGV.reduced?.45:(time*(.9+(i%3)*.2)+i*.231)%1;
    if(phase>.78)continue;
    const end={x:from.x+(to.x-from.x)*phase,y:from.y+(to.y-from.y)*phase};
    const color=i%2?ac:dc;
    if(i%4===0){
      // Curved missiles are a different shape from straight laser fire.
      const lift=Math.sin(phase*Math.PI)*13;end.y-=lift;
      ctx.save();ctx.strokeStyle='#ffd09a';ctx.lineWidth=1;ctx.globalAlpha=.7;
      ctx.beginPath();ctx.moveTo(from.x,from.y);ctx.quadraticCurveTo((from.x+end.x)/2,(from.y+end.y)/2-10,end.x,end.y);ctx.stroke();ctx.restore();
    }else{
      const tail={x:from.x+(to.x-from.x)*Math.max(0,phase-.2),y:from.y+(to.y-from.y)*Math.max(0,phase-.2)};
      stxGVLine(tail,end,color,.8,power>=500&&i%7===0?2.4:1);
    }
    ctx.save();ctx.fillStyle='#effcff';ctx.fillRect(end.x-1,end.y-1,2,2);ctx.restore();
  }
  const effectSlots=Math.floor(allowance*.12),losers=ar<dr?ap:dp;
  for(let i=0;i<effectSlots&&losers.length;i++){
    if(!spend())break;
    const point=losers[(i*3)%losers.length],phase=stxGV.reduced?.4:(time*.8+i*.31)%1;
    if(phase>.6)continue;
    if(i%3===0){stxGVRing(point,4+phase*9,ar<dr?ac:dc,(1-phase)*.8,1.5,-1.2,1.2)}
    else if(i%3===1){
      const r=2+phase*8;ctx.save();ctx.globalAlpha=(1-phase)*.75;ctx.fillStyle='#ffad67';
      ctx.beginPath();ctx.arc(point.x+phase*5,point.y+phase*3,r,0,6.283);ctx.fill();
      ctx.fillStyle='#fff1c5';ctx.beginPath();ctx.arc(point.x+phase*5,point.y+phase*3,r*.35,0,6.283);ctx.fill();ctx.restore();
    }else{
      // A brief drifting broken hull, anchored to an observed damaged side.
      if(Math.min(ar,dr)>.95)continue;
      stxGVLine({x:point.x+phase*9,y:point.y+phase*7},{x:point.x+phase*9+5,y:point.y+phase*7+3},'#ac8790',1-phase,1);
    }
  }
  if(invasion&&remaining>0){
    const pr=clamp(location.r*z,5,42),count=Math.min(5,Math.ceil(allowance*.05));
    const fortified=(location.infra?.defense||0)+(location.orbitals?.base||0)>0;
    for(let i=0;i<count;i++){
      if(!spend())break;
      const phase=stxGV.reduced?.5:(time*.37+i*.23)%1,a=Math.PI+i*.65;
      const from={x:s.x+Math.cos(a)*radius,y:s.y+Math.sin(a)*radius*.6};
      const target={x:s.x+Math.cos(a)*pr*.45,y:s.y+Math.sin(a)*pr*.4};
      const point={x:from.x+(target.x-from.x)*phase,y:from.y+(target.y-from.y)*phase};
      stxGVShip(point.x,point.y,Math.atan2(target.y-from.y,target.x-from.x),1.3,ac,'transport',2);
      if(spend()){
        stxGVLine({x:point.x-5,y:point.y-7},point,'#ffb785',.7,1.5);
        if(phase>.68){
          if(fortified)stxGVRing(s,pr+6,dc,.75,2,a-.2,a+.3);
          else stxGVRing(target,2+(phase-.68)*10,'#ffc78d',.8,2);
        }
      }
    }
  }
  if(selected){
    const y=s.y+radius+19,width=106;
    stxGVLine({x:s.x-width/2,y},{x:s.x-width/2+width*share,y},ac,1,3);
    stxGVLine({x:s.x-width/2+width*share,y},{x:s.x+width/2,y},dc,1,3);
    stxGVLabel(`${Math.round(b.attackerStrength)} vs ${Math.round(b.defenderStrength)} power`,s.x,y+17,'#dce8f6',118);
  }
  stxGV.stats.battles[b.id]=(stxGV.stats.combat||0)-usedBefore;
}
function stxGVDrawBattles(){
  const items=stxGV.index.battles.filter(x=>stxGVPointVisible(x.location,230)).map(item=>{
    const selected=stxGVFocused(item.focusKind||(item.invasion?'planet':'base'),item.location.id)||
      [...(item.b.attackerFleetIds||[]),...(item.b.defenderFleetIds||[])].some(id=>stxGVFocused('fleet',id));
    return {...item,selected,priority:stxGVPriority(item.location,{selected,
      owner:item.b.attacker===0||item.b.defender===0?0:null,
      power:item.b.attackerStrength+item.b.defenderStrength,battle:true})};
  }).sort((a,b)=>b.priority-a.priority);
  for(const item of items){
    const power=item.b.attackerStrength+item.b.defenderStrength;
    const requested=stxGVBattleBudget(power,item.selected);
    stxGVDrawBattle(item,Math.min(stxGV.budget.combat,requested));
  }
}
function stxGVDrawFronts(){
  const z=state.camera.zoom;
  for(const {location,b} of stxGV.index.battles){
    if(!stxGVPointVisible(location,250))continue;
    const s=worldToScreen(location.x,location.y),r=clamp((150+Math.sqrt(b.attackerStrength+b.defenderStrength)*4)*z,45,250);
    const g=ctx.createRadialGradient(s.x,s.y,4,s.x,s.y,r);
    g.addColorStop(0,'#e8436720');g.addColorStop(.45,'#ad36500c');g.addColorStop(1,'#e8436700');
    ctx.save();ctx.fillStyle=g;ctx.fillRect(s.x-r,s.y-r,r*2,r*2);ctx.restore();
  }
  let count=0;
  for(const op of stxGV.index.operations.values()){
    if(count>=24||!stxGVPointVisible(op,op.radius*z))continue;
    count++;const s=worldToScreen(op.x,op.y),radius=op.radius*z;
    stxGVIcon(op.kind==='blockade'?'blockade':'warning',s.x,s.y-35,8,'#ff977d');
    if(state.stxNetworkOverlay==='military'||state.selected?.id===op.targetId){
      ctx.save();ctx.setLineDash([4,9]);stxGVRing(s,radius,'#ff977d',.25,1);ctx.restore();
      stxGVLabel(op.kind==='blockade'?'BLOCKADE':'COMMERCE RAID',s.x,s.y-49,'#ffb59f',55);
    }
  }
}
function stxGVDrawPulses(){
  for(const fx of stxGV.pulses){
    if(fx.until<=stxGV.time||!stxGVPointVisible(fx,100))continue;
    const s=worldToScreen(fx.x,fx.y),progress=clamp((stxGV.time-fx.start)/1.8,0,1);
    // Reduced motion retains the event marker with a fade, without expanding
    // rings, abrupt flashes or continuous particle movement.
    if(!stxGV.reduced){stxGVRing(s,18+progress*65,fx.color,(1-progress)*.5,1.6);stxGVRing(s,12+progress*42,fx.color,(1-progress)*.2,3)}
    if(!['capture','departure'].includes(fx.kind)){
      ctx.save();ctx.globalAlpha=1-progress;
      stxGVIcon(fx.kind,s.x+24,s.y-26,7,fx.color);ctx.restore();
    }
    if(fx.kind==='reinforcement'&&!stxGV.reduced){
      for(let i=0;i<3;i++){
        if(!stxGVSpend('combat'))break;
        stxGVShip(s.x-90+progress*55-i*9,s.y-25+i*8,.3,2,fx.color,i?'fighter':'cruiser',3);
      }
    }
  }
  // Preserve meaningful legacy mandate, launch and trade events within a cap.
  // Old randomly generated battle sparks are represented by the budgeted scene.
  let drawn=0;
  for(const fx of state.effects){
    if(drawn>=16||!['mandate','trade','launch'].includes(fx.type)||!stxGVPointVisible(fx,70))continue;
    drawn++;const s=worldToScreen(fx.x,fx.y),life=clamp(fx.life/Math.max(.01,fx.maxLife),0,1);
    stxGVRing(s,stxGV.reduced?28:12+(1-life)*40,fx.color,life*.6,1.5);
  }
}
function stxGVDrawBackground(){
  const w=innerWidth,h=innerHeight;ctx.setTransform(dpr(),0,0,dpr(),0,0);
  ctx.globalAlpha=1;ctx.globalCompositeOperation='source-over';ctx.setLineDash([]);ctx.shadowBlur=0;
  ctx.fillStyle='#030713';ctx.fillRect(0,0,w,h);drawNebulae();drawStars();
  // Low opacity ownership accents leave geology readable at every zoom.
  if(state.camera.zoom<.75)for(const p of state.planets){
    if(p.owner==null||!stxGVPointVisible(p,120))continue;
    const s=worldToScreen(p.x,p.y),r=(p.home?100:55)*Math.max(.4,state.camera.zoom);
    const color=stxGVColor(p.owner),g=ctx.createRadialGradient(s.x,s.y,5,s.x,s.y,r);
    g.addColorStop(0,color+'16');g.addColorStop(1,color+'00');
    ctx.fillStyle=g;ctx.fillRect(s.x-r,s.y-r,r*2,r*2);
  }
  // Keep exploration routes contextual; their real recruitment progress is
  // already available in the planet inspector.
  for(const p of state.planets){
    const q=p.expansionProject;if(!q||state.selected?.id!==p.id)continue;
    const target=stxGV.index.planets.get(q.targetId);if(!target)continue;
    stxGVLine(worldToScreen(p.x,p.y),worldToScreen(target.x,target.y),'#a9d8b7',.38,1,[2,10]);
  }
  if(state.stxNetworkOverlay==='military')stxDrawScanCoverage();
  // Wreck debris uses a bounded draw list and never modifies the wreck array.
  let wrecks=0;
  for(const wreck of state.wrecks){
    if(wrecks>=32||!stxGVPointVisible(wreck,20)||state.camera.zoom<.6)continue;
    wrecks++;const s=worldToScreen(wreck.x,wreck.y);
    stxGVLine({x:s.x-3,y:s.y-2},{x:s.x+4,y:s.y+2},wreck.color||'#9b7b80',.4,1);
  }
}
function stxGVFindHit(x,y){
  let best=null,bestScore=Infinity;
  for(const hit of stxGV.hits){
    let distance=Math.hypot(x-hit.x,y-hit.y);
    if(hit.kind==='route'){
      const dx=hit.b.x-hit.a.x,dy=hit.b.y-hit.a.y;
      const t=clamp(((x-hit.a.x)*dx+(y-hit.a.y)*dy)/Math.max(1,dx*dx+dy*dy),0,1);
      distance=Math.hypot(x-hit.a.x-dx*t,y-hit.a.y-dy*t);
    }
    if(distance>hit.r)continue;
    const score=distance/Math.max(1,hit.r)+(hit.kind==='route'?2:0);
    if(score<bestScore){best=hit;bestScore=score}
  }
  return best;
}
function stxGVInspection(hit){
  if(!hit)return [];
  const d=hit.data;
  if(hit.kind==='planet'){
    const fleets=state.fleets.filter(f=>f.location===d.id&&!f.destroyed&&!stxGV.index.moving.has(f.id)&&(f.owner===0||stxGVDetected(d)));
    const projects=[d.localProject,d.orbitalProject,d.expansionProject,d.reconstruction,d.buildQueue?.[0]].filter(Boolean).length;
    return [d.name,`${stxGVFamily(d)} · ${['Frontier','Colony','Developed world','Major world','Capital'][stxGVDevelopment(d)]}`,
      `${empire(d.owner)?.name||'Unclaimed'} · ${stxGVRoles(d).join(' / ')||'Undeveloped'}`,
      `Defense ${d.infra?.defense||0} · ${fleets.length} stationed fleets · ${projects} projects`];
  }
  if(hit.kind==='fleet'){
    const {f,pos,power}=d,ship=pos.ship,target=ship&&(stxGV.index.bases.get(ship.deepBaseId)||stxGV.index.planets.get(ship.to));
    return [f.name,`${stxGVTier(power).name} · ${Math.round(power)} power · ${f.status||'In flight'}`,
      ship?`Destination: ${target?.name||'Deep space'}${ship.reinforcement?' · Reinforcement':''}`:pos.planet?.name||pos.base?.name||'On station',
      `${f.flagship?.name||f.flagship||'Flagship'} · ${Math.round((f.veterans||0)*100)}% veterans`,
      f.admiral?.name?`Admiral ${f.admiral.name}`:''];
  }
  if(hit.kind==='route')return [`${d.a.name||'Origin'} → ${d.b.name||'Destination'}`,
    `${d.kind.toUpperCase()} · ${d.blocked||'Active shipping'}`,
    `${[...d.cargo].map(r=>RESOURCE_LABEL[r]||r).join(', ')||'Civilian passage'} · ${d.ships.length} vessels`];
  const f=d.f;return [f.name||d.type,`${d.type} · Tier ${d.tier} · ${d.status}`,
    `${empire(d.p.owner)?.name||'Independent'}${f.supplyReadiness!=null?` · ${Math.round(f.supplyReadiness*100)}% supply`:''}`];
}
function stxGVDrawInspection(){
  const hit=stxGV.hover||stxGV.hits.find(h=>h.kind==='route'&&h.id===stxGV.pickedRoute);
  const lines=stxGVInspection(hit).filter(Boolean);if(!lines.length)return;
  const width=Math.min(330,innerWidth-20),height=lines.length*17+18;
  const x=clamp(hit.x+22,10,innerWidth-width-10),y=clamp(hit.y+20,90,innerHeight-height-65);
  ctx.save();ctx.fillStyle='rgba(4,12,27,.97)';ctx.strokeStyle='#46627e';ctx.lineWidth=1;
  ctx.fillRect(x,y,width,height);ctx.strokeRect(x,y,width,height);ctx.textAlign='left';
  lines.forEach((line,i)=>{ctx.font=`${i?500:700} ${i?10:11}px system-ui`;ctx.fillStyle=i?'#aabfd5':'#f2f7ff';ctx.fillText(line,x+10,y+17+i*17,width-20)});ctx.restore();
  const text=lines.join('. '),description=$('stxGVMapDescription');
  if(description&&description.textContent!==text)description.textContent=text;
}
function stxGVAdapt(ms){
  stxGV.cost=stxGV.cost*.92+ms*.08;
  if(stxGV.cost>18){stxGV.slow++;stxGV.fast=0}else if(stxGV.cost<10){stxGV.fast++;stxGV.slow=0}else{stxGV.slow=stxGV.fast=0}
  if(stxGV.slow>45){stxGV.quality=Math.max(.4,stxGV.quality-.15);stxGV.slow=0}
  if(stxGV.fast>240){stxGV.quality=Math.min(1,stxGV.quality+.1);stxGV.fast=0}
}
draw=function(){
  if(document.hidden)return;
  const now=performance.now(),start=now;
  const dt=stxGV.lastNow==null?0:clamp((now-stxGV.lastNow)/1000,0,.1);stxGV.lastNow=now;
  // Visual time uses real seconds and pauses with the game. It never advances
  // the simulation or multiplies flashes at high game speeds.
  if(state.running&&state.speed>0)stxGV.time+=dt;
  // The original facility-position helper also drives physical ship launches
  // and docking. Keep it untouched; only the drawing clock pauses here.
  if(stxGV.facilityTime==null||state.running&&state.speed>0&&!stxGV.reduced)stxGV.facilityTime=now/1000;
  stxGV.budget={...STX_GV_LIMITS};
  for(const key of ['craft','civilians','orbitTraffic','planetDetail','routes','stations'])stxGV.budget[key]=Math.round(stxGV.budget[key]*stxGV.quality);
  stxGV.stats={battles:{}};stxGV.hits=[];stxGV.labels=[];
  stxGV.index=stxGVIndex();stxGVObserve();state.stats.visible=0;
  stxGVDrawBackground();stxGVDrawFronts();stxGVDrawRoutes();
  const planets=state.planets.filter(p=>stxGVPointVisible(p,150)).sort((a,b)=>
    stxGVPriority(b,{selected:stxGVFocused('planet',b.id),owner:b.owner})-
    stxGVPriority(a,{selected:stxGVFocused('planet',a.id),owner:a.owner}));
  // Keep the highest-priority visible working set resident. An overview with
  // more planets than cache slots must not evict/rebuild sprites every frame.
  stxGV.pinnedSprites=new Set(planets.slice(0,STX_GV_LIMITS.sprites).map(stxGVSpriteKey));
  for(const p of planets)stxGVDrawPlanet(p);
  stxGVDrawStations();stxGVDrawFleets();stxGVDrawCivilianShips();
  stxGVDrawBattles();stxGVDrawPulses();stxGVDrawLabels();
  if(stxGV.pointer&&!drag)stxGV.hover=stxGVFindHit(stxGV.pointer.x,stxGV.pointer.y);
  if(stxGV.pickedRoute&&!stxGV.hits.some(h=>h.kind==='route'&&h.id===stxGV.pickedRoute))stxGV.pickedRoute=null;
  stxGVDrawInspection();stxGVAdapt(performance.now()-start);
  if(!stxGV.hudAt||now-stxGV.hudAt>500){
    stxGV.hudAt=now;const perf=$('perf');
    if(perf)perf.textContent=`${state.stats.visible} visible · ${state.ships.length} ships · ${stxGV.index.battles.length} battles · ${state.wars.filter(w=>w.active).length} wars`;
  }
};

// Use the same presentation positions for locator selection and map hits. If a
// simulation update has just invalidated the index, the original adapter still
// provides the current position until the next draw rebuilds it.
const STX_GV_previousFleetPosition=stxFleetPosition;
stxFleetPosition=function(f){
  return stxGV.index&&stxGV.world===state.planets&&stxGV.lastScan===state.simTime&&stxGV.index.fleets.get(f?.id)===f?
    stxGVFleetPosition(f):STX_GV_previousFleetPosition(f);
};
canvas.addEventListener('pointermove',e=>{stxGV.pointer={x:e.clientX,y:e.clientY};if(drag)stxGV.hover=null});
canvas.addEventListener('pointerleave',()=>{stxGV.pointer=null;stxGV.hover=null});
canvas.addEventListener('pointerdown',e=>{stxGV.pointerDown={x:e.clientX,y:e.clientY}},{capture:true});
window.addEventListener('pointerup',e=>{
  if(e.target!==canvas)return;
  const down=stxGV.pointerDown;stxGV.pointerDown=null;
  if(!down||Math.hypot(e.clientX-down.x,e.clientY-down.y)>6)return;
  const hit=stxGVFindHit(e.clientX,e.clientY);
  if(!['route','fleet','facility'].includes(hit?.kind)){stxGV.pickedRoute=null;return}
  // These hits use the new formation footprint. Stop legacy fleet/planet hit
  // handlers from selecting an unrelated object underneath a large formation.
  e.stopImmediatePropagation();drag=null;stxPointerDown=null;canvas.classList.remove('dragging');
  if(canvas.hasPointerCapture?.(e.pointerId))canvas.releasePointerCapture(e.pointerId);
  if(hit.kind==='route'){stxGV.pickedRoute=hit.id;stxGV.hover=hit}
  else if(hit.kind==='facility'){
    const {p,f,pos}=hit.data;stxGV.pickedRoute=null;state.selected=p;
    state.stxSelectedFacilityId=stxOLFacilityAt(f.id)?f.id:null;
    p.intel=Math.min(1,p.intel+.1);stxFocusPoint(pos.x,pos.y,1.18);
    renderPlanet();$('leftPanel').classList.add('open');showToast(f.name||'Orbital infrastructure');
  }
  else if(stxGV.index.fleets.has(hit.id)){stxGV.pickedRoute=null;stxFocusFleet(hit.id)}
},{capture:true});
canvas.addEventListener('pointercancel',()=>{stxGV.pointerDown=null;stxGV.hover=null});
window.addEventListener('keydown',e=>{if(e.key==='Escape'){stxGV.pickedRoute=null;stxGV.hover=null;stxGV.pointer=null}});
document.addEventListener('visibilitychange',()=>{stxGV.lastNow=null});
if(typeof matchMedia==='function')matchMedia('(prefers-reduced-motion: reduce)').addEventListener?.('change',e=>{stxGV.reduced=e.matches});

function stxGVInstallLegend(){
  const description=document.createElement('div');description.id='stxGVMapDescription';
  description.style.cssText='position:absolute;width:1px;height:1px;overflow:hidden;clip-path:inset(50%)';
  document.body.appendChild(description);canvas.setAttribute('aria-describedby',description.id);
  const controls=$('stxNetworkControls');if(!controls)return;
  const button=document.createElement('button');button.textContent='MAP KEY';button.type='button';
  button.setAttribute('aria-expanded','false');button.setAttribute('aria-controls','stxGVLegend');controls.appendChild(button);
  const legend=document.createElement('section');legend.id='stxGVLegend';legend.hidden=true;
  legend.setAttribute('aria-label','Galaxy map symbols');
  legend.style.cssText='position:fixed;z-index:50;right:12px;bottom:100px;width:375px;max-width:calc(100vw - 48px);max-height:55vh;overflow:auto;padding:16px;background:#091426;border:1px solid #45617d;border-radius:12px;color:#d8e6f5;font:12px/1.7 system-ui';
  const title=document.createElement('strong');title.textContent='GALAXY MAP KEY';legend.appendChild(title);
  const rows=[['capital','Capital / empire core'],['industry','Industry'],['mining','Mining'],['components','Components'],['shipyard','Shipyard'],['logistics','Logistics'],['trade','Trade hub'],['fortress','Fortified world'],['population','Civilian center'],['research','Research'],['battle','Fleet battle'],['invasion','Planetary invasion'],['reinforcement','Reinforcements'],['embargo','Embargo / interrupted route'],['warning','Supply problem'],['diplomacy','Agreement']];
  for(const [icon,label] of rows){
    const row=document.createElement('div'),symbol=document.createElement('canvas');symbol.width=24;symbol.height=22;
    // Symbols share the exact canvas implementation used by the galaxy.
    // The legend uses the same path painter with its own canvas context.
    row.style.cssText='display:inline-flex;width:180px;gap:6px;align-items:center';
    symbol.setAttribute('aria-hidden','true');row.appendChild(symbol);
    const text=document.createElement('span');text.textContent=label;row.appendChild(text);legend.appendChild(row);
    stxGVLegendSymbol(symbol,icon);
  }
  const copy=document.createElement('p');copy.style.cssText='max-width:365px;margin-bottom:0';
  copy.textContent='Military: solid arrows. Trade: gold dashes and cargo ships. Supply: thin cyan lines. Agreements: violet dots. Broken routes show interruptions. Zoom in for detail; tap a route to inspect it. Fleet silhouettes indicate power, not exact ship counts.';
  legend.appendChild(copy);document.body.appendChild(legend);
  button.onclick=()=>{legend.hidden=!legend.hidden;button.setAttribute('aria-expanded',String(!legend.hidden))};
}
function stxGVLegendSymbol(canvas,kind){
  // The map itself creates no DOM objects for individual symbols or effects.
  const target=canvas.getContext('2d');
  stxGVIcon(kind,12,11,7,'#a9d5ed',target);
}
stxGVInstallLegend();
globalThis.SpaceTyrantsVisuals={version:1,limits:STX_GV_LIMITS,
  diagnostics:()=>({quality:stxGV.quality,renderMs:stxGV.cost,spriteCount:stxGV.sprites.size,
    pulseCount:stxGV.pulses.filter(f=>f.until>stxGV.time).length,...stxGV.stats})};
