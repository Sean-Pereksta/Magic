/* Walkable fortification decks. Ground masonry stays solid. */
(function (global) {
  'use strict';
  const D = global.GameData, T = D.TYPES, clamp = D.clamp;
  const DECK = Object.freeze({ type: 'rampart', speed: .65, cover: .55, elevation: 1, blocked: false });
  const inside = (x,y,s,p=0) => x>=s.x+p&&x<=s.x+s.w-p&&y>=s.y+p&&y<=s.y+s.h-p;
  class BattleRamparts {
    constructor(e) { this.e=e; this.revision=-1; this.links=new Map(); this.active=false;this.sequence=0;this.queues=new Map(); }
    rebuildQueues(){this.queues.clear();for(const r of [...this.e.regiments].sort((a,b)=>(a._rampartJob&&a._rampartJob.sequence||0)-(b._rampartJob&&b._rampartJob.sequence||0))){
      if(r._rampartJob&&!r._rampartJob.leaving&&r._rampartJob.queues)for(const[key,ids]of r._rampartJob.queues){if(!this.queues.has(key))this.queues.set(key,[]);this.queues.get(key).push(...ids);}
      if(r._rampartJob&&r._rampartJob.leaving)for(const id of r.unitIds){const u=this.e.units[id];if(!u.alive||!u._rampartAccess||!['exit-deck','descending'].includes(u._rampartStage))continue;const key='out:'+u._rampartAccess.id+':'+u._rampartAccess.lane;if(!this.queues.has(key))this.queues.set(key,[]);this.queues.get(key).push(id);}
    }
      for(const[key,ids]of this.queues)if(key.startsWith('out:'))ids.sort((a,b)=>{const u=this.e.units[a],v=this.e.units[b];return v._rampartRank-u._rampartRank||Math.hypot(u.x-u._rampartAccess.deckX,u.y-u._rampartAccess.deckY)-Math.hypot(v.x-v._rampartAccess.deckX,v.y-v._rampartAccess.deckY)||a-b;});
    }
    decks(team) { return this.e.structures.filter(s=>s.walkable&&!s.breached&&(team===undefined||s.team===team)); }
    rebuild() {
      if(this.revision===this.e.terrainRevision)return;
      this.revision=this.e.terrainRevision;this.links.clear();const ds=this.decks();
      for(const a of ds)this.links.set(a.id,[]);
      for(let i=0;i<ds.length;i++)for(let j=0;j<i;j++){const a=ds[i],b=ds[j];if(a.team!==b.team||Math.abs(a.deckHeight-b.deckHeight)>8)continue;
        if(a.x<=b.x+b.w+.1&&a.x+a.w>=b.x-.1&&a.y<=b.y+b.h+.1&&a.y+a.h>=b.y-.1){this.links.get(a.id).push(b.id);this.links.get(b.id).push(a.id);}}
    }
    component(id) { this.rebuild();const result=[],queue=[id],seen=new Set(queue);while(queue.length){const at=queue.shift(),s=this.e.siege.byId.get(at);if(!s||s.breached||!s.walkable)continue;result.push(s);for(const next of this.links.get(at)||[])if(!seen.has(next)){seen.add(next);queue.push(next);}}return result; }
    route(from,to) { this.rebuild();if(from===to)return[];const queue=[from],prev=new Map([[from,null]]);while(queue.length&&!prev.has(to)){const id=queue.shift();for(const next of this.links.get(id)||[])if(!prev.has(next)){prev.set(next,id);queue.push(next);}}if(!prev.has(to))return null;
      const ids=[];for(let id=to;id!==from;id=prev.get(id))ids.unshift(id);let a=this.e.siege.byId.get(from);const points=[];for(const id of ids){const b=this.e.siege.byId.get(id);const x=(Math.max(a.x,b.x)+Math.min(a.x+a.w,b.x+b.w))/2,y=(Math.max(a.y,b.y)+Math.min(a.y+a.h,b.y+b.h))/2;points.push({x,y,structureId:b.id});a=b;}return points; }
    wallPath(u,slot,access){const start=this.e.siege.byId.get(u._rampartId),raw=this.route(u._rampartId,slot.structureId);if(raw===null)return[];const result=[];let current=start;
      const lane=(p,s)=>{const margin=u._radius+8,point={...p,structureId:s.id};if(s.rampartSide==='west')point.x=s.x+s.w-margin;else if(s.rampartSide==='east')point.x=s.x+margin;else if(s.rampartSide==='north')point.y=s.y+s.h-margin;else if(s.rampartSide==='south')point.y=s.y+margin;return point;};
      if(access&&start){const base=(start.stairs||[]).find(a=>a.id===access.id)||access,first=raw[0]||slot,horizontal=start.w>=start.h,offset=horizontal?access.deckX-base.deckX:access.deckY-base.deckY,preferred=horizontal?first.x-base.deckX:first.y-base.deckY,side=Math.sign(offset)||Math.sign(preferred)||1;
        const exit={x:base.deckX+(horizontal?side*35:0),y:base.deckY+(horizontal?0:side*35),structureId:start.id};result.push(exit,lane(exit,start));}
      for(const p of raw){const next=this.e.siege.byId.get(p.structureId),guide=current.castleTower&&!next.castleTower?next:current;
        // Cross the shared opening on the inner transit lane, clear of guards.
        const q=lane(p,guide),loX=Math.max(current.x,next.x),hiX=Math.min(current.x+current.w,next.x+next.w),loY=Math.max(current.y,next.y),hiY=Math.min(current.y+current.h,next.y+next.h);
        q.x=hiX-loX>u._radius*2?clamp(q.x,loX+u._radius+1,hiX-u._radius-1):(loX+hiX)/2;
        q.y=hiY-loY>u._radius*2?clamp(q.y,loY+u._radius+1,hiY-u._radius-1):(loY+hiY)/2;
        q.structureId=next.id;result.push(q);current=next;}
      if(current)result.push(lane(slot,current));result.push(slot);return result;}
    stairs(decks) { const out=[];for(const s of decks)for(const stair of s.stairs||[])if([stair.groundX,stair.groundY,stair.deckX,stair.deckY].every(Number.isFinite))out.push({...stair,structureId:s.id,height:s.deckHeight});return out; }
    settings(r,options={}) { r.rampartRanks=clamp(Math.floor(Number(options.ranks)||r.rampartRanks||2),1,3);r.rampartSpacing=['compact','normal','wide'].includes(options.spacing)?options.spacing:r.rampartSpacing||'normal'; }
    initializeOrders() { for(const r of this.e.regiments)if(r._initialRampart)this.manRampart([r.id],r._initialRampart.structureId,r._initialRampart); }
    manRampart(ids,structureId,options={}) {
      let accepted=0;this.planningSelection=new Set(ids||[]);
      for(const id of new Set(ids||[])){const r=this.e.getRegiment(id),t=r&&T[r.type];if(!r||r.alive<=0||t.mounted||t.siege||t.commander)continue;
        let wall=this.e.siege.byId.get(structureId);if(!wall){wall=this.decks(r.team).sort((a,b)=>Math.hypot(r.x-a.x-a.w/2,r.y-a.y-a.h/2)-Math.hypot(r.x-b.x-b.w/2,r.y-b.y-b.h/2))[0];}
        if(!wall||!wall.walkable||wall.breached||wall.team!==r.team)continue;
        const component=this.component(wall.id);if(!this.stairs(component).length)continue;
        const deckIds=new Set(component.map(s=>s.id));
        if(r.unitIds.some(id=>{const u=this.e.units[id];return u.alive&&u._rampartZ>0&&!deckIds.has(u._rampartId||u._rampartAccess&&u._rampartAccess.structureId);})){this.leaveRamparts([r.id],false);r._rampartJob.transfer={structureId:wall.id,options:{...options}};r.rampartStatus='Descending before changing castles';accepted++;continue;}
        if(!r.unitIds.some(id=>this.e.units[id]._onRampart)&&!this.stairs(component).some(s=>this.e._findPath(r._centroidX,r._centroidY,s.groundX,s.groundY,t.radius+1).length)){r.rampartStatus='No open route to these stairs';continue;}
        this.settings(r,options);this.e.command([r.id],'hold');r._rampartJob={structureId:wall.id,component:component.map(s=>s.id),leaving:false,courtyard:false,sequence:++this.sequence,bounds:{x:Math.min(...component.map(s=>s.x)),y:Math.min(...component.map(s=>s.y)),right:Math.max(...component.map(s=>s.x+s.w)),bottom:Math.max(...component.map(s=>s.y+s.h))}};r.rampartStatus='Approaching stairs';
        this.planningSelection.delete(r.id);this.assignSlots(r,wall);accepted++;this.active=true;
      }this.planningSelection=null;return accepted;
    }
    assignSlots(r,wall,preview=false,additional=[]) {
      const ds=this.component(wall.id);ds.sort((a,b)=>(a.id===wall.id?-1:b.id===wall.id?1:(a.castleTower?1:0)-(b.castleTower?1:0)||(a.rampartSide===wall.rampartSide?0:1)-(b.rampartSide===wall.rampartSide?0:1)||Math.hypot(a.x-wall.x,a.y-wall.y)-Math.hypot(b.x-wall.x,b.y-wall.y)));
      const t=T[r.type],pitch=Math.max(t.radius*2+3,r.rampartSpacing==='compact'?15:r.rampartSpacing==='wide'?27:20),slots=[];
      const reserved=[...additional];for(const other of this.e.regiments)if(other.id!==r.id&&other._rampartJob&&!other._rampartJob.leaving&&!(this.planningSelection&&this.planningSelection.has(other.id)))reserved.push(...other._rampartSlots);
      const landings=this.stairs(ds);
      outer:for(const s of ds){const horizontal=s.w>=s.h,thickness=horizontal?s.h:s.w,length=horizontal?s.w:s.h;
        const ranks=Math.max(1,Math.min(r.rampartRanks,Math.floor((thickness-2*t.radius-28)/pitch)+1));
        const files=Math.max(1,Math.floor((length-80)/pitch)+1),shift=Math.max(0,(ranks-1)*pitch/2+(t.radius*2+4)+(t.radius+8)-thickness/2),outward=s.rampartSide==='west'||s.rampartSide==='north'?-1:1;
        const alongs=Array.from({length:files},(_,file)=>(file-(files-1)/2)*pitch),localStairs=landings.filter(a=>a.structureId===s.id);
        const distance=v=>localStairs.length?Math.min(...localStairs.map(a=>Math.abs(v-(horizontal?a.deckX-s.x-s.w/2:a.deckY-s.y-s.h/2)))):Math.abs(v);
        alongs.sort((a,b)=>distance(a)-distance(b)||a-b);
        for(let row=0;row<ranks;row++){const quota=Math.ceil((r.alive-slots.length)/(ranks-row));let count=0;for(let file=0;file<files&&count<quota;file++){const across=clamp((row-(ranks-1)/2)*pitch+shift*outward,-thickness/2+t.radius+2,thickness/2-t.radius-2),along=alongs[file];
          const p={x:s.x+s.w/2+(horizontal?along:across),y:s.y+s.h/2+(horizontal?across:along),z:s.deckHeight,structureId:s.id,rank:row};
          if(landings.some(a=>Math.hypot(a.deckX-p.x,a.deckY-p.y)<50)||reserved.some(q=>Math.hypot(q.x-p.x,q.y-p.y)<t.radius*2+3)||slots.some(q=>Math.hypot(q.x-p.x,q.y-p.y)<t.radius*2+3))continue;slots.push(p);count++;if(slots.length>=r.alive)break outer;
        }}}
      if(preview)return slots.slice(0,r.alive);
      r._rampartSlots=slots.slice(0,r.alive);let index=0;const stairs=this.stairs(ds),loads=new Map();r._rampartJob.queues=new Map();
      for(const id of r.unitIds){const u=this.e.units[id];if(!u.alive||u.routed)continue;u._rampartSlot=index;const slot=r._rampartSlots[index++];
        if(!slot){u._rampartStage='waiting';continue;}u._rampartRank=slot.rank;
        if(u._onRampart){u._rampartPath=this.wallPath(u,slot);continue;}
        if(u._rampartStage==='climbing'&&ds.some(s=>s.id===u._rampartAccess.structureId)){const key=u._rampartAccess.id+':'+u._rampartAccess.lane;if(!r._rampartJob.queues.has(key))r._rampartJob.queues.set(key,[]);r._rampartJob.queues.get(key).push(u.id);continue;}
        const choices=stairs.map(stair=>({stair,path:this.e._findPath(u.x,u.y,stair.groundX,stair.groundY,u._radius+1)})).filter(v=>v.path.length);
        const accessCost=v=>Math.hypot(u.x-v.stair.groundX,u.y-v.stair.groundY)+Math.hypot(slot.x-v.stair.deckX,slot.y-v.stair.deckY)*3+(loads.get(v.stair.id)||0)*3;
        choices.sort((a,b)=>accessCost(a)-accessCost(b));
        const choice=choices[0];if(!choice){u._rampartStage='waiting';continue;}
        const n=loads.get(choice.stair.id)||0;loads.set(choice.stair.id,n+1);const lanes=Math.max(1,Math.min(2,Math.floor((choice.stair.width-2*u._radius)/20)+1)),side=choice.stair.axis==='x'?slot.y-choice.stair.deckY:slot.x-choice.stair.deckX,lane=lanes===1?0:side<0?0:1,offset=(lane-(lanes-1)/2)*24;
        const stair={...choice.stair,lane};if(stair.axis==='x'){stair.groundY+=offset;stair.deckY+=offset;}else{stair.groundX+=offset;stair.deckX+=offset;}
        const key=stair.id+':'+lane;if(!r._rampartJob.queues.has(key))r._rampartJob.queues.set(key,[]);r._rampartJob.queues.get(key).push(u.id);
        u._rampartAccess=stair;u._rampartGroundPath=choice.path;u._rampartStage='approach';u._rampartClimb=0;
      }
      r.rampartStatus=slots.length<r.alive?'Man ramparts · overflow waits in court':'Man ramparts';
      this.rebuildQueues();
    }
    preview(ids,structureId,options={}) {const wall=this.e.siege.byId.get(structureId);if(!wall||!wall.walkable||wall.breached)return[];const result=[],prior=this.planningSelection;this.planningSelection=new Set(ids||[]);for(const id of ids||[]){const r=this.e.getRegiment(id);if(!r||r.team!==wall.team)continue;const copy={...r};this.settings(copy,options);result.push(...this.assignSlots(copy,wall,true,result));}this.planningSelection=prior;return result;}
    setRampartFormation(ids,options) { let n=0;for(const id of new Set(ids||[])){const r=this.e.getRegiment(id);if(!r)continue;this.settings(r,options);if(r._rampartJob&&!r._rampartJob.leaving){const s=this.e.siege.byId.get(r._rampartJob.structureId);if(s&&!s.breached)this.assignSlots(r,s);}n++;}return n; }
    courtSlots(r,wall){const e=this.e,t=T[r.type],n=r.unitIds.filter(id=>e.units[id].alive).length,pitch=Math.max(30,t.radius*4+8),cols=Math.ceil(Math.sqrt(n*1.4)),rows=Math.ceil(n/cols),w=cols*pitch,h=rows*pitch,b=r._rampartJob.bounds;
      const access=this.stairs(this.decks(r.team)).find(s=>r._rampartJob.component.includes(s.structureId)),base=access?{x:access.groundX,y:access.groundY}:{x:r.x,y:r.y};
      const cx=wall&&Number.isFinite(wall.courtyardX)?wall.courtyardX:base.x,cy=wall&&Number.isFinite(wall.courtyardY)?wall.courtyardY:base.y,reserved=[];
      for(const other of e.regiments)if(other!==r&&other._rampartJob&&other._rampartJob.courtSlots)reserved.push(...other._rampartJob.courtSlots);
      const accesses=this.stairs(this.decks(r.team));
      const free=p=>{if(e._bodyBlocked(p.x,p.y,t.radius+2)||accesses.some(s=>inside(p.x,p.y,s,-t.radius-35))||reserved.some(q=>Math.hypot(p.x-q.x,p.y-q.y)<t.radius+q.radius+9))return false;
        const padding=t.radius+e._maxRadius+9,x0=Math.max(0,Math.floor((p.x-padding)/32)),x1=Math.min(e._bodyCols-1,Math.floor((p.x+padding)/32)),y0=Math.max(0,Math.floor((p.y-padding)/32)),y1=Math.min(e._bodyRows-1,Math.floor((p.y+padding)/32));
        for(let team=0;team<2;team++)for(let y=y0;y<=y1;y++)for(let x=x0;x<=x1;x++)for(let id=e._bodyHeads[team][y*e._bodyCols+x];id!==-1;id=e._bodyNext[id]){const u=e.units[id];if(u.regiment!==r.id&&u.alive&&!u.routed&&u._rampartZ<8&&Math.hypot(u.x-p.x,u.y-p.y)<u._radius+t.radius+8)return false;}return true;};
      for(let ring=0;ring<14;ring++)for(let oy=-ring;oy<=ring;oy++)for(let ox=-ring;ox<=ring;ox++){if(ring&&Math.max(Math.abs(ox),Math.abs(oy))!==ring)continue;const x=cx+ox*(w+35),y=cy+oy*(h+35);if(b&&(x-w/2<b.x+100||x+w/2>b.right-100||y-h/2<b.y+100||y+h/2>b.bottom-100))continue;
        const slots=Array.from({length:n},(_,i)=>({x:x+(i%cols-(cols-1)/2)*pitch,y:y+(Math.floor(i/cols)-(rows-1)/2)*pitch,radius:t.radius}));
        if(slots.every(free)&&e._findPath(base.x,base.y,x,y,t.radius+1).length){slots.sort((a,b)=>Math.hypot(b.x-base.x,b.y-base.y)-Math.hypot(a.x-base.x,a.y-base.y));r._rampartJob.courtSlots=slots;return slots;}}
      const fallback=Array.from({length:n},(_,i)=>{const p=e._nearestPassable(base.x+(i%cols-(cols-1)/2)*pitch,base.y+100+Math.floor(i/cols)*pitch,t.radius+1);return{...p,radius:t.radius};});r._rampartJob.courtSlots=fallback;return fallback;
    }
    leaveRamparts(ids,courtyard=true) { let n=0;for(const id of new Set(ids||[])){const r=this.e.getRegiment(id);if(!r||!r._rampartJob)continue;r._rampartJob.leaving=true;r._rampartJob.courtyard=courtyard;r.rampartStatus=courtyard?'Reforming in courtyard':'Leaving ramparts';
      const wall=this.e.siege.byId.get(r._rampartJob.structureId),cx=wall&&wall.courtyardX,cy=wall&&wall.courtyardY;
      const court=this.courtSlots(r,wall);
      let i=0;for(const uid of r.unitIds){const u=this.e.units[uid];if(!u.alive)continue;
        const ds=u._onRampart?this.component(u._rampartId):this.decks(r.team),choices=this.stairs(ds).map(stair=>({stair,route:u._onRampart?this.route(u._rampartId,stair.structureId):[]})).filter(v=>v.route!==null);
        choices.sort((a,b)=>Math.hypot(u.x-a.stair.deckX,u.y-a.stair.deckY)-Math.hypot(u.x-b.stair.deckX,u.y-b.stair.deckY));const choice=choices[0];
        if(u._onRampart&&choice){const stair={...choice.stair},side=stair.axis==='x'?u.y-stair.deckY:u.x-stair.deckX,offset=(side<0?-1:1)*12;stair.lane=side<0?0:1;if(stair.axis==='x'){stair.groundY+=offset;stair.deckY+=offset;}else{stair.groundX+=offset;stair.deckX+=offset;}u._rampartAccess=stair;u._rampartPath=this.wallPath(u,{x:stair.deckX,y:stair.deckY,structureId:stair.structureId});u._rampartStage='exit-deck';}
        else if(u._rampartStage==='climbing'){u._rampartStage='descending';}
        else if(!u._onRampart&&u._rampartStage!=='falling')u._rampartStage='courtyard';
        const base=choice?{x:choice.stair.groundX,y:choice.stair.groundY}:{x:u.x,y:u.y};
        let goal=court[i]||base;
        if(!this.e._findPath(base.x,base.y,goal.x,goal.y,u._radius+1).length)goal=this.e._nearestPassable(base.x+(i%8-3.5)*22,base.y+100+Math.floor(i/8)*22,u._radius+1);
        u._rampartCourtGoal=goal;i++;
      }n++;}this.rebuildQueues();return n; }
    onCommand(r,order) { if(r._rampartJob&&order!=='hold'){if(!r._rampartJob.leaving)this.leaveRamparts([r.id],false);r._rampartJob.courtyard=false;r._rampartJob.transfer=null;} }
    isSupported(u) { if(u._rampartStage==='falling')return true;if(u._rampartStage==='climbing'||u._rampartStage==='descending')return !!u._rampartAccess&&inside(u.x,u.y,u._rampartAccess,u._radius-.1);if(!u._onRampart)return !this.e._bodyBlocked(u.x,u.y,u._radius);return this.deckClear(u.x,u.y,u._radius,u._rampartZ,u.team); }
    deckClear(x,y,r,z,team) { const ds=this.decks(team);for(let i=-1;i<12;i++){const a=i/12*Math.PI*2,px=i<0?x:x+Math.cos(a)*r,py=i<0?y:y+Math.sin(a)*r;if(!ds.some(s=>Math.abs(s.deckHeight-z)<9&&inside(px,py,s)))return false;}return true; }
    deckMove(u,goal,dt) {
      const dx=goal.x-u.x,dy=goal.y-u.y,d=Math.hypot(dx,dy);if(d<1){u._vx=u._vy=0;return true;}const amount=Math.min(d,T[u.type].speed*.55*dt),m=this.e._collideMovement(u,dx/d*amount,dy/d*amount,amount);
      if(!this.deckClear(u.x+m.x,u.y+m.y,u._radius,u._rampartZ,u.team)){u._vx=u._vy=0;return false;}
      u.x+=m.x;u.y+=m.y;u._vx=m.x/dt;u._vy=m.y/dt;u.angle=Math.atan2(dy,dx);u.state='move';u.travelDistance+=Math.hypot(m.x,m.y);this.e.impacts.recordMove(u,Math.hypot(m.x,m.y));return d<=amount+1;
    }
    stairMove(u,dt,descending) {
      const s=u._rampartAccess;if(!s){u._rampartStage='waiting';return;}
      const length=Math.hypot(s.deckX-s.groundX,s.deckY-s.groundY),sign=descending?-1:1;
      const desired=clamp(u._rampartClimb+sign*T[u.type].speed*.4*dt/Math.max(1,length),0,1);
      let tx=s.groundX+(s.deckX-s.groundX)*desired,ty=s.groundY+(s.deckY-s.groundY)*desired;const oldZ=u._rampartZ,distance=Math.hypot(tx-u.x,ty-u.y),limit=T[u.type].speed*.8*dt;
      if(distance>limit){tx=u.x+(tx-u.x)/distance*limit;ty=u.y+(ty-u.y)/distance*limit;}
      const progress=clamp(((tx-s.groundX)*(s.deckX-s.groundX)+(ty-s.groundY)*(s.deckY-s.groundY))/Math.max(1,length*length),0,1);
      u._rampartZ=s.height*progress;const amount=Math.hypot(tx-u.x,ty-u.y),m=this.e._collideMovement(u,tx-u.x,ty-u.y,amount);
      if(Math.hypot(m.x-(tx-u.x),m.y-(ty-u.y))>Math.max(.05,amount*.1)){u._rampartZ=oldZ;u._vx=u._vy=0;return;}
      u.x=tx;u.y=ty;u._rampartClimb=progress;u._vx=m.x/dt;u._vy=m.y/dt;u.angle=Math.atan2((s.deckY-s.groundY)*sign,(s.deckX-s.groundX)*sign);u.state='move';this.e.impacts.recordMove(u,amount);u.travelDistance+=amount;
      if(progress>=1-1e-9&&!descending){u._rampartClimb=1;u._rampartZ=s.height;u._onRampart=true;u._rampartId=s.structureId;u._terrain=DECK;u._rampartStage='deck';const r=this.e.regiments[u.regiment],slot=r._rampartSlots[u._rampartSlot];u._rampartPath=slot?this.wallPath(u,slot,s):[];}
      if(progress<=1e-9&&descending){u._onRampart=false;u._rampartId='';u._rampartClimb=0;u._rampartZ=0;u._rampartStage='courtyard';u._terrain=this.e.terrainAt(u.x,u.y);this.e._bodyZ[u.id]=0;
        const base=(this.e.siege.byId.get(s.structureId).stairs||[]).find(a=>a.id===s.id)||s,side=Math.sign(s.axis==='x'?s.groundY-base.groundY:s.groundX-base.groundX)||1;
        const exit=this.e._nearestPassable(s.groundX+(s.axis==='y'?side*65:0),s.groundY+(s.axis==='x'?side*65:0),u._radius+1);u._rampartGroundPath=[exit];}
    }
    tick(u,dt) {u.anim+=dt;u.attackFlash=Math.max(0,u.attackFlash-dt*3.7);u._cooldown-=dt;u._meleeCooldown-=dt;u._sense-=dt;u._terrainTimer-=dt;}
    stepUnit(u,dt) {
      const r=this.e.regiments[u.regiment];if(!r._rampartJob)return false;if(u.routed&&!r._rampartJob.leaving)this.leaveRamparts([r.id],false);
      this.tick(u,dt);
      if(u._rampartStage==='falling'){
        const goal=u._rampartFallTarget,dx=goal.x-u.x,dy=goal.y-u.y,d=Math.hypot(dx,dy),step=Math.min(d,T[u.type].speed*dt);
        if(d>1){u.x+=dx/d*step;u.y+=dy/d*step;}if(!this.e._bodyBlocked(u.x,u.y,u._radius))u._rampartZ=Math.max(0,u._rampartZ-70*dt);
        u.state='knocked';this.e.impacts.recordMove(u,step);
        if(u._rampartZ===0){u._onRampart=false;u._rampartStage='courtyard';u._rampartId='';u.hp-=u._rampartFallDamage;if(u.hp<=0)this.e._kill(u,null);}return true;
      }
      if(u._rampartStage==='approach'){
        const access=u._rampartAccess,queue=this.queues.get(access.id+':'+access.lane)||[u.id],ahead=queue.slice(0,queue.indexOf(u.id)).filter(id=>this.e.units[id].alive&&this.e.units[id]._rampartStage==='approach').length;
        const length=Math.hypot(access.deckX-access.groundX,access.deckY-access.groundY)||1,nx=(access.deckX-access.groundX)/length,ny=(access.deckY-access.groundY)/length;
        const wait={x:access.groundX-nx*ahead*24,y:access.groundY-ny*ahead*24};
        const path=u._rampartGroundPath;if(path.length===1)path[0]=wait;const goal=path[0]||wait;
        if(goal){const p=goal;this.e._moveUnit(u,p.x,p.y,dt,.8);if(Math.hypot(u.x-p.x,u.y-p.y)<3){if(path.length>1)path.shift();
          else if(ahead===0&&!r.unitIds.some(id=>{const v=this.e.units[id],slot=r._rampartSlots[v._rampartSlot],own=r._rampartSlots[u._rampartSlot];return v.alive&&slot&&own&&slot.structureId===own.structureId&&slot.rank<own.rank&&(!v._onRampart||v._rampartPath.length);})&&!queue.some(id=>id!==u.id&&this.e.units[id].alive&&this.e.units[id]._rampartStage==='climbing'&&this.e.units[id]._rampartClimb*length<24)){path.length=0;u._rampartStage='climbing';u._rampartClimb=clamp(((u.x-access.groundX)*nx+(u.y-access.groundY)*ny)/length,0,1);}}}return true;
      }
      if(u._rampartStage==='climbing'||u._rampartStage==='descending'){this.stairMove(u,dt,u._rampartStage==='descending');return true;}
      if(u._onRampart){
        if(!this.e.siege.byId.get(u._rampartId)||this.e.siege.byId.get(u._rampartId).breached){this.fall(u);return true;}
        if(u._rampartStage==='exit-deck'){const access=u._rampartAccess,queue=this.queues.get('out:'+access.id+':'+access.lane)||[u.id];if(queue.slice(0,queue.indexOf(u.id)).some(id=>{const v=this.e.units[id],support=this.e.siege.byId.get(v._rampartAccess.structureId);return v.alive&&(v._rampartStage==='exit-deck'||v._rampartStage==='descending'&&support&&inside(v.x,v.y,support,-v._radius-8));})){u._vx=u._vy=0;u.state='idle';return true;}}
        const path=u._rampartPath,point=path[0];if(point&&this.deckMove(u,point,dt)){u._rampartId=point.structureId;path.shift();}
        if(!path.length&&u._rampartStage==='exit-deck'){u._rampartStage='descending';u._rampartClimb=1;return true;}
        if(!path.length){u._vx=u._vy=0;u.state='idle';u._terrain=DECK;
          const contact=this.e._contactTarget(u,u.type==='musketeer'?32:T[u.type].ranged?28:T[u.type].range*1.12);
          if(contact>=0){const enemy=this.e.units[contact];if(u.type==='musketeer')this.e._musketMelee(u,enemy);else if(u._cooldown<=0)this.e._attack(u,enemy);}
          else if(T[u.type].ranged){if(u._sense<=0||u.target<0){this.e._setTarget(u,this.e._rangedLocalTarget(u,T[u.type].range*1.12));u._sense=.4+u._jitter*.2;}const target=this.e.units[u.target];if(target&&target.alive&&!target.routed&&u._cooldown<=0&&u.type!=='musketeer')this.e._attack(u,target);}
        }return true;
      }
      if(u._rampartStage==='courtyard'){
        if(u._rampartCourtGoal){const path=u._rampartGroundPath,g=path[0]||u._rampartCourtGoal;this.e._moveUnit(u,g.x,g.y,dt,.9);if(Math.hypot(u.x-g.x,u.y-g.y)<2.1){if(path.length)path.shift();else u._rampartStage='settled';}if(r._rampartJob.breach)u.angle=Math.atan2(r._rampartJob.breach.y-u.y,r._rampartJob.breach.x-u.x);}
        else u._rampartStage='settled';
      }
      if(r._rampartJob.leaving&&r.unitIds.every(id=>!this.e.units[id].alive||this.e.units[id]._rampartStage==='settled')){const courtyard=r._rampartJob.courtyard,breach=r._rampartJob.breach,transfer=r._rampartJob.transfer;r._rampartJob=null;r.rampartStatus='Courtyard reorganized';this.e._captureMarchLayout(r);if(transfer&&!r._routed)this.manRampart([r.id],transfer.structureId,transfer.options);else if(courtyard&&!r._routed){this.e.command([r.id],'hold');if(breach){r.facing=Math.atan2(breach.y-r.y,breach.x-r.x);for(const id of r.unitIds)if(this.e.units[id].alive)this.e.units[id].angle=r.facing;}}this.e._strategyTimer=0;}
      return true;
    }
    fall(u) { const s=this.e.siege.byId.get(u._rampartId),r=this.e.regiments[u.regiment];u._rampartFallDamage=Math.max(5,u._rampartZ*.65);u._rampartStage='falling';u._onRampart=false;
      const cx=s&&Number.isFinite(s.courtyardX)?s.courtyardX:r.x,cy=s&&Number.isFinite(s.courtyardY)?s.courtyardY:r.y,d=Math.hypot(cx-u.x,cy-u.y)||1;
      u._rampartFallTarget=this.e._nearestPassable(u.x+(cx-u.x)/d*85,u.y+(cy-u.y)/d*85,u._radius+1);if(!u._rampartCourtGoal)u._rampartCourtGoal=u._rampartFallTarget;
    }
    collapse(s) { this.revision=-1;for(const r of this.e.regiments)if(r._rampartJob&&r._rampartJob.component.includes(s.id)){
      r._rampartJob.breach={x:s.x+s.w/2,y:s.y+s.h/2};
      this.leaveRamparts([r.id],true);r.rampartStatus='Wall breached · reforming in courtyard';r.cohesion=Math.max(25,r.cohesion-18);
      for(const id of r.unitIds){const u=this.e.units[id];if(u.alive&&u._onRampart&&u._rampartId===s.id)this.fall(u);}
    }}
  }
  global.BattleRamparts=BattleRamparts;
})(typeof window!=='undefined'?window:globalThis);
