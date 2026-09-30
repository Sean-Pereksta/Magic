import { localHouseId } from './house-control.mjs';
import { atWar } from './core.mjs';
import { armyArrivalTurns } from './movement-timing.mjs';
import { boardingArrival, boardingCount } from './naval.mjs';
import { fleetSpeed } from './naval-state.mjs';
import { nodeTile } from './naval-graph.mjs';

export function fleetArrivalTurns(f,s=null) {
  const offset=s&&f.resolvedTurn===s.turn?1:0;
  const budget=(fleetSpeed(f)+(f.sailingCarry||0))*(offset?1:1-(s&&f.movementTurn===s.turn?f.movementSpent||0:0));
  return Math.max(0,Math.ceil(((f.path?.length||0)-budget)/fleetSpeed(f)))+offset;
}
export const arrivalText = turns => turns===null?'Route unavailable':turns===0?'this turn':`in ${turns} turn${turns===1?'':'s'}`;
export function mapOrders(s,owner=localHouseId(s)) {
  const orders=[];
  for(const f of s.fleets||[]){
    if(f.owner!==owner||f.cargoUnknown)continue;
    const path=(f.path||[]).map(nodeTile),turns=fleetArrivalTurns(f,s);
    if(path.length||['attack','unload','blockade','intercept','escort'].includes(f.order)){
      const kind=f.order==='attack'?'attack':f.order==='unload'?'unload':'sail';
      const target=f.attackTile||f.landing||path.at(-1)||f.tile;
      if(kind==='unload'&&s.armies.some(a=>a.tile===target&&atWar(s,owner,a.owner)))orders.push({id:f.id+'-landing',kind:'attack',from:f.tile,target,path:[],turns,label:`LANDING BATTLE ${turns===0?'END TURN':`${turns} TURNS`}`});
      orders.push({id:f.id,kind,from:f.tile,target,path,turns,label:`${{attack:'ATTACK',unload:'UNLOAD',sail:'SAIL'}[kind]} ${turns===0?'END TURN':`${turns} TURNS`}`});
    }
    if(f.loadedTurn===s.turn)orders.push({id:f.id+'-loaded',kind:'load',from:f.tile,target:f.tile,path:[],turns:0,label:'LOADED THIS TURN'});
    if(f.unloadedTurn===s.turn)orders.push({id:f.id+'-unloaded',kind:'unload',from:f.tile,target:f.tile,path:[],turns:0,label:'UNLOADED THIS TURN'});
  }
  for(const a of s.armies||[]){
    if(a.owner!==owner)continue;
    if(a.embarkOrder){
      const f=(s.fleets||[]).find(f=>f.id===a.embarkOrder.fleet&&f.owner===owner);
      const route=f&&a.embarkOrder.status!=='paused'&&boardingCount(s,a,f)>=Math.min(a.embarkOrder.count,Object.values(a.units).reduce((n,v)=>n+v,0))?boardingArrival(s,a,f):null;
      if(route)orders.push({id:a.id,kind:'load',from:a.tile,target:route.target,path:[...route.path,route.target],turns:route.turns,label:`BOARD ${arrivalText(route.turns)}`});
      continue;
    }
    if(a.order==='ranged'||a.order==='bombard'){orders.push({id:a.id,kind:'attack',from:a.tile,target:a.target,path:[],turns:a.resolvedTurn===s.turn?1:0,label:'FIRE · END TURN'});continue;}
    if(!a.path?.length&&!a.structureTarget)continue;
    const path=a.path||[];
    let target=path.at(-1)||a.target,kind=a.order==='attack'?'attack':'march',route=[];
    for(const id of path){if(!s.tiles[id])break;route.push(id);
      if(s.armies.some(e=>e.tile===id&&atWar(s,owner,e.owner))){kind='attack';target=id;break;}
    }
    const turns=armyArrivalTurns(s,a,route);
    orders.push({id:a.id,kind,from:a.tile,target,path:route,turns,label:`${kind==='attack'?'ARRIVE AT TARGET':'MARCH'} ${arrivalText(turns)}`});
  }
  return orders.filter(o=>s.tiles[o.from]&&s.tiles[o.target]);
}
function swords(c) {
  // Draw two red blades explicitly: color must not depend on emoji fonts.
  c.strokeStyle='#ff626a';c.fillStyle='#ff626a';c.lineWidth=2;
  for(const sign of [-1,1]){c.save();c.rotate(sign*.72);c.beginPath();c.moveTo(-2,1);c.lineTo(-2,-7);c.lineTo(0,-10);c.lineTo(2,-7);c.lineTo(2,1);c.closePath();c.fill();c.beginPath();c.moveTo(-4,2);c.lineTo(4,2);c.moveTo(0,2);c.lineTo(0,7);c.stroke();c.restore();}
}
export function drawOrderIndicators(map,c,s,pixel,inView) {
  const orders=mapOrders(s);
  // Runs after water, terrain, fog and every unit sprite. Selection is irrelevant.
  for(const o of orders){
    const from=pixel(s.tiles[o.from]),to=pixel(s.tiles[o.target]),color=o.kind==='attack'?'#ff626a':o.kind==='load'?'#8ee8ad':o.kind==='unload'?'#ffd17d':'#84ddff';
    c.save();c.beginPath();c.moveTo(from.x,from.y);
    for(const id of o.path){const p=pixel(s.tiles[id]);c.lineTo(p.x,p.y);}
    c.lineTo(to.x,to.y);c.strokeStyle='#071b2d';c.lineWidth=5;c.stroke();c.setLineDash([6,4]);c.strokeStyle=color;c.lineWidth=2;c.stroke();c.setLineDash([]);
    if(o.kind==='attack'&&inView(to)){
      c.translate(to.x,to.y-13);const scale=Math.max(1,.6/map.zoom);c.scale(scale,scale);swords(c);
    }
    c.restore();
    if(o.id===map.armyId&&o.turns!==null){
      const points=[from,...o.path.map(id=>pixel(s.tiles[id])),to];
      const segments=[];
      // Clip each segment to the viewport so the badge sits on the visible route.
      const width=map.width/map.zoom,height=map.height/map.zoom,left=(map.x||0)-width/2,top=(map.y||0)-height/2;
      for(let i=1;i<points.length;i++){
        let p=points[i-1],q=points[i];
        if(Number.isFinite(width)&&Number.isFinite(height)){
          let lo=0,hi=1;const dx=q.x-p.x,dy=q.y-p.y;
          for(const [v,w] of [[-dx,p.x-left],[dx,left+width-p.x],[-dy,p.y-top],[dy,top+height-p.y]]){
            if(v===0){if(w<0){hi=-1;break;}}else{const t=w/v;if(v<0)lo=Math.max(lo,t);else hi=Math.min(hi,t);}
          }
          if(lo>hi)continue;q={x:p.x+dx*hi,y:p.y+dy*hi};p={x:p.x+dx*lo,y:p.y+dy*lo};
        }
        const length=Math.hypot(q.x-p.x,q.y-p.y);if(length)segments.push({p,q,length});
      }
      let remaining=segments.reduce((n,v)=>n+v.length,0)/2,mid=from;
      for(const segment of segments){if(remaining<=segment.length){const t=remaining/segment.length;mid={x:segment.p.x+(segment.q.x-segment.p.x)*t,y:segment.p.y+(segment.q.y-segment.p.y)*t};break;}remaining-=segment.length;}
      if(!segments.length&&!inView(from))continue;
      c.save();c.translate(mid.x,mid.y);c.scale(1/map.zoom,1/map.zoom);
      c.font='bold 12px sans-serif';c.textAlign='center';c.textBaseline='middle';
      const radius=Math.max(12,c.measureText(String(o.turns)).width/2+6);
      c.beginPath();c.arc(0,0,radius,0,Math.PI*2);c.fillStyle='#071b2d';c.fill();c.strokeStyle=color;c.lineWidth=2;c.stroke();
      c.fillStyle='#ffffff';c.fillText(String(o.turns),0,1);c.restore();
    }
  }
}
