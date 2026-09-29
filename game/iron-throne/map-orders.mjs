import { localHouseId } from './house-control.mjs';
import { atWar, moveCost } from './core.mjs';
import { armySpeed } from './warfare.mjs';
import { fleetSpeed } from './naval-state.mjs';
import { nodeTile } from './naval-graph.mjs';

export function fleetArrivalTurns(f) {
  return Math.max(1,Math.ceil(((f.path?.length||0)-(f.sailingCarry||0))/fleetSpeed(f)));
}
export function mapOrders(s,owner=localHouseId(s)) {
  const orders=[];
  for(const f of s.fleets||[]){
    if(f.owner!==owner||f.cargoUnknown)continue;
    const path=(f.path||[]).map(nodeTile),turns=fleetArrivalTurns(f);
    if(path.length||['attack','unload','blockade','intercept','escort'].includes(f.order)){
      const kind=f.order==='attack'?'attack':f.order==='unload'?'unload':'sail';
      const target=f.attackTile||f.landing||path.at(-1)||f.tile;
      if(kind==='unload'&&s.armies.some(a=>a.tile===target&&atWar(s,owner,a.owner)))orders.push({id:f.id+'-landing',kind:'attack',from:f.tile,target,path:[],turns,label:`LANDING BATTLE ${turns===1?'END TURN':`${turns} TURNS`}`});
      orders.push({id:f.id,kind,from:f.tile,target,path,turns,label:`${{attack:'ATTACK',unload:'UNLOAD',sail:'SAIL'}[kind]} ${turns===1?'END TURN':`${turns} TURNS`}`});
    }
    if(f.loadedTurn===s.turn)orders.push({id:f.id+'-loaded',kind:'load',from:f.tile,target:f.tile,path:[],turns:0,label:'LOADED THIS TURN'});
    if(f.unloadedTurn===s.turn)orders.push({id:f.id+'-unloaded',kind:'unload',from:f.tile,target:f.tile,path:[],turns:0,label:'UNLOADED THIS TURN'});
  }
  for(const a of s.armies||[]){
    if(a.owner!==owner)continue;
    if(a.embarkOrder){const f=(s.fleets||[]).find(f=>f.id===a.embarkOrder.fleet);if(f)orders.push({id:a.id,kind:'load',from:a.tile,target:f.tile,path:[f.tile],turns:1,label:`LOAD ${Math.min(a.embarkOrder.count,Object.values(a.units).reduce((n,v)=>n+v,0))} · END TURN`});continue;}
    if(a.order==='ranged'||a.order==='bombard'){orders.push({id:a.id,kind:'attack',from:a.tile,target:a.target,path:[],turns:1,label:'FIRE · END TURN'});continue;}
    if(!a.path?.length&&!a.structureTarget)continue;
    const path=a.path||[];let from=s.tiles[a.tile],turns=1,remaining=armySpeed(a);
    let target=path.at(-1)||a.target,kind=a.order==='attack'?'attack':'march',route=[];
    for(const id of path){const to=s.tiles[id];if(!to)break;const cost=moveCost(from,to);if(cost>remaining&&remaining!==armySpeed(a)){turns++;remaining=armySpeed(a);}remaining-=cost;route.push(id);from=to;
      if(s.armies.some(e=>e.tile===id&&atWar(s,owner,e.owner))){kind='attack';target=id;break;}
    }
    orders.push({id:a.id,kind,from:a.tile,target,path:route,turns,label:`${kind==='attack'?'ATTACK':'MARCH'} ${turns===1?'END TURN':`${turns} TURNS`}`});
  }
  return orders.filter(o=>s.tiles[o.from]&&s.tiles[o.target]);
}
function swords(c) {
  // Draw two red blades explicitly: color must not depend on emoji fonts.
  c.strokeStyle='#ff626a';c.fillStyle='#ff626a';c.lineWidth=2;
  for(const sign of [-1,1]){c.save();c.rotate(sign*.72);c.beginPath();c.moveTo(-2,1);c.lineTo(-2,-7);c.lineTo(0,-10);c.lineTo(2,-7);c.lineTo(2,1);c.closePath();c.fill();c.beginPath();c.moveTo(-4,2);c.lineTo(4,2);c.moveTo(0,2);c.lineTo(0,7);c.stroke();c.restore();}
}
export function drawOrderIndicators(map,c,s,pixel,inView) {
  const orders=mapOrders(s),rows=new Map();
  // Runs after water, terrain, fog and every unit sprite. Selection is irrelevant.
  for(const o of orders){
    const from=pixel(s.tiles[o.from]),to=pixel(s.tiles[o.target]),color=o.kind==='attack'?'#ff626a':o.kind==='load'?'#8ee8ad':o.kind==='unload'?'#ffd17d':'#84ddff';
    c.save();c.beginPath();c.moveTo(from.x,from.y);
    for(const id of o.path){const p=pixel(s.tiles[id]);c.lineTo(p.x,p.y);}
    c.lineTo(to.x,to.y);c.strokeStyle='#071b2d';c.lineWidth=5;c.stroke();c.setLineDash([6,4]);c.strokeStyle=color;c.lineWidth=2;c.stroke();c.setLineDash([]);
    if(inView(to)){
      const row=rows.get(o.target)||0;rows.set(o.target,row+1);
      c.translate(to.x,to.y-30-row*23);const scale=Math.max(1,.75/map.zoom);c.scale(scale,scale);
      c.font='bold 8px system-ui';c.textAlign='left';const w=c.measureText(o.label).width+29;
      c.fillStyle='#071b2df2';c.strokeStyle=color;c.lineWidth=1.3;c.beginPath();c.roundRect(-w/2,-12,w,22,5);c.fill();c.stroke();
      c.save();c.translate(-w/2+12,0);if(o.kind==='attack')swords(c);else{c.fillStyle=color;c.font='bold 16px system-ui';c.textAlign='center';c.fillText(o.kind==='load'?'↥':o.kind==='unload'?'↧':'➜',0,5);}c.restore();
      c.fillStyle=color;c.fillText(o.label,-w/2+25,3);
    }
    c.restore();
  }
}
