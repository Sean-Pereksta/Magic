// Observation-only intelligence. Hidden live entities are never consulted when
// updating or retiring a record; the only evidence is the current fog grid and
// the entities whose centers that grid presently exposes.
const memories=new WeakMap();
const finite=(v,fallback=0)=>Number.isFinite(v)?v:fallback;
function freeze(value){if(value&&typeof value==='object'){for(const child of Object.values(value))freeze(child);Object.freeze(value);}return value;}
function fogCell(game,team,x,y,kind='visible'){
 const fog=game.fog,grid=fog?.teams?.[team]?.[kind];if(!grid||x<0||y<0||x>=game.width||y>=game.height)return false;
 const column=Math.floor(x/fog.cell),row=Math.floor(y/fog.cell);return !!grid[row*fog.cols+column];
}
export function memoryPointExplored(game,team,x,y){return fogCell(game,team,x,y,'explored');}
function fullyRevisited(game,team,record){
 const fog=game.fog,r=Math.max(24,record.r||24),left=Math.max(0,Math.floor((record.x-r)/fog.cell)),right=Math.min(fog.cols-1,Math.floor((record.x+r)/fog.cell)),top=Math.max(0,Math.floor((record.y-r)/fog.cell)),bottom=Math.min(fog.rows-1,Math.floor((record.y+r)/fog.cell)),visible=fog.teams[team].visible;
 for(let y=top;y<=bottom;y++)for(let x=left;x<=right;x++)if(!visible[y*fog.cols+x])return false;
 return true;
}
function visualSnapshot(game,e,visibleWalls){
 const record={id:e.id,type:e.type,team:e.team,x:e.x,y:e.y,hp:finite(e.hp),maxHp:finite(e.maxHp,e.hp),r:finite(e.r,24),facing:finite(e.facing),constructing:finite(e.constructing),buildTotal:finite(e.buildTotal,10),amount:finite(e.amount),maxAmount:finite(e.maxAmount,e.amount||1500),carrying:finite(e.carrying),paperInvulnerable:finite(e.paperInvulnerable),lastSeen:finite(game.time),remembered:true};
 // The field shows the current production progress and occupied seats, never
 // the contents of an enemy's future queue or the identities inside a CD.
 record.queue=e.queue?.[0]?[{remaining:finite(e.queue[0].remaining),total:finite(e.queue[0].total,1)}]:[];
 record.cargo=Array.from({length:Math.min(6,e.cargo?.length||0)},()=>null);
 if(e.leap)record.lift=Math.sin(Math.PI*(1-finite(e.leap.remaining)/Math.max(.001,finite(e.leap.total,1))))*75;
 else record.lift=e.type==='airplane'?32:e.type==='cd'?18:e.type==='divider'?8:0;
 if(e.type==='wall'){
  record.wallMask=0;
  for(const [dx,dy,bit]of [[0,-48,1],[48,0,2],[0,48,4],[-48,0,8]])if(visibleWalls.get(`${e.x+dx},${e.y+dy}`)?.team===e.team)record.wallMask|=bit;
 }
 return freeze(record);
}

export function updateScoutMemory(game){
 if(!game?.fog?.teams)return;
 let state=memories.get(game);if(!state){state={teams:[]};memories.set(game,state);}
 for(let team=0;team<game.fog.teams.length;team++){
  const view=state.teams[team]??={records:new Map(),visibleIds:new Set()};
  const observed=(game.entities||[]).filter(e=>e.hp>0&&!e.transportedBy&&fogCell(game,team,e.x,e.y));
  const visibleWalls=new Map(observed.filter(e=>e.type==='wall').map(e=>[`${e.x},${e.y}`,e]));
  view.visibleIds=new Set(observed.map(e=>e.id));
  for(const e of observed){if(e.team===team)view.records.delete(e.id);else view.records.set(e.id,visualSnapshot(game,e,visibleWalls));}
  for(const [id,record]of view.records)if(!view.visibleIds.has(id)&&fullyRevisited(game,team,record))view.records.delete(id);
 }
}

export function getRememberedEntities(game,team=0){
 const view=memories.get(game)?.teams?.[team];if(!view)return Object.freeze([]);
 // Returning frozen visual values rather than world entities also keeps these
 // observations out of hit testing, selection, collision and weapon targeting.
 return Object.freeze([...view.records.values()].filter(record=>!view.visibleIds.has(record.id)&&memoryPointExplored(game,team,record.x,record.y)));
}

export function resetScoutMemory(game){memories.delete(game);}
