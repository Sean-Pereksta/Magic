/** Shared, deterministic Stoney rules. No Firebase or rendering dependencies. */
export const RULES = Object.freeze({ duration: 300000, respawn: 15000, shield: 2500,
  onlineGrace: 15000, lease: 6500, cellSize: 4, wallHeight: 3.4, wallThickness: .22,
  playerRadius: .32, playerHeight: 1.65, roomDepth: 10, maxEnemies: 12,
  moveInterval: 350, idleInterval: 5000, worldInterval: 400, spawnWarning: 1300 });
export const COST = Object.freeze({ fire: 20, fog: 15, ghost: 50, demon: 75 });
export const TTL = Object.freeze({ fire: 10000, fog: 18000, ghost: 300000, demon: 300000 });
export const finite = (v, fallback = 0) => Number.isFinite(Number(v)) ? Number(v) : fallback;
export const distance = (a, b) => Math.hypot(a.x - b.x, a.z - b.z);
export const stamp = value => typeof value?.toMillis === 'function' ? value.toMillis() : finite(value);
export const roundKey = s => s ? `${s.seed}:${s.setupDeadline}` : '';
export function random(seed) { let x = (seed | 0) || 123456789; return () => {
  x ^= x << 13; x ^= x >>> 17; x ^= x << 5; return (x >>> 0) / 4294967296;
}; }
export function hash(text) { let h = 2166136261; for (const c of String(text)) h = Math.imul(h ^ c.charCodeAt(0), 16777619); return h >>> 0; }
export function makeMaze(w = 21, h = 21, seed = 1, size = 4) {
  w = Math.max(3, Math.min(41, Math.trunc(finite(w, 21)))); h = Math.max(3, Math.min(41, Math.trunc(finite(h, 21))));
  size = Math.max(3, Math.min(8, finite(size, 4)));
  const cells = Array.from({ length: w * h }, () => ({ v: false, walls: 15 }));
  const dirs = [[1, 0, -1, 4], [2, 1, 0, 8], [4, 0, 1, 1], [8, -1, 0, 2]];
  const rnd = random(seed), stack = [[0, 0]]; cells[0].v = true;
  while (stack.length) {
    const [x, y] = stack.at(-1);
    const options = dirs.filter(([, dx, dy]) => x+dx >= 0 && y+dy >= 0 && x+dx < w && y+dy < h && !cells[(y+dy)*w+x+dx].v);
    if (!options.length) { stack.pop(); continue; }
    const [bit, dx, dy, opposite] = options[Math.floor(rnd()*options.length)], nx=x+dx, ny=y+dy;
    cells[y*w+x].walls &= ~bit; cells[ny*w+nx].walls &= ~opposite;
    cells[ny*w+nx].v = true; stack.push([nx,ny]);
  }
  const entrance = { x: Math.floor(w/2), y: h-1 }; cells[entrance.y*w+entrance.x].walls &= ~4;
  const maze = { w, h, size, seed, cells, entrance, neighbors: [], boxes: [] };
  for (let y=0; y<h; y++) for (let x=0; x<w; x++) {
    const i=y*w+x, walls=cells[i].walls, p=center(maze,i), half=size/2, t=RULES.wallThickness;
    maze.neighbors[i] = dirs.filter(([bit,dx,dy]) => !(walls&bit) && x+dx>=0 && y+dy>=0 && x+dx<w && y+dy<h).map(([,dx,dy]) => (y+dy)*w+x+dx);
    const box=(cx,cz,sx,sz)=>maze.boxes.push({x:cx,z:cz,sx,sz,minX:cx-sx/2,maxX:cx+sx/2,minZ:cz-sz/2,maxZ:cz+sz/2});
    if (walls&1) box(p.x,p.z-half,size+t,t);
    if (walls&8) box(p.x-half,p.z,t,size+t);
    if (y===h-1 && walls&4) box(p.x,p.z+half,size+t,t);
    if (x===w-1 && walls&2) box(p.x+half,p.z,t,size+t);
  }
  // The entrance frame is deliberately NOT a solid box across the doorway.
  const edge=h*size/2, depth=RULES.roomDepth, width=w*size, t=RULES.wallThickness;
  for (const b of [{x:-width/2-t/2,z:edge+depth/2,sx:t,sz:depth}, {x:width/2+t/2,z:edge+depth/2,sx:t,sz:depth}, {x:0,z:edge+depth+t/2,sx:width+t,sz:t}]) maze.boxes.push({...b,minX:b.x-b.sx/2,maxX:b.x+b.sx/2,minZ:b.z-b.sz/2,maxZ:b.z+b.sz/2});
  // Spatial buckets avoid testing every maze wall for every movement step.
  maze.buckets = new Map();
  for (const b of maze.boxes) for (let x=Math.floor(b.minX/size)-1;x<=Math.floor(b.maxX/size)+1;x++) for (let z=Math.floor(b.minZ/size)-1;z<=Math.floor(b.maxZ/size)+1;z++) {
    const key=`${x},${z}`; if (!maze.buckets.has(key)) maze.buckets.set(key,[]); maze.buckets.get(key).push(b);
  }
  return maze;
}
export function center(m, i) { return { x: -m.w*m.size/2+(i%m.w+.5)*m.size, z:-m.h*m.size/2+(Math.floor(i/m.w)+.5)*m.size }; }
export function cellAt(m, p) { const x=Math.floor((p.x+m.w*m.size/2)/m.size), y=Math.floor((p.z+m.h*m.size/2)/m.size); return x>=0&&y>=0&&x<m.w&&y<m.h ? y*m.w+x : -1; }
export function spawnPoint(m, id = '') { const p=center(m,m.entrance.y*m.w+m.entrance.x); return {x:p.x+((hash(id)%5)-2)*.45,z:m.h*m.size/2+RULES.roomDepth*.72}; }
export function blocked(m,p,r=RULES.playerRadius) {
  if (!Number.isFinite(p.x)||!Number.isFinite(p.z)) return true;
  if (p.x < -m.w*m.size/2+r || p.x > m.w*m.size/2-r || p.z < -m.h*m.size/2+r || p.z > m.h*m.size/2+RULES.roomDepth-r) return true;
  return (m.buckets.get(`${Math.floor(p.x/m.size)},${Math.floor(p.z/m.size)}`)||[]).some(b => p.x>=b.minX-r && p.x<=b.maxX+r && p.z>=b.minZ-r && p.z<=b.maxZ+r);
}
export function move(m,p,dx,dz,r=RULES.playerRadius) {
  const n=Math.max(1,Math.ceil(Math.hypot(dx,dz)/.15)), out={x:p.x,z:p.z};
  for (let i=0;i<n;i++) { if (!blocked(m,{x:out.x+dx/n,z:out.z},r)) out.x+=dx/n; if (!blocked(m,{x:out.x,z:out.z+dz/n},r)) out.z+=dz/n; }
  return out;
}
export function isOnline(p,now) { const at=stamp(p.serverAt)||finite(p.updatedAt); return p.online!==false && at>0 && now-at<RULES.onlineGrace && now-at>-10000; }
export function isLiving(p,now) { return p.stoneyRole!=='dm' && p.alive!==false && finite(p.deadUntil)<=now && isOnline(p,now); }
export function energy(s,now) { return Math.min(100,Math.max(0,finite(s.dmEnergyBase)+(now-finite(s.dmEnergyStamp,now))/1000*6)); }
export function trapWindow(t,s) { const start=finite(s.startAt)||finite(s.setupDeadline), armedAt=Math.max(finite(t.placedAt),start)+(t.telegraphMs ?? 0); return {armedAt,expiresAt:armedAt+finite(t.ttlMs,TTL[t.type]||10000)}; }
export class Navigation {
  constructor(maze) { this.maze=maze; this.cache=new Map(); }
  field(goal) {
    if(this.cache.has(goal)) return this.cache.get(goal);
    const d=new Int16Array(this.maze.cells.length).fill(-1);
    if(goal>=0&&goal<d.length) { d[goal]=0; const q=[goal]; for(let head=0;head<q.length;head++) for(const n of this.maze.neighbors[q[head]]) if(d[n]<0){d[n]=d[q[head]]+1;q.push(n);} }
    if(this.cache.size>=32) this.cache.delete(this.cache.keys().next().value);
    this.cache.set(goal,d);return d;
  }
  next(from,to) { const d=this.field(to); return (this.maze.neighbors[from]||[]).filter(i=>d[i]>=0&&d[i]<d[from]).sort((a,b)=>d[a]-d[b]||a-b)[0] ?? from; }
}
export function placementProblem(m,s,p,players,traps,now,type='ghost') {
  if(cellAt(m,p)<0||blocked(m,p,.7)) return 'Choose a clear maze cell.';
  if(type==='crown') return s.phase==='setup' ? '' : 'The crown can only be placed during setup.';
  const entrance=center(m,m.entrance.y*m.w+m.entrance.x), r=m.size*2;
  if(distance(p,entrance)<r) return 'Keep the entrance and respawn route clear.';
  for(const player of players) if(isLiving(player,now)&&distance(p,player)<=r) return `Too close to ${player.name||'a player'}.`;
  const carrier=players.find(v=>v.uid===s.carrierId), crown=carrier||s.crown;
  if(crown&&distance(p,crown)<=r) return 'Keep the crown approach clear.';
  const active=traps.filter(t=>trapWindow(t,s).expiresAt>now);
  if(active.some(t=>distance(p,t)<m.size*.8)) return 'Avoid stacking spawns in one cell.';
  if((type==='ghost'||type==='demon')&&active.filter(t=>t.type==='ghost'||t.type==='demon').length>=RULES.maxEnemies) return 'The active enemy limit is reached.';
  if(active.length>=32) return 'Wait for an existing trap to expire.';
  return '';
}
export function suggestSpawn(m,s,players,traps,now,type) {
  const nav=new Navigation(m), living=players.filter(p=>isLiving(p,now)), entrance=m.entrance.y*m.w+m.entrance.x;
  let best=null,bestScore=-Infinity;
  for(let i=0;i<m.cells.length;i++) {
    const p=center(m,i); if(placementProblem(m,s,p,players,traps,now,type)) continue;
    const steps=living.map(v=>nav.field(cellAt(m,v))[i]).filter(v=>v>=0);
    const nearest=steps.length?Math.min(...steps):nav.field(entrance)[i];
    const score=-Math.abs(nearest-6)*3+Math.min(3,m.neighbors[i].length)+(hash(`${s.seed}:${i}:${type}`)%100)/100;
    if(score>bestScore){bestScore=score;best=p;}
  }
  return best;
}
export function defaultCrown(m) { const d=new Navigation(m).field(m.entrance.y*m.w+m.entrance.x); const budget=Math.floor(RULES.duration/1000*3.9/(m.size*2)*.45); const candidates=Array.from(d,(_,i)=>i).filter(i=>d[i]>=0&&d[i]<=budget).sort((a,b)=>d[b]-d[a]||a-b); return center(m,candidates[0]); }
export function canPickup(s,p,now) { return s.phase==='play'&&!s.carrierId&&s.crown&&isLiving(p,now)&&finite(s.endAt)>now&&distance(p,s.crown)<=1.6; }
export function canEscape(m,s,p,now) { return s.phase==='play'&&s.carrierId===p.uid&&isLiving(p,now)&&now<finite(s.endAt)&&p.z>m.h*m.size/2+1.8&&!blocked(m,p); }
/** Host-only enemy decisions and positions. Clients display snapshots, never run competing AI. */
export class EnemyDirector {
  constructor(m) { this.m=m;this.nav=new Navigation(m);this.enemies=new Map(); }
  restore(list=[]) { this.enemies=new Map((Array.isArray(list)?list:[]).filter(e=>typeof e.id==='string'&&Number.isFinite(e.x)&&Number.isFinite(e.z)&&!blocked(this.m,e,.3)).map(e=>[e.id,{...e,nextThink:0,waypoint:null}])); }
  step(traps,s,players,now,dt) {
    dt=Math.max(0,Math.min(.1,dt)); const valid=new Set(), living=players.filter(p=>isLiving(p,now)&&finite(p.shieldUntil)<=now&&cellAt(this.m,p)>=0);
    const assigned=new Map(); for(const e of this.enemies.values()) if(e.targetId) assigned.set(e.targetId,(assigned.get(e.targetId)||0)+1);
    const hits=new Set();
    for(const t of traps) {
      if(t.round&&t.round!==roundKey(s)) continue;
      const {armedAt,expiresAt}=trapWindow(t,s);if(now<armedAt||now>=expiresAt||s.phase!=='play')continue;
      if(t.type==='fire'){ for(const p of living) if(distance(t,p)<1.1)hits.add(p.uid);continue; }
      if(t.type!=='ghost'&&t.type!=='demon')continue;
      valid.add(t.id);let e=this.enemies.get(t.id);
      if(!e){e={id:t.id,type:t.type,x:t.x,z:t.z,targetId:'',mode:'patrol',nextThink:0,lastSeen:0,goal:-1,windupUntil:0,chargeUntil:0,cooldownUntil:0,waypoint:null};this.enemies.set(t.id,e);}
      if(now>=e.nextThink){
        e.nextThink=now+330+(hash(e.id)%120);
        const from=cellAt(this.m,e), previousTarget=e.targetId;
        const choices=living.map(p=>({p,steps:this.nav.field(cellAt(this.m,p))[from]})).filter(v=>v.steps>=0&&v.steps<=(t.type==='ghost'?9:7));
        choices.sort((a,b)=>(a.steps+(assigned.get(a.p.uid)||0)*1.6-(a.p.uid===s.carrierId?2:0)-(a.p.uid===e.targetId?1.5:0))-(b.steps+(assigned.get(b.p.uid)||0)*1.6-(b.p.uid===s.carrierId?2:0)-(b.p.uid===e.targetId?1.5:0))||a.p.uid.localeCompare(b.p.uid));
        const target=choices[0]?.p;
        if(target){
          if(previousTarget!==target.uid){assigned.set(previousTarget,Math.max(0,(assigned.get(previousTarget)||0)-1));assigned.set(target.uid,(assigned.get(target.uid)||0)+1);}e.targetId=target.uid;e.lastSeen=now;e.goal=cellAt(this.m,target);e.mode='hunt';
          // Demons anticipate one legal next cell instead of cutting through walls.
          if(t.type==='demon'&&distance(e,target)>this.m.size*1.5){const future={x:target.x+finite(target.vx)*.6,z:target.z+finite(target.vz)*.6}, next=cellAt(this.m,future);if(this.m.neighbors[e.goal]?.includes(next))e.goal=next;}
          if(t.type==='demon'&&from===cellAt(this.m,target)&&distance(e,target)<3.6&&now>finite(e.cooldownUntil)) { e.windupUntil=now+650;e.chargeUntil=now+1150;e.cooldownUntil=now+4200; }
        } else if(now-finite(e.lastSeen)>3000){assigned.set(previousTarget,Math.max(0,(assigned.get(previousTarget)||0)-1));e.targetId='';e.mode='patrol';if(e.goal<0||from===e.goal)e.goal=this.m.neighbors[from]?.[hash(`${e.id}:${Math.floor(now/2500)}`)%(this.m.neighbors[from]?.length||1)]??from;}
      }
      if(now<finite(e.windupUntil)){e.mode='windup';continue;}
      if(e.goal<0)continue;
      const from=cellAt(this.m,e);if(from<0)continue;
      if(e.waypoint&&distance(e,e.waypoint)<.12)e.waypoint=null;
      if(!e.waypoint){
        const c=center(this.m,from);
        if(from===e.goal){const p=living.find(p=>p.uid===e.targetId);e.waypoint=p&&cellAt(this.m,p)===from?{x:p.x,z:p.z}:c;}
        else e.waypoint=distance(e,c)>.18?c:center(this.m,this.nav.next(from,e.goal));
      }
      const dist=distance(e,e.waypoint), speed=now<finite(e.chargeUntil)?5.3:t.type==='ghost'?2.65:3.05;
      if(dist>.001){const step=Math.min(dist,speed*dt),p=move(this.m,e,(e.waypoint.x-e.x)/dist*step,(e.waypoint.z-e.z)/dist*step,.3);e.x=p.x;e.z=p.z;}
      for(const p of living)if(distance(e,p)<.8)hits.add(p.uid);
    }
    for(const id of this.enemies.keys())if(!valid.has(id))this.enemies.delete(id);
    return [...hits];
  }
  snapshot() {return [...this.enemies.values()].map(({id,type,x,z,targetId,mode,lastSeen,goal,windupUntil,chargeUntil,cooldownUntil})=>({id,type,x:Math.round(x*100)/100,z:Math.round(z*100)/100,targetId,mode,lastSeen,goal,windupUntil,chargeUntil,cooldownUntil}));}
}
/** Coalesces latest values; acknowledgment, not an attempted write, advances the baseline. */
export class LatestWriter {
  constructor(send,{now=Date.now,onError=()=>{},onAck=()=>{}}={}) {this.send=send;this.now=now;this.onError=onError;this.onAck=onAck;this.pending=null;this.inFlight=false;this.lastAck=0;this.retryAt=0;this.failures=0;this.closed=false;}
  offer(value){if(!this.closed)this.pending=value;}
  async flush(){if(this.closed||this.inFlight||!this.pending||this.now()<this.retryAt)return false;
    const value=this.pending;this.pending=null;this.inFlight=true;
    try{await this.send(value);this.lastAck=this.now();this.failures=0;this.retryAt=0;if(!this.closed)this.onAck(value);return true;}
    catch(error){if(this.closed)return false;if(!this.pending)this.pending=value;this.retryAt=this.now()+Math.min(8000,500*2**this.failures++);this.onError(error);return false;}
    finally{this.inFlight=false;}
  }
  close(){this.closed=true;this.pending=null;}
}
