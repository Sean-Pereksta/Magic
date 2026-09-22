/* Shared rules: no Firebase, DOM or renderer dependency. */
export const RULES = Object.freeze({
  roundMs: 300000, respawnMs: 15000, protectionMs: 3000,
  staleMs: 12000, leaseMs: 6500, moveMs: 500, heartbeatMs: 2500,
  spawnWarningMs: 1200, maxTraps: 24, playerRadius: 0.34,
  cost: Object.freeze({fire: 20, fog: 15, ghost: 50, demon: 75}),
  ttl: Object.freeze({fire: 10000, fog: 18000, ghost: 300000, demon: 300000})
});
export const finite = (n, fallback = 0) => Number.isFinite(Number(n)) ? Number(n) : fallback;
export const distance = (a, b) => Math.hypot(a.x - b.x, a.z - b.z);
export const clamp = (n, lo, hi) => Math.max(lo, Math.min(hi, n));
export function stampMs(v) {
  if (typeof v?.toMillis === 'function') return v.toMillis();
  if (Number.isFinite(v?.seconds)) return v.seconds * 1000 + (v.nanoseconds || 0) / 1e6;
  return typeof v === 'number' && Number.isFinite(v) ? v : 0;
}
export const runKey = s => s ? String(s.runId || `seed:${s.seed}`) : '';
export function fresh(p, now) {
  const age = now - (stampMs(p.serverAt) || finite(p.updatedAt));
  return p.online !== false && age >= -5000 && age <= RULES.staleMs;
}
export function living(p, now, s) {
  return fresh(p, now) && !['dm','viewer'].includes(p.role) && p.ready !== false &&
    (!p.runId || p.runId === runKey(s)) && p.alive !== false && finite(p.deadUntil) <= now;
}
export function targetable(p, now, s) {
  return living(p, now, s) && !p.hidden && finite(p.protectedUntil) <= now;
}
export function hash(text) {
  let h = 2166136261;
  for (const c of String(text)) h = Math.imul(h ^ c.charCodeAt(0), 16777619);
  return h >>> 0;
}
export function xorshift32(seed) {
  let x = (seed | 0) || 123456789;
  return () => { x ^= x << 13; x ^= x >>> 17; x ^= x << 5; return (x >>> 0) / 4294967296; };
}
// Keep the original N/E/S/W ordering so existing seeded dungeons do not change.
export function makeMaze(w, h, seed) {
  if (!Number.isInteger(w) || !Number.isInteger(h) || w < 3 || h < 3 || w > 64 || h > 64)
    throw new Error('Invalid dungeon dimensions. Return to the lobby and start a new match.');
  const rnd = xorshift32(seed), cells = Array.from({length:w*h}, () => ({v:false, walls:15}));
  const dirs = [[1,0,-1,4],[2,1,0,8],[4,0,1,1],[8,-1,0,2]];
  const stack = [[0,0]]; cells[0].v = true;
  while (stack.length) {
    const [x,y] = stack[stack.length-1];
    const options = dirs.filter(([,dx,dy]) => x+dx >= 0 && x+dx < w && y+dy >= 0 && y+dy < h && !cells[(y+dy)*w+x+dx].v);
    if (!options.length) { stack.pop(); continue; }
    const [d,dx,dy,opp] = options[Math.floor(rnd()*options.length)], nx=x+dx, ny=y+dy;
    cells[y*w+x].walls &= ~d; cells[ny*w+nx].walls &= ~opp; cells[ny*w+nx].v=true;
    stack.push([nx,ny]);
  }
  const entrance={x:Math.floor(w/2),y:h-1}; cells[entrance.y*w+entrance.x].walls &= ~4;
  return {w,h,cells,entrance};
}
export class Maze {
  constructor(s) {
    Object.assign(this, makeMaze(s.w, s.h, s.seed));
    this.size = finite(s.cellSize,4);
    if (this.size < 2 || this.size > 12) throw new Error('Invalid dungeon cell size.');
    this.ox=-this.w*this.size/2; this.oz=-this.h*this.size/2;
    this.maxX=-this.ox; this.maxZ=-this.oz;
    this.entry=this.entrance.y*this.w+this.entrance.x;
    this.graph=this.cells.map((c,i) => {
      const x=i%this.w,y=Math.floor(i/this.w), out=[];
      for (const [d,dx,dy] of [[1,0,-1],[2,1,0],[4,0,1],[8,-1,0]]) {
        if (!(c.walls&d) && x+dx>=0 && x+dx<this.w && y+dy>=0 && y+dy<this.h) out.push((y+dy)*this.w+x+dx);
      }
      return out;
    });
    this.boxes=[]; this.buckets=new Map(); this.fields=new Map();
    const wall=(x,z,sx,sz) => {
      const box={x,z,sx,sz,minX:x-sx/2,maxX:x+sx/2,minZ:z-sz/2,maxZ:z+sz/2};
      this.boxes.push(box);
      for(let bx=Math.floor((box.minX-1)/this.size);bx<=Math.floor((box.maxX+1)/this.size);bx++)
        for(let bz=Math.floor((box.minZ-1)/this.size);bz<=Math.floor((box.maxZ+1)/this.size);bz++) {
          const k=`${bx},${bz}`; if(!this.buckets.has(k))this.buckets.set(k,[]); this.buckets.get(k).push(box);
        }
    };
    this.cells.forEach((c,i) => {
      const p=this.center(i),x=i%this.w,y=Math.floor(i/this.w),t=.35,cs=this.size;
      if(c.walls&1)wall(p.x,p.z-cs/2,cs+t,t);
      if(c.walls&8)wall(p.x-cs/2,p.z,t,cs+t);
      if(y===this.h-1&&(c.walls&4))wall(p.x,p.z+cs/2,cs+t,t);
      if(x===this.w-1&&(c.walls&2))wall(p.x+cs/2,p.z,t,cs+t);
    });
    wall(this.ox-.175,this.maxZ+4.5,.35,9.01);
    wall(this.maxX+.175,this.maxZ+4.5,.35,9.01);
    wall(0,this.maxZ+9.175,this.w*this.size+.35,.35);
  }
  center(i) { return {x:this.ox+(i%this.w+.5)*this.size,z:this.oz+(Math.floor(i/this.w)+.5)*this.size}; }
  cell(p) {
    if(!Number.isFinite(p?.x)||!Number.isFinite(p?.z)) return -1;
    const x=Math.floor((p.x-this.ox)/this.size),y=Math.floor((p.z-this.oz)/this.size);
    return x>=0&&x<this.w&&y>=0&&y<this.h ? y*this.w+x : -1;
  }
  spawn(id='') { const p=this.center(this.entry); return {x:p.x+((hash(id)%5)-2)*.23,z:this.maxZ+6.48}; }
  inRoom(p) { return p.x>this.ox+.34&&p.x<this.maxX-.34&&p.z>this.maxZ+1.8&&p.z<this.maxZ+8.66; }
  blocked(x,z,r=RULES.playerRadius) {
    if(!Number.isFinite(x)||!Number.isFinite(z)||x<this.ox+r||x>this.maxX-r||z<this.oz+r||z>this.maxZ+9-r)return true;
    return (this.buckets.get(`${Math.floor(x/this.size)},${Math.floor(z/this.size)}`)||[])
      .some(b => x>=b.minX-r&&x<=b.maxX+r&&z>=b.minZ-r&&z<=b.maxZ+r);
  }
  move(p,dx,dz,r=RULES.playerRadius) {
    const steps=Math.max(1,Math.ceil(Math.hypot(dx,dz)/.22)); let {x,z}=p;
    for(let i=0;i<steps;i++) { if(!this.blocked(x+dx/steps,z,r))x+=dx/steps; if(!this.blocked(x,z+dz/steps,r))z+=dz/steps; }
    return {x,z};
  }
  clear(a,b,r=.05) {
    const steps=Math.max(1,Math.ceil(distance(a,b)/.2));
    for(let i=0;i<=steps;i++)if(this.blocked(a.x+(b.x-a.x)*i/steps,a.z+(b.z-a.z)*i/steps,r))return false;
    return true;
  }
  distances(goal) {
    if(this.fields.has(goal))return this.fields.get(goal);
    const d=new Int16Array(this.cells.length).fill(-1);
    if(goal>=0&&goal<d.length) {
      const q=[goal];d[goal]=0;
      for(let head=0;head<q.length;head++)for(const n of this.graph[q[head]])if(d[n]<0){d[n]=d[q[head]]+1;q.push(n);}
    }
    if(this.fields.size>=64)this.fields.delete(this.fields.keys().next().value);
    this.fields.set(goal,d);return d;
  }
  next(from,goal) {
    if(from===goal)return goal;
    const d=this.distances(goal);
    return (this.graph[from]||[]).find(n=>d[n]>=0&&d[n]<d[from]) ?? from;
  }
}
export function trapWindow(t,s) {
  if(!s || s.phase==='setup' || !finite(s.startAt))return {armedAt:Infinity,expiresAt:Infinity};
  const armedAt=Math.max(finite(s.startAt),finite(t.armedAt)||finite(t.placedAt));
  return {armedAt,expiresAt:armedAt+finite(t.ttlMs,RULES.ttl[t.type]||15000)};
}
export function usableTrap(t,s,now) {
  return Object.hasOwn(RULES.cost,t.type)&&Number.isFinite(t.x)&&Number.isFinite(t.z)&&
    (!t.runId||t.runId===runKey(s))&&trapWindow(t,s).expiresAt>now;
}
export function energy(s,now) { return clamp(finite(s.dmEnergyBase)+Math.max(0,now-finite(s.dmEnergyStamp,now))*.006,0,100); }
export function placementError(type,p,s,maze,players,traps,now) {
  if(!s||!['setup','play'].includes(s.phase))return 'The round is not active.';
  const cell=maze.cell(p);
  if(cell<0||maze.blocked(p.x,p.z))return 'Choose a clear cell inside the maze.';
  if(type==='crown'){
    if(s.phase!=='setup')return 'The crown cannot be moved after play starts.';
    if(maze.distances(maze.entry)[cell]<3)return 'Place the crown farther from the entrance.';
    if([...traps.values()].some(t=>usableTrap(t,s,now)&&distance(p,t)<=maze.size*2))return 'Place the crown away from existing traps.';
    return '';
  }
  if(!Object.hasOwn(RULES.cost,type))return 'Unknown trap type.';
  if(maze.distances(maze.entry)[cell]<3)return 'Keep the entrance and respawn route clear.';
  const roster=[...players.values()].filter(p=>living(p,now,s));
  for(const player of roster)if(distance(p,player)<=maze.size*2)return `Too close to ${player.name||'a player'}.`;
  const crown=s.carrierId ? players.get(s.carrierId) : s.crown;
  if(crown&&distance(p,crown)<=maze.size*2)return 'Too close to the crown.';
  const active=[...traps.values()].filter(t=>usableTrap(t,s,now));
  if(active.length>=RULES.maxTraps)return 'The active trap limit has been reached.';
  if(active.some(t=>distance(p,t)<maze.size*.85))return 'Leave space between spawns.';
  if(['ghost','demon'].includes(type)&&active.filter(t=>['ghost','demon'].includes(t.type)).length>=Math.min(12,Math.max(4,roster.length*2+2)))return 'The enemy limit has been reached for this party.';
  return '';
}
export function chooseSpawn(type,s,maze,players,traps,now) {
  const roster=[...players.values()].filter(p=>living(p,now,s)&&maze.cell(p)>=0);
  const fromEntry=maze.distances(maze.entry), candidates=[];
  for(let i=0;i<maze.cells.length;i++) {
    const p=maze.center(i);
    if(placementError(type,p,s,maze,players,traps,now))continue;
    const near=roster.length?Math.min(...roster.map(p=>maze.distances(maze.cell(p))[i])):fromEntry[i];
    const score=type==='crown'?-Math.abs(fromEntry[i]-55):-Math.abs(near-6)+(maze.graph[i].length>=3?.8:0);
    candidates.push({p,score:score+(hash(`${s.seed}:${type}:${i}:${Math.floor(now/2000)}`)%100)*.002});
  }
  candidates.sort((a,b)=>b.score-a.score);return candidates[0]?.p||null;
}
export function stepEnemies(maze,s,traps,players,previous,dt,now) {
  const out={}, roster=[...players].filter(([,p])=>targetable(p,now,s)&&maze.cell(p)>=0), assigned=new Map();
  for(const [id,t] of [...traps].sort(([a],[b])=>a.localeCompare(b))) {
    const window=trapWindow(t,s);
    if(!usableTrap(t,s,now)||!['ghost','demon'].includes(t.type)||now<window.armedAt)continue;
    const old=previous[id],e=old?{...old}:{x:t.x,z:t.z,mode:'patrol',targetId:'',goal:maze.cell(t),waypoint:-1,thinkAt:0,lockUntil:0};
    if(maze.cell(e)<0||maze.blocked(e.x,e.z,.25)){e.x=t.x;e.z=t.z;e.waypoint=-1;}
    const cell=maze.cell(e);
    if(now>=finite(e.thinkAt)) {
      e.thinkAt=now+250;
      const choices=roster.map(([pid,p])=>{
        const steps=maze.distances(maze.cell(p))[cell];
        return {pid,p,steps,score:steps*(pid===s.carrierId?.7:1)+(assigned.get(pid)||0)*2-(pid===e.targetId?1.2:0)};
      }).filter(c=>c.steps>=0&&(c.steps<=12||c.pid===s.carrierId));
      choices.sort((a,b)=>a.score-b.score||a.pid.localeCompare(b.pid));
      const locked=choices.find(c=>c.pid===e.targetId), target=now<finite(e.lockUntil)&&locked?locked:choices[0];
      if(target) {
        if(e.targetId!==target.pid)e.lockUntil=now+1300;
        e.targetId=target.pid;e.goal=maze.cell(target.p);e.lastKnown=e.goal;e.forgetAt=now+3500;e.mode='chase';
        if(t.type==='ghost'&&target.steps>2) {
          const predicted={x:target.p.x+clamp(finite(target.p.vx),-5.4,5.4)*.8,z:target.p.z+clamp(finite(target.p.vz),-5.4,5.4)*.8};
          const ahead=maze.cell(predicted);
          e.goal=target.pid===s.carrierId?maze.next(e.goal,maze.entry):ahead>=0&&maze.clear(target.p,predicted)?ahead:e.goal;
          e.mode='intercept';
        }
      } else {
        e.targetId='';
        if(now<finite(e.forgetAt)&&Number.isInteger(e.lastKnown)){e.goal=e.lastKnown;e.mode='search';}
        else {
          e.mode='patrol';
          if(!Number.isInteger(e.goal)||e.goal<0||distance(e,maze.center(e.goal))<.2) {
            const ns=maze.graph[cell].filter(n=>n!==e.previousCell), options=ns.length?ns:maze.graph[cell];
            e.goal=options[hash(`${id}:${Math.floor(now/3000)}`)%options.length]??cell;e.previousCell=cell;
          }
        }
      }
    }
    if(e.targetId)assigned.set(e.targetId,(assigned.get(e.targetId)||0)+1);
    const target=players.get(e.targetId);let destination;
    if(e.waypoint>=0&&distance(e,maze.center(e.waypoint))>.08)destination=maze.center(e.waypoint);
    else if(cell===e.goal&&target&&maze.cell(target)===cell&&e.mode==='chase'){destination=target;e.waypoint=-1;}
    else {
      const center=maze.center(cell);
      e.waypoint=distance(e,center)>.12?cell:maze.next(cell,e.goal);
      destination=maze.center(e.waypoint);
    }
    const length=distance(e,destination), speed=(t.type==='demon'?2.65:2.25)*(e.mode==='patrol'?.65:1);
    const step=Math.min(length,speed*clamp(dt,0,.1));
    if(length>.001) {
      const p=maze.move(e,(destination.x-e.x)/length*step,(destination.z-e.z)/length*step,.25);
      if(distance(e,p)<step*.2)e.waypoint=cell;
      e.vx=(p.x-e.x)/Math.max(dt,.001);e.vz=(p.z-e.z)/Math.max(dt,.001);e.x=p.x;e.z=p.z;
    }else{e.vx=0;e.vz=0;}
    out[id]=e;
  }
  return out;
}
export function remotePosition(p,now,maze) {
  const age=clamp(now-(stampMs(p.serverAt)||finite(p.updatedAt,now)),0,350)/1000;
  const dx=clamp(finite(p.vx),-5.4,5.4)*age,dz=clamp(finite(p.vz),-5.4,5.4)*age;
  return maze.move({x:finite(p.x),z:finite(p.z)},dx,dz);
}
export function hazardAt(p,previous,s,traps,enemies,maze,now) {
  if(s?.phase!=='play'||!targetable(p,now,s))return null;
  for(const [id,t] of traps) {
    if(!usableTrap(t,s,now)||now<trapWindow(t,s).armedAt||t.type==='fog')continue;
    const e=t.type==='fire'?t:enemies[id];if(!e)continue;
    const radius=t.type==='fire'?1.6:1.05;
    if(distance(p,e)<radius&&maze.clear(p,e))return {id,type:t.type};
    if(previous&&distance(previous,p)<1&&maze.clear(previous,p)) {
      const dx=p.x-previous.x,dz=p.z-previous.z,l=dx*dx+dz*dz;
      const u=l?clamp(((e.x-previous.x)*dx+(e.z-previous.z)*dz)/l,0,1):0;
      const closest={x:previous.x+dx*u,z:previous.z+dz*u};
      if(distance(closest,e)<radius&&maze.clear(closest,e))return {id,type:t.type};
    }
  }
  return null;
}
// Acknowledgement-based baseline; only one write may be outstanding. Never queue frames.
export class LatestWriter {
  constructor(write,{now=Date.now,onError=()=>{},interval=RULES.moveMs,heartbeat=RULES.heartbeatMs}={}) {
    Object.assign(this,{write,now,onError,interval,heartbeat});this.pending=null;this.last=null;this.active=null;this.lastAck=-Infinity;this.lastAttempt=-Infinity;this.retryAt=0;this.failures=0;this.closed=false;
  }
  offer(value) { this.pending={...value}; }
  changed(a,b) {
    return !b||distance(a,b)>.06||Math.abs(Math.atan2(Math.sin(a.yaw-b.yaw),Math.cos(a.yaw-b.yaw)))>.04||
      ['alive','deadUntil','hidden','ready','runId','online','sessionId'].some(k=>a[k]!==b[k]);
  }
  flush(force=false) {
    if(this.active)return this.active;
    const now=this.now();
    if(this.closed||!this.pending||now<this.retryAt||(!force&&now-this.lastAttempt<this.interval)||
      (!force&&!this.changed(this.pending,this.last)&&now-this.lastAck<this.heartbeat))return Promise.resolve(false);
    const value={...this.pending};this.lastAttempt=now;
    this.active=Promise.resolve().then(()=>this.write(value)).then(()=>{
      this.last=value;this.lastAck=this.now();this.failures=0;this.retryAt=0;return true;
    }).catch(error=>{
      this.retryAt=this.now()+Math.min(16000,500*2**Math.min(5,++this.failures));this.onError(error);return false;
    }).finally(()=>{this.active=null;});
    return this.active;
  }
  close() { this.closed=true;this.pending=null; }
}
