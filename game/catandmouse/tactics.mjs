// Bounded host decisions. Nothing in here runs in a presentation frame.
const dirs = [[1,0],[0,1],[-1,0],[0,-1]];
const distance = (a,b) => Math.abs(a.x-b.x)+Math.abs(a.y-b.y);
const cellKey = p => `${p.x},${p.y}`;
export function stableHash(text) {
  let result = 2166136261;
  for (const char of String(text)) result = Math.imul(result ^ char.charCodeAt(0), 16777619);
  return result >>> 0;
}

export function findOpenSpawn(origin, { size, passable, occupied = () => false, reserved = () => false, radius = 3, seed = 0 }) {
  // Search only reachable cells in a small ring, never through a wall. All
  // cells are visited at most once; a fully enclosed producer simply retries.
  const queue = [{...origin,d:0}], seen = new Set([cellKey(origin)]);
  for (let head=0; head<queue.length; head++) {
    const current = queue[head];
    if (current.d && !occupied(current.x,current.y) && !reserved(current.x,current.y)) return {x:current.x,y:current.y};
    if (current.d >= radius) continue;
    for (let i=0; i<4; i++) {
      const [dx,dy] = dirs[(i+seed)%4];
      const p = {x:current.x+dx,y:current.y+dy,d:current.d+1}, key = cellKey(p);
      if (p.x<0 || p.y<0 || p.x>=size || p.y>=size || seen.has(key) || !passable(p.x,p.y)) continue;
      seen.add(key); queue.push(p);
    }
  }
  return null;
}

export function createReservations({ now = Date.now, ttlMs = 500 } = {}) {
  const cells = new Map();
  const prune = () => { const time=now(); for (const [key,value] of cells) if (value.until<=time) cells.delete(key); };
  return {
    reserve(x,y,owner,ttl = ttlMs) { cells.set(`${x},${y}`,{owner,until:now()+ttl}); },
    blocked(x,y,owner) { const value=cells.get(`${x},${y}`); if (!value) return false;
      if (value.until<=now()) {cells.delete(`${x},${y}`);return false;} return value.owner!==owner; },
    release(owner) { for (const [key,value] of cells) if (value.owner===owner) cells.delete(key); },
    prune, clear:()=>cells.clear(), get size() { prune(); return cells.size; }
  };
}

export function scoreObjective(unit, target, pressure = 1, assignments = 0) {
  let value = target.value || 0;
  if (unit.team === 'friendly') {
    value = 12 + (target.playerThreat ? 14 : 0) + (target.infrastructureThreat ? 6 : 0);
    if (unit.spawnX != null && distance({x:unit.spawnX,y:unit.spawnY},target)<=3) value += 12;
    if (target.kind==='ratking' || target.kind==='ox') value += 3;
  } else if (target.type==='mouse') value += unit.kind==='cat' ? 26 : unit.kind==='vulture' ? 20 : 14;
  else if (target.type==='rabbit' || target.type==='flea') value += distance(unit,target)<=4 ? 13 : 4;
  else if (target.type==='structure') {
    value += 7;
    if (unit.kind==='ox') value += target.wall ? 22 : target.defense ? 18 : 2;
    if (unit.kind==='termite') value += 28 + (1-(target.healthFraction ?? 1))*12;
    if (unit.kind==='stinkrat') value += target.disruptable ? (target.disabled ? -18 : 30) : -8;
    if (unit.kind==='ratking') value += target.defense || target.support ? 17 : 8;
    if (unit.kind==='vulture') value += target.economy || target.support ? 22 : target.wall ? -12 : 2;
  }
  return value*(0.85+pressure*0.15) - distance(unit,target)*3 - assignments*5
    + (stableHash(`${unit.key}:${target.key}`)%101)/101;
}

export function createTacticalDirector({ size, now = Date.now, findPath, passable, targets, occupants,
  revision = () => 0, resolveTarget = target => target, maxDecisions = () => 5, maxPaths = 10, windowMs = 200 }) {
  const brains = new Map(), reservations = createReservations({now});
  let window = -1, decisions = 0, paths = 0, snapshot = null, occupied = new Map();
  const stats = {decisions:0,paths:0,peakDecisions:0,peakPaths:0};
  function begin() {
    const next=Math.floor(now()/windowMs);
    if (next===window) return;
    window=next; decisions=0; paths=0; snapshot=null; occupied=new Map(); reservations.prune();
    const present=new Set();
    for (const unit of occupants()) {
      present.add(unit.key);
      const key=cellKey(unit); if (!occupied.has(key)) occupied.set(key,[]);
      occupied.get(key).push(unit);
    }
    for (const key of brains.keys()) if (!present.has(key)) {brains.delete(key);reservations.release(key);}
  }
  function getTargets(team) { if (!snapshot) snapshot=targets(); return snapshot[team] || []; }
  function takePath(unit,target) {
    if (paths>=maxPaths) return undefined;
    paths++; stats.paths++; stats.peakPaths=Math.max(stats.peakPaths,paths);
    const route=findPath(unit.x,unit.y,target.x,target.y,(x,y)=>(x===target.x && y===target.y) || passable(unit.kind,x,y));
    if (route && target.type==='structure') route.pop(); // Stop at the breach, never enter it.
    return route;
  }
  function plan(unit, {stopRange = 0, pressure = 1, reactionMs = 600, holdMs = 1600} = {}) {
    begin();
    const time=now(), list=getTargets(unit.team).map(resolveTarget).filter(Boolean);
    let brain=brains.get(unit.key);
    if (!brain) {brain={target:null,path:[],nextThink:0,holdUntil:0,stuck:0,previous:null,revision:revision(),failed:new Map()};brains.set(unit.key,brain);}
    let target=list.find(t=>t.key===brain.target) || null;
    if (!target) {brain.target=null;brain.path=[];}
    const endpointMoved=target && (brain.tx!==target.x || brain.ty!==target.y);
    const stale=brain.revision!==revision() || endpointMoved || (brain.path[0] && (distance(unit,brain.path[0])!==1 || !passable(unit.kind,brain.path[0].x,brain.path[0].y)));
    if (stale) brain.path=[];
    const rangeFor = target => target.type==='structure' ? Math.max(1,stopRange) : stopRange;
    const atRange=target && distance(unit,target)<=rangeFor(target);
    const needsDecision=(!target && time>=brain.nextThink) || (target && (stale || brain.stuck>=3 || (!atRange && !brain.path.length) || time>=brain.nextThink));
    if (needsDecision && decisions<maxDecisions()) {
      decisions++;stats.decisions++;stats.peakDecisions=Math.max(stats.peakDecisions,decisions);
      for (const [key,until] of brain.failed) if (until<=time) brain.failed.delete(key);
      const claims = key => {let count=0;for (const [id,b] of brains) if(id!==unit.key && b.target===key) count++;return count;};
      let choices=list.filter(t=>!brain.failed.has(t.key));
      // Fleas keep cheap, nearby swarm objectives; termites always seek a
      // breach when structures exist. The shortlist limits full-grid work.
      if (unit.kind==='termite' && choices.some(t=>t.type==='structure')) choices=choices.filter(t=>t.type==='structure');
      choices.sort((a,b)=>distance(unit,a)-distance(unit,b));
      choices=choices.slice(0,16).sort((a,b)=>scoreObjective(unit,b,pressure,claims(b.key))-scoreObjective(unit,a,pressure,claims(a.key)));
      const challenger=choices[0];
      const gain=target && challenger ? scoreObjective(unit,challenger,pressure,claims(challenger.key))-scoreObjective(unit,target,pressure,claims(target.key)) : 0;
      if (target && brain.stuck<3 && !brain.failed.has(target.key) &&
          (!challenger || (time<brain.holdUntil ? gain<22 : gain<10))) {
        choices=[target];
      }
      let chosen=null, route;
      for (const candidate of choices.slice(0,3)) {
        if (candidate.key===brain.target && !stale && brain.path.length && brain.stuck<3) route=brain.path;
        else if (distance(unit,candidate)<=rangeFor(candidate)) route=[];
        else route=takePath(unit,candidate);
        if (route===undefined) break; // Shared path budget exhausted: reuse next tick.
        if (route!==null) {chosen=candidate;break;}
        brain.failed.set(candidate.key,time+900);
      }
      if (chosen) {
        if (chosen.key!==brain.target) brain.holdUntil=time+holdMs;
        brain.target=chosen.key;target=chosen;brain.path=route;
        brain.tx=target.x;brain.ty=target.y;brain.revision=revision();
      } else if (route===null) {brain.target=null;brain.path=[];target=null;}
      brain.nextThink=time+reactionMs+(stableHash(unit.key)%120);
    }
    if (!target || distance(unit,target)<=rangeFor(target)) {brain.stuck=0;return {target,next:null};}
    const preferred=brain.path[0];
    if (!preferred) {brain.stuck++;return {target,next:null};}
    const crowded = p => reservations.blocked(p.x,p.y,unit.key) || (occupied.get(cellKey(p)) || []).some(o=>o.key!==unit.key && o.team===unit.team);
    const valid = p => p.x>=0 && p.y>=0 && p.x<size && p.y<size && passable(unit.kind,p.x,p.y) && !crowded(p);
    let next=valid(preferred) && (brain.stuck>=3 || !brain.previous || cellKey(preferred)!==cellKey(brain.previous)) ? preferred : null;
    if (!next) {
      const offset=stableHash(unit.key)%4;
      const alternatives=dirs.map((_,i)=>{const [dx,dy]=dirs[(i+offset)%4];return {x:unit.x+dx,y:unit.y+dy};})
        .filter(p=>valid(p) && (brain.stuck>=3 || !brain.previous || cellKey(p)!==cellKey(brain.previous)) && distance(p,target)<=distance(unit,target)+1)
        .sort((a,b)=>distance(a,target)-distance(b,target));
      next=alternatives[0] || null;
    }
    if (!next) {brain.stuck++;if(brain.stuck>=3) brain.nextThink=0;return {target,next:null};}
    // Reserve both ends until the next decisions so groups cannot swap places.
    reservations.reserve(unit.x,unit.y,unit.key,240); reservations.reserve(next.x,next.y,unit.key,450);
    const old=occupied.get(cellKey(unit));if(old) occupied.set(cellKey(unit),old.filter(o=>o.key!==unit.key));
    const key=cellKey(next);if(!occupied.has(key))occupied.set(key,[]);occupied.get(key).push({...unit,...next});
    brain.previous={x:unit.x,y:unit.y};brain.stuck=0;
    if (cellKey(next)===cellKey(preferred)) brain.path.shift();else {brain.path=[];brain.nextThink=0;}
    return {target,next};
  }
  return { plan, reservations, reset(){brains.clear();reservations.clear();window=-1;},
    get stats(){return {...stats,brains:brains.size,reservations:reservations.size};} };
}
