// Ground troops share routes; climbers and wall defenders use their explicit
// rungs and stairs. These derived arrays are rebuilt on load and never saved.
const CELL = 4;
const HALF_FIELD = 80;
const layouts = new WeakMap();

function buildGrid(layout, side, blocked) {
  const cols = HALF_FIELD*2/CELL, rows = cols, size = cols*rows;
  const centers = Array.from({length:size}, (_,i)=>({
    x:(i % cols + .5) * CELL - HALF_FIELD, z:(Math.floor(i / cols) + .5) * CELL - HALF_FIELD
  }));
  const open = centers.map(p=>!blocked(p.x,p.z));
  const edges = new Int16Array(size*4).fill(-1);
  for (let i=0;i<size;i++) {
    if (!open[i]) continue;
    const p=centers[i];
    if (i % cols < cols-1 && open[i+1] && groundLineClear(layout,p,centers[i+1],side)) {
      edges[i*4]=i+1; edges[(i+1)*4+1]=i;
    }
    if (i+cols<size && open[i+cols] && groundLineClear(layout,p,centers[i+cols],side)) {
      edges[i*4+2]=i+cols; edges[(i+cols)*4+3]=i;
    }
  }
  const region=new Int16Array(size).fill(-1), members=[];
  for (let i=0;i<size;i++) {
    if (!open[i] || region[i]!==-1) continue;
    const id=members.length, cells=[i]; members.push(cells); region[i]=id;
    for (let n=0;n<cells.length;n++) {
      const base=cells[n]*4;
      for (let d=0;d<4;d++) {
        const next=edges[base+d];
        if (next<0 || region[next]!==-1) continue;
        region[next]=id; cells.push(next);
      }
    }
  }
  return {cols,rows,edges,region,members,centers,queue:new Int16Array(size),flows:new Map()};
}

function cacheFor(layout) {
  let cached=layouts.get(layout);
  if (!cached || cached.revision!==layout.navRevision || cached.obstacles!==layout.obstacles || cached.count!==layout.obstacles.length) {
    cached={revision:layout.navRevision,obstacles:layout.obstacles,count:layout.obstacles.length,sides:[],geometry:[]};
    layouts.set(layout,cached);
  }
  return cached;
}

function gridFor(layout, side, blocked) {
  const cached=cacheFor(layout);
  // Defenders may walk through their own gates. Attackers use breached gates.
  return cached.sides[side] ||= buildGrid(layout,side,blocked);
}

function groundObstacles(layout,side) {
  const geometry=[];
  const box=(left,right,front,rear)=>{
    if (left<=right && front<=rear) geometry.push({left,right,front,rear});
  };
  for (const o of layout.obstacles) {
    if (o.radius) geometry.push({x:o.x,z:o.z,radius:o.radius+.35});
    else box(o.x-o.width/2-.35,o.x+o.width/2+.35,o.z-o.depth/2-.35,o.z+o.depth/2+.35);
  }
  for (const w of [...layout.walls,layout.gate].filter(Boolean)) {
    if ((w.kind==='gate' || w===layout.gate) && side===1) continue;
    const left=w.x-w.width/2-.35, right=w.x+w.width/2+.35, front=w.z-w.depth/2-.35, rear=w.z+w.depth/2+.35;
    if (w.hp>0) box(left,right,front,rear);
    else if (w.breach) {
      const gap=w.breach.width/2-.35;
      if (w.width>w.depth) { box(left,w.breach.x-gap,front,rear); box(w.breach.x+gap,right,front,rear); }
      else { box(left,right,front,w.breach.z-gap); box(left,right,w.breach.z+gap,rear); }
    }
  }
  return geometry;
}

// Swept segments also catch the corner of a gatehouse between grid samples.
// Without this, a ram can "see" its goal while its very next step hits stone.
export function groundLineClear(layout,from,to,side) {
  if (Math.max(Math.abs(to.x),Math.abs(to.z))>78.5) return false;
  const cache=cacheFor(layout);
  const geometry=cache.geometry[side] ||= groundObstacles(layout,side);
  const dx=to.x-from.x, dz=to.z-from.z, lengthSq=dx*dx+dz*dz;
  const left=Math.min(from.x,to.x), right=Math.max(from.x,to.x), front=Math.min(from.z,to.z), rear=Math.max(from.z,to.z);
  for (const o of geometry) {
    if (o.radius) {
      const t=lengthSq ? Math.max(0,Math.min(1,((o.x-from.x)*dx+(o.z-from.z)*dz)/lengthSq)) : 0;
      if ((from.x+dx*t-o.x)**2+(from.z+dz*t-o.z)**2<o.radius**2) return false;
      continue;
    }
    if (right<o.left || left>o.right || rear<o.front || front>o.rear) continue;
    const tx0=dx ? (o.left-from.x)/dx : -Infinity, tx1=dx ? (o.right-from.x)/dx : Infinity;
    const tz0=dz ? (o.front-from.z)/dz : -Infinity, tz1=dz ? (o.rear-from.z)/dz : Infinity;
    const enter=Math.max(0,Math.min(tx0,tx1),Math.min(tz0,tz1));
    if (enter<=Math.min(1,Math.max(tx0,tx1),Math.max(tz0,tz1))) return false;
  }
  return true;
}

function endpoint(grid, point, region, visible) {
  const col=Math.max(0,Math.min(grid.cols-1,Math.floor((point.x+HALF_FIELD)/CELL)));
  const row=Math.max(0,Math.min(grid.rows-1,Math.floor((point.z+HALF_FIELD)/CELL)));
  let best=-1, bestDistance=Infinity;
  for (let z=Math.max(0,row-1);z<=Math.min(row+1,grid.rows-1);z++) {
    for (let x=Math.max(0,col-1);x<=Math.min(col+1,grid.cols-1);x++) {
      const i=z*grid.cols+x;
      if (grid.region[i]<0 || region!==null && grid.region[i]!==region) continue;
      const p=grid.centers[i], dist=(point.x-p.x)**2+(point.z-p.z)**2;
      if (dist<bestDistance && visible(point,p)) { best=i; bestDistance=dist; }
    }
  }
  return best;
}

function flowTo(grid, goal) {
  let nextStep=grid.flows.get(goal);
  if (nextStep) return nextStep;
  nextStep=new Int16Array(grid.centers.length).fill(-1);
  const queue=grid.queue; queue[0]=goal; nextStep[goal]=goal;
  for (let head=0,tail=1;head<tail;head++) {
    const at=queue[head];
    for (let d=0;d<4;d++) {
      const neighbor=grid.edges[at*4+d];
      if (neighbor<0 || nextStep[neighbor]!==-1) continue;
      nextStep[neighbor]=at; queue[tail++]=neighbor;
    }
  }
  if (grid.flows.size>=96) grid.flows.delete(grid.flows.keys().next().value);
  grid.flows.set(goal,nextStep);
  return nextStep;
}

export function groundRoute(layout, from, target, blocked) {
  const visible=(a,b)=>groundLineClear(layout,a,b,from.side);
  const targetOpen=!blocked(target.x,target.z);
  if (targetOpen && visible(from,target)) return [{x:target.x,z:target.z}];
  const grid=gridFor(layout,from.side,blocked);
  const start=endpoint(grid,from,null,visible);
  if (start<0) return [];
  const region=grid.region[start];
  let goal=targetOpen ? endpoint(grid,target,region,visible) : -1;
  const reachable=goal>=0;
  if (!reachable) {
    // March to the nearest reachable side of an obstruction. Never append a
    // destination in a sealed ward as if the route had entered the courtyard.
    let nearest=Infinity;
    for (const i of grid.members[region]) {
      const p=grid.centers[i], d=(p.x-target.x)**2+(p.z-target.z)**2;
      if (d<nearest) { nearest=d; goal=i; }
    }
  }
  const next=flowTo(grid,goal), path=[];
  let at=start;
  if (at!==goal && !visible(from,grid.centers[next[at]])) path.push({...grid.centers[at]});
  while (at!==goal) { at=next[at]; path.push({...grid.centers[at]}); }
  if (reachable && (start!==goal || visible(from,target))) path.push({x:target.x,z:target.z});
  else if (reachable) path.push({...grid.centers[goal]},{x:target.x,z:target.z});
  return path;
}
