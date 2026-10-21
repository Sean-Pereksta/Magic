/* Small, browser-independent runtime primitives. No save data is stored here. */
(function(root, factory){
  const api = factory();
  if(typeof module === 'object' && module.exports) module.exports = api;
  root.DwarfWorldPerformance = api;
})(typeof globalThis === 'object' ? globalThis : this, function(){
  'use strict';
  class FixedClock {
    constructor(step = 1/30, maxSteps = 3){ this.step=step; this.maxSteps=maxSteps; this.accumulator=0; this.dropped=0; }
    advance(delta, callback, active=true){
      if(!active){ this.accumulator=0; return 0; }
      const elapsed=Math.max(0, Number.isFinite(delta) ? delta : 0);
      this.accumulator += Math.min(elapsed, this.step*this.maxSteps);
      this.dropped += Math.max(0,elapsed-this.step*this.maxSteps);
      let count=0;
      while(this.accumulator+1e-10 >= this.step && count<this.maxSteps){
        this.accumulator=Math.max(0,this.accumulator-this.step); callback(this.step); count++;
      }
      return count;
    }
  }
  function interpolatePosition(entity,previous,alpha,maxDelta=3){
    if(!previous || (entity.x-previous.x)**2+(entity.y-previous.y)**2>maxDelta*maxDelta) return {x:entity.x,y:entity.y};
    const fraction=Math.max(0,Math.min(1,Number.isFinite(alpha)?alpha:1));
    return {x:previous.x+(entity.x-previous.x)*fraction,y:previous.y+(entity.y-previous.y)*fraction};
  }
  class SpatialHash {
    constructor(size=12){ this.size=size; this.cells=new Map(); this.locations=new Map(); }
    key(x,y){ return `${Math.floor(x/this.size)},${Math.floor(y/this.size)}`; }
    clear(){ this.cells.clear(); this.locations.clear(); }
    rebuild(items){ this.clear(); for(const item of items) this.update(item); }
    update(item){
      const key=this.key(item.x,item.y), old=this.locations.get(item);
      if(old===key) return;
      if(old!==undefined){ const bucket=this.cells.get(old); bucket.delete(item); if(!bucket.size) this.cells.delete(old); }
      let bucket=this.cells.get(key); if(!bucket) this.cells.set(key,bucket=new Set());
      bucket.add(item); this.locations.set(item,key);
    }
    query(x,y,r){
      const out=[];
      for(let cy=Math.floor((y-r)/this.size);cy<=Math.floor((y+r)/this.size);cy++)
        for(let cx=Math.floor((x-r)/this.size);cx<=Math.floor((x+r)/this.size);cx++){
          const bucket=this.cells.get(`${cx},${cy}`); if(bucket) for(const item of bucket) out.push(item);
        }
      return out;
    }
  }
  class NavigationQueue {
    constructor(width,height,walkable,{maxNodes=220000,maxQueued=96,maxCache=256}={}){
      this.width=width; this.height=height; this.walkable=walkable;
      this.maxNodes=Math.min(maxNodes,width*height); this.maxQueued=maxQueued; this.maxCache=maxCache;
      this.seen=new Uint32Array(width*height); this.previous=new Int32Array(width*height);
      this.queue=new Int32Array(this.maxNodes); this.mark=0; this.revision=0; this.time=0;
      this.pending=[]; this.entries=new Map(); this.active=null;
      this.regionalRoutes=new Map(); this.regionalCells=0; this.regionColumns=Math.ceil(width/16);
      this.stats={expanded:0,requests:0,cacheHits:0,regionalHits:0,completed:0,deferred:0,canceled:0};
    }
    invalidate(blocking=true){
      this.revision++;
      if(blocking){ this.entries.clear(); this.pending.length=0; this.active=null; this.regionalRoutes.clear(); this.regionalCells=0; }
      else for(const [key,entry] of this.entries) if(entry.state==='failed') this.entries.delete(key);
    }
    request(sx,sy,key,goal,{maxNodes=this.maxNodes,enemy=false,closest=null}={}){
      if(sx<0||sy<0||sx>=this.width||sy>=this.height) return null;
      const cacheKey=`${sx},${sy}:${enemy?'enemy:':''}${key}`;
      const existing=this.entries.get(cacheKey);
      this.stats.requests++;
      if(existing){
        if(existing.state==='pending'){ existing.lastRequest=this.time; return null; }
        if(existing.expires>this.time){
          this.stats.cacheHits++;
          return existing.path ? existing.path.map(p=>({x:p.x,y:p.y})) : null;
        }
        this.entries.delete(cacheKey);
      }
      if(this.pending.length+(this.active?1:0)>=this.maxQueued){ this.stats.deferred++; return null; }
      const routeKey=key.startsWith('point:')?`${enemy?'enemy:':''}${key}`:null;
      const job={key:cacheKey,routeKey,sx,sy,goal,enemy,closest,maxNodes:Math.min(maxNodes,this.maxNodes),state:'pending',revision:this.revision,lastRequest:this.time};
      this.entries.set(cacheKey,job); this.pending.push(job); return null;
    }
    start(job){
      if(++this.mark===0xffffffff){ this.seen.fill(0); this.mark=1; }
      this.active=job; job.head=0; job.tail=1; job.start=job.sx+job.sy*this.width;
      job.best=job.start; job.bestDistance=Infinity;
      job.regional=job.routeKey?this.regionalRoutes.get(job.routeKey):null;
      this.queue[0]=job.start; this.seen[job.start]=this.mark; this.previous[job.start]=-1;
    }
    rememberRegionalRoute(job,path){
      // A bounded shared route, partitioned into 16x16 regions. This is not a
      // coarse portal graph: joining searches still prove a walkable tile path.
      if(!job.routeKey || !path?.length || path.length>2048) return;
      const previous=this.regionalRoutes.get(job.routeKey);
      if(previous && previous.path.length>=path.length) return;
      if(previous){this.regionalCells-=previous.cells;this.regionalRoutes.delete(job.routeKey);}
      const route={path,regions:new Map(),cells:path.length+1};
      const index=(x,y,offset)=>{
        const region=Math.floor(x/16)+Math.floor(y/16)*this.regionColumns;
        let cells=route.regions.get(region); if(!cells)route.regions.set(region,cells=new Map());
        cells.set(x+y*this.width,offset);
      };
      index(job.sx,job.sy,-1);
      for(let i=0;i<path.length;i++)index(path[i].x,path[i].y,i);
      this.regionalRoutes.set(job.routeKey,route);this.regionalCells+=route.cells;
      while(this.regionalCells>8192 || this.regionalRoutes.size>32){
        const key=this.regionalRoutes.keys().next().value;
        this.regionalCells-=this.regionalRoutes.get(key).cells;this.regionalRoutes.delete(key);
      }
    }
    finish(job,end,sharedOffset=null,reachedGoal=true){
      let path=null;
      if(end!==null){
        path=[];
        for(let cur=end;cur!==job.start && cur!==-1;cur=this.previous[cur]) path.push({x:cur%this.width,y:Math.floor(cur/this.width)});
        path.reverse();
        if(sharedOffset!==null){path=path.concat(job.regional.path.slice(sharedOffset+1));this.stats.regionalHits++;}
      }
      job.path=path; job.state=path?'ready':'failed'; job.expires=this.time+(path?5:1.2);
      // A tile opened during this search: a failure can be retried against the new topology.
      if(!path && job.revision!==this.revision) job.expires=this.time;
      // Closest-approach fallbacks are useful to this caller but must never be
      // advertised to other starts as a complete route to an unreachable goal.
      if(reachedGoal) this.rememberRegionalRoute(job,path);
      job.regional=null;job.goal=null;job.closest=null;
      this.active=null; this.stats.completed++;
      while(this.entries.size>this.maxCache){
        let removed=false;
        for(const [key,entry] of this.entries) if(entry.state!=='pending'){ this.entries.delete(key); removed=true; break; }
        if(!removed) break;
      }
    }
    tick(budget=6000,dt=1/30){
      this.time+=Math.max(0,dt); let expanded=0;
      while(expanded<budget){
        if(!this.active){ const next=this.pending.shift(); if(!next) break; this.start(next); }
        const job=this.active;
        // Moving targets, changed jobs and dead agents must not retain obsolete work.
        if(this.time-job.lastRequest>3){this.entries.delete(job.key);this.active=null;this.stats.canceled++;continue;}
        if(job.head>=job.tail){ this.finish(job,job.closest && job.best!==job.start ? job.best : null,null,false); continue; }
        const ii=this.queue[job.head++],x=ii%this.width,y=Math.floor(ii/this.width); expanded++;
        if(job.goal(x,y,ii)){ this.finish(job,ii); continue; }
        if(job.regional){
          const region=Math.floor(x/16)+Math.floor(y/16)*this.regionColumns;
          const offset=job.regional.regions.get(region)?.get(ii);
          if(offset!==undefined){this.finish(job,ii,offset);continue;}
        }
        if(job.closest){ const distance=Math.abs(x-job.closest.x)+Math.abs(y-job.closest.y); if(distance<job.bestDistance){job.bestDistance=distance; job.best=ii;} }
        const neighbors=[x+1<this.width?ii+1:-1,x>0?ii-1:-1,y+1<this.height?ii+this.width:-1,y>0?ii-this.width:-1];
        for(const ni of neighbors){
          if(ni<0||this.seen[ni]===this.mark||job.tail>=job.maxNodes) continue;
          if(!this.walkable(ni%this.width,Math.floor(ni/this.width),job.enemy)) continue;
          this.seen[ni]=this.mark; this.previous[ni]=ii; this.queue[job.tail++]=ni;
        }
      }
      this.stats.expanded+=expanded; return expanded;
    }
  }
  return {FixedClock,SpatialHash,NavigationQueue,interpolatePosition};
});
