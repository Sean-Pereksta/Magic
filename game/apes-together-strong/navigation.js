/* Clearance-aware shared corridors and prioritized, incremental A* work. */
(function(){
'use strict';
const clock=()=>typeof performance!=='undefined'?performance.now():Date.now();
class Heap {
 constructor(){this.items=[]}
 push(n){let a=this.items,i=a.length;a.push(n);while(i){let p=(i-1)>>1;if(a[p].f<=n.f)break;a[i]=a[p];i=p}a[i]=n}
 pop(){const a=this.items,top=a[0],last=a.pop();if(a.length){let i=0;while(i*2+1<a.length){let k=i*2+1;if(k+1<a.length&&a[k+1].f<a[k].f)k++;if(a[k].f>=last.f)break;a[i]=a[k];i=k}a[i]=last}return top}
}
class Navigation {
 constructor(world){
 this.world=world;this.cell=28;this.walkCache=new Map();this.segmentCache=new Map();this.routes=new Map();this.pending=new Map();this.cohorts=new Map();this.revision=-1;this.chunkRevision=-1;this.time=0;
 this.stats={searches:0,cacheHits:0,sharedHits:0,expanded:0,failures:0,requested:0,completed:0,deferred:0,dropped:0,queueLength:0,frameExpanded:0,frameMs:0};
 }
 beginFrame(time,options={}){
 this.time=time;
 if(this.revision!==(this.world.navRevision||0)){this.revision=this.world.navRevision||0;this.walkCache.clear();this.segmentCache.clear();this.routes.clear();this.pending.clear()}
 if(this.chunkRevision!==(this.world.chunkRevision||0)){this.chunkRevision=this.world.chunkRevision||0;this.walkCache.clear();this.segmentCache.clear()}
 if(this.walkCache.size>30000)this.walkCache.clear();if(this.segmentCache.size>9000)this.segmentCache.clear();if(this.routes.size>512)this.routes.delete(this.routes.keys().next().value);
 for(const [key,route]of this.routes)if(time-route.time>(route.path.length?8:1.1))this.routes.delete(key);
 for(const [key,cohort]of this.cohorts)if(time-cohort.touched>2)this.cohorts.delete(key);
 for(const [key,job]of this.pending)if(time-job.touched>3)this.pending.delete(key);
 const started=clock(),searches=this.stats.searches,budget=options.budgetMs??1.6,maxExpanded=options.maxExpanded??192,maxRequests=options.maxRequests??6,maxSearches=options.maxSearches??3;
 const jobs=Array.from(this.pending.values()).sort((a,b)=>(a.priority-Math.min(2,(time-a.created)*.8))-(b.priority-Math.min(2,(time-b.created)*.8))||a.created-b.created);
 let operations=0,expanded=0,visited=0;
 for(const job of jobs){
 if(visited++>=maxRequests)break;
 // Preparation, expansion and smoothing are all resumable, so a long route
 // cannot consume a complete search's cost during one simulation tick.
 while(operations<maxExpanded*2&&expanded<maxExpanded&&(operations===0||clock()-started<budget)){
 if(job.phase==='initialize'&&this.stats.searches-searches>=maxSearches)break;
 const before=job.expanded;const done=this._step(job);expanded+=job.expanded-before;operations++;
 if(done){this.pending.delete(job.key);this.stats.completed++;break}
 if(operations%32===0)break;
 }
 if(expanded>=maxExpanded||clock()-started>=budget)break;
 }
 this.stats.frameExpanded=expanded;this.stats.frameSearches=this.stats.searches-searches;this.stats.expanded+=expanded;this.stats.frameMs=clock()-started;this.stats.queueLength=this.pending.size;this.stats.deferred+=this.pending.size;
 }
 clearSegment(ax,ay,bx,by,r=10){
 const dx=bx-ax,dy=by-ay,length2=dx*dx+dy*dy,pad=r;
 if(this.world.boundsReady&&!this.world.boundsReady(Math.min(ax,bx)-pad,Math.min(ay,by)-pad,Math.max(ax,bx)+pad,Math.max(ay,by)+pad))return false;
 const cacheable=ax%this.cell===0&&ay%this.cell===0&&bx%this.cell===0&&by%this.cell===0;
 const key=cacheable?ax+','+ay+','+bx+','+by+','+r:null;
 if(key&&this.segmentCache.has(key))return this.segmentCache.get(key);
 let clear=true;
 if(this.world._queryCollision)this.world._queryCollision(Math.min(ax,bx)-pad,Math.min(ay,by)-pad,Math.max(ax,bx)+pad,Math.max(ay,by)+pad,o=>{
 if(!o.solid||o.dead||o.hp<=0)return;
 if(o.collision==='rect'){
 const hx=o._collision?.hx??(o.w||o.r*2)/2,hy=o._collision?.hy??(o.h||o.r*2)/2,minX=o.x-hx,maxX=o.x+hx,minY=o.y-hy,maxY=o.y+hy;
 let lo=0,hi=1,intersects=true;
 for(const [start,delta,min,max]of[[ax,dx,minX,maxX],[ay,dy,minY,maxY]]){if(Math.abs(delta)<1e-9){if(start<min||start>max){intersects=false;break}}else{let t1=(min-start)/delta,t2=(max-start)/delta;if(t1>t2)[t1,t2]=[t2,t1];lo=Math.max(lo,t1);hi=Math.min(hi,t2);if(lo>hi){intersects=false;break}}}
 if(intersects&&hi>=0&&lo<=1){clear=false;return false}
 // A swept circle has rounded corners. Expanding the rectangle to a larger
 // rectangle incorrectly traps valid actors just outside a wall corner.
 const endpointDistance=(x,y)=>{const px=Math.max(minX,Math.min(maxX,x)),py=Math.max(minY,Math.min(maxY,y));return(x-px)**2+(y-py)**2};
 let distance=Math.min(endpointDistance(ax,ay),endpointDistance(bx,by));
 for(const x of[minX,maxX])for(const y of[minY,maxY]){const t=length2?Math.max(0,Math.min(1,((x-ax)*dx+(y-ay)*dy)/length2)):0;distance=Math.min(distance,(ax+dx*t-x)**2+(ay+dy*t-y)**2)}
 if(distance<pad*pad){clear=false;return false}
 }else{const t=length2?Math.max(0,Math.min(1,((o.x-ax)*dx+(o.y-ay)*dy)/length2)):0,rr=(o._collision?.radius??o.moveRadius??o.r??15)+pad;if((ax+dx*t-o.x)**2+(ay+dy*t-o.y)**2<rr*rr){clear=false;return false}}
 });
 if(clear){
 // Obstacle intersections above are exact; only terrain needs sampling. This
 // removes the old repeated spatial collision query at every eight pixels.
 const steps=Math.max(1,Math.ceil(Math.sqrt(length2)/8));
 for(let i=0;i<=steps;i++){const x=ax+dx*i/steps,y=ay+dy*i/steps;if(this.world.waterBlocked?this.world.waterBlocked(x,y,r):this.world.blocked(x,y,r)){clear=false;break}}
 }
 if(key)this.segmentCache.set(key,clear);return clear;
 }
 walk(x,y,r){let key=x+','+y+','+r;if(!this.walkCache.has(key))this.walkCache.set(key,!this.world.blocked(x*this.cell,y*this.cell,r+1));return this.walkCache.get(key)}
 _nearestStep(job,which){
 let scan=job.scan;
 if(!scan){const point=which==='start'?job.from:job.to;scan=job.scan={cx:Math.round(point.x/this.cell),cy:Math.round(point.y/this.cell),ring:0,dx:0,dy:0,best:null,score:Infinity,point}}
 const {ring,dx,dy}=scan,x=scan.cx+dx,y=scan.cy+dy;
 if(Math.max(Math.abs(dx),Math.abs(dy))===ring&&this.walk(x,y,job.r)&&
 (which!=='start'||this.clearSegment(job.from.x,job.from.y,x*this.cell,y*this.cell,job.r))){const score=(x*this.cell-scan.point.x)**2+(y*this.cell-scan.point.y)**2;if(score<scan.score){scan.best={x,y};scan.score=score}}
 scan.dy++;
 if(scan.dy>ring){scan.dy=-ring;scan.dx++}
 if(scan.dx>ring){
 if(scan.best){job[which]=scan.best;job.scan=null;job.phase=which==='start'?'goal':'initialize';return false}
 scan.ring++;scan.dx=scan.dy=-scan.ring;if(scan.ring>6)return this._finish(job,[]);
 }
 return false;
 }
 _finish(job,path){this.routes.set(job.key,{time:this.time,path});if(!path.length)this.stats.failures++;return true}
 _step(job){
 if(job.phase==='start'||job.phase==='goal')return this._nearestStep(job,job.phase);
 if(job.phase==='initialize'){
 const {start,goal}=job;job.heap=new Heap();job.nodes=new Map();job.heuristic=(x,y)=>Math.hypot(goal.x-x,goal.y-y);
 const first={...start,g:0,f:job.heuristic(start.x,start.y),parent:null};job.nodes.set(start.x+','+start.y,first);job.heap.push(first);
 job.minX=Math.min(start.x,goal.x)-28;job.maxX=Math.max(start.x,goal.x)+28;job.minY=Math.min(start.y,goal.y)-28;job.maxY=Math.max(start.y,goal.y)+28;job.phase='expand';this.stats.searches++;return false;
 }
 if(job.phase==='expand'){
 if(!job.heap.items.length||job.expanded>=3200)return this._finish(job,[]);
 const cur=job.heap.pop();if(cur.closed)return false;cur.closed=true;job.expanded++;
 if(cur.x===job.goal.x&&cur.y===job.goal.y){job.path=[];job.cursor=cur;job.phase='reconstruct';return false}
 for(let dx=-1;dx<=1;dx++)for(let dy=-1;dy<=1;dy++){
 if(!dx&&!dy)continue;const x=cur.x+dx,y=cur.y+dy,r=job.r;
 if(x<job.minX||x>job.maxX||y<job.minY||y>job.maxY||!this.walk(x,y,r))continue;
 if(dx&&dy&&(!this.walk(cur.x+dx,cur.y,r)||!this.walk(cur.x,cur.y+dy,r)))continue;
 if(!this.clearSegment(cur.x*this.cell,cur.y*this.cell,x*this.cell,y*this.cell,r))continue;
 const terrain=this.world.terrain(x*this.cell,y*this.cell),cost=terrain.road?.92:terrain.biome==='wetland'?1.3:1,g=cur.g+(dx&&dy?Math.SQRT2:1)*cost,id=x+','+y,old=job.nodes.get(id);
 if(!old||g<old.g){const node={x,y,g,f:g+job.heuristic(x,y)*.92,parent:cur};job.nodes.set(id,node);job.heap.push(node)}
 }return false;
 }
 if(job.phase==='reconstruct'){
 if(job.cursor){job.path.push({x:job.cursor.x*this.cell,y:job.cursor.y*this.cell});job.cursor=job.cursor.parent;return false}
 job.path.reverse();const last=job.path.at(-1);if(last&&!this.world.blocked(job.to.x,job.to.y,job.r)&&this.clearSegment(last.x,last.y,job.to.x,job.to.y,job.r))job.path.push({...job.to});
 job.smooth=[];job.index=0;job.next=1;job.phase='smooth';return false;
 }
 const path=job.path,i=job.index;
 if(i>=path.length)return this._finish(job,job.smooth);
 if(job.next===i+1)job.smooth.push(path[i]);
 if(job.next+1<path.length&&job.next-i<18&&this.clearSegment(path[i].x,path[i].y,path[job.next+1].x,path[job.next+1].y,job.r)){job.next++;return false}
 job.index=job.next;job.next=job.index+1;return false;
 }
 findPath(from,to,r=10,priority=3){
 const key=[Math.round(from.x/this.cell),Math.round(from.y/this.cell),Math.round(to.x/this.cell),Math.round(to.y/this.cell),r,this.revision].join(':');
 const cached=this.routes.get(key);if(cached&&this.time-cached.time<(cached.path.length?8:1.1)){this.stats.cacheHits++;return cached.path}
 const old=this.pending.get(key);if(old){old.priority=Math.min(old.priority,priority);old.touched=this.time;return null}
 if(this.pending.size>=384){this.stats.dropped++;return null}
 this.pending.set(key,{key,from:{x:from.x,y:from.y},to:{x:to.x,y:to.y},r,priority,created:this.time,touched:this.time,expanded:0,phase:'start'});this.stats.requested++;this.stats.queueLength=this.pending.size;return null;
 }
 setCohortRoute(id,leader,target,r=10,priority=3){
 let cohort=this.cohorts.get(id);if(!cohort)cohort={path:[],goal:{...target},revision:-1,created:this.time};
 cohort.touched=this.time;cohort.leader=leader;cohort.radius=r;
 if(cohort.revision!==this.revision||Math.hypot(target.x-cohort.goal.x,target.y-cohort.goal.y)>75||!cohort.path.length){
 if(!cohort.request||cohort.requestRevision!==this.revision||Math.hypot(target.x-cohort.request.to.x,target.y-cohort.request.to.y)>75){cohort.request={from:{x:leader.x,y:leader.y},to:{...target}};cohort.requestRevision=this.revision}
 const path=this.findPath(cohort.request.from,cohort.request.to,r,priority);if(path!==null){cohort.path=path;cohort.goal={...cohort.request.to};cohort.revision=this.revision;cohort.request=null}
 }
 this.cohorts.set(id,cohort);return cohort;
 }
 _cohortPoint(a,target,r,nav){
 const cohort=this.cohorts.get(a.navCohort);if(!cohort||!cohort.path.length||cohort.revision!==this.revision||Math.hypot(target.x-cohort.goal.x,target.y-cohort.goal.y)>190)return null;
 const path=cohort.path;
 if(nav.sharedPoint&&nav.sharedPath===path&&this.time<nav.sharedAt&&Math.hypot(a.x-nav.sharedPoint.x,a.y-nav.sharedPoint.y)>22){this.stats.sharedHits++;return nav.sharedPoint}
 if(nav.cohort!==a.navCohort||nav.sharedPath!==path){nav.cohort=a.navCohort;nav.sharedPath=path;nav.sharedIndex=0;let best=Infinity;for(let i=0;i<path.length;i++){const d=(a.x-path[i].x)**2+(a.y-path[i].y)**2;if(d<best){nav.sharedIndex=i;best=d}}}
 let i=nav.sharedIndex;
 if(i>=path.length)return null;
 // Local steering joins the shared corridor only through a reachable segment;
 // a follower on the other side of a wall receives its own recovery route.
 for(let next=Math.min(path.length-1,i+2);next>=i;next--){const p=path[next];if(this.clearSegment(a.x,a.y,p.x,p.y,r)){nav.sharedIndex=next;nav.sharedPoint=p;nav.sharedAt=this.time+.12;this.stats.sharedHits++;if(Math.hypot(a.x-p.x,a.y-p.y)<22&&next<path.length-1&&this.clearSegment(a.x,a.y,path[next+1].x,path[next+1].y,r))nav.sharedIndex++;return p}}
 return null;
 }
 steer(a,target,r,dt){
 let nav=a._nav;if(!nav)nav=a._nav={path:[],index:0,goal:{...target},retry:0,revision:-1,stuck:0,lastX:a.x,lastY:a.y,directAt:-1,pointAt:-1};
 const moved2=(a.x-nav.lastX)**2+(a.y-nav.lastY)**2;nav.stuck=moved2<.0064?nav.stuck+dt:Math.max(0,nav.stuck-dt*2);nav.lastX=a.x;nav.lastY=a.y;
 const goalMoved=Math.hypot(target.x-nav.goal.x,target.y-nav.goal.y)>90;
 if(nav.revision!==this.revision){nav.path=[];nav.revision=this.revision;nav.directAt=-1}
 if(this.time>=nav.directAt||!nav.directGoal||Math.hypot(target.x-nav.directGoal.x,target.y-nav.directGoal.y)>35){nav.direct=this.clearSegment(a.x,a.y,target.x,target.y,r);nav.directAt=this.time+.18;nav.directGoal={...target}}
 if(nav.direct){nav.path=[];nav.goal={...target};nav.revision=this.revision;return target}
 if(a.navCohort&&nav.stuck<1.4){const point=this._cohortPoint(a,target,r,nav);if(point){nav.corridorMiss=0;return point}nav.corridorMiss=(nav.corridorMiss||0)+dt}
 const priority=a._navPriority??a.navPriority??(a.id==='king'?0:a.state==='combat'||a.state==='charge'?1:nav.stuck>.8?2:a.id?.startsWith('ape')?3:a.id?.startsWith('human')?4:5);
 const waitForShared=a.navCohort&&this.cohorts.has(a.navCohort)&&nav.stuck<1.4&&(nav.corridorMiss||0)<.8;
 if(!waitForShared&&(nav.revision!==this.revision||goalMoved||nav.stuck>.8||!nav.path.length)&&this.time>=nav.retry){
 // Retain the original request's start while waiting; sliding one grid cell
 // must not leave an unbounded trail of abandoned requests in the queue.
 if(!nav.request||nav.requestRevision!==this.revision||Math.hypot(target.x-nav.request.to.x,target.y-nav.request.to.y)>90){nav.request={from:{x:a.x,y:a.y},to:{...target}};nav.requestRevision=this.revision}
 const path=this.findPath(nav.request.from,nav.request.to,r,priority);
 if(path!==null){nav.path=path;nav.index=0;nav.goal={...nav.request.to};nav.revision=this.revision;nav.retry=this.time+(path.length?.35:1.1);nav.request=null;nav.stuck=0;nav.pointAt=-1}
 }
 while(nav.index<nav.path.length-1&&Math.hypot(a.x-nav.path[nav.index].x,a.y-nav.path[nav.index].y)<22&&this.clearSegment(a.x,a.y,nav.path[nav.index+1].x,nav.path[nav.index+1].y,r))nav.index++;
 if(nav.index<nav.path.length){const point=nav.path[nav.index];if(nav.point!==point||this.time>=nav.pointAt||nav.revision!==this.revision){nav.point=point;nav.pointClear=this.clearSegment(a.x,a.y,point.x,point.y,r);nav.pointAt=this.time+.1}if(nav.pointClear)return point;nav.path=[];nav.retry=0}
 return target;
 }
 move(a,dx,dy,speed,dt,controlled=false){
 const r=a.radius||(a.id==='king'?12:a.id?.startsWith('vehicle')?21:a.state==='young'?7:10);if(dx*dx+dy*dy<2.25){a.moving=false;return}
 const target=controlled?{x:a.x+dx,y:a.y+dy}:this.steer(a,{x:a.x+dx,y:a.y+dy},r,dt);dx=target.x-a.x;dy=target.y-a.y;const d=Math.hypot(dx,dy)||1,travel=Math.min(d,speed*dt),steps=Math.max(1,Math.ceil(travel/5));let vx=dx/d*travel/steps,vy=dy/d*travel/steps;const ox=a.x,oy=a.y;
 for(let i=0;i<steps;i++){if(!this.world.blocked(a.x+vx,a.y+vy,r)){a.x+=vx;a.y+=vy;continue}if(!this.world.blocked(a.x+vx,a.y,r)){a.x+=vx;continue}if(!this.world.blocked(a.x,a.y+vy,r)){a.y+=vy;continue}
 // Stage one/two recovery: deterministic local sliding and alternative angles.
 // A stalled actor then rejoins its corridor or earns a prioritized A* job.
 const dir=Math.atan2(vy,vx),side=(ATSUtil.hash(a.id)%2?1:-1);for(const turn of[.45,-.45,.9,-.9,1.35,-1.35]){const angle=dir+turn*side,sx=Math.cos(angle)*travel/steps,sy=Math.sin(angle)*travel/steps;if(!this.world.blocked(a.x+sx,a.y+sy,r)){a.x+=sx;a.y+=sy;break}}
 }
 a.moving=(a.x-ox)**2+(a.y-oy)**2>.0001;if(a.moving)a.dir=Math.atan2(a.y-oy,a.x-ox);
 }
}
window.ATSNavigation=Navigation;
})();
