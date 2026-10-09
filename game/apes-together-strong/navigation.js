/* Clearance-aware shared corridors and prioritized, incremental A* work. */
(function(){
'use strict';
const clock=()=>typeof performance!=='undefined'?performance.now():Date.now(),VEHICLE_PROFILES=new Set(['jeep','armored','command','truck','apc','ifv','tank']);
class Heap {
 constructor(){this.items=[]}
 push(n){let a=this.items,i=a.length;a.push(n);while(i){let p=(i-1)>>1;if(a[p].f<=n.f)break;a[i]=a[p];i=p}a[i]=n}
 pop(){const a=this.items,top=a[0],last=a.pop();if(a.length){let i=0;while(i*2+1<a.length){let k=i*2+1;if(k+1<a.length&&a[k+1].f<a[k].f)k++;if(a[k].f>=last.f)break;a[i]=a[k];i=k}a[i]=last}return top}
}
class Navigation {
 constructor(world){
 this.world=world;this.cell=28;this.walkCache=new Map();this.segmentCache=new Map();this.routes=new Map();this.pending=new Map();this.cohorts=new Map();this.corridorAt=new Map();this.revision=-1;this.worldRevision=-1;this.localVersions=new Map();this.chunkRevision=-1;this.time=0;
 this.stats={searches:0,cacheHits:0,sharedHits:0,expanded:0,failures:0,requested:0,completed:0,deferred:0,dropped:0,queueLength:0,frameExpanded:0,frameMs:0,crossings:0,recoveries:0};
 }
 beginFrame(time,options={}){
 this.time=time;
 const revision=this.world.navRevision||0;
 if(this.worldRevision!==revision){
 const changes=this.worldRevision>=0?this.world.navigationChangesSince?.(this.worldRevision):null;
 if(changes&&this._invalidateLocal(changes)){if(changes.some(c=>c.fortification))this.hasFortifications=true}
 else {this.revision++;for(const route of this.routes.values())route.path._navInvalid=true;this.walkCache.clear();this.segmentCache.clear();this.routes.clear();this.pending.clear();this.cohorts.clear();this.localVersions.clear();this.hasFortifications=!!this.world.objects&&Array.from(this.world.objects.values()).some(o=>o.fortification&&o.solid&&!o.dead&&o.hp>0)}
 this.worldRevision=revision;
 }
 if(this.chunkRevision!==(this.world.chunkRevision||0)){this.chunkRevision=this.world.chunkRevision||0;this.walkCache.clear();this.segmentCache.clear()}
 if(this.walkCache.size>30000)this.walkCache.clear();if(this.segmentCache.size>9000)this.segmentCache.clear();if(this.routes.size>512)this.routes.delete(this.routes.keys().next().value);
 for(const [key,route]of this.routes)if(time-route.time>(route.path.length?8:1.1))this.routes.delete(key);
 for(const [key,cohort]of this.cohorts)if(time-cohort.touched>2)this.cohorts.delete(key);
 for(const [key,at]of this.corridorAt)if(time-at>3)this.corridorAt.delete(key);
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
 _overlap(a,b){return a&&b&&a.minX<=b.maxX&&a.maxX>=b.minX&&a.minY<=b.maxY&&a.maxY>=b.minY}
 _requestBounds(from,to,r=10){const margin=this.cell*29+168+r+3;return {minX:Math.min(from.x,to.x)-margin,minY:Math.min(from.y,to.y)-margin,maxX:Math.max(from.x,to.x)+margin,maxY:Math.max(from.y,to.y)+margin}}
 _regionVersion(minX,minY,maxX,maxY){if(!this.localVersions.size)return 0;let version=0,count=0;for(let y=Math.floor(minY/384);y<=Math.floor(maxY/384);y++)for(let x=Math.floor(minX/384);x<=Math.floor(maxX/384);x++){if(++count>256)return this.worldRevision;version=Math.max(version,this.localVersions.get(x+','+y)||0)}return version}
 _invalidateLocal(changes){
 // Cache keys carry the newest geometry version in their small spatial region.
 // No per-gate sweep of tens of thousands of collision-cache entries is needed.
 for(const change of changes){let count=0;for(let y=Math.floor(change.minY/384);y<=Math.floor(change.maxY/384);y++)for(let x=Math.floor(change.minX/384);x<=Math.floor(change.maxX/384);x++){if(++count>256||this.localVersions.size>=4096)return false;this.localVersions.set(x+','+y,change.revision)}}
 const affected=bounds=>!bounds||changes.some(change=>this._overlap(bounds,change));
 for(const [key,route]of this.routes)if(affected(route.bounds)){route.path._navInvalid=true;this.routes.delete(key)}
 // Search bounds include all possible coarse/refined expansion and nearest-cell
 // probes. A changed wall can invalidate closed nodes as well as the final path.
 for(const [key,job]of this.pending)if(affected(job.bounds))this.pending.delete(key);
 for(const [key,cohort]of this.cohorts){const request=cohort.request,bounds=request?this._requestBounds(request.from,request.to,cohort.radius):cohort.path._navBounds||cohort.leader&&this._requestBounds(cohort.leader,cohort.goal,cohort.radius);if(cohort.path._navInvalid||affected(bounds)){cohort.path._navInvalid=true;this.cohorts.delete(key)}}
 return true;
 }
 profile(a){const name=typeof a==='string'?a:a?.navClass||a?.vehicleClass||a?.kind||a?.vehicleType||a?.type;if(this.isVehicle(name))return name;if(this.world.actorBlocked){if(name==='human'||name==='ape')return name;if(a?.id==='king'||a?.id?.startsWith('ape'))return 'ape';if(a?.id?.startsWith('human'))return 'human'}return ''}
 isVehicle(profile){return VEHICLE_PROFILES.has(profile)}
 blocked(x,y,r,profile='',ignoreId){if(this.world.actorBlocked&&profile)return this.world.actorBlocked(x,y,r,profile,ignoreId);return this.isVehicle(profile)&&this.world.vehicleBlocked?this.world.vehicleBlocked(x,y,r,profile,ignoreId):this.world.blocked(x,y,r,ignoreId)}
 terrainCost(x,y,profile=''){
 const t=this.isVehicle(profile)&&this.world.vehicleTerrain?this.world.vehicleTerrain(x,y):this.world.terrain(x,y);
 if(!this.isVehicle(profile))return t.road?.92:t.biome==='wetland'?1.3:1;
 return t.road?.6:t.compound?.8:t.biome==='forest'?(profile==='tank'?5:3.5):t.biome==='rocky'?3:t.biome==='wetland'?8:1.25;
 }
 preferredSegment(ax,ay,bx,by,r,profile=''){
 if(!this.clearSegment(ax,ay,bx,by,r,profile))return false;
 if(!this.isVehicle(profile))return true;
 const d=Math.hypot(bx-ax,by-ay);if(d<64)return true;
 const steps=Math.ceil(d/28);for(let i=0;i<=steps;i++)if(this.terrainCost(ax+(bx-ax)*i/steps,ay+(by-ay)*i/steps,profile)>1.5)return false;
 return true;
 }
 beginSeparation(){this.separationCells||(this.separationCells=new Map());this.separationCells.clear()}
 separationClear(a,b){
 // Every separation pair lies within 38 units. Gather the live collision
 // candidates once per small crowd cell; exact segment and water tests below
 // still run for every pair. This does not cache a pass/fail geometry result.
 if(!this.world._queryCollision)return this.clearSegment(a.x,a.y,b.x,b.y,0);
 const cells=this.separationCells||(this.separationCells=new Map()),x=Math.floor(a.x/64),y=Math.floor(a.y/64),key=x+','+y;
 let objects=cells.get(key);
 if(!objects){objects=[];const minX=x*64-38,minY=y*64-38,maxX=(x+1)*64+38,maxY=(y+1)*64+38;this.world._queryCollision(minX,minY,maxX,maxY,o=>{if(!o.solid||o.dead||o.hp<=0)return;const radius=o._collision?.radius??o.moveRadius??o.r??15,hx=o.collision==='rect'?(o._collision?.hx??(o.w||o.r*2)/2):radius,hy=o.collision==='rect'?(o._collision?.hy??(o.h||o.r*2)/2):radius;if(o.x+hx>=minX&&o.x-hx<=maxX&&o.y+hy>=minY&&o.y-hy<=maxY)objects.push(o)});cells.set(key,objects)}
 return this.clearSegment(a.x,a.y,b.x,b.y,0,'',undefined,objects);
 }
 clearSegment(ax,ay,bx,by,r=10,profile='',ignoreId,objects){
 profile=this.profile(profile);if(this.isVehicle(profile))r=Math.max(r,this.world.vehicleRadius?.(profile)||r);
 const dx=bx-ax,dy=by-ay,length2=dx*dx+dy*dy,pad=r;
 if(this.world.boundsReady&&!this.world.boundsReady(Math.min(ax,bx)-pad,Math.min(ay,by)-pad,Math.max(ax,bx)+pad,Math.max(ay,by)+pad))return false;
 const cacheable=!ignoreId&&ax%this.cell===0&&ay%this.cell===0&&bx%this.cell===0&&by%this.cell===0;
 const key=cacheable?ax+','+ay+','+bx+','+by+','+r+','+profile+','+this._regionVersion(Math.min(ax,bx)-pad,Math.min(ay,by)-pad,Math.max(ax,bx)+pad,Math.max(ay,by)+pad):null;
 if(key&&this.segmentCache.has(key))return this.segmentCache.get(key);
 let clear=true;
 const check=o=>{
 if(o.id===ignoreId||!o.solid||o.dead||o.hp<=0||o.fortification&&this.world.fortificationPassable?.(o,profile))return;
 const obstacleRadius=this.isVehicle(profile)&&this.world.vehicleObstacleRadius?this.world.vehicleObstacleRadius(o,profile):(o._collision?.radius??o.moveRadius??o.r??15);if(obstacleRadius<0)return;
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
 }else{const t=length2?Math.max(0,Math.min(1,((o.x-ax)*dx+(o.y-ay)*dy)/length2)):0,rr=obstacleRadius+pad;if((ax+dx*t-o.x)**2+(ay+dy*t-o.y)**2<rr*rr){clear=false;return false}}
 };
 if(objects){for(const o of objects)if(check(o)===false)break}
 else if(this.world._queryCollision)this.world._queryCollision(Math.min(ax,bx)-pad,Math.min(ay,by)-pad,Math.max(ax,bx)+pad,Math.max(ay,by)+pad,check);
 if(clear&&!( !this.isVehicle(profile)&&this.world.waterFreeBounds?.(Math.min(ax,bx)-pad,Math.min(ay,by)-pad,Math.max(ax,bx)+pad,Math.max(ay,by)+pad))){
 // Obstacle intersections above are exact; only terrain needs sampling. This
 // removes the old repeated spatial collision query at every eight pixels.
 const steps=Math.max(1,Math.ceil(Math.sqrt(length2)/8));
 for(let i=0;i<=steps;i++){const x=ax+dx*i/steps,y=ay+dy*i/steps;if(this.world.waterBlocked?this.world.waterBlocked(x,y,r):this.world.blocked(x,y,r)){clear=false;break}if(this.isVehicle(profile)){const t=this.world.vehicleTerrain?this.world.vehicleTerrain(x,y):this.world.terrain(x,y);if(t.crossing&&!t.crossing.vehicleCompatible||t.biome==='wetland'&&!t.road&&!t.compound){clear=false;break}}}
 }
 if(key)this.segmentCache.set(key,clear);return clear;
 }
 walk(x,y,r,profile='',cell=this.cell){const px=x*cell,py=y*cell,pad=r+1;let key=x+','+y+','+r+','+profile+','+cell+','+this._regionVersion(px-pad,py-pad,px+pad,py+pad);if(!this.walkCache.has(key))this.walkCache.set(key,!this.blocked(px,py,pad,profile));return this.walkCache.get(key)}
 _nearestStep(job,which){
 const cell=job.cell||this.cell;let scan=job.scan;
 if(!scan){const point=which==='start'?job.from:job.to;scan=job.scan={cx:Math.round(point.x/cell),cy:Math.round(point.y/cell),ring:0,dx:0,dy:0,best:null,score:Infinity,point}}
 const {ring,dx,dy}=scan,x=scan.cx+dx,y=scan.cy+dy;
 if(Math.max(Math.abs(dx),Math.abs(dy))===ring&&this.walk(x,y,job.r,job.profile,cell)&&
 (which!=='start'||this.clearSegment(job.from.x,job.from.y,x*cell,y*cell,job.r,job.profile))){const score=(x*cell-scan.point.x)**2+(y*cell-scan.point.y)**2;if(score<scan.score){scan.best={x,y};scan.score=score}}
 scan.dy++;
 if(scan.dy>ring){scan.dy=-ring;scan.dx++}
 if(scan.dx>ring){
 if(scan.best){job[which]=scan.best;job.scan=null;job.phase=which==='start'?'goal':'initialize';return false}
 scan.ring++;scan.dx=scan.dy=-scan.ring;if(scan.ring>Math.ceil(168/cell))return this._finish(job,[]);
 }
 return false;
 }
 _finish(job,path){Object.defineProperties(path,{_navBounds:{value:job.bounds},_navInvalid:{value:false,writable:true}});this.routes.set(job.key,{time:this.time,path,bounds:job.bounds});if(!path.length)this.stats.failures++;return true}
 _step(job){
 const cell=job.cell||this.cell;
 if(job.phase==='start'||job.phase==='goal')return this._nearestStep(job,job.phase);
 if(job.phase==='initialize'){
 const {start,goal}=job;job.heap=new Heap();job.nodes=new Map();job.heuristic=(x,y)=>Math.hypot(goal.x-x,goal.y-y);
 const first={...start,g:0,f:job.heuristic(start.x,start.y),parent:null};job.nodes.set(start.x+','+start.y,first);job.heap.push(first);
 job.minX=Math.min(start.x,goal.x)-28;job.maxX=Math.max(start.x,goal.x)+28;job.minY=Math.min(start.y,goal.y)-28;job.maxY=Math.max(start.y,goal.y)+28;job.phase='expand';this.stats.searches++;return false;
 }
 if(job.phase==='expand'){
 if(!job.heap.items.length||job.expanded>=(job.refined?6400:3200)){
 // An exhausted small coarse region can be a legal, sub-cell passage beside
 // cages or gatehouses. Refine that shared job once; retain both global frame
 // budgets and exact actor clearance. Unbounded/open-world failures stay cheap.
 if(!job.refined&&!job.heap.items.length&&job.expanded<1600&&!this.isVehicle(job.profile)){
 job.refined=true;job.cell=7;job.scan=null;job.phase='start';return false;
 }
 return this._finish(job,[]);
 }
 const cur=job.heap.pop();if(cur.closed)return false;cur.closed=true;job.expanded++;
 if(cur.x===job.goal.x&&cur.y===job.goal.y){job.path=[];job.cursor=cur;job.phase='reconstruct';return false}
 for(let dx=-1;dx<=1;dx++)for(let dy=-1;dy<=1;dy++){
 if(!dx&&!dy)continue;const x=cur.x+dx,y=cur.y+dy,r=job.r;
 if(x<job.minX||x>job.maxX||y<job.minY||y>job.maxY||!this.walk(x,y,r,job.profile,cell))continue;
 if(dx&&dy&&(!this.walk(cur.x+dx,cur.y,r,job.profile,cell)||!this.walk(cur.x,cur.y+dy,r,job.profile,cell)))continue;
 if(!this.clearSegment(cur.x*cell,cur.y*cell,x*cell,y*cell,r,job.profile))continue;
 const cost=this.terrainCost(x*cell,y*cell,job.profile),g=cur.g+(dx&&dy?Math.SQRT2:1)*cost,id=x+','+y,old=job.nodes.get(id);
 if(!old||g<old.g){const node={x,y,g,f:g+job.heuristic(x,y)*(this.isVehicle(job.profile)?.6:.92),parent:cur};job.nodes.set(id,node);job.heap.push(node)}
 }return false;
 }
 if(job.phase==='reconstruct'){
 if(job.cursor){job.path.push({x:job.cursor.x*cell,y:job.cursor.y*cell});job.cursor=job.cursor.parent;return false}
 job.path.reverse();const last=job.path.at(-1);if(last&&!this.blocked(job.to.x,job.to.y,job.r,job.profile)&&this.clearSegment(last.x,last.y,job.to.x,job.to.y,job.r,job.profile))job.path.push({...job.to});
 job.smooth=[];job.index=0;job.next=1;job.phase='smooth';return false;
 }
 const path=job.path,i=job.index;
 if(i>=path.length)return this._finish(job,job.smooth);
 if(job.next===i+1)job.smooth.push(path[i]);
 if(job.next+1<path.length&&job.next-i<18&&this.preferredSegment(path[i].x,path[i].y,path[job.next+1].x,path[job.next+1].y,job.r,job.profile)){job.next++;return false}
 job.index=job.next;job.next=job.index+1;return false;
 }
 findPath(from,to,r=10,priority=3,profile=''){
 profile=this.profile(profile);if(this.isVehicle(profile))r=Math.max(r,this.world.vehicleRadius?.(profile)||r);
 const key=[Math.round(from.x/this.cell),Math.round(from.y/this.cell),Math.round(to.x/this.cell),Math.round(to.y/this.cell),r,this.revision,profile].join(':');
 const cached=this.routes.get(key);if(cached&&!cached.path._navInvalid&&this.time-cached.time<(cached.path.length?8:1.1)){this.stats.cacheHits++;return cached.path}
 const old=this.pending.get(key);if(old){old.priority=Math.min(old.priority,priority);old.touched=this.time;return null}
 if(this.pending.size>=384){this.stats.dropped++;return null}
 this.pending.set(key,{key,from:{x:from.x,y:from.y},to:{x:to.x,y:to.y},bounds:this._requestBounds(from,to,r),r,profile,priority,created:this.time,touched:this.time,expanded:0,phase:'start'});this.stats.requested++;this.stats.queueLength=this.pending.size;return null;
 }
 setCohortRoute(id,leader,target,r=10,priority=3,profile=this.profile(leader)){
 if(!id.startsWith('recovery:'))target=this.strategicTarget(leader,target,r,profile);
 let cohort=this.cohorts.get(id);if(!cohort)cohort={path:[],goal:{...target},revision:-1,created:this.time};
 cohort.touched=this.time;cohort.leader=leader;cohort.radius=r;cohort.profile=profile;
 // Missing streamed chunks are pending information, not a blocked bridge.
 // Do not fill the search queue or reject a target while its local leg loads.
 if(this.world.boundsReady&&!this.world.boundsReady(Math.min(leader.x,target.x)-r,Math.min(leader.y,target.y)-r,Math.max(leader.x,target.x)+r,Math.max(leader.y,target.y)+r)){this.cohorts.set(id,cohort);return cohort}
 if(cohort.failedUntil>this.time&&cohort.revision===this.revision&&Math.hypot(target.x-cohort.goal.x,target.y-cohort.goal.y)<75){this.cohorts.set(id,cohort);return cohort}
 if(cohort.revision!==this.revision||cohort.path._navInvalid||Math.hypot(target.x-cohort.goal.x,target.y-cohort.goal.y)>75||!cohort.path.length){
 if(!cohort.request||cohort.requestRevision!==this.revision||Math.hypot(target.x-cohort.request.to.x,target.y-cohort.request.to.y)>75){cohort.request={from:{x:leader.x,y:leader.y},to:{...target}};cohort.requestRevision=this.revision}
 const path=this.findPath(cohort.request.from,cohort.request.to,r,priority,profile);if(path!==null){cohort.path=path;cohort.goal={...cohort.request.to};cohort.revision=this.revision;cohort.failedUntil=path.length?0:this.time+2.5;cohort.request=null}
 }
 this.cohorts.set(id,cohort);return cohort;
 }
 recoveryPath(from,to,r=10,priority=3,profile=this.profile(from)){
 // A blocked group earns one recovery corridor per small spatial cell. This
 // includes distant stragglers, so a thousand followers never create a
 // thousand independent A* jobs after their shared route becomes obstructed.
 const id=['recovery',Math.floor(from.x/84),Math.floor(from.y/84),Math.floor(to.x/160),Math.floor(to.y/160),r,profile].join(':');
 const cohort=this.setCohortRoute(id,from,to,r,priority,profile);return cohort.path.length?cohort.path:null;
 }
 _cohortPoint(a,target,r,nav,cohortId=a.navCohort){
 const cohort=this.cohorts.get(cohortId);if(!cohort||!cohort.path.length||cohort.path._navInvalid||cohort.revision!==this.revision||Math.hypot(target.x-cohort.goal.x,target.y-cohort.goal.y)>190)return null;
 const path=cohort.path;
 if(nav.sharedPoint&&nav.sharedPath===path&&this.time<nav.sharedAt&&Math.hypot(a.x-nav.sharedPoint.x,a.y-nav.sharedPoint.y)>22){this.stats.sharedHits++;return nav.sharedPoint}
 if(nav.cohort!==cohortId||nav.sharedPath!==path){nav.cohort=cohortId;nav.sharedPath=path;nav.sharedIndex=0;let best=Infinity;for(let i=0;i<path.length;i++){const d=(a.x-path[i].x)**2+(a.y-path[i].y)**2;if(d<best){nav.sharedIndex=i;best=d}}}
 let i=nav.sharedIndex;
 if(i>=path.length)return null;
 // Local steering joins the shared corridor only through a reachable segment;
 // followers on the other side of a wall share a local recovery corridor.
 for(let next=Math.min(path.length-1,i+2);next>=i;next--){const p=path[next];if(this.clearSegment(a.x,a.y,p.x,p.y,r,nav.profile)){nav.sharedIndex=next;nav.sharedPoint=p;nav.sharedAt=this.time+.12;this.stats.sharedHits++;if(Math.hypot(a.x-p.x,a.y-p.y)<22&&next<path.length-1&&this.clearSegment(a.x,a.y,path[next+1].x,path[next+1].y,r,nav.profile))nav.sharedIndex++;return p}}
 // A follower can slide away from the corridor while the shared request is
 // being solved. Rejoin a reachable earlier waypoint before trying to turn
 // across a river or wall; proximity alone does not make that turn safe.
 for(let previous=i-1;previous>=Math.max(0,i-8);previous--){const p=path[previous];if(this.clearSegment(a.x,a.y,p.x,p.y,r,nav.profile)){nav.sharedIndex=previous;nav.sharedPoint=p;nav.sharedAt=this.time+.12;this.stats.sharedHits++;return p}}
 return null;
 }
 resetActor(a){
 for(const key of ['_nav','_navJourney','_navCrossing','_navCrossingAt','_navAvoidCrossings','_abstractPath','_abstractRequest','_abstractRetry','_abstractIndex','navCohort'])delete a[key];
 }
 followTarget(a,king,x,y){
 const world=this.world,region=world.landRegionAt?.(king.x,king.y);
 // The formation may widen across a shore. Its decorative spacing must not
 // order a follower back across the river after it has reached its king.
 if(world.waterBlocked?.(x,y,a.radius||10)||region!=null&&world.landRegionAt(x,y)!==region)return {x:king.x,y:king.y};
 return {x,y};
 }
 strategicTarget(a,target,r=10,profile=this.profile(a)){
 const world=this.world,ground=profile==='ape'||profile==='human'||this.isVehicle(profile);
 if(!ground)return target;
 let crossing=a._navCrossing;
 if(!crossing&&this.time>=(a._navCrossingAt||0)){
 a._navCrossingAt=this.time+.75;
 const bridge=world.crossingFor?.(a,target,profile,a._navAvoidCrossings,this.time);
 if(bridge){
 const direction=target.y>a.y?1:-1,side=direction>0?0:1;
 // Three stable lanes use the physical deck width without crowding its edges.
 const lane=this.isVehicle(profile)?0:((ATSUtil.hash(a.id||'cohort')%3)-1)*Math.max(0,Math.min(28,bridge.width/2-r-15));
 crossing=a._navCrossing={id:bridge.id,bridge,direction,phase:0,near:{x:bridge.x+lane,y:bridge.approaches[side].y},far:{x:bridge.x+lane,y:bridge.approaches[1-side].y}};
 if(a.x>bridge.minX+r&&a.x<bridge.maxX-r&&a.y>bridge.minY&&a.y<bridge.maxY)crossing.phase=1;
 this.stats.crossings++;
 }
 }
 if(crossing){
 if(crossing.phase===0&&Math.hypot(a.x-crossing.near.x,a.y-crossing.near.y)<24)crossing.phase=1;
 if(crossing.phase===1&&(a.y-crossing.far.y)*crossing.direction>=-18){a._navCrossing=null;a._navCrossingAt=this.time+.2;delete a._navJourney;}
 else target=crossing.phase===0?crossing.near:crossing.far;
 }
 const distance=Math.hypot(a.x-target.x,a.y-target.y);
 if(distance>600){
 const old=a._navJourney;
 if(!old||Math.hypot(old.goal.x-target.x,old.goal.y-target.y)>90||Math.hypot(a.x-old.point.x,a.y-old.point.y)<70){
 const point=this.isVehicle(profile)&&world.vehicleWaypoint?world.vehicleWaypoint(a,target):{x:a.x+(target.x-a.x)*560/distance,y:a.y+(target.y-a.y)*560/distance};
 a._navJourney={goal:{...target},point};
 }
 target=a._navJourney.point;
 }else a._navJourney=null;
 // Group streaming requests by region. Distant recalls load only the next
 // bounded leg and crossing approach instead of the entire world at once.
 if(this.isVehicle(profile)||a.responseAllocated||profile==='ape'&&(a.recallOrder||distance>600||crossing)){
 const id=this.isVehicle(profile)||a.responseAllocated?(a.operationId||a.squadId||a.id)+':'+(profile||'infantry'):'nav:'+profile+':'+Math.floor(a.x/768)+':'+Math.floor(a.y/768);
 if(this.time>=(this.corridorAt.get(id)??-1)){this.corridorAt.set(id,this.time+.35);world.requestCorridor?.(a,target,{id,profile:this.isVehicle(profile)?profile:undefined})}
 }
 return target;
 }
 unreachable(a,target){const failed=a._nav?.unreachable;return !!failed&&failed.revision===this.revision&&this.time<failed.until&&Math.hypot(failed.x-target.x,failed.y-target.y)<90}
 steer(a,target,r,dt){
 const profile=this.profile(a);if(this.isVehicle(profile))r=Math.max(r,this.world.vehicleRadius?.(profile)||r);
 target=this.strategicTarget(a,target,r,profile);
 let nav=a._nav;if(!nav)nav=a._nav={path:[],index:0,goal:{...target},retry:0,revision:-1,stuck:0,lastX:a.x,lastY:a.y,directAt:-1,pointAt:-1};
 if(nav.worldRevision!==this.worldRevision){const changes=this.world.navigationChangesSince?.(nav.worldRevision),bounds=nav.path._navBounds||this._requestBounds(a,nav.goal,r);if(nav.path._navInvalid||!changes||changes.some(change=>this._overlap(bounds,change))){nav.path=[];nav.request=null;nav.retry=0;nav.directAt=-1;nav.sharedAt=-1;nav.pointAt=-1;nav.unreachable=null}nav.worldRevision=this.worldRevision}
 const moved2=(a.x-nav.lastX)**2+(a.y-nav.lastY)**2;nav.stuck=moved2<.0064?nav.stuck+dt:Math.max(0,nav.stuck-dt*2);nav.lastX=a.x;nav.lastY=a.y;
 if(!nav.progress||this.time-nav.progress.time>1.4){
 if(nav.progress&&Math.hypot(a.x-nav.progress.x,a.y-nav.progress.y)<12&&Math.hypot(a.x-target.x,a.y-target.y)>35){nav.stuck=Math.max(nav.stuck,1.4);nav.directAt=-1;nav.sharedAt=-1;this.stats.recoveries++}
 nav.progress={x:a.x,y:a.y,time:this.time};
 }
 const goalMoved=Math.hypot(target.x-nav.goal.x,target.y-nav.goal.y)>90;
 if(nav.revision!==this.revision||nav.profile!==profile){nav.path=[];nav.revision=this.revision;nav.directAt=-1;nav.profile=profile}
 if(this.time>=nav.directAt||!nav.directGoal||Math.hypot(target.x-nav.directGoal.x,target.y-nav.directGoal.y)>35){nav.direct=this.preferredSegment(a.x,a.y,target.x,target.y,r,profile);nav.directAt=this.time+(a._simTier===2?.65:a._simTier===1?.28:.18);nav.directGoal={...target}}
 if(nav.direct){nav.path=[];nav.goal={...target};nav.revision=this.revision;return target}
 if(!this.isVehicle(profile)&&a.navCohort&&nav.stuck<1.4){const point=this._cohortPoint(a,target,r,nav);if(point){nav.corridorMiss=0;return point}nav.corridorMiss=(nav.corridorMiss||0)+dt}
 const priority=a._navPriority??a.navPriority??(a.id==='king'?0:a.state==='combat'||a.state==='charge'?1:nav.stuck>.8?2:a.id?.startsWith('ape')?3:a.id?.startsWith('human')?4:5);
 const waitForShared=!this.isVehicle(profile)&&a.navCohort&&this.cohorts.has(a.navCohort)&&nav.stuck<1.4&&(nav.corridorMiss||0)<.8;
 if(!this.isVehicle(profile)&&a.id?.startsWith('ape')&&!waitForShared){
 const recoveryId=['recovery',Math.floor(a.x/84),Math.floor(a.y/84),Math.floor(target.x/160),Math.floor(target.y/160),r,profile].join(':');
 this.recoveryPath(a,target,r,priority,profile);const point=this._cohortPoint(a,target,r,nav,recoveryId);if(point){nav.unreachable=null;return point}
 const cohort=this.cohorts.get(recoveryId);
 if(cohort?.failedUntil>this.time&&cohort.revision===this.revision){
 nav.unreachable={x:target.x,y:target.y,until:cohort.failedUntil,revision:this.revision};
 if(a._navCrossing?.phase===0){(a._navAvoidCrossings||(a._navAvoidCrossings={}))[a._navCrossing.id]=this.time+12;a._navCrossing=null;a._navCrossingAt=0;a._navJourney=null}
 return {x:a.x,y:a.y}}
 // Immediate collision-safe local steering continues while the cohort waits.
 return target;
 }
 if(!waitForShared&&(nav.revision!==this.revision||goalMoved||nav.stuck>.8||!nav.path.length)&&this.time>=nav.retry){
 // Retain the original request's start while waiting; sliding one grid cell
 // must not leave an unbounded trail of abandoned requests in the queue.
 if(!nav.request||nav.requestRevision!==this.revision||Math.hypot(target.x-nav.request.to.x,target.y-nav.request.to.y)>90){nav.request={from:{x:a.x,y:a.y},to:{...target}};nav.requestRevision=this.revision}
 const path=this.findPath(nav.request.from,nav.request.to,r,priority,profile);
 if(path!==null){nav.path=path;nav.index=0;nav.goal={...nav.request.to};nav.revision=this.revision;nav.retry=this.time+(path.length?.35:1.1);nav.request=null;nav.stuck=0;nav.pointAt=-1}
 }
 while(nav.index<nav.path.length-1&&Math.hypot(a.x-nav.path[nav.index].x,a.y-nav.path[nav.index].y)<22&&this.clearSegment(a.x,a.y,nav.path[nav.index+1].x,nav.path[nav.index+1].y,r,profile))nav.index++;
 if(nav.index<nav.path.length){const point=nav.path[nav.index];if(nav.point!==point||this.time>=nav.pointAt||nav.revision!==this.revision){nav.point=point;nav.pointClear=this.clearSegment(a.x,a.y,point.x,point.y,r,profile);nav.pointAt=this.time+.1}if(nav.pointClear)return point;nav.path=[];nav.retry=0}
 return this.isVehicle(profile)?{x:a.x,y:a.y}:target;
 }
 interactFortification(a,target,r,dt){
 const world=this.world,profile=this.profile(a),now=this.game?.time??this.time;if(!world.fortificationAt||!profile||this.hasFortifications===false)return null;
 const crossing=a._barrierCrossing;if(crossing){if(now<crossing.until&&Math.hypot(a.x-crossing.x,a.y-crossing.y)>6){a._barrierIgnore=crossing.id;return crossing}a._barrierCrossing=null;a._barrierIgnore=null}
 const dx=target.x-a.x,dy=target.y-a.y,d=Math.hypot(dx,dy);if(d<1)return null;
 const step=Math.min(d,r+22),b=world.fortificationAt(a.x+dx/d*step,a.y+dy/d*step,r);if(!b||b.dead||b.hp<=0){a.barrierAction=null;return null}
 const team=b.team||b.owner,own=team===profile;if(profile==='human'&&own)return null;if(profile==='ape'&&own&&b.settlementWall)return null;
 if(this.isVehicle(profile)){
  if(profile==='tank'&&team==='ape'&&b.weak){if(now>=(a._barrierAttackAt||0)){a._barrierAttackAt=now+.65;world.damageFortification(b,95,now);this.game?.effect('smash',b.x,b.y,{life:.35,color:'#dfbc82'})}return b.dead?null:false}return null;
 }
 if(profile==='ape'&&!own){if(now>=(a._barrierAttackAt||0)){a._barrierAttackAt=now+.75;world.damageFortification(b,a.id==='king'?55:32,now);a.animation={kind:'overhead',start:now,duration:.55};this.game?.sound('smash',.35,a.x)}a.barrierAction={id:b.id,kind:'breach'};return false}
 if(profile==='human'&&team==='ape'&&(b.settlementWall||b.settlementId)&&a.role!=='engineer'){if(now>=(a._barrierAttackAt||0)){a._barrierAttackAt=now+1;world.damageFortification(b,18,now);a.animation={kind:'breach',start:now,duration:.4}}a.barrierAction={id:b.id,kind:'breach'};return false}
 if(profile==='human'&&a.role==='engineer'){if(a.hitTimer>0){a.barrierAction=null;return false}if(now>=(a._barrierAttackAt||0)){a._barrierAttackAt=now+.55;world.damageFortification(b,65,now);a.animation={kind:'build',start:now,duration:.5}}a.barrierAction={id:b.id,kind:'breach'};return false}
 const duration=own?.28:2.8;let action=a.barrierAction;if(!action||action.id!==b.id||action.kind!=='climb')action=a.barrierAction={id:b.id,kind:'climb',remaining:duration};
 if(!own&&a.hitTimer>0){action.remaining=duration;return false}action.remaining-=dt;a.animation={kind:own?'vault':'climb',start:now,duration:Math.min(duration,.7)};
 if(action.remaining>0)return false;
 // Crossing ignores this section only. Terrain and neighboring obstacles stay solid.
 const half=b.collision==='rect'?Math.abs(dx/d)*(b.w||b.r*2)/2+Math.abs(dy/d)*(b.h||b.r*2)/2:b.r||18,point={x:b.x+dx/d*(half+r+8),y:b.y+dy/d*(half+r+8)};
 if(this.blocked(point.x,point.y,r,profile,b.id)){action.remaining=.25;return false}
 a._barrierCrossing={...point,id:b.id,until:now+1.5};a._barrierIgnore=b.id;a.barrierAction=null;return a._barrierCrossing;
 }
 move(a,dx,dy,speed,dt,controlled=false){
 const profile=this.profile(a),r=Math.max(a.radius||(a.id==='king'?12:a.id?.startsWith('vehicle')?21:a.state==='young'?7:10),this.isVehicle(profile)?(this.world.vehicleRadius?.(profile)||21):0);if(dx*dx+dy*dy<2.25&&(controlled||!a._navCrossing)){a.moving=false;return}
 const intended={x:a.x+dx,y:a.y+dy},planned=controlled?intended:this.steer(a,intended,r,dt),interaction=this.interactFortification(a,planned,r,dt);if(interaction===false){a.moving=false;return}const target=interaction||planned;dx=target.x-a.x;dy=target.y-a.y;const d=Math.hypot(dx,dy)||1,travel=Math.min(d,speed*dt),steps=Math.max(1,Math.ceil(travel/5));let vx=dx/d*travel/steps,vy=dy/d*travel/steps;const ox=a.x,oy=a.y;
 for(let i=0;i<steps;i++){if(!this.blocked(a.x+vx,a.y+vy,r,profile,a._barrierIgnore)){a.x+=vx;a.y+=vy;continue}if(!this.blocked(a.x+vx,a.y,r,profile,a._barrierIgnore)){a.x+=vx;continue}if(!this.blocked(a.x,a.y+vy,r,profile,a._barrierIgnore)){a.y+=vy;continue}
 // Stage one/two recovery: deterministic local sliding and alternative angles.
 // A stalled actor then rejoins its corridor or earns a prioritized A* job.
 const dir=Math.atan2(vy,vx),side=(ATSUtil.hash(a.id)%2?1:-1);for(const turn of[.45,-.45,.9,-.9,1.35,-1.35]){const angle=dir+turn*side,sx=Math.cos(angle)*travel/steps,sy=Math.sin(angle)*travel/steps;if(!this.blocked(a.x+sx,a.y+sy,r,profile,a._barrierIgnore)){a.x+=sx;a.y+=sy;break}}
 }
 a.moving=(a.x-ox)**2+(a.y-oy)**2>.0001;if(a.moving)a.dir=Math.atan2(a.y-oy,a.x-ox);
 }
}
window.ATSNavigation=Navigation;
})();
