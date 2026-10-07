/* Clearance-aware, bounded A* routes shared by nearby creatures. */
(function(){
'use strict';
class Heap {
 constructor(){this.items=[]}
 push(n){let a=this.items,i=a.length;a.push(n);while(i){let p=(i-1)>>1;if(a[p].f<=n.f)break;a[i]=a[p];i=p}a[i]=n}
 pop(){const a=this.items,top=a[0],last=a.pop();if(a.length){let i=0;while(i*2+1<a.length){let k=i*2+1;if(k+1<a.length&&a[k+1].f<a[k].f)k++;if(a[k].f>=last.f)break;a[i]=a[k];i=k}a[i]=last}return top}
}
class Navigation {
 constructor(world){this.world=world;this.cell=28;this.walkCache=new Map();this.routes=new Map();this.revision=-1;this.time=0;this.remaining=3;this.stats={searches:0,cacheHits:0,expanded:0,failures:0}}
 beginFrame(time){this.time=time;this.remaining=3;if(this.revision!==(this.world.navRevision||0)){this.revision=this.world.navRevision||0;this.walkCache.clear();this.routes.clear()}if(this.walkCache.size>30000)this.walkCache.clear();if(this.routes.size>250)this.routes.clear()}
 clearSegment(ax,ay,bx,by,r=10){
 const dx=bx-ax,dy=by-ay,length2=dx*dx+dy*dy,pad=r+.75;let clear=true;
 if(this.world._queryCollision)this.world._queryCollision(Math.min(ax,bx)-pad,Math.min(ay,by)-pad,Math.max(ax,bx)+pad,Math.max(ay,by)+pad,o=>{
 if(!o.solid||o.dead||o.hp<=0)return;
 if(o.collision==='rect'){const hx=(o.w||o.r*2)/2+pad,hy=(o.h||o.r*2)/2+pad;let lo=0,hi=1;for(const [start,delta,min,max]of[[ax,dx,o.x-hx,o.x+hx],[ay,dy,o.y-hy,o.y+hy]]){if(Math.abs(delta)<1e-9){if(start<min||start>max)return}else{let t1=(min-start)/delta,t2=(max-start)/delta;if(t1>t2)[t1,t2]=[t2,t1];lo=Math.max(lo,t1);hi=Math.min(hi,t2);if(lo>hi)return}}if(hi>=0&&lo<=1){clear=false;return false}}
 else{const t=length2?Math.max(0,Math.min(1,((o.x-ax)*dx+(o.y-ay)*dy)/length2)):0,rr=(o.moveRadius??o.r??15)+pad;if((ax+dx*t-o.x)**2+(ay+dy*t-o.y)**2<rr*rr){clear=false;return false}}});
 if(!clear)return false;const steps=Math.max(1,Math.ceil(Math.sqrt(length2)/8));for(let i=1;i<=steps;i++)if(this.world.blocked(ax+dx*i/steps,ay+dy*i/steps,r))return false;return true;
 }
 walk(x,y,r){let key=x+','+y+','+r;if(!this.walkCache.has(key))this.walkCache.set(key,!this.world.blocked(x*this.cell,y*this.cell,r+1));return this.walkCache.get(key)}
 nearest(x,y,r,origin=null){const cx=Math.round(x/this.cell),cy=Math.round(y/this.cell);for(let ring=0;ring<=6;ring++){let best=null,score=Infinity;for(let dx=-ring;dx<=ring;dx++)for(let dy=-ring;dy<=ring;dy++){if(Math.max(Math.abs(dx),Math.abs(dy))!==ring)continue;const xx=cx+dx,yy=cy+dy;if(!this.walk(xx,yy,r))continue;const px=xx*this.cell,py=yy*this.cell;if(origin&&!this.clearSegment(origin.x,origin.y,px,py,r))continue;const dd=Math.hypot(px-x,py-y);if(dd<score){best={x:xx,y:yy};score=dd}}if(best)return best}return null}
 findPath(from,to,r=10){
 const start=this.nearest(from.x,from.y,r,from),goal=this.nearest(to.x,to.y,r);if(!start||!goal)return [];
 const key=[start.x,start.y,goal.x,goal.y,r,this.revision].join(':');const cached=this.routes.get(key);if(cached&&this.time-cached.time<8){this.stats.cacheHits++;return cached.path.map(p=>({...p}))}if(this.remaining<=0)return null;this.remaining--;this.stats.searches++;
 const heap=new Heap(),nodes=new Map(),id=(x,y)=>x+','+y,heuristic=(x,y)=>Math.hypot(goal.x-x,goal.y-y);let first={...start,g:0,f:heuristic(start.x,start.y),parent:null};nodes.set(id(start.x,start.y),first);heap.push(first);let found=null,best=first,expanded=0;
 const margin=28,minX=Math.min(start.x,goal.x)-margin,maxX=Math.max(start.x,goal.x)+margin,minY=Math.min(start.y,goal.y)-margin,maxY=Math.max(start.y,goal.y)+margin;
 while(heap.items.length&&expanded<3200){const cur=heap.pop();if(cur.closed)continue;cur.closed=true;expanded++;if(heuristic(cur.x,cur.y)<heuristic(best.x,best.y))best=cur;if(cur.x===goal.x&&cur.y===goal.y){found=cur;break}
 for(let dx=-1;dx<=1;dx++)for(let dy=-1;dy<=1;dy++){if(!dx&&!dy)continue;const x=cur.x+dx,y=cur.y+dy;if(x<minX||x>maxX||y<minY||y>maxY||!this.walk(x,y,r))continue;if(dx&&dy&&(!this.walk(cur.x+dx,cur.y,r)||!this.walk(cur.x,cur.y+dy,r)))continue;if(!this.clearSegment(cur.x*this.cell,cur.y*this.cell,x*this.cell,y*this.cell,r))continue;
 const terrain=this.world.terrain(x*this.cell,y*this.cell),cost=terrain.road?.92:terrain.biome==='wetland'?1.3:1;const g=cur.g+(dx&&dy?Math.SQRT2:1)*cost;let node=nodes.get(id(x,y));if(!node||g<node.g){node={x,y,g,f:g+heuristic(x,y)*.92,parent:cur};nodes.set(id(x,y),node);heap.push(node)}}
 }
 this.stats.expanded+=expanded;if(!found){this.stats.failures++;return []}const path=[];for(let n=found;n;n=n.parent)path.push({x:n.x*this.cell,y:n.y*this.cell});path.reverse();if(!this.world.blocked(to.x,to.y,r)&&this.clearSegment(path.at(-1).x,path.at(-1).y,to.x,to.y,r))path.push({...to});
 // String-pull while retaining collision clearance, including diagonal wall corners.
 const smooth=[];let i=0;while(i<path.length){smooth.push(path[i]);let next=i+1;while(next+1<path.length&&next-i<18&&this.clearSegment(path[i].x,path[i].y,path[next+1].x,path[next+1].y,r))next++;i=next}
 this.routes.set(key,{time:this.time,path:smooth});return smooth.map(p=>({...p}));
 }
 steer(a,target,r,dt){
 let nav=a._nav;if(!nav)nav=a._nav={path:[],index:0,goal:target,retry:0,revision:-1,stuck:0,lastX:a.x,lastY:a.y};const moved=Math.hypot(a.x-nav.lastX,a.y-nav.lastY);nav.stuck=moved<.08?nav.stuck+dt:Math.max(0,nav.stuck-dt*2);nav.lastX=a.x;nav.lastY=a.y;
 const dx=target.x-a.x,dy=target.y-a.y,d=Math.hypot(dx,dy),look=Math.min(d,90);
 if(this.clearSegment(a.x,a.y,a.x+dx/(d||1)*look,a.y+dy/(d||1)*look,r)&&(!nav.path.length||this.clearSegment(a.x,a.y,target.x,target.y,r))){nav.path=[];return target}
 if(nav.revision!==this.revision||Math.hypot(target.x-nav.goal.x,target.y-nav.goal.y)>90||nav.stuck>.8||!nav.path.length){if(this.time>=nav.retry){const path=this.findPath(a,target,r);if(path!==null){nav.path=path;nav.index=0;nav.goal={...target};nav.revision=this.revision;nav.retry=this.time+(path.length?.35:1.1);nav.stuck=0}}}
 while(nav.index<nav.path.length-1&&Math.hypot(a.x-nav.path[nav.index].x,a.y-nav.path[nav.index].y)<22&&this.clearSegment(a.x,a.y,nav.path[nav.index+1].x,nav.path[nav.index+1].y,r))nav.index++;
 if(nav.index<nav.path.length){let point=nav.path[nav.index];if(this.clearSegment(a.x,a.y,point.x,point.y,r))return point;nav.path=[];nav.retry=0}
 return target;
 }
 move(a,dx,dy,speed,dt,controlled=false){
 const r=a.radius||(a.id==='king'?12:a.id?.startsWith('vehicle')?21:a.state==='young'?7:10);if(Math.hypot(dx,dy)<1.5){a.moving=false;return}
 const target=controlled?{x:a.x+dx,y:a.y+dy}:this.steer(a,{x:a.x+dx,y:a.y+dy},r,dt);dx=target.x-a.x;dy=target.y-a.y;const d=Math.hypot(dx,dy)||1,travel=Math.min(d,speed*dt),steps=Math.max(1,Math.ceil(travel/5));let vx=dx/d*travel/steps,vy=dy/d*travel/steps;const ox=a.x,oy=a.y;
 for(let i=0;i<steps;i++){if(!this.world.blocked(a.x+vx,a.y+vy,r)){a.x+=vx;a.y+=vy;continue}if(!this.world.blocked(a.x+vx,a.y,r)){a.x+=vx;continue}if(!this.world.blocked(a.x,a.y+vy,r)){a.y+=vy;continue}
 // Slide along the trunk or wall consistently; never teleport or reverse steering every frame.
 const dir=Math.atan2(vy,vx),side=(ATSUtil.hash(a.id)%2?1:-1);for(const turn of[.45,-.45,.9,-.9,1.35,-1.35]){const angle=dir+turn*side,sx=Math.cos(angle)*travel/steps,sy=Math.sin(angle)*travel/steps;if(!this.world.blocked(a.x+sx,a.y+sy,r)){a.x+=sx;a.y+=sy;break}}
 }
 a.moving=Math.hypot(a.x-ox,a.y-oy)>.01;if(a.moving)a.dir=Math.atan2(a.y-oy,a.x-ox);
 }
}
window.ATSNavigation=Navigation;
})();
