// Flat, deterministic desk-mat illustrations. Painted once into the scenery
// cache: these marks never create obstacles or compete with unit silhouettes.
const INK='#6e7c6a', TAU=Math.PI*2;
const segmentDistance=(p,a,b)=>{const dx=b[0]-a[0],dy=b[1]-a[1],length=dx*dx+dy*dy,t=length?Math.max(0,Math.min(1,((p.x-a[0])*dx+(p.y-a[1])*dy)/length)):0;return Math.hypot(p.x-a[0]-dx*t,p.y-a[1]-dy*t);};
const boxContains=(p,b,pad)=>p.x>b.x-pad&&p.x<b.x+b.w+pad&&p.y>b.y-pad&&p.y<b.y+b.h+pad;
function stroke(c,points,color=INK,width=1.4){c.beginPath();points.forEach((p,i)=>i?c.lineTo(...p):c.moveTo(...p));c.strokeStyle=color;c.lineWidth=width;c.lineCap='round';c.lineJoin='round';c.stroke();}
function shape(c,fill,outline=null,width=1){if(fill){c.fillStyle=fill;c.fill();}if(outline){c.strokeStyle=outline;c.lineWidth=width;c.stroke();}}
function oval(c,x,y,rx,ry,fill,outline=null,width=1){c.beginPath();c.ellipse(x,y,rx,ry,0,0,TAU);shape(c,fill,outline,width);}
function card(c,x,y,w,h,fill,outline=null){c.beginPath();c.roundRect(x,y,w,h,2);shape(c,fill,outline);}

export function tabletopDetailPlacements(game){
 const map=game.map||{},w=game.width||4800,h=game.height||3200,placements=[];
 const blocked=[...(map.obstacles||[]),...(map.ramps||[]),...(map.conveyors||[]),...(map.bridges||[])];
 const protectedPoints=[...(map.camps||[]).map(p=>({...p,r:250})),...(map.expansions||[]).map(p=>({...p,r:145})),...(game.entities||[]).filter(e=>e.type==='trash').map(p=>({...p,r:95}))];
 const lanes=(map.lanes||[]).map(p=>Array.isArray(p)?p:p.points||[]);
 for(let i=0;i<Math.min(180,Math.ceil(w*h/110000));i++){
  const p={x:125+(i*719.371)%(Math.max(1,w-250)),y:125+(i*431.719)%(Math.max(1,h-250)),kind:i%6,angle:Math.sin(i*1.97)*.28,seed:i};
  if(blocked.some(b=>boxContains(p,b,75))||protectedPoints.some(q=>Math.hypot(p.x-q.x,p.y-q.y)<q.r))continue;
  // Main routes are up to 178 px wide; keep the whole rotated illustration
  // (up to 60 px from its center), rather than just its center, off that surface.
  if(lanes.some(points=>points.some((b,j)=>j&&segmentDistance(p,points[j-1],b)<150)))continue;
  placements.push(p);
 }
 return placements;
}

function memo(c,palette,seed){
 // Printed folds and torn perforations keep the scrap visibly flat.
 card(c,-40,-26,80,52,palette.paper,palette.edge);stroke(c,[[-39,18],[24,18],[38,7]],palette.edge,1.1);
 c.beginPath();c.moveTo(25,-26);c.lineTo(40,-11);c.lineTo(25,-11);c.closePath();shape(c,palette.fold,palette.edge,.8);
 for(let i=0;i<4;i++)stroke(c,[[-29,-12+i*8],[seed%2?9:20,-12+i*8]],palette.rule,1.5);
 for(let x=-31;x<25;x+=11)oval(c,x,24,1.4,1.1,palette.edge);
 card(c,-15,-32,32,11,palette.tape);stroke(c,[[-10,-29],[12,-27]],palette.edge,.6);
}
function clipPrint(c,palette){
 c.save();c.rotate(-.4);c.beginPath();c.moveTo(-12,-22);c.lineTo(-12,21);c.bezierCurveTo(-12,38,15,38,15,20);c.lineTo(15,-24);c.bezierCurveTo(15,-39,-7,-39,-7,-24);c.lineTo(-7,19);c.bezierCurveTo(-7,26,8,26,8,18);c.lineTo(8,-20);shape(c,null,palette.edge,3.3);stroke(c,[[-11,-18],[-11,19]],palette.paper,1);c.restore();
 for(let i=0;i<4;i++){const x=29+i*6,y=9+i%2*8;stroke(c,[[x,y-3],[x+4,y+2]],palette.rule,1.3);}
}
function coffee(c,palette){
 c.beginPath();c.ellipse(0,0,43,29,-.1,.2,TAU-.3);shape(c,null,palette.stain,5);c.beginPath();c.ellipse(-1,-1,37,24,-.1,.1,5.7);shape(c,null,palette.stain,1.3);
 for(let i=0;i<7;i++)oval(c,Math.sin(i*3.1)*38,35+(i*7)%22,2+i%3,1.4+i%2,palette.stain);
 stroke(c,[[-19,-3],[-9,2],[4,0],[17,4]],palette.rule,.8);
}
function circuit(c,palette){
 const paths=[[[-43,18],[-19,18],[-19,-19],[3,-19],[3,-36]],[[-42,32],[-7,32],[-7,4],[23,4],[23,-21]],[[8,40],[8,21],[40,21],[40,-5]]];
 for(const points of paths){stroke(c,points,palette.rule,2.4);for(const q of [points[0],points.at(-1)])oval(c,q[0],q[1],4,4,palette.paper,palette.rule,1.7);}
 card(c,8,-29,29,16,palette.fold,palette.edge);for(let i=0;i<4;i++){stroke(c,[[12+i*6,-32],[12+i*6,-28]],palette.edge);stroke(c,[[12+i*6,-13],[12+i*6,-9]],palette.edge);}
}
function carton(c,palette){
 c.beginPath();c.moveTo(-46,-15);c.lineTo(-18,-24);c.lineTo(0,-17);c.lineTo(25,-26);c.lineTo(46,-17);c.lineTo(42,25);c.lineTo(18,31);c.lineTo(-8,24);c.lineTo(-38,30);c.closePath();shape(c,palette.fold,palette.edge,.9);
 for(let i=0;i<7;i++)stroke(c,[[-33+i*10,-12],[-34+i*10,19]],palette.rule,.8);
 for(let i=0;i<3;i++){c.save();c.rotate(i*TAU/3);stroke(c,[[1,-17],[15,7],[9,3]],palette.edge,2.2);stroke(c,[[15,7],[14,-1]],palette.edge,2.2);c.restore();}
}
function botanical(c,palette){
 stroke(c,[[-32,26],[2,-1],[28,-27]],palette.edge,1.6);
 for(let i=0;i<6;i++){c.save();c.translate(-23+i*9,18-i*8);c.rotate(i%2?-.9:.4);oval(c,i%2?6:-6,-4,12,4,palette.fold,palette.rule,.8);c.restore();}
 for(let i=0;i<5;i++)card(c,-40+(i*29)%75,34+(i%2)*5,5+i%2*3,2,palette.edge);
}

export function paintTabletopDetails(renderer,c){
 const game=renderer.game,map=game.map||{},id=map.id;
 const palette=id==='breakroom'?{paper:'#f5efd38a',fold:'#b4c49663',edge:'#7b916c80',rule:'#9baf8870',tape:'#d4c99570',stain:'#9f74402e'}:
 id==='conveyor'?{paper:'#dfe6ce77',fold:'#adbfab63',edge:'#637d7580',rule:'#87a89870',tape:'#d7bc7b66',stain:'#526b6233'}:
 id==='canyon'?{paper:'#f5e3bb88',fold:'#d2b27b68',edge:'#9e875e88',rule:'#ad996a77',tape:'#d4bb8366',stain:'#9b73412b'}:
 {paper:'#fff6da99',fold:'#d1d9b66b',edge:'#899b7b88',rule:'#a5b28b77',tape:'#e0c57e66',stain:'#a67c3d2b'};
 const motifs=[memo,clipPrint,coffee,circuit,carton,botanical];
 for(const p of tabletopDetailPlacements(game)){c.save();c.translate(p.x,p.y);c.rotate(p.angle);c.globalAlpha=.85;motifs[(p.kind+(id==='conveyor'?3:id==='canyon'?4:0))%motifs.length](c,palette,p.seed);c.restore();}
 // Scuffs follow actual solid props, reinforcing their footprint and scale.
 for(const [index,o]of (map.obstacles||[]).entries()){
  if(['cliff','canyon','gatebarrier','gate','staplergate'].includes(o.type))continue;
  c.save();c.globalAlpha=.38;
  for(let i=0;i<7;i++){const x=o.x+(i+.4)*o.w/7,y=o.y+o.h+18+(index+i)%3*5;stroke(c,[[x-11,y],[x+7,y-3]],palette.edge,1.4);}
  if(['machine','recycler'].includes(o.type))for(const side of [-1,1]){const x=side<0?o.x-22:o.x+o.w+22;for(let y=o.y+12;y<o.y+o.h;y+=27)stroke(c,[[x-7,y],[x+7,y+12]],'#b39655',5);}
  c.restore();
 }
 // Ramp approach marks lie outside the slope, leaving the exact access edge clear.
 for(const r of map.ramps||[]){const horizontal=['west','east'].includes(r.side),cx=r.x+r.w/2,cy=r.y+r.h/2;
  c.save();c.globalAlpha=.45;
  for(const side of [-1,1])for(let i=0;i<3;i++){
   const x=horizontal?cx+side*(r.w/2+12+i*11):cx,y=horizontal?cy:cy+side*(r.h/2+12+i*11);
   if(horizontal)stroke(c,[[x,y-14],[x,y+14]],palette.edge,2-i*.4);else stroke(c,[[x-14,y],[x+14,y]],palette.edge,2-i*.4);
  }c.restore();
 }
}
