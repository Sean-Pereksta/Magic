// Tactical tabletop terrain. Cliffs participate in the same collision system as
// scenery, while wide, two-ended ramps keep every lookout reachable on foot.
const inside=(x,y,r)=>x>=r.x&&x<=r.x+r.w&&y>=r.y&&y<=r.y+r.h;

export function enrichTerrain(map){
  const layouts=map.terrainLayouts?map.terrainLayouts.map(row=>[...row]):map.players===2?
    [[1280,640,600,320,'Northwest overlook'],[2920,2240,600,320,'Southeast overlook'],[2920,640,600,320,'Northeast overlook'],[1280,2240,600,320,'Southwest overlook'],[2200,1490,400,220,'Center crossing']]:map.players===3?
    [[840,980,600,560,'West terrace'],[3360,980,600,560,'East terrace'],[1980,800,840,320,'Northern overlook'],[2120,2460,560,320,'Southern lookout']]:
    [[2040,140,720,400,'North lookout'],[2040,2660,720,400,'South lookout'],[2080,1440,640,320,'Center crossing'],[680,1350,500,500,'West flank'],[3620,1350,500,500,'East flank'],[2120,720,560,240,'Northern ridge']];
  if(map.players===2&&map.id==='breakroom'){
    layouts[2][0]=3500;layouts[3][0]=700;
  }
  if(map.players===2&&map.id==='desktop')layouts[4]=[2200,1440,400,320,'Center crossing'];
  map.plateaus=[];map.ramps=[];map.cliffs=[];
  for(const [index,[x,y,w,h,label,options={}]] of layouts.entries()){
    const id=`lookout-${index}`,height=1,sides=options.sides||['west','east'],depth=options.depth||200;
    const plateau={id,x,y,w,h,height,label,ramps:[]};
    for(const side of sides){
      const vertical=side==='west'||side==='east',opening=Math.min(options.opening||240,(vertical?h:w)-32);
      const ramp={id:`${id}-${side}`,plateauId:id,side,x:vertical?(side==='west'?x-depth/2:x+w-depth/2):x+(w-opening)/2,y:vertical?y+(h-opening)/2:side==='north'?y-depth/2:y+h-depth/2,w:vertical?depth:opening,h:vertical?opening:depth,height};
      plateau.ramps.push(ramp);map.ramps.push(ramp);
    }
    const cliff=(cx,cy,cw,ch)=>map.cliffs.push({x:cx,y:cy,w:cw,h:ch,type:'cliff',terrain:true,plateauId:id,height});
    for(const side of ['west','east','north','south']){
      const ramp=plateau.ramps.find(r=>r.side===side),vertical=side==='west'||side==='east';
      if(vertical){const cx=side==='west'?x-7:x+w-7;if(ramp){cliff(cx,y+7,14,ramp.y-y-7);cliff(cx,ramp.y+ramp.h,14,y+h-7-ramp.y-ramp.h);}else cliff(cx,y+7,14,h-14);}
      else{const cy=side==='north'?y-7:y+h-7;if(ramp){cliff(x-7,cy,ramp.x-x+7,14);cliff(ramp.x+ramp.w,cy,x+w+7-ramp.x-ramp.w,14);}else cliff(x-7,cy,w+14,14);}
    }
    map.plateaus.push(plateau);
  }
  // Natural expansions shorten the first economical step outward, while side
  // fields and elevated lookouts support longer scouting and flanking routes.
  for(const camp of map.camps){
    const dx=map.width/2-camp.x,dy=map.height/2-camp.y,len=Math.hypot(dx,dy),distance=map.players===3&&camp.y<map.height/2?760:620;
    const point={x:Math.round(camp.x+dx/len*distance),y:Math.round(camp.y+dy/len*distance),natural:true};
    if(!map.expansions.some(p=>Math.hypot(p.x-point.x,p.y-point.y)<300))map.expansions.push(point);
  }
  for(const p of map.plateaus){
    const point={x:p.x+p.w/2,y:p.y+p.h/2,lookout:true};
    if(!map.expansions.some(e=>Math.hypot(e.x-point.x,e.y-point.y)<180))map.expansions.push(point);
    const cy=p.y+p.h/2;
    map.lanes.push([[p.x-300,cy],[p.x+p.w/2,cy],[p.x+p.w+300,cy]]);
  }
  map.obstacles.push(...map.cliffs.filter(o=>o.w>0&&o.h>0));
  const overlaps=(a,b,pad=0)=>a.x<b.x+b.w+pad&&a.x+a.w>b.x-pad&&a.y<b.y+b.h+pad&&a.y+a.h>b.y-pad;
  const candidates=map.terrainLayouts?[]:[[180,600],[180,1250],[180,2050],[180,2700],[4380,600],[4380,1250],[4380,2050],[4380,2700],[700,100],[1500,100],[3100,100],[3900,100],[700,2950],[1500,2950],[3100,2950],[3900,2950]];
  let clusters=0;
  for(const [x,y] of candidates){
    const first={x,y,w:200,h:130,type:map.id==='desktop'?'book':'cup'},second={x:x+85,y:y+185,w:240,h:65,type:map.id==='desktop'?'ruler':'eraser'},box={x,y,w:325,h:250};
    if(map.obstacles.some(o=>overlaps(box,o,65))||map.plateaus.some(p=>overlaps(box,p,120))||map.camps.some(p=>Math.hypot(p.x-x-160,p.y-y-120)<460)||map.expansions.some(p=>Math.hypot(p.x-x-160,p.y-y-120)<280))continue;
    map.obstacles.push(first,second);clusters++;
  }
  map.coverClusters=clusters;
  // Decorative regions are deterministic so the main view, minimap and lobby
  // preview describe the same battlefield without a random per-frame cost.
  map.regions=map.expansions.map((p,i)=>({x:p.x,y:p.y,r:200+(i%3)*24,type:map.id==='desktop'?'scrap':'tile',label:p.natural?'Natural expansion':p.lookout?'Elevated scrap field':'Scrap field'}));
  return map;
}

export function terrainHeight(map,x,y){
  for(const ramp of map.ramps||[])if(inside(x,y,ramp)){
    const t=ramp.side==='west'?(x-ramp.x)/ramp.w:ramp.side==='east'?1-(x-ramp.x)/ramp.w:ramp.side==='north'?(y-ramp.y)/ramp.h:1-(y-ramp.y)/ramp.h;
    return Math.max(0,Math.min(ramp.height,t*ramp.height));
  }
  return (map.plateaus||[]).some(p=>inside(x,y,p))?1:0;
}

export function terrainSight(map,source,x,y,flying=false){
  if(flying)return true;
  const height=terrainHeight(map,source.x,source.y),target=terrainHeight(map,x,y);
  if(target>height+.45)return false;
  // A raised shelf also occludes low ground behind it. Sampling along a ray is
  // independent of navigation geometry, so looking through a ramp is gradual.
  const dx=x-source.x,dy=y-source.y,steps=Math.ceil(Math.hypot(dx,dy)/32);
  for(let i=1;i<steps;i++)if(terrainHeight(map,source.x+dx*i/steps,source.y+dy*i/steps)>height+.45)return false;
  return true;
}
