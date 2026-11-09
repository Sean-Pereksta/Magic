export const MAP_BRIEFS={
 desktop:{name:'Desktop Meadow',tag:'Open routes · raised lookouts',plan:'Scout the wide flanks, claim a natural expansion, and use the raised desks to watch the main approach.'},
 breakroom:{name:'Breakroom Blitz',tag:'Cliff ramps · cover pockets',plan:'Control the ramps before committing. Cover protects expansions, while the long outer routes reward a split army.'},
 canyon:{name:'Stapler Canyon',tag:'Three crossings · breakable gate',plan:'Watch both bridges. The central gate offers a direct push, while the southern crossing leaves space for a flank.'},
 conveyor:{name:'Conveyor Chaos',tag:'Moving lanes · capture controls',plan:'Capture belt controls to move reinforcements faster. Scout the outer loop to catch raids before they reach your workers.'},
 inkdelta:{name:'Inkspill Delta',tag:'Three causeways · rich outer fields',plan:'Hold a causeway while scouting the others. Outer expansions pay more, but leave your supply lines exposed.'},
 corkcross:{name:'Corkboard Crossroads',tag:'Central heights · radial flanks',plan:'Trade central vision against safer outer routes. A second army can threaten expansions while your main force holds the crossroads.'},
 serverstack:{name:'Server Stack',tag:'Service corridors · divided fronts',plan:'Use the broad service lanes to split pressure. Aircraft can bypass the racks, so keep anti-air near your economy.'}
};
export const DIFFICULTY_BRIEFS={
 easy:'Easy learns slowly: smaller attack groups, later expansions, and more time to recover. Standard resources.',
 normal:'Normal scouts, expands, and rotates attack plans. Standard resources for both sides.',
 hard:'Hard reacts faster, counters what it scouts, protects its workers, and coordinates raids with larger pushes. Standard resources.',
 impossible:'Impossible cheats: each bot starts with 300 extra trash, earns 2× trash from gathering, and gets 2 trash per second (up to 1,200 extra).'
};

// The preview uses the actual collision, lane and expansion geometry.
export function paintBattlefieldPreview(canvas,map){
 const c=canvas?.getContext?.('2d');if(!c||typeof c.clearRect!=='function')return;
 const w=canvas.width,h=canvas.height,pad=10,scale=Math.min((w-pad*2)/map.width,(h-pad*2)/map.height),ox=(w-map.width*scale)/2,oy=(h-map.height*scale)/2;
 c.clearRect(0,0,w,h);c.fillStyle='#eae6d5';c.fillRect(0,0,w,h);c.save();c.translate(ox,oy);c.scale(scale,scale);
 c.fillStyle=map.color||'#dfdfc9';c.fillRect(0,0,map.width,map.height);
 c.strokeStyle='#89987822';c.lineWidth=1/scale;
 for(let x=0;x<map.width;x+=600){c.beginPath();c.moveTo(x,0);c.lineTo(x,map.height);c.stroke();}
 for(let y=0;y<map.height;y+=600){c.beginPath();c.moveTo(0,y);c.lineTo(map.width,y);c.stroke();}
 for(const p of map.plateaus||[]){c.fillStyle='#899f7750';c.fillRect(p.x,p.y,p.w,p.h);}
 for(const o of map.obstacles||[]){c.fillStyle=['canyon','inkpool'].includes(o.type)?'#6b727b':'#8b856c80';c.fillRect(o.x,o.y,o.w,o.h);}
 for(const [i,lane]of (map.lanes||[]).entries()){
  const info=map.laneInfo?.[i],points=Array.isArray(lane)?lane:lane.points||[];if(points.length<2)continue;
  c.strokeStyle=info?.role==='flank'?'#569080':'#c99a50';c.lineWidth=(info?.role==='flank'?2:2.8)/scale;c.lineCap='round';c.lineJoin='round';c.setLineDash(info?.role==='flank'?[5/scale,4/scale]:[]);c.beginPath();points.forEach(([x,y],j)=>j?c.lineTo(x,y):c.moveTo(x,y));c.stroke();
 }
 c.setLineDash([]);
 for(const p of map.expansions||[]){const rich=(p.amountMultiplier||1)>1;c.beginPath();c.arc(p.x,p.y,(rich?3.8:2.8)/scale,0,Math.PI*2);c.fillStyle=rich?'#e0ae50':'#f6efbd';c.fill();c.strokeStyle='#6d805d';c.lineWidth=1/scale;c.stroke();}
 for(const [i,p]of (map.camps||[]).entries()){const r=5/scale;c.fillStyle=['#438d7a','#cf7761','#9779ac','#be9d48'][i];c.strokeStyle='#fbf3d8';c.lineWidth=1.5/scale;c.fillRect(p.x-r,p.y-r,r*2,r*2);c.strokeRect(p.x-r,p.y-r,r*2,r*2);}
 c.restore();
}
