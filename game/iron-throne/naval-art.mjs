import { ART } from './asset-manifest.mjs';
import { cargoCount, fleetCapacity } from './naval-state.mjs';
export function drawFleets(map,c,s,hexPixel,inView,colors) {
  const groups=new Map();
  for(const f of s.fleets||[]){const key=`${f.node}:${f.owner}`;if(!groups.has(key))groups.set(key,[]);groups.get(key).push(f);}
  const rows=new Map();
  for(const group of groups.values()){
    const f=group[0],tile=s.tiles[f.tile];if(!tile)continue;const p=hexPixel(tile);if(!inView(p))continue;
    const row=rows.get(f.tile)||0;rows.set(f.tile,row+1);
    const ships=group.flatMap(f=>f.ships),type=ships.some(v=>v.type==='warship')?'warship':ships.some(v=>v.type==='transport')?'transport':'warCanoe';
    const y=p.y+row*27+(f.node.startsWith('river:')?14:0),scale=Math.max(1,.65/map.zoom);
    c.save();c.translate(p.x,y);c.scale(scale,scale);c.strokeStyle=colors[f.owner];c.lineWidth=1.8;c.beginPath();c.ellipse(0,4,20,10,0,0,Math.PI*2);c.stroke();
    if(!map.assets.draw(c,ART.ships[type],-20,-22,40,32)){c.fillStyle=colors[f.owner];c.font='22px Georgia';c.textAlign='center';c.fillText('⚓',0,2);}
    c.fillStyle='#102932';c.fillRect(-25,11,50,12);c.fillStyle='#fff1d0';c.font='bold 8px system-ui';c.textAlign='center';
    const troops=group.some(f=>f.cargoUnknown)?'?':group.reduce((n,f)=>n+cargoCount(f),0);
    c.fillText(`${ships.length} ships · ${troops}/${group.reduce((n,f)=>n+fleetCapacity(f),0)}`,0,20);c.restore();
    map.hits.push({tile:f.tile,left:p.x-25*scale,right:p.x+25*scale,top:y-22*scale,bottom:y+24*scale});
  }
  for(const f of s.lastSeenFleets||[]){const t=s.tiles[f.tile];if(!t)continue;const p=hexPixel(t);if(!inView(p))continue;c.save();c.globalAlpha=.5;c.fillStyle=colors[f.owner];c.font='12px Georgia';c.textAlign='center';c.fillText(`⚓ T${f.turn}`,p.x,p.y);c.restore();}
}
