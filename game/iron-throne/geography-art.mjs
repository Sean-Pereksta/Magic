import { maskEdges, shorelineRuns } from './geography.mjs';
import { geographyLayers } from './geography-assets.mjs';

export const HEX_RADIUS=25, APOTHEM=HEX_RADIUS*Math.sqrt(3)/2;
const polar=(r,angle)=>[r*Math.cos(angle*Math.PI/180),r*Math.sin(angle*Math.PI/180)];
export const hexVertex=i=>polar(HEX_RADIUS,60*i-30);
export const edgePort=i=>polar(APOTHEM,60*i);
const mul=(p,k)=>p.map(v=>v*k), add=(a,b)=>a.map((v,i)=>v+b[i]);
const mix=(a,b,k)=>a.map((v,i)=>v+(b[i]-v)*k);
const xy=p=>p.map(v=>Number(v.toFixed(5))).join(' ');
const polygon=ps=>'M '+ps.map(xy).join(' L ')+' Z';
export const HEX_PATH=polygon([0,1,2,3,4,5].map(hexVertex));
const layer=(d,fill,stroke,width=1,extra={})=>({d,fill,stroke,width,...extra});
const circle=(x,y,r)=>`M ${x-r} ${y} a ${r} ${r} 0 1 0 ${r*2} 0 a ${r} ${r} 0 1 0 ${-r*2} 0`;

// Shared shoreline endpoints lie exactly 5.5 units from a wet vertex on
// the adjoining dry edge. Both land tiles compute the same world point.
export function shoreGeometry(mask) {
  if (!mask) return [];
  if (mask===63) return [{shore:circle(0,0,13.5),water:HEX_PATH+' '+circle(0,0,13.5),closed:true}];
  return shorelineRuns(mask).map(run=>{
    const first=run[0],last=run.at(-1),a=hexVertex(first),b=hexVertex(last+1);
    const start=mix(a,hexVertex(first-1),.22),end=mix(b,hexVertex(last+2),.22);
    const firstControl=add(start,mul(edgePort((first+5)%6),-.16));
    const lastControl=add(end,mul(edgePort((last+1)%6),-.16));
    const points=[];
    for (const edge of run) {
      points.push(polar(APOTHEM-(run.length===1?6.7:6),edge*60));
      if (edge!==last) points.push(mul(hexVertex(edge+1),.66));
    }
    let shore=`M ${xy(start)} Q ${xy(firstControl)} ${xy(mix(firstControl,points[0],.5))}`;
    for(let i=0;i<points.length;i++) shore+=` Q ${xy(points[i])} ${xy(mix(points[i],points[i+1]||lastControl,.5))}`;
    shore+=` Q ${xy(lastControl)} ${xy(end)}`;
    let water=shore+` L ${xy(b)}`;
    for(let i=run.length-1;i>=0;i--) water+=` L ${xy(hexVertex(run[i]))}`;
    water+=' Z';
    return {shore,water,start,end};
  });
}

// Reuse the shoreline's exact water cutouts, including islands and split shores.
const landClips=new Map();
export function landClipPath(mask) {
  if(!landClips.has(mask)) landClips.set(mask,new Path2D(
    HEX_PATH+' '+shoreGeometry(mask).map(g=>g.water).join(' ')
  ));
  return landClips.get(mask);
}

export function coastDrawing(mask,style='beach') {
  const layers=[],rocky=style==='cliff';
  for(const g of shoreGeometry(mask)) {
    layers.push(layer(g.shore,null,rocky?'#4e5b51':'#6a7950',8));
    layers.push(layer(g.shore,null,rocky?'#aba38a':'#d2bc89',5.5));
    layers.push(layer(g.shore,null,rocky?'#d2c8ab':'#eddbad',3.2));
    layers.push(layer(g.water,'#286579',null,0,{fillRule:'evenodd'}));
    layers.push(layer(g.shore,null,'#78b5af',1.9));
    layers.push(layer(g.shore,null,'#e0e8cb',.65));
    // Broken inner surf catches the light without changing the edge contract.
    layers.push(layer(g.shore,null,'#fff4d8',.35,{dash:[1.1,1.6]}));
  }
  return layers;
}

export function riverPaths(mask) {
  const edges=maskEdges(mask);
  if(edges.length===2) {
    const a=edgePort(edges[0]),b=edgePort(edges[1]);
    return [`M ${xy(a)} C ${xy(mul(a,.35))} ${xy(mul(b,.35))} ${xy(b)}`];
  }
  return edges.map(edge=>{
    const p=edgePort(edge),control=mul(p,.45);
    return `M ${xy(p)} C ${xy(control)} 0 0 0 0`;
  });
}
export function riverDrawing(mask,mouthMask=0,active=true) {
  if(!active) return [];
  const paths=riverPaths(mask),layers=[];
  // Stroke all banks before all channels so fork intersections have no plugs.
  for(const [color,width] of [['#38594b',6.8],['#b4ab7b',5.5],['#73a99e',4.5],['#286579',3.5]])
    for(const d of paths) layers.push(layer(d,null,color,width));
  if(maskEdges(mask).length<=1) {
    layers.push(layer(circle(0,0,3.3),'#b4ab7b',null));
    layers.push(layer(circle(0,0,2.5),'#286579',null));
    layers.push(layer('M -1.3 -.6 Q 0 -1.5 1.3 -.6',null,'#c8e3d0',.45));
  }
  for(const d of paths) layers.push(layer(d,null,'#b5d7c7',.4,{dash:[1.8,2.4]}));
  return layers.concat(mouthDrawing(mouthMask));
}

export function mouthDrawing(mouthMask=1) {
  const layers=[];
  // A mouth widens into the shoreline water, covers the beach/surf, and
  // finishes flush at the sea edge. It is a filled channel, not an end cap.
  for(const edge of maskEdges(mouthMask)) {
    const u=polar(1,edge*60),v=[-u[1],u[0]],p=(r,w)=>add(mul(u,r),mul(v,w));
    const mouth=`M ${xy(p(6,-1.75))} C ${xy(p(13,-2))} ${xy(p(16,-4.5))} ${xy(p(APOTHEM,-5.5))} L ${xy(p(APOTHEM,5.5))} C ${xy(p(16,4.5))} ${xy(p(13,2))} ${xy(p(6,1.75))} Z`;
    layers.push(layer(mouth,'#286579',null));
    layers.push(layer(`M ${xy(p(8,-.4))} Q ${xy(p(16,-1))} ${xy(p(20,-2.2))}`,null,'#85bbb4',.5));
    layers.push(layer(`M ${xy(p(12,1))} Q ${xy(p(17,1.7))} ${xy(p(21,3))}`,null,'#b0d1be',.35));
  }
  return layers;
}

export function drawingSVG(layers) {
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="-25 -25 50 50" width="512" height="512"><defs><clipPath id="hex"><path d="${HEX_PATH}"/></clipPath></defs><g clip-path="url(#hex)" stroke-linecap="round" stroke-linejoin="round">${layers.map(p=>`<path d="${p.d}" fill="${p.fill||'none'}" fill-rule="${p.fillRule||'nonzero'}" stroke="${p.stroke||'none'}" stroke-width="${p.width}"${p.dash?` stroke-dasharray="${p.dash.join(' ')}"`:''}/>`).join('')}</g></svg>\n`;
}

export class GeographyArt {
  constructor() { this.cache=new Map(); }
  draw(c,g,x,y,assets,urls) {
    if(!g.coastMask&&!g.hasRiver) return;
    // Render the existing shore transparently; opaque coast PNGs would hide water_01.
    if(g.coastMask) this.drawNative(c,{...g,hasRiver:false,riverMask:0,mouthMask:0},x,y);
    if(!g.hasRiver) return;
    g={...g,coastMask:0};
    // Wait for the entire set so a missing PNG never produces a partial river.
    if(assets&&urls) {
      const layers=geographyLayers(g).map(layer=>({...layer,image:assets.get(urls[layer.name])}));
      if(layers.length&&layers.every(layer=>layer.image)) {
        for(const layer of layers) {
          c.save();c.translate(x,y);c.clip(new Path2D(HEX_PATH));c.rotate(layer.rotation*Math.PI/3);
          // Generated spring art has a measured 58px vertical offset on its
          // 1024px canvas. Rotate that correction with the baked sprite.
          const angle=Number(layer.name.slice(-3))*Math.PI/180;
          if(layer.name.startsWith('river_straight_')) {
            c.rotate(angle);c.transform(1,-.02424,0,1,0,-1.57);c.rotate(-angle);
          }
          if(layer.name.startsWith('river_source_'))
            c.translate(Math.sin(angle)*2.83,-Math.cos(angle)*2.83);
          c.drawImage(layer.image,-25,-25,50,50);c.restore();
        }
        return;
      }
    }
    this.drawNative(c,g,x,y);
  }
  drawNative(c,g,x,y) {
    const style=['hills','mountain'].includes(g.ground)?'cliff':'beach';
    const key=`${g.coastMask}:${g.riverMask}:${g.mouthMask}:${g.hasRiver}:${style}`;
    let sprite=this.cache.get(key);
    if(!sprite) {
      sprite=document.createElement('canvas');sprite.width=sprite.height=208;
      const ctx=sprite.getContext('2d');ctx.scale(4,4);ctx.translate(26,26);
      ctx.lineCap=ctx.lineJoin='round';ctx.clip(new Path2D(HEX_PATH));
      for(const p of [...coastDrawing(g.coastMask,style),...riverDrawing(g.riverMask,g.mouthMask,g.hasRiver)]) {
        const path=new Path2D(p.d);
        if(p.fill){
          ctx.save();
          // Erase only the coast sprite's sea side, revealing the water beneath.
          if(p.fillRule==='evenodd')ctx.globalCompositeOperation='destination-out';
          ctx.fillStyle=p.fill;ctx.fill(path,p.fillRule||'nonzero');ctx.restore();
        }
        if(p.stroke){ctx.strokeStyle=p.stroke;ctx.lineWidth=p.width;ctx.setLineDash(p.dash||[]);ctx.stroke(path);}
      }
      if(this.cache.size>=256) this.cache.delete(this.cache.keys().next().value);
      this.cache.set(key,sprite);
    }
    c.drawImage(sprite,x-26,y-26,52,52);
  }
}
