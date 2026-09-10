'use strict';
/* Static ground is cached in room coordinates; scenery, telegraphs and gameplay stay dynamic. */
(() => {
  if(window.AWWorld)return;
  let cache=null,caching=false;
  const margin=60,originX=ROOM_H*TILE_W*.5+margin,originY=margin;
  const width=(ROOM_W+ROOM_H)*TILE_W*.5+margin*2,height=(ROOM_W+ROOM_H)*TILE_H*.5+margin*2;
  function projectStatic(x,y,z=0){return {x:(x-y)*TILE_W*.5+originX,y:(x+y)*TILE_H*.5+originY-z};}
  function floor(room,pal){
    const P=window.AWPresentation,resolution=P.quality==='low'?.6:isTouch?.8:1;
    if(!cache||cache.room!==room||cache.resolution!==resolution){
      const bitmap=typeof OffscreenCanvas==='function'?new OffscreenCanvas(Math.ceil(width*resolution),Math.ceil(height*resolution)):document.createElement('canvas');
      bitmap.width=Math.ceil(width*resolution);bitmap.height=Math.ceil(height*resolution);const target=bitmap.getContext('2d');
      if(!target){drawRoomBase(room,pal);drawFloorDetails(room,pal);return;}
      const screenContext=ctx;
      try{caching=true;ctx=target;ctx.setTransform(resolution,0,0,resolution,0,0);drawRoomBase(room,pal);drawFloorDetails(room,pal);cache={room,resolution,bitmap};}
      finally{ctx=screenContext;caching=false;}
    }
    const origin=worldToScreen(0,0),zoom=P.camera.zoom;ctx.drawImage(cache.bitmap,origin.x-originX*zoom,origin.y-originY*zoom,width*zoom,height*zoom);
  }
  function path(points){ctx.beginPath();for(let i=0;i<points.length;i++){const s=worldToScreen(points[i].x,points[i].y);if(i)ctx.lineTo(s.x,s.y);else ctx.moveTo(s.x,s.y);}ctx.closePath();}
  function circle(x,y,r,progress=1){ctx.beginPath();for(let i=0;i<=48;i++){const a=i/48*TAU*progress,s=worldToScreen(x+Math.cos(a)*r,y+Math.sin(a)*r);if(i)ctx.lineTo(s.x,s.y);else ctx.moveTo(s.x,s.y);}if(progress===1)ctx.closePath();}
  function telegraph(t,entity,time){
    const p=clamp(1-time/(t.maxTime||1),0,1),x=Number.isFinite(t.x)?t.x:entity.x,y=Number.isFinite(t.y)?t.y:entity.y,color=t.color||(entity.boss?'#ff98da':'#ff9a80');
    if(!Number.isFinite(x)||!Number.isFinite(y))return;
    ctx.save();ctx.strokeStyle=color;ctx.fillStyle=colorAlpha(color,.08+p*.11);ctx.lineWidth=2.1;ctx.globalAlpha=.65+p*.35;
    if(t.dir){
      const d=t.dir,range=t.range||7;
      if(t.cone){const a=Math.atan2(d.y,d.x),points=[{x,y}];for(let i=0;i<=24;i++){const angle=a-t.cone+(i/24)*t.cone*2;points.push({x:x+Math.cos(angle)*range,y:y+Math.sin(angle)*range});}path(points);ctx.fill();ctx.stroke();}
      else{const r=t.width||.45,nx=-d.y*r,ny=d.x*r;path([{x:x+nx,y:y+ny},{x:x+nx+d.x*range,y:y+ny+d.y*range},{x:x-nx+d.x*range,y:y-ny+d.y*range},{x:x-nx,y:y-ny}]);ctx.fill();ctx.stroke();}
      const a=worldToScreen(x,y),b=worldToScreen(x+d.x*range*p,y+d.y*range*p);ctx.lineWidth=3;ctx.beginPath();ctx.moveTo(a.x,a.y);ctx.lineTo(b.x,b.y);ctx.stroke();
    }else{const r=t.r||1.2;circle(x,y,r);ctx.fill();ctx.stroke();ctx.lineWidth=3;circle(x,y,r*.91,p);ctx.stroke();const c=worldToScreen(x,y);ctx.beginPath();ctx.moveTo(c.x-4,c.y);ctx.lineTo(c.x+4,c.y);ctx.moveTo(c.x,c.y-3);ctx.lineTo(c.x,c.y+3);ctx.stroke();}
    ctx.restore();
  }
  function propOpacity(p){if(!game.player)return 1;const a=worldToScreen(p.x,p.y),b=worldToScreen(game.player.x,game.player.y);return /tree|oak|pine|willow|house|column|obelisk|market/i.test(p.type)&&Math.abs(a.x-b.x)<38*(p.scale||1)&&b.y<a.y+10&&b.y>a.y-80*(p.scale||1)?.3:1;}
  function villageAccent(n){
    if(n.role!=='Blacksmith')return;
    const s=worldToScreen(n.x,n.y),a=Math.sin(elapsed*5+n.phase);ctx.save();ctx.translate(s.x+12,s.y-19);ctx.rotate(-.7+a*.65);ctx.strokeStyle='#876d54';ctx.lineWidth=3;ctx.beginPath();ctx.moveTo(0,10);ctx.lineTo(0,-5);ctx.stroke();ctx.fillStyle='#afbbc5';ctx.fillRect(-6,-9,12,6);ctx.restore();
  }
  window.AWWorld={floor,telegraph,projectStatic,propOpacity,villageAccent,invalidate(){cache=null;},get caching(){return caching;}};
})();
