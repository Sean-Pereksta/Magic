/* Cached, procedural canvas assets. All random-looking detail is local and
   deterministic. Planet body colors never inherit an empire's color. */
const STX_GV_PALETTES = {
  Terran:['#124572','#3e8868','#b4c594'], Ocean:['#082d67','#128fa9','#77d4d8'],
  Desert:['#744731','#c19157','#eed9a0'], Volcanic:['#211c2b','#59332f','#ff852d'],
  Ice:['#527d9d','#b2d6e5','#edfaff'], Barren:['#423f49','#87818a','#b5a6a0'],
  Toxic:['#343749','#697b33','#c7d55f'], Jungle:['#143b41','#367951','#8eb369'],
  Industrial:['#293440','#65677b','#d49664'], 'Gas giant':['#645375','#c08c88','#ecd0a0'],
  Exotic:['#272c61','#6759bd','#72efd9']
};
function stxGVRgb(hex){return [1,3,5].map(i=>parseInt(hex.slice(i,i+2),16))}
function stxGVMix(a,b,t){return a.map((v,i)=>v+(b[i]-v)*t)}
function stxGVNoise(x,y,z,seed){
  // Coherent spherical harmonics: seamless at the limb, no random calls or
  // per-frame noise. Several frequencies produce continents and fine terrain.
  const phase=(seed%10000)*.017;
  return Math.sin(x*3.4+Math.sin(y*4.1+phase)+z*2.2+phase)*.48+
    Math.sin(y*7.3-z*4.6+Math.sin(x*6.1+phase))*.28+
    Math.sin(z*16+x*13+y*11+phase)*.14+Math.sin(x*39-y*31+z*24)*.07;
}
function stxGVBuildPlanet(p){
  const family=stxGVFamily(p),seed=stxGVHash(`${p.id}:${p.name}:${p.x}:${p.y}`);
  const resolution=96,texture=document.createElement('canvas');texture.width=texture.height=resolution;
  const c=texture.getContext('2d'),pixels=c.createImageData(resolution,resolution);
  // The shipping browser always provides ImageData; lightweight simulation
  // harnesses may intentionally provide only a no-op canvas.
  if(!pixels?.data)return texture;
  const palette=STX_GV_PALETTES[family].map(stxGVRgb),radius=46;
  const craterSeed=seed%101;
  for(let py=0;py<resolution;py++)for(let px=0;px<resolution;px++){
    const x=(px-47.5)/radius,y=(py-47.5)/radius,d=x*x+y*y;if(d>1)continue;
    const z=Math.sqrt(1-d),n=stxGVNoise(x,y,z,seed),fine=Math.sin(x*90+y*73+z*82+seed)*.025;
    let rgb,emission=0;
    if(family==='Terran'||family==='Jungle'){
      rgb=n>(family==='Jungle'?-.25:.06)?stxGVMix(palette[1],palette[2],clamp(n*.6,0,1)):
        stxGVMix(palette[0],palette[1],clamp((n+.2)*.4,0,.22));
      if(Math.abs(y)>.84+n*.055)rgb=stxGVMix(rgb,[220,241,239],.85);
    }else if(family==='Ocean'){
      rgb=stxGVMix(palette[0],palette[1],clamp((n+.65)*.7,0,1));
      if(n>.72)rgb=palette[2];
    }else if(family==='Gas giant'){
      const band=Math.sin(y*28+Math.sin(x*4+seed)*.7+Math.sin(z*7)*.3);
      rgb=stxGVMix(palette[0],palette[1],band*.5+.5);
      if(Math.sin(y*55+Math.sin(x*6)*.8)>.6)rgb=stxGVMix(rgb,palette[2],.5);
      const storm=((x-.35)/.29)**2+((y-.24)/.12)**2;
      if(storm<1.2)rgb=stxGVMix(rgb,palette[0],.5+.2*Math.sin(storm*12));
    }else if(family==='Volcanic'){
      rgb=stxGVMix(palette[0],palette[1],clamp(n*.7+.35,0,1));
      if(Math.abs(n+Math.sin(y*16+x*8)*.12)<.027){rgb=palette[2];emission=.8}
    }else if(family==='Ice'){
      rgb=stxGVMix(palette[1],palette[2],clamp(n+.3,0,1));
      if(Math.abs(n)<.035)rgb=palette[0];
    }else if(family==='Industrial'){
      rgb=stxGVMix(palette[0],palette[1],clamp(n*.7+.35,0,1));
      if(Math.abs(Math.sin(x*44+seed)*Math.sin(y*39))<.035){rgb=palette[2];emission=.3}
    }else if(family==='Barren'){
      rgb=stxGVMix(palette[0],palette[1],clamp(n*.7+.5,0,1));
      for(let k=0;k<5;k++){
        const cx=Math.sin(craterSeed+k*5.7)*.7,cy=Math.cos(craterSeed+k*3.8)*.65;
        const r=.07+(k%3)*.04,dd=Math.hypot(x-cx,y-cy);
        if(dd<r)rgb=stxGVMix(rgb,palette[0],.55);
        else if(dd<r+.018)rgb=stxGVMix(rgb,palette[2],.6);
      }
    }else if(family==='Exotic'){
      rgb=stxGVMix(palette[0],palette[1],clamp(n+.5,0,1));
      if(Math.sin(y*24+x*11+n*5)>.92){rgb=palette[2];emission=.48}
    }else{
      rgb=stxGVMix(palette[0],palette[1],clamp(n*.7+.5,0,1));
      const bands=Math.sin(y*(family==='Desert'?50:21)+n*8+x*9);
      if(bands>.65)rgb=stxGVMix(rgb,palette[2],family==='Desert'?.28:.55);
    }
    // Cloud cover is part of the cached lit sphere. A separate thin cloud arc
    // moves slowly at close LOD, so we never regenerate textures to animate.
    if(['Terran','Ocean','Jungle','Toxic'].includes(family)){
      const cloud=stxGVNoise(x+.3,y*.85,z,seed+67);
      if(cloud>.45)rgb=stxGVMix(rgb,family==='Toxic'?palette[2]:[220,242,247],clamp((cloud-.45)*2.2,0,.8));
    }
    const light=clamp(-x*.52-y*.43+z*.64,.07,1)*.92+.08;
    const shade=Math.max(light,emission),at=(py*resolution+px)*4;
    for(let k=0;k<3;k++)pixels.data[at+k]=clamp(rgb[k]*(shade+fine),0,255);
    pixels.data[at+3]=Math.min(255,(1-Math.sqrt(d))*radius*255);
  }
  c.putImageData(pixels,0,0);return texture;
}
function stxGVSpriteKey(p){return `${p.id}:${p.name}:${p.x}:${p.y}:${stxGVFamily(p)}`}
function stxGVPlanetSprite(p){
  const key=stxGVSpriteKey(p);
  if(stxGV.sprites.has(key)){
    const sprite=stxGV.sprites.get(key);stxGV.sprites.delete(key);stxGV.sprites.set(key,sprite);return sprite;
  }
  if(stxGV.pinnedSprites&&!stxGV.pinnedSprites.has(key))return null;
  if(!stxGVSpend('spriteBuilds'))return null;
  if(stxGV.sprites.size>=STX_GV_LIMITS.sprites){
    const victim=[...stxGV.sprites.keys()].find(k=>!stxGV.pinnedSprites?.has(k));
    if(victim)stxGV.sprites.delete(victim);else return null;
  }
  const sprite=stxGVBuildPlanet(p);stxGV.sprites.set(key,sprite);
  while(stxGV.sprites.size>STX_GV_LIMITS.sprites)stxGV.sprites.delete(stxGV.sprites.keys().next().value);
  return sprite;
}
const STX_GV_HULLS = {
  fighter:[[1.5,0],[-1,-.8],[-.4,0],[-1,.8]],
  corvette:[[1.8,0],[-.8,-.65],[-1.2,-.4],[-1.2,.4],[-.8,.65]],
  frigate:[[2,0],[.2,-.55],[-.4,-1],[-1.2,-1],[-.9,0],[-1.2,1],[-.4,1],[.2,.55]],
  destroyer:[[2.3,0],[1,-.42],[-.8,-.45],[-1.3,-1],[-1.4,1],[-.8,.45],[1,.42]],
  cruiser:[[2.5,0],[1.1,-.8],[-.9,-.8],[-1.5,-.4],[-1.5,.4],[-.9,.8],[1.1,.8]],
  battleship:[[2.7,0],[1.5,-.65],[.4,-.65],[.1,-1.3],[-1.6,-1.3],[-1.3,0],[-1.6,1.3],[.1,1.3],[.4,.65],[1.5,.65]],
  carrier:[[2.2,-.65],[1.6,-1.1],[-1.8,-1.1],[-1.8,1.1],[1.6,1.1],[2.2,.65],[.3,.5],[.3,-.5]],
  dreadnought:[[3,0],[1.8,-.6],[1.4,-1.35],[-1.7,-1.35],[-2,-.65],[-1.4,0],[-2,.65],[-1.7,1.35],[1.4,1.35],[1.8,.6]],
  transport:[[1.6,0],[.9,-.8],[-1.2,-.8],[-1.2,.8],[.9,.8]],
  freighter:[[1.8,0],[1.2,-.5],[.7,-.5],[.7,-1],[-1.6,-1],[-1.6,1],[.7,1],[.7,.5],[1.2,.5]],
  tanker:[[1.9,0],[1,-.8],[-1.3,-.8],[-1.8,0],[-1.3,.8],[1,.8]]
};
function stxGVPolygon(c,points,close=true){
  c.beginPath();points.forEach(([x,y],i)=>i?c.lineTo(x,y):c.moveTo(x,y));if(close)c.closePath();
}
function stxGVShip(x,y,angle,size,color,hull='corvette',trail=0){
  ctx.save();ctx.translate(x,y);ctx.rotate(angle);ctx.scale(size,size);
  if(trail>0){
    ctx.strokeStyle=color;ctx.globalAlpha=.35;ctx.lineWidth=.7;
    ctx.beginPath();ctx.moveTo(-1,0);ctx.lineTo(-2.2-trail,0);ctx.stroke();
    ctx.strokeStyle='#e3fbff';ctx.globalAlpha=.85;ctx.lineWidth=.4;
    ctx.beginPath();ctx.moveTo(-1.2,-.35);ctx.lineTo(-2.4,-.35);ctx.moveTo(-1.2,.35);ctx.lineTo(-2.4,.35);ctx.stroke();
  }
  ctx.globalAlpha=1;ctx.fillStyle='#17283e';ctx.strokeStyle=color;ctx.lineWidth=.45;
  stxGVPolygon(ctx,STX_GV_HULLS[hull]||STX_GV_HULLS.corvette);ctx.fill();ctx.stroke();
  ctx.fillStyle='#d6eaff';ctx.fillRect(.2,-.18,.6,.36);
  if(['battleship','carrier','dreadnought'].includes(hull)){
    ctx.strokeStyle='#7191b3';ctx.lineWidth=.22;
    ctx.beginPath();ctx.moveTo(-1.2,-.7);ctx.lineTo(1.1,-.7);ctx.moveTo(-1.2,.7);ctx.lineTo(1.1,.7);ctx.stroke();
    ctx.fillStyle=color;ctx.fillRect(-.5,-1,.2,.3);ctx.fillRect(-.5,.7,.2,.3);
  }
  if(hull==='freighter'){ctx.strokeStyle='#efc572';ctx.lineWidth=.23;for(let i=0;i<3;i++)ctx.strokeRect(-1.4+i*.65,-.75,.48,1.5)}
  ctx.restore();
}
// One canvas symbol vocabulary for map roles, warnings, events and the legend.
function stxGVIcon(kind,x,y,size,color,paint=ctx){
  paint.save();paint.translate(x,y);paint.scale(size/8,size/8);paint.strokeStyle=color;
  paint.fillStyle='#091324';paint.lineWidth=1.25;paint.lineJoin='round';paint.lineCap='round';
  const line=points=>{stxGVPolygon(paint,points,false);paint.stroke()};
  if(kind==='capital'){
    const points=Array.from({length:10},(_,i)=>{const a=i*Math.PI/5-Math.PI/2,r=i%2?3.2:7;return [Math.cos(a)*r,Math.sin(a)*r]});
    stxGVPolygon(paint,points);paint.fillStyle=color;paint.fill();
  }else if(['battle','invasion','war','fleet-destroyed'].includes(kind)){
    line([[-6,6],[5,-5],[5,-1],[1,-5],[5,-5]]);line([[6,6],[-5,-5],[-5,-1],[-1,-5],[-5,-5]]);
    line([[-6,2],[-2,6]]);line([[6,2],[2,6]]);
    if(kind==='invasion'){paint.beginPath();paint.ellipse(0,4,9,3,0,0,6.283);paint.stroke()}
  }else if(kind==='fortress'){
    stxGVPolygon(paint,[[0,-7],[6,-4],[5,3],[0,7],[-5,3],[-6,-4]]);paint.fill();paint.stroke();line([[0,-3],[0,3]]);
  }else if(['embargo','interruption','blockade'].includes(kind)){
    paint.beginPath();paint.arc(0,0,6,0,6.283);paint.fill();paint.stroke();line([[-4,4],[4,-4]]);
    if(kind==='blockade')line([[-3,-8],[3,-8]]);
  }else if(kind==='warning'||kind==='station-destroyed'){
    stxGVPolygon(paint,[[0,-7],[7,6],[-7,6]]);paint.fill();paint.stroke();line([[0,-2],[0,1]]);line([[0,3],[0,3.4]]);
  }else if(kind==='mining'){
    line([[-5,6],[4,-5]]);line([[-6,-3],[-2,-6],[3,-5],[6,-1]]);
  }else if(kind==='industry'){
    line([[-6,6],[-6,-1],[-2,-4],[-2,0],[3,-3],[3,6],[-6,6]]);line([[4,6],[4,-6],[6,-6],[6,6]]);
  }else if(kind==='components'){
    stxGVPolygon(paint,[[0,-7],[6,-3],[6,3],[0,7],[-6,3],[-6,-3]]);paint.fill();paint.stroke();
    line([[-3,-2],[0,-4],[3,-2],[3,2],[0,4],[-3,2],[-3,-2]]);
  }else if(kind==='shipyard'||kind==='reinforcement'||kind==='departure'){
    line([[-6,3],[0,-6],[6,3]]);line([[0,-5],[0,7]]);
    if(kind==='shipyard')line([[-7,-4],[-7,7],[7,7],[7,-4]]);
  }else if(kind==='logistics'){
    stxGVPolygon(paint,[[-6,-3],[0,-6],[6,-3],[6,4],[0,7],[-6,4]]);paint.fill();paint.stroke();line([[-6,-3],[0,0],[6,-3]]);line([[0,0],[0,7]]);
  }else if(kind==='trade'){
    line([[-7,-3],[6,-3],[3,-6]]);line([[7,3],[-6,3],[-3,6]]);
  }else if(kind==='diplomacy'){
    line([[-7,0],[-3,-4],[1,0],[-3,4],[-7,0]]);line([[7,0],[3,-4],[-1,0],[3,4],[7,0]]);
  }else if(kind==='research'){
    for(let i=0;i<3;i++){paint.beginPath();paint.ellipse(0,0,7,2.5,i*Math.PI/3,0,6.283);paint.stroke()}
  }else if(kind==='population'||kind==='colony'){
    paint.beginPath();paint.arc(0,-3,2.5,0,6.283);paint.stroke();line([[-5,6],[-4,2],[4,2],[5,6]]);
  }else{paint.beginPath();paint.arc(0,0,5,0,6.283);paint.stroke()}
  paint.restore();
}
function stxGVStation(x,y,kind,tier,color,status='operational',selected=false){
  const z=state.camera.zoom,size=clamp((5+tier*1.5)*z,4,17);
  ctx.save();ctx.translate(x,y);ctx.strokeStyle=status==='wreck'?'#bf716d':color;
  ctx.fillStyle='#0c192d';ctx.lineWidth=selected?1.8:1.1;
  if(status==='wreck'){
    stxGVPolygon(ctx,[[-size,-size],[0,-2],[size,0],[size*.5,size]],false);ctx.stroke();
    ctx.fillStyle='#bd846b';ctx.fillRect(-size,4,3,2);ctx.restore();return size;
  }
  if(status==='construction'){ctx.setLineDash([3,4]);ctx.globalAlpha=.6}
  const citadel=tier>=3&&['military','station'].includes(kind);
  if(citadel){
    stxGVPolygon(ctx,Array.from({length:8},(_,i)=>[Math.cos(i*Math.PI/4)*size*1.3,Math.sin(i*Math.PI/4)*size*1.3]));ctx.fill();ctx.stroke();
  }else if(kind==='military'){
    stxGVPolygon(ctx,[[0,-size*1.4],[size,-size*.2],[size*.7,size],[-size*.7,size],[-size,-size*.2]]);ctx.fill();ctx.stroke();
  }else if(kind==='trade'){
    ctx.strokeRect(-size*.7,-size*.6,size*1.4,size*1.2);
    for(let i=-1;i<=1;i++){ctx.fillRect(i*size*.8-2,-size-3,4,5);ctx.strokeRect(i*size*.8-2,-size-3,4,5);ctx.strokeRect(i*size*.8-2,size-2,4,5)}
  }else if(kind==='logistics'||kind==='shipyard'){
    for(let i=0;i<4;i++){const a=i*Math.PI/2;ctx.save();ctx.rotate(a);ctx.strokeRect(0,-size*.18,size*1.5,size*.36);ctx.strokeRect(size,-size*.6,size*.35,size*1.2);ctx.restore()}
  }else{
    ctx.beginPath();ctx.arc(0,0,size,0,6.283);ctx.fill();ctx.stroke();
    ctx.beginPath();ctx.ellipse(0,0,size*1.45,size*.4,-.35,0,6.283);ctx.stroke();
  }
  ctx.setLineDash([]);
  if(kind==='military'||citadel){
    for(let i=0;i<(citadel?6:3);i++){const a=i*6.283/(citadel?6:3);ctx.beginPath();ctx.moveTo(Math.cos(a)*size*.9,Math.sin(a)*size*.9);ctx.lineTo(Math.cos(a)*size*1.7,Math.sin(a)*size*1.7);ctx.stroke()}
  }
  if(tier>=2){ctx.beginPath();ctx.arc(0,0,size*.65,0,6.283);ctx.stroke()}
  ctx.fillStyle=color;ctx.fillRect(-2,-2,4,4);
  if(z>.85){ctx.fillStyle='#defaff';for(let i=0;i<4;i++)ctx.fillRect(Math.cos(i*1.57)*size-1,Math.sin(i*1.57)*size-1,2,2)}
  ctx.restore();return size;
}
