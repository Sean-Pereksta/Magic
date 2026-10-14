/* Bounded visual-only explosions. Combat damage remains in neon-arena.js. */
(function(root){
  'use strict';
  const clamp=(v,a,b)=>Math.max(a,Math.min(b,v)),TAU=Math.PI*2;
  const PROFILES=Object.freeze({
    rocket:{life:1.15,fire:.36,scale:.36,core:[1,.94,.65],fireColor:[1,.32,.045],smoke:[.14,.12,.11],ring:[1,.63,.22]},
    mortar:{life:1.4,fire:.43,scale:.40,core:[1,.98,.8],fireColor:[1,.40,.065],smoke:[.13,.12,.105],ring:[1,.76,.38]},
    plasma:{life:.9,fire:.42,scale:.30,core:[.94,.86,1],fireColor:[.62,.15,1],smoke:[.19,.10,.27],ring:[.76,.40,1]},
    seismic:{life:.75,fire:.19,scale:.22,core:[1,.86,.96],fireColor:[1,.28,.64],smoke:[.2,.14,.2],ring:[1,.45,.82]}
  });
  const PHI=(1+Math.sqrt(5))/2,points=[[-1,PHI,0],[1,PHI,0],[-1,-PHI,0],[1,-PHI,0],[0,-1,PHI],[0,1,PHI],[0,-1,-PHI],[0,1,-PHI],[PHI,0,-1],[PHI,0,1],[-PHI,0,-1],[-PHI,0,1]].map(v=>{const n=Math.hypot(...v);return v.map(x=>x/n)});
  const faces=[[0,11,5],[0,5,1],[0,1,7],[0,7,10],[0,10,11],[1,5,9],[5,11,4],[11,10,2],[10,7,6],[7,1,8],[3,9,4],[3,4,2],[3,2,6],[3,6,8],[3,8,9],[4,9,5],[2,4,11],[6,2,10],[8,6,7],[9,8,1]];
  function createSystem(){
    const active=[],pool=[];let serial=0;
    function emit(kind,x,y,z,options={}){
      if(![x,y,z].every(Number.isFinite))return null;
      const p=PROFILES[kind]||PROFILES.rocket,fx=pool.pop()||{},radius=clamp(Number(options.radius)||7,.4,20);
      Object.assign(fx,{kind:PROFILES[kind]?kind:'rocket',x,y,z,age:0,radius,profile:p,seed:++serial,bounds:options.bounds||null});
      active.push(fx);while(active.length>16)pool.push(active.shift());return fx;
    }
    function update(dt){if(!Number.isFinite(dt)||dt<0)return;for(let i=active.length-1;i>=0;i--){active[i].age+=dt;if(active[i].age>=active[i].profile.life)pool.push(...active.splice(i,1))}}
    function clear(){pool.push(...active.splice(0));}
    return {emit,update,clear,active,stats:()=>({active:active.length,pooled:pool.length,total:serial})};
  }
  function createGeometry(system,camera,quality='high',target=new Float32Array(18000*7)){
    let at=0,fxNow=null;const low=quality==='low',budget=low?6:quality==='medium'?8:10,right=camera.right,up=camera.up;
    const vertex=(x,y,z,c,a)=>{if(at+7>target.length)return;const b=fxNow?.bounds;if(b){x=clamp(x,b.minX,b.maxX);y=clamp(y,b.minY,b.maxY);z=clamp(z,b.minZ,b.maxZ)}target[at++]=x;target[at++]=y;target[at++]=z;target[at++]=c[0];target[at++]=c[1];target[at++]=c[2];target[at++]=clamp(a,0,1)};
    const triangle=(a,b,c,color,alpha)=>{vertex(...a,color,alpha);vertex(...b,color,alpha);vertex(...c,color,alpha)};
    const sphere=(x,y,z,r,color,alpha,stretch=1)=>{for(const f of faces){const shade=.63+.37*Math.max(0,points[f[0]][1]*.7-points[f[0]][0]*.3+.3),c=color.map(v=>v*shade),p=f.map(i=>[x+points[i][0]*r,y+points[i][1]*r*stretch,z+points[i][2]*r]);triangle(...p,c,alpha)}};
    const billboard=(x,y,z,r,color,alpha)=>{const segments=low?8:12;for(let i=0;i<segments;i++){vertex(x,y,z,color,alpha);for(const a of [i*TAU/segments,(i+1)*TAU/segments])vertex(x+(right[0]*Math.cos(a)+up[0]*Math.sin(a))*r,y+(right[1]*Math.cos(a)+up[1]*Math.sin(a))*r,z+(right[2]*Math.cos(a)+up[2]*Math.sin(a))*r,color,0)}};
    // Smoke first, then opaque-looking flame lobes and sharp fragments.
    const selected=system.active.slice(-budget).sort((a,b)=>Math.hypot(b.x-camera.position[0],b.z-camera.position[2])-Math.hypot(a.x-camera.position[0],a.z-camera.position[2]));
    for(const fx of selected){fxNow=fx;const p=fx.profile,t=fx.age,n=t/p.life,r=fx.radius*p.scale,angle=fx.seed*2.39996;
      if(t>.09)for(let i=0;i<(low?3:6);i++){const a=angle+i*TAU/6,k=(t-.09)/(p.life-.09),drift=r*(.12+k*.7),x=fx.x+Math.cos(a)*drift,y=fx.y+r*(.3+k*1.1)+Math.sin(i*2)*r*.12,z=fx.z+Math.sin(a)*drift;billboard(x,y,z,r*(.15+k*.32),p.smoke,(1-k)*.24)}
      if(t<p.fire){const k=t/p.fire,expand=Math.sin(Math.min(1,k*1.8)*Math.PI/2),fade=1-k;
        billboard(fx.x,fx.y+r*.18,fx.z,r*(.6+.55*expand),p.fireColor,fade*.16);
        for(let i=0;i<(low?4:7);i++){const a=angle+i*TAU/7,wobble=.78+.22*Math.sin(fx.seed+i*3),distance=i===0?0:r*.53*expand,heat=.12+(i%3)*.14,color=p.fireColor.map((v,j)=>v*(1-heat)+p.core[j]*heat);sphere(fx.x+Math.cos(a)*distance,fx.y+r*(.18+(i%3)*.15)*expand,fx.z+Math.sin(a)*distance,r*(.16+.34*expand)*wobble,color,fade*.88,1.05+k*.65)}
        // A bright center persists through the burst, with small hot pockets breaking up its surface.
        sphere(fx.x,fx.y+r*.19,fx.z,r*(.12+.23*expand),p.core,fade*.94,1.25);
        for(let i=0;i<(low?2:4);i++){const a=angle+i*2.39996;sphere(fx.x+Math.cos(a)*r*.32*expand,fx.y+r*(.13+i*.11)*expand,fx.z+Math.sin(a)*r*.32*expand,r*(.08+.10*expand),p.core,fade*.75,1.5)}
      }
      if(t<.38){const k=t/.38,outer=r*(.3+k*2),inner=outer-Math.max(.035,r*.09*(1-k)),segments=low?16:28,c=p.ring;
        for(let i=0;i<segments;i++){const a=i*TAU/segments,b=(i+1)*TAU/segments,y=fx.y+.035;const p0=[fx.x+Math.cos(a)*inner,y,fx.z+Math.sin(a)*inner],p1=[fx.x+Math.cos(a)*outer,y,fx.z+Math.sin(a)*outer],p2=[fx.x+Math.cos(b)*outer,y,fx.z+Math.sin(b)*outer],p3=[fx.x+Math.cos(b)*inner,y,fx.z+Math.sin(b)*inner];triangle(p0,p1,p2,c,(1-k)*.6);triangle(p0,p2,p3,c,(1-k)*.6)}
      }
      if(t<.65)for(let i=0;i<(low?4:10);i++){const a=angle+i*2.39996,speed=r*(1.5+(i%3)*.6),x=fx.x+Math.cos(a)*speed*t,y=fx.y+(.9+i%4*.42)*r*t-6*t*t,z=fx.z+Math.sin(a)*speed*t,size=Math.max(.02,r*.027*(1-n));sphere(x,y,z,size,i%3?p.ring:p.core,(1-t/.65)*.9,2.2)}
    }
    return target.subarray(0,at);
  }
  function createRenderer(gl,system){
    const compile=(type,source)=>{const s=gl.createShader(type);gl.shaderSource(s,source);gl.compileShader(s);if(!gl.getShaderParameter(s,gl.COMPILE_STATUS))throw Error(gl.getShaderInfoLog(s));return s};
    const vs=compile(gl.VERTEX_SHADER,'attribute vec3 aPosition;attribute vec4 aColor;uniform vec3 uCamera,uRight,uUp,uForward;uniform float uAspect;varying vec4 vColor;void main(){vec3 d=aPosition-uCamera;vec3 c=vec3(dot(d,uRight),dot(d,uUp),dot(d,uForward));float n=.055,f=180.;gl_Position=vec4(c.x*1.44*uAspect,c.y*1.44,(f+n)/(f-n)*c.z-2.*f*n/(f-n),c.z);vColor=aColor;}');
    const fs=compile(gl.FRAGMENT_SHADER,'precision mediump float;varying vec4 vColor;void main(){gl_FragColor=vColor;}'),program=gl.createProgram();gl.attachShader(program,vs);gl.attachShader(program,fs);gl.linkProgram(program);if(!gl.getProgramParameter(program,gl.LINK_STATUS))throw Error(gl.getProgramInfoLog(program));gl.deleteShader(vs);gl.deleteShader(fs);
    const buffer=gl.createBuffer(),scratch=new Float32Array(18000*7),pos=gl.getAttribLocation(program,'aPosition'),col=gl.getAttribLocation(program,'aColor'),u=Object.fromEntries(['uCamera','uRight','uUp','uForward','uAspect'].map(k=>[k,gl.getUniformLocation(program,k)]));let draws=0,vertices=0;
    function render(camera,quality){const data=createGeometry(system,camera,quality,scratch);draws=0;vertices=data.length/7;if(!data.length)return;
      gl.useProgram(program);gl.enable(gl.DEPTH_TEST);gl.depthFunc(gl.LEQUAL);gl.depthMask(false);gl.enable(gl.BLEND);gl.blendFunc(gl.SRC_ALPHA,gl.ONE_MINUS_SRC_ALPHA);gl.disable(gl.CULL_FACE);
      for(const [key,value]of [['uCamera',camera.position],['uRight',camera.right],['uUp',camera.up],['uForward',camera.forward]])gl.uniform3fv(u[key],value);gl.uniform1f(u.uAspect,camera.aspect);
      gl.bindBuffer(gl.ARRAY_BUFFER,buffer);gl.bufferData(gl.ARRAY_BUFFER,data,gl.DYNAMIC_DRAW);gl.enableVertexAttribArray(pos);gl.vertexAttribPointer(pos,3,gl.FLOAT,false,28,0);gl.enableVertexAttribArray(col);gl.vertexAttribPointer(col,4,gl.FLOAT,false,28,12);gl.drawArrays(gl.TRIANGLES,0,vertices);draws=1;gl.depthMask(true);gl.disable(gl.BLEND);
    }
    return {render,stats:()=>({draws,vertices}),dispose(){gl.deleteBuffer(buffer);gl.deleteProgram(program)}};
  }
  const api={PROFILES,createSystem,createGeometry,createRenderer};root.NeonExplosionFX=api;if(typeof module==='object')module.exports=api;
})(typeof globalThis==='object'?globalThis:this);
