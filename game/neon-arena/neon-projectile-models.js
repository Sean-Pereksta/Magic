/* Local, authored rigid projectile meshes. No downloads, sprites or billboards.
 * +Z is the striking direction; the harpoon tip is the authoritative hit point.
 * Rendering reads simulation records and never advances or damages them. */
(function(root){
  'use strict';
  const TAU=Math.PI*2,STRIDE=12,MAX_LINKS=64;
  const add=(a,b)=>a.map((v,i)=>v+b[i]),sub=(a,b)=>a.map((v,i)=>v-b[i]),mul=(a,s)=>a.map(v=>v*s);
  const dot=(a,b)=>a.reduce((s,v,i)=>s+v*b[i],0),cross=(a,b)=>[a[1]*b[2]-a[2]*b[1],a[2]*b[0]-a[0]*b[2],a[0]*b[1]-a[1]*b[0]];
  const unit=a=>{const d=Math.hypot(...a);return d>1e-8?mul(a,1/d):[0,0,1];};
  const MATERIAL={steel:{c:[.48,.55,.62],e:[0,0,0]},edge:{c:[.79,.86,.91],e:[0,0,0]},palm:{c:[.32,.13,.28],e:[0,0,0]},finger:{c:[.64,.25,.46],e:[0,0,0]},joint:{c:[.055,.065,.085],e:[0,0,0]},accent:{c:[.7,.48,.66],e:[.7,.7,.7]}};
  function builder(){
    const data=[],parts=[];
    function triangle(a,b,c,material,normals){
      const normal=cross(sub(b,a),sub(c,a));if(Math.hypot(...normal)<1e-9)return;
      const flat=unit(normal),m=MATERIAL[material]||MATERIAL.steel;
      for(const [i,p] of [a,b,c].entries())data.push(...p,...(normals?.[i]||flat),...m.c,...m.e);
    }
    const quad=(a,b,c,d,material,normals)=>{triangle(a,b,c,material,normals&&[normals[0],normals[1],normals[2]]);triangle(a,c,d,material,normals&&[normals[0],normals[2],normals[3]]);};
    function part(name,author){const start=data.length/STRIDE;author();parts.push({name,start,count:data.length/STRIDE-start});}
    function loft(rings,material,cap=true){
      for(let r=0;r<rings.length-1;r++)for(let i=0;i<rings[r].length;i++){const j=(i+1)%rings[r].length;quad(rings[r][i],rings[r][j],rings[r+1][j],rings[r+1][i],material);}
      if(cap)for(const [ring,reverse] of [[rings[0],true],[rings.at(-1),false]]){const center=mul(ring.reduce((sum,p)=>add(sum,p),[0,0,0]),1/ring.length);for(let i=0;i<ring.length;i++)reverse?triangle(center,ring[(i+1)%ring.length],ring[i],material):triangle(center,ring[i],ring[(i+1)%ring.length],material);}
    }
    function sweep(points,width,height,material,sides=10){
      const rings=[],normals=[];
      points.forEach((p,i)=>{
        const tangent=unit(sub(points[Math.min(points.length-1,i+1)],points[Math.max(0,i-1)])),right=unit(sub([1,0,0],mul(tangent,tangent[0]))),up=unit(cross(tangent,right)),ring=[],ns=[];
        for(let j=0;j<sides;j++){const a=j/sides*TAU,radial=add(mul(right,Math.cos(a)),mul(up,Math.sin(a)));ring.push(add(p,add(mul(right,Math.cos(a)*width),mul(up,Math.sin(a)*height))));ns.push(unit(radial));}
        rings.push(ring);normals.push(ns);
      });
      for(let r=0;r<rings.length-1;r++)for(let i=0;i<sides;i++){const j=(i+1)%sides;quad(rings[r][i],rings[r][j],rings[r+1][j],rings[r+1][i],material,[normals[r][i],normals[r][j],normals[r+1][j],normals[r+1][i]]);}
      for(const [r,reverse] of [[0,true],[rings.length-1,false]])for(let i=0;i<sides;i++)reverse?triangle(points[r],rings[r][(i+1)%sides],rings[r][i],material):triangle(points[r],rings[r][i],rings[r][(i+1)%sides],material);
    }
    function prismXZ(polygon,thickness,material){
      const bottom=polygon.map(([x,z])=>[x,-thickness/2,z]),top=polygon.map(([x,z])=>[x,thickness/2,z]);
      for(let i=1;i<polygon.length-1;i++){triangle(top[0],top[i+1],top[i],material);triangle(bottom[0],bottom[i],bottom[i+1],material);}
      for(let i=0;i<polygon.length;i++){const j=(i+1)%polygon.length;quad(bottom[i],bottom[j],top[j],top[i],material);}
    }
    return {triangle,quad,part,loft,sweep,prismXZ,finish:()=>({vertices:new Float32Array(data),parts,vertexCount:data.length/STRIDE})};
  }
  const roundRing=(z,rx,ry,sides=12)=>Array.from({length:sides},(_,i)=>[Math.cos(i/sides*TAU)*rx,Math.sin(i/sides*TAU)*ry,z]);
  const chamferRing=(z,w,h)=>[[-w*.72,-h],[-w,-h*.62],[-w,h*.62],[-w*.72,h],[w*.72,h],[w,h*.62],[w,-h*.62],[w*.72,-h]].reverse().map(([x,y])=>[x,y,z]);
  function fistMesh(){
    const b=builder();
    // Bevelled metacarpal shell narrows into a real wrist rather than a cube.
    b.part('palm_shell',()=>b.loft([chamferRing(-.39,.31,.25),chamferRing(-.15,.51,.30),chamferRing(.18,.52,.29),chamferRing(.30,.46,.25)],'palm'));
    b.part('wrist',()=>b.loft([roundRing(-.75,.27,.22),roundRing(-.44,.29,.24),roundRing(-.30,.34,.26)],'joint'));
    b.part('cuff_steel',()=>b.loft([chamferRing(-.73,.36,.29),chamferRing(-.65,.39,.31),chamferRing(-.43,.37,.30)],'steel'));
    b.part('cuff_power_band',()=>b.loft([chamferRing(-.64,.395,.315),chamferRing(-.59,.395,.315)],'accent'));
    // Four distinct curled fingers form raised knuckles and tucked lower folds.
    for(let i=0;i<4;i++){
      const x=(i-1.5)*.265,small=i===0||i===3?.93:1;
      b.part('curled_finger_'+(i+1),()=>b.sweep([[x,.20,.10],[x,.29,.31],[x,.17,.49],[x,-.08,.46],[x,-.19,.29],[x,-.12,.13]],.119*small,.137*small,'finger',10));
      b.part('knuckle_plate_'+(i+1),()=>b.sweep([[x,.29,.24],[x,.32,.34],[x,.28,.40]],.096,.045,'edge',8));
      b.part('knuckle_seam_'+(i+1),()=>b.sweep([[x,-.13,.46],[x,-.19,.35]],.083,.025,'joint',6));
    }
    // The wrapped thumb crosses the curled fingertips; its asymmetric silhouette
    // remains recognisable from the side and as the fist retracts toward its gun.
    b.part('wrapped_thumb',()=>b.sweep([[.49,.02,-.06],[.61,-.16,.12],[.51,-.28,.31],[.30,-.26,.40],[.22,-.19,.38]],.145,.125,'finger',12));
    b.part('thumb_armor',()=>b.sweep([[.61,-.13,.11],[.54,-.23,.25]],.125,.048,'edge',8));
    b.part('backhand_power_inlay',()=>b.loft([chamferRing(-.19,.38,.307),chamferRing(-.14,.38,.31)],'accent'));
    return b.finish();
  }
  function harpoonMesh(){
    const b=builder(),diamond=(z,w,h)=>[[0,-h,z],[w,0,z],[0,h,z],[-w,0,z]];
    b.part('spearhead',()=>b.loft([diamond(-.43,.075,.055),diamond(-.29,.215,.085),diamond(-.04,.035,.018),diamond(0,.002,.002)],'edge'));
    for(const sign of [-1,1])b.part('backward_barb_'+sign,()=>b.prismXZ([[.055*sign,-.30],[.30*sign,-.65],[.16*sign,-.58],[.06*sign,-.45]],.075,'steel'));
    b.part('shaft',()=>b.loft([roundRing(-1.49,.046,.046,8),roundRing(-.55,.046,.046,8),roundRing(-.40,.068,.068,8)],'steel'));
    for(const z of [-1.28,-.62])b.part('shaft_collar_'+z,()=>b.loft([roundRing(z-.045,.075,.075,8),roundRing(z+.045,.075,.075,8)],'joint'));
    b.part('anchor_power_ring',()=>b.loft([roundRing(-.57,.084,.084,10),roundRing(-.51,.084,.084,10)],'accent'));
    for(const sign of [-1,1])b.part('tail_fin_'+sign,()=>b.prismXZ([[.04*sign,-1.02],[.13*sign,-1.40],[.04*sign,-1.32]],.025,'steel'));
    return b.finish();
  }
  function wireMesh(){const b=builder();b.loft([roundRing(0,.005,.005,6),roundRing(1,.005,.005,6)],'steel');return b.finish();}
  function linkMesh(){
    const b=builder(),rings=[],normals=[],around=10,tube=4;
    for(let i=0;i<around;i++){const a=i/around*TAU,radial=unit([Math.cos(a)/.044,0,Math.sin(a)/.14]),center=[Math.cos(a)*.044,0,Math.sin(a)*.14];rings[i]=[];normals[i]=[];for(let j=0;j<tube;j++){const t=j/tube*TAU,n=add(mul(radial,Math.cos(t)),[0,Math.sin(t),0]);rings[i].push(add(center,mul(n,.011)));normals[i].push(n);}}
    for(let i=0;i<around;i++)for(let j=0;j<tube;j++){const i2=(i+1)%around,j2=(j+1)%tube;b.quad(rings[i][j],rings[i2][j],rings[i2][j2],rings[i][j2],'steel',[normals[i][j],normals[i2][j],normals[i2][j2],normals[i][j2]]);}
    return b.finish();
  }
  const MESHES=Object.freeze({fist:fistMesh(),harpoon:harpoonMesh(),wire:wireMesh(),link:linkMesh()});
  function directionMatrix(position,direction,scale=1){
    const forward=unit(direction),right=unit(cross(Math.abs(forward[1])>.96?[0,0,1]:[0,1,0],forward)),up=unit(cross(forward,right));
    return new Float32Array([right[0]*scale,right[1]*scale,right[2]*scale,0,up[0]*scale,up[1]*scale,up[2]*scale,0,forward[0]*scale,forward[1]*scale,forward[2]*scale,0,...position,1]);
  }
  const transform=(matrix,p)=>[matrix[0]*p[0]+matrix[4]*p[1]+matrix[8]*p[2]+matrix[12],matrix[1]*p[0]+matrix[5]*p[1]+matrix[9]*p[2]+matrix[13],matrix[2]*p[0]+matrix[6]*p[1]+matrix[10]*p[2]+matrix[14]];
  function projectileMatrix(p,muzzle){
    const direction=p.kind==='punch'&&p.state==='back'&&muzzle?[muzzle.x-p.x,muzzle.y-p.y,muzzle.z-p.z]:[p.dx??p.vx??0,p.dy??p.vy??0,p.dz??p.vz??1];
    return directionMatrix([p.x,p.y,p.z],direction,p.ultimate?(p.kind==='punch'?1.24:1.16):1);
  }
  const rgb=color=>{const hex=String(color||'#bdfcff').replace('#','');return hex.length===6?[0,2,4].map(i=>parseInt(hex.slice(i,i+2),16)/255):[.74,.99,1];};
  function createRenderer(gl){
    const compile=(type,source)=>{const shader=gl.createShader(type);gl.shaderSource(shader,source);gl.compileShader(shader);if(!gl.getShaderParameter(shader,gl.COMPILE_STATUS))throw Error(gl.getShaderInfoLog(shader));return shader;};
    const program=gl.createProgram(),vs=compile(gl.VERTEX_SHADER,'attribute vec3 aPosition;attribute vec3 aNormal;attribute vec3 aColor;attribute vec3 aEmissive;uniform mat4 uModel;uniform vec3 uCamera;uniform vec3 uRight;uniform vec3 uUp;uniform vec3 uForward;uniform float uAspect;varying vec3 vColor;varying vec3 vNormal;varying vec3 vEmissive;varying vec3 vView;void main(){vec3 world=(uModel*vec4(aPosition,1.0)).xyz;vec3 delta=world-uCamera;vec3 camera=vec3(dot(delta,uRight),dot(delta,uUp),dot(delta,uForward));float near=0.055;float far=180.0;gl_Position=vec4(camera.x*1.44*uAspect,camera.y*1.44,(far+near)/(far-near)*camera.z-2.0*far*near/(far-near),camera.z);vColor=aColor;vEmissive=aEmissive;vView=-delta;vNormal=normalize(cross(uModel[1].xyz,uModel[2].xyz)*aNormal.x+cross(uModel[2].xyz,uModel[0].xyz)*aNormal.y+cross(uModel[0].xyz,uModel[1].xyz)*aNormal.z);}');
    const fs=compile(gl.FRAGMENT_SHADER,'precision mediump float;varying vec3 vColor;varying vec3 vNormal;varying vec3 vEmissive;varying vec3 vView;uniform vec3 uAccent;void main(){vec3 n=normalize(vNormal);vec3 light=normalize(vec3(-0.4,0.8,-0.5));float sun=max(0.0,dot(n,light));float shine=pow(max(0.0,dot(reflect(-light,n),normalize(vView))),14.0)*0.18;vec3 lit=vColor*(0.42+sun*0.58)+vec3(shine)+vEmissive*uAccent*0.66;gl_FragColor=vec4(lit,1.0);}');
    gl.attachShader(program,vs);gl.attachShader(program,fs);gl.linkProgram(program);if(!gl.getProgramParameter(program,gl.LINK_STATUS))throw Error(gl.getProgramInfoLog(program));gl.deleteShader(vs);gl.deleteShader(fs);
    const attributes=['aPosition','aNormal','aColor','aEmissive'].map(name=>gl.getAttribLocation(program,name)),uniforms=Object.fromEntries(['uModel','uCamera','uRight','uUp','uForward','uAspect','uAccent'].map(name=>[name,gl.getUniformLocation(program,name)])),buffers=new Map();
    let drawCalls=0,triangles=0,quality='high';
    const chainData=new Float32Array(MESHES.link.vertices.length*MAX_LINKS),chainBuffer=gl.createBuffer();
    gl.bindBuffer(gl.ARRAY_BUFFER,chainBuffer);gl.bufferData(gl.ARRAY_BUFFER,chainData.byteLength,gl.DYNAMIC_DRAW);
    for(const [name,mesh] of Object.entries(MESHES)){if(name==='link')continue;const buffer=gl.createBuffer();gl.bindBuffer(gl.ARRAY_BUFFER,buffer);gl.bufferData(gl.ARRAY_BUFFER,mesh.vertices,gl.STATIC_DRAW);buffers.set(name,{buffer,count:mesh.vertexCount});}
    const identity=new Float32Array([1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1]);
    function beginView(camera,options={}){
      drawCalls=triangles=0;quality=options.quality||'high';gl.useProgram(program);gl.enable(gl.DEPTH_TEST);gl.depthFunc(gl.LEQUAL);gl.depthMask(true);gl.depthRange(0,1);gl.disable(gl.BLEND);gl.disable(gl.CULL_FACE);
      for(const [name,value] of [['uCamera',camera.position],['uRight',camera.right],['uUp',camera.up],['uForward',camera.forward]])gl.uniform3fv(uniforms[name],value);gl.uniform1f(uniforms.uAspect,camera.aspect);
    }
    function drawBuffer(buffer,count,matrix,color){
      gl.uniformMatrix4fv(uniforms.uModel,false,matrix);gl.uniform3fv(uniforms.uAccent,rgb(color));gl.bindBuffer(gl.ARRAY_BUFFER,buffer);
      for(let i=0;i<attributes.length;i++){gl.enableVertexAttribArray(attributes[i]);gl.vertexAttribPointer(attributes[i],3,gl.FLOAT,false,48,i*12);}gl.drawArrays(gl.TRIANGLES,0,count);drawCalls++;triangles+=count/3;
    }
    function drawCable(from,to,options={}){
      if(!from||!to||!Number.isFinite(from.x+from.y+from.z+to.x+to.y+to.z))return false;
      const delta=[to.x-from.x,to.y-from.y,to.z-from.z],length=Math.hypot(...delta);if(length<.07)return false;
      const matrix=directionMatrix([from.x,from.y,from.z],delta),color=options.color||'#bdfcff';
      if(options.chain){
        const links=Math.min(quality==='low'?24:quality==='medium'?40:MAX_LINKS,Math.max(2,Math.ceil(length/.245))),spacing=length/links,vertices=MESHES.link.vertices;
        let used=0;
        for(let link=0;link<links;link++){
          const twist=link%2,offset=(link+.5)*spacing,axial=spacing/.245;
          for(let i=0;i<vertices.length;i+=STRIDE){
            const x=twist?vertices[i+1]:vertices[i],y=twist?-vertices[i]:vertices[i+1],z=vertices[i+2]*axial+offset,nx=twist?vertices[i+4]:vertices[i+3],ny=twist?-vertices[i+3]:vertices[i+4],nz=vertices[i+5]/axial,inv=1/Math.sqrt(nx*nx+ny*ny+nz*nz);
            chainData[used++]=matrix[0]*x+matrix[4]*y+matrix[8]*z+matrix[12];chainData[used++]=matrix[1]*x+matrix[5]*y+matrix[9]*z+matrix[13];chainData[used++]=matrix[2]*x+matrix[6]*y+matrix[10]*z+matrix[14];
            chainData[used++]=(matrix[0]*nx+matrix[4]*ny+matrix[8]*nz)*inv;chainData[used++]=(matrix[1]*nx+matrix[5]*ny+matrix[9]*nz)*inv;chainData[used++]=(matrix[2]*nx+matrix[6]*ny+matrix[10]*nz)*inv;
            for(let channel=6;channel<12;channel++)chainData[used++]=vertices[i+channel];
          }
        }
        gl.bindBuffer(gl.ARRAY_BUFFER,chainBuffer);gl.bufferSubData(gl.ARRAY_BUFFER,0,chainData.subarray(0,used));drawBuffer(chainBuffer,used/STRIDE,identity,color);
      }else{
        const wire=buffers.get('wire');matrix[8]*=length;matrix[9]*=length;matrix[10]*=length;drawBuffer(wire.buffer,wire.count,matrix,color);
      }
      return true;
    }
    function drawProjectile(p,options={}){
      if(!p||!['punch','harpoon'].includes(p.kind)||p.life<=0||!Number.isFinite(p.x+p.y+p.z))return false;
      const matrix=projectileMatrix(p,options.muzzle),mesh=buffers.get(p.kind==='punch'?'fist':'harpoon');
      drawBuffer(mesh.buffer,mesh.count,matrix,p.color||'#bdfcff');
      if(options.muzzle){const rear=transform(matrix,[0,0,p.kind==='punch'?-.75:-1.49]);drawCable(options.muzzle,{x:rear[0],y:rear[1],z:rear[2]},{chain:p.kind==='punch',color:p.color});}
      return true;
    }
    function dispose(){for(const item of buffers.values())gl.deleteBuffer(item.buffer);gl.deleteBuffer(chainBuffer);gl.deleteProgram(program);}
    return {beginView,drawPunch:(p,options)=>drawProjectile(p,options),drawHarpoon:(p,options)=>drawProjectile(p,options),drawCable,dispose,stats:()=>({drawCalls,triangles,staticMeshes:buffers.size,maxChainLinks:quality==='low'?24:quality==='medium'?40:MAX_LINKS})};
  }
  const api=Object.freeze({MESHES,STRIDE,MAX_LINKS,directionMatrix,projectileMatrix,transform,createRenderer});
  root.NeonProjectileModels=api;if(typeof module!=='undefined'&&module.exports)module.exports=api;
})(typeof window!=='undefined'?window:globalThis);
