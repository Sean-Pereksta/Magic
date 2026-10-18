/* Run: node --test game/neon-arena/tests/surface-materials.test.cjs */
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const maps=require('../neon-survival-maps.js'),base=path.resolve(__dirname,'../assets/survival');
const read=file=>JSON.parse(fs.readFileSync(file,'utf8'));
const near=(a,b,tolerance=1e-6)=>assert.ok(Math.abs(a-b)<tolerance,`${a} != ${b}`);
const camera={position:[0,6,0],right:[1,0,0],up:[0,1,0],forward:[0,0,1],aspect:1};
function fixture(id='breach_ring',image=null){
  const blueprint=read(path.join(base,'maps',id+'.json')),tileset=read(path.join(base,'tilesets',id,'tileset.json')),prefabs=read(path.join(base,'tilesets',id,'prefabs.json')),definitions={};
  for(const p of prefabs.items){const file=path.join(base,'tilesets',id,p.file),definition=read(file);definitions[p.id]={...definition,mesh:read(path.resolve(path.dirname(file),definition.geometry))};}
  maps.hydrateForTest(blueprint,tileset,definitions,{image});return {blueprint,tileset};
}
const wall=(id,x=1,z=1)=>({id,type:'wall',x,z,y:2,w:4,d:1,h:4,hp:100,maxHp:100,c:'#176b87',active:true,builtBy:{}});
function vertices(chunk){const result=[];for(let i=0;i<chunk.data.length;i+=maps.SURFACE_STRIDE)result.push(Array.from(chunk.data.slice(i,i+maps.SURFACE_STRIDE)));return result;}
function fakeGL(){
  let next=1;
  const gl={};
  for(const name of ['CURRENT_PROGRAM','ARRAY_BUFFER_BINDING','ACTIVE_TEXTURE','TEXTURE_BINDING_2D','TEXTURE0','TEXTURE_2D','ARRAY_BUFFER','FLOAT','STATIC_DRAW','TRIANGLES','VERTEX_SHADER','FRAGMENT_SHADER','COMPILE_STATUS','LINK_STATUS','RGBA','UNSIGNED_BYTE','UNPACK_FLIP_Y_WEBGL','TEXTURE_MIN_FILTER','TEXTURE_MAG_FILTER','TEXTURE_WRAP_S','TEXTURE_WRAP_T','LINEAR','CLAMP_TO_EDGE','VERTEX_ATTRIB_ARRAY_ENABLED','VERTEX_ATTRIB_ARRAY_BUFFER_BINDING','VERTEX_ATTRIB_ARRAY_SIZE','VERTEX_ATTRIB_ARRAY_TYPE','VERTEX_ATTRIB_ARRAY_NORMALIZED','VERTEX_ATTRIB_ARRAY_STRIDE','VERTEX_ATTRIB_ARRAY_POINTER','CURRENT_VERTEX_ATTRIB'])gl[name]=next++;
  const originalBuffer={tag:'engineBuffer'},originalProgram={tag:'engineProgram'},originalTexture={tag:'engineTexture'},active=gl.TEXTURE0+3;
  gl.state={program:originalProgram,buffer:originalBuffer,active,textures:new Map([[gl.TEXTURE0,originalTexture],[active,{tag:'engineTexture3'}]])};
  gl.attrs=Array.from({length:5},(_,index)=>({enabled:index%2===0,buffer:originalBuffer,size:3,type:gl.FLOAT,normalized:false,stride:24,offset:12,value:[.2,.3,.4,1]}));
  gl.created={buffers:0,textures:0,programs:0};gl.deleted={buffers:[],textures:[],programs:[]};gl.uploads=[];gl.draws=[];
  gl.getParameter=parameter=>parameter===gl.CURRENT_PROGRAM?gl.state.program:parameter===gl.ARRAY_BUFFER_BINDING?gl.state.buffer:parameter===gl.ACTIVE_TEXTURE?gl.state.active:gl.state.textures.get(gl.state.active)||null;
  gl.createShader=()=>({});gl.shaderSource=(shader,source)=>{shader.source=source};gl.compileShader=()=>{};gl.getShaderParameter=()=>true;gl.deleteShader=()=>{};
  gl.createProgram=()=>({id:++gl.created.programs});gl.attachShader=()=>{};gl.linkProgram=()=>{};gl.getProgramParameter=()=>true;gl.deleteProgram=program=>gl.deleted.programs.push(program);
  gl.createTexture=()=>({id:++gl.created.textures});gl.bindTexture=(type,texture)=>gl.state.textures.set(gl.state.active,texture);gl.activeTexture=unit=>{gl.state.active=unit};gl.pixelStorei=()=>{};gl.texImage2D=()=>{};gl.texParameteri=()=>{};gl.deleteTexture=texture=>gl.deleted.textures.push(texture);
  gl.uniforms={};gl.getUniformLocation=(program,name)=>name;gl.getAttribLocation=(program,name)=>['aPos','aUV','aNormal','aEffects','aTint'].indexOf(name);gl.uniform1i=()=>{};gl.uniform1f=(name,value)=>{assert.ok(Number.isFinite(value));gl.uniforms[name]=value;};gl.uniform3fv=gl.uniform4fv=(name,value)=>{assert.ok([...value].every(Number.isFinite));gl.uniforms[name]=Array.from(value);};gl.useProgram=program=>{gl.state.program=program};
  gl.createBuffer=()=>({id:++gl.created.buffers});gl.bindBuffer=(type,buffer)=>{gl.state.buffer=buffer};gl.bufferData=(type,data)=>{gl.state.buffer.data=new Float32Array(data);gl.uploads.push(gl.state.buffer)};gl.deleteBuffer=buffer=>gl.deleted.buffers.push(buffer);
  gl.getVertexAttrib=(index,parameter)=>{const a=gl.attrs[index],keys=new Map([[gl.VERTEX_ATTRIB_ARRAY_ENABLED,'enabled'],[gl.VERTEX_ATTRIB_ARRAY_BUFFER_BINDING,'buffer'],[gl.VERTEX_ATTRIB_ARRAY_SIZE,'size'],[gl.VERTEX_ATTRIB_ARRAY_TYPE,'type'],[gl.VERTEX_ATTRIB_ARRAY_NORMALIZED,'normalized'],[gl.VERTEX_ATTRIB_ARRAY_STRIDE,'stride'],[gl.CURRENT_VERTEX_ATTRIB,'value']]);return a[keys.get(parameter)];};
  gl.getVertexAttribOffset=index=>gl.attrs[index].offset;gl.enableVertexAttribArray=index=>{gl.attrs[index].enabled=true};gl.disableVertexAttribArray=index=>{gl.attrs[index].enabled=false};gl.vertexAttribPointer=(index,size,type,normalized,stride,offset)=>Object.assign(gl.attrs[index],{buffer:gl.state.buffer,size,type,normalized,stride,offset});gl.vertexAttrib3f=(index,x,y,z)=>{gl.attrs[index].value=[x,y,z,1]};gl.vertexAttrib4fv=(index,value)=>{gl.attrs[index].value=Array.from(value)};
  gl.drawArrays=(type,start,count)=>gl.draws.push({buffer:gl.state.buffer,count,tintEnabled:gl.attrs[4].enabled,tint:gl.attrs[4].value.slice()});
  return gl;
}

test('engine wall centers and platform bases produce six correctly positioned faces',()=>{
  fixture();
  const input=[{...wall('wall'),x:10,z:-4,y:5,w:4,d:2,h:6},{id:'deck',type:'platform',x:-10,z:4,y:3,w:4,d:2,h:.6,builtBy:{}}];
  const chunks=maps.buildSurfaceChunks(input),all=chunks.flatMap(vertices);
  for(const piece of input){const own=chunks.find(c=>c.pieceIds.includes(piece.id)),v=vertices(own),bottom=piece.type==='wall'?piece.y-piece.h/2:piece.y;
    near(Math.min(...v.map(p=>p[0])),piece.x-piece.w/2);near(Math.max(...v.map(p=>p[0])),piece.x+piece.w/2);
    near(Math.min(...v.map(p=>p[2])),piece.z-piece.d/2);near(Math.max(...v.map(p=>p[2])),piece.z+piece.d/2);
    near(Math.min(...v.map(p=>p[1])),bottom);near(Math.max(...v.map(p=>p[1])),bottom+piece.h);
    assert.equal(new Set(v.map(p=>p.slice(5,8).join(','))).size,6);
  }
  assert.ok(all.every(v=>v.length===13&&v.every(Number.isFinite)));
});

test('each repeated face stays within its padded atlas crop, including partial tiles',()=>{
  const {tileset}=fixture(),tileMap=new Map(tileset.tiles.map(t=>[t.id,t])),piece={id:'deck',type:'platform',x:0,z:0,y:1,w:13,d:6,h:.4,builtBy:{}};
  const v=vertices(maps.buildSurfaceChunks([piece],{tileUnit:5})[0]);
  for(const p of v){const tile=tileMap.get(p[6]===1?'floor_01':p[6]===-1?'trim_01':'wall_01'),[u0,v0,u1,v1]=tile.uv;assert.ok(p[3]>=u0-1e-7&&p[3]<=u1+1e-7&&p[4]>=v0-1e-7&&p[4]<=v1+1e-7);}
  const top=v.filter(p=>p[6]===1);assert.equal(top.length,3*2*6);
  const full=top.filter(p=>p[0]<=-1.5+1e-7&&p[2]<=2+1e-7),uv=tileMap.get('floor_01').uv;
  near(Math.min(...full.map(p=>p[3])),uv[0]);near(Math.max(...full.map(p=>p[3])),uv[2]);
  const partial=top.filter(p=>p[0]>3.5+1e-7);near(Math.max(...partial.map(p=>p[3])),uv[0]+(uv[2]-uv[0])*.6);
});

test('surface filtering ignores destroyed boxes, authored survival geometry and invalid dimensions',()=>{
  fixture();const chunks=maps.buildSurfaceChunks([wall(1),{...wall(2),active:false},{...wall(3),survivalEnvironment:true},{...wall(4),w:0},{...wall(5),y:NaN},{...wall(6),type:'alien'}]);
  assert.deepEqual(chunks.flatMap(c=>c.pieceIds),[1]);
});

test('all eight atlas themes render the same engine-space building geometry',()=>{
  let expected;
  for(const map of maps.catalog()){
    fixture(map.id);const chunks=maps.buildSurfaceChunks([wall(1)]),v=vertices(chunks[0]);
    const positions=v.map(p=>p.slice(0,3));if(expected)assert.deepEqual(positions,expected);else expected=positions;
    assert.ok(v.every(p=>p[3]>=0&&p[3]<=1&&p[4]>=0&&p[4]<=1));
  }
});

test('unloaded atlas returns a safe fallback without modifying WebGL state',()=>{
  maps.dispose();const gl=fakeGL();assert.equal(maps.canRenderSurfaces,false);assert.equal(maps.hasSurfaceMaterials,false);
  assert.equal(maps.renderSurfaces(gl,camera,[wall(1)]),false);assert.equal(gl.created.programs,0);assert.equal(gl.uploads.length,0);
});

test('battlefield lighting caps local pools and resets to the legacy profile for the next view',()=>{
  fixture('breach_ring',{width:2048,height:2048});const gl=fakeGL(),pieces=[wall(1)],lighting={ambient:.7,exposure:1.1,sunColor:[1,.9,.8],sunDirection:[0,1,0],fogDensity:.08,zones:Array.from({length:8},(_,i)=>({x:i,y:4,z:2,radius:12,color:[1,.5,.25],intensity:.6}))};
  maps.renderSurfaces(gl,camera,pieces,{lighting});
  assert.equal(gl.uniforms.uAmbient,.7);assert.equal(gl.uniforms.uExposure,1.1);assert.deepEqual(gl.uniforms.uSunDirection,[0,1,0]);assert.equal(gl.uniforms.uFogDensity,.08);
  assert.equal(gl.uniforms.uLightCount,6);assert.equal(gl.uniforms['uLightZones[0]'].length,24);assert.deepEqual(gl.uniforms['uLightZones[0]'].slice(-4),[5,4,2,12]);near(gl.uniforms['uLightColors[0]'][0],.6);
  const uploads=gl.uploads.length;maps.renderSurfaces(gl,camera,pieces,{});
  assert.equal(gl.uniforms.uAmbient,.5);assert.equal(gl.uniforms.uExposure,1);assert.equal(gl.uniforms.uLightCount,0);assert.deepEqual(gl.uniforms.uSunColor,[1,1,1]);assert(gl.uniforms['uLightColors[0]'].every(v=>v===0));assert.equal(gl.uploads.length,uploads,'changing lights reuses cached geometry');
});

test('generic rendering groups nearby pieces and caches both split-screen views',()=>{
  fixture('breach_ring',{width:2048,height:2048});const gl=fakeGL(),pieces=[wall(1,1),wall(2,8),wall(3,30)];
  const previous={program:gl.state.program,buffer:gl.state.buffer,active:gl.state.active,texture:gl.state.textures.get(gl.TEXTURE0),attrs:gl.attrs.map(a=>({...a,value:a.value.slice()}))};
  assert.equal(maps.canRenderSurfaces,true);assert.equal(maps.renderSurfaces(gl,camera,pieces,{time:1}),true);
  const first=maps.surfaceStats(gl);assert.equal(first.chunks,2);assert.equal(first.pieces,3);assert.equal(first.uploads,2);assert.equal(first.geometryBuilds,3);assert.equal(first.drawCalls,2);
  maps.renderSurfaces(gl,{...camera,position:[2,6,0]},pieces,{time:2});assert.equal(maps.surfaceStats(gl).uploads,first.uploads);assert.equal(maps.surfaceStats(gl).geometryBuilds,first.geometryBuilds);
  assert.equal(gl.state.program,previous.program);assert.equal(gl.state.buffer,previous.buffer);assert.equal(gl.state.active,previous.active);assert.equal(gl.state.textures.get(gl.TEXTURE0),previous.texture);assert.deepEqual(gl.attrs,previous.attrs);
  assert.ok(gl.draws.every(draw=>draw.tintEnabled));
});

test('available vertex-array objects isolate atlas attributes with one attribute query per view',()=>{
  fixture('breach_ring',{width:2048,height:2048});const gl=fakeGL(),original=gl.attrs,arrays=new Map([[null,original]]),binding=1000;let bound=null,queries=0,deleted=0;
  const getParameter=gl.getParameter,getVertexAttrib=gl.getVertexAttrib;
  const ext={VERTEX_ARRAY_BINDING_OES:binding,createVertexArrayOES(){const vao={};arrays.set(vao,original.map(a=>({...a,enabled:false,buffer:null,value:a.value.slice()})));return vao;},bindVertexArrayOES(vao){const values=gl.attrs.map(a=>a.value);bound=vao;gl.attrs=arrays.get(vao);gl.attrs.forEach((a,i)=>{a.value=values[i]});},deleteVertexArrayOES(){deleted++}};
  gl.getExtension=name=>name==='OES_vertex_array_object'?ext:null;gl.getParameter=parameter=>parameter===binding?bound:getParameter(parameter);gl.getVertexAttrib=(...args)=>{queries++;return getVertexAttrib(...args)};
  const expected=original.map(a=>({...a,value:a.value.slice()}));maps.renderSurfaces(gl,camera,[wall(1)]);
  assert.equal(bound,null);assert.equal(queries,1);assert.deepEqual(gl.attrs,expected);
  maps.renderSurfaces(gl,camera,[wall(1)]);assert.equal(queries,2);assert.deepEqual(gl.attrs,expected);
  maps.dispose();assert.equal(deleted,1);
});

test('damage, tint, build movement and destruction upload only affected chunks',()=>{
  fixture('breach_ring',{width:2048,height:2048});const gl=fakeGL(),pieces=[wall(1,1),wall(2,8),wall(3,30)];maps.renderSurfaces(gl,camera,pieces);
  let stats=maps.surfaceStats(gl);pieces[0].hp=99;maps.renderSurfaces(gl,camera,pieces);assert.equal(maps.surfaceStats(gl).uploads,stats.uploads,'Small damage within the same visual bucket keeps the buffer');
  pieces[0].hp=40;maps.renderSurfaces(gl,camera,pieces);assert.equal(maps.surfaceStats(gl).uploads,stats.uploads+1);assert.equal(maps.surfaceStats(gl).geometryBuilds,stats.geometryBuilds+1);stats=maps.surfaceStats(gl);
  pieces[0].c='#ff0000';maps.renderSurfaces(gl,camera,pieces);assert.equal(maps.surfaceStats(gl).uploads,stats.uploads+1);stats=maps.surfaceStats(gl);
  pieces[0].x=28;maps.renderSurfaces(gl,camera,pieces);assert.equal(maps.surfaceStats(gl).uploads,stats.uploads+2,'Moving across a chunk boundary refreshes the old and new cells');stats=maps.surfaceStats(gl);
  pieces[0].active=false;maps.renderSurfaces(gl,camera,pieces);assert.equal(maps.surfaceStats(gl).pieces,2);assert.equal(maps.surfaceStats(gl).uploads,stats.uploads+1);stats=maps.surfaceStats(gl);
  maps.renderSurfaces(gl,camera,[pieces[1]]);assert.equal(maps.surfaceStats(gl).chunks,1);assert.equal(maps.surfaceStats(gl).uploads,stats.uploads,'Removing an entire cell deletes its buffer without uploading an empty one');
  maps.renderSurfaces(gl,camera,[]);assert.equal(maps.surfaceStats(gl).chunks,0);assert.equal(maps.surfaceStats(gl).pieces,0);assert.equal(maps.surfaceStats(gl).vertices,0);
});

test('the 320-piece build cap needs one draw for pieces occupying one spatial chunk',()=>{
  fixture('breach_ring',{width:2048,height:2048});const gl=fakeGL(),pieces=Array.from({length:320},(_,i)=>({...wall(i),x:i%20*.8,z:Math.floor(i/20)*.8,w:.6,d:.6}));
  maps.renderSurfaces(gl,camera,pieces);let stats=maps.surfaceStats(gl);assert.equal(stats.pieces,320);assert.equal(stats.chunks,1);assert.equal(stats.drawCalls,1);assert.equal(stats.uploads,1);
  maps.renderSurfaces(gl,camera,pieces);assert.equal(maps.surfaceStats(gl).uploads,1);assert.equal(maps.surfaceStats(gl).geometryBuilds,320);
});

test('authored environments share one program and atlas, retain neutral tint, and dispose all buffers',()=>{
  fixture('breach_ring',{width:2048,height:2048});const gl=fakeGL();maps.renderSurfaces(gl,camera,[wall(1)]);
  maps.install({addPlatform:(x,z,w,d,y,h)=>({active:true}),addWall:(x,z,w,d,h,color,y)=>({active:true})});
  const before=gl.draws.length;assert.equal(maps.render(gl,camera,{time:1}),true);
  assert.equal(gl.created.programs,1);assert.equal(gl.created.textures,1);
  assert.ok(gl.draws.slice(before).every(draw=>!draw.tintEnabled&&draw.tint.join(',')==='1,1,1,1'));
  const count=gl.created.buffers;maps.dispose();assert.equal(gl.deleted.buffers.length,count);assert.equal(gl.deleted.textures.length,1);assert.equal(gl.deleted.programs.length,1);assert.equal(maps.canRenderSurfaces,false);
});
