import {getViewMatrix} from './world-camera.js';
import {buildTerrainMesh,PAINTED_WORLD_ATLASES} from './world-mesh.js';

const VERTEX_SHADER = `
attribute vec3 aPosition;
attribute vec3 aNormal;
attribute vec2 aUV;
attribute vec4 aColor;
attribute float aTile;
uniform mat4 uView;
varying vec3 vNormal;
varying vec2 vUV;
varying vec3 vWorld;
varying vec4 vColor;
varying float vTile;
void main() {
 gl_Position = uView * vec4(aPosition, 1.0);
 vWorld = aPosition;
 vNormal = aNormal;
 vUV = aUV;
 vColor = aColor;
 vTile = aTile;
}`;

const FRAGMENT_SHADER = `
precision highp float;
uniform sampler2D uAtlas;
uniform sampler2D uFog;
uniform vec2 uTiles;
uniform vec2 uWorldSize;
uniform vec3 uFogColor;
uniform vec3 uSun;
uniform float uLight;
uniform float uTime;
uniform float uFogOn;
varying vec3 vNormal;
varying vec2 vUV;
varying vec3 vWorld;
varying vec4 vColor;
varying float vTile;

vec3 paintedPigment(float tile, vec2 uv) {
 vec2 cell = vec2(mod(tile, uTiles.x), floor(tile / uTiles.x));
 vec2 repeatUV = 1.0 - abs(mod(uv, 2.0) - 1.0);
 return texture2D(uAtlas, (cell + mix(vec2(.022), vec2(.978), repeatUV)) / uTiles).rgb;
}

void main() {
 vec3 normal = normalize(vNormal);
 vec3 pigment = vColor.rgb;
 if(vTile > -.5) {
  float tile = floor(vTile + .5);
  float coat = step(31.5,tile);
  tile -= coat * 32.0;
  float terrain = step(15.5,tile);
  tile -= terrain * 16.0;
  vec3 art = paintedPigment(tile, vUV);
  if(terrain > .5) {
   mat2 slant = mat2(.60,.44,-.44,.60);
   vec2 drift = vec2(sin(vUV.y*.41),sin(vUV.x*.32))*.33;
   vec3 widePaint = paintedPigment(tile, slant*vUV + vec2(.34,1.16) + drift);
   art = mix(art,widePaint,.36);
   art *= .94 + .065 * sin(vWorld.x*.0041 + sin(vWorld.y*.0071)*1.82)
                + .032 * cos(vWorld.y*.0034 + vWorld.x*.0011);
  }
  vec3 material = mix(vColor.rgb, art * mix(vec3(1.13),vColor.rgb * 1.26,.27),.84);
  vec3 tinted = vColor.rgb * (.74 + .43 * dot(art,vec3(.2126,.7152,.0722)));
  pigment = mix(material,tinted,coat);
 } else if(vTile < -3.5) {
  pigment *= 1.11 + .055 * sin(uTime * 2.8 + vWorld.x * .03);
 } else if(vTile < -2.5) {
  float rollers = smoothstep(.72,.93,fract(vUV.x - uTime * 1.6));
  float segment = smoothstep(.83,.96,fract(vUV.y));
  pigment = pigment * (.79 + .24 * rollers + .11 * segment);
 } else if(vTile < -1.5) {
  float waves = sin(vWorld.y*.051 + uTime*.84 + sin(vWorld.x*.016)*2.8);
  float eddies = .52 + .48 * sin(vWorld.x*.032 - vWorld.y*.011 + uTime*.45);
  float light = pow(max(0.0,waves),7.0) * eddies * .19;
  vec3 water = mix(pigment * .8,pigment * 1.12,.5+.5*sin(vWorld.y*.008 + sin(vWorld.x*.004)));
  pigment = mix(water,vec3(.62,.76,.77),light);
 }
 float lambert = max(0.0, dot(normal, uSun));
 float sunlight = (.64 + .46 * lambert) * uLight;
 if(vTile < -3.5) sunlight = 1.0;
 vec3 shaded = pigment * sunlight;
 if (vWorld.z < -.5) shaded *= .85;
 float sight = texture2D(uFog, clamp(vWorld.xy / uWorldSize, 0.0, 1.0)).r;
 float reveal = (.09 + .91 * sight) * uFogOn + (1.0-uFogOn);
 vec3 darkness = mix(uFogColor, shaded * vec3(.35,.48,.53), .23);
 gl_FragColor = vec4(mix(darkness, shaded, reveal), vColor.a);
}`;

const attributes = {
 aPosition:[3,0],aNormal:[3,3],aUV:[2,6],aColor:[4,8],aTile:[1,12]
};
const uniforms = ['uView','uAtlas','uFog','uTiles','uWorldSize','uFogColor','uSun','uLight','uTime','uFogOn'];
const atlasImages = new Map();
const atlasLoads = new Map();

function loadAtlas(key) {
 if(!atlasLoads.has(key)) {
  const info=PAINTED_WORLD_ATLASES[key];
  const waiting=new Promise((resolve,reject)=>{
   const image=new Image();
   image.onload=()=>{atlasImages.set(key,image);resolve();};
   image.onerror=()=>reject(new Error(`Could not load ${info.path}.`));
   image.src=new URL('./assets/'+info.path,import.meta.url).href;
  }).catch(error=>{atlasLoads.delete(key);throw error;});
  atlasLoads.set(key,waiting);
 }
 return atlasLoads.get(key);
}

export async function loadWorld3DArtwork(map) {
 if(typeof Image==='undefined')return;
 const id=typeof map==='string'?map:map?.id;
 const themes=id?[id in PAINTED_WORLD_ATLASES?id:'default']:Object.keys(PAINTED_WORLD_ATLASES);
 await Promise.all(themes.map(loadAtlas));
}

function compile(gl,type,source) {
 const result=gl.createShader(type);
 gl.shaderSource(result,source);
 gl.compileShader(result);
 if(!gl.getShaderParameter(result,gl.COMPILE_STATUS)){
  const reason=gl.getShaderInfoLog(result);
  gl.deleteShader(result);
  throw new Error('Could not compile the battlefield shader. '+reason);
 }
 return result;
}

function makeProgram(gl) {
 const vertex=compile(gl,gl.VERTEX_SHADER,VERTEX_SHADER);
 const fragment=compile(gl,gl.FRAGMENT_SHADER,FRAGMENT_SHADER);
 const program=gl.createProgram();
 gl.attachShader(program,vertex);
 gl.attachShader(program,fragment);
 gl.linkProgram(program);
 gl.deleteShader(fragment);
 gl.deleteShader(vertex);
 if(!gl.getProgramParameter(program,gl.LINK_STATUS)){
  const reason=gl.getProgramInfoLog(program);
  gl.deleteProgram(program);
  throw new Error('Could not make the battlefield shader. '+reason);
 }
 return program;
}

function atlasTexture(gl, image) {
 const texture=gl.createTexture();
 gl.bindTexture(gl.TEXTURE_2D,texture);
 gl.pixelStorei(gl.UNPACK_FLIP_Y_WEBGL,false);
 gl.pixelStorei(gl.UNPACK_PREMULTIPLY_ALPHA_WEBGL,false);
 gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_WRAP_S,gl.CLAMP_TO_EDGE);
 gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_WRAP_T,gl.CLAMP_TO_EDGE);
 gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_MAG_FILTER,gl.LINEAR);
 if(!image){
  gl.texImage2D(gl.TEXTURE_2D,0,gl.RGB,1,1,0,gl.RGB,gl.UNSIGNED_BYTE,new Uint8Array([185,178,166]));
  gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_MIN_FILTER,gl.LINEAR);
  return texture;
 }
 const mip=document.createElement('canvas');
 mip.width=2**Math.ceil(Math.log2(image.width));
 mip.height=2**Math.ceil(Math.log2(image.height));
 const context=mip.getContext('2d',{alpha:false});
 context.drawImage(image,0,0,mip.width,mip.height);
 gl.texImage2D(gl.TEXTURE_2D,0,gl.RGB,gl.RGB,gl.UNSIGNED_BYTE,mip);
 gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_MIN_FILTER,gl.LINEAR_MIPMAP_LINEAR);
 gl.generateMipmap(gl.TEXTURE_2D);
 return texture;
}

function drawBuffer(gl,locations,buffer,count,stride) {
 gl.bindBuffer(gl.ARRAY_BUFFER,buffer);
 for(const [attribute,[size,offset]] of Object.entries(attributes)){
  const i=locations[attribute];
  gl.enableVertexAttribArray(i);
  gl.vertexAttribPointer(i,size,gl.FLOAT,false,stride*4,offset*4);
 }
 gl.drawArrays(gl.TRIANGLES,0,count);
}

class World3D {
 constructor(canvas,gl) {
  this.canvas=canvas;
  this.gl=gl;
  this.shader=makeProgram(gl);
  this.buffers={solids:gl.createBuffer(),translucent:gl.createBuffer()};
  this.locations=Object.fromEntries(Object.keys(attributes).map(name=>[name,gl.getAttribLocation(this.shader,name)]));
  this.uniforms=Object.fromEntries(uniforms.map(name=>[name,gl.getUniformLocation(this.shader,name)]));
  this.textures=new Map();
  this.fog=gl.createTexture();
  gl.bindTexture(gl.TEXTURE_2D,this.fog);
  gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_MIN_FILTER,gl.LINEAR);
  gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_MAG_FILTER,gl.LINEAR);
  gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_WRAP_S,gl.CLAMP_TO_EDGE);
  gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_WRAP_T,gl.CLAMP_TO_EDGE);
  gl.texImage2D(gl.TEXTURE_2D,0,gl.LUMINANCE,1,1,0,gl.LUMINANCE,gl.UNSIGNED_BYTE,new Uint8Array([255]));
  this.lastFog=null;
  this.fogPixels=new Uint8Array();
 }

 prepare(map,now=0) {
  if(!map?.width||!map?.height||this.gl.isContextLost())return;
  const gl=this.gl,theme=atlasImages.has(map.id)?map.id:'default';
  const image=atlasImages.get(theme);
  const beltDirections=(map.conveyors||[]).map(b=>b.reversedUntil>now?'r':'f').join('');
  if(this.map===map&&this.theme===theme&&this.atlasImage===image&&this.beltDirections===beltDirections)return;
  this.map=map;
  this.theme=theme;
  this.atlasImage=image;
  this.beltDirections=beltDirections;
  const scene=buildTerrainMesh(map,theme!=='default',now);
  this.stride=scene.stride;
  this.counts={};
  for(const unit of ['solids','translucent']){
   gl.bindBuffer(gl.ARRAY_BUFFER,this.buffers[unit]);
   gl.bufferData(gl.ARRAY_BUFFER,scene[unit],gl.STATIC_DRAW);
   this.counts[unit]=scene[unit].length/this.stride;
  }
  const previous=this.textures.get(theme);
  if(!previous||previous.image!==image){
   if(previous)gl.deleteTexture(previous.texture);
   this.textures.set(theme,{image,texture:atlasTexture(gl,image)});
  }
 }

 uploadVisibility(game,enabled) {
  const gl=this.gl,fog=game.fog,seat=game.localTeam??0,table=fog?.teams?.[seat];
  gl.activeTexture(gl.TEXTURE1);
  gl.bindTexture(gl.TEXTURE_2D,this.fog);
  if(!enabled||!table?.visible||!fog.cols||!fog.rows)return false;
  const last=this.lastFog;
  if(last?.fog===fog&&last.seat===seat&&last.revision===fog.revision&&last.table===table)return true;
  this.lastFog={fog,seat,revision:fog.revision,table};
  const length=fog.cols*fog.rows;
  if(this.fogPixels.length!==length)this.fogPixels=new Uint8Array(length);
  for(let i=0;i<length;i++)this.fogPixels[i]=table.visible[i]?255:table.explored?.[i]?104:0;
  gl.pixelStorei(gl.UNPACK_ALIGNMENT,1);
  if(last?.cols===fog.cols&&last.rows===fog.rows){
   gl.texSubImage2D(gl.TEXTURE_2D,0,0,0,fog.cols,fog.rows,gl.LUMINANCE,gl.UNSIGNED_BYTE,this.fogPixels);
  }else{
   gl.texImage2D(gl.TEXTURE_2D,0,gl.LUMINANCE,fog.cols,fog.rows,0,gl.LUMINANCE,gl.UNSIGNED_BYTE,this.fogPixels);
  }
  this.lastFog.cols=fog.cols;
  this.lastFog.rows=fog.rows;
  gl.pixelStorei(gl.UNPACK_ALIGNMENT,4);
  return true;
 }

 render(game,camera,width,height,dpr=1,options={}) {
  if(this.gl.isContextLost()||width<1||height<1)return null;
  this.prepare(game.map,game.time||0);
  if(!this.counts)return null;
  const gl=this.gl,canvas=this.canvas,location=this.uniforms;
  const pixelsWide=Math.max(1,Math.round(width*dpr)),pixelsHigh=Math.max(1,Math.round(height*dpr));
  if(canvas.width!==pixelsWide)canvas.width=pixelsWide;
  if(canvas.height!==pixelsHigh)canvas.height=pixelsHigh;
  const matrix=getViewMatrix(camera,width,height);
  const rain=this.map.id==='rainyard';
  gl.bindFramebuffer(gl.FRAMEBUFFER,null);
  gl.viewport(0,0,canvas.width,canvas.height);
  gl.clearColor(rain?.055:.082,rain?.086:.110,rain?.115:.111,1);
  gl.clearDepth(1);
  gl.disable(gl.SCISSOR_TEST);
  gl.disable(gl.CULL_FACE);
  gl.disable(gl.BLEND);
  gl.enable(gl.DEPTH_TEST);
  gl.depthFunc(gl.LEQUAL);
  gl.depthMask(true);
  gl.colorMask(true,true,true,true);
  gl.clear(gl.COLOR_BUFFER_BIT|gl.DEPTH_BUFFER_BIT);
  gl.useProgram(this.shader);
  gl.uniformMatrix4fv(location.uView,false,matrix);
  gl.uniform1f(location.uTime,game.time||0);
  gl.uniform3f(location.uFogColor,.073,.115,.139);
  gl.uniform3f(location.uSun,-.47,-.52,.713);
  gl.uniform1f(location.uLight,rain?.96:1.08);
  gl.uniform2f(location.uWorldSize,this.map.width,this.map.height);
  const atlas=PAINTED_WORLD_ATLASES[this.theme];
  gl.uniform2f(location.uTiles,atlas.cols,atlas.rows);
  gl.uniform1i(location.uAtlas,0);
  gl.uniform1i(location.uFog,1);
  gl.uniform1f(location.uFogOn,this.uploadVisibility(game,options.fog!==false)?1:0);
  gl.activeTexture(gl.TEXTURE0);
  gl.bindTexture(gl.TEXTURE_2D,this.textures.get(this.theme).texture);
  drawBuffer(gl,this.locations,this.buffers.solids,this.counts.solids,this.stride);
  gl.enable(gl.BLEND);
  gl.blendFunc(gl.SRC_ALPHA,gl.ONE_MINUS_SRC_ALPHA);
  gl.depthMask(false);
  drawBuffer(gl,this.locations,this.buffers.translucent,this.counts.translucent,this.stride);
  gl.depthMask(true);
  gl.disable(gl.BLEND);
  options.drawActors?.(gl,matrix);
  return canvas;
 }

 dispose() {
  const gl=this.gl;
  gl.deleteProgram(this.shader);
  gl.deleteBuffer(this.buffers.solids);
  gl.deleteBuffer(this.buffers.translucent);
  gl.deleteTexture(this.fog);
  for(const atlas of this.textures.values())gl.deleteTexture(atlas.texture);
  this.canvas.remove();
 }
}

export function createWorld3D() {
 if(typeof document==='undefined')return null;
 const canvas=document.createElement('canvas');
 const gl=canvas.getContext('webgl',{
  alpha:false, antialias:true, depth:true, preserveDrawingBuffer:true,
  powerPreference:'high-performance'
 });
 if(!gl)return null;
 return new World3D(canvas,gl);
}
