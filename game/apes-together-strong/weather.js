/* Shared weather state: no per-particle simulation or extra sight queries. */
(() => {
'use strict';
const TYPES={clear:{label:'Clear night',sight:1,sound:1},fog:{label:'Low fog',sight:.62,sound:1},rain:{label:'Rain',sight:.8,sound:.7},wind:{label:'High winds',sight:.94,sound:.85},storm:{label:'Storm night',sight:.68,sound:.5}};
const sequence=['clear','fog','rain','wind','storm'];
const P=ATSGame.prototype;
Object.defineProperty(P,'weather',{get(){
 if(!this._weather||this.time>=this._weather.until){const epoch=Math.floor(this.time/150),seed=ATSUtil.hash(this.seed+'weather'),kind=epoch===0?'clear':sequence[1+(epoch-1+seed)%4];this._weather={kind,until:(epoch+1)*150,wind:((seed+epoch*17)%101-50)/50,thunderAt:this.time+18};}
 return this._weather;
}});
P.weatherInfo=function(){return TYPES[this.weather.kind]||TYPES.clear};
P.terrainPace=function(a,t){
 const wet=['rain','storm'].includes(this.weather.kind);
 if(t.road||t.bridge)return 1;
 if(t.biome==='wetland')return wet?.88:.98;
 if(t.biome==='rocky')return a.species==='gibbon'?1:.9;
 if(t.biome==='farmland'&&wet)return .91;
 return 1;
};
const perceive=P.perceive;
P.perceive=function(h,light){return perceive.call(this,h,{...light,range:light.range*this.weatherInfo().sight});};
const visible=P.lineVisible;
P.lineVisible=function(a,b,critical=false){
 if(!critical&&a.id?.startsWith('human')){
  const info=this.weatherInfo(),distance=Math.hypot(a.x-b.x,a.y-b.y);
  if(distance>600*info.sight)return false;
  // Dense brush helps isolated infiltration. Local sampling is cached per actor.
  if(info.sight<1&&distance>180&&b.id==='king'&&this.world.terrain(b.x,b.y).biome==='forest'&&distance>400*info.sight)return false;
 }
 return visible.call(this,a,b,critical);
};
const noise=P.noise;
P.noise=function(x,y,radius,kind='roar'){return noise.call(this,x,y,kind==='alert'?radius:radius*this.weatherInfo().sound,kind);};
const update=P.update;
P.update=function(dt,input){
 const w=this.weather;
 if(!this.ended&&w.kind==='storm'&&this.time>=w.thunderAt){w.thunderAt=this.time+23+ATSUtil.hash(this.seed+Math.floor(this.time))%18;w.flashAt=this.time;this.sound('rumble',.38,this.king.x);}
 return update.call(this,dt,input);
};
const save=P.serialize,restore=ATSGame.fromJSON;
P.serialize=function(){const data=save.call(this);data.weather={...this.weather};return data;};
ATSGame.fromJSON=function(data,hooks){const g=restore.call(this,data,hooks),w=data.weather;if(w&&TYPES[w.kind]&&Number.isFinite(w.until)&&w.until>=g.time&&w.until<=g.time+180)g._weather={kind:w.kind,until:w.until,wind:Number.isFinite(w.wind)?Math.max(-1,Math.min(1,w.wind)):0,thunderAt:Number.isFinite(w.thunderAt)?Math.max(g.time+1,w.thunderAt):g.time+18};return g;};
window.ATSWeatherTypes=TYPES;
})();
