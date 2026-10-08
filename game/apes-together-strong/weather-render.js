/* Fixed screen-space weather budget, shared timing and reduced-motion support. */
(() => {
'use strict';
const P=ATSRenderer.prototype,atmosphere=P.drawAtmosphere,draw=P.draw;
P.draw=function(g,dt){this.campaignWeather=g.weather;return draw.call(this,g,dt);};
P.drawAtmosphere=function(c,king,view,g){
 atmosphere.call(this,c,king,view,g);const w=g.weather;if(!w||w.kind==='clear')return;
 const low=this.quality==='low'||this.detailLevel>=2,t=this.reducedMotion?0:g.time;
 c.save();
 if(w.kind==='fog'||w.kind==='storm'){
  c.fillStyle=w.kind==='fog'?'rgba(137,170,166,.075)':'rgba(36,61,76,.08)';c.fillRect(0,0,this.w,this.h);
  // Four translucent bands instead of world-sized fog particles.
  for(let i=0;i<(low?2:4);i++){const y=((i*.27+.08)*this.h+Math.sin(t*.04+i)*24);const gradient=c.createLinearGradient(0,y-55,0,y+55);gradient.addColorStop(0,'rgba(139,177,173,0)');gradient.addColorStop(.5,'rgba(139,177,173,.045)');gradient.addColorStop(1,'rgba(139,177,173,0)');c.fillStyle=gradient;c.fillRect(0,y-55,this.w,110);}
 }
 const rain=w.kind==='rain'||w.kind==='storm';
 if(rain){
  const count=this.reducedMotion?10:low?24:64;c.strokeStyle='rgba(172,204,211,.22)';c.lineWidth=1;c.beginPath();
  for(let i=0;i<count;i++){const x=((i*137.508+t*w.wind*60)%this.w+this.w)%this.w,y=(i*73.31+t*340)%this.h;c.moveTo(x,y);c.lineTo(x+w.wind*7,y+11);}
  c.stroke();
 }
 if(w.kind==='wind'&&!this.reducedMotion){c.strokeStyle='rgba(179,195,151,.23)';c.lineWidth=1;c.beginPath();for(let i=0;i<(low?8:18);i++){const x=((i*121+t*48)%this.w+this.w)%this.w,y=(i*97)%this.h+Math.sin(t+i)*8;c.moveTo(x,y);c.lineTo(x+8,y-2);}c.stroke();}
 const flash=w.flashAt===undefined?10:g.time-w.flashAt;
 // A single soft flash; reduced-motion mode suppresses lightning entirely.
 if(!this.reducedMotion&&flash>=0&&flash<.65){c.fillStyle='rgba(183,202,219,'+(.12*(1-flash/.65))+')';c.fillRect(0,0,this.w,this.h);}
 c.restore();
};
})();
