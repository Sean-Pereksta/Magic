/* Crowns are drawn once for the King, outside all crowd sprite caches. */
(() => {
'use strict';
const P=ATSRenderer.prototype,draw=P.drawApe,map=P.drawMap,crown=P.drawCrown;
P.drawApe=function(c,a,king,exposure){const before=this.crownTier;if(king)this.crownTier=a.crownTier||0;try{return draw.call(this,c,a,king,exposure)}finally{this.crownTier=before}};
P.drawMap=function(game,canvas){const before=this.crownTier;this.crownTier=game.progression?.tier||0;try{return map.call(this,game,canvas)}finally{this.crownTier=before}};
P.drawCrown=function(c,x,y,s=1){const tier=this.crownTier??1;
 if(tier===1){crown.call(this,c,x,y,s*1.12);return}
 c.save();c.translate(x,y);c.scale(s,s);c.lineJoin='round';c.lineWidth=1.3;
 const shape=(points,fill,stroke)=>{c.beginPath();points.forEach(([u,v],i)=>i?c.lineTo(u,v):c.moveTo(u,v));c.closePath();c.fillStyle=fill;c.fill();c.strokeStyle=stroke;c.stroke()};
 if(tier===0){shape([[-12,3],[-13,-6],[-5,-2],[0,-10],[5,-2],[12,-6],[11,3]],'#aa8950','#59452a');c.fillStyle='#d9b567';c.fillRect(-11,0,22,3)}
 else{
  shape([[-17,3],[-20,-15],[-12,-8],[-9,-24],[-3,-13],[2,-29],[8,-12],[14,-23],[15,-7],[23,-15],[17,4]],'#3f504d','#e5bd68');
  shape([[-17,-2],[17,-2],[16,6],[-15,6]],'#b2833d','#f1d28a');
  for(const u of [-12,0,12]){shape([[u,-6],[u+3,0],[u,5],[u-3,0]],u?'#cfb177':'#d95d43','#ffe1a0')}
  c.strokeStyle='#f0d7a3';c.lineWidth=2;for(const side of [-1,1]){c.beginPath();c.moveTo(side*18,0);c.lineTo(side*27,-10);c.lineTo(side*26,-18);c.stroke()}
 }
 c.restore();
};
window.ATSReignArt={draw(canvas,renderer,tier){
 const c=canvas.getContext('2d'),w=canvas.width,h=canvas.height;c.clearRect(0,0,w,h);
 const gold=tier===2?'#de9160':'#e5c475',glow=c.createRadialGradient(w/2,h*.51,10,w/2,h*.51,210);glow.addColorStop(0,tier===2?'#6b3427':'#525338');glow.addColorStop(1,'#0a202200');c.fillStyle=glow;c.fillRect(0,0,w,h);
 c.save();c.translate(w/2,h*.52);c.strokeStyle=gold;c.globalAlpha=.32;c.lineWidth=1;c.beginPath();c.arc(0,0,119,0,Math.PI*2);c.stroke();c.beginPath();c.arc(0,0,128,0,Math.PI*2);c.stroke();
 for(let i=0;i<20;i++){const a=i/20*Math.PI*2;c.beginPath();c.moveTo(Math.cos(a)*138,Math.sin(a)*138);c.lineTo(Math.cos(a)*150,Math.sin(a)*150);c.stroke()}c.restore();
 c.save();c.translate(w/2,h*.86);c.scale(2.15,2.15);renderer.drawApe(c,{id:'king',crownTier:tier,hp:160,maxHp:160,dir:.1,phase:0},true);c.restore();
 c.fillStyle=gold;c.font='600 11px Georgia';c.textAlign='center';c.fillText(tier===2?'III  /  THE WAR CROWN':'II  /  THE ROYAL CROWN',w/2,h-9);
}};
})();
