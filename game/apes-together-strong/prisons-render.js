/* Prison silhouettes and bounded survivor crowds share the existing art/LOD. */
(() => {
'use strict';
const P=ATSRenderer.prototype;
const line=(c,x,y,u,v,color,w=1)=>{c.beginPath();c.moveTo(x,y);c.lineTo(u,v);c.strokeStyle=color;c.lineWidth=w;c.stroke()},poly=(c,p,color,stroke)=>{c.beginPath();p.forEach((v,i)=>i?c.lineTo(...v):c.moveTo(...v));c.closePath();c.fillStyle=color;c.fill();if(stroke){c.strokeStyle=stroke;c.lineWidth=1;c.stroke()}},ellipse=(c,x,y,w,h,color)=>{c.beginPath();c.ellipse(x,y,w,h,0,0,Math.PI*2);c.fillStyle=color;c.fill()};
const drawObject=P.drawObject;
P.drawObject=function(c,o){
 if(o.type==='generator'||o.type==='prisonControl'){
  if(o.dead){this.drawRubble(c,o);line(c,-13,-8,11,1,'#a1a4a0',3);return}
  const power=o.type==='generator';ellipse(c,0,4,power?34:25,12,'rgba(0,12,17,.4)');poly(c,[[-26,-10],[1,-22],[27,-8],[0,7]],'#3a5158');poly(c,[[-26,-10],[-26,-40],[0,-25],[0,7]],'#495c62','#879998');poly(c,[[0,7],[0,-25],[27,-39],[27,-8]],'#243f49','#879998');poly(c,[[-26,-40],[1,-54],[27,-39],[0,-25]],power?'#6b7a78':'#738984','#a3aaa0');
  if(power){for(let i=0;i<4;i++)line(c,-21,-33+i*6,-5,-25+i*6,'#182d35',3);poly(c,[[7,-37],[1,-28],[9,-27],[4,-17],[16,-30],[8,-30]],'#edcc7b');for(const x of[-12,8])line(c,x,-47,x,-64,'#253f46',4)}else{poly(c,[[-19,-35],[-19,-23],[-7,-17],[-7,-29]],'#112d33');ellipse(c,-13,-26,2.6,3,'#ed9a69');line(c,10,-34,10,-14,'#c5b382',3);line(c,6,-20,17,-27,'#daca92',3)}
  if(o.hp<o.maxHp)this.health(c,o.hp/o.maxHp,-74,36,'#d7ad6a');return;
 }
 drawObject.call(this,c,o);
 if(!o.prisonKind||o.dead)return;
 if(o.type==='cage'){
  line(c,-31,-47,27,-35,'#8d9fa6',4);line(c,-31,-45,-31,-4,'#94a4a6',3);line(c,27,-35,27,7,'#6f8790',4);
  const locked=o.prisonLock!=='chain'&&!o.prisonUnlocked,color=locked?'#e6a06e':'#97cba0';c.strokeStyle=color;c.lineWidth=2;c.beginPath();c.arc(7,-24,5,Math.PI,Math.PI*2);c.stroke();poly(c,[[1,-23],[13,-21],[13,-12],[1,-14]],color,'#293d43');
  if(o.prisonReward==='champion'||o.prisonReward==='injuredLegend')poly(c,[[-8,-59],[0,-66],[8,-59],[0,-52]],'#e8d18c','#31464a');
 }else if(o.type==='house'&&o.laboratory){line(c,-15,-55,15,-48,'#8fc2c8',4);line(c,0,-66,0,-39,'#8fc2c8',4)}else if(o.type==='gate'&&o.riotGate&&o.solid){for(let i=-2;i<=2;i++)line(c,i*18,-35+i*8,i*18+9,-31+i*8,'#dcaf72',4)}
};
const siege=P.drawSiege;
P.drawSiege=function(g){siege?.call(this,g);const c=this.ctx,z=this.camera.zoom;let budget=this.quality==='low'||this.detailLevel>2?24:72;
 for(const s of g._prisons?.active||[]){const p=s.prison;if(!p)continue;for(const cohort of p.cohorts){if(!cohort.count||budget<=0)continue;const q=this.project(cohort.x,cohort.y);if(!this.visible(q,120*z))continue;const size=Math.min(cohort.count,z<.65?3:8,budget);budget-=size;
  c.save();c.translate(q.x,q.y);c.scale(z,z);for(let i=size-1;i>=0;i--){const dx=(i%3-1)*18,dy=Math.floor(i/3)*14,bob=cohort.moving&&!this.reducedMotion?Math.sin(this.time*7+i)*1.5:0;this.drawApeMini(c,dx,dy+bob,.58,{id:cohort.id+':'+i,species:cohort.species,coatVariant:i%3,state:cohort.reward==='family'&&i%2?'young':'free',moving:cohort.moving,phase:i});if(cohort.reward==='injuredLegend')line(c,dx-5,dy-17,dx+4,dy-11,'#d5d3b2',3)}
  c.beginPath();c.ellipse(0,12,36,12,0,0,Math.PI*2);c.strokeStyle=cohort.exposed>0?'#e1a179':'#a8ceb4';c.lineWidth=1.5;c.stroke();c.restore();
 }
 }
};
})();
