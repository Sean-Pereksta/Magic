// Short, bounded impact illustrations share the simulation's existing effect
// lifetime. No extra particles/entities or persistent per-frame allocations.
const TAU=Math.PI*2,INK='#354549',colors=['#269b98','#ed795d','#9e7bc5','#d4aa4e'];
function line(c,points,col,width=2){c.beginPath();points.forEach((p,i)=>i?c.lineTo(...p):c.moveTo(...p));c.strokeStyle=col;c.lineWidth=width;c.lineCap='round';c.lineJoin='round';c.stroke();}
function oval(c,x,y,rx,ry,fill,stroke=null,width=1.5){c.beginPath();c.ellipse(x,y,Math.max(.01,rx),Math.max(.01,ry),0,0,TAU);if(fill){c.fillStyle=fill;c.fill();}if(stroke){c.strokeStyle=stroke;c.lineWidth=width;c.stroke();}}
function chip(c,x,y,r,a,fill){c.save();c.translate(x,y);c.rotate(a);c.beginPath();c.moveTo(-r,-r*.7);c.lineTo(r*.7,-r);c.lineTo(r,r*.3);c.lineTo(-r*.3,r*.6);c.closePath();c.fillStyle=fill;c.fill();c.strokeStyle=INK+'80';c.lineWidth=.7;c.stroke();c.restore();}
function burst(c,x,y,p,r,col,count=7){for(let i=0;i<count;i++){const a=i*TAU/count+.15,inner=r*(.38+p*.8),outer=inner+8*(1-p);line(c,[[x+Math.cos(a)*inner,y+Math.sin(a)*inner*.7],[x+Math.cos(a)*outer,y+Math.sin(a)*outer*.7]],col,1.7*(1-p)+.5);}}

export function drawBattleEffect(renderer,c,fx){
 if(!['hit','death','leap','abilityCast','crumple','forcefieldPush'].includes(fx.type))return false;
 const radius=Math.min(100,fx.radius||24);if(!renderer.visible(fx.x,fx.y,Math.max(130,radius*2)))return true;
 const p=Math.max(0,Math.min(1,1-(fx.life||0)/(fx.total||.5))),tc=colors[fx.team]||'#a7b48c',family=fx.family||({sniper:'scissors',shears:'scissors',airplane:'paper',divider:'paper',rollingrock:'rock',mage:'rock'})[fx.sourceType]||fx.sourceType;
 c.save();c.globalAlpha=(1-p)*(fx.type==='abilityCast'?.8:1);
 if(fx.type==='forcefieldPush'){
  const fromVisible=Number.isFinite(fx.fromX)&&Number.isFinite(fx.fromY)&&(renderer.pointVisible?.(fx.fromX,fx.fromY)??true);
  if(fromVisible){
   const dx=fx.x-fx.fromX,dy=fx.y-fx.fromY,distance=Math.hypot(dx,dy),a=Math.atan2(dy,dx),trail=Math.min(90,distance)*(1-p*.7),tailX=fx.x-Math.cos(a)*trail,tailY=fx.y-10-Math.sin(a)*trail;
   line(c,[[tailX,tailY],[fx.x,fx.y-10]],'#d6fff4',5*(1-p)+1);
   line(c,[[tailX,tailY],[fx.x,fx.y-10]],tc+'c0',2);
   for(const side of [-1,1]){const offset=side*Math.min(14,radius*.55),nx=-Math.sin(a)*offset,ny=Math.cos(a)*offset;line(c,[[tailX+Math.cos(a)*trail*.35+nx,tailY+Math.sin(a)*trail*.35+ny],[fx.x+nx,fx.y-10+ny]],'#9de8d8',1.5);}
   c.save();c.translate(fx.x-Math.cos(a)*trail*.3,fx.y-10-Math.sin(a)*trail*.3);c.rotate(a);line(c,[[-6,-5],[0,0],[-6,5]],'#eaffdc',2);c.restore();
  }
  // If the displaced unit has only just entered sight, draw the arrival pulse
  // without exposing its previous position through the fog.
  oval(c,fx.x,fx.y+8,radius*.7+p*15,radius*.3+p*6,null,'#9ce5ce',2*(1-p)+.5);
  burst(c,fx.x,fx.y-3,p,18,'#e2ffd7',5);
 }else if(fx.type==='hit'){
  const a=Number.isFinite(fx.fromX)&&Number.isFinite(fx.fromY)?Math.atan2(fx.y-fx.fromY,fx.x-fx.fromX):-.4,x=fx.x,y=fx.y-10,impact=Math.max(16,Math.min(38,radius+8));
  c.save();c.translate(x,y);c.rotate(a);
  if(family==='scissors'){
   for(let i=0;i<3;i++){c.beginPath();c.ellipse(-6+p*10,(i-1)*7,impact*(.65+p*.35),9,Math.PI*.19,-1.9,1.4);c.strokeStyle=i===1?'#fffce8':'#a8c9c7';c.lineWidth=i===1?4:1.8;c.stroke();}
   for(let i=0;i<4;i++)chip(c,8+p*(17+i*3),(i-1.5)*(5+p*6),2.4,(i+p)*1.7,i%2?'#fff4c9':tc);
  }else if(family==='paper'){
   for(let i=0;i<2;i++){c.beginPath();c.moveTo(-impact*.8,-13+i*23);c.bezierCurveTo(impact*.6,-22+i*27,impact*1.1,18-i*27,-impact*.55,14-i*23);c.strokeStyle=i?'#fffce5':'#79b7ae';c.lineWidth=i?2.5:5;c.stroke();}
   for(let i=0;i<5;i++)chip(c,Math.cos(i*2.399)*(10+p*31),Math.sin(i*2.399)*(7+p*21),3, i+p*4,'#fff6d5');
  }else{
   oval(c,0,6,impact*(.45+p),impact*(.22+p*.4),null,'#b9a275',2.6*(1-p)+.5);
   for(let i=0;i<6;i++){const angle=i*2.399,dist=7+p*(22+i*2);chip(c,Math.cos(angle)*dist,Math.sin(angle)*dist*.65-Math.sin(p*Math.PI)*8,3.1*(1-p)+1,angle+p*2,i%2?'#b4c3c6':'#ddc58b');}
  }
  line(c,[[-10,0],[3,0]],'#fff9d9',4.5*(1-p)+.6);c.restore();
  burst(c,x,y,p,impact,fx.lethal?'#f4be65':'#f1d698',fx.lethal?9:5);
 }else if(fx.type==='death'){
  oval(c,fx.x,fx.y+9,12+p*38,6+p*17,null,tc+'99',1.5);
  for(let i=0;i<11;i++){const a=i*2.399,r=(14+p*48)*(i%3?.8:1.1),lift=Math.sin(p*Math.PI)*(12+i%4*6);chip(c,fx.x+Math.cos(a)*r,fx.y+Math.sin(a)*r*.6-lift,3.4*(1-p)+1,a+p*4,i%3===0?tc:i%2?'#f9edc9':'#c1d1c4');}
 }else if(fx.type==='leap'){
  const r=(fx.radius||60)*(.25+p*.9);oval(c,fx.x,fx.y+8,r,r*.48,null,'#c2aa77',3*(1-p)+1);oval(c,fx.x,fx.y+7,r*.65,r*.3,null,'#fff4cf',2*(1-p)+.5);
  for(let i=0;i<8;i++){const a=i*TAU/8;oval(c,fx.x+Math.cos(a)*r,fx.y+Math.sin(a)*r*.45,5+p*7,2+p*3,'#c5b28e55');if(p<.6)line(c,[[fx.x+Math.cos(a)*12,fx.y+Math.sin(a)*6],[fx.x+Math.cos(a)*(25+p*15),fx.y+Math.sin(a)*(12+p*8)]],'#9d896577',1.3);}
 }else if(fx.type==='crumple'){
  oval(c,fx.x,fx.y-8,17+p*30,21+p*27,'#d5f5e51a','#99ddc9',2);burst(c,fx.x,fx.y-8,p,37,'#eaffd5',8);
  for(let i=0;i<3;i++){const a=i*TAU/3+p*1.2;chip(c,fx.x+Math.cos(a)*28,fx.y-8+Math.sin(a)*26,4,a,'#fff9df');}
 }else{
  // Takeoff windup, and a readable caster cue for future instant abilities.
  const r=15+p*23;oval(c,fx.x,fx.y+10,r,r*.4,null,tc,2);burst(c,fx.x,fx.y,p,28,'#f2cf82',6);
  for(let i=0;i<3;i++){const yy=fx.y-5-p*23+i*7;line(c,[[fx.x-7,yy+4],[fx.x,yy],[fx.x+7,yy+4]],'#fff6d3',1.8);}
 }
 c.restore();return true;
}
