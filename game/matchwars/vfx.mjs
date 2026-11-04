/** Layered spell art drawn in logical battlefield coordinates; no DOM or simulation mutation. */
const TAU=Math.PI*2;
const NEW_TYPES=new Set(['firewave','frostnova','vines','healbeam','radiance','voidrift','resurrect','curse','whirlwind','lightningstorm','blessing','combo']);
function ring(c,x,y,r,steps=6,rotation=0){c.beginPath();for(let i=0;i<=steps;i++){const a=i/steps*TAU+rotation;const px=x+Math.cos(a)*r,py=y+Math.sin(a)*r;i?c.lineTo(px,py):c.moveTo(px,py);}c.stroke();}
function cross(c,x,y,size){c.fillRect(x-size*.18,y-size*.5,size*.36,size);c.fillRect(x-size*.5,y-size*.18,size,size*.36);}
function glow(c,x,y,r,color){const g=c.createRadialGradient(x,y,0,x,y,r);g.addColorStop(0,color+'77');g.addColorStop(1,color+'00');c.fillStyle=g;c.beginPath();c.arc(x,y,r,0,TAU);c.fill();}

export function drawSpellEffect(c,e,baseline,time=0,reduced=false){
  if(!NEW_TYPES.has(e.type))return false;
  const t=Math.max(0,Math.min(1,(e.age||0)/(e.maxLife||1))),fade=Math.sin(Math.PI*Math.max(.05,t));
  const targeted=['frostnova','vines','curse','radiance'].includes(e.type);
  const x=targeted?(e.toX??e.x):e.x,y=baseline-30,color=e.color||'#c9c0f4',r=18+t*48;
  c.save();c.globalAlpha=Math.max(0,fade)*.85;c.strokeStyle=color;c.fillStyle=color;c.lineWidth=2;
  if(reduced){c.beginPath();c.ellipse(x,y,32,20,0,0,TAU);c.stroke();c.restore();return true;}
  switch(e.type){
    case 'firewave':{
      glow(c,x,y,80,'#ff8b55');
      const direction=Math.sign((e.toX??(x+1))-x)||1;
      for(let i=0;i<7;i++){const a=(i-3)*.32,px=x+direction*Math.cos(a)*r*.6,py=y+Math.sin(a)*r;
        c.fillStyle=i%2?'#ffe8a3':'#ff8553';c.beginPath();c.moveTo(px-7,py+12);c.quadraticCurveTo(px-19,py,px+3,py-25-t*14);c.quadraticCurveTo(px+20,py+3,px+7,py+13);c.fill();}
      break;
    }
    case 'frostnova':{
      glow(c,x,y,75,'#6dd5f5');ring(c,x,y,r,6,time*.2);
      for(let i=0;i<8;i++){const a=i/8*TAU,px=x+Math.cos(a)*r,py=y+Math.sin(a)*r;c.beginPath();c.moveTo(x+Math.cos(a)*12,y+Math.sin(a)*12);c.lineTo(px,py);c.lineTo(px+Math.cos(a+2.4)*10,py+Math.sin(a+2.4)*10);c.moveTo(px,py);c.lineTo(px+Math.cos(a-2.4)*10,py+Math.sin(a-2.4)*10);c.stroke();}
      break;
    }
    case 'vines':{
      glow(c,x,y,65,'#76cf89');
      for(let i=0;i<5;i++){const px=x+(i-2)*18,top=y-8-Math.sin(t*Math.PI)*35-i%2*12;c.beginPath();c.moveTo(px,baseline+3);c.bezierCurveTo(px-24,y+15,px+20,top+8,px,top);c.stroke();c.beginPath();c.ellipse(px-6,top+7,9,4,-.6,0,TAU);c.fill();}
      break;
    }
    case 'healbeam':{
      const from=e.sourceX??x,to=e.toX??x;c.lineWidth=4;c.shadowBlur=14;c.shadowColor='#b0efb6';
      c.beginPath();c.moveTo(from,y-12);c.quadraticCurveTo((from+to)/2,y-65,to,y);c.stroke();c.shadowBlur=0;
      glow(c,to,y,46,'#a5e6ac');
      for(let i=0;i<4;i++){const a=i/4*TAU+time,px=to+Math.cos(a)*25,py=y+Math.sin(a)*20-t*20;c.fillStyle='#d4f5c2';cross(c,px,py,8);}
      break;
    }
    case 'radiance':{
      glow(c,x,y,90,'#ffe5a0');c.strokeStyle='#fff3c3';
      for(let i=0;i<10;i++){const a=i/10*TAU+time*.2;c.beginPath();c.moveTo(x+Math.cos(a)*17,y+Math.sin(a)*17);c.lineTo(x+Math.cos(a)*r,y+Math.sin(a)*r);c.stroke();}
      c.fillStyle='#fff3bc';cross(c,x,y,23);ring(c,x,y,r*.55,12,-time*.2);break;
    }
    case 'voidrift':case 'resurrect':{
      glow(c,x,baseline-7,80,'#ad7eee');c.fillStyle='#1e173bc4';c.beginPath();c.ellipse(x,baseline-5,18+t*34,8+t*10,0,0,TAU);c.fill();
      c.strokeStyle='#ca9fff';c.beginPath();c.ellipse(x,baseline-5,18+t*34,8+t*10,0,0,TAU);c.stroke();
      for(let i=0;i<7;i++){const a=i/7*TAU+time*1.5,px=x+Math.cos(a)*(20+t*26),py=baseline-10-Math.sin(t*Math.PI)*60-i%3*7;c.fillStyle=i%2?'#ded4f4':'#ac83db';c.fillRect(px,py,3,8);}
      if(e.type==='resurrect'){c.globalAlpha*=.9;c.fillStyle='#dfd4ee';c.beginPath();c.arc(x,y-10-t*15,9,0,TAU);c.fill();c.fillRect(x-5,y-6-t*15,10,8);c.fillStyle='#5d397c';c.fillRect(x-5,y-13-t*15,3,4);c.fillRect(x+2,y-13-t*15,3,4);}
      break;
    }
    case 'curse':{
      glow(c,x,y,60,'#bd77db');ring(c,x,y,28+t*12,4,Math.PI/4+time*.45);ring(c,x,y,19+t*8,4,-time*.45);
      c.beginPath();c.ellipse(x,y,16,7,0,0,TAU);c.stroke();c.fillStyle='#f2c4ff';c.beginPath();c.arc(x,y,4,0,TAU);c.fill();break;
    }
    case 'whirlwind':{
      glow(c,x,y,65,'#79dce3');
      for(let i=0;i<4;i++){const yy=baseline-i*14-t*10,rr=12+i*9;c.beginPath();c.ellipse(x+Math.sin(time*8+i)*5,yy,rr,5+i*2,time*.4,Math.PI*.15,Math.PI*1.85);c.stroke();}
      break;
    }
    case 'lightningstorm':{
      glow(c,x,y,70,'#b7dfff');const end=e.toX??x+50;
      c.strokeStyle='#e5f6ff';c.lineWidth=3;c.beginPath();c.moveTo(x,y-65);
      for(let i=1;i<=8;i++){const n=Math.sin(i*17+Math.floor(time*17))*14;c.lineTo(x+(end-x)*i/8+n,y-65+i*8);}c.stroke();
      c.strokeStyle='#76cfff';for(let i=0;i<3;i++){const px=end+(i-1)*26;c.beginPath();c.moveTo(end,y-15);c.lineTo(px+8,y+8);c.lineTo(px-3,y+26);c.stroke();}break;
    }
    case 'blessing':{
      glow(c,x,y,65,'#f9d88c');ring(c,x,y,30+t*7,6,Math.PI/6);c.strokeStyle='#fff1bc';ring(c,x,y,23+t*5,6,Math.PI/6);c.fillStyle='#fcebc4';cross(c,x,y,16);break;
    }
    case 'combo':{
      glow(c,x,y,75,color);for(let i=0;i<2;i++){c.strokeStyle=i?(e.secondaryColor||'#fff0b9'):color;c.beginPath();c.ellipse(x,y,26+t*20,13+t*11,(i?1:-1)*time*.7,0,TAU);c.stroke();}
      c.fillStyle='#fff0bc';for(let i=0;i<4;i++){const a=i/4*TAU+time;c.fillRect(x+Math.cos(a)*r,y+Math.sin(a)*r,4,4);}break;
    }
  }
  c.restore();return true;
}

export function drawCastingRune(c,u,y,time=0,reduced=false){
  if(!u.casting)return;
  c.save();c.globalAlpha=.7;c.strokeStyle=u.color||'#d7c2f7';c.lineWidth=1.5;
  c.beginPath();c.ellipse(u.x,y-2,25,9,0,0,TAU);c.stroke();
  for(let i=0;i<5;i++){const a=i/5*TAU+(reduced?0:time*1.4);c.fillStyle=u.color||'#d7c2f7';c.fillRect(u.x+Math.cos(a)*24,y-3+Math.sin(a)*8,3,3);}
  c.restore();
}
