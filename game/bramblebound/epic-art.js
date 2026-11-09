/* Pixel silhouettes, encounter telegraphs and equipped Ancient transformations. */
(() => {
  'use strict';
  const R=Math.round,S=Math.sin,C=Math.cos,TAU=Math.PI*2;
  const colors={leaf:'#9af5a1',fire:'#ff9a58',frost:'#adf3ff',storm:'#ffe58b',stone:'#e7bd86',spirit:'#ddabff'};
  const ink='#252534',white='#fff5d5',cyan='#80ffe2';
  const rect=(g,c,x,y,w=1,h=1)=>{g.fillStyle=c;g.fillRect(R(x),R(y),Math.max(1,R(w)),Math.max(1,R(h)));};
  function line(g,c,x,y,xx,yy,w=1){x=R(x);y=R(y);xx=R(xx);yy=R(yy);let dx=Math.abs(xx-x),sx=x<xx?1:-1,dy=-Math.abs(yy-y),sy=y<yy?1:-1,e=dx+dy;for(let n=0;n<1400;n++){rect(g,c,x,y,w,w);if(x===xx&&y===yy)break;const q=2*e;if(q>=dy){e+=dy;x+=sx;}if(q<=dx){e+=dx;y+=sy;}}}
  function poly(g,c,p){let lo=1e5,hi=-1e5;for(let i=1;i<p.length;i+=2){lo=Math.min(lo,p[i]);hi=Math.max(hi,p[i]);}g.fillStyle=c;for(let y=Math.ceil(lo);y<=hi;y++){const hits=[];for(let i=0,j=p.length-2;i<p.length;j=i,i+=2){const a=p[j+1],b=p[i+1];if((a<=y&&b>y)||(b<=y&&a>y))hits.push(p[j]+(y-a)*(p[i]-p[j])/(b-a));}hits.sort((a,b)=>a-b);for(let i=0;i+1<hits.length;i+=2)g.fillRect(R(hits[i]),y,Math.max(1,R(hits[i+1]-hits[i])),1);}}
  function oval(g,c,x,y,rx,ry){for(let j=-R(ry);j<=ry;j++){const n=R(rx*Math.sqrt(Math.max(0,1-(j/(ry+.4))**2)));rect(g,c,x-n,y+j,2*n+1,1);}}
  function ring(g,c,x,y,rx,ry,clock=0){let px=x+rx*C(clock),py=y+ry*S(clock);for(let i=1;i<=48;i++){const q=clock+i*TAU/48,xx=x+rx*C(q),yy=y+ry*S(q);line(g,c,px,py,xx,yy);px=xx;py=yy;}}
  const diamond=(g,c,x,y,r)=>poly(g,c,[x,y-r,x+r*.65,y,x,y+r,x-r*.65,y]);
  function crown(g,x,y,c){poly(g,ink,[x-13,y+5,x-15,y-7,x-6,y-3,x,y-12,x+6,y-3,x+15,y-7,x+13,y+5]);poly(g,c,[x-11,y+3,x-12,y-4,x-5,y,x,y-8,x+5,y,x+12,y-4,x+10,y+3]);diamond(g,white,x,y-2,3);}
  function boss(g,z,time,reduced){
    const e=z.e,t=reduced?0:time,el=globalThis.Titans?.definitions?.find(d=>d.id===e.titanId)?.element||e.element||e.resist,c=colors[el]||e.color;
    g.save();g.translate(R(z.x),R(z.bodyY));g.scale(z.size,z.size);
    const bob=reduced?0:R(S(t*2)*1.2);g.translate(0,bob);
    if(el==='leaf'){
      for(const side of [-1,1]){
        line(g,ink,side*9,-40,side*24,-61,6);line(g,'#705447',side*9,-40,side*24,-61,3);
        for(let j=0;j<3;j++){const x=side*(14+j*4),y=-47-j*6;line(g,ink,x,y,x+side*12,y-6,4);line(g,'#afaa63',x,y,x+side*11,y-6,2);poly(g,c,[x+side*7,y-5,x+side*9,y-11,x+side*17,y-9,x+side*12,y-4]);}
        poly(g,ink,[side*3,-27,side*23,-28,side*29,-14,side*22,-12,side*21,-1,side*13,1,side*12,-16,side*3,-15]);
        line(g,'#8b704c',side*16,-23,side*18,-2,6);line(g,c,side*16,-16,side*17,-5,1);
      }
      poly(g,ink,[-23,-39,-15,-48,6,-49,24,-37,19,-17,-17,-15,-29,-25]);poly(g,'#59694b',[-20,-37,-12,-45,5,-46,20,-35,15,-20,-15,-19,-25,-26]);
      line(g,'#b5c76a',-13,-39,-8,-22,3);line(g,c,2,-44,8,-29,2);poly(g,ink,[-18,-44,-12,-58,3,-59,11,-48,3,-34,-9,-35]);poly(g,'#8c9862',[-16,-45,-10,-55,2,-56,7,-48,0,-38,-8,-38]);
      rect(g,c,-13,-49,5,3);rect(g,white,-12,-49,2,1);poly(g,'#c1e68f',[-20,-35,-31,-45,-29,-30,-21,-25]);
      diamond(g,white,0,-28,4);
    }else if(el==='fire'){
      for(const s of [-1,1]){poly(g,ink,[s*7,-37,s*26,-65,s*42,-43,s*34,-24,s*21,-31,s*14,-19]);poly(g,'#873f48',[s*10,-37,s*26,-60,s*37,-43,s*32,-29,s*21,-35,s*15,-25]);for(let j=0;j<3;j++)line(g,'#f47a48',s*12,-36,s*(24+j*6),-54+j*10,2);}
      poly(g,ink,[-15,-41,-7,-52,8,-47,19,-28,12,-8,27,-7,32,-14,31,-2,9,1,-13,-7,-20,-25]);poly(g,'#c25b43',[-12,-39,-5,-48,5,-43,15,-27,8,-10,23,-5,8,-3,-10,-10,-16,-25]);
      for(let j=0;j<4;j++)line(g,j%2?white:'#ffce70',-7+j*2,-35+j*6,6+j,-32+j*6,2);
      poly(g,ink,[-13,-46,-18,-57,-9,-54,-2,-62,7,-55,15,-52,12,-41,-4,-39]);poly(g,'#e98c4b',[-11,-47,-14,-53,-7,-51,-1,-58,6,-52,11,-50,9,-44,-4,-43]);rect(g,white,-10,-49,5,2);line(g,'#fff0a7',-13,-41,-21,-36,3);
      for(let j=0;j<5;j++)diamond(g,j%2?white:'#ffb654',-18+j*9,-9-(j%2)*3,2);
    }else if(el==='frost'){
      for(const s of [-1,1]){poly(g,ink,[s*3,-37,s*26,-61,s*42,-40,s*26,-24,s*34,-5,s*13,-11,s*3,-23]);poly(g,'#607aaf',[s*5,-37,s*26,-57,s*37,-40,s*22,-25,s*29,-9,s*14,-15,s*6,-24]);poly(g,'#b6eaff',[s*10,-36,s*26,-51,s*30,-40,s*16,-28]);poly(g,'#88cfef',[s*10,-25,s*21,-22,s*25,-13,s*14,-18]);diamond(g,white,s*26,-40,5);line(g,white,s*7,-34,s*25,-53);}
      poly(g,ink,[-6,-50,5,-50,9,-35,6,-23,15,-3,0,-9,-15,-3,-6,-23,-9,-36]);poly(g,'#b3d8eb',[-4,-48,3,-48,6,-35,3,-23,10,-7,0,-12,-10,-7,-3,-23,-6,-35]);rect(g,'#283e68',-4,-44,8,3);rect(g,white,-3,-44,2,1);rect(g,white,2,-44,2,1);crown(g,0,-54,c);
      for(let j=0;j<3;j++)diamond(g,white,(j-1)*12,-15+Math.abs(j-1)*4,2);
    }else if(el==='storm'){
      for(const s of [-1,1]){poly(g,ink,[s*3,-36,s*23,-58,s*45,-53,s*31,-42,s*43,-41,s*25,-27,s*35,-28,s*13,-12,s*3,-21]);poly(g,'#b8944d',[s*5,-34,s*24,-54,s*39,-51,s*27,-40,s*35,-39,s*20,-26,s*25,-27,s*11,-17,s*5,-23]);for(let j=0;j<4;j++)line(g,j%2?white:c,s*(10+j*4),-32-j*3,s*(24+j*3),-45-j*2,2);}
      poly(g,ink,[-9,-42,-2,-52,8,-49,15,-39,8,-19,0,-1,-7,-17,-14,-32]);poly(g,'#d6ad59',[-6,-41,-1,-48,6,-46,11,-38,5,-20,0,-7,-4,-18,-10,-32]);poly(g,white,[-6,-41,4,-42,7,-35,2,-26,-3,-31]);poly(g,'#eda862',[-8,-44,-19,-40,-9,-38]);rect(g,ink,-6,-44,3,2);crown(g,0,-54,c);
      for(const s of [-1,1])poly(g,c,[s*20,-21,s*29,-21,s*23,-14,s*30,-14,s*18,-2,s*21,-12,s*15,-12]);
    }else if(el==='stone'){
      for(const s of [-1,1]){poly(g,ink,[s*10,-45,s*28,-53,s*34,-39,s*29,-25,s*38,-17,s*34,-5,s*22,-6,s*18,-24,s*9,-26]);poly(g,'#867b72',[s*12,-42,s*26,-48,s*30,-38,s*25,-24,s*34,-16,s*31,-9,s*25,-10,s*21,-27,s*12,-29]);line(g,c,s*19,-42,s*24,-31,2);rect(g,ink,s*10-5,-20,12,21);rect(g,'#a49a81',s*10-3,-16,8,15);line(g,c,s*10,-16,s*10,-6,2);}
      poly(g,ink,[-17,-46,-12,-57,10,-59,19,-44,20,-19,8,-10,-11,-12,-20,-28]);poly(g,'#a89a7d',[-14,-44,-10,-54,8,-55,15,-43,16,-21,6,-14,-9,-16,-16,-29]);poly(g,'#626b68',[-10,-48,8,-50,12,-42,7,-34,-9,-35]);rect(g,c,-9,-44,6,3);rect(g,c,3,-44,6,3);poly(g,c,[-1,-32,7,-27,2,-21,-6,-27]);diamond(g,white,0,-27,3);
      for(let j=0;j<3;j++){const x=(j-1)*23,y=-62-(j%2)*8;poly(g,ink,[x-5,y,x-4,y-5,x+4,y-7,x+7,y,x+2,y+4]);line(g,c,x-3,y,x+4,y-2,2);}
    }else{
      ring(g,'#684782',0,-34,31,29,t*.15);ring(g,c,0,-34,33,30,-t*.15);
      for(const s of [-1,1]){poly(g,ink,[s*7,-39,s*24,-36,s*31,-20,s*24,-11,s*17,-21,s*22,-27,s*9,-27]);poly(g,'#8267a5',[s*8,-36,s*21,-33,s*27,-21,s*24,-17,s*20,-22,s*25,-28,s*10,-30]);diamond(g,c,s*29,-21,5);}
      poly(g,ink,[-13,-49,0,-59,13,-49,12,-32,7,-25,15,-10,5,-13,0,1,-6,-12,-17,-7,-9,-25,-13,-34]);poly(g,'#8b6cad',[-10,-48,0,-55,10,-48,9,-33,4,-25,10,-14,3,-17,0,-5,-4,-17,-12,-12,-6,-25,-10,-34]);oval(g,'#302d4c',0,-42,8,10);rect(g,c,-6,-44,4,2);rect(g,c,3,-44,4,2);diamond(g,white,0,-28,4);crown(g,0,-59,c);
      for(let j=0;j<4;j++){const q=t*.7+j*TAU/4;diamond(g,j%2?white:c,C(q)*31,-34+S(q)*27,3);}
    }
    if(e.titan?.phase===2){for(let j=0;j<6;j++){const q=t*.8+j*TAU/6;diamond(g,j%2?white:c,C(q)*39,-35+S(q)*30,2);}}
    g.restore();
  }
  function bossAura(g,z,time,reduced){
    const e=z.e,def=globalThis.Titans?.definitions?.find(d=>d.id===e.titanId),c=colors[def?.element]||e.color,t=reduced?0:time;
    g.save();g.globalAlpha=.34;ring(g,c,z.x,z.y,33*z.size,7*z.size,t*.2);g.restore();
    const warning=e.titan?.warning;if(!warning)return;
    const u=1-warning.left/warning.duration,r=24+u*16;
    g.save();g.globalAlpha=.55;ring(g,c,z.x,z.y,r*z.size,9*z.size,t*.3);ring(g,c,z.x,z.center,28*z.size,29*z.size,-t*.4);g.restore();
    for(let j=0;j<6;j++){const q=j*TAU/6+(reduced?0:t*.4);diamond(g,white,z.x+C(q)*29*z.size,z.center+S(q)*30*z.size,2);}
  }
  function ancientAura(g,items,x,y,time,reduced,front=false){
    const list=items.map(it=>globalThis.Catalog.items[it.id]).filter(d=>d?.ancient).slice(0,4),t=reduced?0:time;
    for(const [index,d] of list.entries()){
      const el=colors[d.element],a=d.aura,clock=t+index*1.4;
      g.save();
      if(front){
        const q=clock*.8+index;diamond(g,cyan,x-15+C(q)*5,y-45+S(q)*4,2);diamond(g,white,x+16+S(q)*5,y-28+C(q)*8,1.5);
        if(index===0){line(g,cyan,x-8,y-67,x+8,y-67);diamond(g,white,x,y-70,3);}
        g.restore();continue;
      }
      g.globalAlpha=.35;oval(g,el,x,y-28,30+index*2,35);g.globalAlpha=.8;
      ring(g,cyan,x,y+2,26+index*3,5,clock*.1);
      if(a==='worldroot'){
        for(const s of [-1,1]){line(g,'#3c885f',x+s*12,y-3,x+s*27,y-47,3);for(let j=0;j<4;j++){const xx=x+s*(15+j*3),yy=y-12-j*9;poly(g,el,[xx,yy,xx+s*12,yy-11,xx+s*10,yy-1,xx+s*2,yy+3]);}}
        for(const s of [-1,1]){line(g,el,x+s*5,y-57,x+s*17,y-73,2);line(g,white,x+s*12,y-66,x+s*21,y-66);diamond(g,el,x+s*17,y-74,3);}
      }else if(a==='sunfire'){
        for(const s of [-1,1])for(let j=0;j<4;j++)poly(g,j%2?white:el,[x+s*8,y-29,x+s*(19+j*5),y-60+j*5,x+s*(17+j*5),y-29+j*5,x+s*8,y-18]);
      }else if(a==='winter'){
        for(const s of [-1,1]){line(g,'#78cab6',x+s*14,y-58,x+s*26,y-18,2);line(g,'#d7a5f7',x+s*20,y-49,x+s*30,y-8,2);}
        for(let j=0;j<6;j++){const q=clock*.2+j*TAU/6;diamond(g,j%2?white:el,x+C(q)*30,y-29+S(q)*34,6);}
      }else if(a==='storm'){
        for(const s of [-1,1])poly(g,el,[x+s*17,y-57,x+s*32,y-57,x+s*22,y-37,x+s*32,y-37,x+s*16,y-13,x+s*22,y-33,x+s*12,y-33]);
        for(let j=0;j<6;j++){const q=clock*.6+j*TAU/6;diamond(g,white,x+C(q)*26,y-42+S(q)*28,2);}
      }else if(a==='titan'){
        ring(g,el,x,y-29,31,36);for(let j=0;j<5;j++){const q=clock*.25+j*TAU/5,xx=x+C(q)*29,yy=y-29+S(q)*34;poly(g,'#8e8275',[xx-4,yy-4,xx+4,yy-5,xx+6,yy+3,xx-3,yy+5]);line(g,white,xx-3,yy,xx+3,yy);}
      }else if(a==='eclipse'){
        oval(g,'#3a294d',x,y-45,27,27);ring(g,el,x,y-45,28,28);for(let j=0;j<5;j++)diamond(g,white,x-22+j*11,y-41-Math.abs(j-2)*6,2);
        for(const s of [-1,1]){poly(g,el,[x+s*8,y-30,x+s*30,y-49,x+s*25,y-23,x+s*13,y-13]);const xx=x+s*27,yy=y-55+S(clock)*3;oval(g,white,xx,yy,3,4);line(g,el,xx,yy+4,xx-s*4,yy+9);}
      }else if(a==='starfall'){
        const points=[];for(let j=0;j<5;j++){const q=clock*.25+j*TAU/5;points.push([x+C(q)*32,y-32+S(q)*32]);}for(let j=0;j<5;j++){line(g,'#c6ac76',...points[j],...points[(j+2)%5]);oval(g,j%2?'#ffc986':el,...points[j],3+j%2,3+j%2);ring(g,white,...points[j],6,2,-.3);}
      }else if(a==='everdawn'){
        for(let j=0;j<5;j++){const q=-Math.PI+(j+1)*Math.PI/6,xx=x+C(q)*23,yy=y-7+S(q)*14;poly(g,j%2?white:'#f2b9ba',[xx,yy-10,xx+7,yy,xx,yy+5,xx-7,yy]);}
        ring(g,'#ffe6b4',x,y-63,18,5);for(let j=0;j<6;j++){const yy=y-8-((clock*13+j*11)%58);rect(g,white,x+(j%2?1:-1)*(18+j%3*4),yy,2,4);}
      }else if(a==='hourglass'){
        ring(g,el,x,y-31,31,35);for(let j=0;j<12;j++){const q=j*TAU/12;line(g,white,x+C(q)*28,y-31+S(q)*32,x+C(q)*31,y-31+S(q)*35);}line(g,el,x,y-31,x+C(clock*.4)*25,y-31+S(clock*.4)*28);
      }else if(a==='tidal'){
        for(const s of [-1,1])for(let j=0;j<3;j++){const yy=y-10-j*12;ring(g,el,x+s*(15+j*4),yy,9,6,0);line(g,white,x+s*18,yy-4,x+s*25,yy-2);}
        for(const s of [-1,1]){line(g,cyan,x+s*5,y-59,x+s*15,y-71,2);line(g,cyan,x+s*11,y-66,x+s*19,y-65,2);diamond(g,white,x+s*15,y-72,2);}
      }else{
        ring(g,'#af88ff',x,y-34,31,35,clock*.2);for(let j=0;j<4;j++){const q=clock*.4+j*TAU/4,xx=x+C(q)*31,yy=y-34+S(q)*34;oval(g,'#29243e',xx,yy,6,6);ring(g,el,xx,yy,7,7);diamond(g,white,xx,yy,2);}
      }
      g.restore();
    }
  }
  function cast(g,ev,hx,ground,z,reduced){
    const age=ev.age||0,ancient=ev.kind==='ancient',c=colors[ev.element]||cyan,u=Math.min(1,age/.8),fade=Math.max(0,1-age/1.1),x=ev.hero?hx:ancient?z.x:hx,y=ev.hero?ground-29:ancient?z.center:ground-24;
    g.save();g.globalAlpha=fade*(reduced?.6:.9);
    if(ev.kind==='boss'&&['titan-warning','titan-arrival'].includes(ev.pattern)){ring(g,c,z.x,z.center,25+u*24,29+u*22);g.restore();return;}
    if(ev.kind==='boss'&&ev.pattern==='titan-enrage'){for(let j=0;j<8;j++){const q=j*TAU/8;line(g,c,z.x+C(q)*15,z.center+S(q)*15,z.x+C(q)*(20+u*38),z.center+S(q)*(20+u*38),2);}g.restore();return;}
    if(ancient){
      const aura=String(ev.pattern||'').replace('ancient_','');
      if(['storm','starfall','sunfire','void','eclipse','worldroot','tidal'].includes(aura)){
        for(let j=0;j<5;j++){const q=j*TAU/5+u*2,xx=x+C(q)*(9+u*29),yy=y+S(q)*(9+u*29);line(g,j%2?white:c,hx+10,ground-35,xx,yy,j%2?1:2);diamond(g,white,xx,yy,3);}
        if(aura==='worldroot')for(let j=0;j<5;j++){const xx=x+(j-2)*9;line(g,'#74b970',xx,z.y,xx+4,z.y-25*(1-u),3);}
        if(aura==='eclipse'||aura==='void'){oval(g,'#332442',x,y,12+u*16,12+u*16);ring(g,cyan,x,y,15+u*18,15+u*18);}
      }else{
        for(let j=0;j<6;j++){const q=j*TAU/6;diamond(g,j%2?white:c,x+C(q)*(12+u*25),y+S(q)*(12+u*25),4);}
        if(aura==='hourglass'){line(g,white,x-9,y-14,x+9,y+14,2);line(g,white,x+9,y-14,x-9,y+14,2);}
      }
      ring(g,c,x,y,8+u*35,(ev.hero?.65:1)*(8+u*35));ring(g,white,x,y,5+u*26,5+u*26);
    }else if(ev.pattern==='titan-cast'){
      // Each Titan's defensive/alternate signature has a different silhouette
      // from its direct attack, even when both share an elemental palette.
      const pat=ev.abilityPattern;
      if(['titan-canopy','titan-mirrors','titan-bastion'].includes(pat)){
        ring(g,c,z.x,z.center,30+u*12,33+u*12);
        for(let j=0;j<6;j++){
          const q=j*TAU/6,xx=z.x+C(q)*35,yy=z.center+S(q)*35;
          if(pat==='titan-canopy'){line(g,'#52936b',z.x,z.y,xx,yy,2);poly(g,c,[xx-7,yy+3,xx-3,yy-6,xx+7,yy-3,xx+2,yy+4]);}
          else if(pat==='titan-mirrors')diamond(g,j%2?white:c,xx,yy,8);
          else{poly(g,c,[xx-6,yy-7,xx+5,yy-7,xx+8,yy+6,xx-4,yy+7]);line(g,white,xx,yy-5,xx+2,yy+5);}
        }
        g.restore();return;
      }
      if(pat==='titan-meteors'){
        for(let j=0;j<3;j++){const xx=hx-26+j*26,yy=(ground-35)*(Math.min(1,u*1.7+j*.08));line(g,c,xx+18,yy-36,xx,yy,4);oval(g,c,xx,yy,9,9);oval(g,white,xx-2,yy-2,4,4);}
        ring(g,c,hx,ground,15+u*45,3+u*9);g.restore();return;
      }
      if(pat==='titan-hurricane'){
        for(let j=0;j<6;j++){const yy=ground-7-j*10,xx=hx+S(u*5+j*.6)*12;ring(g,j%2?white:c,xx,yy,15+j*5,4);}
        g.restore();return;
      }
      if(pat==='titan-soultide'){
        for(let j=0;j<5;j++){const q=(u+j*.16)%1,xx=hx+(z.x-hx)*q,yy=ground-25+(z.center-ground+25)*q+S(q*5+j)*10;oval(g,c,xx,yy,5,7);rect(g,white,xx-2,yy-2,1,2);rect(g,white,xx+2,yy-2,1,2);line(g,c,xx,yy+5,xx-9,yy+10);}
        g.restore();return;
      }
      const element=ev.element;
      if(element==='fire')for(let j=0;j<7;j++){const xx=hx-32+j*11;poly(g,j%2?white:c,[xx,ground,xx-5,ground-13-u*13,xx,ground-31-u*16,xx+5,ground-10,xx+8,ground]);}
      else if(element==='storm'){for(let j=0;j<3;j++){const xx=hx+(j-1)*18;poly(g,white,[xx-6,4,xx+4,4,xx-2,ground-42,xx+7,ground-42,xx-5,ground,xx-1,ground-34,xx-9,ground-34]);}}
      else if(element==='leaf')for(let j=0;j<6;j++){const xx=hx-25+j*10;line(g,c,xx,ground,xx+(j%2?4:-4),ground-22-u*15,3);diamond(g,white,xx,ground-23-u*15,3);}
      else if(element==='frost')for(let j=0;j<6;j++)diamond(g,j%2?white:c,hx+(j-2.5)*12,ground-15-u*28,8);
      else if(element==='stone'){for(let j=0;j<5;j++){const xx=hx+(j-2)*13;poly(g,c,[xx-7,ground,xx-6,ground-12-u*20,xx,ground-19-u*20,xx+5,ground-9-u*20,xx+7,ground]);}}
      else {oval(g,'#30263d',hx,ground-29,14+u*25,14+u*25);ring(g,c,hx,ground-29,18+u*27,18+u*27);}
      ring(g,c,hx,ground,16+u*49,4+u*11);
    }else ring(g,white,z.x,z.center,15+u*40,15+u*40);
    g.restore();
  }
  globalThis.EpicArt={boss,bossAura,ancientAura,cast};
})();
