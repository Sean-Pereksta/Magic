// Authored pixel silhouettes and readable ability geometry for the four human
// expansion heroes. All animation is derived from authoritative combat state.
import { selectTimeWardTarget } from './solstice-runtime.mjs';
const TAU=Math.PI*2;
const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
const INK='#152838',SKIN='#e6b18b',SKIN_LIGHT='#ffdbac';
const THEMES={
  copper:{main:'#c97a45',light:'#f4d7a1',dark:'#744a37',accent:'#8be8e5'},
  tide:{main:'#3fb8ac',light:'#d0f2e3',dark:'#153c53',accent:'#b9a36b'},
  ivory:{main:'#f4efdc',light:'#ffffff',dark:'#32435b',accent:'#806e9a'},
  time:{main:'#e7b951',light:'#f4dea0',dark:'#272b48',accent:'#9d91c4'},
};
export const EXPANSION_HEROES=Object.freeze({rivet:'copper',brinehook:'tide',vela:'ivory',solstice:'time'});
const ABILITIES=new Set(['screwshot','bolt-sentry','shock-mine','overclock','anchorline','sea-legs','undertow','captains-wake','split-shot','sidestep','steady-aim','silver-volley','hourhand','time-ward','unwind','borrowed-seconds']);
const px=(c,color,x,y,w=2,h=w)=>{c.fillStyle=color;c.fillRect(Math.round(x),Math.round(y),Math.max(1,Math.round(w)),Math.max(1,Math.round(h)));};
function line(c,color,x,y,xx,yy,width=2){c.strokeStyle=color;c.lineWidth=width;c.beginPath();c.moveTo(Math.round(x),Math.round(y));c.lineTo(Math.round(xx),Math.round(yy));c.stroke();}
function ring(c,color,x,y,rx,ry=rx,width=2,start=0,end=TAU,fill=false){
  c.beginPath();const count=Math.max(4,Math.ceil((end-start)/TAU*24));
  for(let k=0;k<=count;k++){const a=start+(end-start)*k/count,xx=Math.round(x+Math.cos(a)*rx),yy=Math.round(y+Math.sin(a)*ry);k?c.lineTo(xx,yy):c.moveTo(xx,yy);}
  if(fill){c.closePath();c.fillStyle=color;c.fill();}else{c.strokeStyle=color;c.lineWidth=width;c.stroke();}
}
function take(budget,channel='accent'){
  if(!budget)return true;
  const key=channel+'Remaining';if(budget.remaining<1||(Number.isFinite(budget[key])&&budget[key]<1))return false;
  budget.remaining--;if(Number.isFinite(budget[key]))budget[key]--;return true;
}
function tintColor(color,tint){
  if(!/^#[0-9a-f]{6}$/i.test(tint||''))return color;
  return '#'+[1,3,5].map(i=>Math.round(parseInt(color.slice(i,i+2),16)*.55+parseInt(tint.slice(i,i+2),16)*.45).toString(16).padStart(2,'0')).join('');
}
function anchor(c,x,y,size,color='#b9a36b',light='#d0f2e3'){
  c.save();c.translate(x,y);c.scale(size,size);
  line(c,INK,0,-12,0,10,6);line(c,color,0,-12,0,9,3);
  ring(c,light,0,-13,3,3,2);line(c,light,-6,-6,6,-6,2);
  line(c,INK,-10,2,-7,9,6);line(c,INK,-7,9,0,12,6);line(c,INK,0,12,7,9,6);line(c,INK,7,9,10,2,6);
  line(c,color,-10,2,-7,9,3);line(c,light,-7,9,0,12,3);line(c,light,0,12,7,9,3);line(c,color,7,9,10,2,3);
  px(c,light,-12,0,5,3);px(c,light,8,0,5,3);c.restore();
}
function hourglass(c,x,y,size,sand=.5,colors=THEMES.time){
  c.save();c.translate(x,y);c.scale(size,size);
  px(c,INK,-8,-12,16,25);px(c,colors.main,-8,-12,16,3);px(c,colors.light,-6,-12,11,1);
  px(c,colors.main,-8,10,16,3);px(c,colors.light,-6,10,11,1);
  px(c,colors.dark,-6,-9,12,19);line(c,colors.accent,-6,-9,5,9,2);line(c,colors.light,6,-9,-5,9,2);
  px(c,colors.light,-4,-7,8,Math.max(1,5*sand));px(c,colors.main,-4,6-Math.round(5*(1-sand)),8,Math.max(1,5*(1-sand)));
  px(c,colors.light,0,-1,1,5);c.restore();
}
function gear(c,x,y,r,color,light,rotation=0){
  for(let k=0;k<8;k++){const a=rotation+k*TAU/8;px(c,color,x+Math.cos(a)*r-3,y+Math.sin(a)*r-3,6,6);}
  ring(c,color,x,y,r*.82,r*.82,4);ring(c,light,x,y,r*.37,r*.37,2);
}
function pistol(c,x,y,a,color,light,recoil=0){
  c.save();c.translate(x,y);c.rotate(a);
  px(c,INK,-3-recoil,-4,20,8);px(c,color,-2-recoil,-3,18,5);px(c,light,-2-recoil,-3,15,1);
  px(c,'#647189',9-recoil,1,7,2);px(c,INK,-3-recoil,0,6,9);px(c,'#806e9a',-2-recoil,2,4,6);
  px(c,'#f4d7a1',0-recoil,-5,4,2);c.restore();
}

/** Local coordinates match the established hero body's 40 x 44 footprint. */
export function drawExpansionHero(c,id,{frame=0,stage=0,direction=0,back=false,casting=false,variant='',tint=null}={}){
  const theme=EXPANSION_HEROES[id];if(!theme)return false;
  const base=THEMES[theme],p={...base,main:tintColor(base.main,tint),dark:tintColor(base.dark,tint)};
  const walk=stage?0:[0,2,0,-2][frame%4],bob=stage?0:frame%2;
  const angle=direction*Math.PI/4,aim=Math.atan2(Math.sin(angle),Math.abs(Math.cos(angle)));
  c.save();c.translate(0,-bob);
  if(id==='rivet'){
    // Low square apron, one oversized shoulder toolbox, small welding visor.
    px(c,INK,8,16,25,23);px(c,p.dark,10,17,21,20);
    px(c,INK,10,34+walk,9,7);px(c,INK,24,34-walk,9,7);
    px(c,'#53636c',11,34+walk,6,3);px(c,'#53636c',25,34-walk,6,3);
    px(c,p.light,12,20,17,15);px(c,'#d5ad76',22,20,7,15);px(c,p.main,15,26,10,6);px(c,'#744a37',16,27,8,2);
    px(c,INK,2,12,12,18);px(c,p.main,3,13,11,15);px(c,p.light,3,13,10,3);px(c,'#744a37',3,24,10,3);px(c,p.accent,6,18,4,4);
    px(c,INK,11,3,20,17);px(c,p.main,12,4,18,15);px(c,p.light,13,4,11,3);px(c,'#744a37',27,7,3,11);
    px(c,INK,14,9,13,5);if(!back){px(c,p.accent,15,10,11,2);px(c,'#ecffef',15,10,4,1);}else px(c,p.dark,15,9,10,5);
    px(c,p.light,15,16,10,2);px(c,INK,9,10,4,6);px(c,'#8e9c99',10,11,2,4);
    const build=variant==='build'&&stage>0,armY=build?28:23,recoil=stage===2?3:0;
    px(c,p.main,27,18,7,9);px(c,SKIN_LIGHT,29,25,5,4);
    c.save();c.translate(31,armY);c.rotate(build?.6:stage===1?aim-.3:stage?aim:Math.PI*.18);
    px(c,INK,-2-recoil,-5,19,10);px(c,p.main,-recoil,-4,15,8);px(c,'#87918c',9-recoil,-3,9,5);px(c,p.accent,2-recoil,-3,7,2);
    px(c,p.light,3-recoil,3,5,2);if(build){line(c,p.accent,17,0,21,4,2);px(c,'#f7f8c4',21,4,3,3);}c.restore();
    if(build){px(c,p.main,18,34,10,3);px(c,p.accent,23,32,3,3);}
  }else if(id==='brinehook'){
    // Triangular captain's coat and sea-glass beard, with a real anchor head.
    px(c,INK,8,15,27,17);px(c,INK,5,28,32,12);px(c,p.dark,9,16,25,20);px(c,p.dark,6,29,30,9);
    px(c,p.main,9,17,8,19);px(c,p.main,29,17,5,18);px(c,p.accent,17,18,3,18);px(c,p.accent,26,18,3,18);
    px(c,p.light,8,16,10,3);px(c,p.light,27,16,8,3);px(c,p.accent,8,28,28,3);
    px(c,INK,10,36+walk,10,6);px(c,INK,25,36-walk,11,6);px(c,p.accent,11,36+walk,8,2);px(c,p.accent,26,36-walk,8,2);
    px(c,INK,12,3,20,16);px(c,SKIN,14,7,15,10);px(c,SKIN_LIGHT,15,7,8,6);
    px(c,p.dark,9,4,27,5);px(c,p.dark,14,0,16,7);px(c,p.main,14,2,15,3);px(c,p.accent,18,0,9,3);px(c,p.light,22,1,3,3);
    if(!back){px(c,INK,24,10,5,2);px(c,p.light,24,10,2,1);px(c,p.main,14,13,16,7);px(c,'#82d2bd',16,15,12,5);px(c,p.dark,19,20,7,3);}else px(c,p.dark,14,8,16,13);
    px(c,p.main,2,18,10,9);px(c,SKIN_LIGHT,3,26,6,5);
    const swing=stage===1?-1.2:stage===2?aim-1.35:stage===3?aim-.5:.2;
    c.save();c.translate(35,24);c.rotate(swing);line(c,p.accent,-2,-2,2,15,2);anchor(c,7,15,.82,p.accent,p.light);c.restore();
    px(c,SKIN_LIGHT,32,23,7,5);
  }else if(id==='vela'){
    const reload=stage===5,volley=stage===6,wide=stage===1||stage===2||volley;
    // A narrow ivory half-cape leaves both long pistols visible.
    px(c,INK,9,12,20,21);px(c,p.dark,14,17,13,17);px(c,p.main,7,13,11,23);
    px(c,'#c9c6cf',7,29,8,9);px(c,p.light,7,15,3,19);px(c,p.accent,12,15,6,4);
    px(c,INK,13-(wide?2:0),31+walk,6,11);px(c,INK,22+(wide?3:0),31-walk,6,11);
    px(c,p.dark,14-(wide?2:0),32+walk,4,7);px(c,p.dark,23+(wide?3:0),32-walk,4,7);px(c,p.main,13-(wide?2:0),40+walk,7,2);px(c,p.main,22+(wide?3:0),40-walk,7,2);
    px(c,p.accent,17,22,10,9);px(c,p.main,18,23,3,7);px(c,'#d8bb85',16,31,12,3);
    px(c,INK,12,2,17,15);px(c,SKIN,14,6,13,10);px(c,SKIN_LIGHT,15,6,7,7);
    px(c,'#a7b7c4',11,3,18,6);px(c,p.main,13,0,17,5);px(c,p.light,15,0,11,2);px(c,p.main,10,4,5,9);px(c,'#c6cfda',8,5,5,5);
    if(!back){px(c,p.dark,22,9,5,2);px(c,'#b4edf1',23,9,2,1);}else px(c,'#c6cfda',14,6,14,10);
    const recoil=stage===2||volley&&frame%2===1?3:0,offhand=variant.includes('offhand');
    const rightAngle=reload?-Math.PI/2+frame*.12:stage?aim:.22,leftAngle=reload?Math.PI/2-frame*.12:volley?aim:stage===2?aim-.1:Math.PI*.85;
    px(c,p.main,25,17,6,9);px(c,SKIN_LIGHT,29,23,4,3);px(c,p.main,9,19,5,9);px(c,SKIN_LIGHT,8,26,4,3);
    pistol(c,31,24,rightAngle,p.main,p.light,offhand?0:recoil);pistol(c,9,26,leftAngle,p.main,p.light,offhand?recoil:0);
    if(variant.includes('aim')){px(c,'#ffffff',31,19,8,2);px(c,'#d7ebff',7,22,5,2);}
    if(reload){px(c,p.accent,18,29,4,7);px(c,p.main,18,29,4,2);px(c,'#d6c69d',25,31+frame,2,3);}
  }else{
    // Tapered midnight robe, segmented shoulder dial and floating hourglass.
    const hover=stage?0:[0,-1,-2,-1][frame%4],castLift=stage===1?2:stage===2?9:stage===3?5:0;
    c.translate(0,hover);px(c,INK,11,13,18,17);px(c,INK,8,28,24,13);px(c,p.dark,12,14,16,16);px(c,p.dark,9,29,22,11);
    px(c,p.accent,12,21,5,16);px(c,p.main,22,18,3,20);px(c,p.main,8,39,25,3);px(c,p.light,10,39,8,1);px(c,p.dark,12,39,3,4);px(c,p.dark,26,39,3,4);
    px(c,p.main,8,13,8,7);px(c,p.light,8,13,7,2);px(c,p.main,6,16,5,7);px(c,p.accent,6,21,4,4);
    px(c,INK,12,0,17,17);px(c,p.dark,13,1,15,15);px(c,p.accent,14,2,3,10);px(c,p.main,14,1,13,2);
    if(!back){px(c,SKIN,17,6,9,10);px(c,SKIN_LIGHT,17,6,6,7);px(c,p.dark,22,10,4,2);px(c,p.light,22,10,2,1);}else px(c,p.accent,17,3,10,12);
    const hx=stage===2?35:30,hy=24-castLift;
    px(c,p.main,9,20,5,7);px(c,SKIN_LIGHT,12,25-castLift*.3,6,3);px(c,p.accent,27,19,5,7);px(c,SKIN_LIGHT,32,25-castLift*.3,5,3);
    hourglass(c,hx,hy,.57,stage===1?.9:stage===2?.15:.5,p);
    if(casting&&stage){px(c,p.light,hx-11,hy-3,2,2);px(c,p.accent,hx+10,hy+3,2,2);}
  }
  c.restore();return true;
}

/** Devices are low machines, not scaled-down hero sprites. */
export function drawDevice(c,u,time,budget,opacity=1){
  const mine=u.subtype==='mine',p=THEMES.copper,team=u.team===1?'#ff8f87':'#67dfef';
  const assembly=mine?1:clamp(1-((u.assembleUntil||0)-time)/.6,0,1),armed=time>=(u.armAt||0),overclock=(u.overclockUntil||0)>time;
  c.save();c.globalAlpha=opacity;ring(c,team,u.x,u.y+2,mine?14:21,mine?7:10,2);
  c.translate(Math.round(u.x),Math.round(u.y));
  if(mine){
    px(c,INK,-17,-8,34,10);px(c,'#744a37',-15,-8,30,8);px(c,p.main,-12,-11,24,6);px(c,p.light,-10,-11,8,2);
    px(c,INK,-6,-20,12,13);px(c,'#47646a',-4,-18,8,10);
    for(let k=0;k<3;k++)px(c,armed?p.accent:p.light,-7,-18+k*4,14,2);
    px(c,armed?'#dffff6':'#edb971',-2,-24,4,4);
    if(!armed){const q=clamp(1-(u.armAt-time)/.8,0,1);ring(c,p.light,0,0,25,16,3,-Math.PI/2,-Math.PI/2+q*TAU);}
    else {px(c,team,-18,-4,3,3);px(c,team,15,-4,3,3);}
  }else{
    for(const side of [-1,1]){line(c,INK,side*5,-12,side*20,2,6);line(c,'#8b9895',side*5,-12,side*20,2,3);px(c,INK,side*20-4,1,10,4);}
    line(c,INK,0,-10,0,7,6);px(c,'#829898',-2,-8,4,14);
    c.save();c.translate(0,budget?.reducedMotion?0:Math.round((1-Math.floor(assembly*4)/4)*20));c.globalAlpha*=.4+.6*assembly;
    px(c,INK,-15,-29,30,18);px(c,p.main,-13,-27,26,14);px(c,p.light,-12,-27,14,3);px(c,p.dark,-10,-19,22,6);
    gear(c,-6,-18,5,p.main,p.light);px(c,p.accent,4,-23,7,4);
    const recoil=u.attackAnim>0?3:0,a=Math.atan2(Math.sin(u.angle||0),Math.abs(Math.cos(u.angle||0)));
    c.save();c.translate(0,-23);if(Math.cos(u.angle||0)<0)c.scale(-1,1);c.rotate(a);
    for(const y of [-6,3]){px(c,INK,6-recoil,y,28,7);px(c,'#859491',6-recoil,y+1,25,4);px(c,p.light,8-recoil,y+1,17,1);px(c,p.main,29-recoil,y,5,7);}c.restore();
    if(overclock)for(let k=0;k<3;k++){px(c,p.accent,-13+k*8,-34,4,7);px(c,'#f2fff0',-12+k*8,-34,2,3);}
    c.restore();
    if(assembly<1){ring(c,p.accent,0,1,25,12,2);px(c,p.accent,-18,-24-assembly*15,4,4);px(c,p.light,16,-12-assembly*20,3,3);}
  }
  if(overclock&&!budget?.reducedMotion)for(let k=0;k<3;k++){
    if(!take(budget))break;const phase=(time*2+k*.31)%1;px(c,k%2?p.accent:p.light,-18+k*15,-28-phase*20,3,4);
  }
  c.restore();
}

/** Six readable chambers remain informative while the sprite reloads. */
export function drawMagazine(c,u,x,y,time){
  if(u.heroId!=='vela')return false;
  const m=u.magazine||{rounds:6},reload=m.reloadRemaining>0,q=reload?clamp(1-m.reloadRemaining/Math.max(.01,m.reloadDuration||1.4),0,1):0;
  c.save();px(c,'#172a39',x-2,y-2,43,10);
  for(let k=0;k<6;k++){
    const full=reload?q*6>=k+1:k<(m.rounds??6),color=full?'#f4efdc':'#495568';
    px(c,color,x+k*7,y,5,6);if(full)px(c,'#b5cce4',x+k*7,y,5,2);
    else if(reload&&q*6>k)px(c,'#d3c9e7',x+k*7,y+5,5,1);
  }
  if((u.steadyAimUntil||u.steadyAim?.endsAt||0)>time||u.status?.steadyAim){
    line(c,'#e8e7f4',x-4,y-4,x+7,y-4);line(c,'#e8e7f4',x-4,y-4,x-4,y+8);
    line(c,'#e8e7f4',x+32,y-4,x+43,y-4);line(c,'#e8e7f4',x+43,y-4,x+43,y+8);
  }
  c.restore();return true;
}

export function drawExpansionUnitAccents(c,u,time,budget){
  if(u.heroId==='vela'&&u.visualAction?.kind==='attack'&&time-u.visualAction.startedAt>=0&&time-u.visualAction.startedAt<.09&&take(budget)){
    const a=u.visualAction.angle||0,big=!!u.visualAction.sixthRound,size=big?10:6,x=u.x+Math.cos(a)*42,y=u.y-30+Math.sin(a)*20;
    c.save();c.globalAlpha=.9;px(c,big?'#cfdeff':'#f4efdc',x-size/2,y-size/2,size,size);px(c,'#ffffff',x-2,y-2,4,4);c.restore();
  }
  if(u.timeWard&&u.timeWard.expiresAt>time&&u.hp>0){
    const w=u.timeWard,remaining=clamp((w.expiresAt-time)/Math.max(.1,w.expiresAt-w.startedAt),0,1);
    c.save();c.globalAlpha=.9;hourglass(c,u.x+26,u.y-45,.48,remaining);
    for(let k=0;k<3;k++)px(c,remaining>k/3?'#f4dea0':'#6a5d75',u.x+18+k*6,u.y-30,4,3);c.restore();
  }
  if(u.heroId==='brinehook'&&u.shield>0&&(u.shieldVisual?.theme==='tide'||u.shieldLayers?.some(s=>s.theme==='tide'))){
    c.save();c.globalAlpha=.55;
    for(let k=0;k<6;k++){if(!take(budget))break;const a=k*TAU/6,phase=budget?.reducedMotion?0:Math.floor(time*4)%3;px(c,THEMES.tide.light,u.x+Math.cos(a)*21-3,u.y+Math.sin(a)*8-phase,7,3);}c.restore();
  }
  if(u.heroId==='rivet'&&(u.overclockUntil||0)>time){
    c.save();c.globalAlpha=.7;gear(c,u.x,u.y-24,28,THEMES.copper.main,THEMES.copper.accent,budget?.reducedMotion?0:Math.floor(time*4)*Math.PI/8);c.restore();
  }
}

/** Icons are drawn inside the existing 32px icon coordinate system. */
export function drawExpansionIcon(c,id){
  if(!ABILITIES.has(id))return false;
  const group=['screwshot','bolt-sentry','shock-mine','overclock'].includes(id)?'copper':['anchorline','sea-legs','undertow','captains-wake'].includes(id)?'tide':['split-shot','sidestep','steady-aim','silver-volley'].includes(id)?'ivory':'time',p=THEMES[group];
  if(id==='screwshot'){
    c.save();c.translate(16,16);c.rotate(-Math.PI/4);px(c,INK,-5,-13,10,26);px(c,p.main,-3,-10,6,22);px(c,p.light,-3,-10,2,22);
    for(let k=0;k<4;k++)line(c,p.accent,-6,-7+k*5,5,-3+k*5,2);px(c,p.light,-8,-13,16,5);c.restore();
  }else if(id==='bolt-sentry'){
    for(const side of [-1,1])line(c,p.light,16,20,16+side*10,28,3);line(c,p.light,16,20,16,30,3);
    px(c,p.main,6,10,19,12);px(c,p.light,7,10,11,3);px(c,p.accent,10,16,5,3);px(c,'#839493',19,8,10,4);px(c,'#c2c9a6',19,15,10,4);
  }else if(id==='shock-mine'){
    px(c,p.main,4,23,24,5);px(c,p.light,7,21,18,3);px(c,p.dark,11,6,10,16);
    for(let k=0;k<4;k++)px(c,k%2?p.light:p.accent,8,6+k*4,16,2);line(c,p.accent,5,8,2,13,2);line(c,p.accent,27,15,30,10,2);
  }else if(id==='overclock'){gear(c,16,16,11,p.main,p.light);px(c,p.accent,15,7,3,10);px(c,p.accent,11,15,8,3);}
  else if(id==='anchorline'){line(c,p.accent,6,4,27,5,2);line(c,p.accent,27,5,28,19,2);anchor(c,15,16,.85,p.accent,p.light);}
  else if(id==='sea-legs'){px(c,p.dark,11,4,10,17);px(c,p.main,12,6,8,14);px(c,p.light,10,18,17,8);px(c,p.main,6,24,22,4);for(let k=0;k<4;k++)px(c,p.light,3+k*7,26+(k%2)*2,6,3);}
  else if(id==='undertow'){for(let k=0;k<3;k++)ring(c,k%2?p.light:p.main,16,16,11-k*3,9-k*2,2,k*.7,Math.PI*1.7+k*.7);px(c,p.light,14,14,4,4);line(c,p.light,3,13,7,17,2);line(c,p.light,29,19,25,15,2);}
  else if(id==='captains-wake'){px(c,p.accent,14,3,4,11);px(c,p.light,18,5,10,6);c.fillStyle=p.dark;c.beginPath();c.moveTo(4,15);c.lineTo(28,15);c.lineTo(21,24);c.lineTo(10,24);c.closePath();c.fill();for(let k=0;k<3;k++){line(c,p.main,3,23+k*3,9,21+k*3,2);line(c,p.light,9,21+k*3,18,24+k*3,2);line(c,p.main,18,24+k*3,28,21+k*3,2);}}
  else if(id==='split-shot'){for(const side of [-1,1]){c.save();c.translate(16+side*5,16);c.rotate(side*.28);px(c,p.main,-3,-10,6,18);px(c,p.light,-2,-12,4,4);px(c,p.accent,-3,7,6,4);c.restore();}line(c,p.light,16,26,11,30,2);line(c,p.light,16,26,21,30,2);}
  else if(id==='sidestep'){px(c,p.main,16,5,7,15);px(c,p.light,15,17,13,7);px(c,p.accent,14,24,14,3);for(let k=0;k<3;k++)line(c,p.main,3,26-k*6,12,18-k*5,2);}
  else if(id==='steady-aim'){ring(c,p.main,16,16,9,9,2);for(const [dx,dy]of[[1,0],[-1,0],[0,1],[0,-1]])line(c,p.light,16+dx*6,16+dy*6,16+dx*14,16+dy*14,2);px(c,p.accent,14,14,4,4);}
  else if(id==='silver-volley'){for(let k=0;k<6;k++){const x=4+k*5;px(c,p.main,x,7+(k%2)*3,3,16);px(c,p.light,x,4+(k%2)*3,3,3);px(c,p.accent,x,24+(k%2)*2,3,4);}}
  else if(id==='hourhand'){ring(c,p.accent,16,16,11,11,2);line(c,p.light,16,16,22,5,3);line(c,p.main,16,16,10,14,3);px(c,p.light,14,14,4,4);px(c,p.main,21,3,4,5);}
  else if(id==='time-ward')hourglass(c,16,16,.95,.55,p);
  else if(id==='unwind'){px(c,p.accent,14,10,7,10);px(c,p.light,13,18,13,6);ring(c,p.main,16,16,12,12,3,.2,Math.PI*1.8);line(c,p.light,7,4,7,11,3);line(c,p.light,7,4,14,4,3);}
  else {for(const [x,y]of[[10,10],[23,13],[15,25]]){ring(c,p.main,x,y,6,6,2);line(c,p.light,x,y,x,y-4,1);line(c,p.light,x,y,x+3,y+1,1);}}
  return true;
}

function chevron(c,color,x,y,angle,size=9){
  line(c,color,x-Math.cos(angle-.65)*size,y-Math.sin(angle-.65)*size,x,y,2);
  line(c,color,x-Math.cos(angle+.65)*size,y-Math.sin(angle+.65)*size,x,y,2);
}
function inward(c,x,y,r,color,phase=0){
  for(let k=0;k<6;k++){const a=k*TAU/6,rr=r*(.78-phase*.15);chevron(c,color,x+Math.cos(a)*rr,y+Math.sin(a)*rr,a+Math.PI,10);}
}
function lane(c,x,y,angle,range,width,color,alpha=.15){
  c.save();c.translate(x,y);c.rotate(angle);c.globalAlpha=alpha;px(c,color,0,-width,range,width*2);
  c.globalAlpha=.8;c.strokeStyle=color;c.lineWidth=2;c.strokeRect(0,-width,range,width*2);chevron(c,color,range,0,0,12);c.restore();
}
function clockFace(c,x,y,r,color,progress=0,reverse=false){
  ring(c,color,x,y,r,r,2);
  for(let k=0;k<12;k++){const a=k*TAU/12-Math.PI/2;line(c,color,x+Math.cos(a)*r*.86,y+Math.sin(a)*r*.86,x+Math.cos(a)*r,y+Math.sin(a)*r,k%3?1:3);}
  const a=-Math.PI/2+Math.round(progress*12)/12*TAU*(reverse?-1:1);
  line(c,color,x,y,x+Math.cos(a)*r*.7,y+Math.sin(a)*r*.7,3);line(c,color,x,y,x+Math.cos(a+1.5)*r*.4,y+Math.sin(a+1.5)*r*.4,2);
}

/** Supplements the engine's exact warning shape with its mechanical meaning. */
export function drawExpansionWarning(c,z,progress,time,budget){
  const id=z.kind||z.type||z.abilityId,color=z.team===0?'#b9f6ed':'#ffe0a3';
  if(!['anchorline','undertow','captains-wake','silver-volley','borrowed-seconds','glass-echo'].includes(id))return false;
  c.save();c.globalAlpha=.85;
  if(id==='undertow')inward(c,z.x,z.y,z.r||110,color,progress);
  else if(id==='captains-wake'){
    c.translate(z.x,z.y);c.rotate(z.angle||0);const width=z.width||50,range=z.range||360;
    for(let k=1;k<=3;k++){const x=range*k/4;chevron(c,color,x,0,0,12);for(const side of [-1,1])chevron(c,color,x,side*width*.4,-side*Math.PI/2,10);}
  }else if(id==='borrowed-seconds'||id==='glass-echo')clockFace(c,z.x,z.y,(z.r||60)*.88,color,progress);
  else {
    c.translate(z.x,z.y);c.rotate(z.angle||0);const range=z.range||440;
    if(id==='anchorline'){for(let k=1;k<=4;k++)px(c,color,range*k/5,-2,9,3);anchor(c,range,0,.55,color,'#f1fff0');}
    else for(let k=0;k<6;k++)px(c,color,20+k*15,-(z.width||24)-7,9,3);
  }
  c.restore();return true;
}

/** Exact skill previews, including both Split Shot lanes and ward selection. */
export function drawExpansionAim(c,game,u,a,{angle,tx,ty,range,color}){
  if(!ABILITIES.has(a.id))return false;
  const id=a.id,r=a.radius||60;
  c.save();c.globalAlpha=.8;
  if(id==='split-shot')for(const side of [-1,1])lane(c,u.x,u.y,angle+side*.055,range,a.radius||10,color,.12);
  else if(id==='captains-wake'){
    const width=a.width?a.width/2:r||50;lane(c,u.x,u.y,angle,range,width,color);
    c.translate(u.x,u.y);c.rotate(angle);
    for(let k=1;k<=3;k++)for(const side of [-1,1])chevron(c,color,range*k/4,side*width*.4,-side*Math.PI/2,10);
  }else if(id==='time-ward'){
    ring(c,color,tx,ty,75,75,2);
    const target=game.getAbility?selectTimeWardTarget(game,u,{x:tx,y:ty}):null;
    if(target){c.globalAlpha=.18;ring(c,color,target.x,target.y,26,16,2,0,TAU,true);c.globalAlpha=1;ring(c,'#f4dea0',target.x,target.y,27,17,3);hourglass(c,target.x,target.y-52,.6);}
    else hourglass(c,tx,ty,.5);
  }else if(['screwshot','anchorline','silver-volley','hourhand'].includes(id))lane(c,u.x,u.y,angle,range,id==='anchorline'?18:id==='silver-volley'?24:r,color);
  else if(id==='sidestep'){
    const reach=Math.hypot(tx-u.x,ty-u.y);lane(c,u.x,u.y,angle,reach,14,color,.1);ring(c,color,tx,ty,18,10,2);
  }else if(id==='bolt-sentry'||id==='shock-mine'){
    ring(c,color,tx,ty,id==='bolt-sentry'?25:20,16,2);c.save();c.translate(tx-16,ty-32);drawExpansionIcon(c,id);c.restore();
    if(id==='shock-mine'){c.globalAlpha=.4;ring(c,color,tx,ty,a.triggerRadius||65,a.triggerRadius||65,1);}
  }else if(id==='undertow'){
    c.globalAlpha=.1;ring(c,color,tx,ty,r,r,1,0,TAU,true);c.globalAlpha=.8;ring(c,color,tx,ty,r,r,2);inward(c,tx,ty,r,color);
  }else if(id==='borrowed-seconds')clockFace(c,tx,ty,r,color,.25);
  else if(id==='unwind'){ring(c,color,tx,ty,r,r,2);for(let k=0;k<4;k++){const aa=k*TAU/4;chevron(c,color,tx+Math.cos(aa)*r*.7,ty+Math.sin(aa)*r*.7,aa-Math.PI/2,12);}}
  else if(id==='overclock'){ring(c,color,u.x,u.y,range||300,range||300,2);gear(c,u.x,u.y-25,20,THEMES.copper.main,THEMES.copper.accent);}
  else if(id==='sea-legs'){ring(c,color,u.x,u.y,31,20,3);for(let k=0;k<4;k++)px(c,THEMES.tide.light,u.x-25+k*15,u.y+8,10,3);}
  else if(id==='steady-aim'){ring(c,color,u.x,u.y,30,30,2);for(const side of [-1,1]){line(c,color,u.x+side*20,u.y-20,u.x+side*35,u.y-20);line(c,color,u.x+side*35,u.y-20,u.x+side*35,u.y-6);}}
  c.restore();return true;
}

/** Projectile heads are semantic silhouettes rather than recolored arrows. */
export function drawExpansionProjectile(c,p,time,budget,game){
  const id=p.abilityId||p.type,theme=p.theme;
  const screw=id==='screwshot'||theme==='copper',hook=id==='anchorline',bullet=['silver-bullet','split-shot','silver-volley'].includes(id)||theme==='ivory',hand=id==='hourhand'||theme==='time';
  if(!screw&&!hook&&!bullet&&!hand)return false;
  const a=p.angle??Math.atan2(p.vy||0,p.vx||1),colors=THEMES[theme]||THEMES[screw?'copper':hook?'tide':bullet?'ivory':'time'];
  c.save();
  if(hook&&Number.isFinite(p.fromX)&&Number.isFinite(p.fromY)){
    const owner=game?.units?.find(u=>u.id===p.sourceId||u.id===p.ownerId),team=game?.player?.team??0;
    if(!game||owner&&(!game.canSee||game.canSee(team,owner))){line(c,'#665b47',p.fromX,p.fromY-8,p.x,p.y,4);line(c,colors.accent,p.fromX,p.fromY-8,p.x,p.y,2);}
  }
  c.translate(p.x,p.y);c.rotate(a);
  if(screw){
    px(c,INK,-15,-5,22,10);px(c,colors.main,-13,-3,19,6);px(c,colors.light,-13,-3,17,2);
    const phase=budget?.reducedMotion?0:Math.floor(time*18)%3;
    for(let k=0;k<3;k++)line(c,colors.accent||'#8be8e5',-10+k*5,-4+phase,-7+k*5,3,2);px(c,colors.light,4,-6,4,12);
  }else if(hook){c.rotate(-Math.PI/2);anchor(c,0,0,1.05,colors.accent,colors.light);}
  else if(bullet){
    const big=p.sixthRound?1.45:1;c.scale(big,big);px(c,INK,-11,-4,19,8);px(c,colors.main,-10,-2,15,4);px(c,colors.light,3,-3,5,6);px(c,colors.accent||'#806e9a',-12,-2,4,4);
    if(p.sixthRound){px(c,'#ffffff',-3,-5,5,2);px(c,'#c9e8ff',-3,4,5,2);}
  }else{
    line(c,colors.main,-17,0,8,0,4);line(c,colors.light,-14,-1,7,-1,1);
    c.rotate(budget?.reducedMotion?0:Math.floor(time*8)*Math.PI/4);line(c,colors.light,0,0,7,-9,3);line(c,colors.accent||'#9d91c4',0,0,-7,-3,3);px(c,colors.main,-3,-3,6,6);
  }
  c.restore();return true;
}

const EXTRA_EFFECTS=new Set(['anchor-cable','reload','perfect-timing','time-return','glass-echo','time-sand','rough-current','repair']);
export function drawExpansionEffect(c,e,time,ground,budget){
  const id=ABILITIES.has(e.type)||EXTRA_EFFECTS.has(e.type)?e.type:ABILITIES.has(e.abilityId)||EXTRA_EFFECTS.has(e.abilityId)?e.abilityId:'';
  if(!id)return false;
  const t=clamp(Number.isFinite(e.createdAt)?(time-e.createdAt)/Math.max(.01,e.maxLife||.6):1-(e.life||0)/Math.max(.01,e.maxLife||.6),0,1);
  if(t>=1)return true;
  const colors=THEMES[e.theme]||THEMES[['overclock','screwshot','shock-mine','bolt-sentry','repair'].includes(id)?'copper':['anchor-cable','anchorline','undertow','captains-wake','sea-legs','rough-current'].includes(id)?'tide':['reload','sidestep','split-shot','silver-volley','steady-aim'].includes(id)?'ivory':'time'];
  const r=e.r||e.radius||60,fade=Math.min(1,(1-t)*3),reduced=budget?.reducedMotion;
  c.save();
  if(id==='anchor-cable'||id==='anchorline'){
    if(!ground&&Number.isFinite(e.x2)&&Number.isFinite(e.y2)){
      c.globalAlpha=(1-t)*.8;line(c,'#665b47',e.x,e.y-8,e.x2,e.y2-8,5);line(c,colors.accent,e.x,e.y-8,e.x2,e.y2-8,2);
      anchor(c,e.x2,e.y2-8,.7,colors.accent,colors.light);
    }
  }else if(id==='undertow'||id==='rough-current'){
    if(ground){
      c.globalAlpha=.15*fade;ring(c,colors.main,e.x,e.y,r,r,1,0,TAU,true);
      c.globalAlpha=.75*fade;ring(c,colors.main,e.x,e.y,r,r,3);inward(c,e.x,e.y,r,colors.light,reduced?0:t);
      for(let k=0;k<3;k++){const rr=r*(.22+((k/3+(reduced?0:t))%1)*.65);ring(c,colors.light,e.x,e.y,rr,rr*.75,2,k,Math.PI*1.2+k);}
    }else if(id==='undertow')for(let k=0;k<12;k++){
      if(!take(budget))break;const a=k*TAU/12,rr=r*(1-t);c.globalAlpha=(1-t)*.8;px(c,k%2?colors.light:colors.main,e.x+Math.cos(a)*rr,e.y+Math.sin(a)*rr-(reduced?0:Math.sin(t*Math.PI)*24),4,7);
    }
  }else if(id==='captains-wake'){
    const range=e.range||360,width=e.width||50,angle=e.angle||0;
    c.translate(e.x,e.y);c.rotate(angle);
    if(ground){c.globalAlpha=.15*fade;px(c,colors.main,0,-width,range,width*2);}
    else{
      const front=range*t;
      // The simulation resolves the warned rectangle at launch. Show foam
      // across that whole impact immediately; the travelling crest is recovery.
      c.globalAlpha=.65*(1-t);
      for(let band=1;band<=3;band++)for(let k=0;k<5;k++)px(c,colors.light,range*band/3-12-(k%2)*5,-width+k*width/2,14,4);
      c.globalAlpha=fade;
      for(let k=0;k<9;k++){
        const y=-width+k*width/4,crest=(k%3)*5;
        px(c,colors.dark,front-22-crest,y-7,25,13);px(c,colors.main,front-15-crest,y-7,23,10);px(c,colors.light,front-8-crest,y-8,14,4);
        if(!reduced&&take(budget))px(c,colors.light,front+9+(k%2)*5,y-10,3,4);
      }
    }
  }else if(id==='overclock'){
    if(ground){c.globalAlpha=(1-t)*.65;gear(c,e.x,e.y,r*(.25+t*.75),colors.main,colors.accent,reduced?0:Math.floor(t*6)*Math.PI/8);}
    else for(let k=0;k<9;k++){if(!take(budget))break;const a=k*TAU/9,rr=20+t*r*.5;c.globalAlpha=fade;px(c,k%2?colors.accent:colors.light,e.x+Math.cos(a)*rr,e.y-24+Math.sin(a)*rr*.5-(reduced?0:t*30),4,6);}
  }else if(id==='repair'){
    if(!ground){c.globalAlpha=fade;gear(c,e.x,e.y-30-(reduced?0:t*14),10,colors.main,colors.accent);px(c,colors.light,e.x-2,e.y-36,4,12);px(c,colors.light,e.x-6,e.y-32,12,4);}
  }else if(id==='shock-mine'){
    if(ground){c.globalAlpha=fade*.65;ring(c,colors.accent,e.x,e.y,r*(.2+t*.8),r*(.2+t*.8),3);}
    else for(let k=0;k<12;k++){
      if(!take(budget))break;const a=k*TAU/12,rr=r*(.2+t*.6);c.globalAlpha=fade;
      line(c,k%2?colors.main:colors.accent,e.x+Math.cos(a)*rr*.4,e.y+Math.sin(a)*rr*.4-8,e.x+Math.cos(a)*rr,e.y+Math.sin(a)*rr-12,3);px(c,colors.light,e.x+Math.cos(a)*rr,e.y+Math.sin(a)*rr-12,4,4);
    }
  }else if(id==='sidestep'){
    if(!ground&&Number.isFinite(e.x2)&&Number.isFinite(e.y2)){
      c.globalAlpha=(1-t)*.6;for(let k=1;k<=3;k++){
        if(!take(budget))break;const q=k/4;line(c,colors.main,e.x+(e.x2-e.x)*q,e.y+(e.y2-e.y)*q-34,e.x+(e.x2-e.x)*q-7,e.y+(e.y2-e.y)*q-9,4);
      }
    }
  }else if(id==='reload'){
    if(!ground){c.globalAlpha=fade;for(let k=0;k<3;k++){if(!take(budget))break;px(c,k%2?colors.main:colors.accent,e.x-16+k*12,e.y-18-(reduced?0:Math.sin(t*Math.PI)*12),3,7);}}
  }else if(id==='steady-aim'||id==='silver-volley'){
    if(ground&&id==='silver-volley'){c.globalAlpha=fade*.4;line(c,colors.main,e.x,e.y,e.x+Math.cos(e.angle||0)*(e.range||480),e.y+Math.sin(e.angle||0)*(e.range||480),2);}
    else if(!ground&&id==='steady-aim'){c.globalAlpha=fade;ring(c,colors.main,e.x,e.y-25,27,27,2);for(const side of [-1,1])line(c,colors.light,e.x+side*20,e.y-25,e.x+side*34,e.y-25,2);}
  }else if(id==='unwind'){
    if(ground){c.globalAlpha=.65*fade;const radius=r*(.4+t*.6);ring(c,colors.main,e.x,e.y,radius,radius,3);for(let k=0;k<6;k++){const a=k*TAU/6-(reduced?0:Math.floor(t*8)*Math.PI/8);chevron(c,colors.light,e.x+Math.cos(a)*radius,e.y+Math.sin(a)*radius,a-Math.PI/2,12);}}
    else for(let k=0;k<8;k++){if(!take(budget))break;const a=k*TAU/8;c.globalAlpha=fade*.75;px(c,colors.accent,e.x+Math.cos(a)*r*.5,e.y+Math.sin(a)*r*.5-25*(1-t),4,8);}
  }else if(id==='borrowed-seconds'||id==='glass-echo'||id==='time-sand'){
    if(ground){c.globalAlpha=(id==='time-sand'?.32:.7)*fade;clockFace(c,e.x,e.y,r*(id==='time-sand'?.9:.55+t*.45),colors.main,reduced?0:1-t,true);}
    else if(id!=='time-sand')for(let k=0;k<12;k++){
      if(!take(budget))break;const a=k*TAU/12,rr=r*(.25+t*.55);c.globalAlpha=fade;line(c,k%2?colors.main:colors.accent,e.x+Math.cos(a)*rr,e.y+Math.sin(a)*rr,e.x+Math.cos(a)*rr,e.y+Math.sin(a)*rr-10-(reduced?0:Math.sin(t*Math.PI)*16),3);
    }
  }else if(id==='time-return'){
    if(!ground){
      const fx=Number.isFinite(e.x2)?e.x2:e.x,fy=Number.isFinite(e.y2)?e.y2:e.y-65;
      for(let k=0;k<5;k++){if(!take(budget))break;const q=clamp(t*1.3-k*.05,0,1),x=fx+(e.x-fx)*q,y=fy+(e.y-fy)*q-15-(reduced?0:Math.sin(q*Math.PI)*30);c.globalAlpha=fade;px(c,k%2?colors.main:colors.light,x,y,3,4);}
    }
  }else if(id==='perfect-timing'||id==='time-ward'){
    if(!ground){c.globalAlpha=fade;hourglass(c,e.x,e.y-55-(reduced?0:t*10),id==='perfect-timing'?.8:.55,1-t,colors);}
    else if(id==='perfect-timing'){c.globalAlpha=.65*fade;ring(c,colors.main,e.x,e.y,20+t*25,12+t*16,3);}
  }else if(id==='sea-legs'){
    if(ground){c.globalAlpha=fade*.6;for(let k=0;k<8;k++){const a=k*TAU/8;px(c,colors.light,e.x+Math.cos(a)*(20+t*12),e.y+Math.sin(a)*(10+t*6),8,3);}}
  }else if(!ground){
    for(let k=0;k<6;k++){if(!take(budget))break;const a=k*TAU/6;c.globalAlpha=fade;px(c,k%2?colors.main:colors.light,e.x+Math.cos(a)*r*t*.5,e.y-15+Math.sin(a)*r*t*.5,3,4);}
  }
  c.restore();return true;
}

export function drawExpansionZones(c,game,time,visible,budget){
  const team=game.player?.team??0;
  for(const z of game.zones||[]){
    if(!['time-sand','rough-current'].includes(z.type)||!visible(z.x,z.y,z.r)||game.pointVisible&&!game.pointVisible(team,z.x,z.y))continue;
    drawExpansionEffect(c,z,time,true,budget);
  }
  for(const u of game.units||[]){
    const ch=u.channel;if(ch?.kind!=='silver-volley'||time<ch.fireAt||time>=ch.endsAt||u.alive===false||game.canSee&&!game.canSee(team,u)||!visible(u.x,u.y,ch.range||480))continue;
    c.save();c.globalAlpha=.4;line(c,THEMES.ivory.main,ch.x,ch.y,ch.x+Math.cos(ch.angle)*ch.range,ch.y+Math.sin(ch.angle)*ch.range,2);c.restore();
  }
}
