import { WORLD, segmentDistance } from './data.mjs';
import { EFFECT_PALETTES, isEffectVisible } from './combat-visuals.mjs';
import { ITEM_BY_ID, drawItemIcon } from './items.mjs';

// These presentations are derived from actual combat events. They contain no
// simulation updates, particle entities, timers, or random-number calls.
const TAU=Math.PI*2, TEAM=['#67dfef','#ff8f87'];
const TYPES=new Set(['ground-mark','structure-collapse','summon-arrive','summon-depart','beast-capture','reward-gold','reward-item','reward-level']);
const clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
const snap=n=>Math.round(n/2)*2;
const palettes=theme=>EFFECT_PALETTES[theme]||EFFECT_PALETTES.stone;
const iconCache=new Map();
function noise(seed,k){let n=(seed^Math.imul(k+1,0x9e3779b1))>>>0;n^=n>>>16;n=Math.imul(n,0x7feb352d);n^=n>>>15;return(n>>>0)/4294967296;}
function pixel(c,color,x,y,w=3,h=w){c.fillStyle=color;c.fillRect(snap(x),snap(y),Math.max(2,snap(w)),Math.max(2,snap(h)));}
function stroke(c,color,x,y,x2,y2,width=2){c.strokeStyle=color;c.lineWidth=width;c.beginPath();c.moveTo(snap(x),snap(y));c.lineTo(snap(x2),snap(y2));c.stroke();}
function oval(c,color,x,y,rx,ry,fill=true,width=2){
  c.beginPath();for(let k=0;k<=16;k++){const a=k/16*TAU,xx=snap(x+Math.cos(a)*rx),yy=snap(y+Math.sin(a)*ry);if(k)c.lineTo(xx,yy);else c.moveTo(xx,yy);}
  if(fill){c.fillStyle=color;c.closePath();c.fill();}else{c.strokeStyle=color;c.lineWidth=width;c.stroke();}
}
function shard(c,color,light,x,y,w,h,lean=0){
  c.fillStyle=color;c.beginPath();c.moveTo(snap(x+lean),snap(y-h));c.lineTo(snap(x+w),snap(y));c.lineTo(snap(x-lean),snap(y+h));c.lineTo(snap(x-w),snap(y));c.closePath();c.fill();
  stroke(c,light,x+lean,y-h+2,x-w,y,2);
}
function take(budget,n=1){
  if(!budget)return true;
  if(budget.remaining<n||(Number.isFinite(budget.accentRemaining)&&budget.accentRemaining<n))return false;
  budget.remaining-=n;if(Number.isFinite(budget.accentRemaining))budget.accentRemaining-=n;return true;
}
export function isWorldEffect(effect){return TYPES.has(effect?.type);}
export function presentationAge(effect,time){
  return Number.isFinite(effect.createdAt)?Math.max(0,time-effect.createdAt):Math.max(0,(effect.maxLife||1)-(effect.life||0));
}
function progress(effect,time){return clamp(presentationAge(effect,time)/Math.max(.01,effect.maxLife||1),0,1);}

/** Camp scenery is permanent map knowledge; it never reads camp occupancy. */
export function drawCampScenery(c,camp){
  if(!Number.isFinite(camp?.x)||!Number.isFinite(camp?.y))return;
  const x=camp.x,y=camp.y,seed=Math.round(x+y*13),type=camp.type;
  c.save();
  oval(c,'#294c3d',x,y,97,82);oval(c,type==='boar'?'#776b48':'#697a51',x,y,81,65);
  if(type==='wolf'){
    oval(c,'#7d8160',x,y,66,50);
    // A recessed den and layered stone arch sit behind the fighting clearing.
    oval(c,'#263b3b',x+10,y-48,37,21);oval(c,'#172d31',x+10,y-45,26,18);
    for(let k=0;k<7;k++){const a=Math.PI+k/6*Math.PI;pixel(c,k%2?'#819184':'#5e776f',x+10+Math.cos(a)*32-7,y-43+Math.sin(a)*25-9,16,17);}
    pixel(c,'#a9b194',x-12,y-72,15,3);pixel(c,'#9ba58b',x+25,y-65,10,3);
    for(let k=0;k<6;k++){
      const xx=x-28+k*12,yy=y+26+(k%2)*6;
      oval(c,'#586a4d',xx,yy,3,2);pixel(c,'#586a4d',xx-2,yy-5,2,2);pixel(c,'#586a4d',xx+2,yy-5,2,2);
    }
    // Small bones keep the den readable without cluttering its center.
    for(const side of [-1,1]){
      const bx=x+side*55,by=y+side*12;
      stroke(c,'#d4cfa4',bx-7,by,bx+6,by+4,3);pixel(c,'#e4dbb3',bx-8,by-2,4,5);pixel(c,'#b8b68e',bx+5,by+2,4,5);
    }
  }else if(type==='boar'){
    oval(c,'#6d573e',x-8,y+3,63,44);oval(c,'#796246',x-9,y-3,49,31);
    oval(c,'#514f3f',x+22,y+12,27,13);oval(c,'#627468',x+19,y+9,20,8);
    stroke(c,'#95a286',x+7,y+6,x+26,y+6,2);
    for(let k=0;k<9;k++){
      const a=k*2.4,rr=39+noise(seed,k)*25,xx=x+Math.cos(a)*rr,yy=y+Math.sin(a)*rr*.65;
      stroke(c,'#4e4934',xx-5,yy,xx+6,yy+2,3);stroke(c,'#a08b58',xx-5,yy-2,xx+3,yy-1,2);
    }
    for(const side of [-1,1]){
      const xx=x+side*65,yy=y-25;
      pixel(c,'#59432f',xx,yy-10,7,21);stroke(c,'#8d7041',xx+2,yy+8,xx-side*10,yy+14,3);
      pixel(c,'#5d8a46',xx-8,yy-13,24,7);pixel(c,'#83a45a',xx-3,yy-16,13,5);
    }
  }else{
    oval(c,'#7b8871',x,y,66,52);oval(c,'#5d7467',x,y,48,34,false,3);
    for(let k=0;k<8;k++){
      const a=k/8*TAU,xx=x+Math.cos(a)*46,yy=y+Math.sin(a)*32;
      stroke(c,'#a2b69a',xx-3,yy-3,xx+3,yy+3,2);stroke(c,'#8b9f83',xx+3,yy-3,xx-3,yy+3,2);
    }
    for(let k=0;k<3;k++){
      const xx=x+(k-1)*48,yy=y-44+(k%2?-14:8),h=k===1?35:26;
      pixel(c,'#324b4b',xx-11,yy-h,22,h+7);pixel(c,'#657c75',xx-9,yy-h+2,18,h+2);pixel(c,'#9aa68d',xx-9,yy-h+2,5,h-3);
      stroke(c,'#c0d7b0',xx-2,yy-h+9,xx+5,yy-h+14,2);stroke(c,'#a8cfa4',xx+5,yy-h+14,xx-4,yy-h+20,2);
      pixel(c,'#3d5c51',xx+4,yy-h,7,6);pixel(c,'#79a367',xx-12,yy+1,17,4);
    }
    for(let k=0;k<5;k++)pixel(c,k%2?'#8b9a80':'#536c61',x-49+k*22,y+38+(k%2)*5,9,5);
  }
  c.restore();
}

/** Footfalls and brush responses are attached only to currently visible units. */
export function drawReactiveTerrain(c,game,time,isVisible,budget){
  if(budget?.reducedMotion)return 0;
  const team=game.player?.team??0;let count=0;
  c.save();
  for(const unit of game.units||[]){
    const action=unit.visualAction,age=action?time-action.startedAt:Infinity;
    const takeoff=unit.kind==='hero'&&['angel','seraph'].includes(unit.heroId)&&action?.kind==='cast'&&age>=0&&age<.35;
    if(!unit.alive||(!unit.moving&&!takeoff)||unit.kind==='tower'||unit.kind==='core'||(isVisible&&!isVisible(unit))||(game.canSee&&!game.canSee(team,unit)))continue;
    const ux=takeoff&&Number.isFinite(action.fromX)?action.fromX:unit.x,uy=takeoff&&Number.isFinite(action.fromY)?action.fromY:unit.y;
    if(takeoff&&game.pointVisible&&!game.pointVisible(team,ux,uy))continue;
    if(!take(budget))break;
    const phase=takeoff?age/.35:(time*(unit.kind==='beast'?3:6)+(unit.id||0)*.31)%1,angle=(unit.angle||0)+Math.PI,heavy=unit.kind==='beast'||unit.subtype==='bear';
    const x=ux+Math.cos(angle)*(9+phase*14),y=uy+Math.sin(angle)*(9+phase*14);
    let water=false,lane=false,brush=null;
    for(let i=1;i<WORLD.river.length;i++)if(segmentDistance(ux,uy,WORLD.river[i-1],WORLD.river[i])<WORLD.riverWidth*.46){water=true;break;}
    if(!water)for(const patch of WORLD.brush)if(Math.hypot(ux-patch.x,uy-patch.y)<patch.r){brush=patch;break;}
    if(!water&&!brush)for(const path of WORLD.lanes){for(let i=1;i<path.points.length;i++)if(segmentDistance(ux,uy,path.points[i-1],path.points[i])<52){lane=true;break;}if(lane)break;}
    c.globalAlpha=(1-phase)*(water?.4:brush?.7:.35);
    if(takeoff&&!water&&!lane){
      for(let k=0;k<5;k++){
        if(k&&!take(budget))break;
        const a=k/5*TAU,reach=9+phase*26;
        c.globalAlpha=(1-phase)*.75;
        pixel(c,k%2?'#a8c97a':'#77a75c',ux+Math.cos(a)*reach,uy+Math.sin(a)*reach*.5-Math.sin(phase*Math.PI)*22,5,3);
      }
    }else if(!unit.moving){continue;}
    else if(water){
      const radius=(heavy?17:8)+phase*(heavy?23:12);
      oval(c,'#c0e4d1',x,y,radius,radius*.35,false,2);
      if(heavy&&take(budget)){pixel(c,'#e4f1d1',x-radius,y-6-phase*9,3,5);pixel(c,'#bde4d3',x+radius,y-4-phase*7,3,4);count++;}
    }else if(brush){
      for(let k=0;k<3;k++){
        const side=k-1,xx=unit.x+side*9,yy=unit.y+5;
        stroke(c,'#3e784b',xx,yy,xx+Math.cos(angle)*10+side*5,yy-7,3);
        pixel(c,k%2?'#96bb6b':'#6d9d55',xx+Math.cos(angle)*(8+phase*14),yy-phase*16,5,3);
      }
    }else if(lane){
      pixel(c,'#dfcea0',x-4,y-2-phase*8,heavy?9:5,heavy?5:3);
      pixel(c,'#af9b70',x+4,y+3,heavy?7:3,3);
    }else{
      pixel(c,'#84a65d',x,y-phase*9,4,2);pixel(c,'#4e8451',x+7,y+2-phase*5,3,2);
    }
    count++;
  }
  // A nearby visible impact makes a small tuft bend; it never reveals a hidden unit.
  for(const effect of game.effects||[]){
    if(effect.type!=='ground-mark'||presentationAge(effect,time)>.28)continue;
    if(!isEffectVisible(game,team,effect))continue;
    for(const patch of WORLD.brush){
      const d=Math.hypot(patch.x-effect.x,patch.y-effect.y);if(d>(effect.r||70)+patch.r)continue;
      if(!take(budget))break;
      const a=Math.atan2(patch.y-effect.y,patch.x-effect.x),strength=1-presentationAge(effect,time)/.28;
      c.globalAlpha=strength*.45;
      for(let k=0;k<3;k++)pixel(c,'#a4bd6b',patch.x+(k-1)*14+Math.cos(a)*strength*15,patch.y-8-Math.sin(k+1)*strength*15,5,3);
      count++;
    }
  }
  c.restore();return count;
}

/** Damage silhouettes persist even when decorative particle allowance is spent. */
export function drawStructureDamage(c,unit,time,budget){
  if(!unit?.alive||!(unit.hp>0)||!['tower','core'].includes(unit.kind))return 0;
  const health=clamp(unit.hp/Math.max(1,unit.maxHp),0,1),stage=health<=.25?2:health<=.5?1:0;
  const x=unit.x,y=unit.y;let count=0;
  c.save();
  if(stage){
    const core=unit.kind==='core',cy=y-(core?72:38),wide=core?25:19;
    c.globalAlpha=.86;
    stroke(c,core?'#e8fff7':'#293c43',x-8,cy-24,x+2,cy-10,core?2:3);
    stroke(c,core?'#dcfbf1':'#293c43',x+2,cy-10,x-6,cy+3,core?2:3);
    stroke(c,core?'#f1fffb':'#293c43',x-6,cy+3,x+10,cy+15,core?2:3);
    stroke(c,core?TEAM[unit.team]:'#435655',x+2,cy-10,x+wide,cy-13,2);
    if(stage===2){
      stroke(c,core?'#eafffb':'#273b43',x+wide,cy-2,x+6,cy+8,3);
      stroke(c,core?'#bde9e9':'#273b43',x+6,cy+8,x+wide-5,cy+28,3);
      if(!core){pixel(c,'#243a42',x-27,y-77,10,8);pixel(c,'#314b50',x+17,y-68,9,9);pixel(c,'#c1c3a8',x-20,y-66,6,3);}
      else{c.globalAlpha=.2+Math.sin(time*7)*.06;oval(c,TEAM[unit.team]||'#c9eecc',x,cy,32,49,false,2);}
    }
  }
  const hit=unit.visualHit,age=hit?time-hit.at:Infinity;
  if(age>=0&&age<.45){
    for(let k=0;k<5;k++){
      if(!take(budget))break;
      const sign=k%2?1:-1,fall=age/.45,xx=x+sign*(10+fall*(12+k*5)),yy=y-40+fall*fall*(35+k*4);
      c.globalAlpha=(1-fall)*.85;pixel(c,k%2?'#b7b99f':'#6a8381',xx,yy,4,5);count++;
    }
  }
  c.restore();return count;
}

function drawGroundMark(c,e,t,budget){
  const fade=Math.min(1,(1-t)*2),r=clamp(e.r||70,12,180),seed=e.seed||0,mark=e.mark||'slam';
  if(!take(budget))return;
  if(mark==='scorch'){
    c.globalAlpha=.23*fade;oval(c,'#332e29',e.x,e.y,r*.73,r*.53);
    for(let k=0;k<7;k++){if(!take(budget))break;const a=k*2.4,rr=r*(.15+noise(seed,k)*.5);c.globalAlpha=.25*fade;pixel(c,k%3?'#574133':'#9d5a3c',e.x+Math.cos(a)*rr,e.y+Math.sin(a)*rr*.65,6,3);}
  }else if(mark==='frost'){
    c.globalAlpha=.12*fade;oval(c,'#b1e3dc',e.x,e.y,r*.7,r*.48);
    for(let k=0;k<8;k++){if(!take(budget))break;const a=k/8*TAU,rr=r*(.2+noise(seed,k)*.55);c.globalAlpha=.3*fade;shard(c,'#88bfcb','#c8e2db',e.x+Math.cos(a)*rr,e.y+Math.sin(a)*rr*.65,4,7,2);}
  }else if(mark==='quake'){
    for(let k=0;k<5;k++){if(!take(budget))break;const a=k/5*TAU+.2,dx=Math.cos(a),dy=Math.sin(a),reach=r*(.55+noise(seed,k)*.2);c.globalAlpha=.31*fade;stroke(c,'#343c3a',e.x,e.y,e.x+dx*reach*.45+5,e.y+dy*reach*.45,3);stroke(c,'#343c3a',e.x+dx*reach*.45+5,e.y+dy*reach*.45,e.x+dx*reach,e.y+dy*reach*.8,3);c.globalAlpha=.13*fade;stroke(c,'#b29ac4',e.x,e.y,e.x+dx*reach*.45,e.y+dy*reach*.45,2);}
  }else if(mark==='poison'){
    for(let k=0;k<7;k++){if(!take(budget))break;const a=k*2.4,rr=r*(.15+noise(seed,k)*.48);c.globalAlpha=.22*fade;oval(c,k%2?'#667e4b':'#789757',e.x+Math.cos(a)*rr,e.y+Math.sin(a)*rr*.7,4+noise(seed,k+9)*5,3);}
  }else{
    c.globalAlpha=.19*fade;oval(c,'#35443a',e.x,e.y,r*.57,r*.35);
    for(let k=0;k<8;k++){if(!take(budget))break;const a=k/8*TAU;c.globalAlpha=.35*fade;pixel(c,k%2?'#99a18b':'#5b6f65',e.x+Math.cos(a)*r*.65,e.y+Math.sin(a)*r*.43,6,4);}
  }
}

function drawCollapse(c,e,t,ground,budget){
  const core=e.kind==='core',team=TEAM[e.team]||'#94d9d2',reduced=budget?.reducedMotion;
  if(ground){
    c.globalAlpha=(1-t)*.24;oval(c,core?team:'#bdb99b',e.x,e.y,(core?56:24)+t*(core?95:44),(core?30:12)+t*(core?48:24));
    if(core){c.globalAlpha=(1-t)*.55;oval(c,team,e.x,e.y,48+t*150,23+t*72,false,3);}
    return;
  }
  if(core){
    for(let k=0;k<9;k++){
      // Large crystal fragments are the destruction silhouette, not optional flecks.
      const a=k/9*TAU,spread=reduced?t*45:t*t*130,hover=Math.sin(Math.min(1,t*2)*Math.PI)*16;
      const x=e.x+Math.cos(a)*spread,y=e.y-60+Math.sin(a)*spread*.5-hover+t*t*30;
      c.globalAlpha=clamp((1-t)*1.8,0,1);
      shard(c,k%2?team:'#466e8b','#d7fff1',x,y,(8+k%3*3)*(1-t*.55),(18+k%3*8)*(1-t*.45),Math.cos(a)*6);
    }
    for(let k=0;k<12;k++){if(!take(budget))break;const a=k*2.4,reach=(24+t*140)*(reduced?.4:1);c.globalAlpha=(1-t)*.8;pixel(c,k%3?team:'#e0fff0',e.x+Math.cos(a)*reach,e.y-45+Math.sin(a)*reach*.6,4,5);}
  }else{
    for(let row=0;row<4;row++)for(let col=0;col<3;col++){
      const k=row*3+col,delay=k%3*.03,q=clamp((t-delay)/.85,0,1),spread=(col-1)*q*(reduced?8:28),fall=q*q*(72-row*12);
      c.globalAlpha=clamp((1-t)*2.2,0,1);
      pixel(c,col===0?'#9aa79a':col===1?'#738b8b':'#4c6a74',e.x-20+col*14+spread,e.y-61+row*13+fall,12,11);
      pixel(c,'#b0b69d',e.x-20+col*14+spread,e.y-61+row*13+fall,12,2);
    }
    c.globalAlpha=clamp((1-t)*1.5,0,1);shard(c,team,'#ddfff2',e.x+t*17,e.y-86+t*t*78,9*(1-t*.4),14*(1-t*.4),t*9);
    for(let k=0;k<6;k++){if(!take(budget))break;const a=k*2.4,reach=15+t*40;c.globalAlpha=(1-t)*.4;pixel(c,'#c1bd9f',e.x+Math.cos(a)*reach,e.y+Math.sin(a)*reach*.4-t*9,7,5);}
  }
}

function drawSummon(c,e,t,ground,budget,options){
  const depart=e.type==='summon-depart',capture=e.type==='beast-capture',q=depart?1-t:t,type=e.subtype||'spirit';
  const colors=palettes(e.theme||(/spirit/.test(type)?'arcane':'nature')),team=TEAM[e.team]||colors.main,r=clamp(e.r||25,18,70),reduced=budget?.reducedMotion;
  if(ground){
    c.globalAlpha=(1-t)*.48;oval(c,capture?team:colors.main,e.x,e.y,r*(.7+t*.9),r*(.28+t*.4),false,3);
    if(type==='treant')for(let k=0;k<5;k++){
      if(!take(budget))break;
      const a=k/5*TAU,x=e.x+Math.cos(a)*r*(.3+q),y=e.y+Math.sin(a)*r*.5;
      c.globalAlpha=(1-t)*.8;stroke(c,'#876342',e.x,e.y,x,y,4);stroke(c,'#a48b55',e.x,e.y-2,x,y-2,2);
    }
    if(type==='bear'&&!depart&&t>.3){c.globalAlpha=(1-t)*.28;oval(c,'#c1b58b',e.x,e.y,20+t*35,8+t*12);}
    return;
  }
  if(depart&&typeof options.drawUnit==='function')options.drawUnit(c,e,(1-t)*.65);
  const count=reduced?5:capture?16:12;
  for(let k=0;k<count;k++){
    if(!take(budget))break;
    const a=k/count*TAU,seed=e.seed||0;
    let x=e.x+Math.cos(a)*r*(depart?t+.3:1.2-t),y=e.y-8-Math.sin(t*Math.PI)*28+Math.sin(a)*r*.5;
    c.globalAlpha=Math.sin(t*Math.PI)*.8;
    if(capture){
      x=e.x+Math.cos(a+t*2)*r*(1.2-t*.4);y=e.y-10-t*(reduced?40:80)+Math.sin(a)*r*.4;
      pixel(c,k%3?team:'#efffd8',x,y,4,7);
    }else if(type==='spirit'){
      if(depart)y=e.y-12-t*65+Math.sin(a)*15;
      else y=e.y-10-q*45+Math.sin(a)*r*.5;
      pixel(c,k%3?colors.main:colors.light,x,y,4,6);
    }else if(type==='treant'){
      y=e.y-5-t*42+Math.sin(a)*r*.4;
      pixel(c,k%3?'#82b75e':'#937649',x,y,5,3);
    }else{
      c.globalAlpha=(1-t)*.45;pixel(c,k%3?colors.main:'#c5bc8b',x,y,4,3);
    }
  }
}

function itemIcon(c,id,x,y){
  if(!ITEM_BY_ID[id])return;
  if(typeof document==='undefined'){drawItemIcon(c,id,x,y,30);return;}
  let icon=iconCache.get(id);
  if(!icon){icon=document.createElement('canvas');icon.width=icon.height=30;drawItemIcon(icon.getContext('2d'),id,0,0,30);iconCache.set(id,icon);}
  c.drawImage(icon,Math.round(x),Math.round(y),30,30);
}

/** Draw one presentation under fog; return true even when its phase has ended. */
export function drawWorldEffect(c,e,time,ground=false,budget,options={}){
  if(!isWorldEffect(e))return false;
  if(!Number.isFinite(e.x)||!Number.isFinite(e.y))return true;
  const t=progress(e,time);if(t>=1)return true;
  c.save();
  if(e.type==='ground-mark'){if(ground)drawGroundMark(c,e,t,budget);}
  else if(e.type==='structure-collapse')drawCollapse(c,e,t,ground,budget);
  else if(e.type==='summon-arrive'||e.type==='summon-depart'||e.type==='beast-capture')drawSummon(c,e,t,ground,budget,options);
  else if(e.type==='reward-level'){
    if(ground){c.globalAlpha=(1-t)*.48;oval(c,'#f1d886',e.x,e.y,22+t*45,9+t*18,false,3);}
    else for(let k=0;k<(budget?.reducedMotion?4:9);k++){
      if(!take(budget))break;const a=k*2.4;c.globalAlpha=(1-t)*.8;pixel(c,k%2?'#ffe7a5':'#d8e99f',e.x+Math.cos(a)*(14+t*17),e.y-8-t*68+Math.sin(a)*10,3,7);
    }
  }else if(e.type==='reward-item'&&!ground){
    c.globalAlpha=Math.min(1,t*8,(1-t)*5);itemIcon(c,e.itemId,e.x-15,e.y-112-t*12);
    if(take(budget)){c.globalAlpha=(1-t)*.65;pixel(c,'#ffe9a8',e.x-23,e.y-99-t*12,4,4);pixel(c,'#ffe9a8',e.x+19,e.y-113-t*12,4,4);}
  }
  // reward-gold is drawn in screen space by drawGoldFlight, after world/fog.
  c.restore();return true;
}

/** Only the player-owned, already-visible gold events reach this overlay. */
export function drawGoldFlight(c,e,time,camera,viewport,rewardTarget,budget){
  if(e?.type!=='reward-gold'||!Number.isFinite(rewardTarget?.x)||!Number.isFinite(rewardTarget?.y)||!(viewport?.width>0)||!(viewport?.height>0)||!(camera?.width>0)||!(camera?.height>0))return 0;
  const age=presentationAge(e,time),duration=e.maxLife||1;if(age<0||age>=duration)return 0;
  const sx=(e.x-camera.x)/camera.width*viewport.width,sy=(e.y-camera.y-12)/camera.height*viewport.height;
  const tx=rewardTarget.x/Math.max(1,viewport.cssWidth||viewport.width)*viewport.width,ty=rewardTarget.y/Math.max(1,viewport.cssHeight||viewport.height)*viewport.height;
  const reduced=budget?.reducedMotion,n=reduced?2:5;let emitted=0;c.save();
  for(let k=0;k<n;k++){
    const t=clamp((age-k*.035)/(duration-.18),0,1);if(t<=0||t>=1||!take(budget))continue;
    const controlX=(sx+tx)/2+(k-2)*(reduced?2:11),controlY=Math.min(sy,ty)-(reduced?8:35),u=1-t;
    const x=u*u*sx+2*u*t*controlX+t*t*tx,y=u*u*sy+2*u*t*controlY+t*t*ty;
    c.globalAlpha=Math.min(1,(1-t)*5);pixel(c,'#91602c',x-2,y-2,5,5);pixel(c,'#ffd56b',x-1,y-2,4,4);pixel(c,'#fff0a2',x-1,y-2,2,2);emitted++;
  }
  c.restore();return emitted;
}
