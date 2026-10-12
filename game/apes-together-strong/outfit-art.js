/* Original tribal Kings and workers, shared species armor composites.
 * Rendering only: no actor writes, resource rewards, collision or facing changes. */
(() => {
'use strict';
const art=()=>window.ATSVisualAssets,meta=()=>art()?.manifest?.outfits;
const species=['gorilla','chimpanzee','orangutan','gibbon','mandrill','capuchin'],tribes=['ironback','shadowhand','emberwood'];
const clamp=(x,a,b)=>Math.max(a,Math.min(b,x)),work=/chop|build|repair|garden|gather|cook|saw|woodwork|clearing/;
const ready=()=>!!art()?.get('characters-ironback')&&!!art()?.get('outfits-follower-armor')&&!!art()?.get('outfits-king-armor');
function pose(a,p,options,time){
 if(p.human)return p;
 if(p.king){const tribe=tribes.includes(options.tribeId||a.tribeId)?options.tribeId||a.tribeId:'ironback';if(!art()?.get('characters-'+tribe))return p;
  const rows=tribe==='emberwood'?10:9,d=[2,1,0,1,2,3,4,3][p.direction];let row=d,column=0,action=false;
  switch(p.state){
   case'run':case'sprint':column=(p.state==='sprint'||a.sprinting||a.state==='charge'?3:1)+(p.beat>=2?1:0);break;
   case'carry':column=5;break;
   case'hurt':case'knockback':column=6;break;
   case'injured':case'rest':column=7;break;
   case'attack':case'throw':case'chargeAttack':action=true;column=0;break;
   case'heavyAttack':action=true;column=1;break;
   case'climb':action=true;column=2+(p.beat%2);break;
   case'prepare':case'command':action=true;column=4;break;
   case'victory':action=true;column=5;break;
   case'death':action=true;column=p.phase<.65?6:7;break;
  }
  if(options.reducedMotion&&a.moving)column=0;
  if(action&&d===4&&tribe!=='emberwood'&&art()?.get('characters-royal-rear'))return {...p,atlas:'royal-rear',row:tribe==='ironback'?0:1,column,tribe,sourceDirectionCount:8};
  if(action)row=5+Math.min(d,rows-6);
  return {...p,atlas:tribe,row,column,tribe,sourceDirectionCount:8};
 }
 if(a.moving||p.state==='climb'||a.hp<=0||a.hitTimer>0||a.attackTimer>0||!work.test(a.activity||''))return p;
 const facing=p.rear?'rear':[0,4].includes(p.direction)?'side':'front',atlas='workers-'+facing;if(!art()?.get('characters-'+atlas))return p;
 let column=4,state='building',phase=p.phase;
 if(/chop|clearing/.test(a.activity)){column=phase<.45?0:1;state='chopping'}
 else if(/garden|gather/.test(a.activity)){column=phase<.65?5:2;state='gathering'}
 else if(/cook/.test(a.activity)){column=6;state='cooking'}
 else if(/saw|woodwork/.test(a.activity)){column=7;state='sawing'}
 else if(time-(a.workDeliveryAt??-10)<.65){column=2;state='placing'}
 else {column=phase<.3?3:phase<.65?4:2;state=column===3||column===2?'placing':'building'}
 if(options.reducedMotion)column=state==='chopping'?1:column;
 return {...p,atlas,row:Math.max(0,species.indexOf(a.species)),column,state,clipId:state,worker:true};
}
function armor(a,p){
 if(p.king){const tier=a.equipment?.armorLevel?Math.min(5,a.equipment.armorLevel+2):a.outfit?.royal==='timberRoyal'?2:a.outfit?.royal==='wovenRoyal'?1:0;if(!tier)return null;return {atlas:'king-armor',row:Math.max(0,tribes.indexOf(p.tribe||a.tribeId||'ironback')),grade:tier}}
 const item=window.ATSOutfitItems?.[a.outfit?.body],grade=item?.grade||(a.elite?4:0);return grade?{atlas:'follower-armor',row:Math.max(0,species.indexOf(a.species)),grade}:null;
}
function paintArmor(c,a,p,slot,g,armor){
 const data=meta(),f=data?.frames?.[armor.atlas]?.[armor.row+':'+((armor.grade-1)*2+(p.rear?1:0))],image=art()?.get('outfits-'+armor.atlas);if(!f||!image)return false;
 const frame=g.f,unit=g.width/frame.w,body=frame.body||{x:frame.w*(p.rear?.5:a.species==='orangutan'?.56:.6),y:frame.h*.51,width:frame.w*.63,height:frame.h*.4};
 const x=(body.x-frame.anchorX)*unit,y=(body.y-frame.anchorY)*unit,side=p.king?[0,4].includes(p.direction):p.atlas==='workers-side',growth=p.king?([0,.88,1,1.12,1.3,p.tribe==='shadowhand'?1.28:1.55][armor.grade]):[0,.88,1.06,1.3,1.3][armor.grade],wide=body.width*unit*(side?.8:1.05)*growth,height=body.height*unit*(p.king?1+(armor.grade-1)*.025:1);
 c.save();c.translate(x,y);if(p.state==='death'&&p.phase>=.65)c.rotate(1.25);else if(p.state==='heavyAttack'||p.worker&&[1,2,5,7].includes(p.column))c.rotate(.12);
 c.drawImage(image,f.x,f.y,f.w,f.h,-wide/2,-height*.57,wide,height);c.restore();
 const guard=data.frames?.['shields-tools']?.['3:4'],guardImage=art()?.get('outfits-shields-tools');
 if(guard&&guardImage&&armor.grade>=2){const anchors=window.ATSHeldItemArt?.sockets(a,p,slot);if(anchors)for(const hand of p.king||p.worker?[anchors.left,anchors.right]:[anchors.right]){c.save();c.translate(hand.x,hand.y-4);c.rotate(.12);const w=p.king?10:7,h=p.king?12:9;c.drawImage(guardImage,guard.x,guard.y,guard.w,guard.h,-w/2,-h,w,h);c.restore()}}
 return true;
}
function restoreHead(c,a,p,g,image,kit){
 const f=g.f,u=g.width/f.w;let x,y,rx,ry;if(p.king){x=.5;y=p.row>=5?.29:.235;rx=.14;ry=.145}else{const i=Math.max(0,species.indexOf(a.species));x=p.rear?.74:[.74,.78,.63,.73,.79,.76][i];y=[.26,.21,.27,.2,.38,.38][i];rx=.13;ry=.17;if(p.atlas==='actions'||p.worker){x=.58;y=.3;rx=.14;ry=.14}}
 c.save();c.beginPath();c.ellipse((f.w*x-f.anchorX)*u,(f.h*y-f.anchorY)*u,f.w*rx*u,f.h*ry*u,0,0,Math.PI*2);c.clip();c.drawImage(image,f.x,f.y,f.w,f.h,g.x,g.y,g.width,g.height);c.restore();
 if(!p.king&&kit?.grade>=3){const helmet=meta()?.frames?.['shields-tools']?.['3:5'],im=art()?.get('outfits-shields-tools');if(helmet&&im){const hx=(f.w*x-f.anchorX)*u,hy=(f.h*y-f.anchorY)*u,w=f.w*.34*u,h=f.h*.24*u;c.drawImage(im,helmet.x,helmet.y,helmet.w,helmet.h,hx-w/2,hy-h*.76,w,h);c.save();c.beginPath();c.ellipse(hx+f.w*.02*u,hy+f.h*.025*u,f.w*rx*u*.74,f.h*ry*u*.68,0,0,Math.PI*2);c.clip();c.drawImage(image,f.x,f.y,f.w,f.h,g.x,g.y,g.width,g.height);c.restore()}}
}
function frame(r,c,a,p,slot,g,image){
 const kit=armor(a,p);if(!kit||!ready())return false;
 const registry=art();let cache=r.outfitComposites;if(!cache||cache.registry!==registry){cache=r.outfitComposites={registry,entries:new Map(),pixels:0,hits:0,misses:0}}
 const key=[p.atlas,p.row,p.column,slot,kit.atlas,kit.row,kit.grade,p.rear,[0,4].includes(p.direction),a.coatVariant||0].join(':');let entry=cache.entries.get(key);
 if(!entry){const density=p.king?2:1.25,width=Math.ceil(g.width+32),height=Math.ceil(g.height+24),pixels=Math.ceil(width*density)*Math.ceil(height*density);
  while(cache.entries.size>=160||cache.pixels+pixels>2200000){const first=cache.entries.keys().next().value;if(first===undefined)break;cache.pixels-=cache.entries.get(first).pixels;cache.entries.delete(first)}
  const canvas=document.createElement('canvas');canvas.width=Math.ceil(width*density);canvas.height=Math.ceil(height*density);const cc=canvas.getContext('2d'),originX=-g.x+16,originY=-g.y+12;cc.scale(density,density);cc.translate(originX,originY);const f=g.f;cc.drawImage(image,f.x,f.y,f.w,f.h,g.x,g.y,g.width,g.height);paintArmor(cc,a,p,slot,g,kit);restoreHead(cc,a,p,g,image,kit);
  entry={canvas,pixels,x:-originX,y:-originY,width,height};cache.entries.set(key,entry);cache.pixels+=pixels;cache.misses++;
 }else cache.hits++;
 c.drawImage(entry.canvas,entry.x,entry.y,entry.width,entry.height);return true;
}

// Complete repeated loadouts are one draw in the crowd, including front/rear
// items and hand occlusion. Sources are composed only on appearance changes.
function actor(r,c,a,p,slot,g,image,items){
 const kit=armor(a,p);if(!g||!image||!ready()||!kit&&!items?.length)return false;
 const registry=art(),low=r.quality==='low',density=p.king?2:Math.min(2,Math.max(1,Math.ceil((r.camera?.zoom||1)*(a.champion?1.5:r.useApeAtlas||low?1:1.25)*4)/4));let cache=r.loadoutComposites;
 if(!cache||cache.registry!==registry||cache.low!==low)cache=r.loadoutComposites={registry,low,entries:new Map(),standins:new Map(),pixels:0,hits:0,misses:0,deferred:0};
 const itemKey=items?.visualKey||items?.map(i=>i.id+':'+i.socket+':'+i.layer+':'+(i.damage===undefined?'':i.damage<.3?2:i.damage<.65?1:0)).join(',')||'';
 const baseKey=[p.atlas,p.row,slot,density,kit?.atlas,kit?.row,kit?.grade,p.rear,low?0:a.coatVariant||0,itemKey].join('|'),key=p.column+'|'+baseKey;let entry=cache.entries.get(key);if(entry&&entry.source!==image){cache.pixels-=entry.pixels;entry.canvas.close?.();cache.entries.delete(key);entry=null}
 if(!entry){
  const prior=cache.standins.get(baseKey);if(!p.king&&!a.champion&&['run','sprint','idle'].includes(p.state)&&prior?.source===image&&(r._outfitNewFrames||0)>=8){c.drawImage(prior.canvas,prior.x,prior.y,prior.width,prior.height);cache.deferred++;return true}
  r._outfitNewFrames=(r._outfitNewFrames||0)+1;
  const held=window.ATSHeldItemArt,pose={...p,phase:0,state:p.king||p.worker?p.state:p.atlas==='actions'&&p.column<=2?'attack':'idle'},anchors=held?.sockets(a,p,slot);let left=g.x-3,top=g.y-3,right=g.x+g.width+3,bottom=g.y+g.height+3;
  for(const item of items||[]){let f=art().manifest['held-items'].frames[item.id];if(['buckler','ironShield','ancientShield'].includes(item.id))f=art().manifest['held-items'].frames[item.id+((p.rear?3:0)+(item.damage<.3?2:item.damage<.65?1:0))];if(item.id==='logShield')f={drawWidth:48,drawHeight:48,w:1,h:1,gripX:.5,gripY:.62};if(!f||!anchors)continue;const at=held.placement(a,pose,item,anchors),w=f.drawWidth,h=f.drawHeight,x=-f.gripX/f.w*w,y=-f.gripY/f.h*h,cos=Math.cos(at.rotation),sin=Math.sin(at.rotation);for(const [u,v]of [[x,y],[x+w,y],[x,y+h],[x+w,y+h]]){const px=at.x+u*cos-v*sin,py=at.y+u*sin+v*cos;left=Math.min(left,px-2);right=Math.max(right,px+2);top=Math.min(top,py-2);bottom=Math.max(bottom,py+2)}}
  const width=Math.ceil(right-left),height=Math.ceil(bottom-top),pixels=Math.ceil(width*density)*Math.ceil(height*density),limit=low?10000000:16000000;
  while(cache.entries.size>=(low?512:1024)||cache.pixels+pixels>limit){const first=cache.entries.keys().next().value;if(first===undefined)break;cache.pixels-=cache.entries.get(first).pixels;const old=cache.entries.get(first);if(cache.standins.get(old.baseKey)===old)cache.standins.delete(old.baseKey);old.canvas.close?.();cache.entries.delete(first)}
  const canvas=typeof OffscreenCanvas==='function'?new OffscreenCanvas(Math.ceil(width*density),Math.ceil(height*density)):document.createElement('canvas');canvas.width=Math.ceil(width*density);canvas.height=Math.ceil(height*density);const cc=canvas.getContext('2d'),originX=-left,originY=-top;cc.scale(density,density);cc.translate(originX,originY);const f=g.f;
  if(items)held.draw(r,cc,a,pose,slot,'rear',items);cc.drawImage(image,f.x,f.y,f.w,f.h,g.x,g.y,g.width,g.height);
  if(kit){paintArmor(cc,a,p,slot,g,kit);restoreHead(cc,a,p,g,image,kit)}
  if(items){held.draw(r,cc,a,pose,slot,'front',items);held.restoreHands(r,cc,a,pose,slot,items,image)}
  entry={baseKey,source:image,canvas:canvas.transferToImageBitmap?.()||canvas,pixels,x:-originX,y:-originY,width,height};cache.entries.set(key,entry);cache.pixels+=pixels;cache.misses++;
 }else cache.hits++;
 c.drawImage(entry.canvas,entry.x,entry.y,entry.width,entry.height);cache.standins.set(baseKey,entry);return true;
}
const render=ATSRenderer.prototype.draw;ATSRenderer.prototype.draw=function(...args){this._outfitNewFrames=0;return render.apply(this,args)};
window.ATSOutfitArt={version:2,ready,pose,armor,frame,actor};
})();
