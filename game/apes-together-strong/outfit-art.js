/* Dedicated illustrated bodies. Armor selection is an atlas lookup; only held
 * weapons/tools are composed into a bounded cache shared by all matching apes. */
(() => {
'use strict';
const art=()=>window.ATSVisualAssets,meta=()=>art()?.manifest?.['armor-sprites'];
const tribes=['ironback','shadowhand','emberwood'],species=['gorilla','chimpanzee','orangutan','gibbon','mandrill','capuchin'];
const looks={age1Royal:'age1',age2Royal:'age2',commandRoyal:'command',powerRoyal:'power',shadowRoyal:'shadow'};
const ready=()=>!!art()?.get('characters-armor-gorilla-base');
function atlasFor(a,p={}){
 if(p.king||a.id==='king'){const tribe=tribes.includes(p.tribeId||a.tribeId)?p.tribeId||a.tribeId:'ironback',id=window.ATSArmorProgression?.canonical(a.outfit?.royal)||a.outfit?.royal;return 'armor-'+tribe+'-'+(looks[id]||'base')}
 const id=window.ATSArmorProgression?.canonical(a.outfit?.body)||a.outfit?.body,item=window.ATSOutfitItems?.[id],kind=species.includes(a.species)?a.species:'gorilla';
 return 'armor-'+kind+'-'+(item?.grade?'tier'+Math.min(3,item.grade):'base');
}
function pose(a,p,options,time){
 if(p.human)return p;let atlas=atlasFor(a,{...options,king:p.king});if(!meta()?.sheets?.[atlas])return p;
 if(!art().get('characters-'+atlas)){atlas=atlas.replace(/-[^-]+$/,'-base');if(!art().get('characters-'+atlas))return p}const spec=meta().sheets[atlas];
 const d=[2,1,0,1,2,3,4,3][p.direction],step=p.beat>=2?1:0;let column=0,action=false,state=p.state,worker=false;
 switch(state){
  case'run':case'sprint':column=(a.carrying||a._carryingWood?5:state==='sprint'?3:1)+step;break;
  case'carry':column=5+(a.moving?step:0);break;
  case'hurt':case'knockback':column=7;break;
  case'attack':case'chargeAttack':action=true;column=p.phase<.3?1:p.phase<.7?0:1;break;
  case'throw':action=true;column=p.phase<.525?0:1;break;
  case'heavyAttack':action=true;column=p.phase<.7?2:0;break;
  case'climb':action=true;column=3+p.beat%2;break;
  case'prepare':action=true;column=0;break;
  case'command':case'victory':action=true;column=5;break;
  case'death':action=true;column=p.phase<.65?6:7;break;
 }
 if(!a.moving&&a.hp>0&&!a.hitTimer&&!a.attackTimer&&['idle','carry'].includes(state)&&/chop|build|repair|garden|gather|cook|saw|woodwork|clearing/.test(a.activity||'')){
  action=true;worker=true;
  if(/chop|clearing/.test(a.activity)){column=p.phase<.45?0:2;state='chopping'}
  else if(/garden|gather/.test(a.activity)){column=p.phase<.5?0:2;state='gathering'}
  else if(/cook/.test(a.activity)){column=p.phase<.5?0:1;state='cooking'}
  else if(/saw|woodwork/.test(a.activity)){column=p.phase<.5?0:1;state='sawing'}
  else{column=p.phase<.5?0:2;state='building'}
 }
 if(options.reducedMotion&&a.moving){action=false;column=a.carrying?5:0}
 const mapping=(action?spec.actionRows:spec.motionRows),row=mapping[d],override=spec.overrides?.[(action?'a':'m')+d+':'+column];
 return {...p,atlas,row:override?.[0]??row,column:override?.[1]??column,state,worker,tribe:options.tribeId||a.tribeId,sourceDirectionCount:8,illustratedArmor:true};
}
function armor(a,p){const atlas=atlasFor(a,p),item=window.ATSOutfitItems?.[window.ATSArmorProgression?.canonical(a.outfit?.[p.king?'royal':'body'])];return item?{atlas,grade:item.grade,fullBody:true}:null}
function frame(){return false}
// Complete repeated loadouts are one draw in the crowd, including front/rear
// items and hand occlusion. Sources are composed only on appearance changes.
function actor(r,c,a,p,slot,g,image,items){
 if(!g||!image||!items?.length)return false;
 const registry=art(),low=r.quality==='low',density=p.king?2:Math.min(2,Math.max(1,Math.ceil((r.camera?.zoom||1)*(a.champion?1.5:r.useApeAtlas||low?1:1.25)*4)/4));let cache=r.loadoutComposites;
 if(!cache||cache.registry!==registry||cache.low!==low)cache=r.loadoutComposites={registry,low,entries:new Map(),standins:new Map(),pixels:0,hits:0,misses:0,deferred:0};
 const itemKey=items?.visualKey||items?.map(i=>i.id+':'+i.socket+':'+i.layer+':'+(i.damage===undefined?'':i.damage<.3?2:i.damage<.65?1:0)).join(',')||'';
 const baseKey=[p.atlas,p.row,p.state,slot,density,p.rear,low?0:a.coatVariant||0,itemKey].join('|'),key=p.column+'|'+baseKey;let entry=cache.entries.get(key);if(entry&&entry.source!==image){cache.pixels-=entry.pixels;entry.canvas.close?.();cache.entries.delete(key);entry=null}
 if(!entry){
  const prior=cache.standins.get(baseKey);if(!p.king&&!a.champion&&['run','sprint','idle'].includes(p.state)&&prior?.source===image&&(r._outfitNewFrames||0)>=8){c.drawImage(prior.canvas,prior.x,prior.y,prior.width,prior.height);cache.deferred++;return true}
  r._outfitNewFrames=(r._outfitNewFrames||0)+1;
  const held=window.ATSHeldItemArt,pose={...p,phase:p.state==='throw'&&p.column===1?.75:0},anchors=held?.sockets(a,p,slot);let left=g.x-3,top=g.y-3,right=g.x+g.width+3,bottom=g.y+g.height+3;
  for(const item of items||[]){let f=art().manifest['held-items'].frames[item.id];if(['buckler','ironShield','ancientShield'].includes(item.id))f=art().manifest['held-items'].frames[item.id+((p.rear?3:0)+(item.damage<.3?2:item.damage<.65?1:0))];if(item.id==='logShield')f={drawWidth:48,drawHeight:48,w:1,h:1,gripX:.5,gripY:.62};if(!f||!anchors)continue;const at=held.placement(a,pose,item,anchors),w=f.drawWidth,h=f.drawHeight,x=-f.gripX/f.w*w,y=-f.gripY/f.h*h,cos=Math.cos(at.rotation),sin=Math.sin(at.rotation);for(const [u,v]of [[x,y],[x+w,y],[x,y+h],[x+w,y+h]]){const px=at.x+u*cos-v*sin,py=at.y+u*sin+v*cos;left=Math.min(left,px-2);right=Math.max(right,px+2);top=Math.min(top,py-2);bottom=Math.max(bottom,py+2)}}
  const width=Math.ceil(right-left),height=Math.ceil(bottom-top),pixels=Math.ceil(width*density)*Math.ceil(height*density),limit=low?10000000:16000000;
  while(cache.entries.size>=(low?512:1024)||cache.pixels+pixels>limit){const first=cache.entries.keys().next().value;if(first===undefined)break;cache.pixels-=cache.entries.get(first).pixels;const old=cache.entries.get(first);if(cache.standins.get(old.baseKey)===old)cache.standins.delete(old.baseKey);old.canvas.close?.();cache.entries.delete(first)}
  const canvas=typeof OffscreenCanvas==='function'?new OffscreenCanvas(Math.ceil(width*density),Math.ceil(height*density)):document.createElement('canvas');canvas.width=Math.ceil(width*density);canvas.height=Math.ceil(height*density);const cc=canvas.getContext('2d'),originX=-left,originY=-top;cc.scale(density,density);cc.translate(originX,originY);const f=g.f;
  if(items)held.draw(r,cc,a,pose,slot,'rear',items);cc.drawImage(image,f.x,f.y,f.w,f.h,g.x,g.y,g.width,g.height);
  if(items){held.draw(r,cc,a,pose,slot,'front',items);held.restoreHands(r,cc,a,pose,slot,items,image)}
  entry={baseKey,source:image,canvas:canvas.transferToImageBitmap?.()||canvas,pixels,x:-originX,y:-originY,width,height};cache.entries.set(key,entry);cache.pixels+=pixels;cache.misses++;
 }else cache.hits++;
 c.drawImage(entry.canvas,entry.x,entry.y,entry.width,entry.height);cache.standins.set(baseKey,entry);return true;
}
const render=ATSRenderer.prototype.draw;ATSRenderer.prototype.draw=function(...args){this._outfitNewFrames=0;return render.apply(this,args)};
window.ATSOutfitArt={version:3,ready,atlasFor,pose,armor,frame,actor};
})();
