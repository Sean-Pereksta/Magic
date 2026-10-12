/* Illustrated tribal buildings, exact-footprint palisades and harvested trees. */
(() => {
'use strict';
const P=ATSRenderer.prototype,tribes=['ironback','shadowhand','emberwood'],clamp=(x,a,b)=>Math.max(a,Math.min(b,x));
const assets=()=>window.ATSVisualAssets,ready=()=>!!assets()?.get('outfits-tribal-structures');
function tile(r,c,column,width,height,y=0){const row=Math.max(0,tribes.indexOf(r._tribalGame?.tribe.id||'ironback')),f=assets()?.manifest.outfits?.frames?.['tribal-structures']?.[row+':'+column],im=assets()?.get('outfits-tribal-structures');if(!im||!f)return false;c.drawImage(im,f.x,f.y,f.w,f.h,-width/2,y-height,width,height);return true}
function structure(r,c,kind,w,h,y){const col=kind==='lodge'?0:['lookout','spearTower','spearBattery'].includes(kind)?1:kind==='workShelter'||kind==='workshop'?5:-1;return col>=0&&tile(r,c,col,w,h,y)}
function env(c,name,w,h,x=0,y=0){const f=assets()?.manifest.environment?.frames?.[name],im=f&&assets().get(f.atlas);if(!f||!im)return false;c.drawImage(im,...f.rect,x-f.anchor[0]/f.rect[2]*w,y-f.anchor[1]/f.rect[3]*h,w,h);return true}
function fall(r,c,o){const age=(r.time||0)-(o.clearedAt||0);if(age<0||age>3.2||r.reducedMotion||r.quality==='low'||r._fallBudget<=0)return false;r._fallBudget--;
 const variants=assets()?.manifest.environment?.treeVariants;if(!variants?.length)return false;let seed=0;for(const ch of String(o.id||''))seed=(Math.imul(seed,31)+ch.charCodeAt(0))|0;seed>>>=0;const name=variants[seed%variants.length],s=clamp((o.r||27)/30,.58,1.22),width=(name==='sapling'?147:name==='conifer'?153:188)*s,height=(name==='sapling'?183:name==='conifer'?236:220)*s;
 const t=clamp(age/1.1,0,1),angle=(seed%2?1:-1)*Math.PI*.48*t*t;c.save();c.rotate(angle);if(age>2.3)c.globalAlpha*=clamp((3.2-age)/.9,0,1);env(c,name,width,height);c.restore();if(age>1.1)env(c,'fallenLog',55*s,29*s,0,5);return true;
}
const draw=P.draw;P.draw=function(g,...args){this._fallBudget=this.quality==='low'||this.reducedMotion?0:8;this._workerImpactBudget=this.quality==='low'||this.reducedMotion?0:18;return draw.call(this,g,...args)};
const barrier=P.drawBarrierGeometry;P.drawBarrierGeometry=function(c,o,damage){if(o.team!=='ape'||damage===3||!ready())return barrier.call(this,c,o,damage);const length=Math.max(o.w||74,o.h||16),width=length*.8+8,height=length*.42+40;c.save();if((o.w||74)>=(o.h||16))c.scale(-1,1);tile(this,c,o.gate?4:damage>0?3:2,width,height,length*.21+3);c.restore()};
const worker=P.drawWorkerDetail;P.drawWorkerDetail=function(c,a){worker?.call(this,c,a);if(a.moving||a.hp<=0||!(/chop|build|repair/.test(a.activity||''))||this._workerImpactBudget<=0)return;const phase=((this.time*1.8+(a.phase||0))%1+1)%1;if(phase<.48||phase>.66)return;this._workerImpactBudget--;const p=window.ATSCharacterArt?.resolve(a,this.time,{tribeId:this._tribalGame?.tribe.id});if(!p?.worker)return;const slot=[76,66,78,64,63,49][Math.max(0,['gorilla','chimpanzee','orangutan','gibbon','mandrill','capuchin'].indexOf(a.species))],at=window.ATSHeldItemArt?.sockets(a,p,slot)?.right;if(!at)return;c.save();c.scale((p.mirror?-1:1)*(a.bodyScale||1),a.bodyScale||1);c.fillStyle='#c9ab73';const t=(phase-.48)/.18;for(let i=0;i<3;i++)c.fillRect(at.x+(i-1)*t*13,at.y-8-t*(9+i*3),2,1.4);c.restore()};
window.ATSLivingArt={ready,structure,fall};
})();
