import { VIEW } from '../config.js';
import { telegraphShapes } from '../bosses/boss-attacks.js';
import { biomeKey, HAZARD_ART, sampleFrame, namedFrame, AssetLoadError } from '../assets/asset-manager.js';
import { INTERIOR_TEXTURE_RECTS } from '../assets/terrain-textures.js';
import { ART_ALPHA_BOUNDS, PLATFORM_SURFACES } from '../assets/art-anchors.js';

const CELL=512, TILE=64, PLATFORM_CONTACT_INSET=8;
const EFFECT_ALIASES={'fx-impact':'fx-gold-impact','fx-enemy-death':'fx-damage-burst','hit-spark':'fx-gold-impact',jump:'fx-jump-dust',land:'fx-landing-dust',dash:'fx-dash-trail',pickup:'fx-pickup-collect',checkpoint:'fx-checkpoint-activation',hurt:'fx-damage-burst',attack:'fx-blessing-attack-arc'};
const PROJECTILE_ART={orb:'fx-radial-orb',fire:'fx-fire-meteor',shockwave:'fx-ground-shockwave',javelin:'projectile-javelin',light:'projectile-lightning',lightning:'projectile-lightning',debris:'projectile-stone',stone:'projectile-stone',tablet:'projectile-tablet',thorn:'projectile-thorn',sweep:'fx-boss-sweep-slam',judgment:'fx-holy-light-judgment',eruption:'fx-fire-eruption',attackZone:'fx-boss-sweep-slam'};
const PICKUP_ART={faith:'faith-light',light:'faith-light',coin:'faith-light',bread:'bread',fish:'fish',scroll:'scroll',relic:'sun-relic',sunRelic:'sun-relic',shield:'ability-shield',sandals:'ability-sandals',wings:'ability-wings',holyLight:'ability-holy-light',fireRain:'ability-fire-rain',chest:'secret-chest-closed',secret:'secret-chest-closed'};
const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
const mix=(a,b,t)=>a+(b-a)*t;
function position(e,alpha) {return {x:mix(e.prevX??e.previousX??e.x,e.x,alpha),y:mix(e.prevY??e.previousY??e.y,e.y,alpha)};}
function visible(e,v,pad=100) {return e.x+(e.w||180)>v.x-pad&&e.x<v.x+v.w+pad&&e.y+(e.h||180)>v.y-pad&&e.y<v.y+v.h+pad;}
function imageRect(ctx,image,r,x,y,w,h) {if(w>0&&h>0)ctx.drawImage(image,r.x,r.y,r.width,r.height,x,y,w,h);}
function content(frame) {const c=frame.tile?.drawContentRect||frame.contentBounds;return c?{x:frame.rect.x+c.x,y:frame.rect.y+c.y,width:c.width,height:c.height}:frame.rect;}
export function groundedArtPlacement(meta,frame,centerX,groundY,frameSize,visualWidth=null,visualHeight=null) {
  const b=ART_ALPHA_BOUNDS[`${meta.id}:${frame.index}`]||frame.contentBounds||{x:0,y:0,width:frame.rect.width,height:frame.rect.height};
  const scale=frameSize/frame.rect.width,sx=visualWidth?visualWidth/b.width:scale,sy=visualHeight?visualHeight/b.height:sx;
  const pivot=frame.origin||meta.origin;
  return {source:{x:frame.rect.x+b.x,y:frame.rect.y+b.y,width:b.width,height:b.height},x:centerX+(b.x-pivot.x)*sx,y:groundY-b.height*sy,w:b.width*sx,h:b.height*sy};
}

export class Renderer {
  constructor(canvas,assets) {
    this.canvas=canvas;this.ctx=canvas.getContext('2d',{alpha:false});this.assets=assets;
    this.terrainCache=new Map();this.missingAssets=new Set();this.lastWorld=null;
    this.quality=1;this.drawnSprites=0;this.frameCost=0;this.ctx.imageSmoothingEnabled=true;
  }
  get error(){return this.missingAssets.size?new AssetLoadError([...this.missingAssets]):null;}
  clearCache(){this.terrainCache.clear();}
  _asset(id,required=true){const e=this.assets?.get(id);if(!e&&required)this.missingAssets.add(id);return e;}
  draw(game,alpha=1) {
    if(!game.world||!game.player||!this.assets)return;
    const started=performance.now(),ctx=this.ctx,world=game.world,time=game.time||0;
    alpha=clamp(alpha,0,1);this.missingAssets.clear();this.drawnSprites=0;
    const biome=biomeKey(world.biome||world.region),camera=game.camera||{x:0,y:0},cam=position(camera,alpha),view={x:cam.x,y:cam.y,w:VIEW.width,h:VIEW.height};
    if(this.lastWorld!==world){this.clearCache();this.lastWorld=world;}
    ctx.setTransform(this.canvas.width/VIEW.width,0,0,this.canvas.height/VIEW.height,0,0);
    ctx.globalAlpha=1;ctx.globalCompositeOperation='source-over';ctx.clearRect(0,0,VIEW.width,VIEW.height);
    this._background(biome,cam);
    ctx.save();const shake=camera.shake||0;ctx.translate(-cam.x+Math.sin(time*117)*shake*.4,-cam.y+Math.sin(time*93)*shake*.25);
    for(const e of world.scenery||[])if(visible(e,view,220))this._decoration(e,biome,time,true);
    this._terrain(world,biome,view);
    for(const p of world.platforms||[])if(!p.dead&&['collapsing','collapse'].includes(p.kind)&&visible(p,view))this._dynamicPlatform(p,biome,time,alpha);
    for(const p of world.movers||[])if(!p.dead&&visible(p,view))this._dynamicPlatform(p,biome,time,alpha,true);
    for(const e of world.props||[])if(!e.dead&&visible(e,view,180))this._decoration(e,biome,time,false);
    for(const e of world.switches||[])if(visible(e,view,60))this._switch(e,time);
    for(const e of world.hazards||[])if(!e.dead&&visible(e,view,180))this._hazard(e,time);
    this._checkpoints(game,view,time);this._telegraphs(game,view,time);
    for(const p of world.pickups||[])if(!p.collected&&visible(p,view,50))this._pickup(p,time);
    for(const e of world.enemies||[])if(visible(e,view,180)&&(!e.dead||(e.fade??1)>0))this._enemy(e,time,alpha);
    const boss=game.bossController?.boss;if(boss&&visible(boss,view,300))this._boss(boss,time,alpha);
    for(const e of [...(world.enemyProjectiles||[]),...(world.bossHazards||[])])if(visible(e,view,160))this._projectile(e,time,alpha);
    this._player(game.player,time,alpha);this._effects(world.effects||[],view);this._exit(game,view);
    ctx.restore();this._weather(biome,cam,time,world.weather);
    if(game.debug)this.drawDebug(game);
    this.frameCost=this.frameCost*.94+(performance.now()-started)*.06;
    if(this.frameCost>13)this.quality=Math.max(.45,this.quality-.015);else if(this.frameCost<7)this.quality=Math.min(1,this.quality+.004);
    return {missingAssets:[...this.missingAssets],sprites:this.drawnSprites,quality:this.quality};
  }
  _background(biome,camera) {
    for(const layer of ['sky','far','near']) {
      const entry=this._asset(`${biome}-background-${layer}`);if(!entry)continue;
      const {image,meta}=entry,p=meta.parallax,h=layer==='sky'?VIEW.height:VIEW.height+80,scale=h/meta.height;
      const width=meta.width*scale,step=width-(p.overlapPixels||0)*scale,offset=camera.x*p.speed,first=Math.floor(offset/step)-1;
      const y=layer==='sky'?0:Math.max(VIEW.floorY+88,VIEW.height+48)-h-camera.y*p.speed;
      for(let i=first;i<first+4;i++) {
        const x=i*step-offset,mirror=p.alternateMirrorX&&Math.abs(i%2)===1;
        this.ctx.save();this.ctx.translate(x+(mirror?width:0),y);this.ctx.scale(mirror?-1:1,1);
        imageRect(this.ctx,image,meta.frames[0].rect,0,0,width,h);this.ctx.restore();
      }
    }
  }
  _terrain(world,biome,view) {
    const platforms=(world.platforms||[]).filter(p=>!p.dead&&!['collapsing','collapse'].includes(p.kind));
    for(let gy=Math.floor((view.y-32)/CELL);gy<=Math.floor((view.y+view.h)/CELL);gy++)for(let gx=Math.floor(view.x/CELL);gx<=Math.floor((view.x+view.w)/CELL);gx++) {
      const x=gx*CELL,y=gy*CELL,parts=platforms.filter(p=>p.x+p.w>x-18&&p.x<x+CELL+18&&Math.max(p.y+(p.oneWay?100:p.h||64),p.supportBottom||0)>y-32&&p.y<y+CELL+100);
      if(!parts.length)continue;
      const key=`${biome}/${gx}/${gy}/${parts.map(p=>`${p.id}:${p.x}:${p.y}:${p.w}:${p.h}:${p.visualKind}:${p.supportBottom}:${p.supportStyle}`).join('|')}`;
      let cell=this.terrainCache.get(key);
      if(!cell) {
        const canvas=document.createElement('canvas');canvas.width=CELL;canvas.height=CELL;
        const local=canvas.getContext('2d');local.translate(-x,-y);const original=this.ctx;this.ctx=local;
        for(const p of parts)this._staticPlatform(p,biome,{x,y,w:CELL,h:CELL});
        this.ctx=original;cell={canvas,lastUsed:0};if(!this.missingAssets.size)this.terrainCache.set(key,cell);
      }
      cell.lastUsed=performance.now();this.ctx.drawImage(cell.canvas,x,y);
    }
    if(this.terrainCache.size>12) {
      const oldest=[...this.terrainCache.entries()].sort((a,b)=>a[1].lastUsed-b[1].lastUsed);
      for(let i=0;i<oldest.length-12;i++)this.terrainCache.delete(oldest[i][0]);
    }
  }
  _staticPlatform(p,biome,view) {
    const blocks=this._asset(`${biome}-blocks`),terrain=this._asset(`${biome}-terrain`);if(!blocks||!terrain)return;
    const ctx=this.ctx;
    if(p.kind==='slope') {
      const frame=namedFrame(terrain.meta,p.slopeDirection==='down'?'slope-down':'slope-up'),rise=p.rise||p.h,up=p.slopeDirection!=='down';
      ctx.save();ctx.beginPath();ctx.moveTo(p.x,p.y+(up?rise:0));ctx.lineTo(p.x+p.w,p.y+(up?0:rise));ctx.lineTo(p.x+p.w,p.y+p.h);ctx.lineTo(p.x,p.y+p.h);ctx.closePath();ctx.clip();
      this._fillTiles(blocks,p.x,p.y,p.w,p.h,view);if(frame)imageRect(ctx,terrain.image,content(frame),p.x,p.y,p.w,rise+1);ctx.restore();return;
    }
    if(p.artFrame&&namedFrame(terrain.meta,p.artFrame)) {imageRect(ctx,terrain.image,content(namedFrame(terrain.meta,p.artFrame)),p.x,p.y-5,p.w,p.h+5);return;}
    if(p.kind==='wall'||p.kind==='gate') {
      const a=namedFrame(blocks.meta,'wall-a'),b=namedFrame(blocks.meta,'wall-b');
      ctx.save();ctx.beginPath();ctx.rect(p.x,p.y,p.w,p.h);ctx.clip();
      for(let y=p.y;y<p.y+p.h;y+=TILE)imageRect(ctx,blocks.image,content(Math.floor(y/TILE)%2?a:b),p.x,y,p.w,TILE+1);
      ctx.restore();return;
    }
    if(p.oneWay) {this._oneWayPlatform(p,biome,terrain);return;}
    ctx.save();ctx.beginPath();ctx.rect(p.x,p.y-8,p.w,p.h+8);ctx.clip();
    this._fillTiles(blocks,p.x,p.y+56,p.w,Math.max(0,p.h-56),view);
    const a=namedFrame(blocks.meta,'top-middle-a'),b=namedFrame(blocks.meta,'top-middle-b'),start=Math.max(0,Math.floor((view.x-p.x)/TILE));
    for(let i=start;i*TILE<p.w&&p.x+i*TILE<view.x+view.w+TILE;i++)imageRect(ctx,blocks.image,content(i%3===1?b:a),p.x+i*TILE,p.y-8,TILE+1,64);
    ctx.restore();
  }
  _oneWayPlatform(p,biome,terrain) {
    const ctx=this.ctx,structures=this._asset(`${biome}-structures`);if(!structures)return;
    // Feet rest inside the illustrated top plane rather than on its rear edge.
    const top=p.y-PLATFORM_CONTACT_INSET;
    const type=p.visualKind||'ledge',bottom=Math.max(p.y+70,p.supportBottom||p.y+150);
    if(type==='bridge') {
      const frame=namedFrame(structures.meta,'bridge'),b=ART_ALPHA_BOUNDS[`${structures.meta.id}:${frame.index}`]||frame.contentBounds;
      const deck=PLATFORM_SURFACES[`${structures.meta.id}:${frame.index}`],crop={x:frame.rect.x+b.x,y:frame.rect.y+b.y,width:b.width,height:b.height};
      const sx=p.w/deck.spanWidth,x=p.x+(b.x-deck.spanX)*sx,w=b.width*sx;
      const deckHeight=Math.min(34,p.h+6),deckScale=deckHeight/Math.max(1,deck.bottomY-deck.y);
      // The support body sits behind the traversable deck. Its reduced contrast
      // identifies posts and braces as background, leaving the platform top clear.
      ctx.save();ctx.globalAlpha=.54;
      const above={x:crop.x,y:crop.y,width:crop.width,height:Math.max(0,deck.y-b.y)};
      imageRect(ctx,structures.image,above,x,top-above.height*sx,w,above.height*sx);
      const below={x:crop.x,y:frame.rect.y+deck.bottomY,width:crop.width,height:Math.max(0,b.y+b.height-deck.bottomY)};
      imageRect(ctx,structures.image,below,x,top+deckHeight,w,bottom-top-deckHeight);ctx.restore();
      const deckCrop={x:crop.x,y:frame.rect.y+deck.y,width:crop.width,height:Math.max(1,deck.bottomY-deck.y)};
      imageRect(ctx,structures.image,deckCrop,x,top,w,deckCrop.height*deckScale);
      return;
    }
    if(type==='supported') {
      ctx.save();ctx.globalAlpha=.4;
      const supportStyle=p.supportStyle||(biome==='city'||biome==='ruins'?'arch':biome==='mountains'||biome==='storm'?'rock':'posts');
      if(supportStyle==='arch') {
        const frame=namedFrame(structures.meta,'arch'),crop=content(frame);
        imageRect(ctx,structures.image,crop,p.x+8,p.y+6,p.w-16,bottom-p.y-6);
      } else if(supportStyle==='posts') {
        const frame=namedFrame(structures.meta,'bridge'),crop=content(frame),deck=PLATFORM_SURFACES[`${structures.meta.id}:${frame.index}`];
        const top=frame.rect.y+deck.bottomY,below={x:crop.x,y:top,width:crop.width,height:crop.y+crop.height-top};
        imageRect(ctx,structures.image,below,p.x+8,p.y+18,p.w-16,bottom-p.y-18);
      } else {
        const frame=namedFrame(terrain.meta,'spire'),crop=content(frame),width=Math.min(72,p.w*.4);
        imageRect(ctx,terrain.image,crop,p.x+(p.w-width)/2,p.y+15,width,bottom-p.y-15);
      }
      ctx.restore();
    }
    // Anchor the opaque stone deck, not the estimated collider or grass tips,
    // to the landing plane used by characters, pickups, and decorations.
    const frame=namedFrame(terrain.meta,'ledge'),key=`${terrain.meta.id}:${frame.index}`;
    const b=ART_ALPHA_BOUNDS[key]||frame.contentBounds,c=frame.collider;
    const crop={x:frame.rect.x+b.x,y:frame.rect.y+b.y,width:b.width,height:b.height};
    const surfaceY=PLATFORM_SURFACES[key]?.y??c.y;
    const sx=p.w/c.width,depth=type==='supported'?48:60,sy=depth/(b.y+b.height-surfaceY);
    imageRect(ctx,terrain.image,crop,p.x+(b.x-c.x)*sx,top+(b.y-surfaceY)*sy,b.width*sx,b.height*sy);
  }
  _fillTiles(entry,x,y,w,h,view) {
    const a=namedFrame(entry.meta,'fill-a'),b=namedFrame(entry.meta,'fill-b');if(!a||!b||h<=0)return;
    const sx=Math.max(0,Math.floor((view.x-x)/TILE)),sy=Math.max(0,Math.floor((view.y-y)/TILE));
    for(let row=sy;row*TILE<h&&y+row*TILE<view.y+view.h+TILE;row++)for(let col=sx;col*TILE<w&&x+col*TILE<view.x+view.w+TILE;col++) {
      const frame=(row*7+col*11)%5<2?a:b;
      const source=INTERIOR_TEXTURE_RECTS[`${entry.meta.id}:${frame.index}`]||content(frame),tw=Math.min(TILE+1,w-col*TILE),th=Math.min(TILE+1,h-row*TILE);
      imageRect(this.ctx,entry.image,{...source,width:source.width*tw/(TILE+1),height:source.height*th/(TILE+1)},x+col*TILE,y+row*TILE,tw,th);
    }
  }
  _dynamicPlatform(p,biome,time,alpha,moving=false) {
    const entry=this._asset(`${biome}-structures`);if(!entry)return;
    const frame=namedFrame(entry.meta,moving?'moving-platform':'collapse-platform');if(!frame)return;
    const point=position(p,alpha),crop=content(frame),b=frame.contentBounds,c=frame.collider;
    const surface=PLATFORM_SURFACES[`${entry.meta.id}:${frame.index}`],surfaceY=surface?.y??c.y,sx=p.w/c.width;
    const above=surfaceY-b.y,below=b.y+b.height-surfaceY;
    const sy=moving?Math.max(.35,Math.min(2,(p.supportTop?point.y-p.supportTop:above*sx)/Math.max(1,above))):Math.min(.75,70/Math.max(1,below));
    const shake=p.collapseTimer>0?Math.sin(time*65)*2.2:0;
    this.ctx.save();this.ctx.globalAlpha=p.falling?Math.max(.2,1-(p.fallTime||0)):1;
    imageRect(this.ctx,entry.image,crop,point.x+(b.x-c.x)*sx+shake,point.y-PLATFORM_CONTACT_INSET+(b.y-surfaceY)*sy,b.width*sx,b.height*sy);this.ctx.restore();
  }
  _decoration(e,biome,time,background) {
    if(e.kind==='checkpoint')return;
    const structures=['bridge','moving-platform','collapse-platform','stairs','gate-closed','gate-open','arch','landmark'];
    let name=e.artFrame||e.sprite||e.kind||e.type;
    if(name==='breakableColumn')name='spire';
    const entry=this._asset(`${biome}-${e.structure||structures.includes(name)?'structures':'terrain'}`);if(!entry)return;
    const frame=namedFrame(entry.meta,name);if(!frame)return;
    const bounds=ART_ALPHA_BOUNDS[`${entry.meta.id}:${frame.index}`];
    const crop=bounds?{x:frame.rect.x+bounds.x,y:frame.rect.y+bounds.y,width:bounds.width,height:bounds.height}:content(frame);
    const scale=e.scale||1,w=e.w||crop.width*.65*scale,h=e.h||crop.height*.65*scale,ctx=this.ctx;
    ctx.save();ctx.globalAlpha=e.opacity??(background?.8:1);
    // Generated prop coordinates identify the ground contact; explicit topAligned is available for wall decorations.
    ctx.translate(e.x+(e.flip?w:0),(e.y??VIEW.floorY)-(e.topAligned||e.type==='breakableColumn'?0:h));ctx.scale(e.flip?-1:1,1);
    if(/grass|wheat|reeds|flowers|fern|shrub/.test(name))ctx.transform(1,0,Math.sin(time*1.7+(e.phase||e.x))*.025,1,0,0);
    imageRect(ctx,entry.image,crop,0,0,w,h);ctx.restore();this.drawnSprites++;
  }
  _hazard(h,time) {
    const id=h.artId||HAZARD_ART[h.type]||(h.type?.startsWith('hazard-')?h.type:null);if(!id)return;
    const entry=this._asset(id);if(!entry)return;
    const frame=sampleFrame(entry.meta,h.active===false?0:time+(h.phase||0)),ctx=this.ctx;
    ctx.save();ctx.globalAlpha=['wind','chase','windGust'].includes(h.type)?.3:h.active===false?.42:1;
    if(id==='hazard-water-surface') {
      ctx.beginPath();ctx.rect(h.x,h.y,h.w,Math.max(h.h,32));ctx.clip();
      for(let x=h.x;x<h.x+h.w;x+=128)imageRect(ctx,entry.image,frame.rect,x,h.y-22,128,72);
    } else {
      const c=frame.collider||entry.meta.collider,sx=(h.w||64)/(c?.width||frame.rect.width),sy=(h.h||50)/(c?.height||frame.rect.height);
      imageRect(ctx,entry.image,frame.rect,h.x-(c?.x||0)*sx,h.y-(c?.y||0)*sy,frame.rect.width*sx,frame.rect.height*sy);
    }
    ctx.restore();this.drawnSprites++;
  }
  _pickup(p,time) {
    if(p.type==='water') {
      const ctx=this.ctx,x=p.x+(p.w||28)/2,y=p.y+(p.h||28)/2+Math.sin(time*3.4+p.x*.02)*3;
      ctx.save();ctx.translate(x,y);ctx.shadowColor='#65dfff';ctx.shadowBlur=12;
      const fill=ctx.createLinearGradient(0,-22,0,20);fill.addColorStop(0,'#edffff');fill.addColorStop(.45,'#58d9f4');fill.addColorStop(1,'#1677b9');
      ctx.fillStyle=fill;ctx.strokeStyle='#ecddad';ctx.lineWidth=1.5;ctx.beginPath();ctx.moveTo(0,-22);
      ctx.bezierCurveTo(-4,-12,-17,-1,-16,8);ctx.bezierCurveTo(-15,25,16,25,16,8);ctx.bezierCurveTo(17,-1,4,-12,0,-22);ctx.fill();ctx.stroke();
      ctx.shadowBlur=0;ctx.strokeStyle='#e1ffff';ctx.lineWidth=2;ctx.beginPath();ctx.moveTo(-7,-3);ctx.quadraticCurveTo(-12,6,-7,11);ctx.stroke();ctx.restore();this.drawnSprites++;return;
    }
    const entry=this._asset('collectibles-atlas');if(!entry)return;
    const frame=namedFrame(entry.meta,p.artFrame||(p.type==='wind'?'ability-sandals':PICKUP_ART[p.type])||p.type);if(!frame)return;
    const size=p.type==='chest'?70:46;
    if(p.grounded) {
      const at=groundedArtPlacement(entry.meta,frame,p.x+(p.w||28)/2,p.groundY??p.y+p.h,size,p.visualWidth,p.visualHeight);
      imageRect(this.ctx,entry.image,at.source,at.x,at.y,at.w,at.h);this.drawnSprites++;return;
    }
    const bob=p.type==='chest'?0:Math.sin(time*3.4+p.x*.02)*4;
    imageRect(this.ctx,entry.image,frame.rect,p.x+(p.w||28)/2-size/2,p.y+(p.h||28)/2-size/2+bob,size,size);this.drawnSprites++;
  }
  _switch(s,time) {
    const entry=this._asset('collectibles-atlas');if(!entry)return;
    const frame=namedFrame(entry.meta,s.activated?'sun-relic':'checkpoint-lantern');
    const at=groundedArtPlacement(entry.meta,frame,s.x+s.w/2,s.groundY??s.y+s.h,s.w+16);
    imageRect(this.ctx,entry.image,at.source,at.x,at.y,at.w,at.h);
    if(s.activated)this._effect({type:'fx-gold-impact',x:s.x+s.w/2,y:s.y+s.h,time:time%.7,scale:.24},false);
  }
  _checkpoints(game,view,time) {
    if(!game.world.checkpoints?.length)return;
    const entry=this._asset('collectibles-atlas');if(!entry)return;
    for(const c of game.world.checkpoints||[]) {
      if(!visible({...c,y:c.y-70,h:140},view,160))continue;
      const frame=namedFrame(entry.meta,'checkpoint-shrine'),size=112,floor=c.floorY??c.y+game.player.h,x=c.x+(c.w||34)/2;
      const at=groundedArtPlacement(entry.meta,frame,x,floor,size);
      this.ctx.save();this.ctx.globalAlpha=c.activated||c.active?1:.72;imageRect(this.ctx,entry.image,at.source,at.x,at.y,at.w,at.h);this.ctx.restore();
      if(c.activated||c.active)this._effect({type:'fx-checkpoint-activation',x,y:floor,time:time%1.2,scale:.55},false);
    }
  }
  _sprite(id,e,seconds,alpha=1,opacity=1) {
    const entry=this._asset(id);if(!entry)return;
    const frame=sampleFrame(entry.meta,seconds),point=position(e,alpha),pivot=frame.origin||entry.meta.origin,c=frame.collider||entry.meta.collider,scale=e.spriteScale||e.h/(c?.height||frame.rect.height),ctx=this.ctx;
    ctx.save();ctx.globalAlpha=opacity;ctx.translate(point.x+e.w/2,point.y+e.h);ctx.scale((e.facing||e.patrol||1)*scale,scale);
    imageRect(ctx,entry.image,frame.rect,-pivot.x,-pivot.y,frame.rect.width,frame.rect.height);ctx.restore();this.drawnSprites++;
  }
  _player(p,time,alpha) {
    const state=p.state||(p.onGround?(Math.abs(p.vx)>20?'run':'idle'):(p.vy<0?'jump':'fall'));
    const alias={doubleJump:'double-jump',wallJump:'wall-jump',wallSlide:'wall-slide',holyLight:'holy-light',fireRain:'fire-rain',landing:'land',running:'run',falling:'fall',jumping:'jump',dead:'death'};
    this._sprite(`player.jesus.${alias[state]||state}`,p,p.animTime??p.stateTime??time,alpha,(p.invulnerable||0)>0&&Math.floor(time*16)%2?.45:1);
    if(p.dashTimer>0)this._effect({type:'fx-dash-trail',x:p.x+p.w/2-p.facing*24,y:p.y+p.h,time:p.animTime||0,scale:.36,facing:p.facing},false);
    if(p.boosts?.wind>0&&Math.abs(p.vx)>100)this._effect({type:'fx-sandals-speed-trail',x:p.x+p.w/2-p.facing*20,y:p.y+p.h,time:time%.6,scale:.26,facing:p.facing},false);
    if(p.powers?.shield&&p.shieldActive>0)this._effect({type:'fx-shield-pulse',x:p.x+p.w/2,y:p.y+p.h,time,scale:.46},false);
    if(p.state==='glide')this._effect({type:'fx-wings-flight-aura',x:p.x+p.w/2,y:p.y+p.h,time,scale:.4},false);
  }
  _enemy(e,time,alpha) {
    let state=e.animState||e.state||(Math.abs(e.vx||0)>2?'move':'idle');if(state==='attack2')state='attack';
    if(!['idle','move','attack','hurt','death'].includes(state))state=e.dead?'death':'move';
    this._sprite(`enemy-${e.spriteType||e.type}-${state}`,e,e.animTime??time,alpha,e.dead?clamp((e.fade??.6)/.6,0,1):1);
  }
  _boss(b,time,alpha) {
    const state=b.animState||({INTRO:'idle',IDLE:'idle',TELL:'attack',ACTIVE:'attack2',RECOVER:'idle',VULNERABLE:'hurt',HURT:'hurt',DEFEATED:'death'}[b.state]||'idle');
    this._sprite(`enemy-${b.spriteType||b.definition?.id||b.kind||b.id}-${state}`,b,b.animTime??b.stateTime??time,alpha,b.state==='DEFEATED'?clamp(b.fade??1,0,1):1);
  }
  _projectile(p,time,alpha) {
    if(p.active===false)return;const id=p.artId||(p.type==='fire'&&p.h>70?'fx-fire-eruption':PROJECTILE_ART[p.type]);if(!id)return;
    const entry=this._asset(id);if(!entry)return;
    const frame=sampleFrame(entry.meta,p.animTime??p.age??time),point=position(p,alpha),ctx=this.ctx,w=Math.max(32,p.w*1.65),h=Math.max(32,p.h*1.65);
    ctx.save();ctx.translate(point.x+p.w/2,point.y+p.h/2);if(['javelin','stone','tablet','thorn'].includes(p.type))ctx.rotate(Math.atan2(p.vy||0,p.vx||1));
    imageRect(ctx,entry.image,frame.rect,-w/2,-h/2,w,h);ctx.restore();this.drawnSprites++;
  }
  _effects(effects,view) {
    let budget=Math.round(64*this.quality);
    for(let i=effects.length-1;i>=0&&budget>0;i--) {const e=effects[i];if(e.time>e.duration||!visible({...e,x:e.x-100,y:e.y-200,w:200,h:240},view,50))continue;this._effect(e);budget--;}
  }
  _effect(e,required=true) {
    const entry=this._asset(EFFECT_ALIASES[e.type]||e.type,required);if(!entry)return;
    const frame=sampleFrame(entry.meta,e.time||0),pivot=frame.origin||entry.meta.origin,scale=e.scale??.6,ctx=this.ctx;
    ctx.save();ctx.translate(e.x,e.y);ctx.scale((e.facing||1)*scale,scale);ctx.globalAlpha=e.opacity??1;
    imageRect(ctx,entry.image,frame.rect,-pivot.x,-pivot.y,frame.rect.width,frame.rect.height);ctx.restore();this.drawnSprites++;
  }
  _telegraphs(game,view,time) {
    const ctx=this.ctx,boss=game.bossController?.boss,shapes=[];
    if(boss?.state==='TELL')shapes.push(...telegraphShapes(boss.currentAttack,game.bossController.arena));
    for(const e of game.world.enemies||[])if(e.telegraph&&visible(e.telegraph,view,80))shapes.push(e.telegraph);
    for(const h of game.world.hazards||[])if(h.warning||h.telegraph)shapes.push(h.telegraph||h);
    ctx.save();ctx.lineWidth=2;ctx.setLineDash([8,5]);ctx.lineDashOffset=-time*22;
    for(const s of shapes) {
      const safe=s.type==='safe';ctx.strokeStyle=safe?'#f7eab0':'#ffc0a0';ctx.fillStyle=safe?'rgba(244,221,130,.11)':'rgba(217,78,46,.16)';
      if(s.type==='circle'){ctx.beginPath();ctx.ellipse(s.x,s.y,s.radius,Math.max(14,s.radius*.2),0,0,Math.PI*2);ctx.fill();ctx.stroke();}
      else {ctx.fillRect(s.x,s.y,s.w,s.h);ctx.strokeRect(s.x,s.y,s.w,s.h);}
    }
    ctx.restore();
  }
  _exit(game,view) {
    const e=game.world.exit;if(!e||!visible(e,view,180))return;
    const entry=this._asset('collectibles-atlas');if(!entry)return;
    const frame=namedFrame(entry.meta,game.world.exitOpen||!game.world.bossId?'exit-gate-open':'exit-gate-closed');
    const at=groundedArtPlacement(entry.meta,frame,e.x+e.w/2,e.groundY??e.y+e.h,e.w+16);
    imageRect(this.ctx,entry.image,at.source,at.x,at.y,at.w,at.h);
  }
  _weather(biome,cam,time,weather) {
    if(!['storm','desert'].includes(biome)||weather?.disabled)return;
    const entry=this._asset(biome==='storm'?'fx-weather-rain':'fx-weather-sandstorm',false);if(!entry)return;
    const frame=sampleFrame(entry.meta,time),ctx=this.ctx,size=320,dx=(time*(biome==='storm'?-90:-38)-cam.x*.08)%size,dy=biome==='storm'?(time*160)%size:0;
    ctx.save();ctx.globalAlpha=biome==='storm'?.2:.12;
    for(let y=-size+dy;y<VIEW.height;y+=size)for(let x=-size+dx;x<VIEW.width;x+=size)imageRect(ctx,entry.image,frame.rect,x,y,size,size);
    ctx.restore();
  }
  drawDebug(game) {
    const ctx=this.ctx,s=this.assets.stats(),lines=[`${Math.round(game.fps||0)} fps · ${this.frameCost.toFixed(1)} ms`,`${this.drawnSprites} sprites · ${this.terrainCache.size} terrain cells`,`${s.images} images · ${Math.round(s.decodedBytes/1048576)} MB decoded`,`${game.world.biome} · ${game.world.chunks?.length||0} chunks`,`${Math.round(game.player.x)}, ${Math.round(game.player.y)} · ${game.player.state}`];
    ctx.save();ctx.fillStyle='rgba(10,24,28,.88)';ctx.fillRect(12,96,285,lines.length*18+20);ctx.fillStyle='#f5edd1';ctx.font='12px ui-monospace, monospace';lines.forEach((text,i)=>ctx.fillText(text,22,118+i*18));ctx.restore();
  }
}
