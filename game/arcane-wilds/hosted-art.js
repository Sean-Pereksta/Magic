'use strict';
/* Region-scoped image streaming. Atlas metadata is local, so public R2 needs no JSON CORS. */
(() => {
  if(window.AWArt)return;
  const catalog=window.AWArtCatalog||{entries:{},regions:{}};
  const config={enabled:true,baseUrl:'https://pub-982943ccc2b14477a62ef61fa83e5179.r2.dev',stripAssetPrefix:true,maxParallel:3,maxCachedAtlases:typeof isTouch!=='undefined'&&isTouch?8:12,timeoutMs:20000,retryMs:30000,...window.AW_ART_CONFIG};
  const records=new Map(),queue=[],pinned=new Set();let active=0,stamp=0,region=null,playerId=null;
  const now=()=>typeof performance!=='undefined'?performance.now():Date.now();
  const entry=id=>catalog.entries[id]||null;
  function url(assetPath){
    const clean=String(assetPath).replace(/^\/+/,''),relative=config.stripAssetPrefix?clean.replace(/^assets\//,''):clean;
    return String(config.baseUrl).replace(/\/+$/,'')+'/'+relative;
  }
  function get(id){const r=records.get(id);if(r?.status!=='ready')return null;r.used=++stamp;return r;}
  function shouldRetry(id){const r=records.get(id);return !!(config.enabled&&r?.status==='error'&&now()-r.failedAt>=config.retryMs);}
  function release(id){const r=records.get(id);if(!r||r.status==='loading')return;r.image=null;records.delete(id);}
  function trim(force=false){
    const ready=[...records.values()].filter(r=>r.status==='ready').sort((a,b)=>a.used-b.used);
    let count=ready.length;for(const r of ready){if(pinned.has(r.id))continue;if(force||count>config.maxCachedAtlases){release(r.id);count--;}}
  }
  function pump(){
    const limit=Math.max(1,Math.min(4,config.maxParallel||3));
    while(active<limit&&queue.length){
      const r=queue.shift();if(records.get(r.id)!==r)continue;r.status='loading';active++;
      const image=new Image();r.image=image;image.decoding='async';
      let timer,finished=false;
      function finish(ok,reason){
        if(finished)return;finished=true;clearTimeout(timer);image.onload=image.onerror=null;active--;
        if(ok&&(image.naturalWidth!==r.meta.width||image.naturalHeight!==r.meta.height)){ok=false;reason='Unexpected atlas dimensions';}
        r.status=ok?'ready':'error';r.error=ok?null:reason;r.failedAt=ok?null:now();r.used=++stamp;
        if(!ok)r.image=null;
        r.resolve(ok?r:null);if(ok&&r.entry.category==='terrain')window.AWWorld?.invalidate();
        trim();pump();
      }
      image.onload=()=>finish(true);image.onerror=()=>finish(false,'Image unavailable');
      timer=setTimeout(()=>finish(false,'Image timeout'),Math.max(1000,config.timeoutMs));
      // Deliberately omit crossOrigin. These images can paint a canvas without R2 CORS.
      image.src=url(r.entry.image);
    }
  }
  function ensure(id,priority=0){
    const definition=entry(id);if(!config.enabled||!definition)return Promise.resolve(null);
    if(definition.category==='armor'&&window.AWArtRenderer?.playerAssetId()===id&&playerId!==id){if(playerId)pinned.delete(playerId);playerId=id;pinned.add(id);}
    const previous=records.get(id);
    if(previous){
      if(previous.status==='ready'){previous.used=++stamp;return Promise.resolve(previous);}
      if(previous.status==='queued'||previous.status==='loading')return previous.promise;
      if(now()-previous.failedAt<config.retryMs)return Promise.resolve(null);
      records.delete(id);
    }
    const r={id,entry:definition,meta:definition.meta,frames:new Map(definition.meta.frames.map(f=>[f.id,f])),status:'queued',priority,used:++stamp,image:null};
    r.promise=new Promise(resolve=>{r.resolve=resolve;});records.set(id,r);queue.push(r);queue.sort((a,b)=>b.priority-a.priority);pump();return r.promise;
  }
  function regionFor(room){
    room=room||(typeof game!=='undefined'?game.roomData:null)||{};
    const node=window.AWCampaignData?.nodes[room.campaignNode];
    if(room.shadow||node?.shadow||String(room.campaignNode||'').startsWith('shadow:'))return 'shadow';
    const continent=node?.continent||room.continent||window.AWCampaign?.current()?.continent;
    if(catalog.regions[continent])return continent;
    const biome=room.biome;
    if(['gloam','celestial','crypt','bloodroot','void','voidsea','astral'].includes(biome))return 'gloam';
    if(['desert','crystal','stormlands','frost','volcanic'].includes(biome))return 'meridian';
    return 'verdant';
  }
  function prepare(room){
    if(!config.enabled)return Promise.resolve([]);
    const next=regionFor(room),changed=next!==region;region=next;
    playerId=window.AWArtRenderer?.playerAssetId()||'armor-wayfarer-cloth';
    const ids=[playerId,...(catalog.regions[next]||[])];
    if(typeof game!=='undefined')for(const enemy of game.enemies||[]){const id=window.AWArtRenderer?.enemyAssetId(enemy,room);if(id)ids.push(id);}
    pinned.clear();for(const id of ids)pinned.add(id);
    // Superseded requests have no room side effects; already in-flight images may finish safely.
    for(let i=queue.length-1;i>=0;i--){const r=queue[i];if(!pinned.has(r.id)){queue.splice(i,1);records.delete(r.id);r.resolve(null);}}
    if(changed)trim(true);
    const wanted=[...new Set(ids)];return Promise.all(wanted.map((id,i)=>ensure(id,i===0?10:entry(id)?.category==='terrain'?5:1)));
  }
  function draw(id,context,x,y,options={}){
    const r=get(id);if(!r||![x,y].every(Number.isFinite))return false;
    let frame;
    if(options.frame)frame=r.frames.get(options.frame);
    else{
      const direction=r.meta.animations[options.facing||'SE'];
      const animation=direction?.[options.state||'idle']||direction?.idle;
      if(!animation?.frames?.length)return false;
      const time=Math.max(0,Number.isFinite(options.time)?options.time:0),index=Math.floor(time*(animation.fps||1));
      frame=r.frames.get(animation.frames[animation.loop?index%animation.frames.length:Math.min(index,animation.frames.length-1)]);
    }
    const rect=frame?.rect,pivot=frame?.pivot||{x:0,y:0},scale=options.scale??1;
    if(!rect||![rect.x,rect.y,rect.w,rect.h,scale,pivot.x,pivot.y].every(Number.isFinite)||scale<=0||rect.x<0||rect.y<0||rect.w<=0||rect.h<=0||rect.x+rect.w>r.meta.width||rect.y+rect.h>r.meta.height)return false;
    context.save();if(Number.isFinite(options.alpha))context.globalAlpha*=Math.max(0,Math.min(1,options.alpha));
    context.drawImage(r.image,rect.x,rect.y,rect.w,rect.h,x-pivot.x*scale,y-pivot.y*scale,rect.w*scale,rect.h*scale);context.restore();return true;
  }
  function status(){return {region,active,queued:queue.length,cached:[...records.values()].filter(r=>r.status==='ready').map(r=>r.id),failed:[...records.values()].filter(r=>r.status==='error').map(r=>r.id),pinned:[...pinned]};}
  window.AWArt={entry,get,ensure,url,regionFor,prepare,draw,status,shouldRetry};
  if(typeof loadRoom==='function'){
    const original=loadRoom;loadRoom=function(...args){const result=original.apply(this,args);prepare(game.roomData);return result;};
  }
  if(typeof queueMicrotask==='function')queueMicrotask(()=>{if(typeof game!=='undefined'&&game.player)prepare(game.roomData);});
})();
