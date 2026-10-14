/* TurnCraft cosmetic animation. Grid positions, timers and snapshots remain authoritative. */
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.TurncraftAnimation=api})(typeof globalThis!=='undefined'?globalThis:this,function(){
  'use strict';
  const DIRECTIONS=['N','NE','E','SE','S','SW','W','NW'];
  const QUALITY={low:{fps:12,particles:48,ghosts:24,textureBytes:48*1024*1024,textureScale:.5,lighting:false,shadows:false},medium:{fps:24,particles:120,ghosts:48,textureBytes:96*1024*1024,textureScale:.75,lighting:false,shadows:true},high:{fps:40,particles:180,ghosts:72,textureBytes:192*1024*1024,textureScale:1,lighting:true,shadows:true}};
  const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
  const finite=(v,fallback=0)=>Number.isFinite(Number(v))?Number(v):fallback;
  const wrap=a=>((a+Math.PI)%(Math.PI*2)+Math.PI*2)%(Math.PI*2)-Math.PI;
  const assetTypeFor=entity=>entity?.kind==='cocoon'?'cocoon':entity?.kind==='larva'?'larva':entity?.type;
  function selectionRenderContext(entities,selectedIds,selectedId,index){
    const lookup=index||new Map(entities.filter(e=>e.alive!==false).map(e=>[e.id,e])),selected=new Set(selectedIds||[]),targets=new Set();if(selectedId!=null)selected.add(selectedId);
    const orderIds=new Set(selectedIds?.length?selectedIds:selectedId!=null?[selectedId]:[]);
    for(const id of orderIds){const unit=lookup.get(id);if(unit?.alive!==false&&unit?.targetId!=null)targets.add(unit.targetId)}
    return{selected,targets};
  }
  function directionForVector(dx,dy,previous='S',hysteresis=.12){
    if(Math.hypot(dx,dy)<.0001)return previous;
    const angle=Math.atan2(dx,-dy),index=DIRECTIONS.indexOf(previous),center=(index<0?4:index)*Math.PI/4;
    if(Math.abs(wrap(angle-center))<=Math.PI/8+hysteresis)return previous;
    return DIRECTIONS[(Math.round(angle/(Math.PI/4))+8)%8];
  }
  function sampleSegment(segment,time){
    if(!segment)return null;const progress=clamp((time-segment.start)/Math.max(1,segment.end-segment.start),0,1);
    // Constant speed avoids a stop/start ease at every grid boundary.
    if(segment.points?.length>1){const total=segment.length||1,distance=total*progress;let walked=0;for(let i=1;i<segment.points.length;i++){const from=segment.points[i-1],to=segment.points[i],length=Math.hypot(to.x-from.x,to.y-from.y);if(distance<=walked+length||i===segment.points.length-1){const q=clamp((distance-walked)/Math.max(.0001,length),0,1);return{x:from.x+(to.x-from.x)*q,y:from.y+(to.y-from.y)*q,progress,moving:progress<1&&total>.001,dx:to.x-from.x,dy:to.y-from.y}}walked+=length}}
    return {x:segment.fromX+(segment.toX-segment.fromX)*progress,y:segment.fromY+(segment.toY-segment.fromY)*progress,progress,moving:progress<1&&Math.hypot(segment.toX-segment.fromX,segment.toY-segment.fromY)>.001};
  }
  function frameForState(state,direction,elapsedMs,options={}){
    if(!state)return null;
    const directions=state.directions||['S'],dir=directions.includes(direction)?direction:directions.includes('S')?'S':directions[0];
    const coordinates=state.coordinates?.[dir],count=Math.max(1,coordinates?.length||finite(state.frames,1));
    const fps=Math.max(.1,finite(state.fps,6)),cycle=count*1000/fps;
    let phase=options.progress==null?Math.max(0,elapsedMs)*finite(options.rate,1)/cycle:clamp(options.progress,0,1);
    if(options.progress==null&&state.loop!==false)phase%=1;else phase=clamp(phase,0,1);
    const index=Math.min(count-1,Math.floor(phase*count));
    const explicit=coordinates?.[index];
    return {x:explicit?.x??((finite(state.firstColumn)+index)*finite(state.frameWidth,128)),y:explicit?.y??(finite(state.rows?.[dir],Math.max(0,directions.indexOf(dir)))*finite(state.frameHeight,128)),w:explicit?.w??finite(state.frameWidth,128),h:explicit?.h??finite(state.frameHeight,128),index,direction:dir,cycle};
  }
  function eventProgress(definition,event,time){
    if(!event)return undefined;const duration=Math.max(1,event.end-event.start);
    const hasFrame=definition?.contactFrame!=null&&Number.isFinite(Number(definition.contactFrame)),hasMs=definition?.contactMs!=null&&Number.isFinite(Number(definition.contactMs));
    if(event.kind!=='attack'||(!hasFrame&&!hasMs)||!Number.isFinite(event.contactAt))return clamp((time-event.start)/duration,0,1);
    const count=Math.max(1,finite(definition.frames,6)),cycle=Math.max(1,count*1000/Math.max(1,finite(definition.fps,10))),contact=clamp(hasFrame?Number(definition.contactFrame)/count:Number(definition.contactMs)/cycle,0,.95),at=clamp(event.contactAt,event.start,event.end);
    // Skip anticipation when the logical shot has already fired, and align melee contact
    // to its existing 270 ms impact; this function never reschedules game events.
    return time<at?contact*clamp((time-event.start)/Math.max(1,at-event.start),0,1):contact+(1-contact)*clamp((time-at)/Math.max(1,event.end-at),0,1);
  }
  class SpriteTintCache{
    constructor({maxBytes=12*1024*1024,createCanvas}={}){this.maxBytes=Math.max(4,maxBytes);this.createCanvas=createCanvas||(()=>typeof document!=='undefined'?document.createElement('canvas'):typeof OffscreenCanvas!=='undefined'?new OffscreenCanvas(1,1):null);this.sources=new WeakMap();this.entries=new Set();this.bytes=0}
    frame(image,{sx=0,sy=0,sw=image?.naturalWidth||image?.width,sh=image?.naturalHeight||image?.height,tint}={}){
      if(!image||!tint||![sx,sy,sw,sh].every(Number.isFinite)||sw<=0||sh<=0)return null;
      let frames=this.sources.get(image);if(!frames){frames=new Map();this.sources.set(image,frames)}
      const key=`${sx}:${sy}:${sw}:${sh}:${tint}`,cached=frames.get(key);if(cached){this.entries.delete(cached);this.entries.add(cached);return cached.image}
      const canvas=this.createCanvas();if(!canvas)return null;const padding=sx||sy||sw<(image.naturalWidth||image.width)||sh<(image.naturalHeight||image.height)?1:0,width=sw+padding*2,height=sh+padding*2,scale=Math.min(1,256/Math.max(width,height),Math.sqrt(this.maxBytes/(4*width*height)));canvas.width=Math.max(1,Math.floor(width*scale));canvas.height=Math.max(1,Math.floor(height*scale));const context=canvas.getContext('2d');if(!context)return null;
      // Keep one sample outside a frame for the same edge interpolation as the
      // original atlas. The extra sample is excluded from its painted bounds.
      context.drawImage(image,sx-padding,sy-padding,width,height,0,0,canvas.width,canvas.height);canvas.spriteTintCrop=[padding*canvas.width/width,padding*canvas.height/height,sw*canvas.width/width,sh*canvas.height/height];
      // Source-atop preserves every transparent edge, even on Cloudflare images
      // without pixel-read access. Retain detail beneath a strong owner color.
      context.globalCompositeOperation='source-atop';context.globalAlpha=.72;context.fillStyle=tint;context.fillRect(0,0,canvas.width,canvas.height);context.globalCompositeOperation='source-over';context.globalAlpha=1;
      const entry={image:canvas,frames,key,bytes:canvas.width*canvas.height*4};frames.set(key,entry);this.entries.add(entry);this.bytes+=entry.bytes;this.evict();return canvas;
    }
    evict(){for(const entry of this.entries){if(this.bytes<=this.maxBytes)break;this.entries.delete(entry);entry.frames.delete(entry.key);this.bytes-=entry.bytes;entry.image.width=0;entry.image.height=0}}
    setBudget(bytes){this.maxBytes=Math.max(4,bytes);this.evict()}
    clear(){for(const entry of this.entries){entry.image.width=0;entry.image.height=0}this.sources=new WeakMap();this.entries.clear();this.bytes=0}
    stats(){return{bytes:this.bytes,limit:this.maxBytes,frames:this.entries.size}}
  }
  class TextureCache{
    constructor({baseUrl='',maxBytes=QUALITY.medium.textureBytes,concurrency=2,createImage,createCanvas}={}){this.baseUrl=baseUrl;this.maxBytes=maxBytes;this.concurrency=concurrency;this.createImage=createImage||(()=>new Image());this.createCanvas=createCanvas||(()=>typeof document!=='undefined'?document.createElement('canvas'):typeof OffscreenCanvas!=='undefined'?new OffscreenCanvas(1,1):null);this.entries=new Map();this.queue=[];this.active=0;this.bytes=0;this.counter=0;this.pinned=new Set();this.resolution=1;this.generation=0}
    resolve(file){try{return new URL(file,this.baseUrl||(typeof location!=='undefined'?location.href:'http://localhost/')).href}catch{return file}}
    beginFrame(){this.pinned.clear()}
    setCropRepairs(repairs){if(this.cropRepairs===repairs)return;this.cropRepairs=repairs;this.generation++;for(const entry of this.entries.values())this.dispose(entry.image);this.entries.clear();this.queue.length=0;this.pinned.clear();this.bytes=0}
    repairAtlas(image,repair){
      // Generated poses do not always respect their nominal grid. Repack the
      // measured crops once, with empty gutters, before any quality downsample.
      // Drawing is allowed for cross-origin images; no canvas pixel reads occur.
      const canvas=this.createCanvas();if(!canvas)throw new Error('Atlas repair needs a canvas');
      canvas.width=repair.width;canvas.height=repair.height;const context=canvas.getContext('2d');if(!context)throw new Error('Atlas repair needs a 2D context');
      const width=image.naturalWidth||image.width,height=image.naturalHeight||image.height;
      for(let row=0;row<repair.crops.length;row++)for(let col=0;col<repair.crops[row].length;col++){
        const [x,y,w,h]=repair.crops[row][col],offset=repair.destinationOffsets?.[row]?.[col]||[(repair.frameWidth-w)/2,y-row*repair.frameHeight],dx=offset[0],dy=offset[1];
        if(![x,y,w,h,dx,dy].every(Number.isFinite)||x<0||y<0||w<=0||h<=0||x+w>width||y+h>height||dx<0||dy<0||dx+w>repair.frameWidth||dy+h>repair.frameHeight)throw new Error('Invalid measured sprite crop');
        const left=col*repair.frameWidth+dx,top=row*repair.frameHeight+dy;
        context.drawImage(image,x,y,w,h,left,top,w,h);
        // A few disconnected neighbor scraps lie inside a body's bounding box.
        for(const [ex,ey,ew,eh]of repair.erasures?.[`${row},${col}`]||[])context.clearRect(left+ex,top+ey,ew,eh);
      }
      return canvas;
    }
    frameExpansion(file){return this.entries.get(file)?.frameExpansion||1}
    frameExpansionY(file){return this.entries.get(file)?.frameExpansionY||1}
    releaseOriginal(image){image.onload=null;image.onerror=null;if(typeof HTMLImageElement!=='undefined'&&image instanceof HTMLImageElement)image.src=''}
    dispose(image){if(!image)return;if(typeof image.close==='function')image.close();else if(typeof image.getContext==='function'){image.width=0;image.height=0}else this.releaseOriginal(image)}
    get(file,{priority=false}={}){if(!file)return null;let entry=this.entries.get(file);if(!entry||entry.status==='evicted'&&Date.now()>=entry.retryAt){entry={status:'queued',last:++this.counter,bytes:0};this.entries.set(file,entry);if(priority)this.queue.unshift(file);else this.queue.push(file);this.pump()}else if(priority&&entry.status==='queued'){const index=this.queue.indexOf(file);if(index>=0){this.queue.splice(index,1);this.queue.unshift(file)}}entry.last=++this.counter;this.pinned.add(file);return entry.status==='ready'?entry.image:null}
    pump(){while(this.active<this.concurrency&&this.queue.length){const file=this.queue.shift(),entry=this.entries.get(file);if(!entry||entry.status!=='queued')continue;const generation=this.generation,resolution=this.resolution;entry.status='loading';this.active++;let image;try{image=this.createImage()}catch{entry.status='failed';this.active--;continue}image.decoding='async';let done=false;const finish=ok=>{if(done)return;done=true;clearTimeout(timer);image.onload=null;image.onerror=null;this.active--;if(generation!==this.generation||this.entries.get(file)!==entry){this.releaseOriginal(image);this.pump();return}if(ok){let output=image,sourceScale=1;const repair=this.cropRepairs?.[file];if(repair){try{output=this.repairAtlas(image,repair);entry.frameExpansion=repair.frameWidth/repair.sourceFrameWidth;entry.frameExpansionY=repair.frameHeight/(repair.sourceFrameHeight||repair.frameHeight)}catch{entry.status='failed';this.releaseOriginal(image);this.pump();return}}const width=finite(output.naturalWidth||output.width),height=finite(output.naturalHeight||output.height);if(resolution<1){try{const canvas=this.createCanvas();if(canvas){canvas.width=Math.max(1,Math.round(width*resolution));canvas.height=Math.max(1,Math.round(height*resolution));const context=canvas.getContext('2d');if(context){context.imageSmoothingEnabled=true;context.imageSmoothingQuality='high';context.drawImage(output,0,0,canvas.width,canvas.height);if(output!==image)this.dispose(output);output=canvas;sourceScale=resolution}}}catch{/* Original artwork remains a safe fallback when a canvas is unavailable. */}}entry.image=output;entry.sourceScale=sourceScale;entry.status='ready';entry.bytes=finite(output.naturalWidth||output.width)*finite(output.naturalHeight||output.height)*4;this.bytes+=entry.bytes;if(output!==image)this.releaseOriginal(image)}else{entry.status='failed';this.releaseOriginal(image)}this.evict();this.pump()};const timer=setTimeout(()=>finish(false),12000);image.onload=()=>finish(true);image.onerror=()=>finish(false);image.src=this.resolve(file)}}
    evict(){if(this.bytes<=this.maxBytes)return;const candidates=[...this.entries].filter(([,e])=>e.status==='ready').sort((a,b)=>Number(this.pinned.has(a[0]))-Number(this.pinned.has(b[0]))||a[1].last-b[1].last);for(const [file,entry]of candidates){if(this.bytes<=this.maxBytes)break;this.bytes-=entry.bytes;this.dispose(entry.image);this.entries.set(file,{status:'evicted',last:entry.last,bytes:0,retryAt:Date.now()+3000})}}
    setBudget(bytes){this.maxBytes=bytes;this.evict()}
    setResolution(scale){const next=clamp(finite(scale,1),.25,1);if(next===this.resolution)return;this.resolution=next;this.generation++;for(const entry of this.entries.values())this.dispose(entry.image);this.entries.clear();this.queue.length=0;this.pinned.clear();this.bytes=0}
    sourceScale(file){return this.entries.get(file)?.sourceScale||1}
    stats(){return{bytes:this.bytes,limit:this.maxBytes,resolution:this.resolution,ready:[...this.entries.values()].filter(e=>e.status==='ready').length,pending:this.queue.length+this.active}}
  }
  class SpriteRuntime{
    constructor({manifest=null,baseUrl='',quality='medium',cache,tintCache}={}){this.manifest=manifest;this.cache=cache||new TextureCache({baseUrl});this.tintCache=tintCache||new SpriteTintCache();this.cache.setCropRepairs?.(manifest?.cropRepairs);this.states=new Map();this.ghosts=[];this.ghostIds=new Set();this.time=0;this.entityIndex=new Map();this.setQuality(quality)}
    setQuality(value){this.quality=Object.hasOwn(QUALITY,value)?value:'medium';this.budget=QUALITY[this.quality];this.cache.setBudget(this.budget.textureBytes);this.cache.setResolution(this.budget.textureScale);this.tintCache.setBudget(this.budget.textureBytes/8);while(this.ghosts.length>this.budget.ghosts){this.ghostIds.delete(this.ghosts.shift().id)}}
    async loadManifest(url){const controller=typeof AbortController!=='undefined'?new AbortController():null,timer=controller?setTimeout(()=>controller.abort(),6000):null;try{const response=await fetch(url,controller?{signal:controller.signal}:undefined);if(!response.ok)return false;const data=await response.json();if(!data||!data.assets)return false;this.manifest=data;this.cache.setCropRepairs?.(data.cropRepairs);return true}catch{return false}finally{if(timer)clearTimeout(timer)}}
    reset(){this.states.clear();this.ghosts.length=0;this.ghostIds.clear();this.entityIndex.clear();this.tintCache.clear()}
    ensure(entity,time){let state=this.states.get(entity.id);if(!state||state.type!==entity.type||state.kind!==entity.kind){state={id:entity.id,type:entity.type,assetType:assetTypeFor(entity),kind:entity.kind,faction:entity.faction,x:finite(entity.x),y:finite(entity.y),direction:'S',state:'idle',stateAt:time,phaseAt:time,event:null,segment:null,logicalX:entity.x,logicalY:entity.y,visible:false,lastMoveReady:finite(entity.moveReadyAt),lastAttackReady:finite(entity.attackReadyAt),lastHit:finite(entity.lastDamagedAt),lastAbility:finite(entity.abilityReadyAt),lastDeploy:false};this.states.set(entity.id,state)}return state}
    turn(state,dx,dy,time){const next=directionForVector(dx,dy,state.direction);if(next!==state.direction){state.previousDirection=state.direction;state.turnAt=time;state.direction=next}}
    route(entity,path){const state=this.ensure(entity,this.time);state.pendingPath=path?.slice(0,16).map(p=>({x:p.x,y:p.y}))}
    move(entity,from,time,duration,{teleport=false,remote=false,maxDistance=8,path}={}){
      const state=this.ensure(entity,time),current=sampleSegment(state.segment,time)||{x:state.x,y:state.y},distance=Math.hypot(entity.x-current.x,entity.y-current.y);
      if(teleport||distance>maxDistance||entity.kind!=='unit'){state.segment=null;state.x=entity.x;state.y=entity.y;state.arrivedAt=time;state.appearAt=teleport?time:0}
      else if(distance>.001){const remaining=finite(entity.moveReadyAt)-time;const interval=remote?(remaining>0?Math.min(duration,remaining):Math.min(300,duration)):duration;state.segment={fromX:from?.x??current.x,fromY:from?.y??current.y,toX:entity.x,toY:entity.y,start:time,end:time+Math.max(90,interval)};const route=path||state.pendingPath;state.pendingPath=null;if(route?.length){const end=route.findIndex(p=>p.x===entity.x&&p.y===entity.y);if(end>=0){state.segment.points=[{x:state.segment.fromX,y:state.segment.fromY},...route.slice(0,end+1)];state.segment.length=state.segment.points.reduce((sum,p,i,points)=>i?sum+Math.hypot(p.x-points[i-1].x,p.y-points[i-1].y):sum,0)}}state.moveDistance=state.segment.length||distance;state.moveDuration=interval;if(state.state!=='move')state.phaseAt=time}
      if(state.segment){this.turn(state,state.segment.points?.[1]?state.segment.points[1].x-state.segment.fromX:entity.x-state.segment.fromX,state.segment.points?.[1]?state.segment.points[1].y-state.segment.fromY:entity.y-state.segment.fromY,time)}state.logicalX=entity.x;state.logicalY=entity.y;state.lastMoveReady=finite(entity.moveReadyAt);return state;
    }
    signal(entity,kind,time,{target,origin,duration,contactAt,visible=true}={}){
      if(!entity||!visible)return;
      const state=this.ensure(entity,time),definition=this.manifest?.assets?.[state.assetType]?.states?.[kind]||this.manifest?.assets?.[state.assetType]?.states?.[kind==='ability'?'special':kind];
      if(definition?.sheet)this.cache.get(definition.sheet,{priority:true});
      const natural=Math.max(100,(finite(definition?.frames,6)*1000/Math.max(1,finite(definition?.fps,10))));
      // Impact/reaction is short; attacks retain the already authoritative projectile/contact clock.
      const length=duration??(kind==='hit'?180:kind==='attack'?520:natural);
      if(kind==='hit'&&state.event?.kind==='attack'&&state.event.end>time){state.hitAt=time;return}
      state.event={kind,start:time,end:time+length,contactAt};if(target)this.turn(state,target.x-(origin?.x??entity.x),target.y-(origin?.y??entity.y),time);
      state.stateAt=time;
    }
    kill(entity,time,visible){
      if(!entity||!visible||entity.garrisonedIn||this.ghostIds.has(entity.id))return false;
      const state=this.ensure(entity,time),position=sampleSegment(state.segment,time)||state,death=this.manifest?.assets?.[state.assetType]?.states?.death,duration=clamp(finite(death?.frames,8)*1000/Math.max(1,finite(death?.fps,10)),350,1800);
      if(death?.sheet)this.cache.get(death.sheet,{priority:true});
      const ghost={id:entity.id,type:state.assetType,kind:entity.kind,faction:entity.faction,x:position.x,y:position.y,logicalX:entity.x,logicalY:entity.y,direction:state.direction,start:time,end:time+duration};
      this.ghosts.push(ghost);this.ghostIds.add(entity.id);while(this.ghosts.length>this.budget.ghosts)this.ghostIds.delete(this.ghosts.shift().id);this.states.delete(entity.id);return true;
    }
    beginFrame(entities,time,{isVisible=()=>true,moveDuration=()=>600,maxDistance=()=>8,workTarget=()=>null,remote=false}={}){
      this.time=time;this.cache.beginFrame();this.entityIndex=new Map(entities.filter(e=>e.alive!==false).map(e=>[e.id,e]));
      const alive=new Set();
      for(const entity of entities){if(entity.alive===false)continue;alive.add(entity.id);const state=this.ensure(entity,time),visible=isVisible(entity);state.visible=visible;
        if(entity.garrisonedIn){state.segment=null;state.x=entity.x;state.y=entity.y;state.logicalX=entity.x;state.logicalY=entity.y;continue}
        if(state.logicalX!==entity.x||state.logicalY!==entity.y){const reset=entity.px===entity.x&&entity.py===entity.y;this.move(entity,null,time,Math.max(90,moveDuration(entity)),{teleport:reset,remote,maxDistance:maxDistance(entity)})}
        const sample=sampleSegment(state.segment,time);if(sample){state.x=sample.x;state.y=sample.y;state.moving=sample.moving;if(!sample.moving)state.segment=null}else state.moving=false;
        // Reuse existing cosmetic coordinates; serializer already explicitly removes them.
        entity.px=state.x;entity.py=state.y;
        if(visible){
          const shot=entity.projectileAnim,melee=entity.attackAnim,event=shot?.endAt>time?shot:melee?.endAt>time?melee:null;
          if(finite(entity.attackReadyAt)>state.lastAttackReady&&event){this.signal(entity,'attack',time,{target:{x:event.targetX??event.tx-.5,y:event.targetY??event.ty-.5},duration:Math.max(90,event.endAt-time),contactAt:shot?.startAt??event.startAt+270});state.event.start=event.startAt;state.event.end=event.endAt}
          if(finite(entity.lastDamagedAt)>state.lastHit&&time-finite(entity.lastDamagedAt)<350)this.signal(entity,'hit',finite(entity.lastDamagedAt),{duration:180});
          if(finite(entity.abilityReadyAt)>state.lastAbility)this.signal(entity,'ability',time,{duration:700});
        }
        state.lastAttackReady=finite(entity.attackReadyAt);state.lastHit=finite(entity.lastDamagedAt);state.lastAbility=finite(entity.abilityReadyAt);
        const deploying=!!(entity.fabricatorDeployAt||entity.tunnelDigging||entity.siegeMode||entity.burrowedUntil>time);
        if(visible&&deploying&&!state.lastDeploy)this.signal(entity,'deploy',time,{duration:600});state.lastDeploy=deploying;
        const target=visible?workTarget(entity):null;state.workTarget=target;
        let next=entity.complete===false?'construction':state.event?.end>time?state.event.kind:entity.kind==='cocoon'?'morph':deploying&&(entity.fabricatorDeployAt||entity.tunnelDigging)?'deploy':state.moving?'move':target?'work':entity.queue?.length?'production':'idle';
        if(entity.channelEffectId||entity.warpChannel)next='ability';
        if(target&&!state.moving&&next==='work')this.turn(state,target.x-entity.x,target.y-entity.y,time);
        if(state.moving&&next==='move'&&state.segment)this.turn(state,sample?.dx??state.segment.toX-state.x,sample?.dy??state.segment.toY-state.y,time);
        if(next!==state.state){state.state=next;state.stateAt=time;if(next==='move')state.phaseAt=time}
      }
      for(const [id,state]of this.states)if(!alive.has(id))this.states.delete(id);
      this.ghosts=this.ghosts.filter(g=>{if(g.end>time&&!alive.has(g.id))return true;this.ghostIds.delete(g.id);return false});this.cache.evict();
    }
    metadata(type,state){const asset=this.manifest?.assets?.[type];if(!asset)return null;const aliases={ability:'special',deploy:'ability',production:'work',construction:'deploy',morph:'construction'};return{asset,state:asset.states?.[state]||asset.states?.[aliases[state]]||asset.states?.idle}}
    drawState(ctx,type,state,{elapsedMs=0,progress,direction='S',size=48,x=0,y=0,rate=1,alpha=1,tint=null}={}){
      const meta=this.metadata(type,state);if(!meta?.state?.sheet)return false;const image=this.cache.get(meta.state.sheet);if(!image)return false;
      const frame=frameForState(meta.state,direction,elapsedMs,{progress,rate}),anchor=meta.state.anchor||meta.asset.anchor||[.5,.5],scale=finite(meta.state.scale,finite(meta.asset.scale,1)),width=size*scale,height=width*frame.h/frame.w;
      const sourceScale=this.cache.sourceScale(meta.state.sheet),expansion=this.cache.frameExpansion?.(meta.state.sheet)||1,expansionY=this.cache.frameExpansionY?.(meta.state.sheet)||1,sx=frame.x*expansion*sourceScale,sy=frame.y*expansionY*sourceScale,sw=frame.w*expansion*sourceScale,sh=frame.h*expansionY*sourceScale,drawWidth=width*expansion,drawHeight=height*expansionY;
      if(sx+sw>(image.naturalWidth||image.width)+1||sy+sh>(image.naturalHeight||image.height)+1)return false;
      // Extra transparent padding surrounds the original cell. Keep its pivot,
      // native pixel scale and logical hit bounds while isolating drifted rows.
      const top=y-height*anchor[1]-(drawHeight-height)/2;
      const colored=this.tintCache.frame(image,{sx,sy,sw,sh,tint}),source=colored||image,crop=colored?colored.spriteTintCrop:[sx,sy,sw,sh];
      ctx.save();ctx.globalAlpha*=alpha;if(meta.state.directionFlipX?.[frame.direction]===true){ctx.translate(x,y);ctx.scale(-1,1);ctx.drawImage(source,...crop,-drawWidth*(1-anchor[0]),top-y,drawWidth,drawHeight)}else ctx.drawImage(source,...crop,x-drawWidth*anchor[0],top,drawWidth,drawHeight);ctx.restore();return true;
    }
    bounds(entity,{size=48}={}){const state=this.ensure(entity,this.time),meta=this.metadata(state.assetType,state.state);if(!meta?.state)return null;const frame=frameForState(meta.state,state.direction,0),anchor=meta.state.anchor||meta.asset.anchor||[.5,.5],assetAnchor=meta.asset.anchor||[.5,.5],scale=finite(meta.state.scale,finite(meta.asset.scale,1)),width=size*scale,height=width*frame.h/frame.w;return{x:-width*anchor[0],y:size*(assetAnchor[1]-.5)-height*anchor[1],width,height}}
    draw(ctx,entity,time,{size=48,zoom=1,constructionProgress=1,morphProgress=0,tint=null}={}){
      const state=this.ensure(entity,time);let elapsed=time-(state.state==='move'?state.phaseAt:state.event?.kind===state.state?state.event.start:state.stateAt),rate=1;
      if(state.state==='move'){const meta=this.metadata(state.assetType,'move');const cycle=Math.max(100,finite(meta?.state?.frames,8)*1000/Math.max(1,finite(meta?.state?.fps,10)));rate=clamp(cycle/Math.max(100,state.moveDuration/Math.max(1,state.moveDistance)),.55,2.8)}
      if(state.state==='idle')elapsed+=finite(entity.id)*137;
      const frequency=zoom<.55?8:this.budget.fps;elapsed=Math.floor(Math.max(0,elapsed)/(1000/frequency))*(1000/frequency);
      const progress=state.state==='construction'?constructionProgress:state.state==='morph'?morphProgress:state.event?.kind===state.state&&state.event.end>time?eventProgress(this.metadata(state.assetType,state.state)?.state,state.event,time):undefined;
      const anchor=this.manifest?.assets?.[state.assetType]?.anchor||[.5,.5];
      // Discrete facing poses represent one body. Blending two directions can
      // expose two heads/sets of legs while a unit turns, even with isolated crops.
      const alpha=state.appearAt?clamp((time-state.appearAt)/180,.25,1):1,options={elapsedMs:elapsed,progress,size,rate,tint,y:size*(anchor[1]-.5)};
      return this.drawState(ctx,state.assetType,state.state,{...options,direction:state.direction,alpha});
    }
    drawGhosts(ctx,time,{visible=()=>true,worldToScreen,size=48,fallback=()=>false,airOffset=0,tintFor=()=>null}={}){
      for(const ghost of this.ghosts){if(!visible(ghost))continue;const screen=worldToScreen(ghost.x+.5,ghost.y+.5),alpha=clamp((ghost.end-time)/220,0,1);const anchor=this.manifest?.assets?.[ghost.type]?.anchor||[.5,.5];ctx.save();ctx.globalAlpha=alpha;const y=screen.y+size*(anchor[1]-.5)+(this.manifest?.assets?.[ghost.type]?.flying?airOffset:0);if(!this.drawState(ctx,ghost.type,'death',{elapsedMs:time-ghost.start,progress:(time-ghost.start)/(ghost.end-ghost.start),direction:ghost.direction,x:screen.x,y,size,tint:tintFor(ghost)}))fallback(ghost,screen,alpha);ctx.restore()}
    }
    stats(){return{quality:this.quality,entities:this.states.size,ghosts:this.ghosts.length,textures:this.cache.stats(),tints:this.tintCache.stats()}}
  }
  return {DIRECTIONS,QUALITY,assetTypeFor,selectionRenderContext,directionForVector,sampleSegment,frameForState,eventProgress,TextureCache,SpriteTintCache,SpriteRuntime};
});
