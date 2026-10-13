/** Faction Forge graphics are opt-in. Never borrow another game's R2 origin. */
export const DEFAULT_ASSET_CONFIG = Object.freeze({assetBaseUrl:"", maxSheets:12, requestTimeout:7000});

export function normalizeAssetBase(value){
  if (!value || !String(value).trim()) return "";
  const parsed = new URL(String(value).trim());
  if (parsed.protocol !== "https:" && !(parsed.protocol === "http:" && ["localhost","127.0.0.1"].includes(parsed.hostname))) throw new Error("Graphics URL must use HTTPS.");
  if(parsed.username || parsed.password || parsed.search || parsed.hash) throw new Error("Graphics URL must be a public folder URL.");
  return parsed.href.replace(/\/+$/, "") + "/";
}

export function safeAssetUrl(base,path){
  if(!base || !path) return "";
  if(typeof path !== "string" || /^[a-z][a-z0-9+.-]*:/i.test(path) || path.startsWith("/") || path.includes("\\") || path.split("/").some(v=>v === "..") || /[?#]/.test(path)) throw new Error("Invalid graphics path.");
  const url = new URL(path,normalizeAssetBase(base));
  if(!url.href.startsWith(normalizeAssetBase(base))) throw new Error("Graphics path leaves configured folder.");
  return url.href;
}

export function frameRectangle(spec, frame, image){
  const frames = spec.frames;
  let rect = typeof frame === "object" && frame ? frame : frames && frames[frame];
  if(Number.isInteger(rect)){frame=rect;rect=null;}
  if(Array.isArray(rect)) rect={x:rect[0],y:rect[1],width:rect[2],height:rect[3]};
  if(!rect){
    const width=Number(spec.frameWidth), height=Number(spec.frameHeight), columns=Number(spec.columns || Math.floor((image?.width || spec.width)/width));
    if(!Number.isInteger(frame) || frame < 0 || !Number.isInteger(columns) || columns < 1) throw new Error("Invalid animation frame.");
    rect={x:(frame%columns)*width,y:Math.floor(frame/columns)*height,width,height};
  }
  rect={x:Number(rect.x),y:Number(rect.y),width:Number(rect.width ?? rect.w),height:Number(rect.height ?? rect.h)};
  if(!Object.values(rect).every(Number.isInteger) || rect.x<0 || rect.y<0 || rect.width<=0 || rect.height<=0) throw new Error("Sprite source rectangles must use positive integer dimensions.");
  if(image && (rect.x+rect.width > image.width || rect.y+rect.height > image.height)) throw new Error("Sprite frame exceeds sheet bounds.");
  return rect;
}

export function animationDefinition(spec,name){
  const aliases={move:"walking",cast:"ability",hit:"hurt",attack:"attacking",death:"dying"};
  const raw=spec.animations?.[name] || spec.animations?.[aliases[name]] || (name === "cast" && spec.animations?.special) || spec.animations?.idle;
  if(!raw) return {frames:[0],duration:1000,loop:true};
  const frames=Array.isArray(raw) ? raw : raw.frames || (Number.isInteger(raw.start) && Number.isInteger(raw.count) ? Array.from({length:raw.count},(_,i)=>raw.start+i) : [0]);
  return {frames,duration:Number(raw.duration || raw.durationMs || raw.frameDuration*frames.length || 1000),loop:raw.loop !== false && name !== "death"};
}

function abortableFetch(fetchImpl,url,timeout){
  const abort=new AbortController(), timer=setTimeout(()=>abort.abort(),timeout);
  return fetchImpl(url,{signal:abort.signal,cache:"force-cache",credentials:"omit"}).finally(()=>clearTimeout(timer));
}

export function createAssetLoader({baseUrl="",manifestUrl,manifest:initialManifest,fetchImpl=globalThis.fetch,imageFactory=()=>new Image(),maxSheets=12,requestTimeout=7000}={}){
  let base=normalizeAssetBase(baseUrl), manifest=initialManifest || null, manifestPromise=null;
  const cache=new Map(), pending=new Map(), failed=new Set();
  const stats={loads:0,failures:0,evictions:0};
  const enabled=()=>!!base;
  async function loadManifest(){
    if(manifest) return manifest;
    if(!base) return null;
    if(!manifestPromise) manifestPromise=(async()=>{
      try{
        const response=await abortableFetch(fetchImpl,manifestUrl || safeAssetUrl(base,"manifest.json"),requestTimeout);
        if(!response.ok) throw new Error("Graphics manifest unavailable.");
        const candidate=await response.json();
        if(!candidate.units || typeof candidate.units !== "object") throw new Error("Invalid graphics manifest.");
        manifest=candidate; return manifest;
      }catch(error){stats.failures++;return null;}
    })();
    return manifestPromise;
  }
  function touch(path,value){cache.delete(path);cache.set(path,value);while(cache.size>maxSheets){const first=cache.keys().next().value;cache.delete(first);stats.evictions++;}return value;}
  async function loadImage(path){
    if(!base || !path || failed.has(path)) return null;
    if(cache.has(path)) return touch(path,cache.get(path));
    if(pending.has(path)) return pending.get(path);
    let src;try{src=safeAssetUrl(base,path);}catch{failed.add(path);stats.failures++;return null;}
    const promise=new Promise(resolve=>{
      const image=imageFactory();let settled=false;
      const finish=value=>{if(settled)return;settled=true;clearTimeout(timer);image.onload=null;image.onerror=null;if(value){stats.loads++;touch(path,image);}else{failed.add(path);stats.failures++;}resolve(value);};
      const timer=setTimeout(()=>{finish(null);image.src="";},requestTimeout);
      image.crossOrigin="anonymous";image.decoding="async";
      image.onload=()=>{if(image.width>0 && image.height>0)finish(image);else finish(null);};image.onerror=()=>finish(null);image.src=src;
    }).finally(()=>pending.delete(path));
    pending.set(path,promise);return promise;
  }
  async function unit(key,tier=1){
    const data=await loadManifest();if(!data) return null;
    const entry=data.units[key], spec=entry?.tiers?.[String(tier)] || entry?.[String(tier)] || data.units[`${key}-tier${tier}`];
    if(!spec) return null;
    const image=await loadImage(spec.path || spec.file);if(!image)return null;
    try{for(const name of Object.keys(spec.animations || {idle:true})){for(const frame of animationDefinition(spec,name).frames) frameRectangle(spec,frame,image);}}catch{failed.add(spec.path || spec.file);return null;}
    return {image,spec,key,tier};
  }
  async function asset(group,key){
    const data=await loadManifest();const entry=data?.[group]?.[key];const path=typeof entry === "string" ? entry : entry?.path || entry?.file;
    if(!path)return null;const image=await loadImage(path);if(!image)return null;
    const spec=typeof entry==="object"?entry:{path};
    if(spec.frameWidth && spec.frameHeight){try{const columns=spec.columns||Math.floor(image.width/spec.frameWidth),rows=spec.rows||Math.floor(image.height/spec.frameHeight);frameRectangle({...spec,columns},0,image);frameRectangle({...spec,columns},columns*rows-1,image);}catch{stats.failures++;failed.add(path);return null;}}
    return {image,spec,url:safeAssetUrl(base,path)};
  }
  function configure(value){base=normalizeAssetBase(value);manifest=initialManifest || null;manifestPromise=null;cache.clear();failed.clear();}
  return {enabled,loadManifest,loadImage,unit,asset,url:path=>safeAssetUrl(base,path),configure,peek:path=>cache.get(path),stats,get baseUrl(){return base;},get manifest(){return manifest;},get cacheSize(){return cache.size;}};
}

export function drawSpriteFrame(context,image,rect,{x=0,y=0,width=rect.width,height=rect.height,flip=false}={}){
  context.save();context.beginPath();context.rect(x,y,width,height);context.clip();
  if(flip){context.translate(x+width,y);context.scale(-1,1);context.drawImage(image,rect.x,rect.y,rect.width,rect.height,0,0,width,height);}
  else context.drawImage(image,rect.x,rect.y,rect.width,rect.height,x,y,width,height);
  context.restore();
}
