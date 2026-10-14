import {ASSET_MANIFEST} from "./manifest.js";

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

/** Include every tier and artwork group, deduplicating UI aliases. */
export function manifestImagePaths(manifest){
  const paths=new Set();
  function visit(entry){
    if(typeof entry==="string"){paths.add(entry);return;}
    if(!entry || typeof entry!=="object")return;
    const path=entry.path || entry.file;
    if(path){paths.add(path);return;}
    for(const value of Object.values(entry))if(value && typeof value==="object")visit(value);
  }
  for(const group of ["units","terrain","backgrounds","effects","ui"]){
    for(const entry of Object.values(manifest?.[group] || {}))visit(entry);
  }
  return [...paths];
}

function abortableFetch(fetchImpl,url,timeout){
  const abort=new AbortController(), timer=setTimeout(()=>abort.abort(),timeout);
  return fetchImpl(url,{signal:abort.signal,cache:"force-cache",credentials:"omit"}).finally(()=>clearTimeout(timer));
}

export function createAssetLoader({baseUrl="",manifestUrl,manifest:initialManifest,fallbackManifest=ASSET_MANIFEST,fetchImpl=globalThis.fetch,imageFactory=()=>new Image(),maxSheets=12,requestTimeout=7000,crossOrigin=null}={}){
  // Checked-in metadata is sufficient when R2 contains only the five artwork folders.
  const configuredManifest=()=>initialManifest || (!manifestUrl ? fallbackManifest : null);
  let base=normalizeAssetBase(baseUrl), manifest=configuredManifest(), manifestPromise=null;
  const cache=new Map(), pending=new Map(), failed=new Set(), retained=new Map();
  const diagnostics=new Map(),diagnosticListeners=new Set();let lastProgress={loaded:0,completed:0,total:0};
  const stats={loads:0,failures:0,evictions:0};
  const enabled=()=>!!base;
  const report=()=>({...lastProgress,assetBaseUrl:base,failures:[...diagnostics.values()]});
  const notifyDiagnostics=()=>{for(const listener of diagnosticListeners)listener(report());};
  function failure(path,url,stage,error,extra={}){
    const entry={path,url,stage,message:error?.message || String(error || "Unknown graphics error"),...extra};
    diagnostics.set(path,entry);failed.add(path);cache.delete(path);retained.delete(path);stats.failures++;notifyDiagnostics();return entry;
  }
  function probeDownload(entry){
    // Image error events hide HTTP status, particularly for cross-origin images.
    // A separate best-effort request can explain a missing object or server error;
    // CORS may hide that response too. This diagnostic never delays game startup.
    if(typeof fetchImpl!=="function")return;
    Promise.resolve().then(async()=>{
      try{
        const response=await abortableFetch(fetchImpl,entry.url,requestTimeout);
        if(diagnostics.get(entry.path)!==entry)return;
        if(response.status){entry.httpStatus=response.status;entry.httpStatusText=response.statusText || "";}
        entry.responseType=response.type || "";
        entry.contentType=response.headers?.get?.("content-type") || "";
        entry.probeMessage=response.status?`Diagnostic request returned HTTP ${response.status}${response.statusText?" "+response.statusText:""}.`:"The browser did not expose an HTTP status.";
        if(entry.contentType)entry.probeMessage+=` Content-Type: ${entry.contentType}.`;
        if(response.ok)entry.probeMessage+=" The download endpoint responds, but the browser could not load its response as an image.";
        // Reading the response body is unnecessary for a status-only diagnostic.
        response.body?.cancel?.().catch?.(()=>{});
      }catch(error){
        if(diagnostics.get(entry.path)!==entry)return;
        entry.probeMessage=`Diagnostic request: ${error?.message || String(error)}. HTTP status is unavailable; the browser may have blocked the request or its response (including CORS).`;
      }
      notifyDiagnostics();
    });
  }
  async function loadManifest(){
    if(manifest) return manifest;
    if(!base) return null;
    if(!manifestPromise) manifestPromise=(async()=>{
      try{
        const response=await abortableFetch(fetchImpl,manifestUrl || safeAssetUrl(base,"manifest.json"),requestTimeout);
        if(!response.ok) throw new Error(`Graphics manifest returned HTTP ${response.status || "unknown"}${response.statusText?" "+response.statusText:""}.`);
        const candidate=await response.json();
        if(!candidate.units || typeof candidate.units !== "object") throw new Error("Invalid graphics manifest.");
        manifest=candidate; return manifest;
      }catch(error){failure("manifest.json",manifestUrl || safeAssetUrl(base,"manifest.json"),"manifest",error,{fallbackUsed:!!fallbackManifest});manifest=fallbackManifest || null;return manifest;}
    })();
    return manifestPromise;
  }
  function touch(path,value){cache.delete(path);cache.set(path,value);while(cache.size>maxSheets){const first=cache.keys().next().value;cache.delete(first);stats.evictions++;}return value;}
  async function loadImage(path){
    if(!base || !path || failed.has(path)) return null;
    if(retained.has(path))return retained.get(path);
    if(cache.has(path)) return touch(path,cache.get(path));
    if(pending.has(path)) return pending.get(path);
    let src;try{src=safeAssetUrl(base,path);}catch(error){failure(path,"","path",error);return null;}
    const promise=new Promise(resolve=>{
      let image;try{image=imageFactory();}catch(error){failure(path,src,"download",error);resolve(null);return;}
      let settled=false,stage="download";
      const finish=(value,error,reason=stage)=>{if(settled)return;settled=true;clearTimeout(timer);image.onload=null;image.onerror=null;if(value){stats.loads++;diagnostics.delete(path);touch(path,image);}else{const entry=failure(path,src,reason,error,reason==="timeout"?{during:stage,timeoutMs:requestTimeout}:{});if(reason==="download")probeDownload(entry);}resolve(value);};
      const timer=setTimeout(()=>{finish(null,new Error(`Timed out after ${requestTimeout} ms while ${stage==="decode"?"decoding":"downloading"} the image.`),"timeout");image.src="";},requestTimeout);
      // Display-only canvases can draw public R2 images without CORS headers.
      // Opt into anonymous CORS only when a caller explicitly needs pixel readback.
      if(crossOrigin)image.crossOrigin=crossOrigin;image.decoding="async";
      image.onload=async()=>{
        if(settled)return;
        try{
          stage="decode";
          if(!(image.width>0 && image.height>0))throw new Error("Empty graphics image.");
          // A completed download is not yet safe to reveal until decoding ends.
          if(typeof image.decode==="function")await image.decode();
          finish(image);
        }catch(error){finish(null,error);}
      };image.onerror=event=>finish(null,new Error(event?.message || "The browser could not load this image response. Image error events do not expose the HTTP status."),"download");
      try{image.src=src;}catch(error){finish(null,error,"download");}
    }).finally(()=>pending.delete(path));
    pending.set(path,promise);return promise;
  }
  async function unit(key,tier=1){
    const data=await loadManifest();if(!data) return null;
    const entry=data.units[key], spec=entry?.tiers?.[String(tier)] || entry?.[String(tier)] || data.units[`${key}-tier${tier}`];
    if(!spec) return null;
    const image=await loadImage(spec.path || spec.file);if(!image)return null;
    try{for(const name of Object.keys(spec.animations || {idle:true})){for(const frame of animationDefinition(spec,name).frames) frameRectangle(spec,frame,image);}}catch(error){const path=spec.path || spec.file;failure(path,safeAssetUrl(base,path),"validation",error);return null;}
    return {image,spec,key,tier};
  }
  async function asset(group,key){
    const data=await loadManifest();const entry=data?.[group]?.[key];const path=typeof entry === "string" ? entry : entry?.path || entry?.file;
    if(!path)return null;const image=await loadImage(path);if(!image)return null;
    const spec=typeof entry==="object"?entry:{path};
    if(spec.frameWidth && spec.frameHeight){try{const columns=spec.columns||Math.floor(image.width/spec.frameWidth),rows=spec.rows||Math.floor(image.height/spec.frameHeight);frameRectangle({...spec,columns},0,image);frameRectangle({...spec,columns},columns*rows-1,image);}catch(error){failure(path,safeAssetUrl(base,path),"validation",error);return null;}}
    return {image,spec,url:safeAssetUrl(base,path)};
  }
  async function preloadAll({onProgress=()=>{},concurrency=6,retry=false,allowFailures=false}={}){
    if(!enabled())return {loaded:0,total:0};
    if(retry&&manifestUrl&&diagnostics.has("manifest.json")){manifest=null;manifestPromise=null;diagnostics.delete("manifest.json");failed.delete("manifest.json");}
    const data=await loadManifest();
    if(!data){if(allowFailures)return report();throw new Error("Faction Forge graphics manifest is unavailable.");}
    const paths=manifestImagePaths(data),failures=[];let next=0,loaded=0,completed=0;
    if(retry)for(const path of paths){failed.delete(path);diagnostics.delete(path);}
    lastProgress={loaded,completed,total:paths.length};
    onProgress({loaded,completed,total:paths.length});
    const worker=async()=>{
      while(next<paths.length){
        const path=paths[next++],image=await loadImage(path);
        // Keep preloaded decoded images alive for the entire game session. The
        // ordinary lazy cache remains bounded for optional, non-manifest art.
        if(image){retained.set(path,image);loaded++;}else failures.push(path);
        completed++;lastProgress={loaded,completed,total:paths.length};onProgress(lastProgress);
      }
    };
    await Promise.all(Array.from({length:Math.min(paths.length,Math.max(1,Math.floor(concurrency)||1))},worker));
    notifyDiagnostics();
    if(failures.length&&!allowFailures){const error=new Error(`Unable to load ${failures.length} Faction Forge graphic${failures.length===1?"":"s"}. Check your connection and retry.`);error.paths=failures;error.report=report();throw error;}
    if(allowFailures)return report();
    return {loaded,total:paths.length};
  }
  function configure(value){base=normalizeAssetBase(value);manifest=configuredManifest();manifestPromise=null;cache.clear();retained.clear();failed.clear();diagnostics.clear();lastProgress={loaded:0,completed:0,total:0};notifyDiagnostics();}
  return {enabled,loadManifest,loadImage,preloadAll,unit,asset,url:path=>safeAssetUrl(base,path),configure,peek:path=>retained.get(path)||cache.get(path),subscribeDiagnostics(listener){diagnosticListeners.add(listener);return ()=>diagnosticListeners.delete(listener);},stats,get report(){return report();},get baseUrl(){return base;},get manifest(){return manifest;},get cacheSize(){return cache.size;},get preloadedSize(){return retained.size;}};
}

export function drawSpriteFrame(context,image,rect,{x=0,y=0,width=rect.width,height=rect.height,flip=false}={}){
  context.save();context.beginPath();context.rect(x,y,width,height);context.clip();
  if(flip){context.translate(x+width,y);context.scale(-1,1);context.drawImage(image,rect.x,rect.y,rect.width,rect.height,0,0,width,height);}
  else context.drawImage(image,rect.x,rect.y,rect.width,rect.height,x,y,width,height);
  context.restore();
}
