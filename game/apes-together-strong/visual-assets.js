/* Offline artwork with one decoded image per shared atlas. Base appearances
 * load at startup; armor variants decode on demand with a bounded LRU. */
(() => {
 'use strict';
 const config=window.ATS_VISUAL_BUNDLE||{manifest:{},sources:{}},outfits=config.manifest.outfits,armor=config.manifest['armor-sprites'];
 const cm=config.manifest.characters;
 if(outfits&&cm){cm.atlases.push(...outfits.atlases.filter(a=>a.id.startsWith('characters-')));Object.assign(cm.frames,outfits.characterFrames)}
 if(armor&&cm){cm.atlases.push(...armor.atlases);Object.assign(cm.frames,armor.characterFrames);cm.version=String(cm.version)+':armor3'}
 if(outfits&&config.manifest['held-items'])Object.assign(config.manifest['held-items'].frames,outfits.heldFrames);
 const images=Object.create(null),failures=[],listeners=new Set(),pending=new Map(),lru=new Map(),lazyLimit=24;
 const lazy=id=>id.startsWith('characters-armor-')&&!id.endsWith('-base'),entries=Object.entries(config.sources),initial=entries.filter(([id])=>!lazy(id));
 const assets=window.ATSVisualAssets={images,manifest:config.manifest,failures,status:'loading',total:initial.length,loaded:0,completed:0,progress:0,lazyLimit,
  get(id){if(images[id]){if(lazy(id)){lru.delete(id);lru.set(id,true)}return images[id]}if(config.sources[id]&&!failures.includes(id))decode(id);return null},
  preload(ids){return Promise.all(ids.map(decode))},
  onProgress(fn){listeners.add(fn);report(fn);return()=>listeners.delete(fn)},ready:null,
  cacheStats(){return{decoded:Object.keys(images).length,lazy:lru.size,pending:pending.size,limit:lazyLimit}}
 };
 const report=fn=>{try{fn(assets)}catch(e){console.error('Artwork progress callback failed',e)}},notify=()=>{for(const fn of listeners)report(fn)};
 function decode(id){
  if(images[id])return Promise.resolve(images[id]);if(pending.has(id))return pending.get(id);if(!config.sources[id]||failures.includes(id))return Promise.resolve(null);
  const promise=new Promise(resolve=>{const im=new Image();let finished=false;
   const finish=ok=>{if(finished)return;finished=true;clearTimeout(timer);im.onload=im.onerror=null;
    if(ok&&im.naturalWidth&&im.naturalHeight){images[id]=im;if(lazy(id)){lru.set(id,true);while(lru.size>lazyLimit){const oldest=lru.keys().next().value;lru.delete(oldest);delete images[oldest]}}else assets.loaded++}
    else{failures.push(id);assets.status='degraded'}
    if(!lazy(id)){assets.completed++;assets.progress=assets.completed/Math.max(1,assets.total)}pending.delete(id);resolve(images[id]||null);notify();
   };
   const timer=setTimeout(()=>finish(false),20000);im.onload=()=>finish(true);im.onerror=()=>finish(false);im.src=config.sources[id];
  });pending.set(id,promise);return promise;
 }
 assets.ready=Promise.all(initial.map(([id])=>decode(id))).then(()=>{assets.status=failures.length?'degraded':'ready';assets.progress=1;delete window.ATS_VISUAL_BUNDLE;notify();return assets});
})();
