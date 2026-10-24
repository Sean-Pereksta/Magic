// The battlefield owns this cache; ordinary campaign play does not import it.
export const BATTLE_ASSET_VERSION = 'v1';
export const BATTLE_ASSET_PATH = `battle/${BATTLE_ASSET_VERSION}/pack.json`;
export const BATTLE_ASSET_BASE = 'https://pub-47f679f65f034fbda4c4b2ee31b3818a.r2.dev';
const LOCAL_PACK = new URL(`./assets/${BATTLE_ASSET_PATH}`,import.meta.url).href;
const CACHE_NAME = `iron-thrones-battle-${BATTLE_ASSET_VERSION}`;
const pending = new Map();
const configuredAssetBase = () => globalThis.IRON_THRONES_BATTLE_ASSET_BASE ?? BATTLE_ASSET_BASE;
const remotePackUrl = base => base ? `${String(base).replace(/\/$/,'')}/${BATTLE_ASSET_PATH}` : null;

export function validateBattlePack(pack) {
  if(pack?.format!=='iron-thrones-rigged-mesh'||pack.version!==1||!pack.models||!pack.meshes)throw new Error('The battlefield model package is incompatible.');
  for(const id of ['levy','spearman','menAtArms','heavyInfantry','archer','veteranArcher','crossbow','scout','lightCavalry','cavalry','knight','ram','catapult','trebuchet','siege']) {
    if(!pack.models[id]?.nodes?.length)throw new Error(`The battlefield package is missing ${id}.`);
  }
  for(const [id,mesh]of Object.entries(pack.meshes))if(!Array.isArray(mesh.positions)||mesh.positions.length%9||mesh.normals?.length!==mesh.positions.length||mesh.positions.some(n=>!Number.isFinite(n)))throw new Error(`The ${id} model mesh is damaged.`);
  return pack;
}

export async function loadBattleAssets({onProgress=()=>{},signal,assetBase=configuredAssetBase(),retry=false}={}) {
  const remote=remotePackUrl(assetBase);
  const key=remote||LOCAL_PACK;
  if(retry)pending.delete(key);
  if(pending.has(key))return pending.get(key);
  const task=(async()=>{
    onProgress({completed:0,total:1,label:'Loading soldiers, horses, weapons, and siege engines'});
    let cache;
    try{cache=await globalThis.caches?.open(CACHE_NAME);}catch{/* Private browsing may disallow persistent cache. */}
    const urls=[...new Set([remote,LOCAL_PACK].filter(Boolean))];let last;
    for(const url of urls){
      if(signal?.aborted)throw new DOMException('Battle loading cancelled','AbortError');
      if(!retry&&cache)try{const hit=await cache.match(url);if(hit){const pack=validateBattlePack(await hit.json());onProgress({completed:1,total:1,label:'Battlefield models ready (offline cache)',cached:true});return pack;}}catch{await cache.delete(url).catch(()=>{});}
      const controller=new AbortController(),cancel=()=>controller.abort(),timer=setTimeout(cancel,12000);
      signal?.addEventListener('abort',cancel,{once:true});
      try{
        const response=await fetch(url,{signal:controller.signal,cache:retry?'reload':'default',credentials:'omit'});
        if(!response.ok)throw new Error(`Model package returned HTTP ${response.status}.`);
        const copy=response.clone(),pack=validateBattlePack(await response.json());
        await cache?.put(url,copy).catch(()=>{});
        onProgress({completed:1,total:1,label:'Battlefield models ready',cached:false});return pack;
      }catch(error){last=error;if(signal?.aborted)throw error;}
      finally{clearTimeout(timer);signal?.removeEventListener('abort',cancel);}
    }
    throw new Error(`Battlefield models could not load. Retry or resolve the pending battle automatically. ${last?.message||''}`);
  })();
  pending.set(key,task);
  try{return await task;}catch(error){pending.delete(key);throw error;}
}
export function releaseBattleAssets(){pending.clear();}
export async function clearBattleAssetCache(){releaseBattleAssets();return globalThis.caches?.delete(CACHE_NAME);}
export async function downloadBattleAssets(onProgress=()=>{}) {
  const assetBase=configuredAssetBase(), pack=await loadBattleAssets({onProgress,assetBase,retry:true});
  let cached=false;
  try{const cache=await globalThis.caches?.open(CACHE_NAME);const remote=remotePackUrl(assetBase);cached=!!(remote&&await cache?.match(remote))||!!(await cache?.match(LOCAL_PACK));}catch{}
  if(!cached)throw new Error('This browser could load the models but could not save them for offline use. Allow site storage and try again.');
  return{models:Object.keys(pack.models).length,cached,version:BATTLE_ASSET_VERSION};
}
