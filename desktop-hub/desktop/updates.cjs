const path=require('node:path');
const fs=require('node:fs');
const fsp=fs.promises;
const crypto=require('node:crypto');
const {execFile}=require('node:child_process');
const {promisify}=require('node:util');
const {pipeline}=require('node:stream/promises');
const {Transform}=require('node:stream');
const {writeJson}=require('./storage.cjs');
const FEED='https://github.com/Sean-Pereksta/Magic/releases/download/catnmice-hub-channel/stable.json';
const HASH=/^[a-f0-9]{64}$/;
const COMPONENT=/^[a-z0-9][a-z0-9_-]{0,79}$/;
const stages=new Map();
function verifyFeed(envelope,key){
  if(!key)throw Error('Automatic updates are available in publisher-signed builds.');
  const body=Buffer.from(envelope.payload,'base64');
  if(!crypto.verify(null,body,key,Buffer.from(envelope.signature,'base64')))throw Error('Publisher signature could not be verified.');
  const manifest=JSON.parse(body);
  if(manifest.schema!==1 || !Number.isSafeInteger(manifest.sequence) || manifest.sequence<=0 || !Array.isArray(manifest.items))throw Error('Unsupported release manifest.');
  const seen=new Set();
  for(const item of manifest.items){
    if(!COMPONENT.test(item.id)||Object.hasOwn(Object.prototype,item.id)||seen.has(item.id)||!/^[a-z0-9_.-]{1,80}$/i.test(item.version)||item.version==='.'||item.version==='..'||!HASH.test(item.sha256)||!Number.isSafeInteger(item.size)||item.size<=0||new URL(item.url).protocol!=='https:')throw Error('Invalid release component.');
    seen.add(item.id);
  }
  return manifest;
}
async function sha(file){const h=crypto.createHash('sha256');for await(const chunk of fs.createReadStream(file))h.update(chunk);return h.digest('hex');}
async function download(item,file,fetchImpl=fetch){
  await fsp.mkdir(path.dirname(file),{recursive:true});
  let have=fs.existsSync(file)?(await fsp.stat(file)).size:0;
  if(have>item.size){await fsp.truncate(file);have=0;}
  let retryWithoutRange=false;
  while(have<item.size){
    const resume=have>0 && !retryWithoutRange;
    const res=await fetchImpl(item.url,{headers:resume?{Range:'bytes='+have+'-','Accept-Encoding':'identity'}:{'Accept-Encoding':'identity'},signal:AbortSignal.timeout(600000)});
    if(res.status===416 && resume){await res.body?.cancel();retryWithoutRange=true;continue;}
    if(!res.ok)throw Error('Download paused ('+res.status+').');
    let end=item.size;
    if(res.status===206){
      const range=/^bytes (\d+)-(\d+)\/(\d+)$/.exec(res.headers.get('content-range')||'');
      const start=resume?have:0;
      if(!range||Number(range[1])!==start||Number(range[2])<start||Number(range[2])>=item.size||Number(range[3])!==item.size)throw Error('Server returned an unexpected download range.');
      have=start;end=Number(range[2])+1;
    }else if(res.status===200){have=0;}else throw Error('Unexpected download response.');
    let bytes=have;
    const bounded=new Transform({transform(chunk,_,cb){bytes+=chunk.length;cb(bytes>end?Error('Package exceeded its expected size.'):null,chunk);}});
    await pipeline(res.body,bounded,fs.createWriteStream(file,{flags:have?'a':'w'}));
    if(bytes!==end)throw Error('Download paused. Retry to resume.');
    have=bytes;retryWithoutRange=false;
  }
  if((await fsp.stat(file)).size!==item.size)throw Error('Download paused. Retry to resume.');
  if(await sha(file)!==item.sha256){await fsp.unlink(file);throw Error('Package verification failed. The current version is still installed.');}
}
function stageItem(item,dir,fetchImpl){
  const key=path.resolve(dir,'versions',item.id,item.sha256);
  if(!stages.has(key))stages.set(key,extractItem(item,dir,fetchImpl).finally(()=>stages.delete(key)));
  return stages.get(key);
}
async function extractItem(item,dir,fetchImpl){
  const dest=path.join(dir,'versions',item.id,item.sha256);
  try{if(JSON.parse(await fsp.readFile(path.join(dest,'.complete'),'utf8')).sha256===item.sha256)return dest;}catch{}
  const archive=path.join(dir,'downloads',item.id+'-'+item.sha256+'.zip');
  await download(item,archive,fetchImpl);
  await fsp.mkdir(path.dirname(dest),{recursive:true});
  const temp=await fsp.mkdtemp(path.join(path.dirname(dest),'.stage-'));
  let prior;
  try {
    await promisify(execFile)(path.join(process.env.SystemRoot,'System32','tar.exe'),['-xf',path.resolve(archive)],{cwd:temp,windowsHide:true});
    writeJson(path.join(temp,'.complete'),{sha256:item.sha256,version:item.version});
    if(fs.existsSync(dest)){
      prior=dest+'.incomplete.'+crypto.randomUUID();
      await fsp.rename(dest,prior);
    }
    await fsp.rename(temp,dest);
    if(prior)await fsp.rm(prior,{recursive:true,force:true}).catch(()=>{});
    await fsp.unlink(archive).catch(()=>{});
  }catch(e){
    if(prior && !fs.existsSync(dest) && fs.existsSync(prior))await fsp.rename(prior,dest);
    await fsp.rm(temp,{recursive:true,force:true});throw e;
  }
  return dest;
}
async function getFeed(key,sequence,fetchImpl=fetch){
  if(!key)throw Error('This preview has no publisher update key. Use a signed release to receive updates.');
  const r=await fetchImpl(FEED,{signal:AbortSignal.timeout(12000),cache:'no-store'});
  if(!r.ok)throw Error('The release channel is unavailable.');
  const m=verifyFeed(await r.json(),key);
  if(m.sequence<sequence)throw Error('This release channel is older than the installed channel.');
  return m;
}
function rejectionFile(data,id,hash){
  if(!COMPONENT.test(id)||!HASH.test(hash))throw Error('Invalid update identity.');
  return path.join(data,'rejected-updates',id,hash+'.json');
}
function markRejectedUpdate(data,id,hash){
  if(hash)writeJson(rejectionFile(data,id,hash),{sha256:hash});
}
function isRejectedUpdate(data,item){
  return fs.existsSync(rejectionFile(data,item.id,item.sha256));
}
const ignoredSaveFiles=new Set(['LOCK','Cache','Code Cache','GPUCache','DawnGraphiteCache']);
const saveVersion=v=>v==='bundled'||(typeof v==='string'&&HASH.test(v));
function prepareSaveDirectory(base,version,previous,touched=new Set()){
  version=version||'bundled';
  previous=previous||'bundled';
  if(!saveVersion(version)||!saveVersion(previous))throw Error('Invalid save version.');
  const next=path.join(base,version);
  fs.mkdirSync(base,{recursive:true});
  const journal=path.join(base,'.save-history.json');
  const history=fs.existsSync(journal)?JSON.parse(fs.readFileSync(journal,'utf8')):{slots:{}};
  if(!fs.existsSync(next)){
    const last=history.last;
    const head=history.slots[last];
    const restoringParent=head?.parent===version;
    // A rollback must use the snapshot that seeded the failed generation.
    // A normal upgrade may skip versions that this player never opened.
    const candidates=version==='bundled'?[]:restoringParent
      ? [head.source,previous,'bundled']
      : [previous,last,'bundled'];
    const source=candidates.find(v=>saveVersion(v)&&v!==version&&fs.existsSync(path.join(base,v)));
    if(source){
      const from=path.join(base,source);
      if(touched.has(from))throw Error('Restart the launcher to finish preparing saves for the updated game.');
      const temp=next+'.copy.'+crypto.randomUUID();
      try{
        fs.cpSync(from,temp,{recursive:true,filter:file=>!ignoredSaveFiles.has(path.basename(file))});
        fs.renameSync(temp,next);
      }finally{fs.rmSync(temp,{recursive:true,force:true});}
    }else fs.mkdirSync(next);
    history.slots[version]={parent:previous,source:source||null};
  }
  history.last=version;writeJson(journal,history);
  touched.add(next);
  return next;
}
module.exports={getFeed,verifyFeed,download,stageItem,sha,FEED,markRejectedUpdate,isRejectedUpdate,prepareSaveDirectory};
