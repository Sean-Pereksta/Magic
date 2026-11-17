const fs=require('node:fs');
const path=require('node:path');
const http=require('node:http');
const {createHash}=require('node:crypto');
const {spawn}=require('node:child_process');
const {inDir}=require('./storage.cjs');
const types={html:'text/html; charset=utf-8',js:'text/javascript',mjs:'text/javascript',css:'text/css',json:'application/json',png:'image/png',jpg:'image/jpeg',jpeg:'image/jpeg',webp:'image/webp',svg:'image/svg+xml',gif:'image/gif',mp3:'audio/mpeg',ogg:'audio/ogg',wav:'audio/wav',wasm:'application/wasm',woff2:'font/woff2',glb:'model/gltf-binary',ico:'image/x-icon',webmanifest:'application/manifest+json'};
function portFor(id){return 20000+createHash('sha256').update('catnmice:'+id).digest().readUInt32BE()%30000;}
function serve(root,req,res,port) {
  const reject=code=>{res.writeHead(code);res.end();};
  if(req.headers.host!=='127.0.0.1:'+port || !['GET','HEAD'].includes(req.method))return reject(403);
  try{
    const clean=decodeURIComponent(new URL(req.url,'http://localhost').pathname);
    let file=inDir(root,clean);
    if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');
    const stat=fs.statSync(file);
    if(!stat.isFile())return reject(404);
    const headers={'Content-Type':types[path.extname(file).slice(1).toLowerCase()]||'application/octet-stream','X-Content-Type-Options':'nosniff','Cache-Control':'no-cache','Accept-Ranges':'bytes'};
    let start=0,end=stat.size-1,status=200;
    if(req.headers.range){
      const range=/^bytes=(\d+)-(\d*)$/.exec(req.headers.range);
      if(!range)return reject(416);
      start=Number(range[1]);end=range[2]?Math.min(Number(range[2]),end):end;
      if(start>end || start>=stat.size)return reject(416);
      headers['Content-Range']='bytes '+start+'-'+end+'/'+stat.size;status=206;
    }
    headers['Content-Length']=end-start+1;res.writeHead(status,headers);
    if(req.method==='HEAD' || !stat.size)return res.end();
    const stream=fs.createReadStream(file,{start,end});
    stream.on('error',()=>res.destroy());res.on('close',()=>stream.destroy());stream.pipe(res);
  }catch{reject(404);}
}
async function startOffline(game,root,port=portFor(game.id)) {
  const spec=game.offline;
  if(!spec)throw Error('This title requires its online edition.');
  const base='http://127.0.0.1:'+port;
  if(spec.type==='node'){
    const script=inDir(root,spec.server);
    const args=(spec.args||[]).map(arg=>arg.replaceAll('{port}',String(port)));
    const env={...process.env,ELECTRON_RUN_AS_NODE:'1',HOST:'127.0.0.1',PORT:String(port)};
    const proc=spawn(process.execPath,[script,...args],{cwd:path.dirname(script),windowsHide:true,env,stdio:'ignore'});
    let failed=false;proc.on('error',()=>{failed=true});
    let ok=false;
    for(let i=0;i<100;i++){
      await new Promise(r=>setTimeout(r,100));
      if(failed||proc.exitCode!==null)break;
      try{const ready=await fetch(base+(spec.health||'/'),{signal:AbortSignal.timeout(500)});if(ready.ok){ok=true;break;}}catch{}
    }
    if(!ok){proc.kill();throw Error('The local match service could not start. The install can be repaired in Setup.');}
    return {url:new URL(spec.entry,base).href,close:()=>proc.kill()};
  }
  const server=http.createServer((req,res)=>serve(root,req,res,port));
  await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(port,'127.0.0.1',resolve);});
  return {url:new URL(spec.entry,base).href,close:()=>{server.close();server.closeAllConnections();}};
}
module.exports={portFor,serve,startOffline};
