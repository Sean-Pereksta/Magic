const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');
const {execFileSync} = require('node:child_process');
const {boot,selectHub} = require('../desktop/bootstrap.cjs');
const {getFeed,stageItem,prepareSaveDirectory,markRejectedUpdate,isRejectedUpdate,canUseReleaseOverlay} = require('../desktop/updates.cjs');
const {writeJson} = require('../desktop/storage.cjs');

const a='a'.repeat(64), b='b'.repeat(64), c='c'.repeat(64);
function sandbox(t) {
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'catnmice-recovery-'));
  t.after(() => fs.rmSync(dir,{recursive:true,force:true}));
  return dir;
}
test('a hub failing asynchronous startup immediately restores its predecessor and quarantines only that hash',async t=>{
  const data=sandbox(t),events=[];
  writeJson(path.join(data,'hub-version.json'),{active:b,previous:a});
  const main=path.join(data,'versions','hub',b,'desktop','main.cjs');
  fs.mkdirSync(path.dirname(main),{recursive:true});
  fs.writeFileSync(main,'exports.start = ({startupFailed}) => setImmediate(() => startupFailed(new Error("failed to initialize")));');
  const exited=new Promise(resolve=>{
    boot({
      isPackaged:true,setName(){},requestSingleInstanceLock:()=>true,getPath:()=>data,
      relaunch:()=>events.push('relaunch'),exit:code=>{events.push(code);resolve();}
    });
  });
  await exited;
  assert.deepEqual(events,['relaunch',1]);
  assert.equal(selectHub(data).active,a);
  assert.equal(isRejectedUpdate(data,{id:'hub',sha256:b}),true);
  assert.equal(isRejectedUpdate(data,{id:'hub',sha256:c}),false);
});
test('an interrupted hub startup returns to bundled files when its predecessor also failed',t=>{
  const data=sandbox(t);
  writeJson(path.join(data,'hub-version.json'),{active:c,previous:b});
  writeJson(path.join(data,'startup.json'),{active:c});
  markRejectedUpdate(data,'hub',b);
  assert.deepEqual(selectHub(data),{active:null,previous:null});
  assert.equal(isRejectedUpdate(data,{id:'hub',sha256:c}),true);
  assert.equal(isRejectedUpdate(data,{id:'cat',sha256:c}),false);
});
test('a fresh installer takes precedence over a stale hub and catalog',async t=>{
  const data=sandbox(t);
  const bundled={provider:'github',sequence:30,components:{hub:c,catalog:c}};
  for(const id of ['hub','catalog']){
    writeJson(path.join(data,'versions',id,a,'.complete'),{sha256:a,version:'catnmice-hub-29-a1b2c3d4'});
    writeJson(path.join(data,'versions',id,c,'.complete'),{sha256:c,version:'catnmice-hub-30-a1b2c3d4'});
    assert.equal(canUseReleaseOverlay(data,id,a,bundled),false);
    assert.equal(canUseReleaseOverlay(data,id,c,bundled),false);
  }
  writeJson(path.join(data,'hub-version.json'),{active:a,previous:null});
  assert.equal(selectHub(data,bundled).active,null);
  assert.equal(isRejectedUpdate(data,{id:'hub',sha256:a}),false);

  await stageItem({id:'hub',sha256:a,version:'catnmice-hub-31-e5f6a7b8'},data,async()=>{throw Error('Unexpected download');});
  assert.equal(selectHub(data,bundled).active,a);
  writeJson(path.join(data,'hub-version.json'),{active:a,previous:c});
  writeJson(path.join(data,'startup.json'),{active:a});
  assert.equal(selectHub(data,bundled).active,null);
  assert.equal(isRejectedUpdate(data,{id:'hub',sha256:a}),true);
});
test('profile saves survive skipped versions and failed upgrades without copying a failed generation back',t=>{
  const data=sandbox(t);
  const base=path.join(data,'saves','player-a','cat');
  const touched=new Set();
  const slot=prepareSaveDirectory(base,null,null,touched);
  fs.writeFileSync(path.join(slot,'save'),'first');
  fs.writeFileSync(path.join(slot,'LOCK'),'locked');
  assert.throws(()=>prepareSaveDirectory(base,a,null,touched),/Restart the launcher/);
  const savedA=prepareSaveDirectory(base,a,null);
  assert.equal(fs.existsSync(path.join(savedA,'LOCK')),false);
  assert.equal(fs.readFileSync(path.join(savedA,'save'),'utf8'),'first');
  fs.writeFileSync(path.join(savedA,'save'),'progress in A');
  const savedC=prepareSaveDirectory(base,c,b);
  assert.equal(fs.readFileSync(path.join(savedC,'save'),'utf8'),'progress in A');
  fs.writeFileSync(path.join(savedC,'save'),'incompatible C format');
  const restoredB=prepareSaveDirectory(base,b,null);
  assert.equal(fs.readFileSync(path.join(restoredB,'save'),'utf8'),'progress in A');
  assert.equal(fs.readFileSync(path.join(slot,'save'),'utf8'),'first');
  assert.equal(fs.readFileSync(path.join(savedC,'save'),'utf8'),'incompatible C format');
  const other=prepareSaveDirectory(path.join(data,'saves','player-b','cat'),b,null);
  assert.equal(fs.existsSync(path.join(other,'save')),false);
});
test('interrupted signed package resumes from disk and repairs incomplete staging before activation',{skip:process.platform!=='win32'},async t=>{
  const data=sandbox(t),source=path.join(data,'source');
  fs.mkdirSync(source);
  fs.writeFileSync(path.join(source,'index.html'),'Game ready');
  const archive=path.join(data,'fixture.zip');
  const tar=path.join(process.env.SystemRoot,'System32','tar.exe');
  execFileSync(tar,['-acf',archive,'-C',source,'index.html'],{windowsHide:true});
  const bytes=fs.readFileSync(archive),half=Math.floor(bytes.length/2);
  const keys=crypto.generateKeyPairSync('ed25519');
  const item={id:'cat',version:'1.0.1',size:bytes.length,sha256:crypto.createHash('sha256').update(bytes).digest('hex'),url:'https://example.invalid/release.zip'};
  const payload=Buffer.from(JSON.stringify({schema:1,sequence:10,items:[item]}));
  const envelope={payload:payload.toString('base64'),signature:crypto.sign(null,payload,keys.privateKey).toString('base64')};
  const feed=await getFeed(keys.publicKey,9,async()=>Response.json(envelope));
  await assert.rejects(getFeed(keys.publicKey,11,async()=>Response.json(envelope)),/older/);
  const dest=path.join(data,'versions',item.id,item.sha256);
  await assert.rejects(stageItem(feed.items[0],data,async()=>new Response(new ReadableStream({
    start(controller){
      controller.enqueue(bytes.subarray(0,half));
      setTimeout(()=>controller.error(Error('connection interrupted')),40);
    }
  }))),/connection interrupted/);
  assert.equal(fs.existsSync(dest),false);
  fs.mkdirSync(dest,{recursive:true});
  fs.writeFileSync(path.join(dest,'.complete'),'{}');
  fs.writeFileSync(path.join(dest,'partial'),'unfinished');
  const offsets=[];
  async function resumed(url,{headers}) {
    assert.equal(url,item.url);
    const offset=Number(/^bytes=(\d+)-$/.exec(headers.Range)[1]);
    offsets.push(offset);
    const end=Math.min(offset+15,bytes.length);
    return new Response(bytes.subarray(offset,end),{
      status:206,headers:{'Content-Range':'bytes '+offset+'-'+(end-1)+'/'+bytes.length}
    });
  }
  const [first,second]=await Promise.all([stageItem(item,data,resumed),stageItem(item,data,resumed)]);
  assert.equal(offsets[0],half);
  assert.equal(offsets.length,new Set(offsets).size);
  assert.equal(first,second);
  assert.equal(fs.readFileSync(path.join(dest,'index.html'),'utf8'),'Game ready');
  assert.equal(fs.existsSync(path.join(dest,'partial')),false);
  assert.equal(JSON.parse(fs.readFileSync(path.join(dest,'.complete'))).sha256,item.sha256);
  assert.equal(await stageItem(item,data,async()=>{throw Error('Unexpected download');}),dest);
});
