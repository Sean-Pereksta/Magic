const fs=require('node:fs');
const path=require('node:path');
const http=require('node:http');
const vm=require('node:vm');
const assert=require('node:assert/strict');
const {chromium}=require(process.env.BRAMBLEBOUND_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../../..'),gameDir=path.resolve(__dirname,'..');
const shots=process.env.BRAMBLEBOUND_SCREENSHOTS;
const KEY='bramblebound.profile.v1',NAME_KEY='bramblebound.cloud.name.v1';
const mime={'.html':'text/html','.js':'text/javascript','.mjs':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.webp':'image/webp','.json':'application/json'};
const server=http.createServer((req,res)=>{try{let file=path.resolve(root,'.'+decodeURIComponent(new URL(req.url,'http://localhost').pathname));if(!file.startsWith(root+path.sep)||file.includes(path.sep+'.git'))throw Error();if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');res.setHeader('Content-Type',mime[path.extname(file)]||'application/octet-stream');fs.createReadStream(file).on('error',()=>res.end()).pipe(res);}catch{res.writeHead(404).end();}});
function profile(points,prepared=false){
  const context=vm.createContext({console});context.window=context;
  for(const name of ['catalog','legacy','progression','living-combat','engine'])vm.runInContext(fs.readFileSync(path.join(gameDir,name+'.js'),'utf8'),context);
  const E=context.GameEngine,p=E.freshProfile();p.points=points;
  const data={game:'bramblebound',format:1,profile:p,settings:{audio:false,motion:false}};
  if(prepared){const camp=E.freshProfile();E.start(camp);const a=camp.run.active[0],b=camp.run.pack.find(item=>item.id===a.id);E.act(camp,'combine',a.uid,b.uid);data.camp=camp;}
  return JSON.stringify(data);
}
// Only the transport is isolated. Actual production PBKDF2/AES-GCM, profile
// validation, cloud service, controls and local-backup code run unchanged.
function installFixture({raw}){
  localStorage.setItem('bramblebound.profile.v1',raw);
  const later=window.setTimeout.bind(window);window.setTimeout=(callback,delay,...args)=>later(callback,window.__accelerateCloudAutosave&&delay===30000?10:delay,...args);
  class Bytes{constructor(value){this.value=new Uint8Array(value);}toUint8Array(){return this.value.slice();}static fromUint8Array(value){return new Bytes(value);}}
  const encode=value=>value instanceof Bytes?{__bytes:Array.from(value.toUint8Array())}:Array.isArray(value)?value.map(encode):value&&typeof value==='object'?Object.fromEntries(Object.entries(value).map(([k,v])=>[k,encode(v)])):value;
  const decode=value=>value?.__bytes?new Bytes(value.__bytes):Array.isArray(value)?value.map(decode):value&&typeof value==='object'?Object.fromEntries(Object.entries(value).map(([k,v])=>[k,decode(v)])):value;
  const snapshot=result=>({metadata:{fromCache:false},exists:()=>result.record!==null,data:()=>decode(result.record)});
  const api={db:{},auth:{currentUser:{uid:'isolated-browser-fixture'}},doc:(_db,...parts)=>({path:parts.join('/')}),Bytes,serverTimestamp:()=>({__serverTimestamp:true}),
    getDocFromServer:async ref=>snapshot(await window.__cloudTransport({op:'get',path:ref.path})),
    runTransaction:async(_db,callback)=>{for(let attempt=0;attempt<3;attempt++){const reads=[],writes=[];const value=await callback({get:async ref=>{const result=await window.__cloudTransport({op:'get',path:ref.path});reads.push({path:ref.path,version:result.version});return snapshot(result);},set:(ref,record)=>writes.push({path:ref.path,record:encode(record)})});const result=await window.__cloudTransport({op:'commit',reads,writes});if(!result.conflict)return value;}throw Error('Simulated transaction retry limit.');}
  };
  Object.defineProperty(window,'BrambleCloud',{configurable:true,set(original){delete window.BrambleCloud;window.BrambleCloud={...original,create:options=>{const client=original.create({...options,firebase:async()=>api});window.__cloudClient=client;return client;}};}});
}
async function main(){
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
  const options={headless:true}; if(process.env.BRAMBLEBOUND_BROWSER)options.executablePath=process.env.BRAMBLEBOUND_BROWSER;const edge='C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe';if(process.platform==='win32'&&fs.existsSync(edge))options.executablePath=edge;
  const browser=await chromium.launch(options),results=[],records=new Map(),versions=new Map(),heldWrites=[];let failWrites=false,holdWrites=false,writes=0;
  const commit=message=>{if(failWrites)throw Error('Simulated Firebase outage.');if(message.reads.some(read=>(versions.get(read.path)||0)!==read.version))return {conflict:true};for(const entry of message.writes){records.set(entry.path,entry.record);versions.set(entry.path,(versions.get(entry.path)||0)+1);writes++;}return {conflict:false};};
  const transport=message=>{if(message.op==='get')return {record:records.get(message.path)||null,version:versions.get(message.path)||0};if(message.op==='commit'){if(holdWrites)return new Promise((resolve,reject)=>heldWrites.push({message,resolve,reject}));return commit(message);}throw Error('Unknown isolated transport action.');};
  async function open(width,raw){
    const context=await browser.newContext({viewport:{width,height:width===390?844:1000}}),page=await context.newPage(),errors=[],external=[];
    await context.exposeBinding('__cloudTransport',(_source,message)=>transport(message));await context.addInitScript(installFixture,{raw});
    await context.route('**/*',route=>{if(!route.request().url().startsWith(origin)){external.push(route.request().url());return route.abort();}return route.continue();});
    page.on('pageerror',error=>errors.push(error.message));await page.goto(origin+'/game/bramblebound/');await page.locator('#title-cloud').waitFor();return {context,page,errors,external};
  }
  async function form(page,mode,name,password){
    if(await page.locator('#modal[open]').count())await page.locator('#close-btn').click();
    await page.locator('#title-cloud').click();await page.locator('[data-cloud-mode="'+mode+'"]').click();await page.locator('#cloud-name').fill(name);await page.locator('#cloud-password').fill(password);
  }
  const stored=page=>page.evaluate(key=>localStorage.getItem(key),KEY);
  try{
    for(const width of [1440,390]){
      const password='Browser-only cloud secret '+width,saveName='Bramble browser '+width,first=await open(width,profile(9,true)),page=first.page;
      await form(page,'save',saveName,password);await page.locator('#cloud-submit').click();await page.locator('#modal').waitFor({state:'hidden'});
      await page.waitForFunction(()=>[...document.querySelectorAll('[data-cloud-status]')].some(el=>/Saved to Firebase/.test(el.textContent)));
      assert.equal(await page.evaluate(key=>localStorage.getItem(key),NAME_KEY),saveName);assert.equal(records.size,width===1440?1:2);
      const record=[...records.values()].find(record=>record.saveName===saveName);assert.equal(record.gameType,'bramblebound-cloud-save');assert.equal(record.status,'active');assert.deepEqual(record.players,[]);assert.ok(record.payload.__bytes.length>16);assert.ok(!JSON.stringify(record).includes(password));
      assert.equal(await page.evaluate(password=>JSON.stringify({...localStorage}).includes(password),password),false,'password must never be persisted');
      await page.reload();await page.locator('#title-cloud').waitFor();await page.locator('#title-cloud').click();assert.equal(await page.locator('#cloud-name').inputValue(),saveName);assert.equal(await page.locator('#cloud-password').inputValue(),'');
      assert.match(await page.locator('#modal [data-cloud-status]').innerText(),/Remembered|Reopen/i);await page.locator('#close-btn').click();await page.locator('#title-profile').click();await page.locator('[data-do="cloud-forget"]').click();
      assert.equal(await page.evaluate(key=>localStorage.getItem(key),NAME_KEY),null);assert.ok([...records.values()].some(r=>r.saveName===saveName),'disconnect must not delete Firebase slot');await page.locator('#close-btn').click();
      const second=await open(width,profile(2)),other=second.page;
      await form(other,'load',saveName,'wrong password');const localBefore=await stored(other);await other.locator('#cloud-submit').click();
      await other.waitForFunction(()=>/password|damaged|decrypt/i.test(document.querySelector('#modal [data-cloud-status]')?.textContent||''));assert.equal(await stored(other),localBefore);
      await other.locator('#cloud-password').fill(password);await other.locator('#cloud-submit').click();await other.locator('[data-do="cloud-restore"]').waitFor();assert.equal(await stored(other),localBefore,'decrypting cloud data must not apply it before confirmation');
      await other.locator('[data-do="cloud-cancel"]').click();assert.equal(await stored(other),localBefore);
      await form(other,'load',saveName,password);await other.locator('#cloud-submit').click();await other.locator('[data-do="cloud-restore"]').click();await other.locator('#modal').waitFor({state:'hidden'});assert.equal(await other.locator('#title-screen').isVisible(),true);
      const restored=JSON.parse(await stored(other));assert.equal(restored.profile.points,9);assert.equal(restored.settings.motion,false);assert.ok(restored.camp.run.active.some(item=>item.level===2),'prepared kit must cross devices');
      assert.equal(await other.evaluate(()=>JSON.parse(localStorage.getItem('bramblebound.cloud.backup.v1')).profile.points),2,'the replaced local profile must have a backup');
      assert.equal(await other.evaluate(password=>JSON.stringify({...localStorage}).includes(password),password),false);
      // Exercise the actual 30-second autosave path with its timer accelerated.
      // A disconnect during an unacknowledged write must stay disconnected even
      // if that old write later succeeds; only the temporary transport is held.
      holdWrites=true;await other.evaluate(()=>{window.__accelerateCloudAutosave=true;});await other.locator('#title-play').click();
      await other.waitForFunction(()=>window.__cloudClient?.status().pending===true);
      const deadline=Date.now()+3000;while(!heldWrites.length&&Date.now()<deadline)await new Promise(resolve=>setTimeout(resolve,10));assert.ok(heldWrites.length,'automatic cloud upload should reach the transport');
      await other.locator('#profile-btn').click();await other.locator('[data-do="cloud-forget"]').click();assert.equal(await other.evaluate(key=>localStorage.getItem(key),NAME_KEY),null);
      holdWrites=false;for(const pending of heldWrites.splice(0)){try{pending.resolve(commit(pending.message));}catch(error){pending.reject(error);}}
      await other.waitForFunction(()=>window.__cloudClient.status().busy===false&&window.__cloudClient.status().pending===false);
      await other.waitForFunction(()=>[...document.querySelectorAll('[data-cloud-status]')].some(el=>/disconnected|not connected/i.test(el.textContent)));
      assert.equal(await other.evaluate(key=>localStorage.getItem(key),NAME_KEY),null,'a late acknowledgement must not reconnect the forgotten slot');
      assert.equal(await other.evaluate(()=>window.__cloudClient.status().name),'','deferred forgetting must clear the service session too');
      await other.evaluate(()=>{window.__accelerateCloudAutosave=false;});await other.locator('#close-btn').click();await other.locator('.brand[data-title]').click();
      // Force a failed new-slot write, then prove local play remains available.
      await form(other,'save','Failure test '+width,password);const beforeFailure=await stored(other),beforeWrites=writes;failWrites=true;await other.locator('#cloud-submit').click();
      await other.waitForFunction(()=>/outage|not confirmed/i.test(document.querySelector('#modal [data-cloud-status]')?.textContent||''));failWrites=false;
      assert.equal(await stored(other),beforeFailure);assert.equal(writes,beforeWrites);await other.locator('#close-btn').click();await other.locator('#title-play').click();await other.waitForFunction(()=>JSON.parse(localStorage.getItem('bramblebound.profile.v1'))?.profile?.run!==null);assert.match(await other.locator('#run-btn').innerText(),/Pause/);
      await other.locator('#run-btn').click();await other.locator('#profile-btn').click();await other.locator('[data-do="cloud-forget"]').click();await other.locator('#close-btn').click();
      assert.equal(await other.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);assert.deepEqual(first.errors,[]);assert.deepEqual(second.errors,[]);assert.deepEqual(first.external,[]);assert.deepEqual(second.external,[]);
      if(shots){fs.mkdirSync(shots,{recursive:true});await other.screenshot({path:path.join(shots,'cloud-local-after-failure-'+width+'.png'),fullPage:true});}
      results.push(width+'px: real encrypted cross-device save/load, wrong password, confirmation/cancel, prepared kit/settings, local backup, disconnect during pending automatic upload, remembered name only, failed upload and continued local play; no external requests or browser errors');
      await first.context.close();await second.context.close();
    }
    console.log(results.join('\n'));
  }finally{await browser.close();}
}
main().catch(error=>{console.error(error);process.exitCode=1;}).finally(()=>server.close());
