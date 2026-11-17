const test=require('node:test');
const assert=require('node:assert/strict');
const {generateKeyPairSync,sign,createHash}=require('node:crypto');
const fs=require('node:fs/promises'),os=require('node:os'),path=require('node:path');
const {verifyFeed,download}=require('../desktop/updates.cjs');
const {inDir}=require('../desktop/storage.cjs');
const {cardScript,validInvite}=require('../desktop/official.cjs');
const bytes=Buffer.from('Catnmice verified package fixture');
const item={id:'cat',version:'1.0.0',size:bytes.length,sha256:createHash('sha256').update(bytes).digest('hex'),url:'https://www.catnmice.com/hub/test.zip'};
test('signed catalog enforces authenticity, ids and transport',()=>{
  const keys=generateKeyPairSync('ed25519');
  function envelope(items){const data=Buffer.from(JSON.stringify({schema:1,sequence:42,items}));return {payload:data.toString('base64'),signature:sign(null,data,keys.privateKey).toString('base64')};}
  const signed=envelope([item]);assert.deepEqual(verifyFeed(signed,keys.publicKey).items,[item]);
  assert.throws(()=>verifyFeed({...signed,payload:Buffer.from('changed').toString('base64')},keys.publicKey));
  for(const bad of [{id:'../cat'},{url:'http://localhost/game'},{size:-1}])assert.throws(()=>verifyFeed(envelope([{...item,...bad}]),keys.publicKey));
});
test('interrupted update resumes verified bytes or restarts when Range is ignored',async t=>{
  const dir=await fs.mkdtemp(path.join(os.tmpdir(),'catnmice-range-'));
  t.after(()=>fs.rm(dir,{recursive:true,force:true}));
  const file=path.join(dir,'pack.zip');
  for(const code of [206,200]){
    await fs.writeFile(file,bytes.subarray(0,7));
    await download(item,file,async(url,opt)=>{
      assert.equal(opt.headers.Range,'bytes=7-');return new Response(code===206?bytes.subarray(7):bytes,{status:code,headers:code===206?{'Content-Range':'bytes 7-'+(bytes.length-1)+'/'+bytes.length}:{}});
    });assert.deepEqual(await fs.readFile(file),bytes);
  }
  await fs.writeFile(file,Buffer.alloc(bytes.length));await assert.rejects(download(item,file));await assert.rejects(fs.stat(file));
});
test('unsafe pack paths and unrelated invite URLs are rejected',()=>{
  assert.throws(()=>inDir('packs','../secret'));assert.throws(()=>inDir('packs','C:/secret'));
  assert.throws(()=>validInvite('https://catnmice.com.example.org/'));assert.throws(()=>cardScript('bad$id','single'));
  assert.equal(validInvite('https://sean-pereksta.github.io/Truth/games/bannerlands/#room=a'),'https://sean-pereksta.github.io/Truth/games/bannerlands/#room=a');
});
