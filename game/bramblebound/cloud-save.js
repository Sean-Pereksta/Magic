/* Named, encrypted Firebase journeys, using the same anonymous-auth/lobbies
   surface as Arcane Wilds. The password stays with the caller in this session.
   This module never writes local storage or replaces a running expedition. */
(function(root,factory){
  const api=factory(root);
  if(typeof module==='object'&&module.exports)module.exports=api;
  else root.BrambleCloud=api;
})(typeof globalThis!=='undefined'?globalThis:this,function(root){
  'use strict';
  const constants=Object.freeze({
    FORMAT:1,COLLECTION:'lobbies',GAME_TYPE:'bramblebound-cloud-save',
    KIND:'brambleboundCloudSave',APP_NAME:'bramblebound-cloud',
    KDF_ITERATIONS:210000,MAX_ITERATIONS:600000,
    MAX_ENCRYPTED_BYTES:700*1024,MAX_PLAIN_BYTES:1024*1024,TIMEOUT_MS:15000
  });
  const encoder=new TextEncoder(),decoder=new TextDecoder('utf-8',{fatal:true});
  let firebaseCache=null;
  function error(message,code){const e=new Error(message);if(code)e.code=code;return e;}
  function cryptoApi(){
    if(!root.crypto?.subtle||!root.crypto?.getRandomValues)throw error('Encrypted cloud saves need a secure browser connection.','crypto-unavailable');
    return root.crypto;
  }
  function cleanName(value){
    if(typeof value!=='string')throw error('Enter a cloud save name.','credentials');
    const name=value.trim().replace(/\s+/g,' ');
    if(name.length<2||name.length>48||/[\u0000-\u001f\u007f]/.test(name))throw error('Use 2–48 characters for the cloud save name.','credentials');
    return name;
  }
  function normalizeName(value){return cleanName(value).toLowerCase();}
  function checkPassword(password){
    if(typeof password!=='string'||password.length<4||password.length>1024)throw error('Use 4–1024 characters for the save password.','credentials');
  }
  const hex=bytes=>Array.from(bytes,b=>b.toString(16).padStart(2,'0')).join('');
  async function documentId(name){return 'bramblebound-save--'+hex(new Uint8Array(await cryptoApi().subtle.digest('SHA-256',encoder.encode('bramblebound:'+normalizeName(name)))));}
  function bytes(value){
    if(value?.toUint8Array)value=value.toUint8Array();
    if(value instanceof Uint8Array)return value;
    if(ArrayBuffer.isView(value)&&value.BYTES_PER_ELEMENT===1)return new Uint8Array(value.buffer,value.byteOffset,value.byteLength);
    if(value instanceof ArrayBuffer)return new Uint8Array(value);
    if(Array.isArray(value)&&value.every(n=>Number.isInteger(n)&&n>=0&&n<=255))return new Uint8Array(value);
    throw error('The cloud save contains malformed encrypted data.','invalid-record');
  }
  function plainBytes(raw){
    if(typeof raw!=='string')throw error('A valid Bramblebound profile is required.','invalid-profile');
    // Avoid allocating an arbitrarily large UTF-8 buffer before enforcing bounds.
    if(raw.length>constants.MAX_PLAIN_BYTES)throw error('This profile exceeds the 1 MB cloud save limit. Export a local copy instead.','save-too-large');
    const result=encoder.encode(raw);
    if(result.byteLength>constants.MAX_PLAIN_BYTES)throw error('This profile exceeds the 1 MB cloud save limit. Export a local copy instead.','save-too-large');
    return result;
  }
  async function readLimited(stream,limit){
    const reader=stream.getReader(),chunks=[];let length=0;
    try{
      while(true){const part=await reader.read();if(part.done)break;length+=part.value.byteLength;
        if(length>limit){await reader.cancel().catch(()=>{});throw error('The cloud save exceeds its supported size limit.','save-too-large');}
        chunks.push(part.value);
      }
    }finally{reader.releaseLock();}
    const output=new Uint8Array(length);let offset=0;for(const chunk of chunks){output.set(chunk,offset);offset+=chunk.byteLength;}return output;
  }
  async function pack(plain){
    if(typeof root.CompressionStream!=='function')return {bytes:plain,compression:'none'};
    try{
      const zipped=await readLimited(new Blob([plain]).stream().pipeThrough(new root.CompressionStream('gzip')),constants.MAX_PLAIN_BYTES+1024);
      if(zipped.byteLength+32<plain.byteLength)return {bytes:zipped,compression:'gzip'};
    }catch{/* Compression is optional; an uncompressed profile is still valid. */}
    return {bytes:plain,compression:'none'};
  }
  async function key(password,salt,iterations){
    const api=cryptoApi(),base=await api.subtle.importKey('raw',encoder.encode(password),'PBKDF2',false,['deriveKey']);
    return api.subtle.deriveKey({name:'PBKDF2',salt,iterations,hash:'SHA-256'},base,{name:'AES-GCM',length:256},false,['encrypt','decrypt']);
  }
  function inspectEnvelope(record){
    if(!record||typeof record!=='object'||record.gameType!==constants.GAME_TYPE||record.format!==constants.FORMAT||record.cipher!=='AES-GCM-256'||record.kdf!=='PBKDF2-SHA256')throw error('This is not a supported Bramblebound cloud save.','invalid-record');
    if(!Number.isInteger(record.iterations)||record.iterations<100000||record.iterations>constants.MAX_ITERATIONS)throw error('The cloud save has unsupported encryption settings.','invalid-record');
    if(!['none','gzip'].includes(record.compression))throw error('The cloud save uses unsupported compression.','invalid-record');
    const salt=bytes(record.salt),iv=bytes(record.iv),payload=bytes(record.payload);
    if(salt.length!==16||iv.length!==12||payload.length<16)throw error('The cloud save contains malformed encrypted data.','invalid-record');
    if(payload.length>constants.MAX_ENCRYPTED_BYTES)throw error('The encrypted cloud save exceeds its size limit.','save-too-large');
    return {salt,iv,payload};
  }
  async function encrypt(raw,password){
    checkPassword(password);const packed=await pack(plainBytes(raw)),api=cryptoApi();
    if(packed.bytes.byteLength+16>constants.MAX_ENCRYPTED_BYTES)throw error('This encrypted profile is too large for a cloud slot. Export a local copy instead.','save-too-large');
    const salt=api.getRandomValues(new Uint8Array(16)),iv=api.getRandomValues(new Uint8Array(12));
    const derived=await key(password,salt,constants.KDF_ITERATIONS);
    const payload=new Uint8Array(await api.subtle.encrypt({name:'AES-GCM',iv},derived,packed.bytes));
    return {format:constants.FORMAT,gameType:constants.GAME_TYPE,cipher:'AES-GCM-256',kdf:'PBKDF2-SHA256',iterations:constants.KDF_ITERATIONS,compression:packed.compression,salt,iv,payload};
  }
  async function decrypt(record,password){
    checkPassword(password);const checked=inspectEnvelope(record),derived=await key(password,checked.salt,record.iterations);
    let plain;
    try{plain=new Uint8Array(await cryptoApi().subtle.decrypt({name:'AES-GCM',iv:checked.iv},derived,checked.payload));}
    catch{throw error('The password is incorrect, or this cloud save is damaged.','decrypt-failed');}
    if(record.compression==='gzip'){
      if(typeof root.DecompressionStream!=='function')throw error('This browser cannot decompress the cloud save.','compression-unavailable');
      try{plain=await readLimited(new Blob([plain]).stream().pipeThrough(new root.DecompressionStream('gzip')),constants.MAX_PLAIN_BYTES);}
      catch(e){if(e.code==='save-too-large')throw e;throw error('The compressed cloud save is damaged.','invalid-record');}
    }
    if(plain.byteLength>constants.MAX_PLAIN_BYTES)throw error('The cloud save exceeds the supported profile size.','save-too-large');
    try{return decoder.decode(plain);}catch{throw error('The cloud save contains invalid profile text.','invalid-profile');}
  }
  async function defaultFirebase(){
    if(firebaseCache)return firebaseCache;
    firebaseCache=(async()=>{
      const [app,fire,authModule,config]=await Promise.all([
        import('https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js'),
        import('https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js'),
        import('https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js'),
        import('/lobby/firebase-config.mjs')
      ]);
      const instance=app.getApps().find(candidate=>candidate.name===constants.APP_NAME)||app.initializeApp(config.firebaseConfig,constants.APP_NAME);
      const auth=authModule.getAuth(instance);if(!auth.currentUser)await authModule.signInAnonymously(auth);
      return {db:fire.getFirestore(instance),auth,doc:fire.doc,getDocFromServer:fire.getDocFromServer,runTransaction:fire.runTransaction,serverTimestamp:fire.serverTimestamp,Bytes:fire.Bytes};
    })().catch(e=>{firebaseCache=null;throw e;});
    return firebaseCache;
  }
  function inspectRecord(record,expectedName){
    const checked=inspectEnvelope(record);
    if(record.kind!==constants.KIND||record.host!=='bramblebound'||record.status!=='active'||!Array.isArray(record.players)||record.players.length!==0||!Number.isSafeInteger(record.revision)||record.revision<1)throw error('The Bramblebound cloud slot is malformed or incompatible.','invalid-record');
    let normalized;try{normalized=normalizeName(record.saveName);}catch{throw error('The cloud slot has an invalid save name.','invalid-record');}
    if(expectedName!==undefined&&normalized!==normalizeName(expectedName))throw error('The cloud save name does not match its requested slot.','invalid-record');
    for(const field of ['createdAtMs','updatedAtMs'])if(!Number.isSafeInteger(record[field])||record[field]<0)throw error('The cloud slot has invalid save timestamps.','invalid-record');
    return checked;
  }
  async function token(record){
    if(!record)return null;const checked=inspectRecord(record);
    const digest=new Uint8Array(await cryptoApi().subtle.digest('SHA-256',checked.payload));
    return [record.revision,hex(checked.iv),hex(checked.salt),hex(digest)].join(':');
  }
  function create(options={}){
    if(typeof options.validate!=='function')throw new TypeError('A Bramblebound profile validator is required.');
    const online=options.online||(()=>root.navigator?.onLine!==false),timeoutMs=options.timeoutMs??constants.TIMEOUT_MS;
    if(!Number.isFinite(timeoutMs)||timeoutMs<=0)throw new TypeError('Cloud timeout must be a positive duration.');
    const sessions=new Map();let busy=false,uncertain=false,uncertainId='',last=null,inFlightWrite=null;
    function status(){return {busy,uncertain,pending:!!inFlightWrite,name:last?.name||'',docId:last?.docId||'',revision:last?.revision||0,updatedAtMs:last?.updatedAtMs||0};}
    async function bounded(promise,label,onTimeout){
      let timer;try{return await Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>{onTimeout?.();reject(error(label+' timed out. Reconnect and reopen the cloud save before trying again.','timeout'));},timeoutMs);})]);}
      finally{clearTimeout(timer);}
    }
    function assertOnline(){if(online()===false)throw error('You are offline. Your local progress is safe; reconnect to use Firebase saves.','offline');}
    async function firebase(){
      const value=options.firebase===undefined?defaultFirebase():typeof options.firebase==='function'?options.firebase():options.firebase;
      const api=await bounded(Promise.resolve(value),'Connecting to Firebase');
      if(!api?.auth?.currentUser||typeof api.doc!=='function'||typeof api.getDocFromServer!=='function'||typeof api.runTransaction!=='function'||typeof api.serverTimestamp!=='function'||typeof api.Bytes?.fromUint8Array!=='function')throw error('Firebase could not establish an authenticated save connection.','firebase-unavailable');
      return api;
    }
    async function validate(raw){plainBytes(raw);if(await options.validate(raw)===false)throw error('This cloud slot does not contain a valid Bramblebound profile.','invalid-profile');}
    function snapshotData(snapshot){
      if(snapshot?.metadata?.fromCache)throw error('Firebase has not confirmed this cloud save from the server. Reconnect and try again.','offline');
      return snapshot.exists()?snapshot.data():null;
    }
    async function exclusive(work){
      if(busy)throw error('Another cloud save action is still pending.','busy');
      assertOnline();busy=true;try{return await work();}finally{busy=false;}
    }
    async function save(name,password,raw){return exclusive(async()=>{
      if(uncertain)throw error('The last cloud save was not confirmed. Reopen the cloud journey before saving again.','uncertain-write');
      const clean=cleanName(name);checkPassword(password);await validate(raw);
      const id=await documentId(clean),api=await firebase(),ref=api.doc(api.db,constants.COLLECTION,id);
      const prior=snapshotData(await bounded(api.getDocFromServer(ref),'Reading the cloud slot'));
      if(prior)inspectRecord(prior,clean);
      const before=await token(prior);
      if(prior){
        // A correct password alone must not overwrite a newer save from another
        // device or a named slot that has not been reopened in this session.
        await validate(await decrypt(prior,password));
        if(sessions.get(id)!==before)throw error('This cloud journey changed, or has not been reopened in this session. Reopen it before saving, or choose a new save name.','conflict');
      }
      const encrypted=await encrypt(raw,password),now=Date.now();
      const record={...encrypted,kind:constants.KIND,status:'active',name:'Bramblebound Save: '+clean,saveName:clean,host:'bramblebound',players:[],revision:(prior?.revision||0)+1,createdAtMs:prior?.createdAtMs||now,updatedAtMs:now,updatedAt:api.serverTimestamp(),salt:api.Bytes.fromUint8Array(encrypted.salt),iv:api.Bytes.fromUint8Array(encrypted.iv),payload:api.Bytes.fromUint8Array(encrypted.payload)};
      let expired=false;
      const write=Promise.resolve().then(()=>api.runTransaction(api.db,async tx=>{
        const current=snapshotData(await tx.get(ref));
        if(current)inspectRecord(current,clean);
        if(expired)throw error('The cloud save confirmation expired. Reopen the journey before saving again.','timeout');
        if(await token(current)!==before)throw error('Another device saved first. Reopen the cloud journey; your local progress has not been replaced.','conflict');
        if(expired)throw error('The cloud save confirmation expired.','timeout');
        tx.set(ref,record);
      }));
      inFlightWrite=write;write.then(()=>{if(inFlightWrite===write)inFlightWrite=null;},()=>{if(inFlightWrite===write)inFlightWrite=null;});
      try{await bounded(write,'Saving to Firebase',()=>{expired=true;});}
      catch(e){uncertain=true;uncertainId=id;throw e;}
      const next=await token(record);sessions.set(id,next);last={name:clean,docId:id,revision:record.revision,updatedAtMs:now};
      return {...last};
    });}
    async function load(name,password){return exclusive(async()=>{
      const clean=cleanName(name);checkPassword(password);
      // A timed-out transaction may still be settling. Do not present an older
      // server snapshot as reconciled while that write can still complete.
      if(inFlightWrite)await bounded(inFlightWrite.catch(()=>{}),'Waiting for the previous save');
      const id=await documentId(clean),api=await firebase(),ref=api.doc(api.db,constants.COLLECTION,id);
      const record=snapshotData(await bounded(api.getDocFromServer(ref),'Reopening the cloud journey'));
      if(!record){
        if(uncertain&&uncertainId===id){uncertain=false;uncertainId='';sessions.delete(id);throw error('Firebase confirmed that this save was not created. Your local progress is safe; you can retry saving.','not-found');}
        throw error('No Bramblebound cloud save exists with that name.','not-found');
      }
      inspectRecord(record,clean);const raw=await decrypt(record,password);await validate(raw);
      sessions.set(id,await token(record));uncertain=false;uncertainId='';last={name:record.saveName?cleanName(record.saveName):clean,docId:id,revision:record.revision,updatedAtMs:Number(record.updatedAtMs)||0};
      return {...last,raw};
    });}
    function forget(){if(busy||inFlightWrite)throw error('Wait for the pending cloud action before forgetting its binding.','busy');sessions.clear();last=null;uncertain=false;uncertainId='';}
    return Object.freeze({save,load,forget,resetSession:forget,status});
  }
  return Object.freeze({create,encrypt,decrypt,documentId,normalizeName,cleanName,constants,...constants});
});
