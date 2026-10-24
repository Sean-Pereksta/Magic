import {validate,newSave} from './core.mjs';
export const ROOT=['games_rogueduel','monkey_trails_v1'],ITERATIONS=310000;
const enc=new TextEncoder(),dec=new TextDecoder(),PREFIX='monkey-trails.v1.';
const b64=a=>{let text='';for(let i=0;i<a.length;i+=8192)text+=String.fromCharCode(...a.subarray(i,i+8192));return btoa(text);};
const bytes=s=>Uint8Array.from(atob(s),c=>c.charCodeAt(0));
export const normalize=name=>String(name).trim().toLowerCase();
export async function identity(name,mode='gps'){
  return [...new Uint8Array(await crypto.subtle.digest('SHA-256',enc.encode(`monkey-trails:${mode}:${normalize(name)}`)))].map(n=>n.toString(16).padStart(2,'0')).join('');
}
export async function derive(password,salt){
  const material=await crypto.subtle.importKey('raw',enc.encode(password),'PBKDF2',false,['deriveKey']);
  return crypto.subtle.deriveKey({name:'PBKDF2',salt:bytes(salt),iterations:ITERATIONS,hash:'SHA-256'},material,{name:'AES-GCM',length:256},false,['encrypt','decrypt']);
}
export async function seal(state,key,salt,id){
  validate(state);const iv=crypto.getRandomValues(new Uint8Array(12));
  const ciphertext=await crypto.subtle.encrypt({name:'AES-GCM',iv,additionalData:enc.encode(id)},key,enc.encode(JSON.stringify(state)));
  return {format:1,salt,iv:b64(iv),cipher:b64(new Uint8Array(ciphertext))};
}
export async function unseal(e,key,id){
  if(!e||e.format!==1||typeof e.cipher!=='string'||e.cipher.length>1000000||bytes(e.iv).length!==12||bytes(e.salt).length!==16)throw Error('Invalid encrypted save.');
  const plaintext=await crypto.subtle.decrypt({name:'AES-GCM',iv:bytes(e.iv),additionalData:enc.encode(id)},key,bytes(e.cipher));
  return validate(JSON.parse(dec.decode(plaintext)));
}
const timeout=(promise,ms=10000)=>{let timer;return Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('Cloud connection timed out.')),ms);})]).finally(()=>clearTimeout(timer));};
let firebasePromise;
export function connectFirebase(){
  if(!firebasePromise)firebasePromise=(async()=>{
    const [apps,authSDK,f,{firebaseConfig}]=await Promise.all([
      import('https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js'),
      import('https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js'),
      import('https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js'),
      import('../../lobby/firebase-config.mjs')
    ]);
    const app=apps.getApps().find(a=>a.options.projectId===firebaseConfig.projectId)||apps.initializeApp(firebaseConfig);
    const auth=authSDK.getAuth(app);await auth.authStateReady();if(!auth.currentUser)await authSDK.signInAnonymously(auth);
    const db=f.getFirestore(app),ref=id=>f.doc(db,...ROOT,'saves',id);
    return {
      context:{db,f,auth},
      async read(id){const d=await f.getDocFromServer(ref(id));return d.exists()?d.data():null;},
      async commit(id,base,envelope,history,sales=[]){
        const operations=await Promise.all(sales.map(async row=>({id:await identity(`${id}:${row.operationId}`,'sale'),fingerprint:await identity(JSON.stringify(row),'receipt')})));
        return f.runTransaction(db,async tx=>{
          const doc=await tx.get(ref(id)),old=doc.exists()?doc.data():null;
          if((old?.revision||0)!==base||old&&old.salt!==envelope.salt){const error=Error('Another device changed this save. Your device copy was kept. Export it or load the cloud copy.');error.code='save-conflict';throw error;}
          const saleIds=operations.map(row=>row.id);
          if((old?.saleIds||[]).some(key=>!saleIds.includes(key))){const error=Error('This checkpoint predates a permanent monkey sale. Load the current cloud copy to preserve the sale and its resources.');error.code='save-conflict';throw error;}
          const fresh=operations.filter(row=>!(old?.saleIds||[]).includes(row.id));
          // Read every receipt before any write. The encrypted removal, reserve
          // payout and immutable receipt commit at the same cloud revision.
          const receipts=await Promise.all(fresh.map(row=>tx.get(f.doc(db,...ROOT,'sale_operations',row.id))));
          for(let i=0;i<fresh.length;i++)if(receipts[i].exists()){const error=Error('This monkey sale is already recorded. Load the current cloud checkpoint before continuing.');error.code='save-conflict';throw error;}
          const published=new Set(old?.scoreIds||[]);
          for(const h of history){
            if(h.mode!=='gps'||published.has(h.id))continue;
            tx.set(f.doc(db,...ROOT,'scores',`${id}_${h.id}`),{...h,verified:false,publishedAt:f.serverTimestamp()});published.add(h.id);
          }
          for(const row of fresh)tx.set(f.doc(db,...ROOT,'sale_operations',row.id),{saveId:id,fingerprint:row.fingerprint,createdAt:f.serverTimestamp()});
          tx.set(ref(id),{...envelope,revision:base+1,scoreIds:[...published].slice(-50),saleIds,updatedAt:f.serverTimestamp()});
          return base+1;
        },{maxAttempts:3});
      },
      async leaderboard(){const data=await f.getDocs(f.query(f.collection(db,...ROOT,'scores'),f.orderBy('score','desc'),f.limit(25)));return data.docs.map(d=>d.data());}
    };
  })().catch(e=>{firebasePromise=null;throw e;});
  return firebasePromise;
}
export async function socialFirebaseContext(){return (await connectFirebase()).context;}
function defaultStorage(){try{return globalThis.localStorage;}catch{return null;}}
function parse(raw){try{const d=JSON.parse(raw);return d&&d.format===1&&typeof d.salt==='string'&&typeof d.cipher==='string'?d:null;}catch{return null;}}
export class Vault {
  constructor(notify=()=>{},storage=defaultStorage(),connect=connectFirebase){
    this.notify=notify;this.storage=storage;this.connect=connect;this.seq=0;this.base=0;this.dirty=false;this.conflict=false;this.lastAttempt=0;this.localOK=false;this.force=false;this.pending=false;this.localTask=null;this.inflight=null;
  }
  status(text,kind='local'){this.notify({text,kind});}
  get(key){try{return this.storage?.getItem(key)||null;}catch{return null;}}
  put(envelope,dirty=this.dirty){
    const data={...envelope,baseRevision:this.base,dirty,seq:this.seq};
    try{
      if(!this.storage)throw Error('Browser storage is unavailable.');
      const old=this.get(PREFIX+this.id);if(parse(old))try{this.storage.setItem(PREFIX+this.id+'.backup',old);}catch{}
      const raw=JSON.stringify(data);this.storage.setItem(PREFIX+this.id,raw);
      if(this.get(PREFIX+this.id)!==raw)throw Error('Browser storage did not retain the checkpoint.');
      this.localOK=true;
    }catch(e){this.localOK=false;this.status(`Device save failed: ${e.message} Keep this page open and export a backup.`,'error');}
  }
  async open(name,password,{create=false,species='capuchin',mode='gps'}={}){
    name=String(name).trim();if(!/^[A-Za-z0-9 _-]{3,24}$/.test(name))throw Error('Use a creature name of 3–24 letters, numbers, spaces, dashes, or underscores.');
    if(typeof password!=='string'||password.length<(create?10:1)||password.length>200)throw Error('Use a password of at least 10 characters (maximum 200).');
    this.id=await identity(name,mode);this.mode=mode;
    const local=parse(this.get(PREFIX+this.id))||parse(this.get(PREFIX+this.id+'.backup'));
    let remote=null,cloudError=null;
    if(mode==='gps')try{this.adapter=await timeout(this.connect());remote=await timeout(this.adapter.read(this.id));}catch(e){cloudError=e;}
    if(create&&(local||remote))throw Error('That creature already has a save. Choose Reopen, or a different creature name.');
    if(create){
      this.salt=b64(crypto.getRandomValues(new Uint8Array(16)));this.key=await derive(password,this.salt);this.base=0;
      const seed=crypto.getRandomValues(new Uint32Array(1))[0];this.snapshot=newSave(name,species,mode,seed);
      await this.checkpoint(this.snapshot,true);return structuredClone(this.snapshot);
    }
    const candidates=[];
    for(const [source,e]of [['device',local],['cloud',remote]])if(e)try{
      const key=await derive(password,e.salt),state=await unseal(e,key,this.id);
      if(state.mode!==mode||normalize(state.name)!==normalize(name))throw Error('Save identity mismatch.');
      candidates.push({source,e,key,state});
    }catch{/* Try the independently encrypted copy before reporting failure. */}
    if(!candidates.length)throw Error(local||remote?'The password did not unlock a valid save. Check the creature name and password.':cloudError?'Cloud unavailable and no device copy found. Try again online; nothing was overwritten.':'No save found for that creature name.');
    const device=candidates.find(x=>x.source==='device'),cloud=candidates.find(x=>x.source==='cloud');
    // Dirty device progress is never silently replaced by a conflicting cloud checkpoint.
    const chosen=device&&(device.e.dirty||!cloud)?device:cloud||device;
    this.salt=chosen.e.salt;this.key=chosen.key;this.snapshot=chosen.state;this.seq=Number(chosen.e.seq)||0;
    this.base=chosen.source==='cloud'?remote.revision:Number(chosen.e.baseRevision)||0;
    this.dirty=chosen.source==='device'&&!!chosen.e.dirty;this.envelope={format:1,salt:this.salt,iv:chosen.e.iv,cipher:chosen.e.cipher};
    this.conflict=!!(device&&device.e.dirty&&remote&&(remote.revision!==this.base||remote.salt!==this.salt));
    if(remote&&!cloud&&device)this.conflict=true;
    this.put(this.envelope);
    if(this.conflict)this.status('Cloud conflict. Device progress kept; export a backup or load the cloud copy.','conflict');
    else if(cloudError)this.status('Device save opened · cloud unavailable; retry when online.','offline');
    else this.status(this.dirty?'Device save opened · cloud sync pending.':mode==='practice'?'Practice save opened · device only.':'Cloud save opened.','saved');
    return structuredClone(this.snapshot);
  }
  checkpoint(state,urgent=false){
    this.snapshot=structuredClone(validate(state));this.seq++;this.dirty=true;this.pending=true;this.force=this.force||urgent;
    if(!this.localTask)this.localTask=(async()=>{
      while(this.pending){
        this.pending=false;const seq=this.seq,snapshot=this.snapshot;
        try{const e=await seal(snapshot,this.key,this.salt,this.id);if(seq!==this.seq)continue;this.envelope=e;this.put(e,true);}
        catch(e){this.status(`Unable to encrypt checkpoint: ${e.message}`,'error');}
      }
    })().finally(()=>{
      this.localTask=null;
      if(this.mode==='practice'){if(this.localOK)this.status('Practice saved on this device · not ranked.','local');return;}
      if(!this.conflict&&this.localOK)this.status('Device saved · cloud sync pending.','local');
      if(this.force||Date.now()-this.lastAttempt>20000)void this.sync();
    });
    return this.localTask;
  }
  async sync(){
    if(this.localTask)await this.localTask;
    if(this.mode==='practice'||!this.dirty||this.conflict||!this.envelope)return;
    if(this.inflight)return this.inflight;
    const seq=this.seq,snapshot=this.snapshot,envelope=this.envelope,base=this.base;
    this.lastAttempt=Date.now();this.force=false;this.status(this.localOK?'Device saved · syncing cloud…':'Syncing cloud · no device backup.','syncing');
    this.inflight=(async()=>{
      let warning;
      try{
        this.adapter=this.adapter||await timeout(this.connect());
        // Do not race a transaction against a timeout and then issue an overlapping write.
        warning=setTimeout(()=>this.status('Cloud sync is delayed. Device checkpoint remains available.','offline'),12000);
        this.base=await this.adapter.commit(this.id,base,envelope,snapshot.meta.history,Object.values(snapshot.meta.monkeySales?.receipts||{}));
        this.dirty=this.seq!==seq;
        this.put(this.envelope,this.dirty);
        this.status(this.dirty?'Device saved · newer changes awaiting sync.':this.localOK?'Saved on this device and in Firestore.':'Cloud saved · device storage unavailable.',this.dirty?'local':'saved');
      }catch(e){
        if(e.code==='save-conflict'){this.conflict=true;this.status(e.message,'conflict');}
        else this.status(`${this.localOK?'Device saved':'Not saved locally'} · cloud unavailable (${e.code||e.message}). Retry sync when online.`,'offline');
      }finally{clearTimeout(warning);this.inflight=null;if(this.force&&!this.conflict)queueMicrotask(()=>void this.sync());}
    })();
    return this.inflight;
  }
  async loadCloud(){
    if(this.inflight)throw Error('A cloud write is still pending. Retry after it finishes.');
    if(this.localTask)await this.localTask;
    this.adapter=this.adapter||await timeout(this.connect());const e=await timeout(this.adapter.read(this.id));
    if(!e||e.salt!==this.salt)throw Error('Cloud identity changed. Export your device copy, then reopen with the correct password.');
    const state=await unseal(e,this.key,this.id);
    try{const old=this.get(PREFIX+this.id);if(old)this.storage.setItem(PREFIX+this.id+'.recovery',old);}catch{throw Error('Cannot keep a recovery copy. Export a backup before reopening from another device.');}
    this.snapshot=state;this.envelope={format:1,salt:e.salt,iv:e.iv,cipher:e.cipher};this.base=e.revision;this.seq++;this.dirty=false;this.conflict=false;this.force=false;
    this.put(this.envelope,false);this.status('Cloud checkpoint loaded; previous device copy retained.','saved');return structuredClone(state);
  }
  async export(){if(this.localTask)await this.localTask;if(!this.envelope)throw Error('No encrypted checkpoint is available.');return JSON.stringify({game:'monkey-trails',id:this.id,...this.envelope,baseRevision:this.base,dirty:this.dirty},null,2);}
  // A pinned GPS target is encrypted on this device only. It is never part of
  // snapshot/envelope/cloud transactions, portable exports, or route history.
  async saveDeviceData(label,data){
    if(!this.key||!this.id||label!=='radar')return;
    const key=PREFIX+this.id+'.private.'+label;
    if(data===null){try{this.storage?.removeItem(key);}catch{}return;}
    const raw=JSON.stringify(data);if(raw.length>4096)throw Error('Device map data is too large.');
    const iv=crypto.getRandomValues(new Uint8Array(12));
    const cipher=await crypto.subtle.encrypt({name:'AES-GCM',iv,additionalData:enc.encode(this.id+':device:'+label)},this.key,enc.encode(raw));
    try{this.storage?.setItem(key,JSON.stringify({iv:b64(iv),cipher:b64(new Uint8Array(cipher))}));}catch{/* Tracking still works in memory. */}
  }
  async loadDeviceData(label){
    if(!this.key||!this.id||label!=='radar')return null;
    try{const e=JSON.parse(this.get(PREFIX+this.id+'.private.'+label));if(!e||e.cipher.length>8192)return null;
      const raw=await crypto.subtle.decrypt({name:'AES-GCM',iv:bytes(e.iv),additionalData:enc.encode(this.id+':device:'+label)},this.key,bytes(e.cipher));return JSON.parse(dec.decode(raw));
    }catch{return null;}
  }
  async leaderboard(){this.adapter=this.adapter||await timeout(this.connect());return timeout(this.adapter.leaderboard());}
}
// Restore a portable backup without granting it authority to overwrite a newer cloud revision.
export async function importBackup(name,password,text,storage=defaultStorage()){
  if(typeof text!=='string'||text.length>1100000)throw Error('Backup file is too large.');
  let e;try{e=JSON.parse(text);}catch{throw Error('This is not a readable JSON backup.');}
  if(e.game!=='monkey-trails'||e.format!==1||!Number.isInteger(e.baseRevision)||e.baseRevision<0)throw Error('Unsupported backup format.');
  const key=await derive(password,e.salt),s=await unseal(e,key,e.id),id=await identity(name,s.mode);
  if(id!==e.id||normalize(s.name)!==normalize(name))throw Error('The backup does not match that creature name.');
  if(!storage)throw Error('Browser storage is unavailable.');
  const old=storage.getItem(PREFIX+id);if(old)storage.setItem(PREFIX+id+'.recovery',old);
  storage.setItem(PREFIX+id,JSON.stringify({format:1,salt:e.salt,iv:e.iv,cipher:e.cipher,baseRevision:e.baseRevision,dirty:true}));
  return s.mode;
}
