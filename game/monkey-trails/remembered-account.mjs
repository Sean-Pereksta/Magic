// Browser-local quick reopen. IndexedDB can clone a non-exportable CryptoKey;
// neither a password nor an unencrypted game checkpoint is stored here.
const DATABASE='monkey-trails.accounts.v1',STORE='accounts',LAST='last-account';
function indexedDBFactory(){try{return globalThis.indexedDB;}catch{return null;}}
export function validRememberedAccount(row){
  return !!(row&&row.format===1&&row.mode==='gps'&&/^[a-f0-9]{64}$/.test(row.id)&&
    typeof row.name==='string'&&/^[A-Za-z0-9 _-]{3,24}$/.test(row.name)&&typeof row.salt==='string'&&
    row.key?.type==='secret'&&row.key.extractable===false&&row.key.algorithm?.name==='AES-GCM'&&
    row.key.algorithm.length===256&&row.key.usages?.includes('encrypt')&&row.key.usages.includes('decrypt'));
}
export class IndexedDBAccountStore {
  constructor(factory=indexedDBFactory()){this.factory=factory;}
  open(){
    if(!this.factory)return Promise.reject(Error('This browser cannot remember accounts.'));
    return new Promise((resolve,reject)=>{
      const request=this.factory.open(DATABASE,1);let finished=false;
      const timer=setTimeout(()=>fail(Error('Remembered account storage did not respond.')),5000);
      const fail=error=>{if(!finished){finished=true;clearTimeout(timer);reject(error);}};
      request.onupgradeneeded=()=>{if(!request.result.objectStoreNames.contains(STORE))request.result.createObjectStore(STORE);};
      request.onerror=()=>fail(request.error||Error('Remembered account storage is unavailable.'));
      request.onblocked=()=>fail(Error('Close other Monkey Trails tabs to update remembered account storage.'));
      request.onsuccess=()=>{if(finished){request.result.close();return;}finished=true;clearTimeout(timer);request.result.onversionchange=()=>request.result.close();resolve(request.result);};
    });
  }
  async run(mode,operation){
    const db=await this.open();
    try{return await new Promise((resolve,reject)=>{
      const tx=db.transaction(STORE,mode);let request;
      tx.oncomplete=()=>resolve(request?.result);
      tx.onerror=()=>reject(tx.error||request?.error||Error('Remembered account could not be saved.'));
      tx.onabort=()=>reject(tx.error||Error('Remembered account storage was interrupted.'));
      try{request=operation(tx.objectStore(STORE));}catch(error){tx.abort();reject(error);}
    });}finally{db.close();}
  }
  read(){return this.run('readonly',store=>store.get(LAST));}
  write(row){return this.run('readwrite',store=>store.put(row,LAST));}
  clear(){return this.run('readwrite',store=>store.delete(LAST));}
}
export class RememberedAccount {
  constructor(store=new IndexedDBAccountStore()){this.store=store;}
  async load(){const row=await this.store.read();return validRememberedAccount(row)?row:null;}
  async remember(vault){
    if(!vault.localOK||!vault.envelope)throw Error('A device checkpoint is needed before remembering this account.');
    const row={format:1,id:vault.id,name:vault.snapshot?.name,mode:vault.mode,salt:vault.salt,key:vault.key};
    if(!validRememberedAccount(row))throw Error('This account cannot be remembered on this device.');
    await this.store.write(row);
    const saved=await this.load();
    if(!saved||saved.id!==row.id||saved.salt!==row.salt)throw Error('This browser did not retain the remembered account.');
    return saved;
  }
  forget(){return this.store.clear();}
}
