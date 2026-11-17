// Call gameplayStarted only after the game confirms play. Keep unacknowledged
// activity for the identity that played, independent of the current sign-in.
export function createActivityClient({storage=localStorage,origin='https://hub.catnmice.com',fetchImpl=fetch,getAccount,guestProfile='default',withLock=task=>navigator.locks.request('catnmice.hub.sync',task)}) {
  const guestKey='catnmice.hub.guest.'+guestProfile;
  const pendingKey='catnmice.hub.pending.v1';
  const countKey='catnmice.hub.counts.v1';
  const read=(key,initial)=>JSON.parse(storage.getItem(key)||JSON.stringify(initial));
  const write=(key,value)=>storage.setItem(key,JSON.stringify(value));
  function guest() {
    let value=storage.getItem(guestKey);
    if(!value){
      const random=crypto.getRandomValues(new Uint8Array(32));
      value=btoa(String.fromCharCode(...random)).replaceAll('+','-').replaceAll('/','_').replaceAll('=','');
      storage.setItem(guestKey,value);
    }
    return value;
  }
  async function request(path,body,auth) {
    const res=await fetchImpl(new URL(path,origin),{method:'POST',headers:{'Content-Type':'application/json',Authorization:auth},body:JSON.stringify(body),credentials:'omit',redirect:'error',signal:AbortSignal.timeout(8000)});
    if(!res.ok)throw new Error('Activity service returned '+res.status);
    return res.json();
  }
  async function gameplayStarted(game,owner) {
    await withLock(()=>{const pending=read(pendingKey,{}),actor=owner?'a:'+owner:'g:'+guest();
      pending[actor]=[...new Set([...(pending[actor]||[]),game])];write(pendingKey,pending);});
  }
  async function sync() {
    const account=await getAccount?.().catch(()=>null);
    const batches=await withLock(()=>Object.entries(read(pendingKey,{})));
    for(const [actor,games] of batches) {
      const accountOwned=actor.startsWith('a:');
      if(accountOwned&&account?.username!==actor.slice(2))continue;
      const auth=accountOwned?'Bearer '+await account.token():'Guest '+actor.slice(2);
      await request('/v1/activity',{games,username:accountOwned?account.username:undefined},auth);
      await withLock(()=>{const pending=read(pendingKey,{});pending[actor]=(pending[actor]||[]).filter(game=>!games.includes(game));if(!pending[actor].length)delete pending[actor];write(pendingKey,pending);});
    }
  }
  async function linkCurrentGuest() {
    const account=await getAccount();
    if(!account)throw new Error('Sign in to link this guest.');
    const credential=await withLock(()=>guest());
    await request('/v1/link',{guest:credential,username:account.username},'Bearer '+await account.token());
    // Future anonymous play starts from a new guest profile on shared devices.
    await withLock(()=>{if(storage.getItem(guestKey)===credential)storage.removeItem(guestKey);});
  }
  async function refreshCounts() {
    const res=await fetchImpl(new URL('/v1/counts',origin),{signal:AbortSignal.timeout(6000),credentials:'omit',redirect:'error',cache:'no-store'});
    if(!res.ok)throw new Error('Lifetime counts are unavailable.');
    const counts=await res.json();if(!Number.isFinite(Date.parse(counts.asOf))||!counts.hub||!counts.games)throw new Error('Invalid count response.');
    write(countKey,counts);return counts;
  }
  return {gameplayStarted,sync,linkCurrentGuest,refreshCounts,lastCounts:()=>read(countKey,null)};
}
