import {createHash,randomBytes,randomInt,timingSafeEqual} from 'node:crypto';

const alphabet='23456789ABCDEFGHJKLMNPQRSTUVWXYZ';
const lifetime=5*60_000;
const pollIntervalMs=3000;
const fail=(status,message)=>Object.assign(new Error(message),{status});
const unavailable=()=>fail(410,'This desktop sign-in expired or has already been used. Start again from the launcher.');
const invalid=()=>fail(403,'The desktop sign-in could not be verified.');
const digest=value=>createHash('sha256').update(value,'ascii').digest();
const b64u=/^[A-Za-z0-9_-]{43}$/;
export function createDesktopAuthorization({mint,now=Date.now,site='https://www.catnmice.com'}) {
  const requests=new Map(),starters=new Map();
  function sweep(){
    const time=now();
    for(const [id,row] of requests)if(time>=row.until)requests.delete(id);
    for(const [client,row] of starters)if(time>=row.until)starters.delete(client);
  }
  function start(challenge,client){
    if(typeof challenge!=='string'||!b64u.test(challenge)||Buffer.from(challenge,'base64url').toString('base64url')!==challenge)throw fail(400,'The launcher supplied an invalid sign-in challenge.');
    sweep();
    const bucket=starters.get(client)??{n:0,until:now()+60000};
    if(requests.size>=2048||starters.size>=4096&&!starters.has(client)||++bucket.n>8)throw fail(429,'Too many desktop sign-in requests. Try again later.');
    starters.set(client,bucket);
    const id=randomBytes(24).toString('base64url'),digits=Array.from({length:8},()=>alphabet[randomInt(alphabet.length)]).join('');
    const code=digits.slice(0,4)+'-'+digits.slice(4),until=now()+lifetime;
    requests.set(id,{until,challenge:Buffer.from(challenge,'base64url'),code:digest(digits),attempts:0,nextPoll:0,approved:null});
    return {id,code,expiresAt:new Date(until).toISOString(),pollIntervalMs,verificationUrl:site+'/hub/authorize-desktop.html#request='+id};
  }
  function find(id){
    const row=typeof id==='string'&&id.length===32&&requests.get(id);
    if(!row||now()>=row.until){if(row)requests.delete(id);throw unavailable();}
    return row;
  }
  function checkCode(id,code){
    const row=find(id);
    if(row.approved)throw unavailable();
    const normalized=typeof code==='string'&&code.length<=12?code.replace(/[- ]/g,'').toUpperCase():'';
    if(!timingSafeEqual(digest(normalized),row.code)){
      if(++row.attempts>=5)requests.delete(id);
      throw fail(403,'That code does not match the launcher.');
    }
    return row;
  }
  function approve(id,code,identity){
    const row=checkCode(id,code);
    if(!identity?.uid||!identity.username)throw invalid();
    row.approved={uid:identity.uid,username:identity.username,approvedAt:now()};
    return {approved:true,username:identity.username};
  }
  function prove(id,verifier){
    const row=find(id);
    if(typeof verifier!=='string'||!b64u.test(verifier)||!timingSafeEqual(digest(verifier),row.challenge))throw invalid();
    return row;
  }
  async function redeem(id,verifier){
    const row=prove(id,verifier);
    if(!row.approved){
      if(now()<row.nextPoll)throw fail(429,'Wait a moment before checking again.');
      row.nextPoll=now()+pollIntervalMs;
      return {state:'pending',expiresAt:new Date(row.until).toISOString()};
    }
    // Consume before the asynchronous signing operation. A second request,
    // including a concurrent one, cannot obtain another credential.
    requests.delete(id);
    const customToken=await mint(row.approved);
    if(now()>=row.until)throw unavailable();
    return {state:'approved',username:row.approved.username,customToken};
  }
  function cancel(id,verifier){prove(id,verifier);requests.delete(id);return {cancelled:true};}
  function deny(id,code){checkCode(id,code);requests.delete(id);return {cancelled:true};}
  return {start,approve,redeem,cancel,deny};
}

// Use the privileged SDK only on the service. Recheck both binding directions
// and global revocation before issuing a custom token for the approved UID.
export function createDesktopTokenIssuer({auth,db}) {
  return async ({uid,username,approvedAt})=>{
    const [user,binding,name,profile]=await Promise.all([auth.getUser(uid),db.collection('factionForgeAccounts').doc(uid).get(),
      db.collection('factionForgeNames').doc(username).get(),db.collection('users').doc(username).get()]);
    if(user.disabled||user.uid!==uid||Date.parse(user.tokensValidAfterTime)>approvedAt||!profile.exists||
      binding.data()?.verified!==true||binding.data()?.username!==username||name.data()?.verified!==true||name.data()?.uid!==uid)throw invalid();
    return auth.createCustomToken(uid);
  };
}
