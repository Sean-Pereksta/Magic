import {newSoloProfile} from './firebase-progress.js';
import {ProgressionError} from './progression.js';
import {FORGE_PACKS} from './packs.js';

const cleanName=value=>typeof value==='string'?value.trim().slice(0,80):'';
const placeholder=name=>!name||/^(Commander|Guest)$/i.test(name);
const validId=value=>typeof value==='string'&&/^[A-Za-z0-9_-]{1,128}$/.test(value);
function queuedReceipts(storage,key,troops){
  const records=new Map(),add=(kind,id)=>{if(validId(id))records.set(kind+'/'+id,{kind,id});};
  try{for(const result of JSON.parse(storage?.getItem(key)||'[]'))add('results',result.id);}catch{}
  try{for(const result of JSON.parse(storage?.getItem(key+'.rounds')||'[]'))if(Number.isInteger(result.round))add('rounds',result.id+'-round-'+result.round);}catch{}
  for(const troop of Object.keys(troops))try{add('upgrades',storage?.getItem(key+'.upgrade.'+troop));}catch{}
  for(const pack of FORGE_PACKS)try{add('purchases',storage?.getItem(key+'.pack.'+pack.id));}catch{}
  return [...records.values()];
}

/** A solo launch belongs to the currently selected hub account, even from an old link. */
export function resolveForgeUsername({params,storage}={}){
  let stored='',linked='';
  try{if(storage===undefined)storage=globalThis.localStorage;}catch{}
  try{stored=cleanName(storage?.getItem('gh.username'));}catch{}
  try{linked=cleanName(params?.get('username'));}catch{}
  return stored||linked||'Guest';
}

/** Hub profiles own durable saves. Firebase Auth only supplies the connection. */
export async function selectSoloAccount({auth,db,api,username,troops,storage,useHubProfile=true}={}){
  try{if(storage===undefined)storage=globalThis.localStorage;}catch{}
  const uid=auth.currentUser?.uid,name=cleanName(username)||'Guest';
  if(!uid)throw new ProgressionError('UNAUTHENTICATED','Sign in to save Faction Forge progress.');
  if(name.includes('/'))throw new ProgressionError('INVALID_PROFILE','Choose a valid Game Hub profile.');
  const assertIdentity=()=>{
    if(auth.currentUser?.uid!==uid)throw new ProgressionError('ACCOUNT_CHANGED','Your account changed. Reopen the game to load its progress.');
  };
  const rootPath=['factionForgeSoloProfiles',uid],rootRef=api.doc(db,...rootPath);
  const key='name-'+encodeURIComponent(name),scopedPath=[...rootPath,'accounts',key];
  const profilePath=['users',name,'factionForge','profile'];
  const queueKey='factionForge.solo.pending.hub.'+encodeURIComponent(name);
  const rootQueue='factionForge.solo.pending.'+uid,scopedQueue=rootQueue+'.'+key;
  const pendingLegacy=queuedReceipts(storage,rootQueue,troops).length||queuedReceipts(storage,scopedQueue,troops).length;
  const optionalLegacyRead=async(ref,tx)=>{
    try{return await (api.getDoc?api.getDoc(ref):tx.get(ref));}
    catch(error){if(error.code==='permission-denied')return {exists:()=>false};throw error;}
  };
  assertIdentity();
  const selection=await api.runTransaction(db,async tx=>{
    assertIdentity();
    // Finish every read before writing, as required by Firestore transactions.
    const hub=useHubProfile?await tx.get(api.doc(db,'users',name)):{exists:()=>false};
    const saved=hub.exists()?await tx.get(api.doc(db,...profilePath)):null;
    if(saved?.exists()&&!pendingLegacy)return {profilePath,queueNamespace:'',queueKey,legacyProfilePaths:[],legacyQueueKeys:[],username:name};
    const root=await tx.get(rootRef);
    // Older deployed rules do not expose scoped saves. They are an optional
    // recovery source; a denied legacy read must never block the hub save.
    const scoped=await optionalLegacyRead(api.doc(db,...scopedPath),tx);
    assertIdentity();
    const profile=root.exists()?root.data():null,rootName=cleanName(profile?.username);
    if(hub.exists()){
      const rootMatches=root.exists()&&(rootName===name||placeholder(rootName)&&!placeholder(name));
      const scopedMatches=scoped.exists()&&cleanName(scoped.data().username)===name;
      const legacyProfilePaths=scopedMatches?[scopedPath]:rootMatches?[rootPath]:[];
      const source=scopedMatches?scoped.data():rootMatches?profile:null;
      const legacyQueueKeys=legacyProfilePaths.map(path=>'factionForge.solo.pending.'+uid+(path.length===4?'.'+key:''));
      const receipts=[];
      for(let index=0;index<legacyProfilePaths.length;index++)for(const {kind,id} of queuedReceipts(storage,legacyQueueKeys[index],troops)){
        const destination=api.doc(db,...profilePath,kind,id),existing=await tx.get(destination);
        if(!existing.exists()){
          const old=await optionalLegacyRead(api.doc(db,...legacyProfilePaths[index],kind,id),tx);
          if(old.exists())receipts.push({destination,data:{...old.data(),uid}});
        }
      }
      assertIdentity();
      if(!saved.exists()){
        if(rootMatches&&rootName!==name)tx.set(rootRef,{...profile,username:name});
        // The original UID save and immutable receipts stay available for recovery.
        tx.set(api.doc(db,...profilePath),source?{...source,uid,username:name}:newSoloProfile(uid,name,troops));
      }
      // Retained purchase/upgrade IDs must also survive a subsequent Auth change.
      if(receipts.length&&saved.exists())tx.set(api.doc(db,...profilePath),{...saved.data(),uid});
      for(const receipt of receipts)tx.set(receipt.destination,receipt.data);
      return {profilePath,queueNamespace:'',queueKey,
        legacyProfilePaths,legacyQueueKeys,username:name};
    }
    // A browser without a Game Hub account keeps its private guest save.
    if(!root.exists()){
      tx.set(rootRef,newSoloProfile(uid,name,troops));
      return {profilePath:rootPath,queueNamespace:'',username:name};
    }
    if(rootName===name)return {profilePath:rootPath,queueNamespace:'',username:name};
    if(placeholder(rootName)&&!placeholder(name)){
      // Claim an unnamed legacy save once, without moving its currency or receipts.
      assertIdentity();
      tx.set(rootRef,{...profile,username:name});
      return {profilePath:rootPath,queueNamespace:'',username:name};
    }
    // The prefix also makes '.', '..', and reserved '__name__' hub names valid IDs.
    if(!scoped.exists())tx.set(api.doc(db,...scopedPath),newSoloProfile(uid,name,troops));
    return {profilePath:scopedPath,queueNamespace:'.'+key,username:name};
  });
  assertIdentity();
  return selection;
}
