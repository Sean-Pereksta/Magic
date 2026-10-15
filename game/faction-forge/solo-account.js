import {newSoloProfile} from './firebase-progress.js';
import {ProgressionError} from './progression.js';

const cleanName=value=>typeof value==='string'?value.trim().slice(0,80):'';
const placeholder=name=>!name||/^(Commander|Guest)$/i.test(name);

/** A solo launch belongs to the currently selected hub account, even from an old link. */
export function resolveForgeUsername({params,storage,solo=true}={}){
  let stored='',linked='';
  try{if(storage===undefined)storage=globalThis.localStorage;}catch{}
  try{stored=cleanName(storage?.getItem('gh.username'));}catch{}
  try{linked=cleanName(params?.get('username'));}catch{}
  return (solo?stored||linked:linked||stored)||'Guest';
}

/** Keep the original save and receipts in place, separating additional hub names by UID. */
export async function selectSoloAccount({auth,db,api,username,troops}){
  const uid=auth.currentUser?.uid,name=cleanName(username)||'Guest';
  if(!uid)throw new ProgressionError('UNAUTHENTICATED','Sign in to save Faction Forge progress.');
  const assertIdentity=()=>{
    if(auth.currentUser?.uid!==uid)throw new ProgressionError('ACCOUNT_CHANGED','Your account changed. Reopen the game to load its progress.');
  };
  const rootPath=['factionForgeSoloProfiles',uid],rootRef=api.doc(db,...rootPath);
  assertIdentity();
  const selection=await api.runTransaction(db,async tx=>{
    assertIdentity();
    const root=await tx.get(rootRef);
    assertIdentity();
    if(!root.exists()){
      tx.set(rootRef,newSoloProfile(uid,name,troops));
      return {profilePath:rootPath,queueNamespace:'',username:name};
    }
    const profile=root.data(),rootName=cleanName(profile.username);
    if(rootName===name)return {profilePath:rootPath,queueNamespace:'',username:name};
    if(placeholder(rootName)&&!placeholder(name)){
      // Claim an unnamed legacy save once, without moving its currency or receipts.
      assertIdentity();
      tx.set(rootRef,{...profile,username:name});
      return {profilePath:rootPath,queueNamespace:'',username:name};
    }
    // The prefix also makes '.', '..', and reserved '__name__' hub names valid IDs.
    const key='name-'+encodeURIComponent(name),profilePath=[...rootPath,'accounts',key];
    const ref=api.doc(db,...profilePath),saved=await tx.get(ref);
    assertIdentity();
    if(!saved.exists())tx.set(ref,newSoloProfile(uid,name,troops));
    return {profilePath,queueNamespace:'.'+key,username:name};
  });
  assertIdentity();
  return selection;
}
