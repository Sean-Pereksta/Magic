export function socialAppName(saveId){
 if(typeof saveId!=='string'||!/^[a-f0-9]{64}$/.test(saveId))throw Error('Open a valid saved trail before connecting multiplayer.');
 return `monkey-trails-${saveId}`;
}

// Keep the existing browser UID for the trail it already owns. Another saved
// trail gets its own persisted anonymous session instead of rebinding that UID.
export function createSocialContextRouter({connectDefault,readPlayer,connectNamed,restoreNamed}){
 const namedContexts=new Map(),restoredContexts=new Map();
 const cached=(cache,name,connect)=>{
  if(!cache.has(name))cache.set(name,Promise.resolve().then(connect).catch(error=>{cache.delete(name);throw error;}));
  return cache.get(name);
 };
 return async saveId=>{
  const name=socialAppName(saveId),context=await connectDefault();
  // A lobby sign-out may replace the shared default UID while this trail's
  // separate session remains intact. Restore that existing identity first.
  if(namedContexts.has(name)){
   const named=await namedContexts.get(name);if(named.auth.currentUser)return named;
   namedContexts.delete(name);
  }
  const restored=restoreNamed?await cached(restoredContexts,name,()=>restoreNamed(name,context)):null;
  if(restored?.auth.currentUser)return restored;
  const uid=context.auth.currentUser?.uid;
  if(!uid)throw Error('Sign in before connecting multiplayer.');
  const player=await readPlayer(context,uid);
  if(!player||player.saveId===saveId)return context;
  return cached(namedContexts,name,()=>connectNamed(name,context));
 };
}

// Firebase Auth persists browser sessions separately for each app name. Wait
// for restoration before considering a new anonymous sign-in on that app.
export async function openNamedSocialContext(name,context,{apps,authSDK,create=true}){
 const app=apps.getApps().find(app=>app.name===name)||apps.initializeApp(context.auth.app.options,name);
 const auth=authSDK.getAuth(app);await auth.authStateReady();
 if(!auth.currentUser){if(!create)return null;await authSDK.signInAnonymously(auth);}
 return{db:context.f.getFirestore(app),f:context.f,auth};
}
