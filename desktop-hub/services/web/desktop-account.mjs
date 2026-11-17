const service='https://hub.catnmice.com';
export const official=()=>location.origin==='https://www.catnmice.com'&&self===top;
export async function getOfficialAuth() {
  const [{getApps,initializeApp},api,{firebaseConfig}]=await Promise.all([import('https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js'),import('https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js'),import('/lobby/firebase-config.mjs')]);
  const app=getApps().find(a=>a.name==='[DEFAULT]')||initializeApp(firebaseConfig);
  if(app.options.projectId!=='bible-game-246c0')throw new Error('Open the official Catnmice website.');
  const auth=api.getAuth(app);await auth.authStateReady();return {auth,api};
}
export async function authorized(path,user,data) {
  const headers={Authorization:'Bearer '+await user.getIdToken()};
  if(data)headers['Content-Type']='application/json';
  const res=await fetch(service+'/v1/desktop/'+path,{method:data?'POST':'GET',headers,body:data?JSON.stringify(data):undefined,redirect:'error',credentials:'omit',cache:'no-store',signal:AbortSignal.timeout(10000)});
  const value=await res.json();
  if(!res.ok)throw new Error(value.error||'The authorization service is unavailable.');
  return value;
}
