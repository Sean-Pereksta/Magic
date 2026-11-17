import {createActivityClient} from './activity.mjs';
const official=['https://www.catnmice.com','https://catnmice.com'].includes(location.origin);
const username=()=>official?localStorage.getItem('gh.username')||null:null;
let authPromise;
async function account(){
  const name=username();if(!name)return null;
  authPromise??=Promise.all([import('https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js'),import('https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js'),import('/lobby/firebase-config.mjs')]).then(([apps,auth,config])=>auth.getAuth(apps.getApps().find(a=>a.name==='[DEFAULT]')||apps.initializeApp(config.firebaseConfig))).catch(e=>{authPromise=null;throw e;});
  const auth=await authPromise;await auth.authStateReady();
  if(!auth.currentUser||username()!==name)return null;
  return {username:name,token:()=>auth.currentUser.getIdToken()};
}
const activity=createActivityClient({getAccount:account});
export async function recordGameplay(game,fromSite) {
  await activity.gameplayStarted(game,fromSite?username():null);
  void activity.sync().catch(()=>{});
}
export const sync=()=>activity.sync();
export const linkGuest=()=>activity.linkCurrentGuest();
addEventListener('online',()=>void sync().catch(()=>{}));
void sync().catch(()=>{});
