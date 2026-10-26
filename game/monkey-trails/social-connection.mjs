import {connectFirebase} from './storage.mjs';
import {createSocialContextRouter,openNamedSocialContext} from './social-identity.mjs';

let namedSDKPromise;
function namedSDK(){
 if(!namedSDKPromise)namedSDKPromise=Promise.all([
  import('https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js'),
  import('https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js')
 ]).then(([apps,authSDK])=>({apps,authSDK})).catch(error=>{namedSDKPromise=null;throw error;});
 return namedSDKPromise;
}
export const socialFirebaseContext=createSocialContextRouter({
 connectDefault:async()=>(await connectFirebase()).context,
 async readPlayer({db,f},uid){const player=await f.getDocFromServer(f.doc(db,'monkeyTrailsSocial','v1','players',uid));return player.exists()?player.data():null;},
 async restoreNamed(name,context){return openNamedSocialContext(name,context,{...await namedSDK(),create:false});},
 async connectNamed(name,context){return openNamedSocialContext(name,context,await namedSDK());}
});
