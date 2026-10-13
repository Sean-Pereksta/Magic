/* Optional synchronization with the lobby's existing profile. Guest play stays local. */
import {firebaseConfig} from '../../lobby/firebase-config.mjs';

let username='';
try{username=(localStorage.getItem('gh.username')||'').trim()}catch(_){/* private browsing */}
const progress=globalThis.NeonSurvivalProgress?.store;
if(progress&&username&&username.length<=32&&!username.includes('/')){
  // Import Firebase only for someone with an existing lobby profile; never create an auth session.
  Promise.all([
    import('https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js'),
    import('https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js'),
    import('https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js')
  ]).then(([apps,authentication,firestore])=>{
    const app=apps.getApps().find(app=>app.options.projectId===firebaseConfig.projectId)||apps.initializeApp(firebaseConfig);
    const auth=authentication.getAuth(app),db=firestore.getFirestore(app),profile=firestore.doc(db,'users',username);
    authentication.onAuthStateChanged(auth,async user=>{
      progress.detachProfileAdapter();
      if(!user)return;
      try{
        const snapshot=await firestore.getDoc(profile),data=snapshot.exists()?snapshot.data():null;
        // Lobby links a profile to its authenticated session using chatAuthUid. Do not guess a user.
        if(!data||data.chatAuthUid!==user.uid||auth.currentUser?.uid!==user.uid)return;
        const uid=user.uid;
        await progress.attachProfileAdapter({
          read:async()=>data.neonSurvival||null,
          write:async state=>{
            if(auth.currentUser?.uid!==uid||localStorage.getItem('gh.username')!==username)return;
            // updateDoc preserves all profile fields and cannot recreate a deleted profile.
            await firestore.updateDoc(profile,{neonSurvival:state});
          }
        });
      }catch(_){/* An offline/denied cloud request never prevents local play. */}
    });
    globalThis.addEventListener('online',()=>progress.syncProfile());
  }).catch(()=>{/* SDK unavailable: versioned local storage remains the baseline. */});
}
