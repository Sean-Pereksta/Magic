import {authorized,getOfficialAuth,official} from './desktop-account.mjs';
let used=false;
if(official())Object.defineProperty(window,'catnmiceCompleteDesktopSignIn',{value:async input=>{
  if(used||typeof input?.username!=='string'||!input.username||typeof input.customToken!=='string'||input.customToken.length>8192)throw new Error('Invalid desktop sign-in.');
  used=true;const expected=input.username;let switched=false,auth,api;
  try{
    ({auth,api}=await getOfficialAuth());
    const result=await api.signInWithCustomToken(auth,input.customToken);switched=true;input.customToken='';
    localStorage.removeItem('gh.username');
    const profile=await authorized('profile',result.user);
    if(profile.username!==expected||auth.currentUser?.uid!==result.user.uid)throw new Error('Account did not match.');
    localStorage.setItem('gh.username',profile.username);
    document.getElementById('status').textContent='Signed in as '+profile.username+'.';
    return {username:profile.username};
  }catch{
    if(switched){localStorage.removeItem('gh.username');await api.signOut(auth).catch(()=>{});}
    document.getElementById('status').textContent='Desktop sign-in did not finish. Start again from the launcher.';
    throw new Error('Desktop sign-in did not finish.');
  }finally{input.customToken='';}
}});
