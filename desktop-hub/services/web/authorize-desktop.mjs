import {authorized,getOfficialAuth,official} from './desktop-account.mjs';
const requestId=new URLSearchParams(location.hash.slice(1)).get('request');
history.replaceState(null,'',location.pathname);
const el=id=>document.getElementById(id);
let account=null,finished=false;
async function check(){
  el('refresh').hidden=true;el('consent').hidden=true;el('status').textContent='';
  if(!official()||!requestId||!/^[-_A-Za-z0-9]{32}$/.test(requestId)){el('account').textContent='Start verified sign-in in your Catnmice Windows launcher.';return;}
  try{
    const {auth}=await getOfficialAuth(),username=localStorage.getItem('gh.username'),user=auth.currentUser;
    if(!username||!user)throw new Error('Sign in through the Catnmice library, then check your profile here.');
    const profile=await authorized('profile',user);
    if(profile.username!==username)throw new Error('Select the Catnmice account already verified in this browser.');
    account={username,user,auth};el('account').textContent='Account: '+username;el('consent').hidden=false;el('code').focus();
  }catch(e){account=null;el('account').textContent=e.message;el('refresh').hidden=false;}
}
async function decide(approve){
  if(!account||finished||!el('code').reportValidity())return;
  el('approve').disabled=true;el('deny').disabled=true;
  try{
    if(account.auth.currentUser?.uid!==account.user.uid||localStorage.getItem('gh.username')!==account.username)throw new Error('Your signed-in account changed. Reopen the link from your launcher.');
    await authorized(approve?'approve':'deny',account.user,{id:requestId,code:el('code').value.trim(),username:account.username,consent:approve});
    finished=true;el('code').value='';el('consent').hidden=true;
    el('status').textContent=approve?'Approved. Return to Catnmice Game Hub on Windows.':'Desktop sign-in cancelled.';
  }catch(e){el('status').textContent=e.message;}
  finally{el('approve').disabled=false;el('deny').disabled=false;}
}
el('refresh').onclick=check;
el('consent').onsubmit=e=>{e.preventDefault();void decide(true);};
el('deny').onclick=()=>void decide(false);
void check();
