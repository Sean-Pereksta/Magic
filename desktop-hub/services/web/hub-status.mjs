let finished=false;
async function poll() {
  if(finished)return;
  try{
    const response=await fetch('/v1/result',{credentials:'same-origin',cache:'no-store',signal:AbortSignal.timeout(5000)});
    if(response.status===401)return;
    if(response.ok){
      const {receipt,username}=await response.json();
      if(receipt){
        const change=receipt.players.find(p=>p.username===username);
        if(!change)return;
        const box=document.createElement('aside');box.setAttribute('role','status');
        Object.assign(box.style,{position:'fixed',bottom:'1rem',right:'1rem',zIndex:'2147483647',padding:'1rem',maxWidth:'min(27rem,84vw)',background:'#201e22',color:'#fff0d5',border:'1px solid #b7924c',borderRadius:'.5rem',font:'600 16px system-ui'});
        box.textContent='Ranked result · '+change.rating+' Elo ('+(change.change>=0?'+':'')+change.change+') · '+change.tier+(change.placementsRemaining?' · '+change.placementsRemaining+' placements remaining':'');
        if(receipt.scores)box.append(document.createElement('br'),receipt.scores.map((s,i)=>receipt.players[i].username+' — '+s.waves+' waves, '+(s.timeMs/1000).toFixed(1)+'s').join(' / '));
        const close=document.createElement('button');close.textContent='Dismiss';close.style.marginLeft='1rem';close.onclick=()=>box.remove();box.append(close);document.body.append(box);finished=true;return;
      }
    }
  }catch{}
  setTimeout(poll,4000);
}
void poll();
