const key=new URLSearchParams(location.hash.slice(1)).get('key');
history.replaceState(null,'',location.pathname);
try{
  if(!key)throw new Error('This invitation has expired. Return to the ranked queue.');
  const res=await fetch('/v1/claim',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({key}),cache:'no-store'});
  const player=await res.json();if(!res.ok)throw new Error(player.error||'The match could not open.');
  if(player.game==='bannerlands'){
    sessionStorage.setItem('bannerlands.active-host',location.origin);
    sessionStorage.setItem('bannerlands-session:'+location.origin,JSON.stringify(player.session));
    location.replace('/');
  }else{
    sessionStorage.setItem('cat-mouse-seat:'+location.origin+':'+player.room,JSON.stringify(player.token));
    location.replace('/#'+new URLSearchParams({room:player.room,returnUrl:'https://www.catnmice.com/lobby/lobby.html'}));
  }
}catch(error){document.getElementById('message').textContent=error.message;}
