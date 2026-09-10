'use strict';
/* Visited villages use the existing saved room.seen flag, including legacy/cloud saves. */
(() => {
  function destinations(){
    return Object.entries(game.rooms).filter(([key,r])=>r.town&&r.seen&&key!==roomKey(game.room.x,game.room.y))
      .map(([key,r])=>({key,...r})).sort((a,b)=>a.name.localeCompare(b.name)||a.x-b.x||a.y-b.y);
  }
  function travel(key){
    const r=game.rooms[key];
    if(!running||paused||roomTransition||!game.player||!game.roomData?.town||!r?.town||!r.seen||key===roomKey(game.room.x,game.room.y))return false;
    roomTransition=true;
    try{
      closeOverlay('npcPanel');
      game.room={x:r.x,y:r.y};
      // Clear room-bound effects as well as the attacks/summons cleared by loadRoom.
      game.effects.length=0;game.particles.length=0;
      game.player.x=ROOM_W/2;game.player.y=ROOM_H/2+1.1;
      game.player.dodgeTime=0;
      loadRoom();saveGame();updateHUD();
      burst(game.player.x,game.player.y,'#9cecff',20,1);
      toastMsg(`Arrived at ${r.name}`);
      return true;
    }finally{roomTransition=false;}
  }
  function open(){
    if(!game.roomData?.town||roomTransition)return;
    $('npcName').textContent='Village Portal';
    const body=$('npcBody');body.replaceChildren();
    const intro=document.createElement('p');intro.className='exp-dialogue';
    intro.textContent='Travel freely to any village you have visited. Discover more villages to expand this network.';
    body.appendChild(intro);
    const list=document.createElement('div');list.className='exp-shop';body.appendChild(list);
    const rooms=destinations();
    if(!rooms.length){const empty=document.createElement('p');empty.textContent='No other villages visited yet. This portal will connect as you explore.';list.appendChild(empty);}
    for(const r of rooms){
      const b=document.createElement('button');b.className='exp-buy';b.dataset.village=r.key;
      const name=document.createElement('b');name.textContent=r.name;
      const detail=document.createElement('span');detail.textContent=`Village (${r.x}, ${r.y}) • Free travel`;
      b.append(name,detail);b.onclick=()=>travel(r.key);list.appendChild(b);
    }
    showOverlay('npcPanel');
  }
  const baseLoad=loadRoom;
  loadRoom=function(){
    baseLoad();
    if(game.roomData?.town){
      game.interactables.push({type:'villagePortal',label:'Village Portal',x:ROOM_W/2,y:ROOM_H/2+2.25});
      saveGame();
    }
  };
  const baseInteract=interact;
  interact=function(){if(currentInteraction()?.type==='villagePortal')return open();return baseInteract();};
  const baseDraw=drawInteractable;
  drawInteractable=function(o){
    if(o.type!=='villagePortal')return baseDraw(o);
    shadowAt(o.x,o.y,26,.3);
    const s=worldToScreen(o.x,o.y);ctx.save();ctx.translate(s.x,s.y);
    ctx.lineWidth=7;ctx.strokeStyle='#617284';ctx.beginPath();ctx.ellipse(0,-25,21,31,0,Math.PI,TAU);ctx.stroke();
    ctx.fillStyle='rgba(71,119,172,.6)';ctx.strokeStyle='#a3efff';ctx.lineWidth=2;
    ctx.beginPath();ctx.ellipse(0,-24,17,26,0,0,TAU);ctx.fill();ctx.stroke();
    ctx.strokeStyle='#e3d0ff';ctx.beginPath();ctx.ellipse(0,-24,10+Math.sin(elapsed*2)*3,21,0,elapsed,elapsed+Math.PI*1.5);ctx.stroke();
    ctx.fillStyle='#ceeaff';for(let i=0;i<5;i++){const a=i*TAU/5+elapsed*.6;ctx.fillRect(Math.cos(a)*21-1,-24+Math.sin(a)*29-1,3,3);}
    ctx.font='bold 10px system-ui';ctx.textAlign='center';ctx.fillText('VILLAGE PORTAL',0,-62);ctx.restore();
  };
  window.AWVillagePortals={destinations,travel,open};
})();
