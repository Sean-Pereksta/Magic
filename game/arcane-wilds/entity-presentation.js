'use strict';
/* Layered, directional poses use transient WeakMap state; collision bodies never move for FX. */
(() => {
  const P=window.AWPresentation;if(!P||P.entities)return;
  function line(ax,ay,bx,by,color,width=3){ctx.strokeStyle=color;ctx.lineWidth=width;ctx.beginPath();ctx.moveTo(ax,ay);ctx.lineTo(bx,by);ctx.stroke();}
  function player(p){
    const state=P.pose(p),a=p.armorGear,w=p.weapon,s=worldToScreen(p.x,p.y),f=p.facing||{x:1,y:0};
    const facing=Math.atan2((f.x+f.y)*.5,f.x-f.y),side=Math.cos(facing)>=0?1:-1,back=Math.sin(facing)<-.25;
    const run=state.speed,stride=Math.sin(state.stride),dodge=p.dodgeTime>0||state.state==='dodge',casting=/cast/i.test(state.state),attack=state.state==='attack';
    const bob=Math.abs(stride)*2.3*run+Math.sin(elapsed*2.7)*.6,castLift=casting?( /nova|ward|furnace|summon|spirits|starfall/i.test(state.cast||'')?-14:-5):0;
    shadowAt(p.x,p.y,dodge?23:18,dodge?.18:.38);
    ctx.save();ctx.translate(s.x+state.recoilX,s.y-19+bob+state.recoilY);ctx.scale(P.camera.zoom,P.camera.zoom);ctx.lineCap='round';ctx.lineJoin='round';
    if(dodge){ctx.rotate(side*.28);ctx.scale(1.14,.83);}
    if(p.hp<=0){ctx.rotate(side*1.3);ctx.globalAlpha=.65;}
    if(p.invuln>0)ctx.globalAlpha=.72+.28*Math.sin(elapsed*36)**2;
    const momentum=typeof intensityState==='function'?intensityState().momentum:0;
    if(momentum>15||state.state==='perfect'){
      ctx.strokeStyle=state.state==='perfect'?'#ffffff':colorAlpha(w.color,.25+momentum*.003);ctx.lineWidth=state.state==='perfect'?3:1.5;ctx.beginPath();ctx.ellipse(0,-3,23,32,0,0,TAU);ctx.stroke();
    }
    drawAura(p,a);
    // Cape hangs behind the torso, with stride-driven cloth rather than a sliding ellipse.
    ctx.fillStyle=colorAlpha(a.color,.95);ctx.strokeStyle=a.trim;ctx.lineWidth=1.5;ctx.beginPath();ctx.moveTo(-11,-17);ctx.lineTo(11,-17);ctx.quadraticCurveTo(14+side*run*5,7,18+stride*run*4,20);ctx.lineTo(0,15);ctx.lineTo(-16+stride*run*4,21);ctx.quadraticCurveTo(-13,0,-11,-17);ctx.fill();ctx.stroke();
    const legSwing=stride*6*run;
    line(-5,4,-6+legSwing,13,'#273544',6);line(-6+legSwing,13,-7+legSwing,20,'#16222e',5);
    line(5,4,6-legSwing,13,'#344350',6);line(6-legSwing,13,7-legSwing,20,'#16222e',5);
    const metal=/plate|mail|iron|steel|warden|bastion/i.test(a.name||'');
    ctx.fillStyle=a.color;ctx.strokeStyle=a.trim;ctx.lineWidth=1.5;ctx.beginPath();ctx.moveTo(-11,-17);ctx.lineTo(10,-17);ctx.lineTo(12,3);ctx.lineTo(5,10);ctx.lineTo(-9,7);ctx.closePath();ctx.fill();ctx.stroke();
    if(metal){ctx.fillStyle=a.trim;ctx.beginPath();ctx.ellipse(-11,-13,6,4,-.2,0,TAU);ctx.ellipse(11,-13,6,4,.2,0,TAU);ctx.fill();line(-7,-9,7,-9,colorAlpha('#fff',.28),2);}else{line(-6,-15,2,5,colorAlpha(a.trim,.7),2);}
    line(-10,4,10,4,'#49382d',3);ctx.fillStyle='#e6c77c';ctx.fillRect(-2,2,4,4);
    const handX=side*(casting?20:attack?23:14),handY=-6+castLift+(attack?-3:stride*run*2);
    line(-side*10,-12,-side*13,-1-stride*run*3,a.color,6);line(side*10,-12,handX,handY,a.color,6);
    ctx.fillStyle='#dfb99a';ctx.beginPath();ctx.arc(handX,handY,3,0,TAU);ctx.fill();
    ctx.save();ctx.translate(handX-side*13,handY+5);ctx.rotate(casting?-side*.35:attack?-side*.55:Math.sin(state.stride)*run*.08);drawWeaponModel(w,f);ctx.restore();
    ctx.fillStyle='#e8c5a7';ctx.beginPath();ctx.ellipse(side*1.5,-25,7.5,8.5,0,0,TAU);ctx.fill();
    ctx.fillStyle='#302b2c';ctx.beginPath();ctx.arc(0,-27,8,Math.PI,TAU);ctx.lineTo(back?8:4,back?-18:-26);ctx.lineTo(-7,-20);ctx.closePath();ctx.fill();
    if(!back){ctx.fillStyle='#302933';ctx.fillRect(side>0?3:-5,-25,2,2);}
    drawHelmet(a);
    if(casting){const c=P.colorFor(state.cast);ctx.fillStyle=c;ctx.shadowColor=c;ctx.shadowBlur=P.quality==='low'?0:12;ctx.beginPath();ctx.arc(handX,handY,4+Math.sin(elapsed*40),0,TAU);ctx.fill();ctx.shadowBlur=0;}
    const legendary=[w,a,...(p.trinkets||[])].some(item=>item?.rarity==='Legendary');
    if(legendary){ctx.strokeStyle='#ffda81';ctx.lineWidth=1.2;for(let i=0;i<3;i++){const angle=elapsed*.9+i*TAU/3,x=Math.cos(angle)*21,y=-7+Math.sin(angle)*10;ctx.beginPath();ctx.moveTo(x,y-4);ctx.lineTo(x+3,y);ctx.lineTo(x,y+4);ctx.lineTo(x-3,y);ctx.closePath();ctx.stroke();}}
    if(p.shield>0){ctx.strokeStyle='rgba(164,225,255,.7)';ctx.lineWidth=1.8;ctx.beginPath();ctx.ellipse(0,-6,24,32,0,0,TAU);ctx.stroke();}
    if(state.state==='heal'||state.state==='levelUp'){ctx.strokeStyle=state.state==='heal'?'#9af4b7':'#ffe39c';ctx.beginPath();ctx.arc(0,-14,30,elapsed*2,elapsed*2+4);ctx.stroke();}
    ctx.restore();
  }
  function enemyPose(e){
    const s=P.pose(e),ai=e.ai||'',f=e.facing||{x:1,y:0},side=f.x-f.y>=0?1:-1;
    ctx.translate(s.recoilX,s.recoilY);
    if(e.stun>0){ctx.rotate(Math.sin(elapsed*28)*.075);return;}
    const wind=e.telegraph?clamp(1-e.stateTime/(e.telegraph.maxTime||1),0,1):0;
    if(/charger|ram|boar|brute|hound|wolf|beast/.test(ai+' '+e.type)){
      ctx.scale(1+wind*.18,1-wind*.2);ctx.rotate(side*(s.speed*.08+wind*.13));
      for(let i=0;i<4;i++){const x=(i-1.5)*7,step=Math.sin(s.stride+(i%2)*Math.PI)*5*s.speed;line(x,0,x+step,9,e.color,3);}
    }else if(/mage|summon|beam|spread|necro|witch|priest/.test(ai)){
      ctx.translate(0,-wind*4);line(-12,-12,-19,-9-wind*17,e.color,4);line(12,-12,19,-9-wind*17,e.color,4);
      if(wind){ctx.strokeStyle=e.proj||e.color;ctx.beginPath();ctx.arc(0,-37,4+wind*7,0,TAU);ctx.stroke();}
    }else if(/orbiter|eye|fly|drake|phoenix|wisp/.test(ai)){
      ctx.translate(0,-6-Math.sin(elapsed*4+e.phase)*3);ctx.scale(1+Math.sin(elapsed*5+e.phase)*.04,1);
    }else if(/blink|vampire|assassin|burrow/.test(ai)){
      ctx.scale(1+wind*.12,1-wind*.32);if(/burrow/.test(ai)&&wind)ctx.globalAlpha=Math.max(.3,1-wind*.6);
    }else if(/golem|giant|shield|turret/.test(ai)){
      ctx.rotate(Math.sin(s.stride)*s.speed*.055);ctx.translate(side*wind*3,wind*2);
      line(-9,0,-10+Math.sin(s.stride)*3*s.speed,9,'#3c3940',5);line(9,0,10-Math.sin(s.stride)*3*s.speed,9,'#3c3940',5);
    }else{
      ctx.scale(1+Math.sin(s.stride)*s.speed*.06,1-Math.sin(s.stride)*s.speed*.07);
      line(-7,0,-9+Math.sin(s.stride)*4*s.speed,8,e.color,3);line(7,0,9-Math.sin(s.stride)*4*s.speed,8,e.color,3);
    }
    if(e.boss&&P.intro?.boss===e){ctx.scale(1+Math.sin(P.intro.time*Math.PI)*.09,1+Math.sin(P.intro.time*Math.PI)*.09);}
  }
  function weapon(w,f){
    if(['staff','scepter','bow','blade','spear','chakram'].includes(w.type))return false;
    const side=f.x-f.y>=0?1:-1;ctx.save();ctx.translate(side*13,-5);ctx.strokeStyle=w.color;ctx.fillStyle=w.color;ctx.lineWidth=2.5;ctx.lineCap='round';
    if(['crossbow','repeater','handcannon'].includes(w.type)){ctx.rotate(-side*.5);ctx.fillRect(-3,-19,6,28);ctx.fillStyle='#4b3840';ctx.fillRect(-4,1,8,10);if(w.type!=='handcannon'){ctx.strokeStyle=w.color;ctx.beginPath();ctx.moveTo(-12,-10);ctx.quadraticCurveTo(0,-24,12,-10);ctx.stroke();line(-12,-10,12,-10,'#d8dce4',1);}}
    else if(w.type==='grimoire'){ctx.rotate(side*.2);ctx.fillRect(-10,-17,20,24);ctx.fillStyle='#e4dbc1';ctx.fillRect(-8,-15,7,20);ctx.fillRect(1,-15,7,20);line(0,-16,0,6,'#765c58',2);}
    else if(w.type==='arcaneorb'||w.type==='sundisc'){ctx.rotate(elapsed*1.4);ctx.beginPath();ctx.arc(0,-8,9,0,TAU);ctx.stroke();for(let i=0;i<6;i++){const a=i*TAU/6;line(Math.cos(a)*12,-8+Math.sin(a)*12,Math.cos(a)*16,-8+Math.sin(a)*16,w.color,2);}}
    else if(w.type==='wand'){line(0,9,side*5,-17,w.color,3);ctx.beginPath();ctx.arc(side*5,-18,3,0,TAU);ctx.fill();}
    else if(w.type==='daggers'){for(const off of [-7,7]){ctx.beginPath();ctx.moveTo(off,6);ctx.lineTo(off+side*7,-17);ctx.lineTo(off+side*10,-4);ctx.closePath();ctx.fill();}}
    else{line(-side*3,14,side*9,-25,'#8c7764',3);ctx.beginPath();if(w.type==='scythe'){ctx.moveTo(side*9,-25);ctx.quadraticCurveTo(side*30,-24,side*32,-10);ctx.quadraticCurveTo(side*19,-19,side*9,-20);}else{ctx.moveTo(side*9,-34);ctx.lineTo(side*16,-18);ctx.lineTo(side*9,-22);ctx.lineTo(side*3,-18);}ctx.closePath();ctx.fill();}
    ctx.restore();return true;
  }
  P.entities={player,enemyPose,weapon};
})();
