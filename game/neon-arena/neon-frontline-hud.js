/* Per-player territory HUD. The map receives friendlies and reported sightings,
 * never an unrestricted live enemy list. DOM updates are throttled by the host. */
(function(root){
  'use strict';
  const COLORS={0:'#a8b7c5',1:'#f0bb74',2:'#77e1d1'},clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
  function captureStatus(state,viewer,clear=()=>true){
    if(!state||viewer.dead||viewer.spawnGrace>0)return null;
    const sector=state.sectors.find(s=>Math.hypot(viewer.x-s.x,viewer.z-s.z)<=s.radius&&Math.abs((viewer.y||1.86)-(s.y||1.86))<2.5&&clear(viewer,s));
    if(!sector)return null;
    if(sector.owner===viewer.team&&!sector.contested&&(!sector.capturing||sector.capturing===viewer.team||!sector.progress))return null;
    const progress=Math.round(clamp(sector.progress,0,1)*100),connected=root.NeonConquest.canCapture(state,sector,viewer.team);
    return {id:sector.id,progress,text:sector.contested?'CONTESTED · '+sector.id:sector.owner===viewer.team?'DEFEND '+sector.id+' · ENEMY CAPTURE':!connected?'CONNECT THE FRONT TO '+sector.id:(sector.owner?'NEUTRALIZING ':'CAPTURING ')+sector.id+' · '+progress+'%',contested:sector.contested};
  }
  function mapContacts(viewer,actors,sightings,now){
    return [...actors.filter(a=>!a.dead&&a.team===viewer.team).map(a=>({x:a.x,z:a.z,yaw:a.yaw,self:a===viewer,color:a===viewer?'#ffffff':COLORS[viewer.team]})),...sightings.filter(s=>s.team===viewer.team&&s.until>now).map(s=>({x:s.x,z:s.z,color:'#ff7789',reported:true}))];
  }
  function killNotice(victim,killer,head,now){
    if(!killer?.human||killer===victim||!victim||killer.team===victim.team)return null;
    return {name:String(victim.name||'Opponent'),head:!!head,until:now+3200};
  }
  function layout(rect,{touch=false}={}){
    const compact=touch||rect.w<600||rect.h<500,map=Math.max(44,Math.min(compact?(rect.h<500||rect.w<360?76:84):112,rect.w*.28));
    return {compact,scoreTop:6,scoreWidth:Math.min(compact?326:360,rect.w-20),scoreHeight:30,mapSize:map,mapTop:58,mapCaptionHeight:14,toastTop:58+map+20,expandedSize:Math.max(100,Math.min(440,rect.w-30,rect.h-112))};
  }
  function create({host,onMapChange=()=>{}}){
    const layer=document.createElement('div');layer.id='frontlineHud';layer.hidden=true;host.appendChild(layer);
    const notices=new Map(),views=[];let expanded=-1,lastArgs=null;
    const node=(tag,className,text)=>{const n=document.createElement(tag);n.className=className;if(text)n.textContent=text;return n;};
    function closeMap(reason='user'){expanded=-1;onMapChange(false,reason);if(lastArgs)render(lastArgs);}
    function makeView(index){
      const frame=node('section','frontlineView'),bar=node('div','frontlineScorebar'),scores=node('div','frontlineScores'),cats=node('span','catScore'),rule=node('span','frontlineRule'),mice=node('span','mouseScore'),flags=node('div','frontlineFlags'),status=node('div','frontlineCapture');
      bar.setAttribute('role','group');bar.setAttribute('aria-label','Territory scores and capture progress');scores.append(cats,rule,mice);bar.append(scores,flags,status);
      const shade=node('button','frontlineMapShade');shade.type='button';shade.setAttribute('aria-label','Close tactical map');shade.onclick=()=>closeMap();
      const map=node('button','frontlineMap'),canvas=node('canvas','frontlineMapCanvas'),caption=node('span','frontlineMapCaption','TACTICAL MAP ↗');map.type='button';map.setAttribute('aria-label','Expand tactical map');map.append(canvas,caption);map.onclick=()=>{expanded=expanded===index?-1:index;onMapChange(expanded>=0);if(lastArgs)render(lastArgs);};
      const toast=node('div','frontlineKills');toast.setAttribute('role','status');toast.setAttribute('aria-live','polite');
      frame.append(bar,shade,map,toast);layer.appendChild(frame);
      const flagNodes=Array.from({length:5},()=>{const f=node('div','frontlineFlag'),label=node('span','flagName'),track=node('div','flagTrack'),fill=node('i','flagFill');track.appendChild(fill);f.append(label,track);flags.append(f);return {f,label,fill};});
      return {frame,bar,cats,mice,rule,status,map,canvas,caption,shade,toast,flagNodes,toastKey:''};
    }
    function paintMap(view,args,viewer,size){
      const canvas=view.canvas,dpr=Math.min(root.devicePixelRatio||1,2),pixels=Math.round(size*dpr);if(canvas.width!==pixels){canvas.width=canvas.height=pixels;}
      const c=canvas.getContext('2d');c.setTransform(dpr,0,0,dpr,0,0);c.clearRect(0,0,size,size);c.fillStyle='#0c1a25';c.fillRect(0,0,size,size);
      const tiny=size<=90,bounds=args.world?.bounds||65,scale=(size-(tiny?12:22))/(bounds*2),project=p=>[size/2+p.x*scale,size/2+p.z*scale];
      c.strokeStyle='#263b48';c.lineWidth=1;for(let i=1;i<4;i++){c.beginPath();c.moveTo(size*i/4,0);c.lineTo(size*i/4,size);c.moveTo(0,size*i/4);c.lineTo(size,size*i/4);c.stroke();}
      c.fillStyle='#314754';for(const p of args.world?.minimap||[]){const [x,z]=project(p);c.fillRect(x-p.w*scale/2,z-p.d*scale/2,Math.max(1,p.w*scale),Math.max(1,p.d*scale));}
      for(const sector of args.state.sectors){const [x,z]=project(sector);for(const id of sector.links){const b=args.state.sectors.find(s=>s.id===id);if(!b||sector.id>b.id)continue;const [bx,bz]=project(b);c.strokeStyle=sector.owner&&sector.owner===b.owner?COLORS[sector.owner]:'#66818f';c.lineWidth=1.5;c.beginPath();c.moveTo(x,z);c.lineTo(bx,bz);c.stroke();}}
      for(const sector of args.state.sectors){const [x,z]=project(sector);c.fillStyle=COLORS[sector.owner];c.beginPath();c.arc(x,z,size>200?8:tiny?3:5,0,Math.PI*2);c.fill();c.strokeStyle=sector.contested?'#fff':'#07121c';c.lineWidth=sector.contested?(tiny?1.5:3):1;c.stroke();c.font='700 '+(size>200?11:tiny?7:9)+'px Arial';c.textAlign='center';c.fillStyle='#fff';c.fillText(sector.id,x,z-(tiny?6:10));}
      for(const [team,hq] of Object.entries(args.state.hq)){const [x,z]=project(hq),r=tiny?2:3;c.fillStyle=COLORS[team];c.fillRect(x-r,z-r,r*2,r*2);}
      for(const p of mapContacts(viewer,args.actors,args.sightings,args.time)){const [x,z]=project(p);c.fillStyle=p.color;c.beginPath();if(p.self){c.save();c.translate(x,z);c.rotate(-(p.yaw||0));c.moveTo(0,tiny?4:6);c.lineTo(tiny?-3:-4,tiny?-3:-4);c.lineTo(tiny?3:4,tiny?-3:-4);c.closePath();c.fill();c.restore();}else{c.arc(x,z,tiny?1.5:p.reported?2.5:2,0,Math.PI*2);c.fill();}}
      c.fillStyle='#b7ccd7';c.font='700 '+(tiny?7:9)+'px Arial';c.textAlign='left';c.fillText('N',tiny?3:5,tiny?9:12);
    }
    function render(args){
      lastArgs=args;layer.hidden=!args.visible;if(!args.visible){if(expanded>=0){expanded=-1;onMapChange(false,'hidden');}return;}
      args.rects.forEach((rect,index)=>{
        const v=views[index]||(views[index]=makeView(index)),ent=rect.ent,geo=layout(rect,{touch:args.touch}),isConquest=!!args.state,open=expanded===index&&isConquest;
        v.frame.hidden=false;Object.assign(v.frame.style,{left:rect.x+'px',top:rect.y+'px',width:rect.w+'px',height:rect.h+'px'});v.frame.classList.toggle('mapExpanded',open);v.frame.classList.toggle('compactHud',geo.compact);v.bar.hidden=v.map.hidden=!isConquest;v.shade.hidden=!open;
        Object.assign(v.bar.style,{top:geo.scoreTop+'px',width:geo.scoreWidth+'px',height:geo.scoreHeight+'px'});
        v.toast.style.top=(isConquest?geo.toastTop:geo.compact?118:180)+'px';
        if(isConquest){
          const s=args.state,values=s.config.rule==='tickets'?s.tickets:s.score;
          const rule=s.config.rule==='tickets'?'TICKETS':'POINTS',wave=Math.ceil(s.nextWave);
          v.cats.textContent='CATS '+Math.ceil(values[1]);v.mice.textContent=Math.ceil(values[2])+' MICE';v.rule.textContent=rule+'\n'+wave+'s';
          v.bar.setAttribute('aria-label','Cats '+Math.ceil(values[1])+', Mice '+Math.ceil(values[2])+', '+rule.toLowerCase()+'. Reinforcements in '+wave+' seconds.');
          s.sectors.forEach((f,i)=>{const n=v.flagNodes[i];if(!n)return;n.label.textContent=f.id+(f.contested?' !':'');n.f.style.setProperty('--flag-color',COLORS[f.owner]);n.f.classList.toggle('contested',f.contested);n.fill.style.width=Math.round(clamp(f.progress,0,1)*100)+'%';n.fill.style.background=COLORS[f.capturing]||COLORS[0];n.f.setAttribute('aria-label','Sector '+f.id+', '+(f.owner===1?'Cats':f.owner===2?'Mice':'neutral')+(f.contested?', contested':f.capturing?', capture '+Math.round(f.progress*100)+'%':''));});
          const capture=captureStatus(s,ent,args.clear);v.status.hidden=!capture;v.status.textContent=capture?.text||'';v.status.classList.toggle('contested',!!capture?.contested);
          const size=open?geo.expandedSize:geo.mapSize;Object.assign(v.map.style,{width:size+'px',top:(open?Math.max(44,(rect.h-size-28)/2):geo.mapTop)+'px',right:open?((rect.w-size)/2)+'px':'10px'});v.map.setAttribute('aria-expanded',String(open));v.map.setAttribute('aria-label',open?'Collapse tactical map':'Expand tactical map');v.caption.textContent=open?'CLOSE MAP ×':args.touch&&index===0?'MAP ↗':'MAP · M';paintMap(v,args,ent,size);
        }
        const list=(notices.get(ent)||[]).filter(n=>n.until>args.now);notices.set(ent,list);const key=JSON.stringify(list);
        if(key!==v.toastKey){v.toastKey=key;v.toast.replaceChildren(...list.map(n=>{const card=node('div','frontlineKill'),title=node('strong','',n.head?'HEADSHOT ELIMINATION':'ELIMINATED'),name=node('span','',n.name);card.append(title,name);return card;}));}
      });
      for(let i=args.rects.length;i<views.length;i++)views[i].frame.hidden=true;
    }
    root.addEventListener('keydown',e=>{if(e.code==='Escape'&&expanded>=0){closeMap();e.preventDefault();}else if(e.code==='KeyM'&&!e.repeat&&lastArgs?.visible&&lastArgs.state&&!/INPUT|SELECT|TEXTAREA/.test(e.target?.tagName||'')){expanded=expanded>=0?-1:0;onMapChange(expanded>=0);render(lastArgs);e.preventDefault();}});
    return {render,recordKill(victim,killer,head,now){const n=killNotice(victim,killer,head,now);if(n)notices.set(killer,[n,...(notices.get(killer)||[]).filter(v=>v.until>now)].slice(0,2));},reset(){notices.clear();closeMap('reset');},get expanded(){return expanded>=0;}};
  }
  const api={captureStatus,mapContacts,killNotice,layout,create};root.NeonFrontlineHUD=api;if(typeof module==='object')module.exports=api;
})(typeof globalThis==='object'?globalThis:this);
