'use strict';
/* Hosted scenery replaces matching drawings only; world geometry and interactions stay authoritative. */
(() => {
  if(window.AWArtWorld)return;
  const regions={verdant:'terrain-verdant-reach',meridian:'terrain-shattered-meridian',gloam:'terrain-astral-gloam',shadow:'terrain-shadow-realms'};
  const prefixes={verdant:'verdant-reach/',meridian:'shattered-meridian/',gloam:'astral-gloam/',shadow:'shadow-realms/'};
  const props={
    verdant:{oak:'oak-tree',pine:'oak-tree',willow:'oak-tree',boulder:'carved-standing-stone',runeStone:'carved-standing-stone',shrineStone:'carved-standing-stone',flowers:'blue-flower-bush',garden:'blue-flower-bush',grass:'fern-grass',fern:'fern-grass',reeds:'fern-grass'},
    meridian:{column:'broken-column',obelisk:'broken-column',boulder:'crystal-boulder',snowRock:'crystal-boulder',sunRock:'crystal-boulder',iceCrystal:'crystal-boulder',purpleCrystal:'crystal-boulder',brokenWall:'broken-ruin-slab',grass:'desert-grass',dryShrub:'desert-grass',brazier:'ember-brazier'},
    gloam:{column:'broken-pillar',brokenWall:'broken-pillar',boulder:'floating-rocks',snowRock:'floating-rocks',basalt:'floating-rocks',purpleCrystal:'floating-rocks',iceCrystal:'floating-rocks',fern:'silver-blue-ferns',grass:'silver-blue-ferns',reeds:'silver-blue-ferns',flowers:'spectral-flower-bush',garden:'spectral-flower-bush'},
    shadow:{column:'distorted-column',obelisk:'distorted-column',deadTree:'twisted-dead-tree',oak:'twisted-dead-tree',pine:'twisted-dead-tree',willow:'twisted-dead-tree',grass:'spectral-grass',fern:'spectral-grass',reeds:'spectral-grass',flowers:'plum-thorn-bush',dryShrub:'plum-thorn-bush',garden:'plum-thorn-bush'}
  };
  const objects={verdant:{well:'mossy-well',seer:'emerald-altar',chest:'wooden-chest'},meridian:{chest:'iron-chest'},gloam:{chest:'celestial-chest',seer:'astral-orrery'},shadow:{chest:'corrupted-chest',seer:'corrupted-shrine'}};
  const heights={oak:55,pine:55,willow:48,deadTree:44,column:43,obelisk:45,brokenWall:23,boulder:24,snowRock:24,sunRock:24,basalt:24,iceCrystal:29,purpleCrystal:29,runeStone:25,shrineStone:23,flowers:17,garden:17,grass:16,fern:17,reeds:23,dryShrub:16,brazier:30,well:29,seer:42,chest:25};
  function regionFor(room=game.roomData){
    if(window.AWArt?.regionFor)return AWArt.regionFor(room);
    const node=window.AWCampaignData?.nodes[room?.campaignNode];
    if(node?.shadow||String(room?.campaignNode||'').startsWith('shadow:'))return 'shadow';
    return node?.continent||room?.continent||window.AWCampaign?.current()?.continent||'verdant';
  }
  function selection(type,room=game.roomData,interactable=false){
    const region=regionFor(room),name=(interactable?objects:props)[region]?.[type];
    return name?{asset:regions[region],frame:prefixes[region]+name,height:heights[type]||24}:null;
  }
  function readyFrame(selected){const atlas=selected&&window.AWArt?.get(selected.asset);return atlas?.frames.get(selected.frame)||null;}
  function zoom(){return window.AWWorld?.caching?1:window.AWPresentation?.camera.zoom||1;}
  function paintScenery(p,selected,bob=0){
    const frame=readyFrame(selected);if(!frame)return false;
    const rect=frame.contentBounds||frame.contentRect||frame.rect||frame.frame;
    if(!rect?.h)return false;
    const s=worldToScreen(p.x,p.y),scale=Number.isFinite(p.scale)?p.scale:1,opacity=window.AWWorld?.propOpacity(p)??1;
    ctx.save();ctx.globalAlpha*=opacity;shadowAt(p.x,p.y,18*scale,.18);
    const painted=AWArt.draw(selected.asset,ctx,s.x,s.y+bob,{frame:selected.frame,scale:selected.height/rect.h*scale*zoom()});
    ctx.restore();return painted;
  }
  const baseProp=drawProp;
  drawProp=function(p,biome){
    const selected=selection(p.type),bob=Math.sin(elapsed*1.2+(p.phase||0))*1.2;
    if(selected&&paintScenery(p,selected,bob))return;
    return baseProp(p,biome);
  };
  const baseObject=drawInteractable;
  function interactionLabel(o){
    if(window.AWModernUI||!game.player||dist(o,game.player)>=1.45)return;
    const s=worldToScreen(o.x,o.y);ctx.save();ctx.translate(s.x,s.y);
    ctx.fillStyle='rgba(4,9,16,.82)';ctx.strokeStyle='rgba(255,255,255,.2)';ctx.beginPath();ctx.roundRect(-50,-58,100,22,10);ctx.fill();ctx.stroke();
    ctx.fillStyle='#fff';ctx.font='900 10px system-ui';ctx.textAlign='center';ctx.fillText(`${isTouch?'USE':'E'} • ${o.label}`,0,-43);ctx.restore();
  }
  drawInteractable=function(o){
    const selected=!o.npc&&!o.used&&!o.opened&&!o.ref?.opened&&selection(o.type,game.roomData,true);
    if(selected&&paintScenery(o,selected)){interactionLabel(o);return;}
    return baseObject(o);
  };
  function groundSelection(room){
    const region=regionFor(room),biome=room.biome;
    const name=region==='verdant'?(['ruins','crypt'].includes(biome)?'ancient-paving':'mossy-grass'):region==='meridian'?(biome==='desert'?'desert-paving':biome==='crystal'?'cyan-crystal-floor':'cracked-stone'):region==='gloam'?(biome==='celestial'?'celestial-paving':'fractured-stone'):'corrupted-stone';
    return {asset:regions[region],frame:prefixes[region]+name};
  }
  function decorateGround(room){
    if(room.town||room.homestead)return;
    const selected=groundSelection(room),frame=readyFrame(selected);if(!frame)return;
    const rect=frame.contentBounds||frame.rect||frame.frame,count=window.AWPresentation?.quality==='low'?6:12;
    // Small, clipped surface patches cannot suggest new paths, blocked terrain, or hazards.
    for(let i=0;i<count;i++){
      const x=2.5+hash2(i,room.x||0,room.seed||0)*(ROOM_W-5),y=2.5+hash2(room.y||0,i,(room.seed||0)+19)*(ROOM_H-5),s=worldToScreen(x,y),z=zoom();
      ctx.save();ctx.globalAlpha*=.38;diamondPath(s.x,s.y,TILE_W*1.4*z,TILE_H*1.4*z);ctx.clip();
      AWArt.draw(selected.asset,ctx,s.x,s.y,{frame:selected.frame,scale:TILE_W*1.5/rect.w*z});ctx.restore();
    }
  }
  const baseDetails=drawFloorDetails;
  drawFloorDetails=function(room,pal){baseDetails(room,pal);decorateGround(room);};
  if(window.AWWorld){
    const baseFloor=AWWorld.floor;let lastAtlas,lastRegion;
    AWWorld.floor=function(room,pal){
      const region=regionFor(room),atlas=window.AWArt?.get(regions[region]);
      if(atlas!==lastAtlas||region!==lastRegion){lastAtlas=atlas;lastRegion=region;AWWorld.invalidate();}
      return baseFloor(room,pal);
    };
  }
  window.AWArtWorld={regions,props,objects,regionFor,selection,groundSelection,decorateGround};

  const weaponTypes={blade:'sword',greatblade:'sword',glaive:'spear',voidpike:'spear',handcannon:'hand-cannon',arcaneorb:'arcane-orb',sundisc:'sun-disc',gauntlet:'unarmed-magic-gauntlet',unarmed:'unarmed-magic-gauntlet'};
  function equipment(item){
    if(!item||item.inventoryEmpty)return null;
    if(item.slot==='weapon')return {asset:'weapon-catalog',frame:'weapon-'+(weaponTypes[item.type]||item.type)};
    if(item.slot==='armor')return {asset:window.AWArtRenderer?.armorId(item)||'armor-wayfarer-cloth',frame:'SE-idle-a'};
    return null;
  }
  function crop(item,size=44){
    const selected=equipment(item),entry=selected&&window.AWArt?.entry(selected.asset),meta=entry?.meta;
    const frame=meta?.frames.find(f=>f.id===selected.frame);if(!frame)return null;
    const rect=frame.rect||frame.frame,content=item.slot==='armor'?(frame.contentBounds||frame.contentRect):null;
    const r=content?{x:rect.x+content.x,y:rect.y+content.y,w:content.w,h:content.h}:rect;
    if(!r?.w||!r?.h)return null;
    const scale=(size-4)/Math.max(r.w,r.h);
    return {...selected,image:entry.image,width:r.w*scale,height:r.h*scale,backgroundWidth:meta.width*scale,backgroundHeight:meta.height*scale,x:r.x*scale,y:r.y*scale};
  }
  function paintPreview(node,item,size){
    const c=crop(item,size);if(!c||!window.AWArt?.get(c.asset))return false;
    node.replaceChildren();node.classList.add('aw-art-preview');node.style.setProperty('--aw-art-size',size+'px');
    const sprite=document.createElement('span');sprite.className='aw-art-crop';sprite.setAttribute('aria-hidden','true');
    Object.assign(sprite.style,{width:c.width+'px',height:c.height+'px',backgroundImage:`url("${AWArt.url(c.image)}")`,backgroundSize:`${c.backgroundWidth}px ${c.backgroundHeight}px`,backgroundPosition:`-${c.x}px -${c.y}px`});
    node.append(sprite);return true;
  }
  function preview(item,size=44,cls=''){
    const node=document.createElement('span');node.className=cls;node.textContent=item?.icon||'';node.setAttribute('aria-hidden','true');
    const c=crop(item,size);if(!c)return node;
    if(!paintPreview(node,item,size))Promise.resolve(AWArt.ensure(c.asset)).then(()=>{if(node.isConnected)paintPreview(node,item,size);}).catch(()=>{});
    return node;
  }
  const escape=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  function html(item,size=88){
    const c=crop(item,size);if(!c)return escape(item?.icon||'');
    const key=c.asset+'|'+c.frame;
    // Gear cards are inserted synchronously by their caller, then painted once the atlas is ready.
    Promise.resolve(AWArt.ensure(c.asset)).then(()=>document.querySelectorAll('[data-aw-art-preview]').forEach(node=>{if(node.dataset.awArtPreview===key)paintPreview(node,item,size);})).catch(()=>{});
    return `<span data-aw-art-preview="${escape(key)}" aria-hidden="true">${escape(item.icon||'')}</span>`;
  }
  const baseGearCard=gearCard;
  gearCard=function(item,label){const card=baseGearCard(item,label);return item?card.replace(/(<div class="gear-preview">)[\s\S]*?(<\/div>)/,(_,open,close)=>open+html(item)+close):card;};
  window.AWArtUI={equipment,crop,preview,html};
})();
