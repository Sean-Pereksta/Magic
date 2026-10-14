(function(root){
  'use strict';
  // This renderer never writes to terrain, resource nodes, entities, or simulation RNG.
  const MAP_IDS=['classic','ember-rift','frostline-basin','verdant-divide','shattered-dunes','orbital-foundry'];
  const CELL=256,PROP_CELL=128,CHUNK=8;
  const RESOURCE_FRAME_MS=480,AMBIENT_FRAME_MS=520;
  const OFFSETS=[[0,-1],[1,0],[0,1],[-1,0],[-1,-1],[1,-1],[1,1],[-1,1]];
  const GROUPS={lane:1,expansion:3,ruins:3,mineral:4,gas:4};
  function hash(seed,x,y,salt=0){let n=((seed>>>0)^Math.imul(x+17,73856093)^Math.imul(y+29,19349663)^Math.imul(salt+1,83492791))>>>0;n=Math.imul(n^(n>>>13),1274126177);return (n^(n>>>16))>>>0}
  function group(tile){return tile?.blocked?2:GROUPS[tile?.type]??0}
  function describeTile(tile,seed,tileAt){
    const type=group(tile),noise=hash(seed,tile.x,tile.y),neighbors=OFFSETS.map(([dx,dy])=>tileAt(tile.x+dx,tile.y+dy));
    let sameMask=0,openMask=0;neighbors.forEach((n,i)=>{if(n&&group(n)===type)sameMask|=1<<i;if(n&&!n.blocked)openMask|=1<<i});
    const variants=type===2?[8,9,10,11]:type===1?[4,5,6,7]:type===3?[12,13,14]:type===4?[0,1,2,3]:[0,1,2,3];
    return {index:variants[noise%variants.length],group:type,noise,sameMask,openMask,
      // Cardinal seams plus diagonal masks produce straight, outer and concave corners.
      edges:(~sameMask)&15,outerCorners:(~sameMask>>>4)&15,
      innerCorners:[16,32,64,128].reduce((m,bit,i)=>m|((sameMask&(1<<i))&&(sameMask&(1<<((i+3)%4)))&&!(sameMask&bit)?1<<i:0),0),
      prop:tile.blocked?(noise%7===0?noise%4:null):type===0&&noise%31===0?noise%4:null,
      ambient:tile.blocked&&noise%9===0||type===0&&noise%113===0};
  }
  function geometryStamp(terrain){let value=2166136261;for(const t of terrain){const key=t.type||'ground';for(let i=0;i<key.length;i++)value=Math.imul(value^key.charCodeAt(i),16777619);value=Math.imul(value^(t.blocked?1:0),16777619)}return value>>>0}
  function createRenderer(options={}){
    const baseUrl=options.baseUrl||'https://pub-cb55798f1eeb4f80a85463da3612ed61.r2.dev/terrain/',effectsUrl=options.effectsUrl||'https://pub-cb55798f1eeb4f80a85463da3612ed61.r2.dev/effects/combat.webp';
    const images=new Map(),loads=new Map(),failures=new Set(),chunks=new Map();
    let prepared=null,lastTerrain=null,lastStamp=0,lastCheck=0,version=0;
    const stats={builds:0,hits:0,draws:0,ambientDraws:0,effectDraws:0};
    function image(name,url){if(loads.has(name))return loads.get(name);const promise=new Promise(resolve=>{const img=new root.Image();img.decoding='async';let done=false;const finish=ok=>{if(done)return;done=true;clearTimeout(timer);img.onload=img.onerror=null;if(ok){images.set(name,img);failures.delete(name);version++;chunks.clear()}else{failures.add(name);loads.delete(name)}resolve(ok)};const timer=setTimeout(()=>finish(false),20000);img.onload=()=>Promise.resolve(img.decode?.()).then(()=>finish(!!(img.naturalWidth||img.width)),()=>finish(false));img.onerror=()=>finish(false);img.src=url});loads.set(name,promise);return promise}
    function load(id='classic'){id=MAP_IDS.includes(id)?id:'classic';return Promise.all([image(id,baseUrl+id+'.terrain.webp'),image(id+':props',baseUrl+id+'.props.webp'),image('effects',effectsUrl)]).then(results=>results[0])}
    function prepare(input){const mapId=MAP_IDS.includes(input.mapId)?input.mapId:'classic',quality=['low','medium','high'].includes(input.quality)?input.quality:'medium',now=input.now??Date.now();
      if(lastTerrain!==input.terrain||now-lastCheck>500){lastStamp=geometryStamp(input.terrain);lastTerrain=input.terrain;lastCheck=now}
      const key=[mapId,input.seed>>>0,lastStamp,quality,version,input.width,input.height].join(':');
      if(prepared?.key!==key){chunks.clear();prepared={...input,mapId,quality,key,resolution:quality==='high'?96:quality==='low'?32:64};prepared.tileAt=(x,y)=>x>=0&&y>=0&&x<input.width&&y<input.height?input.terrain[y*input.width+x]:null}
      load(mapId);return prepared;
    }
    function blit(context,atlas,index,x,y,size){context.drawImage(atlas,(index%4)*CELL,Math.floor(index/4)*CELL,CELL,CELL,x,y,size+.5,size+.5)}
    function propFrame(context,props,index,x,y,size){context.drawImage(props,(index%4)*PROP_CELL,Math.floor(index/4)*PROP_CELL,PROP_CELL,PROP_CELL,x,y,size,size)}
    function paintTile(context,tile,x,y,size){const atlas=images.get(prepared.mapId),props=images.get(prepared.mapId+':props'),d=describeTile(tile,prepared.seed,prepared.tileAt);blit(context,atlas,d.index,x,y,size);
      // Blend atlas pixels across actual topology seams; never infer new collision tiles.
      for(let side=0;side<4;side++)if(d.edges&(1<<side)){const [dx,dy]=OFFSETS[side],neighbor=prepared.tileAt(tile.x+dx,tile.y+dy);if(!neighbor||tile.blocked||neighbor.blocked)continue;
        const nd=describeTile(neighbor,prepared.seed,prepared.tileAt),band=size*.17;context.save();context.beginPath();if(side===0)context.rect(x,y,size,band);else if(side===1)context.rect(x+size-band,y,band,size);else if(side===2)context.rect(x,y+size-band,size,band);else context.rect(x,y,band,size);context.clip();context.globalAlpha=.3;blit(context,atlas,nd.index,x,y,size);context.restore();}
      if(tile.blocked){context.save();context.lineWidth=Math.max(1,size*.07);context.strokeStyle='rgba(7,12,20,.7)';context.beginPath();if(d.openMask&1){context.moveTo(x,y+size*.04);context.lineTo(x+size,y+size*.04)}if(d.openMask&2){context.moveTo(x+size*.96,y);context.lineTo(x+size*.96,y+size)}if(d.openMask&4){context.moveTo(x,y+size*.96);context.lineTo(x+size,y+size*.96)}if(d.openMask&8){context.moveTo(x+size*.04,y);context.lineTo(x+size*.04,y+size)}context.stroke();context.strokeStyle='rgba(220,233,231,.26)';context.lineWidth=Math.max(.7,size*.022);context.stroke();context.restore()}
      // Inner/outer corner seams use feathered bitmap fragments, without arbitrary rotation.
      if(!tile.blocked)for(let corner=0;corner<4;corner++)if(d.innerCorners&(1<<corner)){const [dx,dy]=OFFSETS[corner+4],neighbor=prepared.tileAt(tile.x+dx,tile.y+dy);if(!neighbor||neighbor.blocked)continue;const cx=x+(dx>0?size:0),cy=y+(dy>0?size:0);context.save();context.beginPath();context.arc(cx,cy,size*.23,0,Math.PI*2);context.clip();context.globalAlpha=.18;blit(context,atlas,describeTile(neighbor,prepared.seed,prepared.tileAt).index,x,y,size);context.restore()}
      if(props&&d.prop!==null&&prepared.quality!=='low'){const scale=tile.blocked?.7:.4,offset=(1-scale)*.5;context.save();context.globalAlpha=tile.blocked?.8:.58;propFrame(context,props,12+d.prop,x+size*offset,y+size*offset,size*scale);context.restore()}
    }
    function chunkAt(tile){const cx=Math.floor(tile.x/CHUNK),cy=Math.floor(tile.y/CHUNK),key=cx+','+cy;if(chunks.has(key)){stats.hits++;return chunks.get(key)}const size=prepared.resolution,canvas=options.makeCanvas?options.makeCanvas(CHUNK*size,CHUNK*size):root.document.createElement('canvas');canvas.width=CHUNK*size;canvas.height=CHUNK*size;const context=canvas.getContext('2d');context.imageSmoothingEnabled=true;
      for(let y=0;y<CHUNK;y++)for(let x=0;x<CHUNK;x++){const current=prepared.tileAt(cx*CHUNK+x,cy*CHUNK+y);if(current)paintTile(context,current,x*size,y*size,size)}
      // A full 42×30 field uses only 24 medium 512px chunks (~24 MiB decoded).
      if(chunks.size>=32)chunks.delete(chunks.keys().next().value);chunks.set(key,canvas);stats.builds++;return canvas;
    }
    function drawTile(context,tile,screen,size){if(!prepared||!tile||!images.has(prepared.mapId))return false;const canvas=chunkAt(tile),r=prepared.resolution;context.drawImage(canvas,(tile.x%CHUNK)*r,(tile.y%CHUNK)*r,r,r,screen.x,screen.y,size+1,size+1);stats.draws++;return true}
    function drawAmbient(context,tile,screen,size,now,visible){if(!prepared||prepared.quality==='low'||size<26||!visible||!images.has(prepared.mapId+':props'))return false;const d=describeTile(tile,prepared.seed,prepared.tileAt);if(!d.ambient)return false;const frame=Math.floor((now+2*(d.noise%1700))/AMBIENT_FRAME_MS)%4,scale=tile.blocked?.72:.38;context.save();context.globalAlpha=tile.blocked?.8:.48;propFrame(context,images.get(prepared.mapId+':props'),8+frame,screen.x+size*(1-scale)*.5,screen.y+size*(1-scale)*.5,size*scale);context.restore();stats.ambientDraws++;return true}
    function drawResource(context,node,screen,size,now,visible){if(!visible||node.amount<=0||!prepared)return false;const props=images.get(prepared.mapId+':props');if(!props)return false;const atlas=images.get(prepared.mapId);if(atlas&&prepared.quality!=='low'){context.save();context.globalAlpha*=.28;blit(context,atlas,15,screen.x,screen.y,size);context.restore()}const frame=prepared.quality==='low'||size<26?0:Math.floor((now+2*(hash(prepared.seed,node.x,node.y)%2000))/RESOURCE_FRAME_MS)%4,row=node.type==='gas'?1:0;propFrame(context,props,row*4+frame,screen.x+size*.08,screen.y+size*.06,size*.84);return true}
    const effectRows={ballistic:0,energy:1,organic:2,shield:3,explosion:4,dust:5};
    function drawEffect(context,type,x,y,size,ageMs=0,{visible=true,alpha=1,rotation=0,quality='medium',loop=false}={}){const atlas=images.get('effects'),row=effectRows[type];if(!visible||!atlas||row===undefined||size<4)return false;if(quality==='low'&&type==='dust')return false;const raw=Math.floor(Math.max(0,ageMs)/70),frame=loop?raw%8:Math.min(7,raw);context.save();context.globalAlpha*=alpha;context.translate(x,y);if(rotation)context.rotate(rotation);context.drawImage(atlas,frame*128,row*128,128,128,-size/2,-size/2,size,size);context.restore();stats.effectDraws++;return true}
    return {load,prepare,drawTile,drawAmbient,drawResource,drawEffect,invalidate(){chunks.clear();lastTerrain=null},get stats(){return {...stats,chunks:chunks.size,loaded:[...images.keys()],failures:[...failures],mapId:prepared?.mapId,quality:prepared?.quality,seed:prepared?.seed,geometry:prepared?.key}},
      // Image injection makes image loading and cache contracts testable without network.
      setImage(name,value){images.set(name,value);version++;chunks.clear()}};
  }
  root.TurncraftTerrain={MAP_IDS,describeTile,hash,geometryStamp,createRenderer};
  if(typeof module!=='undefined'&&module.exports)module.exports=root.TurncraftTerrain;
})(typeof window!=='undefined'?window:globalThis);
