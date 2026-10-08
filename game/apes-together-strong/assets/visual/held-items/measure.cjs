/* Deterministic alpha bounds and attachment metadata for the two original atlases.
 * Requires sharp; does not synthesize or modify the authored images. */
'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),sharp=require('sharp');
const combat=['pistol','rifle','shotgun','sniper','assault','machine','rotary','riotShieldFront','riotShieldRear','radio','grenade','flare','medkit','binoculars','mortar','hammer','spear','torch','cuffs','chainWrap'];
const utility=['wood','food','maul','cutters','baton','hook','staff','drum','sling','jammer','scoutSashFront','scoutSashRear','commandSashFront','commandSashRear','ropePack','toolRig','satchel','shoulderPlate','utilityBelt','warMantle'];
const boxes=[[10,60,229,182],[247,91,317,151],[560,91,280,149],[837,85,290,155],[1128,78,274,169],[12,314,316,215],[333,291,296,240],[645,272,198,281],[900,273,215,280],[1196,264,166,290],[67,578,152,230],[353,534,96,279],[549,571,275,235],[851,576,289,236],[1143,548,259,272],[69,822,154,289],[353,815,96,295],[641,806,97,306],[844,864,309,202],[1166,872,232,194]];
// draw width/height and grip as fractions of each trimmed opaque rectangle.
const size={pistol:[16,10,.35,.64],rifle:[36,10,.39,.64],shotgun:[33,10,.36,.62],sniper:[43,11,.40,.61],assault:[31,13,.32,.61],machine:[40,19,.35,.42],rotary:[44,24,.26,.35],riotShieldFront:[19,35,.5,.48],riotShieldRear:[19,35,.5,.48],radio:[7,18,.5,.72],grenade:[7,11,.5,.55],flare:[5,23,.5,.80],medkit:[13,12,.5,.10],binoculars:[11,8,.5,.2],mortar:[25,34,.5,.94],hammer:[15,28,.49,.77],spear:[10,68,.5,.76],torch:[10,47,.5,.83],cuffs:[11,6,.5,.5],chainWrap:[9,8,.5,.5],wood:[37,19,.5,.55],food:[23,23,.5,.1],maul:[30,49,.50,.81],cutters:[13,23,.5,.81],baton:[8,31,.35,.55],hook:[12,32,.5,.5],staff:[11,64,.5,.76],drum:[22,26,.5,.15],sling:[13,28,.47,.1],jammer:[10,25,.5,.77],scoutSashFront:[23,29,.5,.47],scoutSashRear:[23,29,.5,.47],commandSashFront:[24,31,.5,.47],commandSashRear:[24,31,.5,.47],ropePack:[24,31,.5,.5],toolRig:[24,26,.5,.5],satchel:[18,19,.5,.5],shoulderPlate:[21,23,.5,.5],utilityBelt:[27,15,.5,.13],warMantle:[31,32,.5,.38]};
(async()=>{
 const out={version:1,generator:'built-in image_gen',sourcePrompts:'held-items/prompts.json',atlases:[],frames:{}};
 for(const [name,ids]of [['combat',combat],['utility',utility]]){
  const file=path.join(__dirname,name+'.png'),bytes=fs.readFileSync(file),{data,info}=await sharp(bytes).ensureAlpha().raw().toBuffer({resolveWithObject:true});let transparent=0;
  for(let n=3;n<data.length;n+=4)if(data[n]===0)transparent++;
  const atlas='held-'+name;out.atlases.push({id:atlas,file:'held-items/'+name+'.png',width:info.width,height:info.height,columns:5,rows:4,frameCount:20,transparentFraction:Math.round(transparent/(info.width*info.height)*1e4)/1e4,sha256:crypto.createHash('sha256').update(bytes).digest('hex')});
  ids.forEach((id,i)=>{
   const b=name==='combat'?boxes[i]:[Math.floor(i%5*info.width/5),Math.floor(Math.floor(i/5)*info.height/4),Math.ceil(info.width/5),Math.ceil(info.height/4)];
   let x0=info.width,y0=info.height,x1=-1,y1=-1,opaque=0;
   for(let y=b[1];y<Math.min(info.height,b[1]+b[3]);y++)for(let x=b[0];x<Math.min(info.width,b[0]+b[2]);x++)if(data[(y*info.width+x)*4+3]>8){x0=Math.min(x0,x);x1=Math.max(x1,x);y0=Math.min(y0,y);y1=Math.max(y1,y);opaque++}
   if(x1<x0)throw Error('Empty sprite '+id);const w=x1-x0+1,h=y1-y0+1,s=size[id];
   out.frames[id]={atlas,x:x0,y:y0,w,h,gripX:s[2]*w,gripY:s[3]*h,drawWidth:s[0],drawHeight:s[1],opaquePixels:opaque};
  });
 }
 const stoneBytes=fs.readFileSync(path.join(__dirname,'stone.png')),{data:stonePixels,info:stoneInfo}=await sharp(stoneBytes).raw().toBuffer({resolveWithObject:true});
 let sx=128,sy=128,ex=0,ey=0,count=0;for(let y=0;y<128;y++)for(let x=0;x<128;x++)if(stonePixels[(y*128+x)*4+3]>8){sx=Math.min(sx,x);sy=Math.min(sy,y);ex=Math.max(ex,x);ey=Math.max(ey,y);count++}
 out.atlases.push({id:'held-stone',file:'held-items/stone.png',width:stoneInfo.width,height:stoneInfo.height,columns:1,rows:1,frameCount:1,sha256:crypto.createHash('sha256').update(stoneBytes).digest('hex')});
 out.frames.stone={atlas:'held-stone',x:sx,y:sy,w:ex-sx+1,h:ey-sy+1,gripX:(ex-sx+1)*.5,gripY:(ey-sy+1)*.72,drawWidth:8,drawHeight:8,opaquePixels:count};
 fs.writeFileSync(path.join(__dirname,'manifest.json'),JSON.stringify(out,null,2)+'\n');console.log('Measured '+Object.keys(out.frames).length+' held item frames from original RGBA artwork');
})();
