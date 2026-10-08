'use strict';
// Preserve the authored pose grid and ground anchors; change only body artwork.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),sharp=require('sharp');
const root=path.resolve(__dirname,'../assets/visual'),file=path.join(root,'characters/manifest.json');
(async()=>{
 const manifest=JSON.parse(fs.readFileSync(file,'utf8'));
 for(const name of ['humans','humanactions','humanwalk']){
  const atlas=manifest.atlases.find(a=>a.id==='characters-'+name),relative='characters/'+name+'-unarmed.png',bytes=fs.readFileSync(path.join(root,relative));
  const {data,info}=await sharp(bytes).ensureAlpha().raw().toBuffer({resolveWithObject:true});
  if(info.width!==atlas.width||info.height!==atlas.height)throw new Error(name+' body grid changed dimensions');
  let transparent=0;for(let i=3;i<data.length;i+=4)if(data[i]===0)transparent++;
  atlas.file=relative;atlas.transparentFraction=+(transparent/(info.width*info.height)).toFixed(4);
  atlas.sha256=crypto.createHash('sha256').update(bytes).digest('hex');atlas.sourcePrompts='characters/unarmed-prompts.json';atlas.bodyLayer='unarmed';
  console.log(name,info.width,info.height,atlas.transparentFraction);
 }
 fs.writeFileSync(file,JSON.stringify(manifest,null,2)+'\n');
})().catch(error=>{console.error(error);process.exit(1)});
