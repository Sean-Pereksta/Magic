/* Lossless runtime encoding. Original RGBA PNGs stay in the reusable library.
 * Run after either manifest generator. Requires the optional sharp package;
 * normal game builds need no image tooling. */
'use strict';
const fs=require('node:fs'),path=require('node:path'),sharp=require('sharp');
const root=path.resolve(__dirname,'../assets/visual');
(async()=>{
 let original=0,encoded=0;
 for(const family of ['settlements','vehicles']){
  const file=path.join(root,family,'manifest.json'),m=JSON.parse(fs.readFileSync(file));
  // Sequential conversion keeps peak decoding memory predictable.
  for(const a of m.atlases){
   const source=a.sourceFile||a.file,target=source.replace(/\.png$/,'.webp');
   const buffer=await sharp(path.join(root,source)).webp({lossless:true,effort:6}).toBuffer();
   const before=await sharp(path.join(root,source)).ensureAlpha().raw().toBuffer(),after=await sharp(buffer).ensureAlpha().raw().toBuffer();
   if(before.length!==after.length)throw Error('Dimensions changed: '+source);
   for(let i=0;i<before.length;i+=4){if(before[i+3]!==after[i+3])throw Error('Alpha changed: '+source);if(before[i+3]&&(before[i]!==after[i]||before[i+1]!==after[i+1]||before[i+2]!==after[i+2]))throw Error('Visible pixel changed: '+source)}
   fs.writeFileSync(path.join(root,target),buffer);original+=fs.statSync(path.join(root,source)).size;encoded+=buffer.length;a.sourceFile=source;a.file=target;
  }
  fs.writeFileSync(file,JSON.stringify(m,null,2)+'\n');
 }
 console.log(JSON.stringify({originalPNGBytes:original,losslessRuntimeBytes:encoded,bytesSaved:original-encoded,visibleRGBA:'identical'},null,2));
})().catch(e=>{console.error(e);process.exitCode=1});
