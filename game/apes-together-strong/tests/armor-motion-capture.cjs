/* Optional visual evidence from the actual renderer, used by outfitting-browser. */
'use strict';
const fs=require('node:fs'),path=require('node:path');
module.exports=async function(page,out){
 for(const group of ['followers','kings']){
  const result=await page.evaluate(async group=>{
   const names=group==='followers'?['gorilla','chimpanzee','orangutan','gibbon','mandrill','capuchin']:['ironback','shadowhand','emberwood'],looks=group==='followers'?['base','tier1','tier2','tier3']:['base','age1','age2','command','power','shadow'];
   await ATSVisualAssets.preload(names.flatMap(n=>looks.map(l=>'characters-armor-'+n+'-'+l)));
   const cv=document.createElement('canvas'),r=new ATSRenderer(cv);cv.width=1200;cv.height=names.length*190+55;r.camera.zoom=1;r.detailLevel=0;r.quality='high';const c=cv.getContext('2d'),states=['idle','walk','run','attack','heavy','hurt','carry','climb','command','death','chop','build'],chunks=[],samples={};
   const stream=cv.captureStream(24),recorder=new MediaRecorder(stream,{mimeType:'video/webm;codecs=vp9',videoBitsPerSecond:3500000});recorder.ondataavailable=e=>{if(e.data.size)chunks.push(e.data)};const stopped=new Promise(resolve=>recorder.onstop=resolve);recorder.start();
   for(let frame=0;frame<288;frame++){
    const t=frame/24,state=states[Math.floor(t)],phase=t%1;r.time=10+t;r._outfitNewFrames=0;c.fillStyle='#122b27';c.fillRect(0,0,cv.width,cv.height);c.fillStyle='#ead5ab';c.font='22px Arial';c.textAlign='left';c.fillText('APES TOGETHER STRONG  /  '+state.toUpperCase(),25,34);
    for(let row=0;row<names.length;row++)for(let col=0;col<looks.length;col++){
     const name=names[row]+'-'+looks[col],a=armorActor(name,state,Math.floor(t/3)%4);if(a.animation)a.animation.start=r.time-phase*.5;if(a.hp<=0)a.deathAge=phase;
     r._tribalGame={tribe:{id:a.tribeId||'ironback'}};const x=(col+.5)*cv.width/looks.length,y=55+row*190+158;c.save();c.translate(x,y);c.scale(1.55,1.55);r.drawApe(c,a,a.id==='king');c.restore();c.fillStyle='#ead5ab';c.font='13px Arial';c.textAlign='center';c.fillText(name,x,y+24);
    }
    if([29,40,54,66,84,177,227].includes(frame))samples[frame]=cv.toDataURL('image/png').split(',')[1];
    await new Promise(resolve=>setTimeout(resolve,1000/24));
   }
   recorder.stop();await stopped;stream.getTracks().forEach(t=>t.stop());const blob=new Blob(chunks,{type:'video/webm'}),data=await new Promise(resolve=>{const reader=new FileReader();reader.onload=()=>resolve(reader.result.split(',')[1]);reader.readAsDataURL(blob)});return{data,samples};
  },group);
  fs.writeFileSync(path.join(out,group+'-armor-motion.webm'),Buffer.from(result.data,'base64'));
  for(const [frame,data]of Object.entries(result.samples))fs.writeFileSync(path.join(out,group+'-motion-'+frame+'.png'),Buffer.from(data,'base64'));
 }
};
