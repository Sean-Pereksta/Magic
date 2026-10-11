// Loaded only after the listener explicitly requests the free voice download.
let model;
self.onmessage=async({data})=>{
 try{
  if(data.type==='load'){
   const {KokoroTTS}=await import('https://cdn.jsdelivr.net/npm/kokoro-js@1.2.1/+esm');
   model=await KokoroTTS.from_pretrained('onnx-community/Kokoro-82M-v1.0-ONNX',{dtype:'q8',device:'wasm',progress_callback:p=>{if(p.status==='progress')self.postMessage({type:'progress',progress:Math.round(p.progress||0)});}});
   self.postMessage({type:'ready'});
  }else if(data.type==='speak'&&model){
   const audio=await model.generate(data.text,{voice:data.voice||'af_heart',speed:data.rate||1});
   self.postMessage({type:'audio',id:data.id,samples:audio.audio,rate:audio.sampling_rate},[audio.audio.buffer]);
  }
 }catch(error){self.postMessage({type:'error',id:data.id,error:String(error?.message||error)});}
};
