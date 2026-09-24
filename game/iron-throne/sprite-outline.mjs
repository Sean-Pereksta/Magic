// Alpha compositing works for cross-origin R2 images without CORS pixel readback.
// Cache small, display-sized surfaces instead of full-resolution tinted PNGs.
export class SpriteOutlines {
  constructor(limit=192,createCanvas=()=>document.createElement('canvas')) {
    this.limit=limit;this.createCanvas=createCanvas;this.cache=new Map();
  }
  get(image,key,w,h,color,zoom=1,dpr=1) {
    const edge=zoom<.55?3:zoom<1?2:1.5;
    const density=Math.min(3,Math.max(1,Math.ceil(dpr*Math.max(1,zoom))));
    const id=JSON.stringify([key,w,h,color,edge,density]);
    const hit=this.cache.get(id);
    if(hit?.image===image){this.cache.delete(id);this.cache.set(id,hit);return hit;}
    const pad=edge+2, width=w+pad*2,height=h+pad*2;
    const canvas=this.createCanvas(),mask=this.createCanvas();
    canvas.width=mask.width=Math.ceil(width*density);canvas.height=mask.height=Math.ceil(height*density);
    const c=canvas.getContext('2d'),m=mask.getContext('2d');
    c.scale(density,density);m.scale(density,density);
    const scale=Math.min(w/image.naturalWidth,h/image.naturalHeight),dw=image.naturalWidth*scale,dh=image.naturalHeight*scale;
    const x=pad+(w-dw)/2,y=pad+(h-dh)/2;
    const tint=fill=>{m.clearRect(0,0,width,height);m.globalCompositeOperation='source-over';m.drawImage(image,x,y,dw,dh);m.globalCompositeOperation='source-in';m.fillStyle=fill;m.fillRect(0,0,width,height);m.globalCompositeOperation='source-over';};
    const ring=radius=>{for(let i=0;i<16;i++){const angle=i*Math.PI/8;c.drawImage(mask,Math.cos(angle)*radius,Math.sin(angle)*radius,width,height);}};
    tint('#07131d');ring(edge+1); // Dark separation remains visible on bright terrain.
    tint(color);ring(edge);
    c.drawImage(image,x,y,dw,dh); // Exactly one original sprite, above its silhouette.
    const value={image,canvas,pad,width,height};this.cache.set(id,value);
    if(this.cache.size>this.limit)this.cache.delete(this.cache.keys().next().value);
    return value;
  }
}
