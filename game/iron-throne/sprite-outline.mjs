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

// Prepare all tiny display-sized frame contours together, once per atlas/house/
// zoom bucket. Playback crops this surface and allocates no per-frame canvases.
export class SpriteAtlasOutlines {
  constructor(maxPixels=16*1024*1024,createCanvas=()=>document.createElement('canvas')) {this.maxPixels=maxPixels;this.createCanvas=createCanvas;this.cache=new Map();this.pixels=0;}
  get(image,key,spec,w,h,color,zoom=1,dpr=1) {
    const edge=zoom<.55?3:zoom<1?2:1.5,density=Math.min(1.5,Math.max(1,dpr));
    const id=JSON.stringify([key,w,h,color,edge,density]),hit=this.cache.get(id);
    if(hit?.image===image){this.cache.delete(id);this.cache.set(id,hit);return hit;}
    if(hit){this.pixels-=hit.pixels;this.cache.delete(id);}
    const pad=edge+2,cellWidth=Math.ceil((w+2*pad)*density),cellHeight=Math.ceil((h+2*pad)*density),width=cellWidth/density,height=cellHeight/density;
    const canvas=this.createCanvas(),mask=this.createCanvas();
    canvas.width=cellWidth*spec.columns;canvas.height=cellHeight*spec.rows;mask.width=cellWidth;mask.height=cellHeight;
    const c=canvas.getContext('2d'),m=mask.getContext('2d');c.scale(density,density);m.scale(density,density);
    const scale=Math.min(w/spec.cell.width,h/spec.cell.height),dw=spec.cell.width*scale,dh=spec.cell.height*scale,x=pad+(w-dw)/2,y=pad+(h-dh)/2;
    for(let row=0;row<spec.rows;row++)for(let col=0;col<spec.columns;col++) {
      const draw=ctx=>ctx.drawImage(image,col*spec.cell.width,row*spec.cell.height,spec.cell.width,spec.cell.height,x,y,dw,dh);
      const tint=fill=>{m.clearRect(0,0,width,height);m.globalCompositeOperation='source-over';draw(m);m.globalCompositeOperation='source-in';m.fillStyle=fill;m.fillRect(0,0,width,height);m.globalCompositeOperation='source-over';};
      c.save();c.translate(col*width,row*height);
      const ring=radius=>{for(let i=0;i<16;i++){const angle=i*Math.PI/8;c.drawImage(mask,Math.cos(angle)*radius,Math.sin(angle)*radius,width,height);}};
      tint('#07131d');ring(edge+1);tint(color);ring(edge);draw(c);c.restore();
    }
    const value={image,canvas,pad,width,height,cellWidth,cellHeight,pixels:canvas.width*canvas.height};
    while(this.cache.size && this.pixels+value.pixels>this.maxPixels){const oldest=this.cache.keys().next().value;this.pixels-=this.cache.get(oldest).pixels;this.cache.delete(oldest);}
    this.cache.set(id,value);this.pixels+=value.pixels;return value;
  }
}
