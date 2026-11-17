// Generate the setup wizard art and multi-resolution icon from the launcher artwork.
const path=require('node:path');
const fs=require('node:fs');
const sharp=require('sharp');
const build=__dirname,art=path.join(build,'../ui/art');
async function icon(){
  const sizes=[16,24,32,48,64,128,256];
  const pngs=await Promise.all(sizes.map(n=>sharp(path.join(art,'catnmice-mark.svg'),{density:384}).resize(n,n).png().toBuffer()));
  const header=Buffer.alloc(6+16*sizes.length);
  header.writeUInt16LE(1,2);header.writeUInt16LE(sizes.length,4);
  let offset=header.length;
  for(let i=0;i<sizes.length;i++){
    const pos=6+16*i;
    header[pos]=header[pos+1]=sizes[i]%256;
    header.writeUInt16LE(1,pos+4);header.writeUInt16LE(32,pos+6);
    header.writeUInt32LE(pngs[i].length,pos+8);header.writeUInt32LE(offset,pos+12);offset+=pngs[i].length;
  }
  fs.writeFileSync(path.join(build,'icon.ico'),Buffer.concat([header,...pngs]));
}
async function wizard(){
  const width=164,height=314;
  const pixels=await sharp(path.join(art,'bannerlands-hero.webp')).resize(width,height,{fit:'cover',position:'right'}).removeAlpha().raw().toBuffer();
  const stride=Math.ceil(width*3/4)*4, bmp=Buffer.alloc(54+height*stride);
  bmp.write('BM');bmp.writeUInt32LE(bmp.length,2);bmp.writeUInt32LE(54,10);bmp.writeUInt32LE(40,14);bmp.writeInt32LE(width,18);bmp.writeInt32LE(height,22);bmp.writeUInt16LE(1,26);bmp.writeUInt16LE(24,28);
  for(let y=0;y<height;y++)for(let x=0;x<width;x++){
    const s=(y*width+x)*3,d=54+(height-y-1)*stride+x*3;
    bmp[d]=pixels[s+2];bmp[d+1]=pixels[s+1];bmp[d+2]=pixels[s];
  }
  fs.writeFileSync(path.join(build,'wizard.bmp'),bmp);
}
Promise.all([icon(),wizard()]).catch(e=>{console.error(e);process.exitCode=1;});
