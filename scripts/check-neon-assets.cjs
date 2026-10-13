// Usage: node scripts/check-neon-assets.cjs https://PUBLIC_R2_ROOT https://catnmice.com
const {parseGLB} = require('../game/neon-arena/neon-models.js');
const asset = require('../game/neon-arena/neon-assets.js');
async function main() {
  const base = new URL(process.argv[2]);
  const origin = new URL(process.argv[3] || 'https://catnmice.com').origin;
  if(!['https:','http:'].includes(base.protocol)) throw new Error('Supply the public HTTP asset root');
  const paths=[...Object.values(asset.weapons),...Object.values(asset.characters),'ui/menu/logo.png','ui/menu/background_desktop.webp','ui/menu/background_mobile.webp','ui/menu/arena_circuit_citadel.webp','ui/menu/arena_prism_garden.webp','ui/menu/arena_obsidian_reactor.webp','ui/menu/arena_breach_ring.webp','ui/menu/menu_panel_frame.png','ui/menu/button_primary.png','ui/menu/button_secondary.png',...['build','fire','health','jump','platform','reload','swap','upgrade','wall'].map(key=>'ui/hud/'+key+'.png')];
  const failures=[];let next=0,checked=0;
  async function worker(){
    while(next<paths.length){const path=paths[next++];
      try{
        const url=base.href.replace(/\/+$/,'')+'/'+path;
        const response=await fetch(url,{headers:{Origin:origin},signal:AbortSignal.timeout(12000)});
        if(!response.ok)throw new Error('HTTP '+response.status);
        const allow=response.headers.get('access-control-allow-origin');
        if(path.endsWith('.glb')&&allow!=='*'&&allow!==origin)throw new Error('GET does not allow model fetch from '+origin);
        const expected=path.endsWith('.glb')?'model/gltf-binary':path.endsWith('.png')?'image/png':'image/webp';
        const type=(response.headers.get('content-type')||'').split(';')[0];
        if(type!==expected)throw new Error('Expected Content-Type '+expected+', received '+type);
        const bytes=await response.arrayBuffer();if(!bytes.byteLength)throw new Error('Empty file');
        if(path.endsWith('.glb'))parseGLB(bytes,path);checked++;
      }catch(error){failures.push({path,error:error.message})}
    }
  }
  await Promise.all([worker(),worker()]);
  console.log(JSON.stringify({checked,total:paths.length,origin,failures},null,2));
  if(failures.length)process.exitCode=1;
}
main().catch(error=>{console.error(error.message);process.exitCode=1});
